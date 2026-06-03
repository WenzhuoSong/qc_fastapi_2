"""Post-risk executable target contract.

TargetEnvelope is the shared accounting container for the post-risk execution
path. It does not decide whether a target is desirable; it records every
executable target mutation so final validation can replay the ledger instead of
guessing drift ownership from loose dictionaries.
"""
from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any, Literal

from services.mutation_ledger import MutationLedger
from services.mutation_ledger import MutationLedgerError
from services.mutation_ledger import normalize_mutation_type
from services.weight_ops import assert_invariants


WeightMap = dict[str, float]
AuthorityLevel = Literal[
    "construction",
    "risk_approved",
    "post_risk_envelope",
    "post_risk_tightened",
    "final",
]


def default_target_envelope_config(raw: dict[str, Any] | None = None) -> dict[str, Any]:
    """Return rollout config for the post-risk envelope contract."""
    value = dict(raw or {})
    mode = str(value.get("mode") or "active").lower().strip()
    if mode not in {"shadow", "active", "strict"}:
        mode = "active"
    return {
        "enabled": bool(value.get("enabled", True)),
        "mode": mode,
        "shadow_compare_enabled": bool(value.get("shadow_compare_enabled", True)),
        "block_on_accounting_failure": bool(value.get("block_on_accounting_failure", True)),
        "block_on_safety_failure": bool(value.get("block_on_safety_failure", True)),
    }


@dataclass
class TargetEnvelope:
    """Single post-risk executable target container.

    Non-CASH changes must flow through :meth:`mutate`. CASH is derived from the
    non-CASH deltas and checked by ledger replay; direct CASH mutations are not
    tracked by MutationLedger and are therefore rejected.
    """

    current_weights: dict[str, Any]
    risk_approved_target: dict[str, Any]
    stage_base_target: dict[str, Any] | None = None
    authority: AuthorityLevel = "post_risk_envelope"
    cash_key: str = "CASH"
    tolerance: float = 1e-6
    _final_target: WeightMap = field(init=False, repr=False)
    _ledger: MutationLedger = field(init=False, repr=False)
    _stage_snapshots: list[dict[str, Any]] = field(default_factory=list, init=False, repr=False)

    def __post_init__(self) -> None:
        self.cash_key = _clean_ticker(self.cash_key) or "CASH"
        self.current_weights = _clean_weights(self.current_weights, cash_key=self.cash_key)
        self.risk_approved_target = _clean_weights(self.risk_approved_target, cash_key=self.cash_key)
        if self.stage_base_target is None:
            self.stage_base_target = dict(self.risk_approved_target)
        else:
            self.stage_base_target = _clean_weights(self.stage_base_target, cash_key=self.cash_key)
        self._final_target = deepcopy(self.stage_base_target)
        self._final_target.setdefault(self.cash_key, 0.0)
        self._ledger = MutationLedger()

    @property
    def final_target(self) -> WeightMap:
        """Return a copy so callers cannot mutate the envelope by accident."""
        return dict(self._final_target)

    @property
    def ledger(self) -> MutationLedger:
        return self._ledger

    @property
    def stage_snapshots(self) -> list[dict[str, Any]]:
        return deepcopy(self._stage_snapshots)

    def mutate(
        self,
        ticker: str,
        new_weight: float,
        mutation_type: str,
        reason: str,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Change one non-CASH ticker and record the mutation.

        The CASH leg is adjusted by the opposite non-CASH delta. This keeps the
        executable target internally replayable while preserving the rule that
        MutationLedger only records non-CASH ticker mutations.
        """
        clean_ticker = _clean_ticker(ticker)
        if not clean_ticker:
            raise ValueError("ticker is required")
        if clean_ticker == self.cash_key:
            raise ValueError("CASH is derived from non-CASH mutations and cannot be mutated directly")

        before = float(self._final_target.get(clean_ticker, 0.0) or 0.0)
        after = max(float(new_weight or 0.0), 0.0)
        if abs(after - before) <= self.tolerance:
            return

        self._ledger.record(
            mutation_type=mutation_type,
            ticker=clean_ticker,
            before=before,
            after=after,
            reason=reason,
            metadata=metadata or {},
        )
        self._final_target[clean_ticker] = after
        self._final_target[self.cash_key] = float(self._final_target.get(self.cash_key, 0.0) or 0.0) - (after - before)

    def apply_stage_target(
        self,
        new_weights: dict[str, Any],
        mutation_type: str,
        reason: str,
        stage: str,
    ) -> None:
        """Import a stage output through the envelope mutation contract.

        This bridge is intended for migration PRs: existing modules can continue
        returning dict outputs while executable changes are also represented in
        the envelope ledger.
        """
        requested = _clean_weights(new_weights, cash_key=self.cash_key)
        before = dict(self._final_target)
        for ticker in sorted((set(before) | set(requested)) - {self.cash_key}):
            self.mutate(
                ticker=ticker,
                new_weight=float(requested.get(ticker, 0.0) or 0.0),
                mutation_type=mutation_type,
                reason=reason,
                metadata={"stage": stage},
            )

        requested_cash = float(requested.get(self.cash_key, 0.0) or 0.0)
        actual_cash = float(self._final_target.get(self.cash_key, 0.0) or 0.0)
        self._stage_snapshots.append(
            {
                "stage": str(stage or "unknown"),
                "base_target": before,
                "requested_target": requested,
                "final_target": dict(self._final_target),
                "mutation_type": mutation_type,
                "ledger_count": len(self._ledger.mutations),
                "cash_requested": round(requested_cash, 6),
                "cash_actual": round(actual_cash, 6),
                "cash_matches_requested": abs(requested_cash - actual_cash) <= self.tolerance,
            }
        )
        self.stage_base_target = dict(self._final_target)

    def apply_stage_ledger(
        self,
        new_weights: dict[str, Any],
        mutation_ledger: dict[str, Any] | MutationLedger | None,
        fallback_mutation_type: str,
        reason: str,
        stage: str,
    ) -> None:
        """Import a stage output, preserving any structured ledger rows.

        Existing post-risk modules already emit `MutationLedger` diagnostics.
        During migration, this bridge keeps their per-ticker mutation types
        authoritative and only uses `fallback_mutation_type` for unledgered
        diffs between the current envelope target and the requested stage
        output.
        """
        requested = _clean_weights(new_weights, cash_key=self.cash_key)
        before = dict(self._final_target)
        ledger_rows = _raw_ledger_rows(mutation_ledger)
        imported_rows = 0

        for row in ledger_rows:
            ticker = _clean_ticker(row.get("ticker"))
            if not ticker or ticker == self.cash_key:
                continue
            after = _optional_float(row.get("after", row.get("weight_after")))
            if after is None:
                continue
            raw_type = str(row.get("type") or row.get("mutation_type") or "").strip()
            if not raw_type:
                continue
            self.mutate(
                ticker=ticker,
                new_weight=after,
                mutation_type=raw_type,
                reason=str(row.get("reason") or reason),
                metadata={
                    **dict(row.get("metadata") or {}),
                    "stage": stage,
                    "bridge_source": "stage_ledger",
                },
            )
            imported_rows += 1

        for ticker in sorted((set(before) | set(requested) | set(self._final_target)) - {self.cash_key}):
            current_value = float(self._final_target.get(ticker, 0.0) or 0.0)
            requested_value = float(requested.get(ticker, 0.0) or 0.0)
            if abs(current_value - requested_value) <= self.tolerance:
                continue
            self.mutate(
                ticker=ticker,
                new_weight=requested_value,
                mutation_type=fallback_mutation_type,
                reason=reason,
                metadata={
                    "stage": stage,
                    "bridge_source": "fallback_diff",
                },
            )

        requested_cash = float(requested.get(self.cash_key, 0.0) or 0.0)
        actual_cash = float(self._final_target.get(self.cash_key, 0.0) or 0.0)
        self._stage_snapshots.append(
            {
                "stage": str(stage or "unknown"),
                "base_target": before,
                "requested_target": requested,
                "final_target": dict(self._final_target),
                "fallback_mutation_type": fallback_mutation_type,
                "ledger_rows_imported": imported_rows,
                "ledger_count": len(self._ledger.mutations),
                "cash_requested": round(requested_cash, 6),
                "cash_actual": round(actual_cash, 6),
                "cash_matches_requested": abs(requested_cash - actual_cash) <= self.tolerance,
            }
        )
        self.stage_base_target = dict(self._final_target)

    def apply_stage_mutation_ledger(
        self,
        mutation_ledger: dict[str, Any] | MutationLedger | None,
        stage: str,
        reason: str,
    ) -> None:
        """Apply only structured mutation rows, without fallback drift guessing."""
        before = dict(self._final_target)
        imported_rows = 0
        for row in _raw_ledger_rows(mutation_ledger):
            ticker = _clean_ticker(row.get("ticker"))
            if not ticker or ticker == self.cash_key:
                continue
            after = _optional_float(row.get("after", row.get("weight_after")))
            if after is None:
                continue
            raw_type = str(row.get("type") or row.get("mutation_type") or "").strip()
            if not raw_type:
                continue
            self.mutate(
                ticker=ticker,
                new_weight=after,
                mutation_type=raw_type,
                reason=str(row.get("reason") or reason),
                metadata={
                    **dict(row.get("metadata") or {}),
                    "stage": stage,
                    "bridge_source": "direct_stage_ledger",
                },
            )
            imported_rows += 1

        self._stage_snapshots.append(
            {
                "stage": str(stage or "unknown"),
                "base_target": before,
                "final_target": dict(self._final_target),
                "ledger_rows_imported": imported_rows,
                "ledger_count": len(self._ledger.mutations),
                "direct_mutation_ledger_only": True,
            }
        )
        self.stage_base_target = dict(self._final_target)

    def advance_stage(self, stage_name: str) -> None:
        """Record a stage boundary without mutating weights."""
        self._stage_snapshots.append(
            {
                "stage": str(stage_name or "unknown"),
                "base_target": dict(self.stage_base_target or {}),
                "final_target": dict(self._final_target),
                "ledger_count": len(self._ledger.mutations),
                "boundary_only": True,
            }
        )
        self.stage_base_target = dict(self._final_target)

    def replay_ledger(self) -> WeightMap:
        """Replay the full mutation ledger from the risk-approved target."""
        replay = dict(self.risk_approved_target)
        replay.setdefault(self.cash_key, 0.0)
        for mutation in self._ledger.mutations:
            before = float(replay.get(mutation.ticker, 0.0) or 0.0)
            after = float(mutation.weight_after or 0.0)
            replay[mutation.ticker] = after
            replay[self.cash_key] = float(replay.get(self.cash_key, 0.0) or 0.0) - (after - before)
        return replay

    def accounting_check(self) -> list[dict[str, Any]]:
        """Return violations when ledger replay does not match final target."""
        violations: list[dict[str, Any]] = []
        replay = self.replay_ledger()
        for ticker in sorted(set(replay) | set(self._final_target)):
            expected = float(replay.get(ticker, 0.0) or 0.0)
            actual = float(self._final_target.get(ticker, 0.0) or 0.0)
            if abs(expected - actual) > self.tolerance:
                violations.append(
                    {
                        "type": "ledger_replay_mismatch",
                        "ticker": ticker,
                        "expected": round(expected, 6),
                        "actual": round(actual, 6),
                        "delta": round(actual - expected, 6),
                    }
                )

        try:
            assert_invariants(self._final_target, cash_key=self.cash_key, label="target_envelope")
        except AssertionError as exc:
            violations.append(
                {
                    "type": "weight_invariant_violation",
                    "message": str(exc),
                }
            )
        return violations

    def safety_diagnostics(self, policy_context: dict[str, Any] | None = None) -> dict[str, Any]:
        """Direction-only diagnostics for PR1.

        PR3 will move final validation's full safety contract onto the
        envelope. This method already exposes the inputs needed for that move.
        """
        ctx = policy_context or {}
        restricted = _clean_ticker_set(ctx.get("restricted_tickers") or ctx.get("scorecard_restricted_tickers") or [])
        hard_risk = _clean_ticker_set(ctx.get("hard_risk_tickers") or [])
        rows: list[dict[str, Any]] = []
        for ticker in sorted((set(self._final_target) | set(self.current_weights)) - {self.cash_key}):
            current = float(self.current_weights.get(ticker, 0.0) or 0.0)
            final = float(self._final_target.get(ticker, 0.0) or 0.0)
            direction = "neutral"
            if final > current + self.tolerance:
                direction = "increase"
            elif final < current - self.tolerance:
                direction = "reduce"
            rows.append(
                {
                    "ticker": ticker,
                    "current": round(current, 6),
                    "final": round(final, 6),
                    "direction": direction,
                    "restricted": ticker in restricted,
                    "hard_risk": ticker in hard_risk,
                }
            )
        return {
            "restricted_tickers": sorted(restricted),
            "hard_risk_tickers": sorted(hard_risk),
            "rows": rows,
        }

    def to_dict(self) -> dict[str, Any]:
        accounting_violations = self.accounting_check()
        return {
            "contract_version": "target_envelope_v1",
            "authority": self.authority,
            "current_weights": dict(self.current_weights),
            "risk_approved_target": dict(self.risk_approved_target),
            "stage_base_target": dict(self.stage_base_target or {}),
            "final_target": dict(self._final_target),
            "ledger": self._ledger.to_dict(),
            "stage_snapshots": self.stage_snapshots,
            "accounting_ok": not accounting_violations,
            "accounting_violations": accounting_violations,
        }


def _clean_weights(weights: dict[str, Any] | None, *, cash_key: str) -> WeightMap:
    out: WeightMap = {}
    for raw_ticker, raw_weight in (weights or {}).items():
        ticker = _clean_ticker(raw_ticker)
        if not ticker:
            continue
        try:
            value = float(raw_weight or 0.0)
        except (TypeError, ValueError):
            continue
        if value != value:
            continue
        out[ticker] = max(value, 0.0)
    out.setdefault(cash_key, 0.0)
    return out


def _clean_ticker(value: Any) -> str:
    return str(value or "").upper().strip()


def _clean_ticker_set(values: Any) -> set[str]:
    if isinstance(values, str):
        raw_values = [values]
    else:
        raw_values = list(values or [])
    return {
        _clean_ticker(value)
        for value in raw_values
        if _clean_ticker(value)
    }


def _optional_float(value: Any) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _raw_ledger_rows(mutation_ledger: dict[str, Any] | MutationLedger | None) -> list[dict[str, Any]]:
    if isinstance(mutation_ledger, MutationLedger):
        return [mutation.to_dict() for mutation in mutation_ledger.mutations]
    if not isinstance(mutation_ledger, dict):
        return []
    rows = mutation_ledger.get("mutations") or []
    if not isinstance(rows, list):
        return []
    out: list[dict[str, Any]] = []
    for raw in rows:
        if not isinstance(raw, dict):
            continue
        raw_type = str(raw.get("type") or raw.get("mutation_type") or "").strip()
        try:
            normalize_mutation_type(raw_type)
        except MutationLedgerError:
            # Unknown rows remain a final-validation problem; the bridge should
            # not import an untyped mutation into the executable envelope.
            continue
        out.append(raw)
    return out
