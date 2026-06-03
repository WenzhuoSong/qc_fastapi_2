"""Final validation for post-risk execution targets.

This module is deliberately read-only. It compares the final target that would
be sent to execution against the risk-approved target after governance,
position-manager, and final policy-cap edits have run.
"""
from __future__ import annotations

from typing import Any

from services.execution_policy import (
    ROLE_POLICIES,
    TickerRole,
    evaluate_policy,
    get_role,
)
from services.mutation_ownership import REGIME_CONSTRAINT_MUTATION_TYPE
from services.mutation_ledger import MutationLedger, MutationLedgerError, normalize_mutation_type


ALLOWED_POST_RISK_MUTATIONS = {
    "cap_new_buy_to_current",
    "cap_single_buy_delta",
    "cap_trade_count_buys",
    "cash_raise_from_policy_cap",
    "cash_raise_from_group_cap",
    "decay_risk_auto_reduce",
    "emergency_reduce_only",
    "execution_buy_delta_throttle",
    "loss_trim",
    REGIME_CONSTRAINT_MUTATION_TYPE,
}

CONDITIONAL_POST_RISK_MUTATIONS = {
    "turnover_scale_toward_current",
    "defer_sell_due_to_min_hold_days",
    "min_hold_defer_sell",
    "sell_delta_throttle",
}

SEVERE_CAP_MULTIPLIER = 1.20


def validate_final_execution_target(
    *,
    risk_approved_target: dict[str, Any],
    final_target: dict[str, Any],
    current_weights: dict[str, Any],
    risk_context: dict[str, Any] | None = None,
    policy_context: dict[str, Any] | None = None,
    mode: str = "observe",
) -> dict[str, Any]:
    """Validate final execution target after all post-risk mutations.

    In observe mode, only severe hard-block violations set approved=false. Other
    violations are recorded for calibration before blocking mode is enabled.
    """
    risk_ctx = risk_context or {}
    policy_ctx = policy_context or {}
    risk_target = _clean_weights(risk_approved_target)
    final = _clean_weights(final_target)
    current = _clean_weights(current_weights)
    legacy_mutation_types = _unique([str(item) for item in policy_ctx.get("post_risk_mutation_types") or []])
    mutation_details = _clean_mutation_details(policy_ctx.get("post_risk_mutation_details") or [])
    mutation_ledger, mutation_ledger_errors = _build_mutation_ledger(
        policy_ctx=policy_ctx,
        legacy_details=mutation_details,
    )
    mutation_types = _unique(legacy_mutation_types + mutation_ledger.mutation_types())
    policy_evaluation = evaluate_policy(
        weights=final,
        current_weights=current,
        context=policy_ctx.get("execution_policy_context") or {},
    )
    drift_rows = _drift_rows(risk_target, final)
    severe_violations = _severe_violations(
        final=final,
        current=current,
        hard_risk_tickers=set(policy_ctx.get("hard_risk_tickers") or []),
    )
    unknown_mutation_types = [
        item for item in mutation_types
        if not _is_known_mutation_type(item)
    ]
    conditional_mutation_types = [
        item for item in mutation_types if _is_conditional_mutation_type(item)
    ]
    conditional_detail_tickers = _conditional_ledger_tickers(mutation_ledger)
    if conditional_mutation_types and not conditional_detail_tickers:
        conditional_detail_tickers = _conditional_detail_tickers(
            mutation_details=mutation_details,
            conditional_mutation_types=set(conditional_mutation_types),
        )
    ledger_affected_tickers = mutation_ledger.affected_tickers()
    drift_tickers = {
        str(row.get("ticker") or "").upper().strip()
        for row in drift_rows
        if str(row.get("ticker") or "").upper().strip() != "CASH"
    }
    missing_mutation_ledger_tickers = sorted(drift_tickers - ledger_affected_tickers)
    material_drift_threshold = _optional_float(policy_ctx.get("material_drift_threshold"))
    max_abs_drift = max((abs(float(row["delta"])) for row in drift_rows), default=0.0)
    material_drift = (
        material_drift_threshold is not None
        and max_abs_drift > material_drift_threshold + 1e-12
    )
    human_confirmed = bool(policy_ctx.get("human_confirmed"))
    require_human_confirmation_for_conditional_material_drift = bool(
        policy_ctx.get("require_human_confirmation_for_conditional_material_drift", True)
    )
    conditional_review_required = bool(
        require_human_confirmation_for_conditional_material_drift
        and conditional_mutation_types
        and material_drift
        and not human_confirmed
    )
    conditional_mutation_violations = _conditional_mutation_violations(
        drift_rows=drift_rows,
        risk_target=risk_target,
        final=final,
        current=current,
        restricted_tickers=_restricted_tickers(policy_ctx),
        hard_risk_tickers={
            str(ticker or "").upper().strip()
            for ticker in policy_ctx.get("hard_risk_tickers") or []
            if str(ticker or "").strip()
        },
        affected_tickers=conditional_detail_tickers,
        forced_trim_min_delta=(
            _optional_float(policy_ctx.get("forced_trim_min_delta")) or 0.005
        ),
    ) if conditional_mutation_types else []
    unsafe_untyped_drift = bool(drift_tickers and not mutation_types and not ledger_affected_tickers)
    incomplete_mutation_ledger = bool(drift_tickers and (missing_mutation_ledger_tickers or mutation_ledger_errors))
    severe_block = bool(severe_violations)
    blocking_mode = str(mode or "observe") == "blocking"
    blocking_violations: list[str] = []
    if not policy_evaluation.get("allowed"):
        blocking_violations.append("execution_policy_violation")
    if unknown_mutation_types:
        blocking_violations.append("unknown_post_risk_mutation_type")
    if conditional_review_required:
        blocking_violations.append("conditional_mutation_material_drift_requires_human_confirmation")
    if conditional_mutation_violations:
        blocking_violations.append("conditional_mutation_contract_violation")
    if unsafe_untyped_drift:
        blocking_violations.append("untyped_post_risk_drift")
    if incomplete_mutation_ledger:
        blocking_violations.append("incomplete_mutation_ledger")

    approved = not severe_block
    if blocking_mode:
        approved = approved and bool(policy_evaluation.get("allowed"))
        approved = approved and not unknown_mutation_types
        approved = approved and not conditional_review_required
        approved = approved and not conditional_mutation_violations
        approved = approved and not unsafe_untyped_drift
        approved = approved and not incomplete_mutation_ledger

    return {
        "approved": approved,
        "mode": str(mode or "observe"),
        "severe_block": severe_block,
        "severe_violations": severe_violations,
        "policy_evaluation": policy_evaluation,
        "risk_approved_target": risk_target,
        "final_target": final,
        "current_weights": current,
        "drift": {
            "rows": drift_rows,
            "max_abs_drift": round(max_abs_drift, 6),
            "material_drift_threshold": material_drift_threshold,
            "material_drift": material_drift,
        },
        "mutation_types": mutation_types,
        "mutation_details": mutation_details,
        "mutation_ledger": mutation_ledger.to_dict(),
        "mutation_ledger_errors": mutation_ledger_errors,
        "ledger_affected_tickers": sorted(ledger_affected_tickers),
        "missing_mutation_ledger_tickers": missing_mutation_ledger_tickers,
        "incomplete_mutation_ledger": incomplete_mutation_ledger,
        "allowed_mutation_types": sorted(ALLOWED_POST_RISK_MUTATIONS),
        "conditional_mutation_types": conditional_mutation_types,
        "conditional_detail_tickers": sorted(conditional_detail_tickers) if conditional_detail_tickers is not None else None,
        "unknown_mutation_types": unknown_mutation_types,
        "unsafe_untyped_drift": unsafe_untyped_drift,
        "conditional_review_required": conditional_review_required,
        "require_human_confirmation_for_conditional_material_drift": (
            require_human_confirmation_for_conditional_material_drift
        ),
        "conditional_mutation_violations": conditional_mutation_violations,
        "human_confirmed": human_confirmed,
        "blocking_violations": blocking_violations if blocking_mode else [],
        "risk_context": risk_ctx,
        "execution_effect": "hard_block" if not approved else ("blocking_pass" if blocking_mode else "observe"),
    }


def _build_mutation_ledger(
    *,
    policy_ctx: dict[str, Any],
    legacy_details: list[dict[str, Any]],
) -> tuple[MutationLedger, list[str]]:
    ledger = MutationLedger()
    errors: list[str] = []
    seen: set[tuple[str, str, float, float]] = set()

    def record(raw: dict[str, Any], *, source: str) -> None:
        raw_type = str(raw.get("type") or raw.get("mutation_type") or "").strip()
        ticker = str(raw.get("ticker") or "").upper().strip()
        before = _optional_float(raw.get("before", raw.get("weight_before")))
        after = _optional_float(raw.get("after", raw.get("weight_after")))
        if not raw_type or not ticker or ticker == "CASH":
            errors.append(f"{source}: missing mutation type or ticker")
            return
        if before is None or after is None:
            errors.append(f"{source}:{raw_type}:{ticker}: missing before/after")
            return
        try:
            canonical_type = normalize_mutation_type(raw_type)
        except MutationLedgerError:
            canonical_type = raw_type
        key = (canonical_type, ticker, round(before, 9), round(after, 9))
        if key in seen:
            return
        seen.add(key)
        try:
            ledger.record(
                mutation_type=raw_type,
                ticker=ticker,
                before=before,
                after=after,
                reason=str(raw.get("reason") or f"{source} mutation detail"),
                metadata=dict(raw.get("metadata") or {}),
            )
        except MutationLedgerError as exc:
            errors.append(f"{source}:{raw_type}:{ticker}: {exc}")

    raw_ledgers = policy_ctx.get("post_risk_mutation_ledgers") or []
    if isinstance(raw_ledgers, dict):
        raw_ledgers = [raw_ledgers]
    if not isinstance(raw_ledgers, list):
        errors.append("post_risk_mutation_ledgers must be a list or dict")
        raw_ledgers = []

    for index, raw_ledger in enumerate(raw_ledgers):
        if not isinstance(raw_ledger, dict):
            errors.append(f"ledger[{index}] is not an object")
            continue
        raw_mutations = raw_ledger.get("mutations") or []
        if not isinstance(raw_mutations, list):
            errors.append(f"ledger[{index}].mutations is not a list")
            continue
        for raw_mutation in raw_mutations:
            if not isinstance(raw_mutation, dict):
                errors.append(f"ledger[{index}].mutations contains non-object row")
                continue
            record(raw_mutation, source=f"ledger[{index}]")

    for raw_detail in legacy_details:
        record(raw_detail, source="legacy")

    return ledger, errors


def _is_known_mutation_type(value: str) -> bool:
    clean = str(value or "").strip()
    if clean in ALLOWED_POST_RISK_MUTATIONS or clean in CONDITIONAL_POST_RISK_MUTATIONS:
        return True
    try:
        canonical = normalize_mutation_type(clean)
    except MutationLedgerError:
        return False
    return canonical in ALLOWED_POST_RISK_MUTATIONS or canonical in CONDITIONAL_POST_RISK_MUTATIONS


def _is_conditional_mutation_type(value: str) -> bool:
    clean = str(value or "").strip()
    if clean in CONDITIONAL_POST_RISK_MUTATIONS:
        return True
    try:
        canonical = normalize_mutation_type(clean)
    except MutationLedgerError:
        return False
    return canonical in CONDITIONAL_POST_RISK_MUTATIONS


def _conditional_ledger_tickers(ledger: MutationLedger) -> set[str] | None:
    conditional = ledger.conditional_mutations()
    if not conditional:
        return set()
    return {mutation.ticker for mutation in conditional}


def _severe_violations(
    *,
    final: dict[str, float],
    current: dict[str, float],
    hard_risk_tickers: set[str],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    role_totals: dict[TickerRole, float] = {}
    for ticker, weight in sorted(final.items()):
        if ticker == "CASH" or weight <= 0:
            continue
        role = get_role(ticker)
        policy = ROLE_POLICIES[role]
        if role == TickerRole.UNKNOWN:
            rows.append({"type": "unknown_ticker_positive_weight", "ticker": ticker, "weight": round(weight, 6)})
        if role == TickerRole.WATCHLIST:
            rows.append({"type": "watchlist_ticker_positive_weight", "ticker": ticker, "weight": round(weight, 6)})
        if (
            role not in {TickerRole.UNKNOWN, TickerRole.WATCHLIST}
            and policy.max_single_weight > 0
            and weight > policy.max_single_weight * SEVERE_CAP_MULTIPLIER + 1e-12
        ):
            rows.append(
                {
                    "type": "role_single_cap_severe",
                    "ticker": ticker,
                    "role": role.value,
                    "weight": round(weight, 6),
                    "cap": policy.max_single_weight,
                    "severe_threshold": round(policy.max_single_weight * SEVERE_CAP_MULTIPLIER, 6),
                }
            )
        if ticker in hard_risk_tickers and current.get(ticker, 0.0) <= 1e-9:
            rows.append(
                {
                    "type": "new_hard_risk_exposure",
                    "ticker": ticker,
                    "weight": round(weight, 6),
                }
            )
        role_totals[role] = role_totals.get(role, 0.0) + weight

    for role, total in sorted(role_totals.items(), key=lambda item: item[0].value):
        if role in {TickerRole.UNKNOWN, TickerRole.WATCHLIST}:
            continue
        cap = ROLE_POLICIES[role].max_total_group_weight
        if cap > 0 and total > cap * SEVERE_CAP_MULTIPLIER + 1e-12:
            rows.append(
                {
                    "type": "role_group_cap_severe",
                    "role": role.value,
                    "weight": round(total, 6),
                    "cap": cap,
                    "severe_threshold": round(cap * SEVERE_CAP_MULTIPLIER, 6),
                }
            )
    return rows


def _drift_rows(
    risk_target: dict[str, float],
    final: dict[str, float],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for ticker in sorted(set(risk_target) | set(final)):
        before = float(risk_target.get(ticker, 0.0) or 0.0)
        after = float(final.get(ticker, 0.0) or 0.0)
        delta = after - before
        if abs(delta) <= 1e-9:
            continue
        rows.append(
            {
                "ticker": ticker,
                "risk_approved": round(before, 6),
                "final": round(after, 6),
                "delta": round(delta, 6),
            }
        )
    return rows


def _conditional_mutation_violations(
    *,
    drift_rows: list[dict[str, Any]],
    risk_target: dict[str, float],
    final: dict[str, float],
    current: dict[str, float],
    restricted_tickers: set[str],
    hard_risk_tickers: set[str],
    affected_tickers: set[str] | None = None,
    forced_trim_min_delta: float = 0.005,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for drift in drift_rows:
        ticker = str(drift.get("ticker") or "").upper().strip()
        if not ticker or ticker == "CASH":
            continue
        if affected_tickers is not None and ticker not in affected_tickers:
            continue
        final_weight = float(final.get(ticker, 0.0) or 0.0)
        current_weight = float(current.get(ticker, 0.0) or 0.0)
        restricted = ticker in restricted_tickers
        hard_risk = ticker in hard_risk_tickers
        risk_approved_weight = float(risk_target.get(ticker, 0.0) or 0.0)
        if restricted and current_weight <= 1e-9 and final_weight > 1e-9:
            rows.append(
                {
                    "type": "conditional_creates_new_restricted_exposure",
                    "ticker": ticker,
                    "final": round(final_weight, 6),
                }
            )
            continue
        if restricted and final_weight > risk_approved_weight + 1e-9:
            rows.append(
                {
                    "type": "conditional_reverses_risk_trim",
                    "ticker": ticker,
                    "risk_approved": round(risk_approved_weight, 6),
                    "final": round(final_weight, 6),
                }
            )
            continue
        if restricted and final_weight > current_weight + 1e-9:
            rows.append(
                {
                    "type": "conditional_increases_restricted_ticker",
                    "ticker": ticker,
                    "current": round(current_weight, 6),
                    "final": round(final_weight, 6),
                }
            )
            continue
        if hard_risk:
            actual_trim = current_weight - final_weight
            required_trim = min(forced_trim_min_delta, max(current_weight, 0.0))
            if actual_trim < required_trim - 1e-9:
                rows.append(
                    {
                        "type": "hard_risk_trim_suppressed",
                        "ticker": ticker,
                        "current": round(current_weight, 6),
                        "final": round(final_weight, 6),
                        "actual_trim": round(actual_trim, 6),
                        "min_trim": round(required_trim, 6),
                    }
                )
                continue
        if restricted:
            rows.append(
                {
                    "type": "conditional_reduces_restricted_ticker",
                    "ticker": ticker,
                    "current": round(current_weight, 6),
                    "final": round(final_weight, 6),
                    "blocking": False,
                }
            )
    return [row for row in rows if row.get("blocking", True)]


def _restricted_tickers(policy_ctx: dict[str, Any]) -> set[str]:
    tickers: set[str] = set()
    for key in (
        "hard_risk_tickers",
        "critical_alert_tickers",
        "forced_trim_tickers",
        "scorecard_restricted_tickers",
    ):
        tickers.update(str(item or "").upper().strip() for item in policy_ctx.get(key) or [])
    return {ticker for ticker in tickers if ticker}


def _clean_mutation_details(values: Any) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    if not isinstance(values, list):
        return out
    for raw in values:
        if not isinstance(raw, dict):
            continue
        mutation_type = str(raw.get("type") or "").strip()
        ticker = str(raw.get("ticker") or "").upper().strip()
        if not mutation_type or not ticker or ticker == "CASH":
            continue
        row = {"type": mutation_type, "ticker": ticker}
        for key in ("before", "after"):
            value = _optional_float(raw.get(key))
            if value is not None:
                row[key] = round(value, 6)
        out.append(row)
    return out


def _conditional_detail_tickers(
    *,
    mutation_details: list[dict[str, Any]],
    conditional_mutation_types: set[str],
) -> set[str] | None:
    if not conditional_mutation_types:
        return set()
    conditional_details = [
        row for row in mutation_details
        if str(row.get("type") or "").strip() in conditional_mutation_types
    ]
    detail_types = {
        str(row.get("type") or "").strip()
        for row in conditional_details
    }
    if not conditional_details or detail_types != conditional_mutation_types:
        return None
    rows = [
        str(row.get("ticker") or "").upper().strip()
        for row in conditional_details
    ]
    return {ticker for ticker in rows if ticker and ticker != "CASH"}


def _clean_weights(weights: dict[str, Any] | None) -> dict[str, float]:
    out: dict[str, float] = {}
    for raw_ticker, raw_weight in (weights or {}).items():
        ticker = str(raw_ticker or "").upper().strip()
        if not ticker:
            continue
        try:
            weight = float(raw_weight or 0.0)
        except (TypeError, ValueError):
            weight = 0.0
        if weight > 1e-12:
            out[ticker] = round(max(weight, 0.0), 6)
    return out


def _optional_float(value: Any) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _unique(values: list[str]) -> list[str]:
    out: list[str] = []
    for value in values:
        if value and value not in out:
            out.append(value)
    return out
