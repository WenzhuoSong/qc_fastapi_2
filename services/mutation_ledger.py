"""Structured post-risk mutation ledger.

This module defines the shared contract for ticker-level weight mutations.
It is intentionally independent from the pipeline for PR2; later PRs can wire
position-manager/final-validation into this contract without changing the
semantics again.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from math import isfinite
from typing import Any, Literal


MutationType = Literal[
    "cap_new_buy_to_current",
    "cap_single_buy_delta",
    "cap_trade_count_buys",
    "cash_raise_from_policy_cap",
    "cash_raise_from_group_cap",
    "emergency_reduce_only",
    "decay_risk_auto_reduce",
    "execution_buy_delta_throttle",
    "loss_trim",
    "min_executable_weight_floor",
    "regime_constraint_tighten",
    "sell_delta_throttle",
    "min_hold_defer_sell",
    "turnover_scale_toward_current",
]


TIGHTEN_ONLY_TYPES = frozenset(
    {
        "cap_new_buy_to_current",
        "cap_single_buy_delta",
        "cap_trade_count_buys",
        "cash_raise_from_policy_cap",
        "cash_raise_from_group_cap",
        "emergency_reduce_only",
        "decay_risk_auto_reduce",
        "execution_buy_delta_throttle",
        "loss_trim",
        "min_executable_weight_floor",
        "regime_constraint_tighten",
    }
)

CONDITIONAL_TYPES = frozenset(
    {
        "sell_delta_throttle",
        "min_hold_defer_sell",
        "turnover_scale_toward_current",
    }
)

LEGACY_TYPE_ALIASES = {
    "defer_sell_due_to_min_hold_days": "min_hold_defer_sell",
    "turnover_scale": "turnover_scale_toward_current",
}

ALL_MUTATION_TYPES = frozenset(TIGHTEN_ONLY_TYPES | CONDITIONAL_TYPES)


class MutationLedgerError(ValueError):
    """Raised when a mutation violates the mutation-ledger contract."""


@dataclass(frozen=True)
class TickerMutation:
    mutation_type: str
    ticker: str
    weight_before: float
    weight_after: float
    reason: str
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        mutation_type = normalize_mutation_type(self.mutation_type)
        ticker = _clean_ticker(self.ticker)
        before = _finite_float(self.weight_before, "weight_before")
        after = _finite_float(self.weight_after, "weight_after")
        reason = str(self.reason or "").strip()

        if not ticker:
            raise MutationLedgerError("ticker is required")
        if ticker == "CASH":
            raise MutationLedgerError("MutationLedger tracks non-CASH ticker mutations only")
        if before < 0.0 or after < 0.0:
            raise MutationLedgerError(
                f"{mutation_type} on {ticker} has negative weight: {before}->{after}"
            )

        object.__setattr__(self, "mutation_type", mutation_type)
        object.__setattr__(self, "ticker", ticker)
        object.__setattr__(self, "weight_before", before)
        object.__setattr__(self, "weight_after", after)
        object.__setattr__(self, "reason", reason)

        if self.is_tighten_only and self.weight_after > self.weight_before + 1e-6:
            raise MutationLedgerError(
                f"TIGHTEN_ONLY VIOLATED: {self.mutation_type} on {self.ticker}: "
                f"{self.weight_before:.6f} -> {self.weight_after:.6f}"
            )

    @property
    def is_tighten_only(self) -> bool:
        return self.mutation_type in TIGHTEN_ONLY_TYPES

    @property
    def is_conditional(self) -> bool:
        return self.mutation_type in CONDITIONAL_TYPES

    @property
    def delta(self) -> float:
        return self.weight_after - self.weight_before

    @property
    def delta_vs_target(self) -> float:
        return self.delta

    def to_dict(self) -> dict[str, Any]:
        return {
            "type": self.mutation_type,
            "ticker": self.ticker,
            "before": round(self.weight_before, 6),
            "after": round(self.weight_after, 6),
            "delta": round(self.delta, 6),
            "tighten_only": self.is_tighten_only,
            "conditional": self.is_conditional,
            "reason": self.reason,
            "metadata": dict(self.metadata or {}),
        }


@dataclass
class MutationLedger:
    mutations: list[TickerMutation] = field(default_factory=list)

    def record(
        self,
        *,
        mutation_type: str,
        ticker: str,
        before: float,
        after: float,
        reason: str,
        metadata: dict[str, Any] | None = None,
    ) -> TickerMutation:
        mutation = TickerMutation(
            mutation_type=mutation_type,
            ticker=ticker,
            weight_before=before,
            weight_after=after,
            reason=reason,
            metadata=metadata or {},
        )
        self.mutations.append(mutation)
        return mutation

    def extend(self, mutations: list[TickerMutation]) -> None:
        for mutation in mutations or []:
            if not isinstance(mutation, TickerMutation):
                raise MutationLedgerError("MutationLedger.extend requires TickerMutation objects")
            self.mutations.append(mutation)

    def has_unclassified(self) -> bool:
        return False

    def affected_tickers(self) -> set[str]:
        return {mutation.ticker for mutation in self.mutations}

    def mutation_types(self) -> list[str]:
        out: list[str] = []
        for mutation in self.mutations:
            if mutation.mutation_type not in out:
                out.append(mutation.mutation_type)
        return out

    def is_all_tighten_only(self) -> bool:
        return all(mutation.is_tighten_only for mutation in self.mutations)

    def conditional_mutations(self) -> list[TickerMutation]:
        return [mutation for mutation in self.mutations if mutation.is_conditional]

    def tighten_only_mutations(self) -> list[TickerMutation]:
        return [mutation for mutation in self.mutations if mutation.is_tighten_only]

    def by_ticker(self) -> dict[str, list[TickerMutation]]:
        out: dict[str, list[TickerMutation]] = {}
        for mutation in self.mutations:
            out.setdefault(mutation.ticker, []).append(mutation)
        return out

    def to_dict(self) -> dict[str, Any]:
        conditional = self.conditional_mutations()
        return {
            "contract_version": "mutation_ledger_v1",
            "total_mutations": len(self.mutations),
            "all_tighten_only": self.is_all_tighten_only(),
            "conditional_count": len(conditional),
            "affected_tickers": sorted(self.affected_tickers()),
            "mutation_types": self.mutation_types(),
            "mutations": [mutation.to_dict() for mutation in self.mutations],
        }

    @classmethod
    def from_details(cls, details: list[dict[str, Any]] | None) -> "MutationLedger":
        ledger = cls()
        for raw in details or []:
            if not isinstance(raw, dict):
                continue
            ledger.record(
                mutation_type=str(raw.get("type") or raw.get("mutation_type") or ""),
                ticker=str(raw.get("ticker") or ""),
                before=_finite_float(raw.get("before", raw.get("weight_before", 0.0)), "before"),
                after=_finite_float(raw.get("after", raw.get("weight_after", 0.0)), "after"),
                reason=str(raw.get("reason") or ""),
                metadata=dict(raw.get("metadata") or {}),
            )
        return ledger


def normalize_mutation_type(value: Any) -> str:
    clean = str(value or "").strip()
    clean = LEGACY_TYPE_ALIASES.get(clean, clean)
    if clean not in ALL_MUTATION_TYPES:
        raise MutationLedgerError(f"unknown mutation_type: {value!r}")
    return clean


def _clean_ticker(value: Any) -> str:
    return str(value or "").upper().strip()


def _finite_float(value: Any, field_name: str) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        raise MutationLedgerError(f"{field_name} must be numeric") from None
    if not isfinite(parsed):
        raise MutationLedgerError(f"{field_name} must be finite")
    return parsed
