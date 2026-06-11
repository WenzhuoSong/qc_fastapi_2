"""Execution-facing semantics for market scorecard constraints."""
from __future__ import annotations

from typing import Any


NO_ADD_PERMISSIONS = {"hold_or_trim", "reduce_risk_only", "defensive_only", "cash_only"}

STRATEGY_AUTONOMY_NO_ADD_RULES = {
    "strategy_advisory_only",
    "no_actionable_strategy_confidence",
    "strategy_consensus_regime_conflict",
}


def scorecard_no_add_reason(scorecard: dict[str, Any] | None) -> str | None:
    """Return the execution reason that blocks new risk, if any.

    ``require_human_confirmation`` is not itself a FULL_AUTO no-add signal. It
    remains diagnostic unless the underlying scorecard reason says strategy
    evidence lacks enough autonomy for automatic adds.
    """
    row = scorecard or {}
    permission = str(row.get("investment_permission") or "").lower().strip()
    if permission in NO_ADD_PERMISSIONS:
        return "scorecard_no_add"

    triggered = {
        str(item or "").lower().strip()
        for item in (row.get("triggered_rules") or [])
    }
    for rule in sorted(STRATEGY_AUTONOMY_NO_ADD_RULES):
        if rule in triggered:
            return f"scorecard_{rule}"
    return None


def scorecard_blocks_new_risk(scorecard: dict[str, Any] | None) -> bool:
    return scorecard_no_add_reason(scorecard) is not None
