"""Final execution-policy cap after governance/position-manager edits."""
from __future__ import annotations

from typing import Any

from services.execution_policy import apply_policy_caps, policy_snapshot
from strategies import compute_rebalance_actions, estimate_cost_pct


def apply_final_execution_policy_cap(
    *,
    target_weights: dict[str, Any],
    current_weights: dict[str, Any],
    rebalance_threshold: float,
) -> dict[str, Any]:
    pre_cap = dict(target_weights or {})
    capped, cap_events, cash_raised = apply_policy_caps(pre_cap)
    if cash_raised > 0:
        capped["CASH"] = float(capped.get("CASH", 0.0) or 0.0) + cash_raised
    capped = _normalize_weights(capped)
    rebalance_actions = compute_rebalance_actions(capped, current_weights or {}, rebalance_threshold)
    return {
        "target_weights": capped,
        "policy_version": policy_snapshot()["version"],
        "cap_events": cap_events,
        "cash_raised": cash_raised,
        "triggered": bool(cap_events),
        "rebalance_actions": rebalance_actions,
        "estimated_cost_pct": estimate_cost_pct(rebalance_actions),
        "n_holdings": sum(
            1 for ticker, weight in capped.items()
            if ticker != "CASH" and float(weight or 0.0) > 0.01
        ),
    }


def _normalize_weights(weights: dict[str, Any]) -> dict[str, float]:
    clean = {
        str(ticker).upper().strip(): max(float(weight or 0.0), 0.0)
        for ticker, weight in (weights or {}).items()
        if str(ticker or "").strip()
    }
    total = sum(clean.values())
    if total <= 0:
        return {"CASH": 1.0}
    return {ticker: round(weight / total, 6) for ticker, weight in clean.items() if weight > 1e-9}
