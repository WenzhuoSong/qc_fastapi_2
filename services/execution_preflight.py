"""Final execution preflight checks before commands are sent to QC."""
from __future__ import annotations

from typing import Any

from services.execution_policy import check_portfolio_exposure, check_weight_allowed, policy_snapshot


def preflight_execution_weights(weights: dict[str, Any]) -> dict[str, Any]:
    """Return blocking policy violations for a proposed execution payload."""
    cap_violations = []
    for ticker, raw_weight in (weights or {}).items():
        ticker = str(ticker or "").upper().strip()
        if ticker == "CASH":
            continue
        weight = float(raw_weight or 0.0)
        if weight <= 0.0:
            continue
        allowed, reason = check_weight_allowed(ticker, weight)
        if not allowed:
            cap_violations.append(
                {
                    "ticker": ticker,
                    "weight": round(weight, 6),
                    "reason": reason,
                }
            )

    group_violations = [row for row in check_portfolio_exposure(weights) if row["violated"]]
    return {
        "allowed": not cap_violations and not group_violations,
        "cap_violations": cap_violations,
        "group_violations": group_violations,
        "policy_version": policy_snapshot()["version"],
    }
