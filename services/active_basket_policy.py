"""Active basket diagnostics for execution-bound targets.

This module makes portfolio breadth explicit. It does not approve, reject, or
mutate weights; enforcement can be introduced later once the diagnostics have
settled in production.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from services.execution_policy import MIN_EXECUTABLE_WEIGHT, TickerRole, get_role


CONTRACT_VERSION = "active_basket_policy_v1"


@dataclass(frozen=True)
class RoleBasketPolicy:
    min_positions: int
    max_positions: int
    min_weight: float
    requires_hedge_intent: bool = False


ACTIVE_BASKET_POLICY: dict[TickerRole, RoleBasketPolicy] = {
    TickerRole.CORE: RoleBasketPolicy(min_positions=1, max_positions=3, min_weight=0.05),
    TickerRole.SECTOR: RoleBasketPolicy(min_positions=0, max_positions=5, min_weight=0.01),
    TickerRole.THEMATIC: RoleBasketPolicy(min_positions=0, max_positions=4, min_weight=0.01),
    TickerRole.SATELLITE: RoleBasketPolicy(min_positions=0, max_positions=3, min_weight=0.005),
    TickerRole.HEDGE: RoleBasketPolicy(
        min_positions=0,
        max_positions=2,
        min_weight=0.005,
        requires_hedge_intent=True,
    ),
}


GLOBAL_ACTIVE_COUNT_TARGET = (4, 10)


def evaluate_active_basket_policy(
    weights: dict[str, Any] | None,
    *,
    minimum_weight_floor_events: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Return active basket diagnostics for a target weight map."""
    clean = _clean_weights(weights)
    role_rows: dict[str, dict[str, Any]] = {}
    active_positions: list[dict[str, Any]] = []
    subscale_positions: list[dict[str, Any]] = []
    warnings: list[str] = []

    for role, policy in ACTIVE_BASKET_POLICY.items():
        role_rows[role.value] = {
            "role": role.value,
            "policy": asdict(policy),
            "active_count": 0,
            "active_weight": 0.0,
            "positions": [],
            "subscale_positions": [],
            "within_count_range": True,
            "over_max_positions": False,
            "below_min_positions": False,
        }

    for ticker, weight in sorted(clean.items()):
        if ticker == "CASH" or weight < MIN_EXECUTABLE_WEIGHT:
            continue
        role = get_role(ticker)
        if role not in ACTIVE_BASKET_POLICY:
            continue
        policy = ACTIVE_BASKET_POLICY[role]
        below_role_min = weight < policy.min_weight
        row = {
            "ticker": ticker,
            "weight": round(weight, 6),
            "role": role.value,
            "below_role_min_weight": below_role_min,
            "role_min_weight": policy.min_weight,
        }
        active_positions.append(row)
        role_summary = role_rows[role.value]
        role_summary["active_count"] += 1
        role_summary["active_weight"] = round(float(role_summary["active_weight"]) + weight, 6)
        role_summary["positions"].append(row)
        if below_role_min:
            role_summary["subscale_positions"].append(row)
            subscale_positions.append(row)

    for role, summary in role_rows.items():
        policy = summary["policy"]
        count = int(summary["active_count"] or 0)
        below_min = count < int(policy["min_positions"])
        over_max = count > int(policy["max_positions"])
        summary["below_min_positions"] = below_min
        summary["over_max_positions"] = over_max
        summary["within_count_range"] = not below_min and not over_max
        if below_min:
            warnings.append(f"{role}_active_count_below_min:{count}<{policy['min_positions']}")
        if over_max:
            warnings.append(f"{role}_active_count_above_max:{count}>{policy['max_positions']}")

    active_count = len(active_positions)
    target_min, target_max = GLOBAL_ACTIVE_COUNT_TARGET
    if active_count < target_min:
        warnings.append(f"global_active_count_below_target:{active_count}<{target_min}")
    if active_count > target_max:
        warnings.append(f"global_active_count_above_target:{active_count}>{target_max}")

    floor_cleared = _floor_cleared_positions(minimum_weight_floor_events or [])
    return {
        "contract_version": CONTRACT_VERSION,
        "execution_effect": "diagnostic_only",
        "minimum_executable_weight": MIN_EXECUTABLE_WEIGHT,
        "target_active_count_min": target_min,
        "target_active_count_max": target_max,
        "active_count": active_count,
        "active_weight": round(sum(row["weight"] for row in active_positions), 6),
        "cash_weight": round(float(clean.get("CASH", 0.0) or 0.0), 6),
        "within_target_active_count": target_min <= active_count <= target_max,
        "roles": role_rows,
        "subscale_positions": subscale_positions,
        "subscale_count": len(subscale_positions),
        "floor_cleared_positions": floor_cleared,
        "floor_cleared_count": len(floor_cleared),
        "warnings": warnings,
    }


def _floor_cleared_positions(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for event in events:
        if not isinstance(event, dict):
            continue
        ticker = str(event.get("ticker") or "").upper().strip()
        if not ticker:
            continue
        try:
            original = float(event.get("original") or 0.0)
        except (TypeError, ValueError):
            original = 0.0
        rows.append(
            {
                "ticker": ticker,
                "weight": round(original, 6),
                "role": str(event.get("role") or get_role(ticker).value),
                "reason": event.get("reason") or "below_minimum_executable_weight",
            }
        )
    return rows


def _clean_weights(weights: dict[str, Any] | None) -> dict[str, float]:
    out: dict[str, float] = {}
    for raw_ticker, raw_weight in (weights or {}).items():
        ticker = str(raw_ticker or "").upper().strip()
        if not ticker:
            continue
        try:
            weight = max(float(raw_weight or 0.0), 0.0)
        except (TypeError, ValueError):
            continue
        if weight > 1e-12 or ticker == "CASH":
            out[ticker] = weight
    return out
