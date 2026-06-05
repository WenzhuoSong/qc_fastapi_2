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
CALIBRATION_REPORT_VERSION = "active_basket_calibration_v1"
MATERIAL_TRANSACTION_COST_DRAG_PCT = 0.002


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
    strategy_breadth_report: dict[str, Any] | None = None,
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
    breadth = _strategy_breadth_summary(strategy_breadth_report)
    diagnostics = {
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
        "strategy_breadth": breadth,
        "estimated_independent_clusters": breadth.get("estimated_independent_clusters"),
        "warnings": warnings,
    }
    diagnostics["active_basket_calibration"] = build_active_basket_calibration_report(
        active_basket_diagnostics=diagnostics,
        strategy_breadth_report=strategy_breadth_report or {},
        transaction_cost_summary={},
        realized_contribution_summary={},
    )
    return diagnostics


def build_active_basket_calibration_report(
    *,
    active_basket_diagnostics: dict[str, Any],
    strategy_breadth_report: dict[str, Any] | None,
    transaction_cost_summary: dict[str, Any] | None,
    realized_contribution_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    """Suggest an operator-review basket range from diagnostics.

    This report is deliberately diagnostic-only. It explains whether the
    current active-count range looks too wide/narrow, but it does not mutate
    weights or grant execution authority.
    """
    basket = active_basket_diagnostics if isinstance(active_basket_diagnostics, dict) else {}
    breadth = strategy_breadth_report if isinstance(strategy_breadth_report, dict) else {}
    cost = transaction_cost_summary if isinstance(transaction_cost_summary, dict) else {}
    contribution = realized_contribution_summary if isinstance(realized_contribution_summary, dict) else {}

    current_min = _to_int(basket.get("target_active_count_min"), GLOBAL_ACTIVE_COUNT_TARGET[0])
    current_max = _to_int(basket.get("target_active_count_max"), GLOBAL_ACTIVE_COUNT_TARGET[1])
    observed_count = _to_int(basket.get("active_count"), 0)
    estimated_clusters = _first_int(
        breadth.get("estimated_independent_clusters"),
        (basket.get("strategy_breadth") or {}).get("estimated_independent_clusters")
        if isinstance(basket.get("strategy_breadth"), dict)
        else None,
        basket.get("estimated_independent_clusters"),
    )
    subscale_count = _to_int(basket.get("subscale_count"), 0)
    floor_cleared_count = _to_int(basket.get("floor_cleared_count"), 0)
    cost_drag = _first_float(
        cost.get("transaction_cost_drag_pct"),
        cost.get("drag_pct"),
        cost.get("estimated_cost_pct"),
        cost.get("total_cost_pct"),
        default=0.0,
    )
    low_contribution_tail_count = _first_int(
        contribution.get("low_contribution_tail_count"),
        contribution.get("near_zero_contribution_count"),
        len(contribution.get("low_contribution_tail") or [])
        if isinstance(contribution.get("low_contribution_tail"), list)
        else None,
    ) or 0

    reasons: list[str] = [f"current_range_{current_min}_{current_max}"]
    if estimated_clusters is None:
        suggested_min, suggested_max = current_min, current_max
        reasons.append("estimated_breadth_missing")
    else:
        suggested_min = max(3, min(4, int(estimated_clusters)))
        suggested_max = min(12, max(6, int(estimated_clusters) + 3))
        reasons.append(f"estimated_breadth_{estimated_clusters}")

    if subscale_count > 0:
        suggested_max = max(suggested_min, suggested_max - 1)
        reasons.append("subscale_positions_present")
    if floor_cleared_count > 0:
        suggested_max = max(suggested_min, suggested_max - 1)
        reasons.append("floor_cleared_positions_present")
    if low_contribution_tail_count > 0:
        suggested_max = max(suggested_min, suggested_max - 1)
        reasons.append("low_contribution_tail")
    if cost_drag >= MATERIAL_TRANSACTION_COST_DRAG_PCT:
        suggested_max = max(suggested_min, suggested_max - 1)
        reasons.append("transaction_cost_drag_material")

    if (
        estimated_clusters is not None
        and estimated_clusters > current_max
        and subscale_count == 0
        and floor_cleared_count == 0
        and low_contribution_tail_count == 0
        and cost_drag < MATERIAL_TRANSACTION_COST_DRAG_PCT
    ):
        suggested_max = min(12, max(suggested_max, estimated_clusters + 2))
        reasons.append("high_breadth_without_tail_or_cost_penalty")

    suggested_range = [int(suggested_min), int(suggested_max)]
    if suggested_range[1] < current_max:
        suggestion = "shrink_range_review"
    elif suggested_range[1] > current_max:
        suggestion = "expand_range_review"
    elif suggested_range[0] != current_min:
        suggestion = "retune_min_range_review"
    else:
        suggestion = "keep_current_range_review"

    return {
        "report_version": CALIBRATION_REPORT_VERSION,
        "execution_effect": "diagnostic_only",
        "execution_authority": "none",
        "target_weight_mutation": "none",
        "current_policy": {
            "target_active_count_min": current_min,
            "target_active_count_max": current_max,
        },
        "observed_active_count": observed_count,
        "estimated_independent_clusters": estimated_clusters,
        "estimated_breadth_is_approximation": (
            breadth.get("estimated_breadth_is_approximation", True)
            if isinstance(breadth, dict)
            else True
        ),
        "subscale_position_count": subscale_count,
        "floor_cleared_count": floor_cleared_count,
        "transaction_cost_drag_pct": round(float(cost_drag or 0.0), 6),
        "low_contribution_tail_count": low_contribution_tail_count,
        "suggested_range": suggested_range,
        "suggestion": suggestion,
        "suggestion_reason": reasons,
        "operator_action": "review_only",
        "uses_effective_n_as_breadth": False,
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


def _strategy_breadth_summary(report: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(report, dict) or not report:
        return {
            "available": False,
            "execution_authority": "none",
            "target_weight_mutation": "none",
        }
    return {
        "available": True,
        "report_version": report.get("report_version"),
        "estimated_independent_clusters": report.get("estimated_independent_clusters"),
        "eligible_alpha_strategy_count": report.get("eligible_alpha_strategy_count"),
        "duplication_ratio": report.get("duplication_ratio"),
        "minimum_overlap": report.get("minimum_overlap"),
        "insufficient_overlap_pairs": report.get("insufficient_overlap_pairs"),
        "estimated_breadth_is_approximation": report.get("estimated_breadth_is_approximation", True),
        "execution_authority": report.get("execution_authority", "none"),
        "target_weight_mutation": report.get("target_weight_mutation", "none"),
    }


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _first_int(*values: Any) -> int | None:
    for value in values:
        if value is None or value == "":
            continue
        try:
            return int(value)
        except (TypeError, ValueError):
            continue
    return None


def _first_float(*values: Any, default: float = 0.0) -> float:
    for value in values:
        if value is None or value == "":
            continue
        try:
            number = float(value)
        except (TypeError, ValueError):
            continue
        if number == number:
            return number
    return default
