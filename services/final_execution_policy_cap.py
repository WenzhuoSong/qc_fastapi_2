"""Final execution-policy cap after governance/position-manager edits."""
from __future__ import annotations

from typing import Any

from services.execution_policy import (
    MIN_EXECUTABLE_WEIGHT,
    ROLE_POLICIES,
    TickerRole,
    check_weight_allowed,
    evaluate_policy,
    get_role,
    policy_snapshot,
)
from services.weight_ops import (
    apply_minimum_weight_floor,
    apply_group_caps_cash_first,
    apply_single_caps_cash_first,
    normalize_cash_first,
)
from services.mutation_ledger import MutationLedger
from strategies import compute_rebalance_actions, estimate_cost_pct


def apply_final_execution_policy_cap(
    *,
    target_weights: dict[str, Any],
    current_weights: dict[str, Any],
    rebalance_threshold: float,
) -> dict[str, Any]:
    pre_cap = dict(target_weights or {})
    capped, cap_events, cap_cash_raised, cap_diagnostics = _apply_policy_caps_with_weight_ops(pre_cap)
    post_cap, normalization_diagnostics = normalize_cash_first(capped)
    floored, floor_diagnostics = apply_minimum_weight_floor(
        post_cap,
        min_weight=MIN_EXECUTABLE_WEIGHT,
    )
    floor_events = _floor_events(floor_diagnostics)
    cash_raised = cap_cash_raised + float(floor_diagnostics.get("total_released") or 0.0)
    mutation_ledger = _mutation_ledger_for_final_policy(
        before_weights=pre_cap,
        post_cap_weights=post_cap,
        final_weights=floored,
        cap_events=cap_events,
        floor_events=floor_events,
    )
    policy_evaluation = evaluate_policy(weights=floored, current_weights=current_weights)
    rebalance_actions = compute_rebalance_actions(floored, current_weights or {}, rebalance_threshold)
    mutation_types = mutation_ledger.get("mutation_types") or []
    return {
        "target_weights": floored,
        "policy_version": policy_snapshot()["version"],
        "cap_events": cap_events,
        "floor_events": floor_events,
        "cash_raised": round(cash_raised, 6),
        "cash_raised_by_policy_cap": round(cap_cash_raised, 6),
        "cash_raised_by_minimum_weight_floor": round(
            float(floor_diagnostics.get("total_released") or 0.0),
            6,
        ),
        "cap_diagnostics": cap_diagnostics,
        "floor_diagnostics": floor_diagnostics,
        "normalization_diagnostics": normalization_diagnostics,
        "mutation_types": mutation_types,
        "mutation_ledger": mutation_ledger,
        "policy_evaluation": policy_evaluation,
        "triggered": bool(cap_events or floor_events),
        "rebalance_actions": rebalance_actions,
        "estimated_cost_pct": estimate_cost_pct(rebalance_actions),
        "n_holdings": sum(
            1 for ticker, weight in floored.items()
            if ticker != "CASH" and float(weight or 0.0) >= MIN_EXECUTABLE_WEIGHT
        ),
    }


def _mutation_ledger_for_final_policy(
    *,
    before_weights: dict[str, Any],
    post_cap_weights: dict[str, Any],
    final_weights: dict[str, Any],
    cap_events: list[dict[str, Any]],
    floor_events: list[dict[str, Any]],
) -> dict[str, Any]:
    ledger = MutationLedger()

    before = _clean_float_weights(before_weights)
    post_cap = _clean_float_weights(post_cap_weights)
    if cap_events:
        for ticker in sorted((set(before) | set(post_cap)) - {"CASH"}):
            before_w = float(before.get(ticker, 0.0) or 0.0)
            after_w = float(post_cap.get(ticker, 0.0) or 0.0)
            if after_w < before_w - 1e-9:
                ledger.record(
                    mutation_type="cash_raise_from_policy_cap",
                    ticker=ticker,
                    before=before_w,
                    after=after_w,
                    reason="final execution policy cap released excess weight to CASH",
                )
    final = _clean_float_weights(final_weights)
    for event in floor_events:
        ticker = str(event.get("ticker") or "").upper().strip()
        if not ticker or ticker == "CASH":
            continue
        before_w = float(post_cap.get(ticker, event.get("original", 0.0)) or 0.0)
        after_w = float(final.get(ticker, 0.0) or 0.0)
        if after_w < before_w - 1e-9:
            ledger.record(
                mutation_type="min_executable_weight_floor",
                ticker=ticker,
                before=before_w,
                after=after_w,
                reason=event.get("reason") or "below minimum executable weight",
                metadata={"min_weight": MIN_EXECUTABLE_WEIGHT},
            )
    return ledger.to_dict()


def _clean_float_weights(weights: dict[str, Any]) -> dict[str, float]:
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


def _apply_policy_caps_with_weight_ops(
    weights: dict[str, Any],
) -> tuple[dict[str, float], list[dict[str, Any]], float, dict[str, Any]]:
    single_caps = _single_caps_for_positive_weights(weights)
    after_single, single_diag = apply_single_caps_cash_first(weights, single_caps)
    role_map = {
        ticker: get_role(ticker).value
        for ticker in after_single
        if ticker != "CASH"
    }
    group_caps = {
        role.value: policy.max_total_group_weight
        for role, policy in ROLE_POLICIES.items()
        if role not in {TickerRole.UNKNOWN, TickerRole.WATCHLIST}
        and policy.max_total_group_weight > 0
    }
    after_group, group_diag = apply_group_caps_cash_first(after_single, group_caps, role_map)
    cap_events = _single_cap_events(single_diag) + _group_cap_events(group_diag)
    cash_raised = float(single_diag.get("total_released") or 0.0) + float(
        group_diag.get("total_released") or 0.0
    )
    return after_group, cap_events, cash_raised, {
        "single_cap": single_diag,
        "group_cap": group_diag,
        "contract": "weight_ops_cash_first_v1",
    }


def _single_caps_for_positive_weights(weights: dict[str, Any]) -> dict[str, float]:
    caps: dict[str, float] = {}
    for raw_ticker, raw_weight in (weights or {}).items():
        ticker = str(raw_ticker or "").upper().strip()
        if not ticker or ticker == "CASH":
            continue
        try:
            weight = float(raw_weight or 0.0)
        except (TypeError, ValueError):
            continue
        if weight <= 0.0:
            continue
        role = get_role(ticker)
        policy = ROLE_POLICIES[role]
        caps[ticker] = policy.max_single_weight
    return caps


def _single_cap_events(single_diag: dict[str, Any]) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for row in single_diag.get("cap_events") or []:
        ticker = str(row.get("ticker") or "").upper().strip()
        if not ticker:
            continue
        before = float(row.get("before") or 0.0)
        after = float(row.get("after") or 0.0)
        _, reason = check_weight_allowed(ticker, before)
        events.append(
            {
                "ticker": ticker,
                "role": get_role(ticker).value,
                "original": round(before, 6),
                "capped_to": round(after, 6),
                "released_to_cash": round(float(row.get("released") or 0.0), 6),
                "reason": reason,
            }
        )
    return events


def _group_cap_events(group_diag: dict[str, Any]) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for row in group_diag.get("group_scale_events") or []:
        role = str(row.get("role") or "").strip()
        if not role:
            continue
        events.append(
            {
                "group_role": role,
                "original_total": round(float(row.get("before_total") or 0.0), 6),
                "cap": round(float(row.get("cap") or 0.0), 6),
                "released_to_cash": round(float(row.get("released") or 0.0), 6),
                "action": "proportional_scale_down",
            }
        )
    return events


def _floor_events(floor_diag: dict[str, Any]) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for row in floor_diag.get("cleared_positions") or []:
        ticker = str(row.get("ticker") or "").upper().strip()
        if not ticker:
            continue
        before = float(row.get("before") or row.get("released") or 0.0)
        events.append(
            {
                "ticker": ticker,
                "role": get_role(ticker).value,
                "original": round(before, 6),
                "cleared_to": 0.0,
                "released_to_cash": round(float(row.get("released") or before), 6),
                "min_weight": round(float(floor_diag.get("min_weight") or 0.0), 6),
                "reason": row.get("reason") or "below_minimum_executable_weight",
            }
        )
    return events
