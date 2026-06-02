"""Final execution-policy cap after governance/position-manager edits."""
from __future__ import annotations

from typing import Any

from services.execution_policy import (
    ROLE_POLICIES,
    TickerRole,
    check_weight_allowed,
    evaluate_policy,
    get_role,
    policy_snapshot,
)
from services.weight_ops import (
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
    capped, cap_events, cash_raised, cap_diagnostics = _apply_policy_caps_with_weight_ops(pre_cap)
    capped, normalization_diagnostics = normalize_cash_first(capped)
    mutation_ledger = _mutation_ledger_for_cap(pre_cap, capped, cap_events)
    policy_evaluation = evaluate_policy(weights=capped, current_weights=current_weights)
    rebalance_actions = compute_rebalance_actions(capped, current_weights or {}, rebalance_threshold)
    return {
        "target_weights": capped,
        "policy_version": policy_snapshot()["version"],
        "cap_events": cap_events,
        "cash_raised": round(cash_raised, 6),
        "cap_diagnostics": cap_diagnostics,
        "normalization_diagnostics": normalization_diagnostics,
        "mutation_types": ["cash_raise_from_policy_cap"] if cap_events else [],
        "mutation_ledger": mutation_ledger,
        "policy_evaluation": policy_evaluation,
        "triggered": bool(cap_events),
        "rebalance_actions": rebalance_actions,
        "estimated_cost_pct": estimate_cost_pct(rebalance_actions),
        "n_holdings": sum(
            1 for ticker, weight in capped.items()
            if ticker != "CASH" and float(weight or 0.0) > 0.01
        ),
    }


def _mutation_ledger_for_cap(
    before_weights: dict[str, Any],
    after_weights: dict[str, Any],
    cap_events: list[dict[str, Any]],
) -> dict[str, Any]:
    ledger = MutationLedger()
    if not cap_events:
        return ledger.to_dict()

    before = _clean_float_weights(before_weights)
    after = _clean_float_weights(after_weights)
    for ticker in sorted((set(before) | set(after)) - {"CASH"}):
        before_w = float(before.get(ticker, 0.0) or 0.0)
        after_w = float(after.get(ticker, 0.0) or 0.0)
        if after_w < before_w - 1e-9:
            ledger.record(
                mutation_type="cash_raise_from_policy_cap",
                ticker=ticker,
                before=before_w,
                after=after_w,
                reason="final execution policy cap released excess weight to CASH",
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
