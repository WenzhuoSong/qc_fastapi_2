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


ALLOWED_POST_RISK_MUTATIONS = {
    "cap_new_buy_to_current",
    "cap_single_buy_delta",
    "cap_trade_count_buys",
    "cash_raise_from_policy_cap",
    "emergency_reduce_only",
}

CONDITIONAL_POST_RISK_MUTATIONS = {
    "turnover_scale_toward_current",
    "defer_sell_due_to_min_hold_days",
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
    mutation_types = _unique([str(item) for item in policy_ctx.get("post_risk_mutation_types") or []])
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
        if item not in ALLOWED_POST_RISK_MUTATIONS and item not in CONDITIONAL_POST_RISK_MUTATIONS
    ]
    conditional_mutation_types = [
        item for item in mutation_types if item in CONDITIONAL_POST_RISK_MUTATIONS
    ]
    material_drift_threshold = _optional_float(policy_ctx.get("material_drift_threshold"))
    max_abs_drift = max((abs(float(row["delta"])) for row in drift_rows), default=0.0)
    material_drift = (
        material_drift_threshold is not None
        and max_abs_drift > material_drift_threshold + 1e-12
    )
    conditional_review_required = bool(conditional_mutation_types and material_drift)
    unsafe_untyped_drift = bool(drift_rows and not mutation_types)
    severe_block = bool(severe_violations)
    blocking_mode = str(mode or "observe") == "blocking"
    approved = not severe_block
    if blocking_mode:
        approved = approved and bool(policy_evaluation.get("allowed"))
        approved = approved and not unknown_mutation_types
        approved = approved and not conditional_review_required
        approved = approved and not unsafe_untyped_drift

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
        "allowed_mutation_types": sorted(ALLOWED_POST_RISK_MUTATIONS),
        "conditional_mutation_types": conditional_mutation_types,
        "unknown_mutation_types": unknown_mutation_types,
        "unsafe_untyped_drift": unsafe_untyped_drift,
        "conditional_review_required": conditional_review_required,
        "risk_context": risk_ctx,
        "execution_effect": "hard_block" if severe_block else "observe",
    }


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
