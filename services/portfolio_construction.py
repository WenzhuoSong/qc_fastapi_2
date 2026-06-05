"""Deterministic portfolio construction layer.

This module operates at portfolio level before per-ticker target governance. It
does not consume raw LLM weights and does not approve execution.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

from services.active_basket_policy import (
    ACTIVE_BASKET_POLICY,
    GLOBAL_ACTIVE_COUNT_TARGET,
    evaluate_active_basket_policy,
)
from services.execution_policy import MIN_EXECUTABLE_WEIGHT, TickerRole, apply_policy_caps, evaluate_policy, get_role
from services.group_contract import GROUP_DEFINITIONS, calc_factor_exposure, get_factor_tags
from services.alpha_decision_profile import redundancy_multiplier
from services.alpha_decision_policy import (
    default_alpha_decision_policy_config,
    evaluate_alpha_decision_policy,
)
from services.weight_ops import (
    apply_group_caps_cash_first,
    apply_single_caps_cash_first,
    normalize_cash_first,
    normalize_proportional,
)


NO_ADD_PERMISSIONS = {"hold_or_trim", "reduce_risk_only", "defensive_only", "cash_only"}
ALPHA_DECISION_OBJECTIVE_CONTRACT_VERSION = "pc_alpha_decision_objective_v1"
PC_BASKET_OBJECTIVE_VERSION = "maximize_effective_n_with_active_basket_v1"
PC_MODE_SHADOW = "shadow"


@dataclass
class ConstructionObjective:
    primary: str = "maximize_effective_n_with_active_basket_policy"
    subject_to: list[str] = field(default_factory=lambda: [
        "signal_quality_not_diluted",
        "alpha_decision_quality_not_diluted",
        "global_active_count_within_active_basket_policy",
        "role_position_counts_within_active_basket_policy",
        "sub_min_executable_positions_excluded",
        "hedge_role_requires_hedge_intent",
        "factor_concentration_within_group_limits",
        "active_basket_exposure_within_multiplier_limit",
        "turnover_within_budget",
        "execution_policy_allowed",
        "etf_evidence_caps",
        "cost_aware_weak_signal_constraints",
        "max_cluster_exposure_by_correlated_strategy_group",
    ])
    turnover_budget: float | None = None
    effective_n_target: int = 8
    allow_cash_raise: bool = True
    rationale: str = (
        "Paper-live canary objective: maximize effective_N inside active_basket_policy "
        "without diluting higher-quality alpha-decision evidence, subject to role/global "
        "position counts, minimum executable weights, hedge intent, factor concentration, "
        "active-basket, evidence-cap, execution-policy, cost, cluster-exposure, and turnover "
        "constraints."
    )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class PortfolioConstructionResult:
    target_weights: dict[str, float]
    pc_objective_version: str
    execution_authority: str
    target_weight_mutation: str
    pc_mode: str
    candidate_weights: dict[str, float]
    basket_evaluation: dict[str, Any]
    objective_terms: dict[str, Any]
    ready_for_gated_review: bool
    factor_exposures: dict[str, float]
    factor_exposure_before: dict[str, float]
    factor_exposure_after: dict[str, float]
    basket_exposure_before: dict[str, Any]
    basket_exposure_after: dict[str, Any]
    effective_n: float
    effective_n_before: float
    effective_n_after: float
    signal_weighted_effective_n_before: float
    signal_weighted_effective_n_after: float
    signal_alignment_score_before: float
    signal_alignment_score_after: float
    signal_objective_metrics: dict[str, Any]
    signal_objective_rows: list[dict[str, Any]]
    independence_adjusted_net_signal_effective_n_before: float
    independence_adjusted_net_signal_effective_n_after: float
    independence_adjusted_signal_alignment_score_before: float
    independence_adjusted_signal_alignment_score_after: float
    alpha_decision_objective_metrics: dict[str, Any]
    alpha_decision_objective_rows: list[dict[str, Any]]
    strategy_cluster_exposure_rows: list[dict[str, Any]]
    turnover: dict[str, Any]
    construction_steps: list[str]
    violations: list[str]
    policy_evaluation: dict[str, Any]
    objective: dict[str, Any]
    construction_source: str
    diagnostics: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class PortfolioConstructionModel:
    """Construct portfolio-level target weights from deterministic inputs."""

    def __init__(self, *, basket_limit_multiplier: float = 0.70) -> None:
        self.basket_limit_multiplier = max(min(float(basket_limit_multiplier), 1.0), 0.0)

    def construct(
        self,
        *,
        base_weights: dict[str, Any],
        current_weights: dict[str, Any],
        signal_strengths: dict[str, Any] | None = None,
        alpha_decision_context: dict[str, Any] | None = None,
        basket_reviews: dict[str, Any] | list[dict[str, Any]] | None = None,
        scorecard_permission: str | None = None,
        turnover_budget: float | None = None,
        objective: ConstructionObjective | None = None,
        hedge_intent: dict[str, Any] | None = None,
    ) -> PortfolioConstructionResult:
        base = _proportional_weights(_clean_weights(base_weights))
        current = _cash_first_weights(_clean_weights(current_weights))
        signals = _clean_signals(signal_strengths or {})
        active_baskets = _active_basket_groups(basket_reviews)
        budget = _optional_float(turnover_budget)
        objective = objective or ConstructionObjective(turnover_budget=budget)
        alpha_context = _clean_alpha_decision_context(alpha_decision_context or {})
        alpha_signals = _alpha_adjusted_signal_strengths(signals, alpha_context)
        steps: list[str] = ["base_weights"]
        violations: list[str] = []
        factor_before = _factor_exposures(base)
        basket_before = _basket_exposures(base, active_baskets, self.basket_limit_multiplier)
        effective_n_before = round(_effective_n(base), 6)
        signal_metrics_before = _signal_objective_metrics(base, signals)
        alpha_metrics_before = _signal_objective_metrics(base, alpha_signals)

        weights = dict(base)
        weights, factor_violations = self._apply_factor_limits(weights)
        violations.extend(factor_violations)
        steps.append("factor_limits")

        weights, basket_violations = self._apply_basket_constraints(weights, active_baskets)
        violations.extend(basket_violations)
        steps.append("basket_constraints")

        if str(scorecard_permission or "") in NO_ADD_PERMISSIONS:
            weights, no_add_violations = _clip_adds_to_current(weights, current)
            violations.extend(no_add_violations)
            steps.append("scorecard_no_add")

        turnover_before = _turnover(weights, current)
        if budget is not None and turnover_before > budget + 1e-9:
            weights = self._allocate_turnover_budget(
                target=weights,
                current=current,
                signal_strengths=signals,
                budget=budget,
            )
            violations.append(f"turnover_budget:{turnover_before:.2%}->{budget:.2%}")
            steps.append("turnover_budget")

        weights = _cash_first_weights(weights)
        turnover_after = _turnover(weights, current)
        factor_exposures = {
            key: round(value, 6)
            for key, value in sorted(calc_factor_exposure(weights).items())
        }
        basket_after = _basket_exposures(weights, active_baskets, self.basket_limit_multiplier)
        effective_n_after = round(_effective_n(weights), 6)
        signal_metrics_after = _signal_objective_metrics(weights, signals)
        alpha_metrics_after = _signal_objective_metrics(weights, alpha_signals)
        signal_objective_metrics = _signal_objective_summary(
            before=signal_metrics_before,
            after=signal_metrics_after,
        )
        alpha_cluster_rows = _strategy_cluster_exposure_rows(
            before=base,
            after=weights,
            alpha_context=alpha_context,
        )
        alpha_decision_objective_metrics = _alpha_decision_objective_summary(
            old_before=signal_metrics_before,
            old_after=signal_metrics_after,
            before=alpha_metrics_before,
            after=alpha_metrics_after,
            cluster_rows=alpha_cluster_rows,
            alpha_context=alpha_context,
        )
        signal_objective_rows = _signal_objective_rows(
            before=base,
            after=weights,
            signals=signals,
        )
        alpha_decision_objective_rows = _alpha_decision_objective_rows(
            before=base,
            after=weights,
            signals=signals,
            alpha_signals=alpha_signals,
            alpha_context=alpha_context,
        )
        policy_evaluation = evaluate_policy(
            weights=weights,
            current_weights=current,
            context={
                "max_turnover_per_cycle": budget,
                "hedge_allowed": str(scorecard_permission or "") not in NO_ADD_PERMISSIONS,
            },
        )
        candidate_weights, candidate_events = _build_active_basket_candidate_weights(
            weights=weights,
            hedge_intent=hedge_intent,
        )
        basket_evaluation = evaluate_active_basket_policy(
            candidate_weights,
            minimum_weight_floor_events=candidate_events.get("minimum_weight_floor_events") or [],
            strategy_breadth_report=alpha_context.get("strategy_breadth_report")
            if isinstance(alpha_context.get("strategy_breadth_report"), dict)
            else None,
        )
        candidate_policy_evaluation = evaluate_policy(
            weights=candidate_weights,
            current_weights=current,
            context={
                "max_turnover_per_cycle": budget,
                "hedge_allowed": bool((hedge_intent or {}).get("add_hedge_etf")),
            },
        )
        basket_evaluation["candidate_policy_evaluation"] = candidate_policy_evaluation
        basket_evaluation["candidate_policy_ok"] = bool(candidate_policy_evaluation.get("allowed"))
        basket_evaluation["candidate_cleanup_events"] = candidate_events
        objective_terms = _active_basket_objective_terms(
            candidate_weights=candidate_weights,
            current_weights=current,
            alpha_metrics=alpha_metrics_after,
            factor_exposures=factor_exposures,
            basket_evaluation=basket_evaluation,
            policy_evaluation=candidate_policy_evaluation,
            turnover_budget=budget,
        )

        return PortfolioConstructionResult(
            target_weights=weights,
            pc_objective_version=PC_BASKET_OBJECTIVE_VERSION,
            execution_authority="none",
            target_weight_mutation="none",
            pc_mode=PC_MODE_SHADOW,
            candidate_weights=candidate_weights,
            basket_evaluation=basket_evaluation,
            objective_terms=objective_terms,
            ready_for_gated_review=False,
            factor_exposures=factor_exposures,
            factor_exposure_before=factor_before,
            factor_exposure_after=factor_exposures,
            basket_exposure_before=basket_before,
            basket_exposure_after=basket_after,
            effective_n=effective_n_after,
            effective_n_before=effective_n_before,
            effective_n_after=effective_n_after,
            signal_weighted_effective_n_before=signal_metrics_before["signal_weighted_effective_n"],
            signal_weighted_effective_n_after=signal_metrics_after["signal_weighted_effective_n"],
            signal_alignment_score_before=signal_metrics_before["signal_alignment_score"],
            signal_alignment_score_after=signal_metrics_after["signal_alignment_score"],
            signal_objective_metrics=signal_objective_metrics,
            signal_objective_rows=signal_objective_rows,
            independence_adjusted_net_signal_effective_n_before=alpha_metrics_before["signal_weighted_effective_n"],
            independence_adjusted_net_signal_effective_n_after=alpha_metrics_after["signal_weighted_effective_n"],
            independence_adjusted_signal_alignment_score_before=alpha_metrics_before["signal_alignment_score"],
            independence_adjusted_signal_alignment_score_after=alpha_metrics_after["signal_alignment_score"],
            alpha_decision_objective_metrics=alpha_decision_objective_metrics,
            alpha_decision_objective_rows=alpha_decision_objective_rows,
            strategy_cluster_exposure_rows=alpha_cluster_rows,
            turnover={
                "estimated_before_budget": round(turnover_before, 6),
                "estimated": round(turnover_after, 6),
                "budget": budget,
                "within_budget": True if budget is None else turnover_after <= budget + 1e-9,
            },
            construction_steps=steps + ["normalization"],
            violations=violations + self._check_violations(weights, active_baskets),
            policy_evaluation=policy_evaluation,
            objective=objective.to_dict(),
            construction_source="portfolio_construction",
            diagnostics={
                "mode": "portfolio_construction",
                "construction_source": "portfolio_construction",
                "objective": objective.to_dict(),
                "execution_effect": "diagnostic_only",
                "execution_authority": "none",
                "target_weight_mutation": "none",
                "pc_mode": PC_MODE_SHADOW,
                "pc_objective_version": PC_BASKET_OBJECTIVE_VERSION,
                "pc_shadow_candidate_is_not_target_builder_input": True,
                "ready_for_gated_review": False,
                "deterministic": True,
                "consumes_raw_llm_adjusted_weights": False,
                "basket_limit_multiplier": self.basket_limit_multiplier,
                "active_basket_reviews": sorted(active_baskets),
                "ticker_count": len([ticker for ticker in weights if ticker != "CASH" and weights[ticker] > 1e-9]),
                "signal_strength_count": len(signals),
                "signal_weighted_objective_enabled": True,
                "alpha_decision_objective_enabled": True,
                "alpha_decision_context_available": bool(alpha_context.get("strategy_rows")),
                "alpha_decision_context_contract_version": alpha_context.get("contract_version"),
                "alpha_decision_policy_mode": alpha_context.get("policy_mode"),
                "alpha_decision_policy_effective_mode": alpha_context.get("policy_effective_mode"),
                "alpha_decision_policy_allocation_effect": alpha_context.get("policy_allocation_effect"),
                "alpha_decision_policy_recommendation_effect": alpha_context.get("policy_recommendation_effect"),
                "signal_objective_warnings": signal_objective_metrics.get("warnings") or [],
                "alpha_decision_objective_warnings": alpha_decision_objective_metrics.get("warnings") or [],
            },
        )

    def _apply_factor_limits(self, weights: dict[str, float]) -> tuple[dict[str, float], list[str]]:
        out = dict(weights)
        violations: list[str] = []
        for group_name, definition in sorted(GROUP_DEFINITIONS.items()):
            exposure = _factor_exposure_for_group(out, group_name)
            if exposure <= definition.limit_pct + 1e-9:
                continue
            members = set(_tickers_with_factor_tag(out, group_name))
            role_map = {
                ticker: group_name if ticker in members else "__other__"
                for ticker in out
                if ticker != "CASH"
            }
            out, _ = apply_group_caps_cash_first(out, {group_name: definition.limit_pct}, role_map)
            violations.append(f"factor_limit:{group_name} {exposure:.2%}->{definition.limit_pct:.2%}")
        return _cash_first_weights(out), violations

    def _apply_basket_constraints(
        self,
        weights: dict[str, float],
        active_baskets: set[str],
    ) -> tuple[dict[str, float], list[str]]:
        out = dict(weights)
        violations: list[str] = []
        for group_name in sorted(active_baskets):
            definition = GROUP_DEFINITIONS.get(group_name)
            if not definition:
                continue
            reduced_limit = definition.limit_pct * self.basket_limit_multiplier
            exposure = sum(float(out.get(ticker, 0.0) or 0.0) for ticker in definition.tickers)
            if exposure <= reduced_limit + 1e-9:
                continue
            members = set(definition.tickers)
            role_map = {
                ticker: group_name if ticker in members else "__other__"
                for ticker in out
                if ticker != "CASH"
            }
            out, _ = apply_group_caps_cash_first(out, {group_name: reduced_limit}, role_map)
            violations.append(f"basket_limit:{group_name} {exposure:.2%}->{reduced_limit:.2%}")
        return _cash_first_weights(out), violations

    def _allocate_turnover_budget(
        self,
        *,
        target: dict[str, float],
        current: dict[str, float],
        signal_strengths: dict[str, float],
        budget: float,
    ) -> dict[str, float]:
        if budget <= 0:
            return dict(current)

        keys = sorted((set(target) | set(current)) - {"CASH"})
        deltas = {
            ticker: float(target.get(ticker, 0.0) or 0.0) - float(current.get(ticker, 0.0) or 0.0)
            for ticker in keys
        }
        priority = sorted(
            keys,
            key=lambda ticker: (
                abs(signal_strengths.get(ticker, 0.0)) * abs(deltas[ticker]),
                abs(deltas[ticker]),
                ticker,
            ),
            reverse=True,
        )

        out = dict(current)
        remaining = budget
        for ticker in priority:
            delta = deltas[ticker]
            if abs(delta) <= 1e-12 or remaining <= 1e-12:
                continue
            allowed_abs_delta = min(abs(delta), remaining)
            out[ticker] = float(current.get(ticker, 0.0) or 0.0) + (1 if delta > 0 else -1) * allowed_abs_delta
            remaining -= allowed_abs_delta
        out["CASH"] = max(1.0 - sum(value for ticker, value in out.items() if ticker != "CASH"), 0.0)
        return _cash_first_weights(out)

    def _check_violations(self, weights: dict[str, float], active_baskets: set[str]) -> list[str]:
        violations: list[str] = []
        for group_name, definition in sorted(GROUP_DEFINITIONS.items()):
            exposure = _factor_exposure_for_group(weights, group_name)
            if exposure > definition.limit_pct + 1e-6:
                violations.append(f"factor_limit_remaining:{group_name} {exposure:.2%}>{definition.limit_pct:.2%}")
        for group_name in sorted(active_baskets):
            definition = GROUP_DEFINITIONS.get(group_name)
            if not definition:
                continue
            reduced_limit = definition.limit_pct * self.basket_limit_multiplier
            exposure = sum(float(weights.get(ticker, 0.0) or 0.0) for ticker in definition.tickers)
            if exposure > reduced_limit + 1e-6:
                violations.append(f"basket_limit_remaining:{group_name} {exposure:.2%}>{reduced_limit:.2%}")
        return violations


def _build_active_basket_candidate_weights(
    *,
    weights: dict[str, float],
    hedge_intent: dict[str, Any] | None,
) -> tuple[dict[str, float], dict[str, Any]]:
    """Build the basket-aware shadow candidate without changing execution target."""
    capped, policy_cap_events, policy_cash_raised = apply_policy_caps(weights)
    candidate = _cash_first_weights(capped)
    events: dict[str, Any] = {
        "policy_cap_events": policy_cap_events,
        "policy_cash_raised": round(float(policy_cash_raised or 0.0), 6),
        "minimum_weight_floor_events": [],
        "role_max_trim_events": [],
        "global_max_trim_events": [],
        "hedge_clear_events": [],
    }

    hedge_allowed = bool((hedge_intent or {}).get("add_hedge_etf"))
    for ticker, weight in sorted(candidate.items()):
        if ticker == "CASH" or weight <= 0:
            continue
        role = get_role(ticker)
        role_policy = ACTIVE_BASKET_POLICY.get(role)
        if role_policy and role_policy.requires_hedge_intent and not hedge_allowed:
            _clear_candidate_weight(
                candidate,
                ticker,
                events["hedge_clear_events"],
                reason="hedge_role_requires_hedge_intent",
            )

    for ticker, weight in sorted(candidate.items()):
        if ticker == "CASH" or weight <= 0:
            continue
        if weight < MIN_EXECUTABLE_WEIGHT:
            _clear_candidate_weight(
                candidate,
                ticker,
                events["minimum_weight_floor_events"],
                reason=f"below_minimum_executable_weight:{MIN_EXECUTABLE_WEIGHT:.4f}",
            )

    for role, role_policy in ACTIVE_BASKET_POLICY.items():
        active = _active_role_positions(candidate, role)
        if len(active) <= role_policy.max_positions:
            continue
        for ticker, _weight in active[role_policy.max_positions:]:
            _clear_candidate_weight(
                candidate,
                ticker,
                events["role_max_trim_events"],
                reason=f"{role.value}_active_count_above_max:{len(active)}>{role_policy.max_positions}",
            )

    _target_min, target_max = GLOBAL_ACTIVE_COUNT_TARGET
    active_positions = _active_positions(candidate)
    if len(active_positions) > target_max:
        for ticker, _weight in active_positions[target_max:]:
            _clear_candidate_weight(
                candidate,
                ticker,
                events["global_max_trim_events"],
                reason=f"global_active_count_above_target:{len(active_positions)}>{target_max}",
            )

    return _cash_first_weights(candidate), events


def _clear_candidate_weight(
    weights: dict[str, float],
    ticker: str,
    event_bucket: list[dict[str, Any]],
    *,
    reason: str,
) -> None:
    weight = float(weights.get(ticker, 0.0) or 0.0)
    if weight <= 0:
        return
    weights[ticker] = 0.0
    weights["CASH"] = float(weights.get("CASH", 0.0) or 0.0) + weight
    event_bucket.append(
        {
            "ticker": ticker,
            "role": get_role(ticker).value,
            "original": round(weight, 6),
            "cleared_to": 0.0,
            "released_to_cash": round(weight, 6),
            "reason": reason,
        }
    )


def _active_positions(weights: dict[str, float]) -> list[tuple[str, float]]:
    rows = [
        (ticker, float(weight or 0.0))
        for ticker, weight in weights.items()
        if ticker != "CASH" and float(weight or 0.0) >= MIN_EXECUTABLE_WEIGHT
    ]
    return sorted(rows, key=lambda item: (-item[1], item[0]))


def _active_role_positions(weights: dict[str, float], role: TickerRole) -> list[tuple[str, float]]:
    return [
        (ticker, weight)
        for ticker, weight in _active_positions(weights)
        if get_role(ticker) == role
    ]


def _active_basket_objective_terms(
    *,
    candidate_weights: dict[str, float],
    current_weights: dict[str, float],
    alpha_metrics: dict[str, Any],
    factor_exposures: dict[str, float],
    basket_evaluation: dict[str, Any],
    policy_evaluation: dict[str, Any],
    turnover_budget: float | None,
) -> dict[str, Any]:
    target_min, target_max = GLOBAL_ACTIVE_COUNT_TARGET
    alpha_support_score = _clamp01(_optional_float(alpha_metrics.get("signal_alignment_score")) or 0.0)
    diversification_score = _clamp01(_effective_n(candidate_weights) / max(float(target_max), 1.0))
    turnover = _turnover(candidate_weights, current_weights)
    turnover_penalty = _clamp01(turnover / turnover_budget) if turnover_budget and turnover_budget > 0 else _clamp01(turnover)
    max_factor_exposure = max([float(value or 0.0) for value in factor_exposures.values()] or [0.0])
    concentration_penalty = _clamp01(max(max_factor_exposure - 0.35, 0.0) / 0.35)
    warnings = list(basket_evaluation.get("warnings") or [])
    active_basket_violation_penalty = _clamp01(len(warnings) * 0.10)
    subscale_count = int(basket_evaluation.get("subscale_count") or 0)
    subscale_position_penalty = _clamp01(subscale_count * 0.05)
    policy_violations = list(policy_evaluation.get("violations") or [])
    policy_violation_penalty = _clamp01(len(policy_violations) * 0.10)
    score = (
        alpha_support_score
        + diversification_score
        - turnover_penalty
        - concentration_penalty
        - active_basket_violation_penalty
        - subscale_position_penalty
        - policy_violation_penalty
    )
    return {
        "pc_objective_version": PC_BASKET_OBJECTIVE_VERSION,
        "execution_authority": "none",
        "target_weight_mutation": "none",
        "alpha_support_score": round(alpha_support_score, 6),
        "diversification_score": round(diversification_score, 6),
        "turnover_penalty": round(turnover_penalty, 6),
        "concentration_penalty": round(concentration_penalty, 6),
        "active_basket_violation_penalty": round(active_basket_violation_penalty, 6),
        "subscale_position_penalty": round(subscale_position_penalty, 6),
        "policy_violation_penalty": round(policy_violation_penalty, 6),
        "score": round(score, 6),
        "turnover": round(turnover, 6),
        "candidate_active_count": int(basket_evaluation.get("active_count") or 0),
        "target_active_count_min": target_min,
        "target_active_count_max": target_max,
        "candidate_policy_ok": bool(policy_evaluation.get("allowed")),
        "warnings": warnings,
        "policy_violations": policy_violations,
    }


def _clamp01(value: float) -> float:
    return max(min(float(value or 0.0), 1.0), 0.0)


def build_construction_signal_strengths(evidence_bundle: dict | None) -> dict[str, float]:
    """Merge deterministic strategy and rotation signals for construction."""
    bundle = evidence_bundle or {}
    strategy_signals = _strategy_signal_strengths(bundle.get("strategies") or {})
    rotation_signals = _clean_signals((bundle.get("rotation") or {}).get("signals") or {})
    return _merge_signal_strengths(strategy_signals, rotation_signals)


def build_construction_alpha_decision_context(
    evidence_bundle: dict | None,
    *,
    alpha_decision_profiles: dict | None = None,
    policy_config: dict | None = None,
) -> dict[str, Any]:
    """Build alpha-quality context for PC diagnostics.

    The context is diagnostic-only. It does not authorize target construction
    and does not change the target weights produced by ``construct``.
    """
    bundle = evidence_bundle or {}
    strategies = bundle.get("strategies") if isinstance(bundle.get("strategies"), dict) else {}
    evidence_rows = _strategy_result_rows(strategies)
    profile_rows = _alpha_decision_profile_rows(alpha_decision_profiles)
    policy = evaluate_alpha_decision_policy(
        default_alpha_decision_policy_config(policy_config or {}),
        alpha_decision_summary=alpha_decision_profiles or {},
    )
    profiles_by_strategy = {
        str(row.get("strategy_id") or ""): row
        for row in profile_rows
        if str(row.get("strategy_id") or "")
    }
    independence = (
        strategies.get("strategy_independence")
        if isinstance(strategies.get("strategy_independence"), dict)
        else {}
    )
    independence_context = _strategy_independence_context(independence)

    strategy_rows: list[dict[str, Any]] = []
    ticker_adjustments: dict[str, dict[str, Any]] = {}
    for row in evidence_rows:
        name = row["strategy_name"]
        profile = profiles_by_strategy.get(name, {})
        selected_tickers = sorted(set(row["selected_tickers"]) | set(profile.get("tickers") or []))
        if not selected_tickers:
            continue
        confidence = _optional_float(row.get("confidence_score"))
        if confidence is None:
            confidence = 1.0 if profile else 0.0
        redundancy = _optional_float(profile.get("redundancy_multiplier"))
        if redundancy is None:
            redundancy = independence_context["redundancy_by_strategy"].get(name, 1.0)
        decision_multiplier = _optional_float(profile.get("decision_multiplier"))
        net_signal_multiplier = decision_multiplier if decision_multiplier is not None else redundancy
        adjusted_signal = round(max(min(confidence * net_signal_multiplier, 1.0), 0.0), 6)
        cluster_id = (
            profile.get("independence_cluster_id")
            or independence_context["cluster_by_strategy"].get(name)
            or f"independent:{name}"
        )
        strategy_row = {
            "strategy_name": name,
            "selected_tickers": selected_tickers,
            "confidence_score": round(float(confidence), 6),
            "cluster_id": cluster_id,
            "redundancy_multiplier": round(float(redundancy), 6),
            "decision_multiplier": round(float(net_signal_multiplier), 6),
            "independence_adjusted_signal_strength": adjusted_signal,
            "decision_status": profile.get("decision_status"),
            "statistical_status": profile.get("statistical_status"),
            "residual_alpha_status": profile.get("residual_alpha_status"),
            "cost_status": profile.get("cost_status"),
            "net_edge_status": profile.get("net_edge_status"),
            "gross_expected_edge": profile.get("gross_expected_edge"),
            "estimated_ibkr_cost_pct": profile.get("estimated_ibkr_cost_pct"),
            "cost_adjusted_edge": profile.get("cost_adjusted_edge"),
            "edge_to_cost_ratio": profile.get("edge_to_cost_ratio"),
        }
        strategy_rows.append(strategy_row)
        for ticker in selected_tickers:
            current = ticker_adjustments.get(ticker)
            candidate = {
                "ticker": ticker,
                "strategy_name": name,
                "cluster_id": cluster_id,
                "redundancy_multiplier": round(float(redundancy), 6),
                "decision_multiplier": round(float(net_signal_multiplier), 6),
                "independence_adjusted_signal_strength": adjusted_signal,
                "net_edge_status": profile.get("net_edge_status"),
                "gross_expected_edge": profile.get("gross_expected_edge"),
                "estimated_ibkr_cost_pct": profile.get("estimated_ibkr_cost_pct"),
                "cost_adjusted_edge": profile.get("cost_adjusted_edge"),
                "edge_to_cost_ratio": profile.get("edge_to_cost_ratio"),
            }
            if current is None or adjusted_signal > float(current.get("independence_adjusted_signal_strength") or 0.0):
                ticker_adjustments[ticker] = candidate

    raw_alpha_count = len({row["strategy_name"] for row in strategy_rows})
    adjusted_count = round(sum(float(row.get("redundancy_multiplier") or 0.0) for row in strategy_rows), 6)
    return {
        "contract_version": ALPHA_DECISION_OBJECTIVE_CONTRACT_VERSION,
        "source": "evidence_bundle_and_alpha_decision_profiles",
        "strategy_rows": strategy_rows,
        "ticker_adjustments": ticker_adjustments,
        "raw_alpha_strategy_count": raw_alpha_count,
        "independence_adjusted_strategy_count": adjusted_count,
        "strategy_independence_status": independence.get("status"),
        "alpha_decision_policy": policy,
        "policy_mode": policy.get("mode"),
        "policy_effective_mode": policy.get("effective_mode"),
        "policy_recommendation_effect": policy.get("recommendation_effect"),
        "policy_allocation_effect": policy.get("allocation_effect"),
        "would_affect_allocation": policy.get("would_affect_allocation"),
        "execution_authority": "none",
        "target_weight_mutation": "none",
    }


def _strategy_result_rows(strategies: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in strategies.get("strategy_results") or []:
        if not isinstance(row, dict):
            continue
        use = str(row.get("suggested_use") or "")
        if use not in {"primary", "advisory"}:
            continue
        if row.get("alpha_source") is False:
            continue
        name = str(row.get("strategy_name") or row.get("strategy") or "").strip()
        if not name:
            continue
        selected = [
            str(ticker or "").upper().strip()
            for ticker in row.get("selected_tickers") or []
            if str(ticker or "").upper().strip() and str(ticker or "").upper().strip() != "CASH"
        ]
        rows.append({
            "strategy_name": name,
            "confidence_score": _optional_float(row.get("confidence_score")) or 0.0,
            "selected_tickers": sorted(set(selected)),
            "suggested_use": use,
        })
    return rows


def _alpha_decision_profile_rows(raw: dict | None) -> list[dict[str, Any]]:
    if not isinstance(raw, dict):
        return []
    rows = raw.get("rows") if isinstance(raw.get("rows"), list) else []
    return [row for row in rows if isinstance(row, dict)]


def _strategy_independence_context(raw: dict[str, Any]) -> dict[str, Any]:
    pairs = [
        _strategy_pair_row(row)
        for row in ((raw.get("pair_rows") or []) + (raw.get("high_correlation_pairs") or []))
        if isinstance(row, dict)
    ]
    graph: dict[str, set[str]] = {}
    positive_by_strategy: dict[str, list[float]] = {}
    for row in pairs:
        left = row.get("left_strategy")
        right = row.get("right_strategy")
        corr = _optional_float(row.get("correlation"))
        if not left or not right or corr is None or corr <= 0:
            continue
        positive_by_strategy.setdefault(left, []).append(corr)
        positive_by_strategy.setdefault(right, []).append(corr)
        if corr >= 0.65:
            graph.setdefault(left, set()).add(right)
            graph.setdefault(right, set()).add(left)

    cluster_by_strategy: dict[str, str] = {}
    for component in _connected_components(graph):
        cluster_id = "corr_cluster:" + _stable_hash(component)[:12]
        for strategy_name in component:
            cluster_by_strategy[strategy_name] = cluster_id

    redundancy_by_strategy = {
        strategy_name: redundancy_multiplier(max(correlations))
        for strategy_name, correlations in positive_by_strategy.items()
        if correlations
    }
    return {
        "cluster_by_strategy": cluster_by_strategy,
        "redundancy_by_strategy": redundancy_by_strategy,
    }


def _strategy_pair_row(row: dict[str, Any]) -> dict[str, Any]:
    left = (
        row.get("left_strategy")
        or row.get("left")
        or row.get("strategy_a")
        or row.get("strategy_1")
    )
    right = (
        row.get("right_strategy")
        or row.get("right")
        or row.get("strategy_b")
        or row.get("strategy_2")
    )
    return {
        "left_strategy": str(left or "").strip(),
        "right_strategy": str(right or "").strip(),
        "correlation": _optional_float(row.get("correlation")),
    }


def _active_basket_groups(raw: dict[str, Any] | list[dict[str, Any]] | None) -> set[str]:
    if not raw:
        return set()
    if isinstance(raw, dict):
        return {str(group).strip() for group, value in raw.items() if str(group).strip() and value}
    groups: set[str] = set()
    for row in raw:
        if not isinstance(row, dict):
            continue
        group = str(row.get("group") or "").strip()
        if group:
            groups.add(group)
    return groups


def _clean_weights(raw: dict[str, Any] | None) -> dict[str, float]:
    out: dict[str, float] = {}
    for ticker, value in (raw or {}).items():
        clean = str(ticker or "").upper().strip()
        if not clean:
            continue
        parsed = _optional_float(value)
        out[clean] = max(parsed if parsed is not None else 0.0, 0.0)
    return out


def _clean_signals(raw: dict[str, Any]) -> dict[str, float]:
    out: dict[str, float] = {}
    for ticker, value in raw.items():
        clean = str(ticker or "").upper().strip()
        parsed = _optional_float(value)
        if clean and parsed is not None:
            out[clean] = max(min(parsed, 1.0), -1.0)
    return out


def _strategy_signal_strengths(strategies: dict | None) -> dict[str, float]:
    out: dict[str, float] = {}
    for row in (strategies or {}).get("strategy_results") or []:
        if not isinstance(row, dict):
            continue
        use = str(row.get("suggested_use") or "")
        if use not in {"primary", "advisory"}:
            continue
        confidence = _optional_float(row.get("confidence_score")) or 0.0
        if confidence <= 0:
            continue
        if row.get("alpha_source") is False:
            continue
        for ticker in row.get("selected_tickers") or []:
            clean = str(ticker or "").upper().strip()
            if not clean or clean == "CASH":
                continue
            out[clean] = max(out.get(clean, 0.0), min(confidence, 1.0))
    return out


def _merge_signal_strengths(
    strategy_signals: dict[str, float],
    rotation_signals: dict[str, float],
    *,
    strategy_weight: float = 0.60,
    rotation_weight: float = 0.40,
) -> dict[str, float]:
    out: dict[str, float] = {}
    for ticker in sorted(set(strategy_signals) | set(rotation_signals)):
        score = (
            strategy_weight * float(strategy_signals.get(ticker, 0.0) or 0.0)
            + rotation_weight * float(rotation_signals.get(ticker, 0.0) or 0.0)
        )
        out[ticker] = round(max(min(score, 1.0), -1.0), 6)
    return out


def _clip_adds_to_current(target: dict[str, float], current: dict[str, float]) -> tuple[dict[str, float], list[str]]:
    out = dict(target)
    violations: list[str] = []
    caps: dict[str, float] = {}
    for ticker in sorted((set(out) | set(current)) - {"CASH"}):
        target_w = float(out.get(ticker, 0.0) or 0.0)
        current_w = float(current.get(ticker, 0.0) or 0.0)
        if target_w > current_w + 1e-9:
            caps[ticker] = current_w
            violations.append(f"scorecard_no_add:{ticker} {target_w:.2%}->{current_w:.2%}")
    if caps:
        out, _ = apply_single_caps_cash_first(out, caps)
    return _cash_first_weights(out), violations


def _factor_exposure_for_group(weights: dict[str, float], group_name: str) -> float:
    return sum(
        float(weights.get(ticker, 0.0) or 0.0)
        for ticker in weights
        if group_name in get_factor_tags(ticker)
    )


def _factor_exposures(weights: dict[str, float]) -> dict[str, float]:
    return {
        key: round(value, 6)
        for key, value in sorted(calc_factor_exposure(weights).items())
    }


def _basket_exposures(
    weights: dict[str, float],
    active_baskets: set[str],
    basket_limit_multiplier: float,
) -> dict[str, Any]:
    rows: dict[str, Any] = {}
    for group_name in sorted(active_baskets):
        definition = GROUP_DEFINITIONS.get(group_name)
        if not definition:
            rows[group_name] = {
                "exposure": 0.0,
                "limit": None,
                "reduced_limit": None,
                "unknown_group": True,
            }
            continue
        exposure = sum(float(weights.get(ticker, 0.0) or 0.0) for ticker in definition.tickers)
        reduced_limit = definition.limit_pct * basket_limit_multiplier
        rows[group_name] = {
            "exposure": round(exposure, 6),
            "limit": round(definition.limit_pct, 6),
            "reduced_limit": round(reduced_limit, 6),
            "violated": exposure > reduced_limit + 1e-9,
        }
    return rows


def _tickers_with_factor_tag(weights: dict[str, float], group_name: str) -> list[str]:
    return [
        ticker
        for ticker in sorted(weights)
        if ticker != "CASH" and group_name in get_factor_tags(ticker)
    ]


def _cash_first_weights(weights: dict[str, Any]) -> dict[str, float]:
    normalized, _ = normalize_cash_first(weights)
    return _round_weight_map(normalized)


def _proportional_weights(weights: dict[str, Any]) -> dict[str, float]:
    normalized, _ = normalize_proportional(weights)
    return _round_weight_map(normalized)


def _round_weight_map(weights: dict[str, Any]) -> dict[str, float]:
    out: dict[str, float] = {}
    for ticker, value in (weights or {}).items():
        clean = str(ticker or "").upper().strip()
        parsed = _optional_float(value)
        if clean and parsed is not None and parsed > 1e-9:
            out[clean] = round(parsed, 6)
    out.setdefault("CASH", 0.0)
    return out


def _turnover(target: dict[str, Any], current: dict[str, Any]) -> float:
    keys = set(target) | set(current)
    return sum(
        abs(float(target.get(ticker, 0.0) or 0.0) - float(current.get(ticker, 0.0) or 0.0))
        for ticker in keys
    ) / 2.0


def _effective_n(weights: dict[str, float]) -> float:
    equity_weights = [
        float(value or 0.0)
        for ticker, value in weights.items()
        if ticker != "CASH" and float(value or 0.0) > 0
    ]
    denom = sum(value * value for value in equity_weights)
    return 1.0 / denom if denom > 0 else 0.0


def _signal_objective_metrics(weights: dict[str, float], signals: dict[str, float]) -> dict[str, float]:
    equity_items = [
        (ticker, float(weight or 0.0))
        for ticker, weight in weights.items()
        if ticker != "CASH" and float(weight or 0.0) > 0
    ]
    equity_weight = sum(weight for _, weight in equity_items)
    if equity_weight <= 0:
        return {
            "effective_n": 0.0,
            "signal_weighted_effective_n": 0.0,
            "signal_alignment_score": 0.0,
            "signal_coverage": 0.0,
            "scored_equity_weight": 0.0,
            "unscored_equity_weight": 0.0,
            "positive_signal_weight": 0.0,
            "negative_signal_weight": 0.0,
            "weighted_positive_signal": 0.0,
        }

    scored_equity_weight = 0.0
    unscored_equity_weight = 0.0
    positive_signal_weight = 0.0
    negative_signal_weight = 0.0
    weighted_positive_signal = 0.0
    adjusted_signal_weights: list[float] = []

    for ticker, weight in equity_items:
        if ticker not in signals:
            unscored_equity_weight += weight
            continue
        scored_equity_weight += weight
        signal = float(signals.get(ticker, 0.0) or 0.0)
        if signal > 0:
            positive_signal_weight += weight
            weighted_positive_signal += weight * signal
            adjusted_signal_weights.append(weight * signal)
        elif signal < 0:
            negative_signal_weight += weight

    signal_alignment = weighted_positive_signal / equity_weight
    adjusted_total = sum(adjusted_signal_weights)
    if adjusted_total > 0:
        denom = sum((value / adjusted_total) ** 2 for value in adjusted_signal_weights)
        signal_weighted_effective_n = 1.0 / denom if denom > 0 else 0.0
    else:
        signal_weighted_effective_n = 0.0

    return {
        "effective_n": round(_effective_n(weights), 6),
        "signal_weighted_effective_n": round(signal_weighted_effective_n, 6),
        "signal_alignment_score": round(signal_alignment, 6),
        "signal_coverage": round(scored_equity_weight / equity_weight, 6),
        "scored_equity_weight": round(scored_equity_weight, 6),
        "unscored_equity_weight": round(unscored_equity_weight, 6),
        "positive_signal_weight": round(positive_signal_weight, 6),
        "negative_signal_weight": round(negative_signal_weight, 6),
        "weighted_positive_signal": round(weighted_positive_signal, 6),
    }


def _signal_objective_summary(
    *,
    before: dict[str, float],
    after: dict[str, float],
) -> dict[str, Any]:
    warnings: list[str] = []
    if (
        after["effective_n"] > before["effective_n"] + 1e-9
        and after["signal_weighted_effective_n"] < before["signal_weighted_effective_n"] - 1e-9
    ):
        warnings.append("diversification_diluted_signal_weighted_effective_n")
    if after["unscored_equity_weight"] > 0.50:
        warnings.append("majority_equity_weight_has_no_signal_strength")
    if after["negative_signal_weight"] > 0.05:
        warnings.append("portfolio_holds_material_negative_signal_weight")

    return {
        "before": before,
        "after": after,
        "delta": {
            key: round(float(after.get(key, 0.0) or 0.0) - float(before.get(key, 0.0) or 0.0), 6)
            for key in sorted(set(before) | set(after))
        },
        "warnings": warnings,
        "execution_authority": "none",
        "target_weight_mutation": "none",
    }


def _alpha_adjusted_signal_strengths(
    signals: dict[str, float],
    alpha_context: dict[str, Any],
) -> dict[str, float]:
    adjustments = alpha_context.get("ticker_adjustments")
    adjustments = adjustments if isinstance(adjustments, dict) else {}
    out: dict[str, float] = {}
    for ticker in sorted(set(signals) | set(adjustments)):
        raw_signal = float(signals.get(ticker, 0.0) or 0.0)
        row = adjustments.get(ticker) if isinstance(adjustments.get(ticker), dict) else {}
        multiplier = _optional_float(row.get("decision_multiplier"))
        if multiplier is None:
            adjusted_strength = _optional_float(row.get("independence_adjusted_signal_strength"))
            if adjusted_strength is not None and ticker not in signals:
                out[ticker] = round(max(min(adjusted_strength, 1.0), -1.0), 6)
            else:
                out[ticker] = 0.0
            continue
        out[ticker] = round(max(min(raw_signal * multiplier, 1.0), -1.0), 6)
    return out


def _alpha_decision_objective_summary(
    *,
    old_before: dict[str, float],
    old_after: dict[str, float],
    before: dict[str, float],
    after: dict[str, float],
    cluster_rows: list[dict[str, Any]],
    alpha_context: dict[str, Any],
) -> dict[str, Any]:
    warnings: list[str] = []
    if not alpha_context.get("strategy_rows"):
        warnings.append("missing_alpha_decision_context")
    if (
        after["effective_n"] > before["effective_n"] + 1e-9
        and after["signal_weighted_effective_n"] < before["signal_weighted_effective_n"] - 1e-9
    ):
        warnings.append("diversification_diluted_independence_adjusted_net_signal_effective_n")
    max_cluster_share = max(
        (_optional_float(row.get("cluster_equity_share_after")) or 0.0 for row in cluster_rows),
        default=0.0,
    )
    if max_cluster_share > 0.70:
        warnings.append("portfolio_construction_mostly_one_strategy_cluster")
    raw_count = _optional_float(alpha_context.get("raw_alpha_strategy_count")) or 0.0
    adjusted_count = _optional_float(alpha_context.get("independence_adjusted_strategy_count")) or 0.0
    if raw_count >= 2 and adjusted_count < raw_count * 0.50:
        warnings.append("alpha_strategy_count_collapses_after_redundancy")

    return {
        "contract_version": ALPHA_DECISION_OBJECTIVE_CONTRACT_VERSION,
        "old_objective": {
            "name": "signal_weighted_effective_n",
            "before": old_before,
            "after": old_after,
        },
        "new_objective": {
            "name": "independence_adjusted_net_signal_effective_n",
            "before": before,
            "after": after,
            "delta": {
                key: round(float(after.get(key, 0.0) or 0.0) - float(before.get(key, 0.0) or 0.0), 6)
                for key in sorted(set(before) | set(after))
            },
        },
        "raw_alpha_strategy_count": alpha_context.get("raw_alpha_strategy_count"),
        "independence_adjusted_strategy_count": alpha_context.get("independence_adjusted_strategy_count"),
        "alpha_decision_policy": alpha_context.get("alpha_decision_policy") or {},
        "policy_mode": alpha_context.get("policy_mode"),
        "policy_effective_mode": alpha_context.get("policy_effective_mode"),
        "policy_recommendation_effect": alpha_context.get("policy_recommendation_effect"),
        "policy_allocation_effect": alpha_context.get("policy_allocation_effect"),
        "would_affect_allocation": alpha_context.get("would_affect_allocation"),
        "max_cluster_equity_share_after": round(max_cluster_share, 6),
        "cluster_count": len(cluster_rows),
        "warnings": sorted(set(warnings)),
        "execution_authority": "none",
        "target_weight_mutation": "none",
    }


def _signal_objective_rows(
    *,
    before: dict[str, float],
    after: dict[str, float],
    signals: dict[str, float],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for ticker in sorted((set(before) | set(after) | set(signals)) - {"CASH"}):
        signal = signals.get(ticker)
        before_weight = float(before.get(ticker, 0.0) or 0.0)
        after_weight = float(after.get(ticker, 0.0) or 0.0)
        positive_signal = max(float(signal or 0.0), 0.0) if signal is not None else 0.0
        rows.append({
            "ticker": ticker,
            "signal_strength": None if signal is None else round(float(signal), 6),
            "weight_before": round(before_weight, 6),
            "weight_after": round(after_weight, 6),
            "weight_delta": round(after_weight - before_weight, 6),
            "signal_weighted_before": round(before_weight * positive_signal, 6),
            "signal_weighted_after": round(after_weight * positive_signal, 6),
            "has_signal": signal is not None,
        })
    return rows


def _alpha_decision_objective_rows(
    *,
    before: dict[str, float],
    after: dict[str, float],
    signals: dict[str, float],
    alpha_signals: dict[str, float],
    alpha_context: dict[str, Any],
) -> list[dict[str, Any]]:
    adjustments = alpha_context.get("ticker_adjustments")
    adjustments = adjustments if isinstance(adjustments, dict) else {}
    rows: list[dict[str, Any]] = []
    for ticker in sorted((set(before) | set(after) | set(signals) | set(alpha_signals)) - {"CASH"}):
        raw_signal = signals.get(ticker)
        alpha_signal = alpha_signals.get(ticker)
        before_weight = float(before.get(ticker, 0.0) or 0.0)
        after_weight = float(after.get(ticker, 0.0) or 0.0)
        adjustment = adjustments.get(ticker) if isinstance(adjustments.get(ticker), dict) else {}
        positive_alpha = max(float(alpha_signal or 0.0), 0.0) if alpha_signal is not None else 0.0
        rows.append({
            "ticker": ticker,
            "raw_signal_strength": None if raw_signal is None else round(float(raw_signal), 6),
            "independence_adjusted_signal_strength": (
                None if alpha_signal is None else round(float(alpha_signal), 6)
            ),
            "weight_before": round(before_weight, 6),
            "weight_after": round(after_weight, 6),
            "weight_delta": round(after_weight - before_weight, 6),
            "alpha_decision_weighted_before": round(before_weight * positive_alpha, 6),
            "alpha_decision_weighted_after": round(after_weight * positive_alpha, 6),
            "strategy_name": adjustment.get("strategy_name"),
            "cluster_id": adjustment.get("cluster_id"),
            "redundancy_multiplier": adjustment.get("redundancy_multiplier"),
            "decision_multiplier": adjustment.get("decision_multiplier"),
            "net_edge_status": adjustment.get("net_edge_status"),
            "gross_expected_edge": adjustment.get("gross_expected_edge"),
            "estimated_ibkr_cost_pct": adjustment.get("estimated_ibkr_cost_pct"),
            "cost_adjusted_edge": adjustment.get("cost_adjusted_edge"),
            "edge_to_cost_ratio": adjustment.get("edge_to_cost_ratio"),
            "policy_effective_mode": alpha_context.get("policy_effective_mode"),
            "allocation_effect": alpha_context.get("policy_allocation_effect"),
            "has_alpha_decision_signal": alpha_signal is not None and abs(float(alpha_signal or 0.0)) > 1e-12,
        })
    return rows


def _strategy_cluster_exposure_rows(
    *,
    before: dict[str, float],
    after: dict[str, float],
    alpha_context: dict[str, Any],
) -> list[dict[str, Any]]:
    strategy_rows = [
        row for row in alpha_context.get("strategy_rows") or []
        if isinstance(row, dict)
    ]
    clusters: dict[str, dict[str, Any]] = {}
    for row in strategy_rows:
        cluster_id = str(row.get("cluster_id") or f"independent:{row.get('strategy_name') or 'unknown'}")
        cluster = clusters.setdefault(
            cluster_id,
            {
                "cluster_id": cluster_id,
                "strategies": set(),
                "tickers": set(),
                "redundancy_multipliers": [],
            },
        )
        strategy_name = str(row.get("strategy_name") or "").strip()
        if strategy_name:
            cluster["strategies"].add(strategy_name)
        for ticker in row.get("selected_tickers") or []:
            clean = str(ticker or "").upper().strip()
            if clean and clean != "CASH":
                cluster["tickers"].add(clean)
        multiplier = _optional_float(row.get("redundancy_multiplier"))
        if multiplier is not None:
            cluster["redundancy_multipliers"].append(multiplier)

    equity_after = sum(float(value or 0.0) for ticker, value in after.items() if ticker != "CASH")
    out: list[dict[str, Any]] = []
    for cluster_id, row in sorted(clusters.items()):
        tickers = sorted(row["tickers"])
        before_weight = sum(float(before.get(ticker, 0.0) or 0.0) for ticker in tickers)
        after_weight = sum(float(after.get(ticker, 0.0) or 0.0) for ticker in tickers)
        multipliers = row["redundancy_multipliers"]
        avg_multiplier = sum(multipliers) / len(multipliers) if multipliers else None
        out.append({
            "cluster_id": cluster_id,
            "strategies": sorted(row["strategies"]),
            "tickers": tickers,
            "weight_before": round(before_weight, 6),
            "weight_after": round(after_weight, 6),
            "weight_delta": round(after_weight - before_weight, 6),
            "cluster_equity_share_after": round(after_weight / equity_after, 6) if equity_after > 0 else 0.0,
            "strategy_count": len(row["strategies"]),
            "ticker_count": len(tickers),
            "avg_redundancy_multiplier": round(avg_multiplier, 6) if avg_multiplier is not None else None,
        })
    return out


def _clean_alpha_decision_context(raw: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(raw, dict):
        return {
            "contract_version": ALPHA_DECISION_OBJECTIVE_CONTRACT_VERSION,
            "strategy_rows": [],
            "ticker_adjustments": {},
            "raw_alpha_strategy_count": 0,
            "independence_adjusted_strategy_count": 0.0,
            "alpha_decision_policy": evaluate_alpha_decision_policy(),
            "policy_mode": "observe",
            "policy_effective_mode": "observe",
            "policy_recommendation_effect": False,
            "policy_allocation_effect": False,
            "would_affect_allocation": True,
            "execution_authority": "none",
            "target_weight_mutation": "none",
        }
    strategy_rows = [
        row for row in raw.get("strategy_rows") or []
        if isinstance(row, dict)
    ]
    adjustments = raw.get("ticker_adjustments") if isinstance(raw.get("ticker_adjustments"), dict) else {}
    policy = raw.get("alpha_decision_policy") if isinstance(raw.get("alpha_decision_policy"), dict) else {}
    return {
        "contract_version": str(raw.get("contract_version") or ALPHA_DECISION_OBJECTIVE_CONTRACT_VERSION),
        "source": raw.get("source"),
        "strategy_rows": strategy_rows,
        "ticker_adjustments": {
            str(ticker or "").upper().strip(): dict(row)
            for ticker, row in adjustments.items()
            if str(ticker or "").upper().strip() and isinstance(row, dict)
        },
        "raw_alpha_strategy_count": int(raw.get("raw_alpha_strategy_count") or len(strategy_rows)),
        "independence_adjusted_strategy_count": _optional_float(raw.get("independence_adjusted_strategy_count")) or 0.0,
        "alpha_decision_policy": policy or evaluate_alpha_decision_policy(),
        "policy_mode": raw.get("policy_mode") or policy.get("mode"),
        "policy_effective_mode": raw.get("policy_effective_mode") or policy.get("effective_mode"),
        "policy_recommendation_effect": bool(raw.get("policy_recommendation_effect")),
        "policy_allocation_effect": bool(raw.get("policy_allocation_effect")),
        "would_affect_allocation": bool(raw.get("would_affect_allocation", True)),
        "execution_authority": "none",
        "target_weight_mutation": "none",
    }


def _connected_components(graph: dict[str, set[str]]) -> list[list[str]]:
    seen: set[str] = set()
    out: list[list[str]] = []
    for node in sorted(graph):
        if node in seen:
            continue
        stack = [node]
        component: list[str] = []
        while stack:
            current = stack.pop()
            if current in seen:
                continue
            seen.add(current)
            component.append(current)
            stack.extend(sorted(graph.get(current, set()) - seen))
        out.append(sorted(component))
    return out


def _stable_hash(value: Any) -> str:
    import hashlib
    import json

    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
