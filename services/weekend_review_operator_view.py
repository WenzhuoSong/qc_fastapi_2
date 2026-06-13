"""Read-only operator view for weekend trading review artifacts.

PR5 is deliberately data-first: it projects the persisted weekend review
payload into a stable dashboard/Telegram shape without adding any execution
controls or mutating state.
"""
from __future__ import annotations

from typing import Any

from services.json_safety import json_safe
from services.weekend_review_loader import EXECUTION_AUTHORITY, TARGET_WEIGHT_MUTATION


OPERATOR_VIEW_SCHEMA_VERSION = "weekend_review_operator_view_v1"
OPERATOR_PACK_SCHEMA_VERSION = "weekend_review_operator_pack_v1"


def build_weekend_review_operator_view(payload: dict[str, Any]) -> dict[str, Any]:
    """Build a data-first, review-only projection from a weekend review payload."""
    _assert_review_only(payload)
    metrics_payload = payload.get("weekend_review_metrics") if isinstance(payload.get("weekend_review_metrics"), dict) else {}
    sections = metrics_payload.get("sections") if isinstance(metrics_payload.get("sections"), dict) else {}
    summary = payload.get("weekend_review_summary") if isinstance(payload.get("weekend_review_summary"), dict) else {}
    safety = payload.get("safety_invariants") if isinstance(payload.get("safety_invariants"), dict) else {}

    degradation = _section(sections, "decision_degradation")
    execution = _section(sections, "execution_truth")
    intent = _section(sections, "intent_execution")
    labels = _section(sections, "label_maturity")
    hedge = _section(sections, "hedge_review")
    debate = _section(sections, "debate_impact")
    basket = _section(sections, "basket_portfolio")
    style_opportunity = _section(sections, "style_opportunity")
    decision_funnel = _section(sections, "decision_funnel")
    self_assessment = _section(sections, "weekly_self_assessment")
    scorecard_acceptance = decision_funnel.get("scorecard_semantic_acceptance") or {}
    scorecard_acceptance_status = scorecard_acceptance.get("acceptance") or {}
    cash_drift = decision_funnel.get("cash_drift_attribution") or {}
    strategy_evidence_monitor = decision_funnel.get("strategy_execution_evidence_monitor") or {}

    view = {
        "schema_version": OPERATOR_VIEW_SCHEMA_VERSION,
        "execution_authority": EXECUTION_AUTHORITY,
        "target_weight_mutation": TARGET_WEIGHT_MUTATION,
        "review_only": True,
        "week_start": payload.get("week_start"),
        "week_end": payload.get("week_end"),
        "review_as_of": payload.get("review_as_of"),
        "artifact_count": int(payload.get("weekend_review_artifact_count") or 0),
        "headline": {
            "commands_sent": _metric(execution, "commands_sent"),
            "filled_count": _metric(execution, "filled_count"),
            "noop_count": _metric(execution, "noop_count"),
            "stuck_in_flight_count": _metric(execution, "stuck_in_flight_count"),
            "true_qc_rejected_count": _metric(execution, "true_qc_rejected_count"),
            "preflight_blocked_count": _metric(execution, "preflight_blocked_count"),
            "not_sent_count": _metric(execution, "not_sent_count"),
            "timeout_no_ack_count": _metric(execution, "timeout_no_ack_count"),
            "timeout_no_execution_confirmed_count": _metric(execution, "timeout_no_execution_confirmed_count"),
            "duplicate_target_count": _metric(execution, "duplicate_target_count"),
            "top_blocker": _top_item(intent.get("blocker_distribution") or {}),
            "eligible_label_count": _metric(labels, "eligible_label_count"),
            "excluded_immature_count": _metric(labels, "excluded_immature_count"),
            "hedge_false_negative_count": _metric(hedge, "false_negative_count"),
            "hedge_triggered_no_drop_count": _metric(hedge, "triggered_no_drop_count"),
            "debate_change_rate_status": _rate_status(debate, "debate_change_rate"),
            "decision_degraded_sample_count": _metric(degradation, "degraded_sample_count"),
            "decision_normal_sample_count": _metric(degradation, "normal_sample_count"),
            "scorecard_limited_small_add_status": (
                (scorecard_acceptance_status.get("limited_data_quality_human_required_small_add") or {}).get("status")
            ),
            "scorecard_strategy_advisory_block_status": (
                (scorecard_acceptance_status.get("strategy_advisory_only_scorecard_block") or {}).get("status")
            ),
            "cash_drift_residual_sum": cash_drift.get("residual_sum"),
            "certification_flip_count_7d": int(strategy_evidence_monitor.get("total_flip_count_7d") or 0),
            "certification_flip_alert_strategy_count": len(strategy_evidence_monitor.get("alert_strategies") or []),
            "certification_readiness_strategy_count": int(
                strategy_evidence_monitor.get("readiness_strategy_count") or 0
            ),
            "certification_insufficient_strategy_count": int(
                strategy_evidence_monitor.get("insufficient_execution_evidence_strategy_count") or 0
            ),
            "certification_live_samples_min_failed_strategy_count": int(
                strategy_evidence_monitor.get("live_samples_min_failed_strategy_count") or 0
            ),
            "certification_zero_live_sample_strategy_count": int(
                strategy_evidence_monitor.get("zero_live_sample_strategy_count") or 0
            ),
            "certification_live_sample_stalled_strategy_count": int(
                strategy_evidence_monitor.get("live_sample_stalled_strategy_count") or 0
            ),
            "blocked_buy_mature_count_1d": _metric(style_opportunity, "blocked_buy_mature_count_1d"),
            "blocked_buy_mature_count_5d": _metric(style_opportunity, "blocked_buy_mature_count_5d"),
            "blocked_buy_mature_count_20d": _metric(style_opportunity, "blocked_buy_mature_count_20d"),
            "blocked_buy_outperformed_benchmark_count_1d": _metric(
                style_opportunity,
                "blocked_buy_outperformed_benchmark_count_1d",
            ),
            "blocked_buy_outperformed_benchmark_count_5d": _metric(
                style_opportunity,
                "blocked_buy_outperformed_benchmark_count_5d",
            ),
            "blocked_buy_outperformed_benchmark_count_20d": _metric(
                style_opportunity,
                "blocked_buy_outperformed_benchmark_count_20d",
            ),
            "safety_invariant_finding_count": int(safety.get("finding_count") or 0),
            "safety_fail_safe_required": bool(safety.get("fail_safe_required")),
        },
        "sections": {
            "decision_degradation": {
                "metrics": degradation.get("metrics") or {},
                "mode_distribution": degradation.get("mode_distribution") or {},
                "fallback_distribution": degradation.get("fallback_distribution") or {},
                "missing_input_distribution": degradation.get("missing_input_distribution") or {},
                "by_observation_type": degradation.get("by_observation_type") or {},
            },
            "safety_invariants": {
                "schema_version": safety.get("schema_version"),
                "finding_count": int(safety.get("finding_count") or 0),
                "fail_safe_required": bool(safety.get("fail_safe_required")),
                "findings": safety.get("findings") or [],
                "effective_states": safety.get("effective_states") or {},
            },
            "execution_truth": {
                "metrics": execution.get("metrics") or {},
                "evidence_refs": execution.get("evidence_refs") or [],
            },
            "blocker_distribution": {
                "metrics": intent.get("metrics") or {},
                "blocker_distribution": intent.get("blocker_distribution") or {},
                "decision_degradation_split": intent.get("decision_degradation_split") or {},
                "unexecuted_intents": intent.get("unexecuted_intents") or [],
            },
            "label_maturity": {
                "metrics": labels.get("metrics") or {},
                "hard_rule": labels.get("hard_rule"),
            },
            "hedge_review": {
                "metrics": hedge.get("metrics") or {},
                "rates": hedge.get("rates") or {},
                "decision_degradation_split": hedge.get("decision_degradation_split") or {},
                "counterfactuals": hedge.get("counterfactuals") or [],
            },
            "debate_value": {
                "metrics": debate.get("metrics") or {},
                "rates": debate.get("rates") or {},
                "decision_degradation_split": debate.get("decision_degradation_split") or {},
            },
            "basket_portfolio": {
                "metrics": basket.get("metrics") or {},
                "rates": basket.get("rates") or {},
                "decision_degradation_split": basket.get("decision_degradation_split") or {},
            },
            "style_opportunity": {
                "metrics": style_opportunity.get("metrics") or {},
                "rates": style_opportunity.get("rates") or {},
                "decision_degradation_split": style_opportunity.get("decision_degradation_split") or {},
                "metric_contract": style_opportunity.get("metric_contract") or {},
            },
            "decision_funnel": {
                "metrics": decision_funnel.get("metrics") or {},
                "rates": decision_funnel.get("rates") or {},
                "first_blocker_distribution": decision_funnel.get("first_blocker_distribution") or {},
                "scorecard_semantic_acceptance": scorecard_acceptance,
                "cash_drift_attribution": cash_drift,
                "strategy_execution_evidence_monitor": strategy_evidence_monitor,
                "sell_side_attribution": decision_funnel.get("sell_side_attribution") or {},
            },
            "prior_review_self_assessment": {
                "metrics": self_assessment.get("metrics") or {},
                "rates": self_assessment.get("rates") or {},
            },
        },
        "recommendations": _review_only_recommendations(summary),
        "summary": {
            "schema_version": summary.get("schema_version"),
            "removed_forbidden_line_count": int(summary.get("removed_forbidden_line_count") or 0),
            "summary_text": summary.get("summary_text") or "",
        },
    }
    view["acceptance_answers"] = build_weekend_review_acceptance_answers(view)
    return json_safe(view)


def build_weekend_review_operator_pack(
    payload: dict[str, Any],
    *,
    include_full_report: bool = False,
) -> dict[str, Any]:
    """Build the read-only operator pack exposed by API/dashboard surfaces."""
    view = build_weekend_review_operator_view(payload)
    pack = {
        "schema_version": OPERATOR_PACK_SCHEMA_VERSION,
        "execution_authority": EXECUTION_AUTHORITY,
        "target_weight_mutation": TARGET_WEIGHT_MUTATION,
        "review_only": True,
        "text": format_weekend_review_operator_text(view),
        "view": view,
        "full_report": payload if include_full_report else None,
    }
    return json_safe(pack)


async def load_latest_weekend_review_operator_view(*, limit: int = 1) -> list[dict[str, Any]]:
    """Load latest weekend review rows as read-only operator views."""
    from sqlalchemy import desc, select

    from db.models import AgentAnalysis
    from db.session import AsyncSessionLocal

    async with AsyncSessionLocal() as db:
        rows = list((
            await db.execute(
                select(AgentAnalysis)
                .where(AgentAnalysis.trigger_type == "weekend_review")
                .order_by(desc(AgentAnalysis.analyzed_at), desc(AgentAnalysis.id))
                .limit(limit)
            )
        ).scalars().all())
    return [
        build_weekend_review_operator_view(row.risk_output or {})
        for row in rows
    ]


async def load_latest_weekend_review_operator_pack(
    *,
    include_full_report: bool = False,
) -> dict[str, Any] | None:
    """Load the latest weekend review row as a stable operator pack."""
    from sqlalchemy import desc, select

    from db.models import AgentAnalysis
    from db.session import AsyncSessionLocal

    async with AsyncSessionLocal() as db:
        row = (
            await db.execute(
                select(AgentAnalysis)
                .where(AgentAnalysis.trigger_type == "weekend_review")
                .order_by(desc(AgentAnalysis.analyzed_at), desc(AgentAnalysis.id))
                .limit(1)
            )
        ).scalar_one_or_none()

    if row is None:
        return None
    payload = row.risk_output or {}
    pack = build_weekend_review_operator_pack(payload, include_full_report=include_full_report)
    pack["agent_analysis_id"] = int(row.id)
    pack["analyzed_at"] = row.analyzed_at.isoformat() if row.analyzed_at else None
    return json_safe(pack)


def format_weekend_review_operator_text(view: dict[str, Any]) -> str:
    """Format a compact read-only operator summary."""
    _assert_review_only(view)
    headline = view.get("headline") if isinstance(view.get("headline"), dict) else {}
    sections = view.get("sections") if isinstance(view.get("sections"), dict) else {}
    blockers = (sections.get("blocker_distribution") or {}).get("blocker_distribution") or {}
    degradation = (sections.get("decision_degradation") or {}).get("metrics") or {}
    safety = sections.get("safety_invariants") or {}
    hedge = (sections.get("hedge_review") or {}).get("metrics") or {}
    labels = (sections.get("label_maturity") or {}).get("metrics") or {}
    debate = (sections.get("debate_value") or {}).get("rates") or {}
    style_opportunity = sections.get("style_opportunity") or {}
    style_metrics = style_opportunity.get("metrics") or {}
    decision_funnel = sections.get("decision_funnel") or {}
    scorecard_acceptance = (decision_funnel.get("scorecard_semantic_acceptance") or {}).get("acceptance") or {}
    cash_drift = decision_funnel.get("cash_drift_attribution") or {}
    strategy_evidence_monitor = decision_funnel.get("strategy_execution_evidence_monitor") or {}
    self_assessment = (sections.get("prior_review_self_assessment") or {}).get("metrics") or {}
    recommendations = view.get("recommendations") if isinstance(view.get("recommendations"), list) else []
    recommendation_lines = [
        f"- review-only: {item.get('text')}"
        for item in recommendations[:5]
    ] or ["- review-only: no follow-up recommendations in summary"]
    return "\n".join([
        "Weekend Review Operator View",
        f"Week: {view.get('week_start')} -> {view.get('week_end')}",
        "execution_authority=none | target_weight_mutation=none",
        (
            "Decision degradation: "
            f"normal={degradation.get('normal_sample_count', 0)} "
            f"degraded={degradation.get('degraded_sample_count', 0)}"
        ),
        (
            "Safety invariants: "
            f"findings={int(safety.get('finding_count') or 0)} "
            f"fail_safe_required={bool(safety.get('fail_safe_required'))}"
        ),
        (
            "Execution truth: "
            f"sent={headline.get('commands_sent', 0)} "
            f"filled={headline.get('filled_count', 0)} "
            f"noop={headline.get('noop_count', 0)} "
            f"stuck={headline.get('stuck_in_flight_count', 0)}"
        ),
        (
            "Execution outcomes: "
            f"qc_reject={headline.get('true_qc_rejected_count', 0)} "
            f"preflight={headline.get('preflight_blocked_count', 0)} "
            f"not_sent={headline.get('not_sent_count', 0)} "
            f"timeout_ack={headline.get('timeout_no_ack_count', 0)} "
            f"no_exec={headline.get('timeout_no_execution_confirmed_count', 0)} "
            f"dedupe={headline.get('duplicate_target_count', 0)}"
        ),
        f"Top blocker: {headline.get('top_blocker') or 'none'}",
        f"Blocker distribution: {blockers}",
        (
            "Labels: "
            f"eligible={labels.get('eligible_label_count', 0)} "
            f"immature_excluded={labels.get('excluded_immature_count', 0)} "
            f"fallback={labels.get('fallback_label_count', 0)}"
        ),
        (
            "Hedge: "
            f"false_negative={hedge.get('false_negative_count', 0)} "
            f"triggered_no_drop={hedge.get('triggered_no_drop_count', 0)} "
            f"would_hurt={hedge.get('hedge_would_have_hurt_count', 0)}"
        ),
        f"Debate change rate: {_rate_label(debate.get('debate_change_rate'))}",
        (
            "Scorecard semantics: "
            f"limited_small_add="
            f"{(scorecard_acceptance.get('limited_data_quality_human_required_small_add') or {}).get('status', 'unknown')} "
            f"strategy_advisory_block="
            f"{(scorecard_acceptance.get('strategy_advisory_only_scorecard_block') or {}).get('status', 'unknown')}"
        ),
        (
            "Cash drift attribution: "
            f"actual={cash_drift.get('actual_cash_delta_sum', 0)} "
            f"bucket_total={cash_drift.get('bucket_total_sum', 0)} "
            f"residual={cash_drift.get('residual_sum', 0)}"
        ),
        (
            "Strategy evidence flips: "
            f"7d={int(strategy_evidence_monitor.get('total_flip_count_7d') or 0)} "
            f"alerts={len(strategy_evidence_monitor.get('alert_strategies') or [])}"
        ),
        (
            "Strategy evidence readiness: "
            f"strategies={int(strategy_evidence_monitor.get('readiness_strategy_count') or 0)} "
            f"insufficient={int(strategy_evidence_monitor.get('insufficient_execution_evidence_strategy_count') or 0)} "
            f"live_min_failed={int(strategy_evidence_monitor.get('live_samples_min_failed_strategy_count') or 0)} "
            f"live0={int(strategy_evidence_monitor.get('zero_live_sample_strategy_count') or 0)} "
            f"stalled={int(strategy_evidence_monitor.get('live_sample_stalled_strategy_count') or 0)} "
            f"closest={_strategy_evidence_closest_label(strategy_evidence_monitor)}"
        ),
        (
            "Blocked-buy counterfactual: "
            f"1d={style_metrics.get('blocked_buy_outperformed_benchmark_count_1d', 0)}/"
            f"{style_metrics.get('blocked_buy_mature_count_1d', 0)} "
            f"5d={style_metrics.get('blocked_buy_outperformed_benchmark_count_5d', 0)}/"
            f"{style_metrics.get('blocked_buy_mature_count_5d', 0)} "
            f"20d={style_metrics.get('blocked_buy_outperformed_benchmark_count_20d', 0)}/"
            f"{style_metrics.get('blocked_buy_mature_count_20d', 0)} "
            "review_only=true"
        ),
        (
            "Prior review: "
            f"mature={self_assessment.get('prior_recommendation_mature_count', 0)} "
            f"supported={self_assessment.get('prior_recommendation_supported_count', 0)} "
            f"contradicted={self_assessment.get('prior_recommendation_contradicted_count', 0)}"
        ),
        "Review-only recommendations:",
        *recommendation_lines,
    ])


def _strategy_evidence_closest_label(monitor: dict[str, Any]) -> str:
    rows = monitor.get("closest_to_execution_grade") if isinstance(monitor.get("closest_to_execution_grade"), list) else []
    if not rows:
        return "none"
    row = rows[0] if isinstance(rows[0], dict) else {}
    name = row.get("strategy_name") or "unknown"
    live_actual = row.get("live_samples_actual")
    live_threshold = row.get("live_samples_threshold")
    failed = row.get("failed_check_count")
    if live_actual is not None and live_threshold is not None:
        return f"{name}(live={live_actual}/{live_threshold}, failed={failed})"
    return f"{name}(failed={failed})"


def build_weekend_review_acceptance_answers(view: dict[str, Any]) -> list[dict[str, Any]]:
    """Map the plan's acceptance questions to deterministic metric refs."""
    _assert_review_only(view)
    sections = view.get("sections") if isinstance(view.get("sections"), dict) else {}
    execution = sections.get("execution_truth") if isinstance(sections.get("execution_truth"), dict) else {}
    degradation = sections.get("decision_degradation") if isinstance(sections.get("decision_degradation"), dict) else {}
    safety = sections.get("safety_invariants") if isinstance(sections.get("safety_invariants"), dict) else {}
    blockers = sections.get("blocker_distribution") if isinstance(sections.get("blocker_distribution"), dict) else {}
    labels = sections.get("label_maturity") if isinstance(sections.get("label_maturity"), dict) else {}
    hedge = sections.get("hedge_review") if isinstance(sections.get("hedge_review"), dict) else {}
    debate = sections.get("debate_value") if isinstance(sections.get("debate_value"), dict) else {}
    basket = sections.get("basket_portfolio") if isinstance(sections.get("basket_portfolio"), dict) else {}
    self_assessment = sections.get("prior_review_self_assessment") if isinstance(sections.get("prior_review_self_assessment"), dict) else {}
    return json_safe([
        _answer(
            1,
            "What did the system try to do this week?",
            "intent_execution.metrics + unexecuted_intents",
            blockers.get("metrics"),
        ),
        _answer(
            2,
            "What did it actually send to QC?",
            "execution_truth.metrics.commands_sent",
            (execution.get("metrics") or {}).get("commands_sent"),
        ),
        _answer(
            3,
            "What did QC actually execute?",
            "execution_truth.metrics filled/noop/partial/rejected",
            execution.get("metrics"),
        ),
        _answer(
            4,
            "Why did approved targets fail to execute?",
            "intent_execution.blocker_distribution",
            blockers.get("blocker_distribution"),
        ),
        _answer(
            5,
            "Which blockers dominated?",
            "intent_execution.blocker_distribution",
            blockers.get("blocker_distribution"),
        ),
        _answer(
            6,
            "Which labels are mature enough to evaluate?",
            "label_maturity.metrics",
            labels.get("metrics"),
        ),
        _answer(
            7,
            "Were hedge thresholds too conservative or too aggressive?",
            "hedge_review.metrics + hedge_review.rates",
            {
                "metrics": hedge.get("metrics") or {},
                "rates": hedge.get("rates") or {},
            },
        ),
        _answer(
            8,
            "Did bull/bear debate materially change outcomes?",
            "debate_value.metrics + debate_value.rates",
            {
                "metrics": debate.get("metrics") or {},
                "rates": debate.get("rates") or {},
            },
        ),
        _answer(
            9,
            "Did active basket constraints reduce noise without hiding risk?",
            "basket_portfolio.metrics",
            basket.get("metrics"),
        ),
        _answer(
            10,
            "Did last week's review recommendations age well?",
            "prior_review_self_assessment.metrics + rates",
            {
                "metrics": self_assessment.get("metrics") or {},
                "rates": self_assessment.get("rates") or {},
            },
        ),
        _answer(
            11,
            "Were decision samples separated by degraded versus normal mode?",
            "decision_degradation.metrics + intent_execution.decision_degradation_split",
            {
                "metrics": degradation.get("metrics") or {},
                "intent_split_available": "decision_degradation_split" in blockers,
            },
        ),
        _answer(
            12,
            "Did config fail-safe scans find safety invariant violations?",
            "safety_invariants.findings",
            {
                "finding_count": safety.get("finding_count"),
                "fail_safe_required": safety.get("fail_safe_required"),
            },
        ),
    ])


def _assert_review_only(payload: dict[str, Any]) -> None:
    if payload.get("execution_authority") != EXECUTION_AUTHORITY:
        raise ValueError("weekend review operator view requires execution_authority=none")
    if payload.get("target_weight_mutation") != TARGET_WEIGHT_MUTATION:
        raise ValueError("weekend review operator view requires target_weight_mutation=none")


def _answer(
    idx: int,
    question: str,
    deterministic_source: str,
    payload: Any,
) -> dict[str, Any]:
    has_payload = payload is not None and payload != {} and payload != []
    return {
        "id": idx,
        "question": question,
        "status": "available" if has_payload else "no_data",
        "deterministic_source": deterministic_source,
        "llm_computed": False,
        "execution_authority": EXECUTION_AUTHORITY,
        "answer_payload": payload if has_payload else None,
    }


def _section(sections: dict[str, Any], name: str) -> dict[str, Any]:
    section = sections.get(name)
    return section if isinstance(section, dict) else {}


def _metric(section: dict[str, Any], name: str) -> int | float:
    metrics = section.get("metrics")
    if not isinstance(metrics, dict):
        return 0
    value = metrics.get(name, 0)
    return value if isinstance(value, (int, float)) else 0


def _rate_status(section: dict[str, Any], name: str) -> str:
    rates = section.get("rates")
    if not isinstance(rates, dict):
        return "missing"
    rate = rates.get(name)
    if isinstance(rate, dict):
        return str(rate.get("status") or "unknown")
    return "missing"


def _top_item(values: dict[str, Any]) -> str | None:
    if not values:
        return None
    key, value = max(values.items(), key=lambda item: _numeric(item[1]))
    return f"{key}:{value}"


def _numeric(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _review_only_recommendations(summary: dict[str, Any]) -> list[dict[str, Any]]:
    text = str(summary.get("summary_text") or "")
    recommendations: list[dict[str, Any]] = []
    for line in text.splitlines():
        clean = line.strip(" -\t")
        if not clean:
            continue
        lower = clean.lower()
        if "review-only" in lower or "operator review" in lower or "inspect" in lower:
            recommendations.append({
                "text": clean,
                "label": "review-only",
                "execution_authority": EXECUTION_AUTHORITY,
            })
    return recommendations


def _rate_label(rate: Any) -> str:
    if not isinstance(rate, dict):
        return "missing"
    status = str(rate.get("status") or "unknown")
    if status != "ok":
        sample_n = rate.get("sample_n")
        min_sample = rate.get("min_sample_n")
        return f"{status} (n={sample_n}, min={min_sample})"
    value = rate.get("value")
    if isinstance(value, (int, float)):
        return f"{value:.2%}"
    return status
