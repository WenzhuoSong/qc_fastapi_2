"""Deterministic weekend trading review metrics.

PR1 intentionally stays pure: it consumes the authority-gated
``WeekendReviewDataset`` from ``weekend_review_loader`` and returns JSON-safe
metrics. There are no LLM calls and no execution imports here.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any

from services.json_safety import json_safe
from services.weekend_review_loader import (
    EXECUTION_AUTHORITY,
    TARGET_WEIGHT_MUTATION,
    WeekendReviewDataset,
)


METRICS_CONTRACT_VERSION = "weekend_review_metrics_v1"
DEFAULT_HEDGE_DRAWDOWN_THRESHOLD = -0.03
DEFAULT_HEDGE_HORIZON_DAYS = 5
DEFAULT_IN_FLIGHT_TIMEOUT_MINUTES = 30
DEFAULT_HEDGE_WEIGHT_POLICY_VERSION = "hedge_weight_policy_v1"

IN_FLIGHT_STATES = {
    "pending_ack",
    "orders_submitted",
    "pending_reconcile",
    "partial",
}

TERMINAL_FILLED_STATES = {
    "filled",
    "reconciled",
    "noop_reconciled",
}

REJECTED_STATES = {
    "rejected",
    "failed",
}

PREFLIGHT_BLOCKER_CATEGORY = {
    "daily_command_count_ok": "execution_daily_cap",
    "daily_gross_turnover_ok": "execution_turnover_cap",
}

EVENT_STATE_PRIORITY: dict[str, tuple[str, int]] = {
    "reconciliation_drift": ("diverged", 100),
    "reconciled": ("reconciled", 95),
    "filled": ("filled", 90),
    "failed_no_fill": ("failed_no_fill", 85),
    "qc_rejected": ("rejected", 85),
    "timeout_reconciled_no_execution": ("timeout_no_execution_confirmed", 82),
    "partial": ("partial", 75),
    "orders_submitted": ("orders_submitted", 65),
    "qc_accepted": ("accepted", 55),
    "submitted_to_qc": ("accepted", 45),
    "qc_timeout": ("timeout_no_ack", 40),
    "preflight_blocked": ("not_sent", 35),
}


@dataclass(frozen=True)
class RateGuardConfig:
    execution_truth: int = 1
    blocker_distribution: int = 1
    hedge_outcome: int = 20
    debate_outcome: int = 20
    regime_risk: int = 30
    basket_outcome: int = 20
    style_opportunity: int = 20
    decision_funnel: int = 20
    weekly_self_assessment: int = 5


def build_weekly_review_metrics(
    dataset: WeekendReviewDataset | dict[str, Any],
    *,
    review_as_of: datetime | None = None,
    rate_guard: RateGuardConfig | None = None,
    hedge_drawdown_threshold: float = DEFAULT_HEDGE_DRAWDOWN_THRESHOLD,
    hedge_horizon_days: int = DEFAULT_HEDGE_HORIZON_DAYS,
    hedge_weight_policy_version: str = DEFAULT_HEDGE_WEIGHT_POLICY_VERSION,
) -> dict[str, Any]:
    """Build deterministic weekly review metrics from loader-approved inputs."""
    data = _dataset_dict(dataset)
    guard = rate_guard or RateGuardConfig()
    as_of = _ensure_utc(review_as_of or datetime.now(timezone.utc))
    sections = {
        "decision_degradation": build_decision_degradation_metrics(data),
        "execution_truth": build_execution_truth_metrics(
            data,
            review_as_of=as_of,
            in_flight_timeout_minutes=DEFAULT_IN_FLIGHT_TIMEOUT_MINUTES,
        ),
        "intent_execution": build_intent_execution_metrics(data),
        "label_maturity": build_label_maturity_metrics(data, review_as_of=as_of),
        "hedge_review": build_hedge_review_metrics(
            data,
            hedge_drawdown_threshold=hedge_drawdown_threshold,
            hedge_horizon_days=hedge_horizon_days,
            hedge_weight_policy_version=hedge_weight_policy_version,
            min_sample_n=guard.hedge_outcome,
        ),
        "debate_impact": build_debate_impact_metrics(data, min_sample_n=guard.debate_outcome),
        "basket_portfolio": build_basket_portfolio_metrics(data),
        "regime_risk": build_regime_risk_metrics(data, min_sample_n=guard.regime_risk),
        "style_opportunity": build_style_opportunity_metrics(
            data,
            min_sample_n=guard.style_opportunity,
        ),
        "decision_funnel": build_decision_funnel_metrics(
            data,
            min_sample_n=guard.decision_funnel,
        ),
        "weekly_self_assessment": build_weekly_self_assessment_metrics(
            data,
            min_sample_n=guard.weekly_self_assessment,
        ),
    }
    return json_safe({
        "contract_version": METRICS_CONTRACT_VERSION,
        "execution_authority": EXECUTION_AUTHORITY,
        "target_weight_mutation": TARGET_WEIGHT_MUTATION,
        "review_as_of": as_of.isoformat(),
        "source_counts": data.get("source_counts") or {},
        "excluded_input_count": len(data.get("excluded_inputs") or []),
        "exclusion_counts": data.get("exclusion_counts") or {},
        "sections": sections,
    })


def build_execution_truth_metrics(
    dataset: WeekendReviewDataset | dict[str, Any],
    *,
    review_as_of: datetime | None = None,
    in_flight_timeout_minutes: int = DEFAULT_IN_FLIGHT_TIMEOUT_MINUTES,
) -> dict[str, Any]:
    data = _dataset_dict(dataset)
    as_of = _ensure_utc(review_as_of or datetime.now(timezone.utc))
    logs = list(data.get("execution_logs") or [])
    lifecycle_events = list(data.get("command_lifecycle_events") or [])
    event_summary = _command_lifecycle_event_summary(lifecycle_events)
    observations = [
        row for row in data.get("validation_observations") or []
        if row.get("observation_type") == "execution_truth"
    ]
    rows = _merge_execution_rows_with_lifecycle_events(_dedupe_command_rows(logs), event_summary)
    counts = {
        "commands_sent": 0,
        "accepted_count": 0,
        "filled_count": 0,
        "noop_count": 0,
        "partial_count": 0,
        "rejected_count": 0,
        "true_qc_rejected_count": 0,
        "preflight_blocked_count": 0,
        "not_sent_count": 0,
        "timeout_no_ack_count": 0,
        "timeout_no_execution_confirmed_count": 0,
        "failed_no_fill_count": 0,
        "duplicate_target_count": 0,
        "reconciliation_divergence_count": 0,
        "stuck_in_flight_count": 0,
    }
    week_buckets: dict[str, int] = {}
    evidence_refs: list[dict[str, Any]] = []

    for row in rows:
        state = _execution_state(row)
        event_types = set(_event_types(row))
        event_statuses = set(_event_statuses(row))
        command_id = str(row.get("command_id") or "")
        if state not in {"deduped", "duplicate_target", "not_sent"}:
            counts["commands_sent"] += 1
        if state in {"accepted", "ownership_accepted"} or _bool_path(row, "qc_response", "accepted"):
            counts["accepted_count"] += 1
        if state in TERMINAL_FILLED_STATES:
            counts["filled_count"] += 1
        if _is_noop(row):
            counts["noop_count"] += 1
        if state == "partial":
            counts["partial_count"] += 1
        if state in REJECTED_STATES or "qc_rejected" in event_types:
            counts["rejected_count"] += 1
        if state == "rejected" or "qc_rejected" in event_types:
            counts["true_qc_rejected_count"] += 1
        if "preflight_blocked" in event_types:
            counts["preflight_blocked_count"] += 1
        if state == "not_sent" or "preflight_blocked" in event_types:
            counts["not_sent_count"] += 1
        if state == "timeout_no_ack" or "qc_timeout" in event_types or "timeout_no_ack" in event_statuses:
            counts["timeout_no_ack_count"] += 1
        if state == "timeout_no_execution_confirmed" or "timeout_reconciled_no_execution" in event_types:
            counts["timeout_no_execution_confirmed_count"] += 1
        if state == "failed_no_fill" or "failed_no_fill" in event_types:
            counts["failed_no_fill_count"] += 1
        if _is_deduped_execution_row(row):
            counts["duplicate_target_count"] += 1
        if state == "diverged" or _contains_any(row, {"reconciliation_divergence", "diverged"}):
            counts["reconciliation_divergence_count"] += 1
        if state in IN_FLIGHT_STATES and _is_stuck_in_flight(row, as_of, in_flight_timeout_minutes):
            counts["stuck_in_flight_count"] += 1

        bucket = _week_bucket(row)
        if bucket:
            week_buckets[bucket] = week_buckets.get(bucket, 0) + 1
        evidence_refs.append({
            "command_id": command_id,
            "state": state,
            "event_state": row.get("event_state"),
            "event_types": row.get("event_types") or [],
            "event_statuses": row.get("event_statuses") or [],
            "week_bucket": bucket,
        })

    for obs in observations:
        outcome = obs.get("outcome_payload") if isinstance(obs.get("outcome_payload"), dict) else {}
        if bool(outcome.get("is_noop")):
            counts["noop_count"] = max(counts["noop_count"], 1)
        if str(outcome.get("qc_status") or "").lower() == "partial":
            counts["partial_count"] += 1

    return _section({
        "schema_version": "weekly_execution_truth_review_v1",
        "metrics": counts,
        "week_buckets": dict(sorted(week_buckets.items())),
        "evidence_refs": evidence_refs,
    })


def build_intent_execution_metrics(dataset: WeekendReviewDataset | dict[str, Any]) -> dict[str, Any]:
    data = _dataset_dict(dataset)
    observations = [
        row for row in data.get("validation_observations") or []
        if row.get("observation_type") == "intent_vs_execution"
    ]
    metrics = {
        "risk_block_count": 0,
        "final_validation_block_count": 0,
        "execution_preflight_block_count": 0,
        "daily_command_cap_block_count": 0,
        "daily_turnover_cap_block_count": 0,
        "dedupe_count": 0,
        "execution_timeout_count": 0,
        "qc_reject_count": 0,
        "approved_not_sent_count": 0,
        "hedge_triggered_not_added_count": 0,
    }
    blocker_distribution: dict[str, int] = {}
    degradation_split = _empty_intent_degradation_split()
    unexecuted_intents: list[dict[str, Any]] = []

    for row in observations:
        degraded_bucket = "degraded" if _is_degraded_observation(row) else "normal"
        degradation_split[degraded_bucket]["sample_count"] += 1
        payload = row.get("observation_payload") if isinstance(row.get("observation_payload"), dict) else {}
        outcome = row.get("outcome_payload") if isinstance(row.get("outcome_payload"), dict) else {}
        events = payload.get("blocker_events") if isinstance(payload.get("blocker_events"), list) else []
        blockers = {str(item) for item in payload.get("blockers") or []}
        categories = _blocker_categories(events)
        for category, count in categories.items():
            blocker_distribution[category] = blocker_distribution.get(category, 0) + count
            split_dist = degradation_split[degraded_bucket]["blocker_distribution"]
            split_dist[category] = split_dist.get(category, 0) + count

        if any(category in categories for category in ("risk_validation", "risk_manager")) or _any_contains(blockers, "risk"):
            metrics["risk_block_count"] += 1
            degradation_split[degraded_bucket]["metrics"]["risk_block_count"] += 1
        if "final_validation" in categories or _any_contains(blockers, "final_validation"):
            metrics["final_validation_block_count"] += 1
            degradation_split[degraded_bucket]["metrics"]["final_validation_block_count"] += 1
        if any(category.startswith("execution_") for category in categories) or _any_contains(blockers, "preflight"):
            metrics["execution_preflight_block_count"] += 1
            degradation_split[degraded_bucket]["metrics"]["execution_preflight_block_count"] += 1
        if "execution_daily_cap" in categories or "daily_command_count_ok" in blockers:
            metrics["daily_command_cap_block_count"] += 1
            degradation_split[degraded_bucket]["metrics"]["daily_command_cap_block_count"] += 1
        if "execution_turnover_cap" in categories or "daily_gross_turnover_ok" in blockers:
            metrics["daily_turnover_cap_block_count"] += 1
            degradation_split[degraded_bucket]["metrics"]["daily_turnover_cap_block_count"] += 1

        not_sent_reason = str(outcome.get("not_sent_reason") or "").lower()
        row_unexecuted = [str(item) for item in payload.get("unexecuted_intents") or []]
        if "dedupe" in not_sent_reason or any("dedupe" in item for item in row_unexecuted):
            metrics["dedupe_count"] += 1
            degradation_split[degraded_bucket]["metrics"]["dedupe_count"] += 1
        if bool(payload.get("risk_approved")) and not bool(outcome.get("command_sent")):
            metrics["approved_not_sent_count"] += 1
            degradation_split[degraded_bucket]["metrics"]["approved_not_sent_count"] += 1
        hedge = payload.get("hedge_intent") if isinstance(payload.get("hedge_intent"), dict) else {}
        if bool(hedge.get("triggered")) and not bool(hedge.get("add_hedge_etf")):
            metrics["hedge_triggered_not_added_count"] += 1
            degradation_split[degraded_bucket]["metrics"]["hedge_triggered_not_added_count"] += 1

        for item in row_unexecuted:
            unexecuted_intents.append({
                "observation_id": row.get("observation_id"),
                "intent": item,
            })

    observed_commands = {
        str(row.get("command_id") or "").strip()
        for row in observations
        if str(row.get("command_id") or "").strip()
    }
    _apply_lifecycle_event_intent_fallback(
        data.get("command_lifecycle_events") or [],
        observed_commands=observed_commands,
        metrics=metrics,
        blocker_distribution=blocker_distribution,
        unexecuted_intents=unexecuted_intents,
    )

    return _section({
        "schema_version": "weekly_intent_execution_review_v1",
        "metrics": metrics,
        "blocker_distribution": dict(sorted(blocker_distribution.items())),
        "decision_degradation_split": _sorted_degradation_split(degradation_split),
        "unexecuted_intents": unexecuted_intents,
    })


def build_decision_degradation_metrics(dataset: WeekendReviewDataset | dict[str, Any]) -> dict[str, Any]:
    data = _dataset_dict(dataset)
    metrics = {
        "sample_count": 0,
        "normal_sample_count": 0,
        "degraded_sample_count": 0,
        "degraded_intent_execution_count": 0,
    }
    mode_distribution: dict[str, int] = {}
    fallback_distribution: dict[str, int] = {}
    missing_input_distribution: dict[str, int] = {}
    by_observation_type: dict[str, dict[str, int]] = {}

    rows = [
        *list(data.get("validation_observations") or []),
        *list(data.get("diagnostic_artifacts") or []),
    ]
    for row in rows:
        metrics["sample_count"] += 1
        observation_type = str(row.get("observation_type") or row.get("artifact_type") or "unknown")
        by_type = by_observation_type.setdefault(observation_type, {"normal": 0, "degraded": 0})
        degradation = _decision_degradation_payload(row)
        if bool(degradation.get("is_degraded")):
            metrics["degraded_sample_count"] += 1
            by_type["degraded"] += 1
            if observation_type == "intent_vs_execution":
                metrics["degraded_intent_execution_count"] += 1
            for mode in degradation.get("degraded_modes") or []:
                _count_distribution(mode_distribution, str(mode))
            for path in degradation.get("fallback_paths") or []:
                _count_distribution(fallback_distribution, str(path))
            for missing in degradation.get("missing_inputs") or []:
                _count_distribution(missing_input_distribution, str(missing))
        else:
            metrics["normal_sample_count"] += 1
            by_type["normal"] += 1

    return _section({
        "schema_version": "weekly_decision_degradation_review_v1",
        "metrics": metrics,
        "mode_distribution": dict(sorted(mode_distribution.items())),
        "fallback_distribution": dict(sorted(fallback_distribution.items())),
        "missing_input_distribution": dict(sorted(missing_input_distribution.items())),
        "by_observation_type": {
            key: by_observation_type[key]
            for key in sorted(by_observation_type)
        },
        "evaluation_guidance": "stratify_strategy_metrics_by_decision_degraded_before_drawing_edge_conclusions",
    })


def build_label_maturity_metrics(
    dataset: WeekendReviewDataset | dict[str, Any],
    *,
    review_as_of: datetime | None = None,
) -> dict[str, Any]:
    data = _dataset_dict(dataset)
    as_of_date = _ensure_utc(review_as_of or datetime.now(timezone.utc)).date()
    metrics = {
        "label_1d_mature_count": 0,
        "label_5d_mature_count": 0,
        "label_20d_mature_count": 0,
        "label_1d_pending_count": 0,
        "label_5d_pending_count": 0,
        "label_20d_pending_count": 0,
        "eligible_label_count": len(data.get("outcome_labels") or []),
        "fallback_label_count": int(data.get("fallback_label_count") or 0),
        "excluded_immature_count": 0,
    }
    for label in data.get("outcome_labels") or []:
        horizon = _horizon_key(label.get("horizon"))
        if horizon:
            metrics[f"label_{horizon}_mature_count"] += 1

    for row in data.get("validation_observations") or []:
        horizon = _horizon_from_days(row.get("horizon_days"))
        if not horizon:
            continue
        status = str(row.get("status") or "").lower()
        maturity = _parse_date(row.get("maturity_date"))
        if status in {"completed", "matured"} or row.get("outcome_payload"):
            metrics[f"label_{horizon}_mature_count"] += 1
        elif maturity is None or maturity > as_of_date:
            metrics[f"label_{horizon}_pending_count"] += 1
            metrics["excluded_immature_count"] += 1
        else:
            metrics[f"label_{horizon}_pending_count"] += 1
            metrics["excluded_immature_count"] += 1

    return _section({
        "schema_version": "weekly_label_maturity_review_v1",
        "metrics": metrics,
        "hard_rule": "immature_labels_counted_and_excluded",
    })


def build_hedge_review_metrics(
    dataset: WeekendReviewDataset | dict[str, Any],
    *,
    hedge_drawdown_threshold: float = DEFAULT_HEDGE_DRAWDOWN_THRESHOLD,
    hedge_horizon_days: int = DEFAULT_HEDGE_HORIZON_DAYS,
    hedge_weight_policy_version: str = DEFAULT_HEDGE_WEIGHT_POLICY_VERSION,
    min_sample_n: int = 20,
) -> dict[str, Any]:
    data = _dataset_dict(dataset)
    feature_rows = list(data.get("market_features") or [])
    samples = _hedge_samples(data)
    metrics = _empty_hedge_metrics()
    split = _empty_metric_split(_empty_hedge_metrics)
    counterfactuals: list[dict[str, Any]] = []

    for sample in samples:
        bucket = "degraded" if bool(sample.get("decision_degraded")) else "normal"
        split[bucket]["sample_count"] += 1
        metric_targets = [metrics, split[bucket]["metrics"]]
        triggered = bool(sample.get("triggered"))
        added = bool(sample.get("add_hedge_etf"))
        if triggered:
            _increment_metric_targets(metric_targets, "hedge_trigger_count")
        if added:
            _increment_metric_targets(metric_targets, "hedge_added_count")
        if triggered and not added:
            _increment_metric_targets(metric_targets, "triggered_not_added_count")

        market_return = _market_forward_return(
            feature_rows,
            observation_date=sample.get("observation_date"),
            horizon_days=hedge_horizon_days,
        )
        if market_return is None:
            _increment_metric_targets(metric_targets, "insufficient_market_outcome_count")
        else:
            dropped = market_return <= hedge_drawdown_threshold
            if not triggered and dropped:
                _increment_metric_targets(metric_targets, "false_negative_count")
            if triggered and not dropped:
                _increment_metric_targets(metric_targets, "triggered_no_drop_count")
            if not added and dropped:
                _increment_metric_targets(metric_targets, "missed_protection_count")

        cf = hedge_counterfactual_return(
            candidate_hedge_instrument=sample.get("candidate_hedge_instrument") or sample.get("selected_instrument"),
            severity=sample.get("severity"),
            decision_date=sample.get("observation_date"),
            feature_rows=feature_rows,
            horizon_days=hedge_horizon_days,
            policy_version=hedge_weight_policy_version,
        )
        counterfactuals.append(cf)
        if cf["status"] != "ok":
            _increment_metric_targets(metric_targets, "insufficient_counterfactual_count")
            continue
        contribution = float(cf["hedge_contribution"])
        if contribution > 0.0:
            _increment_metric_targets(metric_targets, "hedge_would_have_helped_count")
        elif contribution < 0.0:
            _increment_metric_targets(metric_targets, "hedge_would_have_hurt_count")
            if triggered:
                _increment_metric_targets(metric_targets, "triggered_hedge_would_hurt_count")

    return _section({
        "schema_version": "weekly_hedge_review_v1",
        "metrics": metrics,
        "rates": _hedge_rates(metrics, sample_count=len(samples), min_sample_n=min_sample_n),
        "decision_degradation_split": _finalize_metric_split(
            split,
            rate_builder=lambda item: _hedge_rates(
                item["metrics"],
                sample_count=item["sample_count"],
                min_sample_n=min_sample_n,
            ),
        ),
        "counterfactuals": counterfactuals,
        "hedge_drawdown_threshold": hedge_drawdown_threshold,
        "hedge_horizon_days": hedge_horizon_days,
        "counterfactual_contract": {
            "uses_real_candidate_etf_price_path": True,
            "does_not_use_negative_underlying_approximation": True,
            "policy_version": hedge_weight_policy_version,
        },
    })


def build_debate_impact_metrics(
    dataset: WeekendReviewDataset | dict[str, Any],
    *,
    min_sample_n: int = 20,
) -> dict[str, Any]:
    data = _dataset_dict(dataset)
    artifacts = [
        row for row in data.get("diagnostic_artifacts") or []
        if row.get("schema_version") == "debate_impact_v1"
    ]
    changed = 0
    total_disagreements = 0
    failures = 0
    changed_ticker_samples = 0
    changed_ticker_wins = 0
    split = _empty_debate_split()

    for row in artifacts:
        bucket = "degraded" if _is_degraded_record(row) else "normal"
        split[bucket]["sample_count"] += 1
        split[bucket]["metrics"]["debate_available_count"] += 1
        total_disagreements += _int(row.get("disagreement_count"), default=0)
        split[bucket]["metrics"]["disagreement_count_total"] += _int(row.get("disagreement_count"), default=0)
        changed_tickers = row.get("disagreement_tickers_changed_by_target_builder") or []
        final_tickers = row.get("disagreement_tickers_in_final_target") or []
        if changed_tickers or final_tickers or _int(row.get("arbitration_count"), default=0) > 0:
            changed += 1
            split[bucket]["metrics"]["debate_changed_target_count"] += 1
        if row.get("bull_failed") or row.get("bear_failed") or row.get("cross_exam_failed"):
            failures += 1
            split[bucket]["metrics"]["debate_failure_count"] += 1
        outcome = row.get("outcome_evaluation") if isinstance(row.get("outcome_evaluation"), dict) else {}
        if outcome.get("mature") is True:
            changed_ticker_samples += 1
            split[bucket]["changed_ticker_samples"] += 1
            if outcome.get("win") is True:
                changed_ticker_wins += 1
                split[bucket]["changed_ticker_wins"] += 1

    return _section({
        "schema_version": "weekly_debate_impact_review_v1",
        "metrics": {
            "debate_available_count": len(artifacts),
            "disagreement_count_total": total_disagreements,
            "debate_changed_target_count": changed,
            "debate_failure_count": failures,
        },
        "rates": {
            "debate_change_rate": rate_metric(
                "debate_change_rate",
                numerator=changed,
                denominator=len(artifacts),
                min_sample_n=1,
            ),
            "changed_ticker_outcome_win_rate": rate_metric(
                "changed_ticker_outcome_win_rate",
                numerator=changed_ticker_wins,
                denominator=changed_ticker_samples,
                min_sample_n=min_sample_n,
            ),
        },
        "decision_degradation_split": _finalize_debate_split(split, min_sample_n=min_sample_n),
    })


def build_basket_portfolio_metrics(dataset: WeekendReviewDataset | dict[str, Any]) -> dict[str, Any]:
    data = _dataset_dict(dataset)
    active_counts: list[float] = []
    cash_values: list[float] = []
    effective_ns: list[float] = []
    out_of_range = 0
    subscale = 0
    floor_cleared = 0
    split = _empty_basket_split()

    for row in data.get("validation_observations") or []:
        if row.get("observation_type") != "active_basket":
            continue
        bucket = "degraded" if _is_degraded_record(row) else "normal"
        split[bucket]["sample_count"] += 1
        metrics = row.get("metrics") if isinstance(row.get("metrics"), dict) else {}
        payload = row.get("observation_payload") if isinstance(row.get("observation_payload"), dict) else {}
        policy = payload.get("active_basket_policy") if isinstance(payload.get("active_basket_policy"), dict) else {}
        active = _float(metrics.get("active_count", policy.get("active_count")))
        if active is not None:
            active_counts.append(active)
            split[bucket]["active_counts"].append(active)
        if policy.get("within_target_active_count") is False:
            out_of_range += 1
            split[bucket]["active_count_out_of_range_count"] += 1
        subscale += _int(metrics.get("subscale_count", policy.get("subscale_count")), default=0)
        split[bucket]["subscale_position_count"] += _int(metrics.get("subscale_count", policy.get("subscale_count")), default=0)
        floor_cleared += _int(metrics.get("floor_cleared_count", policy.get("floor_cleared_count")), default=0)
        split[bucket]["floor_cleared_count"] += _int(metrics.get("floor_cleared_count", policy.get("floor_cleared_count")), default=0)
        eff = _float(metrics.get("effective_n", policy.get("effective_n")))
        if eff is not None:
            effective_ns.append(eff)
            split[bucket]["effective_ns"].append(eff)

    for artifact in data.get("diagnostic_artifacts") or []:
        if artifact.get("schema_version") != "portfolio_mix_event_v1":
            continue
        bucket = "degraded" if _is_degraded_record(artifact) else "normal"
        split[bucket]["sample_count"] += 1
        active = _float(artifact.get("active_count"))
        if active is not None:
            active_counts.append(active)
            split[bucket]["active_counts"].append(active)
        cash = _float(artifact.get("cash_weight"))
        if cash is not None:
            cash_values.append(cash)
            split[bucket]["cash_values"].append(cash)
        diagnostics = artifact.get("diagnostics") if isinstance(artifact.get("diagnostics"), dict) else {}
        eff = _float(diagnostics.get("effective_n"))
        if eff is not None:
            effective_ns.append(eff)
            split[bucket]["effective_ns"].append(eff)

    return _section({
        "schema_version": "weekly_strategy_basket_review_v1",
        "metrics": {
            "active_count_avg": _avg(active_counts),
            "active_count_out_of_range_count": out_of_range,
            "subscale_position_count": subscale,
            "floor_cleared_count": floor_cleared,
            "cash_avg": _avg(cash_values),
            "effective_n_avg": _avg(effective_ns),
        },
        "decision_degradation_split": _finalize_basket_split(split),
    })


def build_regime_risk_metrics(
    dataset: WeekendReviewDataset | dict[str, Any],
    *,
    min_sample_n: int = 30,
) -> dict[str, Any]:
    data = _dataset_dict(dataset)
    assessments = [
        row for row in data.get("diagnostic_artifacts") or []
        if row.get("schema_version") == "market_risk_assessment_v1"
    ]
    risk_off_calls = sum(
        1 for row in assessments
        if str(row.get("market_regime") or "").lower() in {"risk_off", "defensive"}
        or str(row.get("risk_direction") or "").lower() in {"down", "risk_down", "defensive"}
    )
    split = _empty_regime_split()
    for row in assessments:
        bucket = "degraded" if _is_degraded_record(row) else "normal"
        split[bucket]["sample_count"] += 1
        split[bucket]["metrics"]["market_risk_assessment_count"] += 1
        if (
            str(row.get("market_regime") or "").lower() in {"risk_off", "defensive"}
            or str(row.get("risk_direction") or "").lower() in {"down", "risk_down", "defensive"}
        ):
            split[bucket]["metrics"]["risk_off_call_count"] += 1
    return _section({
        "schema_version": "weekly_regime_risk_review_v1",
        "metrics": {
            "market_risk_assessment_count": len(assessments),
            "risk_off_call_count": risk_off_calls,
            "hard_risk_outcome_count": 0,
        },
        "rates": {
            "risk_off_recall_proxy": {
                **rate_metric(
                    "risk_off_recall_proxy",
                    numerator=0,
                    denominator=0,
                    min_sample_n=min_sample_n,
                ),
                "proxy_caveat": (
                    "denominator is observable negative forward-return windows; "
                    "true should-have-been-risk-off is not directly observable"
                ),
            }
        },
        "decision_degradation_split": _finalize_regime_split(split, min_sample_n=min_sample_n),
    })


def build_style_opportunity_metrics(
    dataset: WeekendReviewDataset | dict[str, Any],
    *,
    horizon_days: int = 5,
    min_sample_n: int = 20,
    market_return_threshold: float = 0.0,
    opportunity_benchmark_ticker: str = "SPY",
) -> dict[str, Any]:
    """Evaluate defensive style calls and blocked-buy opportunity cost separately."""
    data = _dataset_dict(dataset)
    feature_rows = list(data.get("market_features") or [])
    style_events = [
        row for row in data.get("diagnostic_artifacts") or []
        if row.get("schema_version") == "decision_style_event_v1"
    ]
    metrics = {
        "style_event_count": len(style_events),
        "defensive_style_count": 0,
        "defensive_style_market_outcome_count": 0,
        "defensive_style_market_down_count": 0,
        "defensive_style_market_outcome_count_1d": 0,
        "defensive_style_market_down_count_1d": 0,
        "defensive_style_market_outcome_count_5d": 0,
        "defensive_style_market_down_count_5d": 0,
        "blocked_buy_candidate_count": 0,
        "blocked_buy_mature_count": 0,
        "blocked_buy_outperformed_benchmark_count": 0,
        "insufficient_defensive_market_outcome_count": 0,
        "insufficient_defensive_market_outcome_count_1d": 0,
        "insufficient_defensive_market_outcome_count_5d": 0,
        "insufficient_blocked_buy_outcome_count": 0,
    }
    excess_returns: list[float] = []
    split = _empty_style_opportunity_split()

    for row in style_events:
        bucket = "degraded" if _is_degraded_record(row) else "normal"
        split[bucket]["sample_count"] += 1
        split_metrics = split[bucket]["metrics"]
        split_metrics["style_event_count"] += 1
        observation_date = _artifact_observation_date(row)
        if _is_defensive_style_event(row):
            metrics["defensive_style_count"] += 1
            split_metrics["defensive_style_count"] += 1
            for defensive_horizon in _defensive_style_horizons(horizon_days):
                _record_defensive_style_market_outcome(
                    metrics,
                    split_metrics,
                    feature_rows=feature_rows,
                    observation_date=observation_date,
                    horizon_days=defensive_horizon,
                    market_return_threshold=market_return_threshold,
                    primary_horizon_days=horizon_days,
                )
            if horizon_days != 5:
                _record_defensive_style_market_outcome(
                    metrics,
                    split_metrics,
                    feature_rows=feature_rows,
                    observation_date=observation_date,
                    horizon_days=5,
                    market_return_threshold=market_return_threshold,
                    primary_horizon_days=horizon_days,
                    update_primary_aliases=False,
                )

        for ticker in _blocked_new_positions(row):
            metrics["blocked_buy_candidate_count"] += 1
            split_metrics["blocked_buy_candidate_count"] += 1
            ticker_return = _forward_return_from_prices(
                feature_rows,
                ticker=ticker,
                observation_date=observation_date,
                horizon_days=horizon_days,
            )
            benchmark_return = _forward_return_from_prices(
                feature_rows,
                ticker=opportunity_benchmark_ticker,
                observation_date=observation_date,
                horizon_days=horizon_days,
            )
            if ticker_return is None or benchmark_return is None:
                metrics["insufficient_blocked_buy_outcome_count"] += 1
                split_metrics["insufficient_blocked_buy_outcome_count"] += 1
                continue
            excess = ticker_return - benchmark_return
            excess_returns.append(excess)
            split[bucket]["blocked_buy_excess_returns"].append(excess)
            metrics["blocked_buy_mature_count"] += 1
            split_metrics["blocked_buy_mature_count"] += 1
            if excess > 0.0:
                metrics["blocked_buy_outperformed_benchmark_count"] += 1
                split_metrics["blocked_buy_outperformed_benchmark_count"] += 1

    return _section({
        "schema_version": "weekly_style_opportunity_review_v1",
        "metrics": {
            **metrics,
            "blocked_buy_avg_excess_return_vs_benchmark": _avg(excess_returns),
        },
        "rates": _style_opportunity_rates(
            metrics,
            min_sample_n=min_sample_n,
        ),
        "decision_degradation_split": _finalize_style_opportunity_split(
            split,
            min_sample_n=min_sample_n,
        ),
        "horizon_days": horizon_days,
        "market_return_threshold": market_return_threshold,
        "opportunity_benchmark_ticker": opportunity_benchmark_ticker,
        "metric_contract": {
            "defensive_style_hit_rate": (
                "defensive style is evaluated against forward broad-market return, "
                "not against a high-cash actual portfolio"
            ),
            "blocked_buy_opportunity_cost": (
                "blocked new buys are evaluated against a benchmark ticker, "
                "not against cash-heavy actual allocation"
            ),
            "blocked_buy_selection_bias_caveat": (
                "blocked-buy outcomes are measured on candidates selected by the shadow/style path; "
                "the rate reflects shadow candidate quality multiplied by gate opportunity cost, "
                "so it cannot by itself prove the gate is too strict"
            ),
            "defensive_style_horizons": (
                "risk_reduce_fast-style calls are evaluated at both 1d and 5d horizons; "
                "small samples remain insufficient_sample"
            ),
        },
    })


def build_decision_funnel_metrics(
    dataset: WeekendReviewDataset | dict[str, Any],
    *,
    min_sample_n: int = 20,
) -> dict[str, Any]:
    """Aggregate decision-funnel observability artifacts without execution authority."""
    data = _dataset_dict(dataset)
    rows = sorted([
        row for row in data.get("diagnostic_artifacts") or []
        if row.get("schema_version") == "decision_funnel_observability_v1"
    ], key=_decision_funnel_artifact_sort_key)
    metrics = _empty_decision_funnel_metrics()
    suppression_values: list[float] = []
    net_drifts: list[float] = []
    cash_points: list[dict[str, Any]] = []
    stateless_distribution: dict[str, int] = {}
    stateful_distribution: dict[str, int] = {}
    stateful_pass_through_base: dict[str, int] = {}
    first_blockers: dict[str, int] = {}
    sell_reason_distribution: dict[str, int] = {}
    sell_reason_delta: dict[str, float] = {}
    sell_changed_by_distribution: dict[str, int] = {}
    sell_changed_by_delta: dict[str, float] = {}
    semantic_acceptance = _empty_scorecard_semantic_acceptance_metrics()
    cash_drift = _empty_cash_drift_attribution_metrics()
    strategy_evidence_history: dict[str, list[dict[str, Any]]] = {}
    reconciled_analysis_ids = _reconciled_analysis_ids(data)
    split = _empty_decision_funnel_split()

    for sequence, row in enumerate(rows):
        bucket = "degraded" if _is_degraded_record(row) else "normal"
        style_bucket = _decision_funnel_style_bucket(row)
        _ensure_decision_funnel_style_bucket(split, style_bucket)
        split[bucket]["sample_count"] += 1
        split["by_decision_style"][style_bucket]["sample_count"] += 1
        metrics["funnel_artifact_count"] += 1
        buy_delta = row.get("buy_delta_metrics") if isinstance(row.get("buy_delta_metrics"), dict) else {}
        intent_count = int(buy_delta.get("buy_intent_count") or len(row.get("buy_intents") or []))
        blocked_count = int(buy_delta.get("blocked_buy_count") or 0)
        metrics["buy_intent_count"] += intent_count
        metrics["blocked_buy_count"] += blocked_count
        split[bucket]["metrics"]["buy_intent_count"] += intent_count
        split[bucket]["metrics"]["blocked_buy_count"] += blocked_count
        split["by_decision_style"][style_bucket]["metrics"]["buy_intent_count"] += intent_count
        split["by_decision_style"][style_bucket]["metrics"]["blocked_buy_count"] += blocked_count

        desired = _float(buy_delta.get("desired_buy_delta_before_gates"), 0.0) or 0.0
        allowed = _float(buy_delta.get("allowed_buy_delta_after_gates"), 0.0) or 0.0
        blocked_delta = _float(buy_delta.get("blocked_buy_delta"), 0.0) or 0.0
        metrics["desired_buy_delta_before_gates"] += desired
        metrics["allowed_buy_delta_after_gates"] += allowed
        metrics["blocked_buy_delta"] += blocked_delta
        split[bucket]["metrics"]["desired_buy_delta_before_gates"] += desired
        split[bucket]["metrics"]["allowed_buy_delta_after_gates"] += allowed
        split[bucket]["metrics"]["blocked_buy_delta"] += blocked_delta
        split["by_decision_style"][style_bucket]["metrics"]["desired_buy_delta_before_gates"] += desired
        split["by_decision_style"][style_bucket]["metrics"]["allowed_buy_delta_after_gates"] += allowed
        split["by_decision_style"][style_bucket]["metrics"]["blocked_buy_delta"] += blocked_delta

        ratio = buy_delta.get("buy_delta_suppression_ratio") if isinstance(buy_delta.get("buy_delta_suppression_ratio"), dict) else {}
        if ratio.get("status") == "ok" and ratio.get("value") is not None:
            value = float(ratio.get("value"))
            suppression_values.append(value)
            split[bucket]["suppression_values"].append(value)
            split["by_decision_style"][style_bucket]["suppression_values"].append(value)
            metrics["suppression_ratio_sample_count"] += 1
            split[bucket]["metrics"]["suppression_ratio_sample_count"] += 1
            split["by_decision_style"][style_bucket]["metrics"]["suppression_ratio_sample_count"] += 1
        elif ratio.get("status") == "not_applicable":
            metrics["suppression_ratio_not_applicable_count"] += 1
            split[bucket]["metrics"]["suppression_ratio_not_applicable_count"] += 1
            split["by_decision_style"][style_bucket]["metrics"]["suppression_ratio_not_applicable_count"] += 1

        drift = row.get("net_position_drift") if isinstance(row.get("net_position_drift"), dict) else {}
        drift_value = _float(drift.get("net_position_drift"))
        if drift_value is not None:
            net_drifts.append(float(drift_value))
            split[bucket]["net_drifts"].append(float(drift_value))
            split["by_decision_style"][style_bucket]["net_drifts"].append(float(drift_value))

        cash = row.get("cash_trajectory_point") if isinstance(row.get("cash_trajectory_point"), dict) else {}
        if cash:
            cash_points.append(cash)
            split[bucket]["cash_points"].append(cash)
            split["by_decision_style"][style_bucket]["cash_points"].append(cash)

        sell_attr = row.get("sell_side_attribution") if isinstance(row.get("sell_side_attribution"), dict) else {}
        sell_count = int(sell_attr.get("sell_intent_count") or 0)
        sell_delta = _float(sell_attr.get("sell_delta_after_gates"), 0.0) or 0.0
        hard_risk_sell_delta = _float(sell_attr.get("hard_risk_sell_delta"), 0.0) or 0.0
        metrics["sell_intent_count"] += sell_count
        metrics["sell_delta_after_gates"] += sell_delta
        metrics["hard_risk_sell_delta"] += hard_risk_sell_delta
        split[bucket]["metrics"]["sell_intent_count"] += sell_count
        split[bucket]["metrics"]["sell_delta_after_gates"] += sell_delta
        split[bucket]["metrics"]["hard_risk_sell_delta"] += hard_risk_sell_delta
        split["by_decision_style"][style_bucket]["metrics"]["sell_intent_count"] += sell_count
        split["by_decision_style"][style_bucket]["metrics"]["sell_delta_after_gates"] += sell_delta
        split["by_decision_style"][style_bucket]["metrics"]["hard_risk_sell_delta"] += hard_risk_sell_delta
        _merge_counts(sell_reason_distribution, sell_attr.get("reason_code_distribution") or {})
        _merge_float_counts(sell_reason_delta, sell_attr.get("reason_code_sell_delta") or {})
        _merge_counts(sell_changed_by_distribution, sell_attr.get("changed_by_distribution") or {})
        _merge_float_counts(sell_changed_by_delta, sell_attr.get("changed_by_sell_delta") or {})
        _merge_counts(split[bucket]["sell_reason_distribution"], sell_attr.get("reason_code_distribution") or {})
        _merge_float_counts(split[bucket]["sell_reason_delta"], sell_attr.get("reason_code_sell_delta") or {})
        _merge_counts(split[bucket]["sell_changed_by_distribution"], sell_attr.get("changed_by_distribution") or {})
        _merge_float_counts(split[bucket]["sell_changed_by_delta"], sell_attr.get("changed_by_sell_delta") or {})
        style_split = split["by_decision_style"][style_bucket]
        _merge_counts(style_split["sell_reason_distribution"], sell_attr.get("reason_code_distribution") or {})
        _merge_float_counts(style_split["sell_reason_delta"], sell_attr.get("reason_code_sell_delta") or {})
        _merge_counts(style_split["sell_changed_by_distribution"], sell_attr.get("changed_by_distribution") or {})
        _merge_float_counts(style_split["sell_changed_by_delta"], sell_attr.get("changed_by_sell_delta") or {})

        _merge_counts(stateless_distribution, row.get("stateless_all_blocker_distribution") or {})
        _merge_counts(stateful_distribution, row.get("stateful_incremental_blocker_distribution") or {})
        _merge_counts(stateful_pass_through_base, row.get("stateful_pass_through_base_by_layer") or {})
        _merge_counts(first_blockers, row.get("first_blocker_distribution") or {})
        _merge_counts(split[bucket]["stateless_distribution"], row.get("stateless_all_blocker_distribution") or {})
        _merge_counts(split[bucket]["stateful_distribution"], row.get("stateful_incremental_blocker_distribution") or {})
        _merge_counts(split[bucket]["stateful_pass_through_base"], row.get("stateful_pass_through_base_by_layer") or {})
        _merge_counts(split[bucket]["first_blockers"], row.get("first_blocker_distribution") or {})
        style_split = split["by_decision_style"][style_bucket]
        _merge_counts(style_split["stateless_distribution"], row.get("stateless_all_blocker_distribution") or {})
        _merge_counts(style_split["stateful_distribution"], row.get("stateful_incremental_blocker_distribution") or {})
        _merge_counts(style_split["stateful_pass_through_base"], row.get("stateful_pass_through_base_by_layer") or {})
        _merge_counts(style_split["first_blockers"], row.get("first_blocker_distribution") or {})
        metrics["single_blocker_candidate_count"] += int(row.get("single_blocker_candidate_count") or 0)
        split[bucket]["metrics"]["single_blocker_candidate_count"] += int(row.get("single_blocker_candidate_count") or 0)
        style_split["metrics"]["single_blocker_candidate_count"] += int(row.get("single_blocker_candidate_count") or 0)

        _accumulate_scorecard_semantic_acceptance(
            semantic_acceptance,
            row,
            reconciled_analysis_ids=reconciled_analysis_ids,
        )
        _accumulate_cash_drift_attribution(cash_drift, row)
        _accumulate_strategy_evidence_history(strategy_evidence_history, row, sequence=sequence)

    strategy_evidence_monitor = _finalize_strategy_evidence_monitor(strategy_evidence_history)
    finalized_metrics = _round_decision_funnel_metrics({
        **metrics,
        "certification_flip_count_7d": strategy_evidence_monitor["total_flip_count_7d"],
        "certification_flip_alert_strategy_count": len(strategy_evidence_monitor["alert_strategies"]),
        "buy_delta_suppression_ratio_avg": _avg(suppression_values),
        "net_position_drift_avg": _avg(net_drifts),
        "cash_trajectory": _cash_trajectory_summary(cash_points),
    })
    return _section({
        "schema_version": "weekly_decision_funnel_review_v1",
        "metrics": finalized_metrics,
        "rates": _decision_funnel_rates(finalized_metrics, min_sample_n=min_sample_n),
        "first_blocker_distribution": dict(sorted(first_blockers.items())),
        "stateless_blocker_distribution": dict(sorted(stateless_distribution.items())),
        "stateful_incremental_blocker_distribution": dict(sorted(stateful_distribution.items())),
        "stateful_pass_through_base_by_layer": dict(sorted(stateful_pass_through_base.items())),
        "sell_side_attribution": {
            "reason_code_distribution": dict(sorted(sell_reason_distribution.items())),
            "reason_code_sell_delta": _round_float_counts(sell_reason_delta),
            "changed_by_distribution": dict(sorted(sell_changed_by_distribution.items())),
            "changed_by_sell_delta": _round_float_counts(sell_changed_by_delta),
            "metric_contract": (
                "sell-side attribution is diagnostic-only and explains net cash build-up; "
                "it must not block risk-reducing sells"
            ),
        },
        "scorecard_semantic_acceptance": _finalize_scorecard_semantic_acceptance(semantic_acceptance),
        "cash_drift_attribution": _finalize_cash_drift_attribution(cash_drift),
        "strategy_execution_evidence_monitor": strategy_evidence_monitor,
        "decision_degradation_split": _finalize_decision_funnel_split(
            {"normal": split["normal"], "degraded": split["degraded"]},
            min_sample_n=min_sample_n,
        ),
        "decision_style_split": _finalize_decision_funnel_split(
            split["by_decision_style"],
            min_sample_n=min_sample_n,
        ),
        "metric_contract": {
            "min_buy_intent_delta": (
                "buy-intent threshold is intentionally lower than execution floor so "
                "small buy intents suppressed by execution filters remain visible"
            ),
            "stateless_vs_stateful_denominators": (
                "stateless blocker rates use all buy intents; stateful incremental blocker rates "
                "use only candidates reaching that layer and must not be compared as one distribution"
            ),
            "suppression_ratio": (
                "desired_buy_delta=0 returns not_applicable and is excluded from aggregate suppression ratios"
            ),
            "llm_shadow": (
                "LLM influence must be measured only from explicit no-LLM dry-run shadow targets; "
                "missing shadows are not inferred from final target fields"
            ),
        },
    })


def build_weekly_self_assessment_metrics(
    dataset: WeekendReviewDataset | dict[str, Any],
    *,
    min_sample_n: int = 5,
) -> dict[str, Any]:
    data = _dataset_dict(dataset)
    prior = [
        row for row in data.get("diagnostic_artifacts") or []
        if str(row.get("schema_version") or "").startswith("weekly_")
    ]
    mature = [
        row for row in prior
        if (row.get("self_assessment") or {}).get("mature") is True
    ]
    supported = [
        row for row in mature
        if (row.get("self_assessment") or {}).get("supported") is True
    ]
    contradicted = [
        row for row in mature
        if (row.get("self_assessment") or {}).get("contradicted") is True
    ]
    return _section({
        "schema_version": "weekly_review_self_assessment_v1",
        "metrics": {
            "prior_recommendation_count": len(prior),
            "prior_recommendation_mature_count": len(mature),
            "prior_recommendation_supported_count": len(supported),
            "prior_recommendation_contradicted_count": len(contradicted),
            "prior_recommendation_pending_count": max(len(prior) - len(mature), 0),
        },
        "rates": {
            "prior_recommendation_supported_rate": rate_metric(
                "prior_recommendation_supported_rate",
                numerator=len(supported),
                denominator=len(mature),
                min_sample_n=min_sample_n,
            )
        },
    })


def rate_metric(metric: str, *, numerator: int, denominator: int, min_sample_n: int) -> dict[str, Any]:
    sample_n = max(int(denominator or 0), 0)
    numer = max(int(numerator or 0), 0)
    min_n = max(int(min_sample_n or 1), 1)
    if sample_n < min_n:
        return {
            "metric": metric,
            "value": None,
            "status": "insufficient_sample",
            "sample_n": sample_n,
            "min_sample_n": min_n,
            "numerator": numer,
            "denominator": sample_n,
        }
    return {
        "metric": metric,
        "value": numer / sample_n if sample_n else None,
        "status": "ok",
        "sample_n": sample_n,
        "min_sample_n": min_n,
        "numerator": numer,
        "denominator": sample_n,
    }


def hedge_counterfactual_return(
    *,
    candidate_hedge_instrument: Any,
    severity: Any,
    decision_date: Any,
    feature_rows: list[dict[str, Any]],
    horizon_days: int = DEFAULT_HEDGE_HORIZON_DAYS,
    policy_version: str = DEFAULT_HEDGE_WEIGHT_POLICY_VERSION,
) -> dict[str, Any]:
    ticker = str(candidate_hedge_instrument or "").upper().strip()
    decision = _parse_date(decision_date)
    if not ticker or decision is None:
        return {
            "status": "insufficient_data",
            "reason": "missing_candidate_or_decision_date",
            "candidate_hedge_instrument": ticker or None,
            "policy_version": policy_version,
        }
    hedge_weight = hedge_weight_from_severity(severity, policy_version=policy_version)
    if hedge_weight is None:
        return {
            "status": "missing_counterfactual_policy",
            "candidate_hedge_instrument": ticker,
            "policy_version": policy_version,
        }
    prices = _feature_prices(feature_rows, ticker)
    entry_candidates = [row for row in prices if row["trading_date"] <= decision]
    if not entry_candidates:
        return {
            "status": "insufficient_data",
            "reason": "missing_entry_price_at_or_before_decision",
            "candidate_hedge_instrument": ticker,
            "hedge_weight": hedge_weight,
            "policy_version": policy_version,
        }
    entry = entry_candidates[-1]
    path = [row for row in prices if row["trading_date"] >= entry["trading_date"]]
    if len(path) <= int(horizon_days):
        return {
            "status": "insufficient_data",
            "reason": "missing_exit_price_at_horizon",
            "candidate_hedge_instrument": ticker,
            "hedge_weight": hedge_weight,
            "entry_date": entry["trading_date"].isoformat(),
            "entry_price": entry["price"],
            "policy_version": policy_version,
        }
    exit_row = path[int(horizon_days)]
    hedge_return = exit_row["price"] / entry["price"] - 1.0
    contribution = hedge_weight * hedge_return
    return {
        "status": "ok",
        "candidate_hedge_instrument": ticker,
        "policy_version": policy_version,
        "hedge_weight": round(hedge_weight, 6),
        "entry_date": entry["trading_date"].isoformat(),
        "exit_date": exit_row["trading_date"].isoformat(),
        "entry_price": round(entry["price"], 6),
        "exit_price": round(exit_row["price"], 6),
        "entry_price_source": entry["source"],
        "exit_price_source": exit_row["source"],
        "price_source": entry["source"] if entry["source"] == exit_row["source"] else "mixed",
        "hedge_return": round(hedge_return, 6),
        "hedge_contribution": round(contribution, 8),
        "uses_real_candidate_etf_price_path": True,
    }


def hedge_weight_from_severity(
    severity: Any,
    *,
    policy_version: str = DEFAULT_HEDGE_WEIGHT_POLICY_VERSION,
) -> float | None:
    if policy_version != DEFAULT_HEDGE_WEIGHT_POLICY_VERSION:
        return None
    sev = _float(severity)
    if sev is None or sev <= 0.0:
        return 0.0
    return min(0.02, max(0.005, sev * 0.02))


def _section(payload: dict[str, Any]) -> dict[str, Any]:
    out = {
        "execution_authority": EXECUTION_AUTHORITY,
        "target_weight_mutation": TARGET_WEIGHT_MUTATION,
        **payload,
    }
    return json_safe(out)


def _empty_intent_metrics() -> dict[str, int]:
    return {
        "risk_block_count": 0,
        "final_validation_block_count": 0,
        "execution_preflight_block_count": 0,
        "daily_command_cap_block_count": 0,
        "daily_turnover_cap_block_count": 0,
        "dedupe_count": 0,
        "execution_timeout_count": 0,
        "qc_reject_count": 0,
        "approved_not_sent_count": 0,
        "hedge_triggered_not_added_count": 0,
    }


def _empty_intent_degradation_split() -> dict[str, dict[str, Any]]:
    return {
        "normal": {
            "sample_count": 0,
            "metrics": _empty_intent_metrics(),
            "blocker_distribution": {},
        },
        "degraded": {
            "sample_count": 0,
            "metrics": _empty_intent_metrics(),
            "blocker_distribution": {},
        },
    }


def _sorted_degradation_split(split: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for bucket in ("normal", "degraded"):
        payload = split.get(bucket) or {}
        out[bucket] = {
            "sample_count": int(payload.get("sample_count") or 0),
            "metrics": payload.get("metrics") or _empty_intent_metrics(),
            "blocker_distribution": dict(sorted((payload.get("blocker_distribution") or {}).items())),
        }
    return out


def _empty_hedge_metrics() -> dict[str, int]:
    return {
        "hedge_trigger_count": 0,
        "hedge_added_count": 0,
        "triggered_not_added_count": 0,
        "false_negative_count": 0,
        "triggered_no_drop_count": 0,
        "triggered_hedge_would_hurt_count": 0,
        "missed_protection_count": 0,
        "hedge_would_have_helped_count": 0,
        "hedge_would_have_hurt_count": 0,
        "insufficient_market_outcome_count": 0,
        "insufficient_counterfactual_count": 0,
    }


def _hedge_rates(metrics: dict[str, int], *, sample_count: int, min_sample_n: int) -> dict[str, Any]:
    return {
        "false_negative_rate": rate_metric(
            "hedge_false_negative_rate",
            numerator=metrics["false_negative_count"],
            denominator=sample_count,
            min_sample_n=min_sample_n,
        ),
        "triggered_hedge_would_hurt_rate": rate_metric(
            "triggered_hedge_would_hurt_rate",
            numerator=metrics["triggered_hedge_would_hurt_count"],
            denominator=max(metrics["hedge_trigger_count"], 0),
            min_sample_n=min_sample_n,
        ),
    }


def _empty_metric_split(metrics_factory) -> dict[str, dict[str, Any]]:
    return {
        "normal": {"sample_count": 0, "metrics": metrics_factory()},
        "degraded": {"sample_count": 0, "metrics": metrics_factory()},
    }


def _increment_metric_targets(targets: list[dict[str, int]], key: str, amount: int = 1) -> None:
    for target in targets:
        target[key] = int(target.get(key) or 0) + amount


def _finalize_metric_split(split: dict[str, dict[str, Any]], *, rate_builder=None) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for bucket in ("normal", "degraded"):
        item = split.get(bucket) or {}
        payload = {
            "sample_count": int(item.get("sample_count") or 0),
            "metrics": item.get("metrics") or {},
        }
        if rate_builder is not None:
            payload["rates"] = rate_builder(payload)
        out[bucket] = payload
    return out


def _empty_debate_metrics() -> dict[str, int]:
    return {
        "debate_available_count": 0,
        "disagreement_count_total": 0,
        "debate_changed_target_count": 0,
        "debate_failure_count": 0,
    }


def _empty_debate_split() -> dict[str, dict[str, Any]]:
    return {
        "normal": {
            "sample_count": 0,
            "metrics": _empty_debate_metrics(),
            "changed_ticker_samples": 0,
            "changed_ticker_wins": 0,
        },
        "degraded": {
            "sample_count": 0,
            "metrics": _empty_debate_metrics(),
            "changed_ticker_samples": 0,
            "changed_ticker_wins": 0,
        },
    }


def _finalize_debate_split(split: dict[str, dict[str, Any]], *, min_sample_n: int) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for bucket in ("normal", "degraded"):
        item = split.get(bucket) or {}
        metrics = item.get("metrics") or _empty_debate_metrics()
        out[bucket] = {
            "sample_count": int(item.get("sample_count") or 0),
            "metrics": metrics,
            "rates": {
                "debate_change_rate": rate_metric(
                    "debate_change_rate",
                    numerator=metrics["debate_changed_target_count"],
                    denominator=int(item.get("sample_count") or 0),
                    min_sample_n=1,
                ),
                "changed_ticker_outcome_win_rate": rate_metric(
                    "changed_ticker_outcome_win_rate",
                    numerator=int(item.get("changed_ticker_wins") or 0),
                    denominator=int(item.get("changed_ticker_samples") or 0),
                    min_sample_n=min_sample_n,
                ),
            },
        }
    return out


def _empty_basket_split() -> dict[str, dict[str, Any]]:
    def bucket() -> dict[str, Any]:
        return {
            "sample_count": 0,
            "active_counts": [],
            "cash_values": [],
            "effective_ns": [],
            "active_count_out_of_range_count": 0,
            "subscale_position_count": 0,
            "floor_cleared_count": 0,
        }

    return {"normal": bucket(), "degraded": bucket()}


def _finalize_basket_split(split: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for bucket in ("normal", "degraded"):
        item = split.get(bucket) or {}
        out[bucket] = {
            "sample_count": int(item.get("sample_count") or 0),
            "metrics": {
                "active_count_avg": _avg(item.get("active_counts") or []),
                "active_count_out_of_range_count": int(item.get("active_count_out_of_range_count") or 0),
                "subscale_position_count": int(item.get("subscale_position_count") or 0),
                "floor_cleared_count": int(item.get("floor_cleared_count") or 0),
                "cash_avg": _avg(item.get("cash_values") or []),
                "effective_n_avg": _avg(item.get("effective_ns") or []),
            },
        }
    return out


def _empty_regime_split() -> dict[str, dict[str, Any]]:
    return _empty_metric_split(lambda: {
        "market_risk_assessment_count": 0,
        "risk_off_call_count": 0,
        "hard_risk_outcome_count": 0,
    })


def _finalize_regime_split(split: dict[str, dict[str, Any]], *, min_sample_n: int) -> dict[str, dict[str, Any]]:
    return _finalize_metric_split(
        split,
        rate_builder=lambda item: {
            "risk_off_recall_proxy": {
                **rate_metric(
                    "risk_off_recall_proxy",
                    numerator=0,
                    denominator=0,
                    min_sample_n=min_sample_n,
                ),
                "proxy_caveat": (
                    "denominator is observable negative forward-return windows; "
                    "true should-have-been-risk-off is not directly observable"
                ),
            }
        },
    )


def _empty_style_opportunity_metrics() -> dict[str, int]:
    return {
        "style_event_count": 0,
        "defensive_style_count": 0,
        "defensive_style_market_outcome_count": 0,
        "defensive_style_market_down_count": 0,
        "defensive_style_market_outcome_count_1d": 0,
        "defensive_style_market_down_count_1d": 0,
        "defensive_style_market_outcome_count_5d": 0,
        "defensive_style_market_down_count_5d": 0,
        "blocked_buy_candidate_count": 0,
        "blocked_buy_mature_count": 0,
        "blocked_buy_outperformed_benchmark_count": 0,
        "insufficient_defensive_market_outcome_count": 0,
        "insufficient_defensive_market_outcome_count_1d": 0,
        "insufficient_defensive_market_outcome_count_5d": 0,
        "insufficient_blocked_buy_outcome_count": 0,
    }


def _empty_style_opportunity_split() -> dict[str, dict[str, Any]]:
    return {
        "normal": {
            "sample_count": 0,
            "metrics": _empty_style_opportunity_metrics(),
            "blocked_buy_excess_returns": [],
        },
        "degraded": {
            "sample_count": 0,
            "metrics": _empty_style_opportunity_metrics(),
            "blocked_buy_excess_returns": [],
        },
    }


def _style_opportunity_rates(metrics: dict[str, int], *, min_sample_n: int) -> dict[str, Any]:
    return {
        "defensive_style_hit_rate_1d": rate_metric(
            "defensive_style_hit_rate_1d",
            numerator=int(metrics.get("defensive_style_market_down_count_1d") or 0),
            denominator=int(metrics.get("defensive_style_market_outcome_count_1d") or 0),
            min_sample_n=min_sample_n,
        ),
        "defensive_style_hit_rate_5d": rate_metric(
            "defensive_style_hit_rate_5d",
            numerator=int(metrics.get("defensive_style_market_down_count_5d") or 0),
            denominator=int(metrics.get("defensive_style_market_outcome_count_5d") or 0),
            min_sample_n=min_sample_n,
        ),
        "blocked_buy_outperform_rate_5d": rate_metric(
            "blocked_buy_outperform_rate_5d",
            numerator=int(metrics.get("blocked_buy_outperformed_benchmark_count") or 0),
            denominator=int(metrics.get("blocked_buy_mature_count") or 0),
            min_sample_n=min_sample_n,
        ),
    }


def _finalize_style_opportunity_split(
    split: dict[str, dict[str, Any]],
    *,
    min_sample_n: int,
) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for bucket in ("normal", "degraded"):
        item = split.get(bucket) or {}
        metrics = item.get("metrics") or _empty_style_opportunity_metrics()
        out[bucket] = {
            "sample_count": int(item.get("sample_count") or 0),
            "metrics": {
                **metrics,
                "blocked_buy_avg_excess_return_vs_benchmark": _avg(
                    item.get("blocked_buy_excess_returns") or []
                ),
            },
            "rates": _style_opportunity_rates(metrics, min_sample_n=min_sample_n),
        }
    return out


def _empty_decision_funnel_metrics() -> dict[str, Any]:
    return {
        "funnel_artifact_count": 0,
        "buy_intent_count": 0,
        "blocked_buy_count": 0,
        "desired_buy_delta_before_gates": 0.0,
        "allowed_buy_delta_after_gates": 0.0,
        "blocked_buy_delta": 0.0,
        "suppression_ratio_sample_count": 0,
        "suppression_ratio_not_applicable_count": 0,
        "single_blocker_candidate_count": 0,
        "sell_intent_count": 0,
        "sell_delta_after_gates": 0.0,
        "hard_risk_sell_delta": 0.0,
    }


def _empty_scorecard_semantic_acceptance_metrics() -> dict[str, Any]:
    return {
        "limited_data_quality_human_required_occurrence_count": 0,
        "limited_data_quality_human_required_small_add_attempted_count": 0,
        "limited_data_quality_human_required_small_add_reconciled_count": 0,
        "limited_data_quality_human_required_small_add_attempted_ticker_count": 0,
        "strategy_advisory_only_occurrence_count": 0,
        "strategy_advisory_only_scorecard_block_count": 0,
        "strategy_advisory_only_blocked_ticker_count": 0,
    }


def _empty_cash_drift_attribution_metrics() -> dict[str, Any]:
    return {
        "bucket_delta": {
            "buy_blocked_by_gate": 0.0,
            "target_builder_conservative": 0.0,
            "sell_side_active_trim": 0.0,
            "execution_not_realized": 0.0,
        },
        "bucket_event_count": {
            "buy_blocked_by_gate": 0,
            "target_builder_conservative": 0,
            "sell_side_active_trim": 0,
            "execution_not_realized": 0,
        },
        "actual_cash_delta_sum": 0.0,
        "bucket_total_sum": 0.0,
        "residual_sum": 0.0,
        "sample_count": 0,
    }


def _accumulate_scorecard_semantic_acceptance(
    out: dict[str, Any],
    row: dict[str, Any],
    *,
    reconciled_analysis_ids: set[int],
) -> None:
    payload = row.get("scorecard_semantic_acceptance") if isinstance(row.get("scorecard_semantic_acceptance"), dict) else {}
    limited = payload.get("limited_data_quality_human_required_small_add") if isinstance(payload.get("limited_data_quality_human_required_small_add"), dict) else {}
    strategy = payload.get("strategy_advisory_only_scorecard_block") if isinstance(payload.get("strategy_advisory_only_scorecard_block"), dict) else {}

    if bool(limited.get("occurrence")):
        out["limited_data_quality_human_required_occurrence_count"] += 1
    attempted = int(limited.get("attempted_count") or 0)
    out["limited_data_quality_human_required_small_add_attempted_count"] += attempted
    out["limited_data_quality_human_required_small_add_attempted_ticker_count"] += int(limited.get("attempted_ticker_count") or 0)
    analysis_id = _int(row.get("analysis_id"), default=-1)
    if attempted > 0 and analysis_id in reconciled_analysis_ids:
        out["limited_data_quality_human_required_small_add_reconciled_count"] += attempted

    out["strategy_advisory_only_occurrence_count"] += int(strategy.get("occurrence_count") or (1 if bool(strategy.get("occurrence")) else 0))
    out["strategy_advisory_only_scorecard_block_count"] += int(strategy.get("blocked_count") or 0)
    out["strategy_advisory_only_blocked_ticker_count"] += int(strategy.get("blocked_ticker_count") or 0)


def _accumulate_cash_drift_attribution(out: dict[str, Any], row: dict[str, Any]) -> None:
    payload = row.get("cash_drift_attribution") if isinstance(row.get("cash_drift_attribution"), dict) else {}
    if not payload:
        return
    out["sample_count"] += 1
    out["actual_cash_delta_sum"] += float(_float(payload.get("actual_cash_delta"), 0.0) or 0.0)
    out["bucket_total_sum"] += float(_float(payload.get("bucket_total"), 0.0) or 0.0)
    out["residual_sum"] += float(_float(payload.get("residual"), 0.0) or 0.0)
    buckets = payload.get("buckets") if isinstance(payload.get("buckets"), dict) else {}
    for bucket, bucket_payload in buckets.items():
        if not isinstance(bucket_payload, dict):
            continue
        if bucket not in out["bucket_delta"]:
            out["bucket_delta"][bucket] = 0.0
            out["bucket_event_count"][bucket] = 0
        out["bucket_delta"][bucket] += float(_float(bucket_payload.get("delta"), 0.0) or 0.0)
        out["bucket_event_count"][bucket] += int(bucket_payload.get("event_count") or 0)


def _accumulate_strategy_evidence_history(
    out: dict[str, list[dict[str, Any]]],
    row: dict[str, Any],
    *,
    sequence: int,
) -> None:
    flags = row.get("data_quality_flags") if isinstance(row.get("data_quality_flags"), dict) else {}
    summary = flags.get("strategy_execution_evidence") if isinstance(flags.get("strategy_execution_evidence"), dict) else {}
    if not summary:
        summary = row.get("strategy_execution_evidence") if isinstance(row.get("strategy_execution_evidence"), dict) else {}
    for item in summary.get("rows") or []:
        if not isinstance(item, dict):
            continue
        strategy_name = str(item.get("strategy_name") or "").strip()
        status = str(item.get("execution_evidence_status") or "").strip()
        if not strategy_name or not status:
            continue
        out.setdefault(strategy_name, []).append({
            "sequence": sequence,
            "created_at": row.get("created_at") or row.get("as_of_time"),
            "analysis_id": row.get("analysis_id"),
            "execution_evidence_status": status,
            "failed_checks": item.get("failed_checks") or [],
        })


def _finalize_strategy_evidence_monitor(history: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    by_strategy: dict[str, dict[str, Any]] = {}
    total_flips = 0
    for strategy_name, rows in sorted(history.items()):
        ordered = sorted(rows, key=lambda item: int(item.get("sequence") or 0))
        previous: str | None = None
        transitions: list[dict[str, Any]] = []
        for row in ordered:
            status = str(row.get("execution_evidence_status") or "")
            if previous is not None and status != previous:
                transitions.append({
                    "from": previous,
                    "to": status,
                    "analysis_id": row.get("analysis_id"),
                    "created_at": row.get("created_at"),
                })
            previous = status
        flip_count = len(transitions)
        total_flips += flip_count
        by_strategy[strategy_name] = {
            "sample_count": len(ordered),
            "flip_count_7d": flip_count,
            "latest_status": ordered[-1].get("execution_evidence_status") if ordered else None,
            "latest_failed_checks": ordered[-1].get("failed_checks") if ordered else [],
            "transitions": transitions[:10],
        }
    alert_strategies = [
        strategy_name
        for strategy_name, item in by_strategy.items()
        if int(item.get("flip_count_7d") or 0) >= 3
    ]
    return {
        "schema_version": "strategy_execution_evidence_monitor_v1",
        "total_flip_count_7d": total_flips,
        "alert_threshold_7d": 3,
        "alert_strategies": alert_strategies,
        "by_strategy": by_strategy,
        "contract": (
            "Certification status is recomputed fail-closed; flip counts are observability-only "
            "and should trigger hysteresis review, not mutate thresholds."
        ),
    }


def _finalize_scorecard_semantic_acceptance(metrics: dict[str, Any]) -> dict[str, Any]:
    limited_attempted = int(metrics.get("limited_data_quality_human_required_small_add_attempted_count") or 0)
    limited_reconciled = int(metrics.get("limited_data_quality_human_required_small_add_reconciled_count") or 0)
    strategy_occurrences = int(metrics.get("strategy_advisory_only_occurrence_count") or 0)
    strategy_blocks = int(metrics.get("strategy_advisory_only_scorecard_block_count") or 0)
    limited_status = (
        "CONFIRMED"
        if limited_reconciled >= 3 and limited_reconciled == limited_attempted
        else "NOT_YET_EXERCISED"
        if limited_attempted == 0
        else "PENDING_OR_FAILED"
    )
    strategy_status = (
        "CONFIRMED"
        if strategy_occurrences >= 1 and strategy_blocks == strategy_occurrences
        else "NOT_YET_EXERCISED"
        if strategy_occurrences == 0
        else "UNEXPECTED_NOT_BLOCKED"
    )
    return {
        "schema_version": "weekly_scorecard_semantic_acceptance_v1",
        "metrics": dict(metrics),
        "acceptance": {
            "limited_data_quality_human_required_small_add": {
                "status": limited_status,
                "required_reconciled_count": 3,
                "attempted_count": limited_attempted,
                "reconciled_count": limited_reconciled,
            },
            "strategy_advisory_only_scorecard_block": {
                "status": strategy_status,
                "required_occurrence_count": 1,
                "occurrence_count": strategy_occurrences,
                "blocked_count": strategy_blocks,
            },
        },
        "contract": (
            "Do not mark scorecard semantic changes green when the path has not been exercised; "
            "attempted/reconciled and occurrence/blocked denominators are required."
        ),
    }


def _finalize_cash_drift_attribution(metrics: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": "weekly_cash_drift_four_bucket_v1",
        "sample_count": int(metrics.get("sample_count") or 0),
        "actual_cash_delta_sum": round(float(metrics.get("actual_cash_delta_sum") or 0.0), 6),
        "bucket_total_sum": round(float(metrics.get("bucket_total_sum") or 0.0), 6),
        "residual_sum": round(float(metrics.get("residual_sum") or 0.0), 6),
        "bucket_delta": _round_float_counts(metrics.get("bucket_delta") or {}),
        "bucket_event_count": dict(sorted((metrics.get("bucket_event_count") or {}).items())),
        "accounting_note": (
            "Buckets are mutually named but not all are actual cash-flow buckets: buy_blocked_by_gate "
            "and target_builder_conservative are counterfactual/opportunity attribution; residual must be reviewed."
        ),
    }


def _reconciled_analysis_ids(dataset: dict[str, Any]) -> set[int]:
    out: set[int] = set()
    for row in dataset.get("execution_logs") or []:
        if not isinstance(row, dict):
            continue
        state = _execution_state(row)
        if state not in TERMINAL_FILLED_STATES:
            continue
        for key in ("analysis_id", "source_analysis_id"):
            value = _int(row.get(key), default=-1)
            if value >= 0:
                out.add(value)
    return out


def _empty_decision_funnel_bucket() -> dict[str, Any]:
    return {
        "sample_count": 0,
        "metrics": _empty_decision_funnel_metrics(),
        "suppression_values": [],
        "net_drifts": [],
        "cash_points": [],
        "first_blockers": {},
        "stateless_distribution": {},
        "stateful_distribution": {},
        "stateful_pass_through_base": {},
        "sell_reason_distribution": {},
        "sell_reason_delta": {},
        "sell_changed_by_distribution": {},
        "sell_changed_by_delta": {},
    }


def _empty_decision_funnel_split() -> dict[str, Any]:
    return {
        "normal": _empty_decision_funnel_bucket(),
        "degraded": _empty_decision_funnel_bucket(),
        "by_decision_style": {},
    }


def _ensure_decision_funnel_style_bucket(split: dict[str, Any], style_bucket: str) -> None:
    by_style = split.setdefault("by_decision_style", {})
    if style_bucket not in by_style:
        by_style[style_bucket] = _empty_decision_funnel_bucket()


def _decision_funnel_style_bucket(row: dict[str, Any]) -> str:
    style = row.get("decision_style") if isinstance(row.get("decision_style"), dict) else {}
    trade_style = str(style.get("trade_style") or "unknown").strip() or "unknown"
    analysis_style = str(style.get("analysis_style") or "unknown").strip() or "unknown"
    return f"{analysis_style}:{trade_style}"


def _decision_funnel_artifact_sort_key(row: dict[str, Any]) -> tuple[str, int]:
    raw_time = row.get("created_at") or row.get("as_of_time") or row.get("observation_date") or ""
    return (str(raw_time), _int(row.get("analysis_id"), default=0))


def _decision_funnel_rates(metrics: dict[str, Any], *, min_sample_n: int) -> dict[str, Any]:
    return {
        "blocked_buy_rate": rate_metric(
            "blocked_buy_rate",
            numerator=int(metrics.get("blocked_buy_count") or 0),
            denominator=int(metrics.get("buy_intent_count") or 0),
            min_sample_n=min_sample_n,
        ),
        "single_blocker_candidate_rate": rate_metric(
            "single_blocker_candidate_rate",
            numerator=int(metrics.get("single_blocker_candidate_count") or 0),
            denominator=int(metrics.get("buy_intent_count") or 0),
            min_sample_n=min_sample_n,
        ),
    }


def _finalize_decision_funnel_split(
    split: dict[str, dict[str, Any]],
    *,
    min_sample_n: int,
) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for bucket, item in sorted(split.items()):
        metrics = _round_decision_funnel_metrics({
            **(item.get("metrics") or _empty_decision_funnel_metrics()),
            "buy_delta_suppression_ratio_avg": _avg(item.get("suppression_values") or []),
            "net_position_drift_avg": _avg(item.get("net_drifts") or []),
            "cash_trajectory": _cash_trajectory_summary(item.get("cash_points") or []),
        })
        out[bucket] = {
            "sample_count": int(item.get("sample_count") or 0),
            "metrics": metrics,
            "rates": _decision_funnel_rates(metrics, min_sample_n=min_sample_n),
            "first_blocker_distribution": dict(sorted((item.get("first_blockers") or {}).items())),
            "stateless_blocker_distribution": dict(sorted((item.get("stateless_distribution") or {}).items())),
            "stateful_incremental_blocker_distribution": dict(sorted((item.get("stateful_distribution") or {}).items())),
            "stateful_pass_through_base_by_layer": dict(sorted((item.get("stateful_pass_through_base") or {}).items())),
            "sell_side_attribution": {
                "reason_code_distribution": dict(sorted((item.get("sell_reason_distribution") or {}).items())),
                "reason_code_sell_delta": _round_float_counts(item.get("sell_reason_delta") or {}),
                "changed_by_distribution": dict(sorted((item.get("sell_changed_by_distribution") or {}).items())),
                "changed_by_sell_delta": _round_float_counts(item.get("sell_changed_by_delta") or {}),
            },
        }
    return out


def _round_decision_funnel_metrics(metrics: dict[str, Any]) -> dict[str, Any]:
    out = dict(metrics)
    for key in (
        "desired_buy_delta_before_gates",
        "allowed_buy_delta_after_gates",
        "blocked_buy_delta",
        "buy_delta_suppression_ratio_avg",
        "net_position_drift_avg",
        "sell_delta_after_gates",
        "hard_risk_sell_delta",
    ):
        if out.get(key) is not None:
            out[key] = round(float(out.get(key) or 0.0), 6)
    return out


def _cash_trajectory_summary(points: list[dict[str, Any]]) -> dict[str, Any]:
    parsed: list[tuple[datetime, float]] = []
    for row in points:
        if not isinstance(row, dict):
            continue
        raw_time = row.get("as_of_time") or row.get("created_at")
        value = _float(row.get("target_cash_weight"))
        if value is None:
            value = _float(row.get("current_cash_weight"))
        parsed_time = _parse_datetime(raw_time)
        if parsed_time is None or value is None:
            continue
        parsed.append((parsed_time, float(value)))
    parsed.sort(key=lambda item: item[0])
    if not parsed:
        return {
            "sample_count": 0,
            "cash_weight_start": None,
            "cash_weight_end": None,
            "cash_slope_per_day": None,
            "status": "insufficient_sample",
        }
    start_time, start_cash = parsed[0]
    end_time, end_cash = parsed[-1]
    elapsed_days = max((end_time - start_time).total_seconds() / 86400.0, 0.0)
    slope = None if elapsed_days <= 0 else (end_cash - start_cash) / elapsed_days
    return {
        "sample_count": len(parsed),
        "cash_weight_start": round(start_cash, 6),
        "cash_weight_end": round(end_cash, 6),
        "cash_delta": round(end_cash - start_cash, 6),
        "cash_slope_per_day": round(slope, 6) if slope is not None else None,
        "status": "ok" if len(parsed) >= 2 and slope is not None else "insufficient_sample",
    }


def _merge_counts(target: dict[str, int], source: dict[str, Any]) -> None:
    for key, raw in (source or {}).items():
        target[str(key)] = target.get(str(key), 0) + int(raw or 0)


def _merge_float_counts(target: dict[str, float], source: dict[str, Any]) -> None:
    for key, raw in (source or {}).items():
        target[str(key)] = target.get(str(key), 0.0) + float(_float(raw, 0.0) or 0.0)


def _round_float_counts(values: dict[str, Any]) -> dict[str, float]:
    return {
        str(key): round(float(_float(raw, 0.0) or 0.0), 6)
        for key, raw in sorted((values or {}).items())
    }


def _defensive_style_horizons(primary_horizon_days: int) -> list[int]:
    horizons: list[int] = []
    for value in (1, int(primary_horizon_days)):
        if value > 0 and value not in horizons:
            horizons.append(value)
    return horizons


def _record_defensive_style_market_outcome(
    metrics: dict[str, int],
    split_metrics: dict[str, int],
    *,
    feature_rows: list[dict[str, Any]],
    observation_date: Any,
    horizon_days: int,
    market_return_threshold: float,
    primary_horizon_days: int,
    update_primary_aliases: bool = True,
) -> None:
    suffix = f"{int(horizon_days)}d"
    market_return = _market_forward_return(
        feature_rows,
        observation_date=observation_date,
        horizon_days=int(horizon_days),
    )
    if market_return is None:
        _increment_named_metric(metrics, f"insufficient_defensive_market_outcome_count_{suffix}")
        _increment_named_metric(split_metrics, f"insufficient_defensive_market_outcome_count_{suffix}")
        if update_primary_aliases and int(horizon_days) == int(primary_horizon_days):
            _increment_named_metric(metrics, "insufficient_defensive_market_outcome_count")
            _increment_named_metric(split_metrics, "insufficient_defensive_market_outcome_count")
        return

    _increment_named_metric(metrics, f"defensive_style_market_outcome_count_{suffix}")
    _increment_named_metric(split_metrics, f"defensive_style_market_outcome_count_{suffix}")
    if update_primary_aliases and int(horizon_days) == int(primary_horizon_days):
        _increment_named_metric(metrics, "defensive_style_market_outcome_count")
        _increment_named_metric(split_metrics, "defensive_style_market_outcome_count")
    if market_return <= market_return_threshold:
        _increment_named_metric(metrics, f"defensive_style_market_down_count_{suffix}")
        _increment_named_metric(split_metrics, f"defensive_style_market_down_count_{suffix}")
        if update_primary_aliases and int(horizon_days) == int(primary_horizon_days):
            _increment_named_metric(metrics, "defensive_style_market_down_count")
            _increment_named_metric(split_metrics, "defensive_style_market_down_count")


def _increment_named_metric(target: dict[str, int], key: str, amount: int = 1) -> None:
    target[key] = int(target.get(key) or 0) + amount


def _decision_degradation_payload(row: dict[str, Any]) -> dict[str, Any]:
    direct = row.get("decision_degradation")
    if isinstance(direct, dict):
        return direct
    payload = row.get("observation_payload") if isinstance(row.get("observation_payload"), dict) else {}
    degradation = payload.get("decision_degradation")
    return degradation if isinstance(degradation, dict) else {}


def _is_degraded_observation(row: dict[str, Any]) -> bool:
    return bool(_decision_degradation_payload(row).get("is_degraded"))


def _is_degraded_record(row: dict[str, Any]) -> bool:
    return bool(_decision_degradation_payload(row).get("is_degraded"))


def _artifact_observation_date(row: dict[str, Any]) -> Any:
    return (
        row.get("as_of_time")
        or row.get("created_at")
        or row.get("observation_date")
        or row.get("data_time")
    )


def _is_defensive_style_event(row: dict[str, Any]) -> bool:
    limits = row.get("style_limits") if isinstance(row.get("style_limits"), dict) else {}
    return bool(
        row.get("defensive_style")
        or str(row.get("analysis_style") or "") == "macro_defensive"
        or str(row.get("trade_style") or "") in {"risk_reduce_fast", "cash_only"}
        or limits.get("allow_new_positions") is False
    )


def _blocked_new_positions(row: dict[str, Any]) -> list[str]:
    raw = row.get("blocked_new_positions") or []
    if isinstance(raw, str):
        raw = [raw]
    out: list[str] = []
    seen: set[str] = set()
    for item in raw:
        ticker = str(item or "").upper().strip()
        if ticker and ticker not in seen:
            seen.add(ticker)
            out.append(ticker)
    if out:
        return out
    enforcement = row.get("style_enforcement") if isinstance(row.get("style_enforcement"), dict) else {}
    for item in enforcement.get("violations") or []:
        text = str(item)
        if not text.startswith("style_new_position_blocked:"):
            continue
        ticker = text.split(":", 1)[1].split(" ", 1)[0].upper().strip()
        if ticker and ticker not in seen:
            seen.add(ticker)
            out.append(ticker)
    return out


def _dataset_dict(dataset: WeekendReviewDataset | dict[str, Any]) -> dict[str, Any]:
    if isinstance(dataset, WeekendReviewDataset):
        return dataset.to_dict()
    return dict(dataset or {})


def _dedupe_command_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_id: dict[str, dict[str, Any]] = {}
    anonymous: list[dict[str, Any]] = []
    for row in rows:
        command_id = str(row.get("command_id") or "").strip()
        if not command_id:
            anonymous.append(row)
            continue
        current = by_id.get(command_id)
        if current is None or _event_sort_key(row) >= _event_sort_key(current):
            by_id[command_id] = row
    return [*by_id.values(), *anonymous]


def _merge_execution_rows_with_lifecycle_events(
    rows: list[dict[str, Any]],
    event_summary: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        command_id = str(row.get("command_id") or "").strip()
        summary = event_summary.get(command_id)
        if not summary:
            merged.append(row)
            if command_id:
                seen.add(command_id)
            continue
        enriched = dict(row)
        enriched["event_state"] = summary.get("state")
        enriched["event_types"] = summary.get("event_types") or []
        enriched["event_statuses"] = summary.get("event_statuses") or []
        enriched["event_time"] = summary.get("event_time")
        if summary.get("state"):
            enriched["lifecycle_state"] = summary["state"]
        if summary.get("event_time") and not enriched.get("latest_qc_ack_at"):
            enriched["latest_qc_ack_at"] = summary["event_time"]
        merged.append(enriched)
        if command_id:
            seen.add(command_id)

    for command_id, summary in event_summary.items():
        if command_id in seen:
            continue
        merged.append({
            "command_id": command_id,
            "command_type": "weight_adjustment",
            "lifecycle_state": summary.get("state") or "unknown",
            "event_state": summary.get("state"),
            "event_types": summary.get("event_types") or [],
            "event_statuses": summary.get("event_statuses") or [],
            "event_time": summary.get("event_time"),
            "latest_qc_ack_at": summary.get("event_time"),
            "command_payload": {},
        })
    return merged


def _command_lifecycle_event_summary(events: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    summary: dict[str, dict[str, Any]] = {}
    for event in events:
        command_id = str(event.get("command_id") or "").strip()
        if not command_id:
            continue
        state = _state_from_lifecycle_event(event)
        event_type = str(event.get("event_type") or "").lower().strip()
        event_time = _parse_datetime(event.get("event_time"))
        bucket = summary.setdefault(command_id, {
            "state": "",
            "priority": -1,
            "event_time": None,
            "event_types": [],
            "event_statuses": [],
        })
        if event_type and event_type not in bucket["event_types"]:
            bucket["event_types"].append(event_type)
        event_status = str(event.get("event_status") or "").lower().strip()
        if event_status and event_status not in bucket["event_statuses"]:
            bucket["event_statuses"].append(event_status)
        existing_time = _parse_datetime(bucket.get("event_time"))
        if not state:
            if event_time is not None and (
                existing_time is None
                or _ensure_utc(event_time) > _ensure_utc(existing_time)
            ):
                bucket["event_time"] = event_time.isoformat()
            continue
        mapped_state, priority = state
        should_replace = priority > int(bucket.get("priority") or -1)
        if priority == int(bucket.get("priority") or -1) and event_time and existing_time:
            should_replace = _ensure_utc(event_time) >= _ensure_utc(existing_time)
        if should_replace:
            bucket["state"] = mapped_state
            bucket["priority"] = priority
            bucket["event_time"] = event_time.isoformat() if event_time else bucket.get("event_time")
        elif event_time is not None and existing_time is None:
            bucket["event_time"] = event_time.isoformat()
    return summary


def _state_from_lifecycle_event(event: dict[str, Any]) -> tuple[str, int] | None:
    event_type = str(event.get("event_type") or "").lower().strip()
    event_status = str(event.get("event_status") or "").lower().strip()
    if event_type == "execution_result" and event_status:
        if event_status == "deduped":
            return ("deduped", 50)
        if event_status in {"accepted", "rejected", "timeout_no_ack"}:
            return (_normalize_execution_state(event_status), 50)
    return EVENT_STATE_PRIORITY.get(event_type)


def _event_types(row: dict[str, Any]) -> list[str]:
    values = row.get("event_types") if isinstance(row.get("event_types"), list) else []
    return [str(value or "").lower().strip() for value in values if str(value or "").strip()]


def _event_statuses(row: dict[str, Any]) -> list[str]:
    values = row.get("event_statuses") if isinstance(row.get("event_statuses"), list) else []
    return [str(value or "").lower().strip() for value in values if str(value or "").strip()]


def _apply_lifecycle_event_intent_fallback(
    events: list[dict[str, Any]],
    *,
    observed_commands: set[str],
    metrics: dict[str, int],
    blocker_distribution: dict[str, int],
    unexecuted_intents: list[dict[str, Any]],
) -> None:
    by_command: dict[str, list[dict[str, Any]]] = {}
    for event in events or []:
        command_id = str(event.get("command_id") or "").strip()
        if not command_id or command_id in observed_commands:
            continue
        by_command.setdefault(command_id, []).append(event)

    for command_id, command_events in by_command.items():
        event_types = {str(event.get("event_type") or "").lower().strip() for event in command_events}
        event_statuses = {str(event.get("event_status") or "").lower().strip() for event in command_events}
        if "preflight_blocked" in event_types:
            metrics["execution_preflight_block_count"] += 1
            _count_distribution(blocker_distribution, "execution_preflight")
            unexecuted_intents.append({
                "command_id": command_id,
                "intent": "blocked_by_lifecycle_preflight",
            })
            for category in _preflight_categories_from_lifecycle_events(command_events):
                _count_distribution(blocker_distribution, category)
                if category == "execution_daily_cap":
                    metrics["daily_command_cap_block_count"] += 1
                elif category == "execution_turnover_cap":
                    metrics["daily_turnover_cap_block_count"] += 1

        if "execution_result" in event_types and "deduped" in event_statuses:
            metrics["dedupe_count"] += 1
            _count_distribution(blocker_distribution, "execution_dedupe")
            unexecuted_intents.append({
                "command_id": command_id,
                "intent": "not_sent:lifecycle_deduped",
            })

        if "qc_timeout" in event_types or "timeout_no_ack" in event_statuses:
            metrics["execution_timeout_count"] += 1
            _count_distribution(blocker_distribution, "execution_feedback_timeout")

        if "qc_rejected" in event_types:
            metrics["qc_reject_count"] += 1
            _count_distribution(blocker_distribution, "qc_rejected")


def _preflight_categories_from_lifecycle_events(events: list[dict[str, Any]]) -> set[str]:
    categories: set[str] = set()
    for event in events:
        payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
        audit_payload = payload.get("audit_payload") if isinstance(payload.get("audit_payload"), dict) else {}
        for blocker in _preflight_blocker_codes(audit_payload):
            category = PREFLIGHT_BLOCKER_CATEGORY.get(blocker)
            if category:
                categories.add(category)
    return categories


def _preflight_blocker_codes(audit_payload: dict[str, Any]) -> set[str]:
    codes: set[str] = set()
    containers = [audit_payload]
    for key in ("command_preflight", "preflight_result", "execution_preflight"):
        value = audit_payload.get(key)
        if isinstance(value, dict):
            containers.append(value)

    for container in containers:
        blockers = container.get("blockers")
        if isinstance(blockers, list):
            codes.update(str(item or "").lower().strip() for item in blockers if str(item or "").strip())

        checks = container.get("checks")
        if isinstance(checks, dict):
            for code, check in checks.items():
                if _preflight_check_failed(check):
                    clean_code = str(code or "").lower().strip()
                    if clean_code:
                        codes.add(clean_code)

        blocker_events = container.get("blocker_events")
        if isinstance(blocker_events, list):
            for event in blocker_events:
                if not isinstance(event, dict):
                    continue
                clean_code = str(event.get("code") or "").lower().strip()
                if clean_code:
                    codes.add(clean_code)
    return codes


def _preflight_check_failed(check: Any) -> bool:
    if not isinstance(check, dict):
        return False
    if "pass" in check:
        return check.get("pass") is False
    if "passed" in check:
        return check.get("passed") is False
    if "ok" in check:
        return check.get("ok") is False
    return False


def _count_distribution(target: dict[str, int], key: str) -> None:
    target[key] = target.get(key, 0) + 1


def _execution_state(row: dict[str, Any]) -> str:
    lifecycle = _normalize_execution_state(row.get("lifecycle_state"))
    if lifecycle and lifecycle != "created":
        return lifecycle

    # Legacy rows often keep lifecycle_state at the default "created" while
    # qc_status/status carry the real outcome. Prefer explicit not-sent/dedupe
    # statuses before QC ownership states so old dedupe rows do not count as
    # commands sent.
    status = _normalize_execution_state(row.get("status"))
    qc_status = _normalize_execution_state(row.get("qc_status"))
    if status == "deduped":
        return status
    if qc_status:
        return qc_status
    if status:
        return status
    return "unknown"


def _is_deduped_execution_row(row: dict[str, Any]) -> bool:
    state = _execution_state(row)
    if state in {"deduped", "duplicate_target"}:
        return True

    event_types = set(_event_types(row))
    event_statuses = set(_event_statuses(row))
    if "execution_result" in event_types and "deduped" in event_statuses:
        return True

    payload = row.get("command_payload") if isinstance(row.get("command_payload"), dict) else {}
    qc_response = row.get("qc_response") if isinstance(row.get("qc_response"), dict) else {}
    for source in (row, payload, qc_response):
        if str(source.get("action_status") or "").lower().strip() == "deduped":
            return True
        reason = str(source.get("not_sent_reason") or source.get("reason") or "").lower().strip()
        if reason in {"deduped", "recent_same_target_reconciled", "same_target_deduped"}:
            return True
        same_target = source.get("same_target_dedupe")
        if isinstance(same_target, dict):
            dedupe_reason = str(same_target.get("reason") or "").lower().strip()
            should_send = same_target.get("should_send")
            if dedupe_reason == "recent_same_target_reconciled" or should_send is False:
                return True
    return False


def _normalize_execution_state(value: Any) -> str:
    state = str(value or "").lower().strip()
    if not state:
        return ""
    aliases = {
        "success": "filled",
    }
    return aliases.get(state, state)


def _is_noop(row: dict[str, Any]) -> bool:
    payload = row.get("command_payload") if isinstance(row.get("command_payload"), dict) else {}
    qc_response = row.get("qc_response") if isinstance(row.get("qc_response"), dict) else {}
    for source in (payload, qc_response):
        summary = source.get("order_summary") if isinstance(source.get("order_summary"), dict) else {}
        if summary.get("is_noop") is True or summary.get("execution_state") == "noop_reconciled":
            return True
    return _execution_state(row) == "noop_reconciled"


def _is_stuck_in_flight(row: dict[str, Any], review_as_of: datetime, timeout_minutes: int) -> bool:
    event_time = _event_time(row)
    if event_time is None:
        return False
    return (_ensure_utc(review_as_of) - _ensure_utc(event_time)) > timedelta(minutes=timeout_minutes)


def _event_time(row: dict[str, Any]) -> datetime | None:
    for key in ("event_time", "latest_qc_ack_at", "qc_ack_at", "submitted_at", "executed_at"):
        parsed = _parse_datetime(row.get(key))
        if parsed is not None:
            return parsed
    return None


def _event_sort_key(row: dict[str, Any]) -> float:
    event_time = _event_time(row)
    if event_time is None:
        return 0.0
    return _ensure_utc(event_time).timestamp()


def _week_bucket(row: dict[str, Any]) -> str | None:
    event_time = (
        _parse_datetime(row.get("submitted_at"))
        or _parse_datetime(row.get("executed_at"))
        or _parse_datetime(row.get("event_time"))
    )
    if event_time is None:
        return None
    day = _ensure_utc(event_time).date()
    start = day - timedelta(days=day.weekday())
    end = start + timedelta(days=6)
    return f"{start.isoformat()}..{end.isoformat()}"


def _bool_path(row: dict[str, Any], *keys: str) -> bool:
    value: Any = row
    for key in keys:
        if not isinstance(value, dict):
            return False
        value = value.get(key)
    return bool(value)


def _contains_any(row: dict[str, Any], needles: set[str]) -> bool:
    text = str(row).lower()
    return any(needle.lower() in text for needle in needles)


def _blocker_categories(events: list[Any]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for event in events:
        if not isinstance(event, dict):
            continue
        category = str(event.get("category") or event.get("blocker_category") or "unknown").strip()
        if category:
            counts[category] = counts.get(category, 0) + 1
    return counts


def _any_contains(values: set[str], needle: str) -> bool:
    needle = needle.lower()
    return any(needle in value.lower() for value in values)


def _horizon_key(value: Any) -> str | None:
    text = str(value or "").lower().strip()
    if text in {"1d", "5d", "20d"}:
        return text
    return _horizon_from_days(value)


def _horizon_from_days(value: Any) -> str | None:
    days = _int(value, default=-1)
    if days in {1, 5, 20}:
        return f"{days}d"
    return None


def _hedge_samples(data: dict[str, Any]) -> list[dict[str, Any]]:
    samples: list[dict[str, Any]] = []
    seen: set[str] = set()
    rows = list(data.get("validation_observations") or [])

    for row in rows:
        if row.get("observation_type") != "hedge_intent":
            continue
        payload = row.get("observation_payload") if isinstance(row.get("observation_payload"), dict) else {}
        raw = payload.get("hedge_intent_outcome") if isinstance(payload.get("hedge_intent_outcome"), dict) else payload
        sample = {
            "observation_date": row.get("observation_date") or raw.get("date"),
            "triggered": raw.get("triggered"),
            "severity": raw.get("severity"),
            "add_hedge_etf": raw.get("add_hedge_etf"),
            "selected_instrument": raw.get("selected_instrument"),
            "candidate_hedge_instrument": raw.get("candidate_hedge_instrument"),
            "decision_degraded": _is_degraded_record(row),
        }
        key = _hedge_sample_key(row, sample)
        seen.add(key)
        samples.append(sample)

    for row in rows:
        if row.get("observation_type") != "intent_vs_execution":
            continue
        payload = row.get("observation_payload") if isinstance(row.get("observation_payload"), dict) else {}
        hedge = payload.get("hedge_intent") if isinstance(payload.get("hedge_intent"), dict) else {}
        if hedge:
            sample = {
                "observation_date": row.get("observation_date"),
                "triggered": hedge.get("triggered"),
                "severity": hedge.get("severity"),
                "add_hedge_etf": hedge.get("add_hedge_etf"),
                "selected_instrument": hedge.get("selected_instrument"),
                "candidate_hedge_instrument": hedge.get("candidate_hedge_instrument"),
                "decision_degraded": _is_degraded_record(row),
            }
            key = _hedge_sample_key(row, sample)
            if key in seen:
                continue
            seen.add(key)
            samples.append(sample)
    return samples


def _hedge_sample_key(row: dict[str, Any], sample: dict[str, Any]) -> str:
    return str(
        row.get("analysis_id")
        or row.get("command_id")
        or sample.get("observation_date")
        or row.get("observation_id")
        or ""
    )


def _market_forward_return(
    feature_rows: list[dict[str, Any]],
    *,
    observation_date: Any,
    horizon_days: int,
) -> float | None:
    returns: list[float] = []
    for ticker in ("SPY", "QQQ"):
        result = _forward_return_from_prices(feature_rows, ticker=ticker, observation_date=observation_date, horizon_days=horizon_days)
        if result is not None:
            returns.append(result)
    if not returns:
        return None
    return min(returns)


def _forward_return_from_prices(
    rows: list[dict[str, Any]],
    *,
    ticker: str,
    observation_date: Any,
    horizon_days: int,
) -> float | None:
    obs_date = _parse_date(observation_date)
    if obs_date is None:
        return None
    prices = _feature_prices(rows, ticker)
    path = [row for row in prices if row["trading_date"] >= obs_date]
    if len(path) <= int(horizon_days):
        return None
    start = path[0]
    end = path[int(horizon_days)]
    if start["price"] <= 0:
        return None
    return end["price"] / start["price"] - 1.0


def _feature_prices(rows: list[dict[str, Any]], ticker: str) -> list[dict[str, Any]]:
    clean = str(ticker or "").upper().strip()
    out: list[dict[str, Any]] = []
    for row in rows:
        if str(row.get("ticker") or "").upper().strip() != clean:
            continue
        trading_date = _parse_date(row.get("trading_date"))
        price = _float(row.get("price", row.get("adj_close_price", row.get("close_price"))))
        if trading_date is None or price is None or price <= 0:
            continue
        out.append({
            "trading_date": trading_date,
            "price": price,
            "source": row.get("source") or "unknown",
        })
    return sorted(out, key=lambda item: item["trading_date"])


def _avg(values: list[float]) -> float | None:
    clean = [float(value) for value in values if value is not None]
    if not clean:
        return None
    return sum(clean) / len(clean)


def _float(value: Any, default: float | None = None) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _int(value: Any, *, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str) and value.strip():
        text = value.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            return datetime.fromisoformat(text)
        except ValueError:
            return None
    return None


def _parse_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str) and value.strip():
        try:
            return date.fromisoformat(value[:10])
        except ValueError:
            return None
    return None


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
