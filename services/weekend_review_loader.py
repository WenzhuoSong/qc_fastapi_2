"""Authority-gated loader for weekend trading reviews.

The weekend review loop must not grade the system from raw, unversioned JSON.
This module builds a review dataset from already-durable records and rejects
non-authoritative inputs before PR1 metrics consume anything.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any

from services.json_safety import json_safe
from services.training_data_authority import evaluate_training_data_source


LOADER_CONTRACT_VERSION = "weekend_review_loader_v1"
EXECUTION_AUTHORITY = "none"
TARGET_WEIGHT_MUTATION = "none"


@dataclass
class WeekendReviewDataset:
    """Pure data container produced by the PR0 authority gate."""

    contract_version: str = LOADER_CONTRACT_VERSION
    execution_authority: str = EXECUTION_AUTHORITY
    target_weight_mutation: str = TARGET_WEIGHT_MUTATION
    validation_observations: list[dict[str, Any]] = field(default_factory=list)
    diagnostic_artifacts: list[dict[str, Any]] = field(default_factory=list)
    execution_logs: list[dict[str, Any]] = field(default_factory=list)
    command_lifecycle_events: list[dict[str, Any]] = field(default_factory=list)
    account_snapshots: list[dict[str, Any]] = field(default_factory=list)
    market_features: list[dict[str, Any]] = field(default_factory=list)
    outcome_labels: list[dict[str, Any]] = field(default_factory=list)
    excluded_inputs: list[dict[str, Any]] = field(default_factory=list)
    source_counts: dict[str, int] = field(default_factory=dict)
    exclusion_counts: dict[str, int] = field(default_factory=dict)
    fallback_label_count: int = 0
    mixed_feature_authority_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        return json_safe({
            "contract_version": self.contract_version,
            "execution_authority": self.execution_authority,
            "target_weight_mutation": self.target_weight_mutation,
            "validation_observations": self.validation_observations,
            "diagnostic_artifacts": self.diagnostic_artifacts,
            "execution_logs": self.execution_logs,
            "command_lifecycle_events": self.command_lifecycle_events,
            "account_snapshots": self.account_snapshots,
            "market_features": self.market_features,
            "outcome_labels": self.outcome_labels,
            "excluded_inputs": self.excluded_inputs,
            "source_counts": self.source_counts,
            "exclusion_counts": self.exclusion_counts,
            "fallback_label_count": self.fallback_label_count,
            "mixed_feature_authority_count": self.mixed_feature_authority_count,
        })


def build_weekend_review_dataset(
    *,
    validation_observations: list[Any] | None = None,
    diagnostic_artifacts: list[Any] | None = None,
    agent_analyses: list[Any] | None = None,
    execution_logs: list[Any] | None = None,
    command_lifecycle_events: list[Any] | None = None,
    account_snapshots: list[Any] | None = None,
    market_features: list[Any] | None = None,
    outcome_labels: list[Any] | None = None,
    legacy_records: list[Any] | None = None,
) -> WeekendReviewDataset:
    """Build a review dataset from caller-provided rows without DB access."""
    dataset = WeekendReviewDataset()

    for row in validation_observations or []:
        _add_validation_observation(dataset, row)

    for row in diagnostic_artifacts or []:
        _add_diagnostic_artifact(dataset, _record_dict(row))

    for analysis in agent_analyses or []:
        for artifact in _diagnostic_artifacts_from_analysis(analysis):
            _add_diagnostic_artifact(dataset, artifact)

    for row in execution_logs or []:
        _add_execution_log(dataset, row)

    for row in command_lifecycle_events or []:
        _add_command_lifecycle_event(dataset, row)

    for row in account_snapshots or []:
        _add_account_snapshot(dataset, row)

    for row in market_features or []:
        _add_market_feature(dataset, row)

    for row in outcome_labels or []:
        _add_outcome_label(dataset, row)

    for row in legacy_records or []:
        payload = _record_dict(row)
        source_type = str(payload.get("source_type") or "legacy_json")
        verdict = evaluate_training_data_source(source_type=source_type, payload=payload)
        _exclude(dataset, source_type, payload, verdict["reasons"] or ["legacy_not_authoritative"])

    return dataset


async def load_weekend_review_dataset(
    *,
    week_start: date | None = None,
    week_end: date | None = None,
    limit: int = 500,
) -> WeekendReviewDataset:
    """Load recent records from DB through the PR0 authority gate."""
    from sqlalchemy import desc, select

    from db.models import (
        AccountStateSnapshot,
        AgentAnalysis,
        CommandLifecycleEvent,
        ExecutionLog,
        MarketDailyFeature,
        ValidationObservation,
    )
    from db.session import AsyncSessionLocal

    start, end = _week_window(week_start=week_start, week_end=week_end)
    async with AsyncSessionLocal() as db:
        observations = list((
            await db.execute(
                select(ValidationObservation)
                .where(ValidationObservation.observation_date >= start)
                .where(ValidationObservation.observation_date <= end)
                .order_by(desc(ValidationObservation.observed_at), desc(ValidationObservation.id))
                .limit(limit)
            )
        ).scalars().all())
        analyses = list((
            await db.execute(
                select(AgentAnalysis)
                .where(AgentAnalysis.analyzed_at >= datetime.combine(start, datetime.min.time()))
                .where(AgentAnalysis.analyzed_at <= datetime.combine(end, datetime.max.time()))
                .order_by(desc(AgentAnalysis.analyzed_at), desc(AgentAnalysis.id))
                .limit(limit)
            )
        ).scalars().all())
        executions = list((
            await db.execute(
                select(ExecutionLog)
                .where(ExecutionLog.executed_at >= datetime.combine(start, datetime.min.time()))
                .where(ExecutionLog.executed_at <= datetime.combine(end, datetime.max.time()))
                .order_by(desc(ExecutionLog.executed_at), desc(ExecutionLog.id))
                .limit(limit)
            )
        ).scalars().all())
        lifecycle_events = list((
            await db.execute(
                select(CommandLifecycleEvent)
                .where(CommandLifecycleEvent.event_time >= datetime.combine(start, datetime.min.time()))
                .where(CommandLifecycleEvent.event_time <= datetime.combine(end, datetime.max.time()))
                .order_by(desc(CommandLifecycleEvent.event_time), desc(CommandLifecycleEvent.id))
                .limit(limit * 10)
            )
        ).scalars().all())
        snapshots = list((
            await db.execute(
                select(AccountStateSnapshot)
                .where(AccountStateSnapshot.recorded_at >= datetime.combine(start, datetime.min.time()))
                .where(AccountStateSnapshot.recorded_at <= datetime.combine(end, datetime.max.time()))
                .order_by(desc(AccountStateSnapshot.recorded_at), desc(AccountStateSnapshot.id))
                .limit(limit)
            )
        ).scalars().all())
        features = list((
            await db.execute(
                select(MarketDailyFeature)
                .where(MarketDailyFeature.trading_date >= start)
                .where(MarketDailyFeature.trading_date <= end)
                .order_by(MarketDailyFeature.trading_date, MarketDailyFeature.ticker, MarketDailyFeature.source)
                .limit(limit * 20)
            )
        ).scalars().all())

    return build_weekend_review_dataset(
        validation_observations=observations,
        agent_analyses=analyses,
        execution_logs=executions,
        command_lifecycle_events=lifecycle_events,
        account_snapshots=snapshots,
        market_features=features,
    )


def _add_validation_observation(dataset: WeekendReviewDataset, row: Any) -> None:
    payload = _validation_observation_payload(row)
    verdict = evaluate_training_data_source(source_type="validation_observation", payload=payload)
    if verdict["allowed"]:
        dataset.validation_observations.append(payload)
        _count(dataset.source_counts, "validation_observation")
    else:
        _exclude(dataset, "validation_observation", payload, verdict["reasons"])


def _add_diagnostic_artifact(dataset: WeekendReviewDataset, artifact: dict[str, Any]) -> None:
    verdict = evaluate_training_data_source(source_type="diagnostic_artifact", payload=artifact)
    if verdict["allowed"]:
        dataset.diagnostic_artifacts.append(json_safe(artifact))
        _count(dataset.source_counts, "diagnostic_artifact")
        if artifact.get("training_authority") == "feature_scope_limited":
            for reason in artifact.get("scope_limit_reasons") or []:
                if reason == "mixed_feature_authority":
                    dataset.mixed_feature_authority_count += 1
    else:
        _exclude(dataset, "diagnostic_artifact", artifact, verdict["reasons"])


def _add_execution_log(dataset: WeekendReviewDataset, row: Any) -> None:
    payload = _execution_log_payload(row)
    reasons: list[str] = []
    if not payload.get("command_id"):
        reasons.append("missing_command_id")
    if not isinstance(payload.get("command_payload"), dict):
        reasons.append("missing_command_payload")
    if not payload.get("command_type"):
        reasons.append("missing_command_type")
    if not (payload.get("status") or payload.get("qc_status") or payload.get("lifecycle_state")):
        reasons.append("missing_execution_state")
    if reasons:
        _exclude(dataset, "execution_log", payload, reasons)
        return
    dataset.execution_logs.append(json_safe(payload))
    _count(dataset.source_counts, "execution_log")


def _add_command_lifecycle_event(dataset: WeekendReviewDataset, row: Any) -> None:
    payload = _command_lifecycle_event_payload(row)
    reasons: list[str] = []
    if not payload.get("command_id"):
        reasons.append("missing_command_id")
    if not payload.get("event_type"):
        reasons.append("missing_event_type")
    if not payload.get("event_time"):
        reasons.append("missing_event_time")
    if reasons:
        _exclude(dataset, "command_lifecycle_event", payload, reasons)
        return
    dataset.command_lifecycle_events.append(json_safe(payload))
    _count(dataset.source_counts, "command_lifecycle_event")


def _add_account_snapshot(dataset: WeekendReviewDataset, row: Any) -> None:
    payload = _account_snapshot_payload(row)
    reasons: list[str] = []
    if not payload.get("id"):
        reasons.append("missing_snapshot_id")
    if not payload.get("recorded_at"):
        reasons.append("missing_recorded_at")
    if not payload.get("source_packet_type"):
        reasons.append("missing_source_packet_type")
    if not payload.get("contract_version"):
        reasons.append("missing_contract_version")
    if reasons:
        _exclude(dataset, "account_state_snapshot", payload, reasons)
        return
    dataset.account_snapshots.append(json_safe(payload))
    _count(dataset.source_counts, "account_state_snapshot")


def _add_market_feature(dataset: WeekendReviewDataset, row: Any) -> None:
    payload = _market_feature_payload(row)
    reasons: list[str] = []
    if not payload.get("ticker"):
        reasons.append("missing_ticker")
    if not payload.get("trading_date"):
        reasons.append("missing_trading_date")
    if not payload.get("source"):
        reasons.append("missing_source")
    if payload.get("price") is None:
        reasons.append("missing_price")
    if reasons:
        _exclude(dataset, "market_daily_feature", payload, reasons)
        return
    dataset.market_features.append(json_safe(payload))
    _count(dataset.source_counts, "market_daily_feature")


def _add_outcome_label(dataset: WeekendReviewDataset, row: Any) -> None:
    payload = _record_dict(row)
    verdict = evaluate_training_data_source(source_type="outcome_label", payload=payload)
    if verdict["allowed"]:
        dataset.outcome_labels.append(json_safe(payload))
        _count(dataset.source_counts, "outcome_label")
    else:
        if _is_fallback_label(payload):
            dataset.fallback_label_count += 1
        _exclude(dataset, "outcome_label", payload, verdict["reasons"])


def _diagnostic_artifacts_from_analysis(row: Any) -> list[dict[str, Any]]:
    risk = _record_get(row, "risk_output")
    if not isinstance(risk, dict):
        return []
    artifacts = risk.get("diagnostic_artifacts")
    if not isinstance(artifacts, list):
        return []
    return [_record_dict(item) for item in artifacts if isinstance(item, dict)]


def _validation_observation_payload(row: Any) -> dict[str, Any]:
    return {
        "id": _record_get(row, "id"),
        "observation_id": _record_get(row, "observation_id"),
        "observation_type": _record_get(row, "observation_type"),
        "analysis_id": _record_get(row, "analysis_id"),
        "command_id": _record_get(row, "command_id"),
        "observed_at": _iso_or_none(_record_get(row, "observed_at")),
        "observation_date": _date_or_none(_record_get(row, "observation_date")),
        "horizon_days": _int_or_none(_record_get(row, "horizon_days")),
        "maturity_date": _date_or_none(_record_get(row, "maturity_date")),
        "status": _record_get(row, "status"),
        "execution_authority": _record_get(row, "execution_authority"),
        "target_weight_mutation": _record_get(row, "target_weight_mutation"),
        "observation_payload": _record_get(row, "observation_payload") or {},
        "outcome_payload": _record_get(row, "outcome_payload"),
        "metrics": _record_get(row, "metrics") or {},
        "recommendation": _record_get(row, "recommendation") or {},
        "content_hash": _record_get(row, "content_hash"),
    }


def _execution_log_payload(row: Any) -> dict[str, Any]:
    return {
        "id": _record_get(row, "id"),
        "analysis_id": _record_get(row, "analysis_id"),
        "command_id": _record_get(row, "command_id"),
        "correlation_id": _record_get(row, "correlation_id"),
        "command_type": _record_get(row, "command_type"),
        "policy_version": _record_get(row, "policy_version"),
        "target_fingerprint": _record_get(row, "target_fingerprint"),
        "lifecycle_state": _record_get(row, "lifecycle_state"),
        "executed_at": _iso_or_none(_record_get(row, "executed_at")),
        "submitted_at": _iso_or_none(_record_get(row, "submitted_at")),
        "latest_qc_ack_at": _iso_or_none(_record_get(row, "latest_qc_ack_at")),
        "qc_ack_at": _iso_or_none(_record_get(row, "qc_ack_at")),
        "command_payload": _record_get(row, "command_payload") or {},
        "qc_response": _record_get(row, "qc_response") or {},
        "status": _record_get(row, "status"),
        "qc_status": _record_get(row, "qc_status"),
        "qc_rejection_reason": _record_get(row, "qc_rejection_reason"),
    }


def _command_lifecycle_event_payload(row: Any) -> dict[str, Any]:
    return {
        "id": _record_get(row, "id"),
        "analysis_id": _record_get(row, "analysis_id"),
        "command_id": _record_get(row, "command_id"),
        "event_type": _record_get(row, "event_type"),
        "event_status": _record_get(row, "event_status"),
        "event_time": _iso_or_none(_record_get(row, "event_time")),
        "source": _record_get(row, "source"),
        "reason": _record_get(row, "reason"),
        "payload": _record_get(row, "payload") or {},
        "created_at": _iso_or_none(_record_get(row, "created_at")),
    }


def _account_snapshot_payload(row: Any) -> dict[str, Any]:
    return {
        "id": _record_get(row, "id"),
        "qc_snapshot_id": _record_get(row, "qc_snapshot_id"),
        "recorded_at": _iso_or_none(_record_get(row, "recorded_at")),
        "account_timestamp": _iso_or_none(_record_get(row, "account_timestamp")),
        "source_packet_type": _record_get(row, "source_packet_type"),
        "contract_version": _record_get(row, "contract_version"),
        "account_status": _record_get(row, "account_status"),
        "data_status": _record_get(row, "data_status"),
        "policy_version": _record_get(row, "policy_version"),
        "total_value": _float_or_none(_record_get(row, "total_value")),
        "cash_pct": _float_or_none(_record_get(row, "cash_pct")),
        "open_order_count": _int_or_none(_record_get(row, "open_order_count")),
        "has_open_orders": _record_get(row, "has_open_orders"),
        "is_market_open": _record_get(row, "is_market_open"),
        "last_command_id": _record_get(row, "last_command_id"),
        "holdings_weights": _record_get(row, "holdings_weights") or {},
        "target_weights": _record_get(row, "target_weights") or {},
    }


def _market_feature_payload(row: Any) -> dict[str, Any]:
    price = _first_float(
        _record_get(row, "adj_close_price"),
        _record_get(row, "close_price"),
        _record_get(row, "price"),
    )
    return {
        "id": _record_get(row, "id"),
        "trading_date": _date_or_none(_record_get(row, "trading_date")),
        "ticker": str(_record_get(row, "ticker") or "").upper().strip(),
        "source": _record_get(row, "source"),
        "price": price,
        "adj_close_price": _float_or_none(_record_get(row, "adj_close_price")),
        "close_price": _float_or_none(_record_get(row, "close_price")),
        "return_1d": _float_or_none(_record_get(row, "return_1d")),
        "return_5d": _float_or_none(_record_get(row, "return_5d")),
        "return_20d": _float_or_none(_record_get(row, "return_20d")),
    }


def _exclude(dataset: WeekendReviewDataset, source_type: str, payload: dict[str, Any], reasons: list[str]) -> None:
    clean_reasons = sorted({str(reason) for reason in reasons if str(reason)})
    if not clean_reasons:
        clean_reasons = ["not_authoritative"]
    dataset.excluded_inputs.append(json_safe({
        "source_type": source_type,
        "reasons": clean_reasons,
        "payload_ref": _payload_ref(payload),
    }))
    for reason in clean_reasons:
        _count(dataset.exclusion_counts, reason)


def _is_fallback_label(payload: dict[str, Any]) -> bool:
    source_metadata = payload.get("source_metadata") if isinstance(payload.get("source_metadata"), dict) else {}
    scope_limit_reasons = payload.get("scope_limit_reasons") if isinstance(payload.get("scope_limit_reasons"), list) else []
    return (
        payload.get("label_schema_version") == "outcome_label_v1"
        and (
            source_metadata.get("label_source_role") == "fallback"
            or "fallback_label_source" in scope_limit_reasons
        )
    )


def _payload_ref(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        key: payload.get(key)
        for key in (
            "id",
            "analysis_id",
            "observation_id",
            "observation_type",
            "artifact_id",
            "schema_version",
            "command_id",
            "ticker",
            "source",
            "label_schema_version",
        )
        if payload.get(key) is not None
    }


def _week_window(*, week_start: date | None, week_end: date | None) -> tuple[date, date]:
    if week_start and week_end:
        return week_start, week_end
    today = datetime.now(timezone.utc).date()
    start = week_start or (today - timedelta(days=today.weekday()))
    end = week_end or (start + timedelta(days=6))
    return start, end


def _count(target: dict[str, int], key: str) -> None:
    target[key] = target.get(key, 0) + 1


def _record_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if value is None:
        return {}
    keys = (
        "id",
        "analysis_id",
        "observation_id",
        "observation_type",
        "artifact_id",
        "schema_version",
        "execution_authority",
        "target_weight_mutation",
        "training_authority",
        "scope_limit_reasons",
        "source_type",
        "label_schema_version",
        "source_metadata",
    )
    return {key: getattr(value, key) for key in keys if hasattr(value, key)}


def _record_get(value: Any, key: str) -> Any:
    if isinstance(value, dict):
        return value.get(key)
    return getattr(value, key, None)


def _iso_or_none(value: Any) -> str | None:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, str) and value.strip():
        return value
    return None


def _date_or_none(value: Any) -> str | None:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, str) and value.strip():
        return value[:10]
    return None


def _float_or_none(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _first_float(*values: Any) -> float | None:
    for value in values:
        parsed = _float_or_none(value)
        if parsed is not None:
            return parsed
    return None
