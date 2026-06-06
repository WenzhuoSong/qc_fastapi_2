"""Observe-only validation data loop.

This module makes strategy/basket/hedge/execution calibration durable. It
records what the system believed at decision time, then backfills mature
outcomes where possible. It never mutates targets or changes execution
authority.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any

from services.hedge_intent_outcome_log import (
    OUTCOME_COMPLETED,
    OUTCOME_PENDING,
    backfill_hedge_intent_outcome,
    summarize_hedge_threshold_assessments,
)
from services.json_safety import json_safe
from services.outcome_label_policy import outcome_label_contract_summary


CONTRACT_VERSION = "validation_observation_loop_v1"
EXECUTION_AUTHORITY = "none"
TARGET_WEIGHT_MUTATION = "none"
OBS_HEDGE_INTENT = "hedge_intent"
OBS_ACTIVE_BASKET = "active_basket"
OBS_EXECUTION_TRUTH = "execution_truth"
OBS_INTENT_EXECUTION = "intent_vs_execution"
STATUS_OBSERVED = "observed"
STATUS_PENDING_OUTCOME = "pending_outcome"
STATUS_COMPLETED = "completed"
STATUS_INSUFFICIENT_DATA = "insufficient_data"
HEDGE_OUTCOME_HORIZON_DAYS = 5
INTENT_EXECUTION_SCHEMA_VERSION = "intent_vs_execution_v1"


@dataclass(frozen=True)
class ValidationObservationRefreshResult:
    analyses_seen: int
    execution_logs_seen: int
    observations_written: int
    hedge_outcomes_completed: int
    hedge_outcomes_pending: int
    summary: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "analyses_seen": self.analyses_seen,
            "execution_logs_seen": self.execution_logs_seen,
            "observations_written": self.observations_written,
            "hedge_outcomes_completed": self.hedge_outcomes_completed,
            "hedge_outcomes_pending": self.hedge_outcomes_pending,
            "summary": dict(self.summary),
        }


def build_validation_observation_records_from_analysis(analysis: Any) -> list[dict[str, Any]]:
    """Build observe-only records from one AgentAnalysis-like row."""
    analysis_id = _to_int(_record_get(analysis, "id"))
    if analysis_id is None:
        return []
    analyzed_at = _parse_datetime(_record_get(analysis, "analyzed_at")) or _utcnow()
    risk = _record_get(analysis, "risk_output") or {}
    trigger_type = _record_get(analysis, "trigger_type")
    execution_status = _record_get(analysis, "execution_status")
    records: list[dict[str, Any]] = []

    intent_record = _build_intent_vs_execution_record(
        analysis_id=analysis_id,
        analyzed_at=analyzed_at,
        trigger_type=trigger_type,
        execution_status=execution_status,
        risk=risk if isinstance(risk, dict) else {},
    )
    if intent_record:
        records.append(intent_record)

    hedge_outcome = risk.get("hedge_intent_outcome") if isinstance(risk, dict) else None
    if isinstance(hedge_outcome, dict) and hedge_outcome:
        records.append(
            _build_observation_record(
                observation_id=f"{OBS_HEDGE_INTENT}:{analysis_id}",
                observation_type=OBS_HEDGE_INTENT,
                analysis_id=analysis_id,
                command_id=f"analysis_{analysis_id}",
                observed_at=analyzed_at,
                observation_date=_parse_date(hedge_outcome.get("date")) or analyzed_at.date(),
                horizon_days=HEDGE_OUTCOME_HORIZON_DAYS,
                maturity_date=(analyzed_at.date() + timedelta(days=HEDGE_OUTCOME_HORIZON_DAYS + 3)),
                status=(
                    STATUS_COMPLETED
                    if hedge_outcome.get("outcome_status") == OUTCOME_COMPLETED
                    else STATUS_PENDING_OUTCOME
                ),
                observation_payload={
                    "contract_version": CONTRACT_VERSION,
                    "source": "agent_analysis.risk_output.hedge_intent_outcome",
                    "trigger_type": trigger_type,
                    "execution_status": execution_status,
                    "hedge_intent_outcome": hedge_outcome,
                },
                outcome_payload=(
                    hedge_outcome
                    if hedge_outcome.get("outcome_status") == OUTCOME_COMPLETED
                    else None
                ),
                metrics=_hedge_metrics(hedge_outcome),
                recommendation=_hedge_recommendation(hedge_outcome),
            )
        )

    active_basket = _active_basket_payload(risk if isinstance(risk, dict) else {})
    if active_basket:
        records.append(
            _build_observation_record(
                observation_id=f"{OBS_ACTIVE_BASKET}:{analysis_id}",
                observation_type=OBS_ACTIVE_BASKET,
                analysis_id=analysis_id,
                command_id=f"analysis_{analysis_id}",
                observed_at=analyzed_at,
                observation_date=analyzed_at.date(),
                horizon_days=None,
                maturity_date=None,
                status=STATUS_OBSERVED,
                observation_payload={
                    "contract_version": CONTRACT_VERSION,
                    "source": "agent_analysis.risk_output.active_basket_policy",
                    "trigger_type": trigger_type,
                    "execution_status": execution_status,
                    "active_basket_policy": active_basket,
                    "portfolio_construction_mode": _portfolio_construction_mode(risk),
                },
                outcome_payload=None,
                metrics=_active_basket_metrics(active_basket),
                recommendation=_active_basket_recommendation(active_basket),
            )
        )

    return records


def _build_intent_vs_execution_record(
    *,
    analysis_id: int,
    analyzed_at: datetime,
    trigger_type: Any,
    execution_status: Any,
    risk: dict[str, Any],
) -> dict[str, Any] | None:
    """Record what the system intended versus what it actually sent."""
    risk_approved = bool(risk.get("approved"))
    target_weights = _clean_weight_map(risk.get("target_weights") or {})
    active_count = sum(1 for ticker, weight in target_weights.items() if ticker != "CASH" and weight > 0.0)
    final_validation = risk.get("final_validation") if isinstance(risk.get("final_validation"), dict) else {}
    hedge_intent = risk.get("hedge_intent") if isinstance(risk.get("hedge_intent"), dict) else {}
    hedge_outcome = risk.get("hedge_intent_outcome") if isinstance(risk.get("hedge_intent_outcome"), dict) else {}
    blockers = _risk_blockers(risk, final_validation)
    command_sent = _execution_sent(execution_status)
    unexecuted_intents = _unexecuted_intents(
        risk_approved=risk_approved,
        target_weights=target_weights,
        execution_status=str(execution_status or ""),
        final_validation=final_validation,
        hedge_intent=hedge_intent,
        hedge_outcome=hedge_outcome,
        command_sent=command_sent,
    )
    intended_action = _intended_action(
        risk_approved=risk_approved,
        target_weights=target_weights,
        final_validation=final_validation,
    )
    payload = {
        "schema_version": INTENT_EXECUTION_SCHEMA_VERSION,
        "contract_version": CONTRACT_VERSION,
        "source": "agent_analysis.risk_output",
        "trigger_type": trigger_type,
        "execution_status": execution_status,
        "intended_action": intended_action,
        "risk_approved": risk_approved,
        "final_validation_approved": (
            final_validation.get("approved") if final_validation else None
        ),
        "target_weights": target_weights,
        "target_active_count": active_count,
        "blockers": blockers,
        "unexecuted_intents": unexecuted_intents,
        "hedge_intent": {
            "triggered": bool(hedge_intent.get("triggered")),
            "severity": _to_float(hedge_intent.get("severity"), 0.0),
            "add_hedge_etf": bool(hedge_intent.get("add_hedge_etf")),
            "selected_instrument": (
                hedge_intent.get("hedge_instrument")
                or hedge_intent.get("selected_hedge")
                or hedge_outcome.get("selected_instrument")
            ),
            "candidate_hedge_instrument": hedge_outcome.get("candidate_hedge_instrument"),
            "why_not_add_hedge": hedge_outcome.get("why_not_add_hedge"),
            "trigger_reasons": list(hedge_intent.get("reasons") or hedge_intent.get("trigger_reasons") or []),
            "cash_raise_pct": (
                hedge_intent.get("cash_raise_pct")
                or hedge_intent.get("target_cash_raise_pct")
                or hedge_outcome.get("cash_raise_pct")
                or 0.0
            ),
        },
        "outcome_label_contract": outcome_label_contract_summary(),
    }
    outcome = {
        "execution_status": execution_status,
        "command_sent": command_sent,
        "not_sent_reason": _not_sent_reason(
            risk_approved=risk_approved,
            execution_status=str(execution_status or ""),
            blockers=blockers,
            unexecuted_intents=unexecuted_intents,
        ),
    }
    return _build_observation_record(
        observation_id=f"{OBS_INTENT_EXECUTION}:{analysis_id}",
        observation_type=OBS_INTENT_EXECUTION,
        analysis_id=analysis_id,
        command_id=f"analysis_{analysis_id}",
        observed_at=analyzed_at,
        observation_date=analyzed_at.date(),
        horizon_days=None,
        maturity_date=None,
        status=STATUS_OBSERVED,
        observation_payload=payload,
        outcome_payload=outcome,
        metrics={
            "risk_approved": risk_approved,
            "command_sent": command_sent,
            "target_active_count": active_count,
            "blocker_count": len(blockers),
            "unexecuted_intent_count": len(unexecuted_intents),
            "hedge_triggered": bool(hedge_intent.get("triggered")),
            "hedge_add_requested": bool(hedge_intent.get("add_hedge_etf")),
        },
        recommendation=_intent_execution_recommendation(
            command_sent=command_sent,
            risk_approved=risk_approved,
            blockers=blockers,
            unexecuted_intents=unexecuted_intents,
        ),
    )


def build_execution_truth_observation_record(execution_log: Any) -> dict[str, Any] | None:
    """Build an execution truth record from one ExecutionLog-like row."""
    command_id = str(_record_get(execution_log, "command_id") or "").strip()
    if not command_id:
        return None
    executed_at = _parse_datetime(_record_get(execution_log, "executed_at")) or _utcnow()
    payload = _record_get(execution_log, "command_payload") or {}
    qc_response = _record_get(execution_log, "qc_response") or {}
    qc_status = str(_record_get(execution_log, "qc_status") or "unknown")
    status = str(_record_get(execution_log, "status") or "unknown")
    outcome = {
        "status": status,
        "qc_status": qc_status,
        "qc_ack_at": _date_time_str(_record_get(execution_log, "qc_ack_at")),
        "qc_rejection_reason": _record_get(execution_log, "qc_rejection_reason"),
        "is_noop": _is_noop_execution(payload, qc_response),
        "actual_order_count": _actual_order_count(payload, qc_response),
        "filled_order_count": _filled_order_count(payload, qc_response),
        "open_order_count_after": _open_order_count_after(payload, qc_response),
    }
    return _build_observation_record(
        observation_id=f"{OBS_EXECUTION_TRUTH}:{command_id}",
        observation_type=OBS_EXECUTION_TRUTH,
        analysis_id=_to_int(_record_get(execution_log, "analysis_id")),
        command_id=command_id,
        observed_at=executed_at,
        observation_date=executed_at.date(),
        horizon_days=None,
        maturity_date=None,
        status=STATUS_COMPLETED,
        observation_payload={
            "contract_version": CONTRACT_VERSION,
            "source": "execution_log",
            "command_payload": payload,
            "qc_response": qc_response,
        },
        outcome_payload=outcome,
        metrics={
            "status": status,
            "qc_status": qc_status,
            "is_noop": outcome["is_noop"],
            "actual_order_count": outcome["actual_order_count"],
            "filled_order_count": outcome["filled_order_count"],
        },
        recommendation=_execution_truth_recommendation(outcome),
    )


def complete_hedge_observation_if_mature(
    observation: dict[str, Any],
    feature_rows: list[Any],
    *,
    as_of_date: date,
) -> dict[str, Any] | None:
    """Return an updated hedge observation if a T+5 price path is mature."""
    if observation.get("observation_type") != OBS_HEDGE_INTENT:
        return None
    if observation.get("status") == STATUS_COMPLETED:
        return None
    payload = observation.get("observation_payload") or {}
    hedge_record = payload.get("hedge_intent_outcome") or {}
    obs_date = _parse_date(observation.get("observation_date")) or _parse_date(hedge_record.get("date"))
    if obs_date is None or obs_date > as_of_date:
        return None
    candidate = str(
        hedge_record.get("selected_instrument")
        or hedge_record.get("candidate_hedge_instrument")
        or "SH"
    ).upper().strip()
    spy_result = forward_return_from_feature_rows(
        feature_rows,
        ticker="SPY",
        observation_date=obs_date,
        horizon_days=int(observation.get("horizon_days") or HEDGE_OUTCOME_HORIZON_DAYS),
    )
    hedge_result = forward_return_from_feature_rows(
        feature_rows,
        ticker=candidate,
        observation_date=obs_date,
        horizon_days=int(observation.get("horizon_days") or HEDGE_OUTCOME_HORIZON_DAYS),
    )
    if spy_result is None:
        return None
    completed = backfill_hedge_intent_outcome(
        hedge_record,
        spy_return_5d=spy_result["forward_return"],
        hedge_instrument_return_5d=(
            hedge_result["forward_return"] if hedge_result is not None else None
        ),
        outcome_date=spy_result["label_date"],
    )
    updated = dict(observation)
    updated["status"] = STATUS_COMPLETED
    updated["outcome_payload"] = completed
    updated["metrics"] = {
        **(updated.get("metrics") or {}),
        "spy_return_5d": completed.get("spy_return_5d"),
        "hedge_instrument_return_5d": completed.get("hedge_instrument_return_5d"),
        "threshold_assessment": completed.get("threshold_assessment"),
        "outcome_date": completed.get("outcome_date"),
    }
    updated["recommendation"] = _hedge_recommendation(completed)
    updated["content_hash"] = _content_hash(_hash_payload(updated))
    return updated


def forward_return_from_feature_rows(
    rows: list[Any],
    *,
    ticker: str,
    observation_date: date,
    horizon_days: int,
) -> dict[str, Any] | None:
    """Compute forward return using close/adj-close rows, not trailing returns."""
    clean_ticker = str(ticker or "").upper().strip()
    normalized = sorted(
        (
            row
            for row in (_feature_row_dict(item) for item in rows)
            if row.get("ticker") == clean_ticker
            and row.get("trading_date") is not None
            and row.get("price") is not None
            and row.get("trading_date") >= observation_date
        ),
        key=lambda row: row["trading_date"],
    )
    if len(normalized) <= int(horizon_days):
        return None
    start = normalized[0]
    label = normalized[int(horizon_days)]
    start_price = float(start["price"])
    label_price = float(label["price"])
    if start_price <= 0.0:
        return None
    return {
        "ticker": clean_ticker,
        "start_date": start["trading_date"].isoformat(),
        "label_date": label["trading_date"].isoformat(),
        "start_price": round(start_price, 6),
        "label_price": round(label_price, 6),
        "forward_return": round(label_price / start_price - 1.0, 6),
    }


async def persist_validation_observations(db: Any, records: list[dict[str, Any]]) -> int:
    """Upsert observation rows by stable observation_id."""
    if not records:
        return 0
    from sqlalchemy.dialects.postgresql import insert

    from db.models import ValidationObservation

    payloads = [_db_record(record) for record in records]
    stmt = insert(ValidationObservation).values(payloads)
    update_cols = {
        key: getattr(stmt.excluded, key)
        for key in payloads[0]
        if key not in {"id", "observation_id", "created_at"}
    }
    stmt = stmt.on_conflict_do_update(
        constraint="uq_validation_observation_id",
        set_=update_cols,
    )
    await db.execute(stmt)
    await db.commit()
    return len(payloads)


async def persist_observations_for_analysis(
    db: Any,
    analysis: Any,
) -> int:
    """Persist one analysis' validation observations."""
    return await persist_validation_observations(
        db,
        build_validation_observation_records_from_analysis(analysis),
    )


async def refresh_validation_observation_loop(
    db: Any,
    *,
    as_of_date: date | None = None,
    analysis_limit: int = 300,
    execution_limit: int = 300,
    feature_source: str = "yfinance",
) -> ValidationObservationRefreshResult:
    """Backfill observations from existing rows and mature hedge outcomes."""
    from sqlalchemy import desc, select

    from db.models import AgentAnalysis, ExecutionLog, MarketDailyFeature, ValidationObservation

    target_date = as_of_date or datetime.now(timezone.utc).date()
    analysis_rows = list(
        (
            await db.execute(
                select(AgentAnalysis)
                .order_by(desc(AgentAnalysis.analyzed_at), desc(AgentAnalysis.id))
                .limit(analysis_limit)
            )
        ).scalars().all()
    )
    execution_rows = list(
        (
            await db.execute(
                select(ExecutionLog)
                .order_by(desc(ExecutionLog.executed_at), desc(ExecutionLog.id))
                .limit(execution_limit)
            )
        ).scalars().all()
    )

    records: list[dict[str, Any]] = []
    for analysis in analysis_rows:
        records.extend(build_validation_observation_records_from_analysis(analysis))
    for row in execution_rows:
        exec_record = build_execution_truth_observation_record(row)
        if exec_record:
            records.append(exec_record)
    written = await persist_validation_observations(db, records)

    pending_rows = list(
        (
            await db.execute(
                select(ValidationObservation)
                .where(ValidationObservation.observation_type == OBS_HEDGE_INTENT)
                .where(ValidationObservation.status == STATUS_PENDING_OUTCOME)
                .order_by(ValidationObservation.observed_at)
                .limit(analysis_limit)
            )
        ).scalars().all()
    )
    tickers = sorted({
        "SPY",
        *[
            str(
                ((row.observation_payload or {}).get("hedge_intent_outcome") or {}).get("selected_instrument")
                or ((row.observation_payload or {}).get("hedge_intent_outcome") or {}).get("candidate_hedge_instrument")
                or "SH"
            ).upper().strip()
            for row in pending_rows
        ],
    })
    start_dates = [row.observation_date for row in pending_rows if row.observation_date]
    feature_rows: list[Any] = []
    if pending_rows and tickers and start_dates:
        feature_rows = list(
            (
                await db.execute(
                    select(MarketDailyFeature)
                    .where(MarketDailyFeature.ticker.in_(tickers))
                    .where(MarketDailyFeature.trading_date >= min(start_dates))
                    .where(MarketDailyFeature.trading_date <= target_date)
                    .where(MarketDailyFeature.source == feature_source)
                    .order_by(MarketDailyFeature.trading_date, MarketDailyFeature.ticker)
                )
            ).scalars().all()
        )

    completed_records: list[dict[str, Any]] = []
    for row in pending_rows:
        candidate = complete_hedge_observation_if_mature(
            _model_observation_to_dict(row),
            feature_rows,
            as_of_date=target_date,
        )
        if candidate:
            completed_records.append(candidate)
    if completed_records:
        await persist_validation_observations(db, completed_records)

    hedge_summary = await load_validation_observation_summary(db, limit=50)
    return ValidationObservationRefreshResult(
        analyses_seen=len(analysis_rows),
        execution_logs_seen=len(execution_rows),
        observations_written=written + len(completed_records),
        hedge_outcomes_completed=len(completed_records),
        hedge_outcomes_pending=max(len(pending_rows) - len(completed_records), 0),
        summary={
            "contract_version": CONTRACT_VERSION,
            "execution_authority": EXECUTION_AUTHORITY,
            "target_weight_mutation": TARGET_WEIGHT_MUTATION,
            "as_of_date": target_date.isoformat(),
            "hedge_threshold_summary": hedge_summary.get("hedge_threshold_summary") or {},
            "observation_counts": hedge_summary.get("observation_counts") or {},
        },
    )


async def load_validation_observation_summary(db: Any, *, limit: int = 50) -> dict[str, Any]:
    """Load compact dashboard/report summary from durable observations."""
    from sqlalchemy import desc, select

    from db.models import ValidationObservation

    rows = list(
        (
            await db.execute(
                select(ValidationObservation)
                .order_by(desc(ValidationObservation.observed_at), desc(ValidationObservation.id))
                .limit(limit)
            )
        ).scalars().all()
    )
    compact = [_model_observation_to_dict(row) for row in rows]
    counts: dict[str, int] = {}
    for row in compact:
        key = f"{row.get('observation_type')}:{row.get('status')}"
        counts[key] = counts.get(key, 0) + 1
    hedge_records = [
        row.get("outcome_payload") or (row.get("observation_payload") or {}).get("hedge_intent_outcome")
        for row in compact
        if row.get("observation_type") == OBS_HEDGE_INTENT
    ]
    return {
        "contract_version": CONTRACT_VERSION,
        "execution_authority": EXECUTION_AUTHORITY,
        "target_weight_mutation": TARGET_WEIGHT_MUTATION,
        "sampled": len(compact),
        "observation_counts": dict(sorted(counts.items())),
        "hedge_threshold_summary": summarize_hedge_threshold_assessments(hedge_records),
        "recent_observations": [
            {
                "observation_id": row.get("observation_id"),
                "observation_type": row.get("observation_type"),
                "status": row.get("status"),
                "analysis_id": row.get("analysis_id"),
                "command_id": row.get("command_id"),
                "observed_at": row.get("observed_at"),
                "metrics": row.get("metrics") or {},
                "recommendation": row.get("recommendation") or {},
            }
            for row in compact[:10]
        ],
    }


def _build_observation_record(
    *,
    observation_id: str,
    observation_type: str,
    analysis_id: int | None,
    command_id: str | None,
    observed_at: datetime,
    observation_date: date,
    horizon_days: int | None,
    maturity_date: date | None,
    status: str,
    observation_payload: dict[str, Any],
    outcome_payload: dict[str, Any] | None,
    metrics: dict[str, Any] | None,
    recommendation: dict[str, Any] | None,
) -> dict[str, Any]:
    record = {
        "observation_id": observation_id,
        "observation_type": observation_type,
        "analysis_id": analysis_id,
        "command_id": command_id,
        "observed_at": observed_at.replace(tzinfo=None),
        "observation_date": observation_date,
        "horizon_days": horizon_days,
        "maturity_date": maturity_date,
        "status": status,
        "execution_authority": EXECUTION_AUTHORITY,
        "target_weight_mutation": TARGET_WEIGHT_MUTATION,
        "observation_payload": json_safe(observation_payload),
        "outcome_payload": json_safe(outcome_payload),
        "metrics": json_safe(metrics or {}),
        "recommendation": json_safe(recommendation or {}),
    }
    record["content_hash"] = _content_hash(_hash_payload(record))
    return record


def _active_basket_payload(risk: dict[str, Any]) -> dict[str, Any]:
    payload = risk.get("active_basket_policy") if isinstance(risk.get("active_basket_policy"), dict) else {}
    if payload:
        return payload
    for key in ("portfolio_construction_candidate", "portfolio_construction_shadow"):
        pc = risk.get(key) if isinstance(risk.get(key), dict) else {}
        diagnostics = pc.get("diagnostics") if isinstance(pc.get("diagnostics"), dict) else {}
        evaluation = diagnostics.get("active_basket_policy")
        if isinstance(evaluation, dict) and evaluation:
            return evaluation
        basket_eval = pc.get("active_basket_evaluation") if isinstance(pc.get("active_basket_evaluation"), dict) else {}
        if basket_eval:
            return basket_eval
    return {}


def _portfolio_construction_mode(risk: dict[str, Any]) -> str | None:
    for key in ("portfolio_construction_candidate", "portfolio_construction_shadow"):
        pc = risk.get(key) if isinstance(risk.get(key), dict) else {}
        mode = pc.get("portfolio_construction_mode") or (pc.get("diagnostics") or {}).get("runtime_mode")
        if mode:
            return str(mode)
    return None


def _hedge_metrics(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "triggered": bool(row.get("triggered")),
        "severity": _to_float(row.get("severity"), 0.0),
        "add_hedge_etf": bool(row.get("add_hedge_etf")),
        "selected_instrument": row.get("selected_instrument"),
        "candidate_hedge_instrument": row.get("candidate_hedge_instrument"),
        "outcome_status": row.get("outcome_status"),
        "threshold_assessment": row.get("threshold_assessment"),
        "spy_return_5d": row.get("spy_return_5d"),
        "hedge_instrument_return_5d": row.get("hedge_instrument_return_5d"),
    }


def _hedge_recommendation(row: dict[str, Any]) -> dict[str, Any]:
    assessment = str(row.get("threshold_assessment") or "")
    action = "collect_more_samples"
    if assessment in {"too_conservative", "severity_threshold_too_high"}:
        action = "review_lower_hedge_threshold"
    elif assessment == "too_aggressive":
        action = "review_raise_hedge_threshold"
    return {
        "recommendation_only": True,
        "operator_action": action,
        "threshold_assessment": assessment or "pending",
    }


def _active_basket_metrics(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "active_count": _to_int(row.get("active_count")),
        "target_active_count_min": _to_int(row.get("target_active_count_min")),
        "target_active_count_max": _to_int(row.get("target_active_count_max")),
        "within_target_active_count": row.get("within_target_active_count"),
        "subscale_count": _to_int(row.get("subscale_count"), 0),
        "floor_cleared_count": _to_int(row.get("floor_cleared_count"), 0),
        "estimated_independent_clusters": _to_int(row.get("estimated_independent_clusters")),
        "warnings": list(row.get("warnings") or []),
    }


def _active_basket_recommendation(row: dict[str, Any]) -> dict[str, Any]:
    metrics = _active_basket_metrics(row)
    action = "keep_collecting_samples"
    if metrics.get("within_target_active_count") is False:
        action = "review_active_count_range"
    if (metrics.get("subscale_count") or 0) > 0:
        action = "review_subscale_position_tail"
    return {
        "recommendation_only": True,
        "operator_action": action,
    }


def _execution_truth_recommendation(outcome: dict[str, Any]) -> dict[str, Any]:
    action = "no_action"
    if outcome.get("is_noop"):
        action = "review_dedupe_or_snapshot_freshness"
    elif str(outcome.get("qc_status") or "") in {"rejected", "failed_no_fill", "reconciliation_drift"}:
        action = "review_execution_failure"
    return {
        "recommendation_only": True,
        "operator_action": action,
    }


def _intent_execution_recommendation(
    *,
    command_sent: bool,
    risk_approved: bool,
    blockers: list[str],
    unexecuted_intents: list[str],
) -> dict[str, Any]:
    action = "no_action"
    if unexecuted_intents:
        action = "review_unexecuted_intent"
    elif risk_approved and not command_sent:
        action = "review_approved_not_sent"
    elif blockers:
        action = "review_blockers"
    return {
        "recommendation_only": True,
        "operator_action": action,
        "unexecuted_intents": list(unexecuted_intents),
    }


def _intended_action(
    *,
    risk_approved: bool,
    target_weights: dict[str, float],
    final_validation: dict[str, Any],
) -> str:
    if not target_weights:
        return "no_target"
    if not risk_approved:
        if final_validation and final_validation.get("approved") is False:
            return "blocked_by_final_validation"
        return "blocked_by_risk"
    return "send_qc_command"


def _unexecuted_intents(
    *,
    risk_approved: bool,
    target_weights: dict[str, float],
    execution_status: str,
    final_validation: dict[str, Any],
    hedge_intent: dict[str, Any],
    hedge_outcome: dict[str, Any],
    command_sent: bool,
) -> list[str]:
    intents: list[str] = []
    if hedge_intent.get("triggered") and not hedge_intent.get("add_hedge_etf"):
        intents.append("hedge_triggered_without_inverse_etf")
    if hedge_outcome.get("why_not_add_hedge") and not hedge_outcome.get("add_hedge_etf"):
        intents.append(f"hedge_not_added:{hedge_outcome.get('why_not_add_hedge')}")
    if target_weights and not risk_approved:
        if final_validation and final_validation.get("approved") is False:
            intents.append("target_blocked_by_final_validation")
        else:
            intents.append("target_blocked_by_risk")
    if risk_approved and target_weights and not command_sent:
        status = execution_status or "unknown"
        intents.append(f"approved_target_not_sent:{status}")
    return _unique_strings(intents)


def _risk_blockers(risk: dict[str, Any], final_validation: dict[str, Any]) -> list[str]:
    blockers: list[str] = []
    for value in (
        risk.get("blockers"),
        risk.get("failed_checks"),
        final_validation.get("blockers") if isinstance(final_validation, dict) else None,
        final_validation.get("details") if isinstance(final_validation, dict) else None,
    ):
        blockers.extend(_string_list(value))
    reason = final_validation.get("reason") if isinstance(final_validation, dict) else None
    if reason:
        blockers.append(str(reason))
    return _unique_strings(blockers)


def _execution_sent(execution_status: Any) -> bool:
    status = str(execution_status or "").strip().lower()
    return status in {
        "sent",
        "accepted",
        "filled",
        "success",
        "orders_submitted",
        "partial",
        "reconciled",
    }


def _not_sent_reason(
    *,
    risk_approved: bool,
    execution_status: str,
    blockers: list[str],
    unexecuted_intents: list[str],
) -> str | None:
    if _execution_sent(execution_status):
        return None
    status = str(execution_status or "").strip().lower()
    if status in {"deduped", "deferred_by_active_execution", "rejected", "not_sent", "skipped"}:
        return status
    if unexecuted_intents:
        return unexecuted_intents[0]
    if blockers:
        return blockers[0]
    if not risk_approved:
        return "risk_not_approved"
    return "unknown_not_sent"


def _clean_weight_map(raw: dict[str, Any] | None) -> dict[str, float]:
    out: dict[str, float] = {}
    for ticker, value in (raw or {}).items():
        clean = str(ticker or "").upper().strip()
        if not clean:
            continue
        parsed = _to_float(value)
        if parsed is not None and parsed > 1e-9:
            out[clean] = round(parsed, 6)
    return out


def _is_noop_execution(payload: dict[str, Any], qc_response: dict[str, Any]) -> bool:
    summary = _order_summary(payload, qc_response)
    if summary.get("is_noop") is not None:
        return bool(summary.get("is_noop"))
    state = str(summary.get("execution_state") or "").lower()
    return state == "noop_reconciled"


def _actual_order_count(payload: dict[str, Any], qc_response: dict[str, Any]) -> int | None:
    summary = _order_summary(payload, qc_response)
    return _to_int(
        summary.get("actual_order_count"),
        _to_int(summary.get("submitted_order_count")),
    )


def _filled_order_count(payload: dict[str, Any], qc_response: dict[str, Any]) -> int | None:
    summary = _order_summary(payload, qc_response)
    return _to_int(summary.get("filled_order_count"))


def _open_order_count_after(payload: dict[str, Any], qc_response: dict[str, Any]) -> int | None:
    summary = _order_summary(payload, qc_response)
    return _to_int(summary.get("open_order_count_after"))


def _order_summary(payload: dict[str, Any], qc_response: dict[str, Any]) -> dict[str, Any]:
    for container in (qc_response, payload):
        if not isinstance(container, dict):
            continue
        summary = container.get("order_summary")
        if isinstance(summary, dict):
            return summary
        ack = container.get("ack") if isinstance(container.get("ack"), dict) else {}
        summary = ack.get("order_summary")
        if isinstance(summary, dict):
            return summary
    return {}


def _db_record(record: dict[str, Any]) -> dict[str, Any]:
    now = _utcnow()
    return {
        **record,
        "observation_payload": json_safe(record.get("observation_payload") or {}),
        "outcome_payload": json_safe(record.get("outcome_payload")),
        "metrics": json_safe(record.get("metrics") or {}),
        "recommendation": json_safe(record.get("recommendation") or {}),
        "updated_at": now,
    }


def _model_observation_to_dict(row: Any) -> dict[str, Any]:
    return {
        "observation_id": row.observation_id,
        "observation_type": row.observation_type,
        "analysis_id": row.analysis_id,
        "command_id": row.command_id,
        "observed_at": _date_time_str(row.observed_at),
        "observation_date": row.observation_date,
        "horizon_days": row.horizon_days,
        "maturity_date": row.maturity_date,
        "status": row.status,
        "execution_authority": row.execution_authority,
        "target_weight_mutation": row.target_weight_mutation,
        "observation_payload": row.observation_payload or {},
        "outcome_payload": row.outcome_payload,
        "metrics": row.metrics or {},
        "recommendation": row.recommendation or {},
        "content_hash": row.content_hash,
    }


def _feature_row_dict(value: Any) -> dict[str, Any]:
    trading_date = _parse_date(_record_get(value, "trading_date"))
    price = _first_float(
        _record_get(value, "adj_close_price"),
        _record_get(value, "close_price"),
        _record_get(value, "price"),
    )
    return {
        "ticker": str(_record_get(value, "ticker") or "").upper().strip(),
        "trading_date": trading_date,
        "price": price,
    }


def _record_get(value: Any, key: str) -> Any:
    if isinstance(value, dict):
        return value.get(key)
    return getattr(value, key, None)


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str) and value.strip():
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)
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


def _to_float(value: Any, default: float | None = None) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_int(value: Any, default: int | None = None) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        return [str(item) for item in value if str(item)]
    if isinstance(value, dict):
        return [str(value)]
    text = str(value)
    return [text] if text else []


def _unique_strings(values: list[Any]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        clean = str(value or "").strip()
        if not clean or clean in seen:
            continue
        seen.add(clean)
        out.append(clean)
    return out


def _first_float(*values: Any) -> float | None:
    for value in values:
        parsed = _to_float(value)
        if parsed is not None:
            return parsed
    return None


def _date_time_str(value: Any) -> str | None:
    parsed = _parse_datetime(value)
    return parsed.isoformat() if parsed else None


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _hash_payload(record: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in record.items()
        if key not in {"created_at", "updated_at", "content_hash"}
    }


def _content_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(json_safe(value), sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
