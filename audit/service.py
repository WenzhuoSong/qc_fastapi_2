"""Versioned audit runner over immutable QC observer projections."""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy import desc, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from audit.calculators import (
    decimal,
    first_post_execution_snapshot,
    risk_intent_candidate,
    summarize_intent,
    target_reconciliation,
)
from db.models import (
    QCAccountSnapshotHoldingProjection,
    QCAccountSnapshotProjection,
    QCAuditRun,
    QCDecisionExecutionAudit,
    QCDecisionManifestProjection,
    QCLocalRiskProjection,
    QCOrderIntentProjection,
    QCOrderLifecycleProjection,
    QCProjectionCheckpoint,
    QCRiskExitAudit,
)
from services.qc_observer_projections import PROJECTION_VERSION


AUDIT_NAME = "observer_execution_audit"
AUDIT_METHOD_VERSION = "observer_audit.v2"


def _row_dict(row: Any, fields: tuple[str, ...]) -> dict[str, Any]:
    return {field: getattr(row, field) for field in fields}


def _seconds(start: datetime, end: datetime | None) -> Decimal | None:
    if end is None:
        return None
    return Decimal(str((end - start).total_seconds()))


def _json_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return value


async def run_observer_audit(db: AsyncSession) -> dict[str, Any]:
    await db.execute(text("SELECT pg_advisory_xact_lock(hashtext('observer_execution_audit_v1'))"))
    checkpoint = await db.scalar(
        select(QCProjectionCheckpoint).where(
            QCProjectionCheckpoint.projection_name == "observer_business_facts",
            QCProjectionCheckpoint.event_stream_id == "__all__",
            QCProjectionCheckpoint.event_channel == "ALL",
        )
    )
    cutoff = int(checkpoint.last_processed_sequence) if checkpoint else 0
    if cutoff <= 0:
        return {"status": "blocked", "reason": "projection_not_ready"}
    existing = await db.scalar(
        select(QCAuditRun).where(
            QCAuditRun.audit_name == AUDIT_NAME,
            QCAuditRun.method_version == AUDIT_METHOD_VERSION,
            QCAuditRun.projection_cutoff_raw_event_id == cutoff,
        )
    )
    if existing is not None:
        return {"status": "unchanged", "audit_run_id": existing.id, "cutoff": cutoff}

    decisions = list(
        (await db.scalars(
            select(QCDecisionManifestProjection)
            .where(QCDecisionManifestProjection.raw_event_id <= cutoff)
            .order_by(QCDecisionManifestProjection.event_stream_id, QCDecisionManifestProjection.event_time)
        )).all()
    )
    intents = list(
        (await db.scalars(
            select(QCOrderIntentProjection)
            .where(QCOrderIntentProjection.raw_event_id <= cutoff)
            .order_by(QCOrderIntentProjection.event_time)
        )).all()
    )
    lifecycle = list(
        (await db.scalars(
            select(QCOrderLifecycleProjection)
            .where(QCOrderLifecycleProjection.raw_event_id <= cutoff)
            .order_by(QCOrderLifecycleProjection.event_time)
        )).all()
    )
    snapshots = list(
        (await db.scalars(
            select(QCAccountSnapshotProjection)
            .where(QCAccountSnapshotProjection.raw_event_id <= cutoff)
            .order_by(QCAccountSnapshotProjection.event_time)
        )).all()
    )
    risks = list(
        (await db.scalars(
            select(QCLocalRiskProjection)
            .where(QCLocalRiskProjection.raw_event_id <= cutoff)
            .order_by(QCLocalRiskProjection.event_time)
        )).all()
    )
    input_ids = sorted(
        [row.raw_event_id for row in decisions]
        + [row.raw_event_id for row in intents]
        + [row.raw_event_id for row in lifecycle]
        + [row.raw_event_id for row in snapshots]
        + [row.raw_event_id for row in risks]
    )
    input_hash = hashlib.sha256(
        json.dumps({"cutoff": cutoff, "raw_event_ids": input_ids}, separators=(",", ":")).encode()
    ).hexdigest()
    run = QCAuditRun(
        audit_name=AUDIT_NAME,
        method_version=AUDIT_METHOD_VERSION,
        projection_version=PROJECTION_VERSION,
        projection_cutoff_raw_event_id=cutoff,
        input_hash=input_hash,
        status="running",
        summary={},
    )
    db.add(run)
    await db.flush()

    lifecycle_by_intent: dict[str, list[Any]] = {}
    for event in lifecycle:
        if event.order_intent_id:
            lifecycle_by_intent.setdefault(event.order_intent_id, []).append(event)
    intents_by_decision: dict[tuple[str, str], list[Any]] = {}
    for intent in intents:
        if intent.decision_id:
            intents_by_decision.setdefault((intent.event_stream_id, intent.decision_id), []).append(intent)

    decision_status_counts: dict[str, int] = {}
    for index, decision in enumerate(decisions):
        next_time = None
        for candidate in decisions[index + 1:]:
            if candidate.event_stream_id == decision.event_stream_id:
                next_time = candidate.event_time
                break
        decision_intents = intents_by_decision.get((decision.event_stream_id, decision.decision_id), [])
        candidate_snapshots = [
            row for row in snapshots
            if row.event_stream_id == decision.event_stream_id
            and row.event_time >= decision.event_time
            and (next_time is None or row.event_time < next_time)
        ]
        intent_summaries = []
        evidence = [int(decision.raw_event_id)]
        for intent in decision_intents:
            summary = summarize_intent(
                _row_dict(intent, ("requested_quantity", "submitted_quantity")),
                [
                    _row_dict(
                        event,
                        (
                            "raw_event_id", "event_time", "status", "fill_quantity",
                            "fill_quantity_semantics", "remaining_quantity", "fee",
                        ),
                    )
                    for event in lifecycle_by_intent.get(intent.order_intent_id, [])
                ],
            )
            summary["intent"] = intent
            intent_summaries.append(summary)
            evidence.append(int(intent.raw_event_id))
            evidence.extend(summary["evidence_raw_event_ids"])
        terminal_count = sum(1 for item in intent_summaries if item["terminal"])
        ratios = [item["fill_ratio"] for item in intent_summaries if item["fill_ratio"] is not None]
        first_times = [item["first_event_time"] for item in intent_summaries if item["first_event_time"]]
        terminal_times = [item["terminal_time"] for item in intent_summaries if item["terminal_time"]]
        snapshot = first_post_execution_snapshot(
            candidate_snapshots,
            decision_time=decision.event_time,
            terminal_times=terminal_times,
        )
        holdings = []
        if snapshot is not None:
            holdings = list(
                (await db.scalars(
                    select(QCAccountSnapshotHoldingProjection)
                    .where(QCAccountSnapshotHoldingProjection.account_snapshot_id == snapshot.id)
                )).all()
            )
        reconciliation = target_reconciliation(
            dict(decision.final_target_weights or {}),
            equity=snapshot.equity if snapshot else None,
            cash=snapshot.cash if snapshot else None,
            holdings=[
                _row_dict(row, ("security_id", "ticker", "quantity", "holdings_value"))
                for row in holdings
            ],
        )
        if snapshot is None:
            status = "evidence_blocked"
        elif decision_intents and terminal_count < len(decision_intents):
            status = "open"
        elif not decision_intents:
            status = "observed_no_orders"
        else:
            status = "complete"
        decision_status_counts[status] = decision_status_counts.get(status, 0) + 1
        db.add(QCDecisionExecutionAudit(
            audit_run_id=run.id,
            decision_manifest_id=decision.id,
            event_stream_id=decision.event_stream_id,
            decision_id=decision.decision_id,
            decision_time=decision.event_time,
            account_snapshot_id=snapshot.id if snapshot else None,
            status=status,
            target_exposure=reconciliation["target_exposure"],
            actual_exposure=reconciliation["actual_exposure"],
            cash_weight=reconciliation["cash_weight"],
            tracking_error_l1=reconciliation["tracking_error_l1"],
            intent_count=len(decision_intents),
            terminal_intent_count=terminal_count,
            partial_fill_count=sum(1 for item in intent_summaries if item["partial"]),
            rejected_intent_count=sum(1 for item in intent_summaries if item["rejected"]),
            clipped_intent_count=sum(1 for item in decision_intents if item.clipped),
            aggregate_fill_ratio=sum(ratios, Decimal("0")) / len(ratios) if ratios else None,
            total_fees=sum((item["fees"] for item in intent_summaries), Decimal("0")),
            first_submit_latency_seconds=_seconds(decision.event_time, min(first_times) if first_times else None),
            terminal_latency_seconds=_seconds(decision.event_time, max(terminal_times) if terminal_times else None),
            evidence_raw_event_ids=sorted(set(evidence + ([int(snapshot.raw_event_id)] if snapshot else []))),
            details=_json_value({
                "snapshot_selection": "first_broker_reconciled_snapshot_after_terminal_execution",
                "snapshot_event_time": snapshot.event_time if snapshot else None,
                "unmatched_actual": reconciliation["unmatched_actual"],
                "target_weights": reconciliation["target_weights"],
                "actual_weights": reconciliation["actual_weights"],
                "intent_fill_ratios": {
                    item["intent"].order_intent_id: item["fill_ratio"] for item in intent_summaries
                },
            }),
        ))

    risk_status_counts: dict[str, int] = {}
    intent_dicts = [
        {**_row_dict(row, ("event_stream_id", "security_id", "event_time", "risk_class", "reason")), "row": row}
        for row in intents
    ]
    for risk in risks:
        risk_dict = _row_dict(risk, ("event_stream_id", "security_id", "event_time"))
        matches = [item for item in intent_dicts if risk_intent_candidate(risk_dict, item)]
        intent = min(matches, key=lambda item: item["event_time"])["row"] if matches else None
        summary = None
        evidence = [int(risk.raw_event_id)]
        if intent is not None:
            summary = summarize_intent(
                _row_dict(intent, ("requested_quantity", "submitted_quantity")),
                [
                    _row_dict(
                        event,
                        (
                            "raw_event_id", "event_time", "status", "fill_quantity",
                            "fill_quantity_semantics", "remaining_quantity", "fee",
                        ),
                    )
                    for event in lifecycle_by_intent.get(intent.order_intent_id, [])
                ],
            )
            evidence.append(int(intent.raw_event_id))
            evidence.extend(summary["evidence_raw_event_ids"])
        if intent is None:
            status = "evidence_blocked"
        elif summary and summary["terminal"] and not summary["partial"]:
            status = "complete"
        elif summary and summary["event_count"]:
            status = "open_or_partial"
        else:
            status = "awaiting_broker_event"
        risk_status_counts[status] = risk_status_counts.get(status, 0) + 1
        requested = summary["requested_quantity"] if summary else None
        filled = summary["filled_quantity"] if summary else None
        residual = summary["remaining_quantity"] if summary else None
        reference_price = decimal(intent.reference_price) if intent and intent.reference_price is not None else None
        db.add(QCRiskExitAudit(
            audit_run_id=run.id,
            local_risk_event_id=risk.id,
            event_stream_id=risk.event_stream_id,
            risk_event_id=risk.risk_event_id,
            security_id=risk.security_id,
            ticker=risk.ticker,
            risk_type=risk.risk_type,
            risk_time=risk.event_time,
            order_intent_id=intent.id if intent else None,
            status=status,
            intent_latency_seconds=_seconds(risk.event_time, intent.event_time if intent else None),
            first_fill_latency_seconds=_seconds(risk.event_time, summary["first_fill_time"] if summary else None),
            terminal_fill_latency_seconds=_seconds(risk.event_time, summary["terminal_time"] if summary else None),
            requested_quantity=requested,
            filled_quantity=filled,
            residual_quantity=residual,
            residual_notional=abs(residual * reference_price) if residual is not None and reference_price is not None else None,
            evidence_raw_event_ids=sorted(set(evidence)),
            details={"link_method": "same_stream_security_risk_marker_within_7d"},
        ))

    run.status = "completed"
    run.completed_at = datetime.now(timezone.utc)
    run.summary = {
        "decision_count": len(decisions),
        "decision_status_counts": decision_status_counts,
        "risk_event_count": len(risks),
        "risk_status_counts": risk_status_counts,
        "projection_cutoff_raw_event_id": cutoff,
    }
    await db.flush()
    return {"status": "completed", "audit_run_id": run.id, **run.summary}


async def load_latest_audit_status(db: AsyncSession) -> dict[str, Any]:
    run = await db.scalar(
        select(QCAuditRun)
        .where(QCAuditRun.audit_name == AUDIT_NAME)
        .order_by(desc(QCAuditRun.id))
        .limit(1)
    )
    if run is None:
        return {
            "available": False,
            "audit_name": AUDIT_NAME,
            "method_version": AUDIT_METHOD_VERSION,
            "execution_authority": "none",
        }
    latest_decision = await db.scalar(
        select(QCDecisionExecutionAudit)
        .where(QCDecisionExecutionAudit.audit_run_id == run.id)
        .order_by(desc(QCDecisionExecutionAudit.decision_time))
        .limit(1)
    )
    return {
        "available": True,
        "audit_run_id": run.id,
        "audit_name": run.audit_name,
        "method_version": run.method_version,
        "projection_version": run.projection_version,
        "projection_cutoff_raw_event_id": run.projection_cutoff_raw_event_id,
        "status": run.status,
        "summary": run.summary,
        "completed_at": run.completed_at.isoformat() if run.completed_at else None,
        "execution_authority": "none",
        "latest_decision": None if latest_decision is None else {
            "decision_id": latest_decision.decision_id,
            "decision_time": latest_decision.decision_time.isoformat(),
            "status": latest_decision.status,
            "target_exposure": float(latest_decision.target_exposure),
            "actual_exposure": float(latest_decision.actual_exposure) if latest_decision.actual_exposure is not None else None,
            "cash_weight": float(latest_decision.cash_weight) if latest_decision.cash_weight is not None else None,
            "tracking_error_l1": float(latest_decision.tracking_error_l1) if latest_decision.tracking_error_l1 is not None else None,
            "intent_count": latest_decision.intent_count,
            "terminal_intent_count": latest_decision.terminal_intent_count,
            "partial_fill_count": latest_decision.partial_fill_count,
            "rejected_intent_count": latest_decision.rejected_intent_count,
            "clipped_intent_count": latest_decision.clipped_intent_count,
            "aggregate_fill_ratio": float(latest_decision.aggregate_fill_ratio) if latest_decision.aggregate_fill_ratio is not None else None,
            "total_fees": float(latest_decision.total_fees),
        },
    }
