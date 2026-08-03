"""Rebuildable typed projections over immutable QC observer facts."""
from __future__ import annotations

from datetime import date, datetime, time, timezone
import logging
from typing import Any

from sqlalchemy import exists, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import (
    QCAccountSnapshotHoldingProjection,
    QCAccountSnapshotOpenOrderProjection,
    QCAccountSnapshotProjection,
    QCDecisionManifestProjection,
    QCDecisionRankingProjection,
    QCLocalRiskProjection,
    QCObserverEvent,
    QCObserverEventAnnotation,
    QCOrderIntentProjection,
    QCOrderLifecycleProjection,
    QCProjectionCheckpoint,
)


PROJECTION_VERSION = "observer_projection.v1"
logger = logging.getLogger(__name__)
PROJECTED_EVENT_TYPES = frozenset(
    {
        "decision_manifest",
        "order_intent",
        "broker_lifecycle_event",
        "local_risk_event",
        "account_snapshot",
    }
)


def projection_record_for_event(raw: Any) -> dict[str, Any] | None:
    """Return a normalized record without mutating or recomputing strategy facts."""
    envelope = dict(raw.payload)
    event_type = str(envelope.get("event_type") or raw.event_type)
    if event_type not in PROJECTED_EVENT_TYPES:
        return None
    payload = dict(envelope.get("payload") or {})
    common = {
        "raw_event_id": raw.id,
        "event_id": raw.event_id,
        "event_stream_id": raw.event_stream_id,
        "account_id": raw.account_id,
        "deployment_id": raw.deployment_id,
        "strategy_id": raw.strategy_id,
        "event_sequence": raw.event_sequence,
        "event_time": raw.event_time,
        "projection_version": PROJECTION_VERSION,
    }
    if event_type == "decision_manifest":
        known = {
            "decision_id", "decision_kind", "as_of", "basis_account_snapshot_id",
            "market_data_cutoff", "strategy_version", "execution_style",
            "execution_authority", "raw_target_weights", "risk_adjusted_weights",
            "final_target_weights", "suppressed_deltas", "ranked_candidates",
        }
        return {
            "kind": event_type,
            **common,
            "decision_id": str(payload["decision_id"]),
            "decision_kind": str(payload["decision_kind"]),
            "as_of": _datetime(payload["as_of"]),
            "basis_account_snapshot_id": _optional(payload.get("basis_account_snapshot_id")),
            "market_data_cutoff": _optional_datetime(payload.get("market_data_cutoff")),
            "strategy_version": _optional(payload.get("strategy_version")),
            "execution_style": _optional(payload.get("execution_style")),
            "execution_authority": str(payload["execution_authority"]),
            "raw_target_weights": dict(payload.get("raw_target_weights") or {}),
            "risk_adjusted_weights": dict(payload.get("risk_adjusted_weights") or {}),
            "final_target_weights": dict(
                payload.get("final_target_weights")
                or payload.get("risk_adjusted_weights")
                or {}
            ),
            "suppressed_deltas": dict(payload.get("suppressed_deltas") or {}),
            "ranked_candidates": list(payload.get("ranked_candidates") or []),
            "payload_extra": _extra(payload, known),
        }
    if event_type == "order_intent":
        known = {
            "order_intent_id", "decision_id", "security_id", "ticker",
            "requested_quantity", "submitted_quantity", "target_weight",
            "reference_price", "reference_time", "order_type", "reason",
            "risk_class", "clipped",
        }
        return {
            "kind": event_type,
            **common,
            "order_intent_id": str(payload["order_intent_id"]),
            "decision_id": _optional(payload.get("decision_id")),
            "security_id": str(payload["security_id"]),
            "ticker": _optional(payload.get("ticker")),
            "requested_quantity": payload["requested_quantity"],
            "submitted_quantity": payload.get("submitted_quantity"),
            "target_weight": payload.get("target_weight"),
            "reference_price": payload.get("reference_price"),
            "reference_time": _optional_datetime(payload.get("reference_time")),
            "order_type": str(payload["order_type"]),
            "reason": str(payload["reason"]),
            "risk_class": str(payload["risk_class"]),
            "clipped": payload.get("clipped"),
            "payload_extra": _extra(payload, known),
        }
    if event_type == "broker_lifecycle_event":
        reconstructed = bool(payload.get("reconstructed"))
        known = {
            "broker_order_id", "order_intent_id", "status", "security_id",
            "ticker", "event_time", "quantity", "fill_quantity", "fill_price",
            "fee", "message", "reconstructed", "reconstruction_source",
            "remaining_quantity",
        }
        return {
            "kind": event_type,
            **common,
            "broker_order_id": str(payload["broker_order_id"]),
            "order_intent_id": _optional(payload.get("order_intent_id")),
            "security_id": str(payload["security_id"]),
            "ticker": _optional(payload.get("ticker")),
            "status": str(payload["status"]),
            "quantity": payload.get("quantity"),
            "fill_quantity": payload.get("fill_quantity"),
            "fill_quantity_semantics": (
                "cumulative_reconstructed" if reconstructed else "incremental_callback"
            ),
            "fill_price": payload.get("fill_price"),
            "fee": payload.get("fee"),
            "message": _optional(payload.get("message")),
            "reconstructed": reconstructed,
            "reconstruction_source": _optional(payload.get("reconstruction_source")),
            "remaining_quantity": payload.get("remaining_quantity"),
            "payload_extra": _extra(payload, known),
        }
    if event_type == "local_risk_event":
        known = {
            "risk_event_id", "security_id", "ticker", "risk_type", "stage",
            "event_time", "price", "entry", "entry_price", "anchor",
            "anchor_price", "cooldown_until",
        }
        return {
            "kind": event_type,
            **common,
            "risk_event_id": str(payload["risk_event_id"]),
            "security_id": str(payload["security_id"]),
            "ticker": _optional(payload.get("ticker")),
            "risk_type": str(payload["risk_type"]),
            "stage": str(payload["stage"]),
            "price": payload.get("price"),
            "entry_price": payload.get("entry_price", payload.get("entry")),
            "anchor_price": payload.get("anchor_price", payload.get("anchor")),
            "cooldown_until": _optional_cooldown_datetime(payload.get("cooldown_until")),
            "details": _extra(payload, known),
        }
    known = {
        "account_snapshot_id", "equity", "cash", "buying_power", "holdings",
        "open_orders", "state_generation", "state_active", "broker_reconciled",
        "pending_orders", "deferred_targets",
    }
    return {
        "kind": event_type,
        **common,
        "account_snapshot_id": str(payload["account_snapshot_id"]),
        "equity": payload["equity"],
        "cash": payload["cash"],
        "buying_power": payload.get("buying_power"),
        "state_generation": payload["state_generation"],
        "state_active": payload.get("state_active"),
        "broker_reconciled": bool(payload["broker_reconciled"]),
        "pending_order_count": _count(payload.get("pending_orders")),
        "deferred_target_count": _count(payload.get("deferred_targets")),
        "holdings": list(payload.get("holdings") or []),
        "open_orders": list(payload.get("open_orders") or []),
        "payload_extra": _extra(payload, known),
    }


async def project_observer_event(db: AsyncSession, raw: QCObserverEvent) -> bool:
    record = projection_record_for_event(raw)
    if record is None:
        return False
    kind = record.pop("kind")
    if kind == "decision_manifest":
        rankings = record.pop("ranked_candidates")
        if await _exists(db, QCDecisionManifestProjection, raw.id):
            return False
        parent = QCDecisionManifestProjection(**record)
        db.add(parent)
        await db.flush()
        for candidate in rankings:
            security_id = candidate.get("security_id")
            if security_id is None:
                continue
            known = {"security_id", "ticker", "rank", "score", "history_hash"}
            db.add(
                QCDecisionRankingProjection(
                    decision_manifest_id=parent.id,
                    raw_event_id=raw.id,
                    security_id=str(security_id),
                    ticker=_optional(candidate.get("ticker")),
                    rank=int(candidate["rank"]),
                    score=candidate.get("score"),
                    history_hash=_optional(candidate.get("history_hash")),
                    payload_extra=_extra(candidate, known),
                )
            )
        return True
    model = {
        "order_intent": QCOrderIntentProjection,
        "broker_lifecycle_event": QCOrderLifecycleProjection,
        "local_risk_event": QCLocalRiskProjection,
        "account_snapshot": QCAccountSnapshotProjection,
    }[kind]
    if await _exists(db, model, raw.id):
        return False
    holdings = record.pop("holdings", [])
    open_orders = record.pop("open_orders", [])
    parent = model(**_fields_for_model(record, model))
    db.add(parent)
    await db.flush()
    if kind == "account_snapshot":
        for holding in holdings:
            security_id = holding.get("security_id")
            if security_id is None:
                continue
            known = {"security_id", "ticker", "quantity", "average_price", "holdings_value"}
            db.add(
                QCAccountSnapshotHoldingProjection(
                    account_snapshot_id=parent.id,
                    raw_event_id=raw.id,
                    security_id=str(security_id),
                    ticker=_optional(holding.get("ticker")),
                    quantity=holding.get("quantity", 0),
                    average_price=holding.get("average_price"),
                    holdings_value=holding.get("holdings_value"),
                    payload_extra=_extra(holding, known),
                )
            )
        for order in open_orders:
            broker_order_id = order.get("broker_order_id")
            if broker_order_id is None:
                continue
            known = {"broker_order_id", "ticker", "quantity", "quantity_filled"}
            db.add(
                QCAccountSnapshotOpenOrderProjection(
                    account_snapshot_id=parent.id,
                    raw_event_id=raw.id,
                    broker_order_id=str(broker_order_id),
                    ticker=_optional(order.get("ticker")),
                    quantity=order.get("quantity"),
                    quantity_filled=order.get("quantity_filled"),
                    payload_extra=_extra(order, known),
                )
            )
    return True


async def project_pending_observer_events(
    db: AsyncSession,
    *,
    limit: int = 1000,
) -> dict[str, int]:
    checkpoint = await db.scalar(
        select(QCProjectionCheckpoint)
        .where(
            QCProjectionCheckpoint.projection_name == "observer_business_facts",
            QCProjectionCheckpoint.event_stream_id == "__all__",
            QCProjectionCheckpoint.event_channel == "ALL",
        )
        .with_for_update()
    )
    if checkpoint is None:
        checkpoint = QCProjectionCheckpoint(
            projection_name="observer_business_facts",
            projection_version=PROJECTION_VERSION,
            event_stream_id="__all__",
            event_channel="ALL",
            last_processed_sequence=0,
            status="ready",
        )
        db.add(checkpoint)
        await db.flush()
    excluded = exists(
        select(QCObserverEventAnnotation.id).where(
            QCObserverEventAnnotation.raw_event_id == QCObserverEvent.id,
            QCObserverEventAnnotation.excludes_from_evidence.is_(True),
        )
    )
    rows = (
        await db.scalars(
            select(QCObserverEvent)
            .where(
                QCObserverEvent.id > checkpoint.last_processed_sequence,
                QCObserverEvent.validation_status == "valid",
                QCObserverEvent.event_type.in_(PROJECTED_EVENT_TYPES),
                ~excluded,
            )
            .order_by(QCObserverEvent.id)
            .limit(max(1, limit))
        )
    ).all()
    projected = 0
    skipped = 0
    for raw in rows:
        if await project_observer_event(db, raw):
            projected += 1
        else:
            skipped += 1
    if rows:
        checkpoint.last_processed_sequence = int(rows[-1].id)
    checkpoint.projection_version = PROJECTION_VERSION
    checkpoint.status = "ready"
    checkpoint.last_error = None
    await db.flush()
    return {
        "scanned": len(rows),
        "projected": projected,
        "skipped": skipped,
        "checkpoint_raw_event_id": int(checkpoint.last_processed_sequence),
    }


async def run_observer_projection_batch(*, limit: int = 1000) -> dict[str, int]:
    """Run after raw commit; projection failure never invalidates the raw ACK."""
    from db.session import AsyncSessionLocal

    async with AsyncSessionLocal() as db:
        try:
            result = await project_pending_observer_events(db, limit=limit)
            await db.commit()
            return result
        except Exception:
            await db.rollback()
            logger.exception("QC observer projection batch failed; raw facts remain committed")
            return {
                "scanned": 0,
                "projected": 0,
                "skipped": 0,
                "checkpoint_raw_event_id": 0,
                "failed": 1,
            }


async def backfill_observer_projections(
    *,
    limit: int = 1000,
    max_batches: int = 100,
) -> dict[str, int]:
    """Drain historical raw facts after startup without blocking ingestion."""
    totals = {
        "scanned": 0,
        "projected": 0,
        "skipped": 0,
        "checkpoint_raw_event_id": 0,
        "batches": 0,
    }
    for _ in range(max(1, max_batches)):
        result = await run_observer_projection_batch(limit=limit)
        totals["batches"] += 1
        for key in ("scanned", "projected", "skipped"):
            totals[key] += int(result.get(key, 0))
        totals["checkpoint_raw_event_id"] = int(
            result.get("checkpoint_raw_event_id", totals["checkpoint_raw_event_id"])
        )
        if result.get("failed") or int(result.get("scanned", 0)) < limit:
            break
    logger.info("QC observer projection startup backfill complete: %s", totals)
    return totals


async def load_observer_projection_status(db: AsyncSession) -> dict[str, Any]:
    """Return read-only projection progress without changing the raw fact plane."""
    checkpoint = await db.scalar(
        select(QCProjectionCheckpoint).where(
            QCProjectionCheckpoint.projection_name == "observer_business_facts",
            QCProjectionCheckpoint.event_stream_id == "__all__",
            QCProjectionCheckpoint.event_channel == "ALL",
        )
    )
    excluded = exists(
        select(QCObserverEventAnnotation.id).where(
            QCObserverEventAnnotation.raw_event_id == QCObserverEvent.id,
            QCObserverEventAnnotation.excludes_from_evidence.is_(True),
        )
    )
    raw_projectable_count = int(
        await db.scalar(
            select(func.count(QCObserverEvent.id)).where(
                QCObserverEvent.validation_status == "valid",
                QCObserverEvent.event_type.in_(PROJECTED_EVENT_TYPES),
                ~excluded,
            )
        )
        or 0
    )
    projection_models = {
        "decision_manifests": QCDecisionManifestProjection,
        "order_intents": QCOrderIntentProjection,
        "broker_lifecycle_events": QCOrderLifecycleProjection,
        "local_risk_events": QCLocalRiskProjection,
        "account_snapshots": QCAccountSnapshotProjection,
    }
    counts = {
        name: int(await db.scalar(select(func.count(model.id))) or 0)
        for name, model in projection_models.items()
    }
    projected_parent_count = sum(counts.values())
    return {
        "schema_version": "qc_observer_projection_status.v1",
        "read_only": True,
        "projection_name": "observer_business_facts",
        "projection_version": PROJECTION_VERSION,
        "checkpoint_raw_event_id": (
            int(checkpoint.last_processed_sequence) if checkpoint is not None else 0
        ),
        "checkpoint_status": checkpoint.status if checkpoint is not None else "not_started",
        "checkpoint_error": checkpoint.last_error if checkpoint is not None else None,
        "raw_projectable_count": raw_projectable_count,
        "projected_parent_count": projected_parent_count,
        "pending_parent_count": max(raw_projectable_count - projected_parent_count, 0),
        "counts": counts,
    }


async def _exists(db: AsyncSession, model: Any, raw_event_id: int) -> bool:
    return (
        await db.scalar(select(model.id).where(model.raw_event_id == raw_event_id))
    ) is not None


def _fields_for_model(record: dict[str, Any], model: Any) -> dict[str, Any]:
    allowed = set(model.__table__.columns.keys())
    return {key: value for key, value in record.items() if key in allowed}


def _datetime(value: Any) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("projection timestamp must include timezone")
    return parsed


def _optional_datetime(value: Any) -> datetime | None:
    return None if value is None or value == "" else _datetime(value)


def _optional_cooldown_datetime(value: Any) -> datetime | None:
    """Normalize QC's session-date cooldown marker without weakening timestamps."""
    if value is None or value == "":
        return None
    text = str(value)
    try:
        cooldown_date = date.fromisoformat(text)
    except ValueError:
        return _datetime(value)
    return datetime.combine(cooldown_date, time.min, tzinfo=timezone.utc)


def _optional(value: Any) -> str | None:
    return None if value is None or value == "" else str(value)


def _extra(payload: dict[str, Any], known: set[str]) -> dict[str, Any]:
    return {key: value for key, value in payload.items() if key not in known}


def _count(value: Any) -> int:
    if isinstance(value, (list, dict)):
        return len(value)
    if value is None:
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0
