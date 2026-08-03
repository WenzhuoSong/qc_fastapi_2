"""Read-only health classification for committed QC observer receipts."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any


CHANNEL_FRESHNESS_LIMITS = {
    "P0": timedelta(days=8),
    "P1": timedelta(hours=36),
    "P2": timedelta(hours=36),
}


def classify_observer_receipt(
    receipt: Any,
    *,
    now: datetime | None = None,
    excluded_artifact_events: int = 0,
) -> dict[str, Any]:
    observed_at = now or datetime.now(timezone.utc)
    if observed_at.tzinfo is None:
        observed_at = observed_at.replace(tzinfo=timezone.utc)
    event_time = getattr(receipt, "last_event_time", None)
    if event_time is not None and event_time.tzinfo is None:
        event_time = event_time.replace(tzinfo=timezone.utc)
    channel = str(getattr(receipt, "event_channel", ""))
    gaps = list(getattr(receipt, "missing_sequences", None) or [])
    age_seconds = None
    if event_time is not None:
        age_seconds = max(0.0, (observed_at - event_time).total_seconds())
    limit = CHANNEL_FRESHNESS_LIMITS.get(channel, timedelta(hours=36))
    stale = age_seconds is None or age_seconds > limit.total_seconds()
    strict_gap = channel == "P0" and bool(gaps)
    if strict_gap:
        status = "audit_incomplete"
    elif stale:
        status = "stale"
    else:
        status = "healthy"
    return {
        "event_stream_id": str(getattr(receipt, "event_stream_id", "")),
        "event_channel": channel,
        "status": status,
        "highest_contiguous_sequence": int(
            getattr(receipt, "highest_contiguous_sequence", 0) or 0
        ),
        "highest_seen_sequence": int(getattr(receipt, "highest_seen_sequence", 0) or 0),
        "missing_sequences": gaps,
        "last_event_id": getattr(receipt, "last_event_id", None),
        "last_event_time": event_time.isoformat() if event_time is not None else None,
        "age_seconds": age_seconds,
        "freshness_limit_seconds": limit.total_seconds(),
        "excluded_artifact_events": int(excluded_artifact_events),
    }


def summarize_observer_receipts(
    receipts: list[Any],
    *,
    now: datetime | None = None,
    artifact_counts: dict[tuple[str, str], int] | None = None,
) -> dict[str, Any]:
    counts = artifact_counts or {}
    rows = [
        classify_observer_receipt(
            receipt,
            now=now,
            excluded_artifact_events=counts.get(
                (
                    str(getattr(receipt, "event_stream_id", "")),
                    str(getattr(receipt, "event_channel", "")),
                ),
                0,
            ),
        )
        for receipt in receipts
    ]
    if not rows:
        overall = "awaiting_first_event"
    elif any(row["status"] == "audit_incomplete" for row in rows):
        overall = "audit_incomplete"
    elif any(row["status"] == "stale" for row in rows):
        overall = "degraded"
    else:
        overall = "healthy"
    return {
        "read_only": True,
        "execution_authority": "none",
        "overall_status": overall,
        "stream_count": len({row["event_stream_id"] for row in rows}),
        "receipt_count": len(rows),
        "excluded_artifact_event_count": sum(
            row["excluded_artifact_events"] for row in rows
        ),
        "receipts": rows,
    }


async def load_current_observer_summary(
    db: Any | None = None,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Load health for the newest non-smoke deployment only.

    Historical deployments remain audit evidence, but they must never make the
    currently running observer look stale. The caller may provide a session so
    this can participate in an existing read transaction.
    """
    from sqlalchemy import desc, func, select

    from db.models import (
        QCObserverEvent,
        QCObserverEventAnnotation,
        QCObserverRequestNonce,
        QCObserverStreamReceipt,
    )
    from db.session import AsyncSessionLocal
    from services.execution_authority import execution_authority_status

    async def _load(session: Any) -> dict[str, Any]:
        latest_event = await session.scalar(
            select(QCObserverEvent)
            .where(~QCObserverEvent.event_stream_id.like("observer-smoke:%"))
            .order_by(desc(QCObserverEvent.received_at), desc(QCObserverEvent.id))
            .limit(1)
        )
        fence = execution_authority_status()
        if latest_event is None:
            return {
                "available": False,
                "schema_version": "qc_observer_current_status_v1",
                "status": "awaiting_first_event",
                "overall_status": "awaiting_first_event",
                "should_alert": True,
                "reason": "no_observer_events",
                "read_only": True,
                "execution_authority": "none",
                "execution_fence": fence,
                "channels": [],
                "event_type_counts": {},
            }

        stream_id = str(latest_event.event_stream_id)
        receipts = list(
            (
                await session.scalars(
                    select(QCObserverStreamReceipt)
                    .where(QCObserverStreamReceipt.event_stream_id == stream_id)
                    .order_by(QCObserverStreamReceipt.event_channel)
                )
            ).all()
        )
        artifact_rows = (
            await session.execute(
                select(
                    QCObserverEvent.event_channel,
                    func.count(func.distinct(QCObserverEvent.id)),
                )
                .join(
                    QCObserverEventAnnotation,
                    QCObserverEventAnnotation.raw_event_id == QCObserverEvent.id,
                )
                .where(
                    QCObserverEvent.event_stream_id == stream_id,
                    QCObserverEventAnnotation.excludes_from_evidence.is_(True),
                )
                .group_by(QCObserverEvent.event_channel)
            )
        ).all()
        artifact_counts = {
            (stream_id, str(channel)): int(count)
            for channel, count in artifact_rows
        }
        health = summarize_observer_receipts(
            receipts,
            now=now,
            artifact_counts=artifact_counts,
        )
        event_type_rows = (
            await session.execute(
                select(QCObserverEvent.event_type, func.count(QCObserverEvent.id))
                .where(QCObserverEvent.event_stream_id == stream_id)
                .group_by(QCObserverEvent.event_type)
                .order_by(QCObserverEvent.event_type)
            )
        ).all()
        invalid_count = int(
            await session.scalar(
                select(func.count(QCObserverEvent.id)).where(
                    QCObserverEvent.event_stream_id == stream_id,
                    QCObserverEvent.validation_status != "valid",
                )
            )
            or 0
        )
        raw_event_count = int(sum(int(count) for _, count in event_type_rows))
        excluded_count = int(health.get("excluded_artifact_event_count") or 0)
        status = str(health.get("overall_status") or "unknown")
        should_alert = status not in {"healthy"}
        latest_v2_request = await session.scalar(
            select(QCObserverRequestNonce)
            .where(
                QCObserverRequestNonce.account_id == latest_event.account_id,
                QCObserverRequestNonce.deployment_id == latest_event.deployment_id,
            )
            .order_by(desc(QCObserverRequestNonce.created_at))
            .limit(1)
        )
        return {
            "available": True,
            "schema_version": "qc_observer_current_status_v1",
            "status": status,
            "overall_status": status,
            "should_alert": should_alert,
            "reason": "observer_current_stream_healthy" if not should_alert else f"observer_{status}",
            "read_only": True,
            "execution_authority": "none",
            "target_weight_mutation": "none",
            "execution_fence": fence,
            "event_stream_id": stream_id,
            "account_id": latest_event.account_id,
            "deployment_id": latest_event.deployment_id,
            "strategy_id": latest_event.strategy_id,
            "source_fingerprint": latest_event.source_fingerprint,
            "policy_fingerprint": latest_event.policy_fingerprint,
            "last_event_type": latest_event.event_type,
            "last_event_time": _iso(latest_event.event_time),
            "last_received_at": _iso(latest_event.received_at),
            "raw_event_count": raw_event_count,
            "effective_event_count": max(raw_event_count - excluded_count, 0),
            "excluded_event_count": excluded_count,
            "invalid_event_count": invalid_count,
            "request_auth_version": "v2" if latest_v2_request is not None else "legacy_or_unknown",
            "last_v2_request_at": (
                _iso(latest_v2_request.created_at)
                if latest_v2_request is not None
                else None
            ),
            "channels": health.get("receipts") or [],
            "event_type_counts": {
                str(event_type): int(count) for event_type, count in event_type_rows
            },
        }

    if db is not None:
        return await _load(db)
    async with AsyncSessionLocal() as session:
        return await _load(session)


def format_current_observer_report(summary: dict[str, Any]) -> str:
    """Return a compact operator-facing report for Telegram and cron logs."""
    if not summary.get("available"):
        return "QC Observer: unavailable\n- Reason: " + str(summary.get("reason") or "unknown")

    channels = {str(row.get("event_channel")): row for row in summary.get("channels") or []}
    channel_bits = []
    for name in ("P0", "P1", "P2"):
        row = channels.get(name)
        if row is None:
            channel_bits.append(f"{name}=missing")
            continue
        contiguous = int(row.get("highest_contiguous_sequence") or 0)
        seen = int(row.get("highest_seen_sequence") or 0)
        channel_bits.append(f"{name}={contiguous}/{seen} {row.get('status', 'unknown')}")

    event_counts = summary.get("event_type_counts") or {}
    lifecycle_count = sum(
        int(count)
        for event_type, count in event_counts.items()
        if any(token in str(event_type).lower() for token in ("order", "fill", "broker"))
    )
    lines = [
        f"QC Observer: {summary.get('status', 'unknown')}",
        f"- Strategy: {summary.get('strategy_id') or 'unknown'}",
        f"- Deployment: {summary.get('deployment_id') or 'unknown'}",
        f"- Channels: {', '.join(channel_bits)}",
        f"- Evidence: {summary.get('effective_event_count', 0)} effective, "
        f"{summary.get('invalid_event_count', 0)} invalid, {lifecycle_count} order/fill events",
        f"- Latest: {summary.get('last_event_type') or 'unknown'} at "
        f"{summary.get('last_event_time') or 'unknown'}",
        "- Execution authority: NONE (hard fence)",
    ]
    return "\n".join(lines)


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    return value.isoformat() if hasattr(value, "isoformat") else str(value)
