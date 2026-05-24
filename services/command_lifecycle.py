"""Append-only command lifecycle events."""
from __future__ import annotations

from datetime import UTC, datetime
from typing import Any


VALID_EVENT_TYPES = {
    "created",
    "preflight_passed",
    "preflight_blocked",
    "submitted_to_qc",
    "qc_accepted",
    "qc_rejected",
    "qc_timeout",
    "execution_result",
    "reconciled",
    "reconciliation_drift",
}


def build_command_lifecycle_event(
    *,
    command_id: str,
    event_type: str,
    analysis_id: int | None = None,
    event_status: str | None = None,
    source: str = "fastapi",
    reason: str | None = None,
    payload: dict[str, Any] | None = None,
    event_time: datetime | None = None,
) -> dict[str, Any]:
    """Return a normalized lifecycle event payload."""
    command = str(command_id or "").strip()
    if not command:
        raise ValueError("command_id is required for lifecycle events")
    event = str(event_type or "").strip()
    if event not in VALID_EVENT_TYPES:
        raise ValueError(f"unknown command lifecycle event_type: {event}")
    return {
        "command_id": command,
        "analysis_id": analysis_id,
        "event_type": event,
        "event_status": str(event_status or event).strip(),
        "event_time": _strip_tz(event_time or datetime.now(UTC)),
        "source": str(source or "fastapi").strip(),
        "reason": reason,
        "payload": payload or {},
    }


async def append_command_lifecycle_event(
    db,
    *,
    command_id: str,
    event_type: str,
    analysis_id: int | None = None,
    event_status: str | None = None,
    source: str = "fastapi",
    reason: str | None = None,
    payload: dict[str, Any] | None = None,
    event_time: datetime | None = None,
) -> None:
    """Append an event to the current DB session."""
    from db.models import CommandLifecycleEvent

    event = build_command_lifecycle_event(
        command_id=command_id,
        analysis_id=analysis_id,
        event_type=event_type,
        event_status=event_status,
        source=source,
        reason=reason,
        payload=payload,
        event_time=event_time,
    )
    db.add(CommandLifecycleEvent(**event))


async def load_command_lifecycle(command_id: str) -> list[dict[str, Any]]:
    """Load ordered lifecycle events for one command."""
    from sqlalchemy import select

    from db.models import CommandLifecycleEvent
    from db.session import AsyncSessionLocal

    async with AsyncSessionLocal() as db:
        rows = (
            await db.execute(
                select(CommandLifecycleEvent)
                .where(CommandLifecycleEvent.command_id == command_id)
                .order_by(CommandLifecycleEvent.event_time, CommandLifecycleEvent.id)
            )
        ).scalars().all()
    return [_event_to_dict(row) for row in rows]


def _event_to_dict(row) -> dict[str, Any]:
    return {
        "id": row.id,
        "command_id": row.command_id,
        "analysis_id": row.analysis_id,
        "event_type": row.event_type,
        "event_status": row.event_status,
        "event_time": row.event_time.isoformat() if row.event_time else None,
        "source": row.source,
        "reason": row.reason,
        "payload": row.payload or {},
    }


def _strip_tz(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(UTC).replace(tzinfo=None)
