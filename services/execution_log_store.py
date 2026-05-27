"""DB access helpers for execution command lifecycle records."""
from __future__ import annotations

from datetime import UTC, datetime, time
from typing import Any

from sqlalchemy import select
from sqlalchemy import desc

from db.models import CommandLifecycleEvent, ExecutionLog
from db.session import AsyncSessionLocal
from services.command_lifecycle import append_command_lifecycle_event, build_command_reconciliation_events


def _utcnow_db_naive() -> datetime:
    """Return UTC time in the naive form expected by DateTime columns."""
    return datetime.now(UTC).replace(tzinfo=None)


async def create_or_update_submitted_log(
    *,
    command_id: str,
    target_weights: dict[str, Any],
    analysis_id: int | None = None,
    policy_version: str | None = None,
    preflight_result: dict[str, Any] | None = None,
    policy_sync_result: dict[str, Any] | None = None,
    qc_response: dict[str, Any] | None = None,
) -> None:
    async with AsyncSessionLocal() as db:
        result = await db.execute(select(ExecutionLog).where(ExecutionLog.command_id == command_id))
        row = result.scalar_one_or_none()
        payload = {
            "action_status": "sent",
            "command_id": command_id,
            "sent_weights": target_weights,
            "policy_version": policy_version,
            "command_preflight": preflight_result or {},
            "policy_sync": policy_sync_result or {},
            "recorded_at": datetime.now(UTC).isoformat(),
        }
        if row:
            row.analysis_id = analysis_id or row.analysis_id
            row.command_type = row.command_type or "weight_adjustment"
            row.command_payload = payload
            row.qc_response = qc_response or row.qc_response
            row.status = "sent"
            row.qc_status = row.qc_status or "submitted"
        else:
            db.add(
                ExecutionLog(
                    analysis_id=analysis_id,
                    command_id=command_id,
                    command_type="weight_adjustment",
                    command_payload=payload,
                    qc_response=qc_response,
                    status="sent",
                    qc_status="submitted",
                )
            )
        await append_command_lifecycle_event(
            db,
            command_id=command_id,
            analysis_id=analysis_id,
            event_type="created",
            event_status="sent",
            source="fastapi",
            payload={
                "target_weights": target_weights,
                "policy_version": policy_version,
                "preflight_result": preflight_result or {},
            },
        )
        await append_command_lifecycle_event(
            db,
            command_id=command_id,
            analysis_id=analysis_id,
            event_type="submitted_to_qc",
            event_status="submitted",
            source="fastapi",
            payload={
                "policy_sync_result": policy_sync_result or {},
                "qc_response": qc_response or {},
            },
        )
        await db.commit()


async def create_or_update_policy_sync_log(
    *,
    command_id: str,
    analysis_id: int | None = None,
    policy_version: str | None = None,
    policy_payload: dict[str, Any] | None = None,
    qc_response: dict[str, Any] | None = None,
    status: str = "pending_send",
    qc_status: str = "pending",
) -> None:
    """Create a local row before PolicySync is sent so fast QC ACKs are not lost."""
    payload = {
        "action_status": status,
        "command_id": command_id,
        "policy_version": policy_version,
        "policy_payload": policy_payload or {},
        "qc_response": qc_response or {},
        "recorded_at": datetime.now(UTC).isoformat(),
    }
    async with AsyncSessionLocal() as db:
        result = await db.execute(select(ExecutionLog).where(ExecutionLog.command_id == command_id))
        row = result.scalar_one_or_none()
        if row:
            row.analysis_id = analysis_id or row.analysis_id
            row.command_type = row.command_type or "policy_sync"
            row.command_payload = payload
            row.qc_response = qc_response or row.qc_response
            row.status = status
            if row.qc_status not in {"accepted", "rejected"}:
                row.qc_status = qc_status
        else:
            db.add(
                ExecutionLog(
                    analysis_id=analysis_id,
                    command_id=command_id,
                    command_type="policy_sync",
                    command_payload=payload,
                    qc_response=qc_response,
                    status=status,
                    qc_status=qc_status,
                )
            )
        await append_command_lifecycle_event(
            db,
            command_id=command_id,
            analysis_id=analysis_id,
            event_type="created" if status == "pending_send" else "submitted_to_qc",
            event_status=status,
            source="fastapi",
            payload=payload,
        )
        await db.commit()


async def update_execution_result(
    *,
    command_id: str,
    analysis_id: int | None,
    audit_payload: dict[str, Any],
    qc_response: dict[str, Any] | None,
    status: str,
) -> None:
    async with AsyncSessionLocal() as db:
        result = await db.execute(select(ExecutionLog).where(ExecutionLog.command_id == command_id))
        row = result.scalar_one_or_none()
        if row:
            row.analysis_id = analysis_id or row.analysis_id
            row.command_payload = audit_payload
            row.qc_response = qc_response or row.qc_response
            row.status = status
            if status in {"failed", "rejected", "skipped"} and row.qc_ack_at is None and row.qc_status in {None, "submitted"}:
                row.qc_status = "not_sent"
        else:
            db.add(
                ExecutionLog(
                    analysis_id=analysis_id,
                    command_id=command_id,
                    command_type="weight_adjustment",
                    command_payload=audit_payload,
                    qc_response=qc_response,
                    status=status,
                    qc_status="not_sent" if status in {"failed", "rejected", "skipped"} else "submitted",
                )
            )
        event_type = "execution_result"
        if status == "rejected":
            event_type = "preflight_blocked"
        await append_command_lifecycle_event(
            db,
            command_id=command_id,
            analysis_id=analysis_id,
            event_type=event_type,
            event_status=status,
            source="fastapi",
            reason=audit_payload.get("reason") if isinstance(audit_payload, dict) else None,
            payload={
                "audit_payload": audit_payload,
                "qc_response": qc_response or {},
            },
        )
        await db.commit()


async def record_preflight_block(
    *,
    command_id: str,
    analysis_id: int | None,
    target_weights: dict[str, Any],
    preflight_result: dict[str, Any],
    policy_version: str | None,
    policy_sync_result: dict[str, Any] | None = None,
) -> None:
    payload = {
        "action_status": "rejected",
        "command_id": command_id,
        "sent_weights": {},
        "proposed_weights": target_weights,
        "policy_version": policy_version,
        "command_preflight": preflight_result,
        "policy_sync": policy_sync_result or {},
        "reason": "blocked_by_command_preflight",
        "recorded_at": datetime.now(UTC).isoformat(),
    }
    await update_execution_result(
        command_id=command_id,
        analysis_id=analysis_id,
        audit_payload=payload,
        qc_response=None,
        status="rejected",
    )


async def update_qc_status(
    command_id: str,
    qc_status: str,
    rejection_reason: str | None = None,
    qc_response: dict[str, Any] | None = None,
) -> None:
    ack_time = _utcnow_db_naive()
    values: dict[str, Any] = {
        "qc_status": qc_status,
        "qc_ack_at": ack_time,
    }
    if rejection_reason is not None:
        values["qc_rejection_reason"] = rejection_reason
    if qc_response is not None:
        values["qc_response"] = qc_response
    async with AsyncSessionLocal() as db:
        result = await db.execute(select(ExecutionLog).where(ExecutionLog.command_id == command_id))
        row = result.scalar_one_or_none()
        if row:
            row.qc_status = qc_status
            row.qc_ack_at = ack_time
            if rejection_reason is not None:
                row.qc_rejection_reason = rejection_reason
            if qc_response is not None:
                row.qc_response = qc_response
        else:
            row = ExecutionLog(
                analysis_id=_analysis_id_from_command_id(command_id),
                command_id=command_id,
                command_type=_command_type_from_id(command_id),
                command_payload={
                    "action_status": "qc_ack_without_local_row",
                    "command_id": command_id,
                    "reason": rejection_reason,
                    "qc_response": qc_response or {},
                    "recorded_at": datetime.now(UTC).isoformat(),
                },
                qc_response=qc_response,
                status="ack_received",
                qc_status=qc_status,
                qc_ack_at=ack_time,
                qc_rejection_reason=rejection_reason,
            )
            db.add(row)
        event_type = {
            "accepted": "qc_accepted",
            "rejected": "qc_rejected",
            "timeout_no_ack": "qc_timeout",
        }.get(qc_status, "execution_result")
        await append_command_lifecycle_event(
            db,
            command_id=command_id,
            event_type=event_type,
            event_status=qc_status,
            source="qc" if qc_status in {"accepted", "rejected"} else "fastapi",
            reason=rejection_reason,
            payload=qc_response or {},
            event_time=ack_time,
        )
        await _append_reconciliation_events(
            db,
            command_id=command_id,
            analysis_id=getattr(row, "analysis_id", None),
            command_payload=(getattr(row, "command_payload", None) or {}),
            qc_response=qc_response or {},
            event_time=ack_time,
        )
        await db.commit()


async def get_qc_status(command_id: str) -> str | None:
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(ExecutionLog.qc_status).where(ExecutionLog.command_id == command_id)
        )
        return result.scalar_one_or_none()


async def get_execution_log_by_command_id(command_id: str) -> ExecutionLog | None:
    async with AsyncSessionLocal() as db:
        result = await db.execute(select(ExecutionLog).where(ExecutionLog.command_id == command_id))
        return result.scalar_one_or_none()


async def command_submission_state(command_id: str, analysis_id: int | None = None) -> dict[str, Any]:
    async with AsyncSessionLocal() as db:
        command_row = None
        if command_id:
            command_row = (
                await db.execute(select(ExecutionLog).where(ExecutionLog.command_id == command_id))
            ).scalar_one_or_none()
        analysis_row = None
        if analysis_id is not None:
            analysis_row = (
                await db.execute(
                    select(ExecutionLog)
                    .where(ExecutionLog.analysis_id == analysis_id)
                    .where(ExecutionLog.command_type == "weight_adjustment")
                    .order_by(desc(ExecutionLog.executed_at))
                    .limit(1)
                )
            ).scalar_one_or_none()
    return {
        "command_id_exists": command_row is not None,
        "command_id_status": getattr(command_row, "status", None),
        "command_id_qc_status": getattr(command_row, "qc_status", None),
        "analysis_id_submitted": analysis_row is not None,
        "analysis_command_id": getattr(analysis_row, "command_id", None),
        "analysis_status": getattr(analysis_row, "status", None),
        "analysis_qc_status": getattr(analysis_row, "qc_status", None),
    }


async def summarize_today_execution_activity(now: datetime | None = None) -> dict[str, Any]:
    now = now or _utcnow_db_naive()
    start = datetime.combine(now.date(), time.min)
    async with AsyncSessionLocal() as db:
        rows = (
            await db.execute(
                select(ExecutionLog)
                .where(ExecutionLog.command_type == "weight_adjustment")
                .where(ExecutionLog.executed_at >= start)
            )
        ).scalars().all()
    command_rows = [row for row in rows if _counts_toward_daily_command(row)]
    gross_turnover = 0.0
    for row in command_rows:
        if not _counts_toward_daily_turnover(row):
            continue
        payload = getattr(row, "command_payload", None) or {}
        preflight = payload.get("command_preflight") or {}
        metrics = preflight.get("metrics") or {}
        try:
            gross_turnover += float(metrics.get("gross_turnover") or 0.0)
        except (TypeError, ValueError):
            continue
    return {
        "command_count": len(command_rows),
        "gross_turnover": round(gross_turnover, 6),
    }


def _counts_toward_daily_command(row: Any) -> bool:
    """Count only weight commands that were actually sent or are pending/accepted."""
    if getattr(row, "command_type", None) != "weight_adjustment":
        return False
    status = str(getattr(row, "status", "") or "").lower()
    qc_status = str(getattr(row, "qc_status", "") or "").lower()
    if qc_status in {"not_sent", "rejected"}:
        return False
    return status in {"sent", "accepted", "timeout_no_ack"} or qc_status in {"submitted", "accepted", "timeout_no_ack"}


def _counts_toward_daily_turnover(row: Any) -> bool:
    if not _counts_toward_daily_command(row):
        return False
    qc_status = str(getattr(row, "qc_status", "") or "").lower()
    return qc_status in {"submitted", "accepted", "timeout_no_ack"}


async def mark_timeout(command_id: str) -> None:
    await update_qc_status(command_id, "timeout_no_ack", rejection_reason="no QC ack before timeout")


def _command_type_from_id(command_id: str) -> str:
    command = str(command_id or "")
    if command.endswith("_policy"):
        return "policy_sync"
    return "weight_adjustment" if command.startswith("analysis_") else "unknown"


def _analysis_id_from_command_id(command_id: str) -> int | None:
    command = str(command_id or "")
    if command.endswith("_policy"):
        command = command[:-7]
    if not command.startswith("analysis_"):
        return None
    try:
        return int(command.split("_", 1)[1])
    except (TypeError, ValueError, IndexError):
        return None


async def append_reconciliation_from_account_snapshot(db, account_state: dict[str, Any]) -> int:
    """Append delayed reconciliation events from a QC heartbeat account state."""
    raw = account_state.get("raw_snapshot") if isinstance(account_state.get("raw_snapshot"), dict) else {}
    command_id = str(raw.get("last_command_id") or "").strip()
    if not command_id:
        return 0
    result = await db.execute(select(ExecutionLog).where(ExecutionLog.command_id == command_id))
    row = result.scalar_one_or_none()
    if not row or str(row.qc_status or "").lower() != "accepted":
        return 0
    return await _append_reconciliation_events(
        db,
        command_id=command_id,
        analysis_id=row.analysis_id,
        command_payload=row.command_payload or {},
        qc_response=row.qc_response or {"status": "accepted"},
        account_state={
            "timestamp_utc": raw.get("timestamp_utc"),
            "policy_version": account_state.get("policy_version"),
            "open_order_count": account_state.get("open_order_count"),
            "has_open_orders": account_state.get("has_open_orders"),
            "holdings_weights": account_state.get("holdings_weights") or {},
            "target_weights": account_state.get("target_weights") or {},
            "last_command_id": command_id,
        },
        event_time=account_state.get("recorded_at"),
    )


async def _append_reconciliation_events(
    db,
    *,
    command_id: str,
    analysis_id: int | None,
    command_payload: dict[str, Any],
    qc_response: dict[str, Any],
    account_state: dict[str, Any] | None = None,
    event_time: datetime | None = None,
) -> int:
    events = build_command_reconciliation_events(
        command_id=command_id,
        analysis_id=analysis_id,
        command_payload=command_payload,
        qc_response=qc_response,
        account_state=account_state,
        event_time=event_time,
    )
    if not events:
        return 0

    existing = (
        await db.execute(
            select(CommandLifecycleEvent.event_type)
            .where(CommandLifecycleEvent.command_id == command_id)
            .where(CommandLifecycleEvent.event_type.in_(("filled", "partial", "reconciled", "reconciliation_drift")))
        )
    ).scalars().all()
    existing_types = set(existing)
    if "reconciled" in existing_types:
        return 0

    appended = 0
    for event in events:
        event_type = event["event_type"]
        if event_type in existing_types and event_type in {"filled", "partial", "reconciliation_drift"}:
            continue
        await append_command_lifecycle_event(
            db,
            command_id=event["command_id"],
            analysis_id=event.get("analysis_id"),
            event_type=event_type,
            event_status=event.get("event_status"),
            source=event.get("source") or "fastapi",
            reason=event.get("reason"),
            payload=event.get("payload") or {},
            event_time=event.get("event_time"),
        )
        appended += 1
    return appended
