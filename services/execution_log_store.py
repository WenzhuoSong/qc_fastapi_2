"""DB access helpers for execution command lifecycle records."""
from __future__ import annotations

from datetime import UTC, datetime, time
from typing import Any

from sqlalchemy import select, update
from sqlalchemy import desc

from db.models import ExecutionLog
from db.session import AsyncSessionLocal


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
        else:
            db.add(
                ExecutionLog(
                    analysis_id=analysis_id,
                    command_id=command_id,
                    command_type="weight_adjustment",
                    command_payload=audit_payload,
                    qc_response=qc_response,
                    status=status,
                    qc_status="submitted",
                )
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
    values: dict[str, Any] = {
        "qc_status": qc_status,
        "qc_ack_at": _utcnow_db_naive(),
    }
    if rejection_reason is not None:
        values["qc_rejection_reason"] = rejection_reason
    if qc_response is not None:
        values["qc_response"] = qc_response
    async with AsyncSessionLocal() as db:
        await db.execute(
            update(ExecutionLog)
            .where(ExecutionLog.command_id == command_id)
            .values(**values)
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
    command_rows = [
        row for row in rows
        if getattr(row, "status", None) in {"sent", "accepted", "timeout_no_ack"}
        or getattr(row, "qc_status", None) in {"submitted", "accepted", "rejected", "timeout_no_ack"}
    ]
    gross_turnover = 0.0
    for row in command_rows:
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


async def mark_timeout(command_id: str) -> None:
    await update_qc_status(command_id, "timeout_no_ack", rejection_reason="no QC ack before timeout")
