"""DB access helpers for execution command lifecycle records."""
from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select, update

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
    qc_response: dict[str, Any] | None = None,
) -> None:
    async with AsyncSessionLocal() as db:
        result = await db.execute(select(ExecutionLog).where(ExecutionLog.command_id == command_id))
        row = result.scalar_one_or_none()
        payload = {
            "action_status": "sent",
            "command_id": command_id,
            "sent_weights": target_weights,
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


async def mark_timeout(command_id: str) -> None:
    await update_qc_status(command_id, "timeout_no_ack", rejection_reason="no QC ack before timeout")
