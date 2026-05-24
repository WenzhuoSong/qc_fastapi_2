"""Polling-based QC ACK tracker backed by the execution_log table."""
from __future__ import annotations

import asyncio


async def get_qc_status(command_id: str) -> str | None:
    from services.execution_log_store import get_qc_status as _get_qc_status

    return await _get_qc_status(command_id)


async def get_qc_ack_detail(command_id: str) -> dict:
    from services.execution_log_store import get_execution_log_by_command_id

    row = await get_execution_log_by_command_id(command_id)
    if not row:
        return {"qc_status": None}
    return {
        "qc_status": row.qc_status,
        "qc_rejection_reason": row.qc_rejection_reason,
        "qc_response": row.qc_response,
        "qc_ack_at": row.qc_ack_at.isoformat() if row.qc_ack_at else None,
    }


async def mark_timeout(command_id: str) -> None:
    from services.execution_log_store import mark_timeout as _mark_timeout

    await _mark_timeout(command_id)


async def wait_for_qc_ack(command_id: str, timeout_seconds: int = 30) -> str:
    for _ in range(max(timeout_seconds, 0)):
        await asyncio.sleep(1)
        status = await get_qc_status(command_id)
        if status in {"accepted", "rejected"}:
            return status
    await mark_timeout(command_id)
    return "timeout_no_ack"


async def wait_for_qc_ack_detail(command_id: str, timeout_seconds: int = 30) -> dict:
    status = await wait_for_qc_ack(command_id, timeout_seconds=timeout_seconds)
    detail = await get_qc_ack_detail(command_id)
    detail["qc_status"] = status
    return detail
