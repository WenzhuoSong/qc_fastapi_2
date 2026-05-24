"""Execution lifecycle endpoints."""
from __future__ import annotations

from typing import Any

from pydantic import BaseModel
from fastapi import APIRouter, HTTPException, Request

from services.execution_log_store import update_qc_status
from services.qc_webhook_auth import verify_qc_signature


router = APIRouter(tags=["execution"])


class QCExecutionAck(BaseModel):
    cmd_id: str
    status: str
    reason: str | None = None
    policy_version: str | None = None
    policy_mismatch: bool = False
    actual_target_weights: dict[str, float] | None = None
    actual_holdings_weights: dict[str, float] | None = None
    order_summary: dict[str, Any] | None = None
    fill_summary: dict[str, Any] | None = None
    account_state: dict[str, Any] | None = None
    qc_timestamp: str | None = None
    rejected_tickers: list[str] | None = None


@router.post("/execution/qc_ack")
async def receive_qc_ack(request: Request, ack: QCExecutionAck):
    body = await request.body()
    signature = request.headers.get("X-QC-Signature")
    if not verify_qc_signature(body, signature):
        raise HTTPException(status_code=401, detail="Invalid QC signature")

    status = ack.status.lower().strip()
    if status not in {"accepted", "rejected"}:
        raise HTTPException(status_code=400, detail="Invalid QC status")

    await update_qc_status(
        ack.cmd_id,
        status,
        rejection_reason=ack.reason,
        qc_response=ack.model_dump(),
    )
    return {"received": True}
