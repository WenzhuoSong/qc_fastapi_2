"""Execution lifecycle endpoints."""
from __future__ import annotations

import logging
from typing import Any

from pydantic import BaseModel
from fastapi import APIRouter, HTTPException, Request

from services.account_snapshot_store import ingest_execution_ack_snapshot
from services.execution_log_store import update_qc_status
from services.qc_webhook_auth import verify_qc_signature


router = APIRouter(tags=["execution"])
logger = logging.getLogger(__name__)

VALID_QC_EXECUTION_STATUSES = {
    "accepted",
    "rejected",
    "orders_submitted",
    "partial",
    "filled",
    "canceled",
    "reconciled",
    "reconciliation_drift",
    "failed_no_fill",
    "superseded",
}


class QCExecutionAck(BaseModel):
    cmd_id: str
    status: str
    reason: str | None = None
    execution_state: str | None = None
    active_command_id: str | None = None
    policy_version: str | None = None
    policy_mismatch: bool = False
    actual_target_weights: dict[str, float] | None = None
    actual_holdings_weights: dict[str, float] | None = None
    order_summary: dict[str, Any] | None = None
    fill_summary: dict[str, Any] | None = None
    account_state: dict[str, Any] | None = None
    superseded_command_id: str | None = None
    canceled_order_count: int | None = None
    qc_timestamp: str | None = None
    rejected_tickers: list[str] | None = None


@router.post("/execution/qc_ack")
async def receive_qc_ack(request: Request, ack: QCExecutionAck):
    body = await request.body()
    signature = request.headers.get("X-QC-Signature")
    if not verify_qc_signature(body, signature):
        raise HTTPException(status_code=401, detail="Invalid QC signature")

    status = ack.status.lower().strip()
    if status not in VALID_QC_EXECUTION_STATUSES:
        raise HTTPException(status_code=400, detail="Invalid QC status")

    qc_status_update = await update_qc_status(
        ack.cmd_id,
        status,
        rejection_reason=ack.reason,
        qc_response=ack.model_dump(),
    )
    snapshot_ingestion: dict[str, Any] = {"ingested": False, "reason": "no_account_state"}
    if ack.account_state:
        try:
            snapshot_ingestion = await ingest_execution_ack_snapshot(
                account_state=ack.account_state,
                command_id=ack.cmd_id,
                ack_status=status,
                holdings_weights=ack.actual_holdings_weights,
                target_weights=ack.actual_target_weights,
            )
        except Exception as exc:  # pragma: no cover - defensive; ACK must still return 200.
            logger.warning(
                "QC ACK account snapshot ingestion failed for %s: %s",
                ack.cmd_id,
                exc,
                exc_info=True,
            )
            snapshot_ingestion = {
                "ingested": False,
                "reason": "snapshot_ingestion_failed",
                "error": str(exc),
            }
    return {
        "received": True,
        "snapshot_ingestion": snapshot_ingestion,
        "qc_status_update": qc_status_update,
        "feedback_trust": qc_status_update.get("feedback_trust") if isinstance(qc_status_update, dict) else None,
    }
