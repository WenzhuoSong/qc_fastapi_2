"""Execution lifecycle endpoints."""
from __future__ import annotations

import logging
from typing import Any

from pydantic import BaseModel
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from services.account_snapshot_store import ingest_execution_ack_snapshot
from services.execution_observer import build_execution_tape_summary
from services.execution_log_store import update_qc_status
from services.qc_webhook_auth import verify_qc_signature
from db.models import ExecutionObservedFill, ExecutionObservedOrder
from db.session import get_db


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
    per_leg_fill_status: list[dict[str, Any]] | dict[str, Any] | None = None
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


@router.get("/execution/observed/summary")
async def observed_execution_summary(
    db: AsyncSession = Depends(get_db),
    strategy_id: str | None = Query(default="newbase"),
    limit: int = Query(default=50, ge=1, le=500),
):
    """Read-only execution tape summary from QC-owned observed orders/fills."""
    return await build_execution_tape_summary(db, strategy_id=strategy_id, limit=limit)


@router.get("/execution/observed/fills")
async def observed_execution_fills(
    db: AsyncSession = Depends(get_db),
    strategy_id: str | None = Query(default="newbase"),
    limit: int = Query(default=100, ge=1, le=1000),
):
    stmt = select(ExecutionObservedFill).order_by(
        desc(ExecutionObservedFill.event_time),
        desc(ExecutionObservedFill.id),
    ).limit(limit)
    if strategy_id:
        stmt = stmt.where(ExecutionObservedFill.strategy_id == strategy_id.lower().strip())
    rows = list((await db.execute(stmt)).scalars().all())
    return {
        "read_only": True,
        "execution_authority": "none",
        "count": len(rows),
        "fills": [
            {
                "event_time": row.event_time.isoformat() if row.event_time else None,
                "trading_date": row.trading_date.isoformat() if row.trading_date else None,
                "strategy_id": row.strategy_id,
                "algorithm_version": row.algorithm_version,
                "order_id": row.order_id,
                "ticker": row.ticker,
                "direction": row.direction,
                "quantity": row.quantity,
                "fill_price": row.fill_price,
                "fee": row.fee,
                "notional": row.notional,
                "fee_bps": row.fee_bps,
                "snapshot_uid": row.snapshot_uid,
            }
            for row in rows
        ],
    }


@router.get("/execution/observed/orders")
async def observed_execution_orders(
    db: AsyncSession = Depends(get_db),
    strategy_id: str | None = Query(default="newbase"),
    limit: int = Query(default=100, ge=1, le=1000),
):
    stmt = select(ExecutionObservedOrder).order_by(
        desc(ExecutionObservedOrder.event_time),
        desc(ExecutionObservedOrder.id),
    ).limit(limit)
    if strategy_id:
        stmt = stmt.where(ExecutionObservedOrder.strategy_id == strategy_id.lower().strip())
    rows = list((await db.execute(stmt)).scalars().all())
    return {
        "read_only": True,
        "execution_authority": "none",
        "count": len(rows),
        "orders": [
            {
                "event_time": row.event_time.isoformat() if row.event_time else None,
                "trading_date": row.trading_date.isoformat() if row.trading_date else None,
                "strategy_id": row.strategy_id,
                "algorithm_version": row.algorithm_version,
                "order_id": row.order_id,
                "ticker": row.ticker,
                "direction": row.direction,
                "quantity": row.quantity,
                "status": row.status,
                "order_type": row.order_type,
                "snapshot_uid": row.snapshot_uid,
            }
            for row in rows
        ],
    }
