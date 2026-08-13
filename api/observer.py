"""Read-only QC observer fact-plane endpoints."""
from __future__ import annotations

import json

from fastapi import APIRouter, BackgroundTasks, Depends, Header, HTTPException, Request
from sqlalchemy.ext.asyncio import AsyncSession

from db.session import get_db
from services.qc_observer_health import load_current_observer_summary
from services.qc_observer_ingestion import (
    ObserverEventConflict,
    ObserverEventValidationError,
    enforce_observer_request_identity,
    ingest_observer_events,
    quarantine_rejected_observer_events,
    reserve_observer_request_nonce,
    validate_observer_batch_partial,
)
from services.qc_webhook_auth import (
    ObserverRequestAuthError,
    authenticate_observer_request,
)
from services.qc_observer_projections import (
    load_observer_projection_status,
    run_observer_projection_batch,
)
from config import get_settings
from audit.service import load_latest_audit_status


router = APIRouter(prefix="/observer", tags=["observer"])


@router.get("/status")
async def observer_status(db: AsyncSession = Depends(get_db)) -> dict:
    return await load_current_observer_summary(db)


@router.get("/projections/status")
async def observer_projection_status(db: AsyncSession = Depends(get_db)) -> dict:
    return await load_observer_projection_status(db)


@router.get("/audit/status")
async def observer_audit_status(db: AsyncSession = Depends(get_db)) -> dict:
    return await load_latest_audit_status(db)


@router.post("/events")
async def receive_observer_events(
    request: Request,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    x_qc_signature: str | None = Header(None, alias="X-QC-Signature"),
    x_qc_timestamp: str | None = Header(None, alias="X-QC-Timestamp"),
    x_qc_nonce: str | None = Header(None, alias="X-QC-Nonce"),
    x_qc_account_id: str | None = Header(None, alias="X-QC-Account-ID"),
    x_qc_deployment_id: str | None = Header(None, alias="X-QC-Deployment-ID"),
) -> dict:
    body = await request.body()
    try:
        auth = authenticate_observer_request(
            body,
            signature=x_qc_signature,
            timestamp=x_qc_timestamp,
            nonce=x_qc_nonce,
            account_id=x_qc_account_id,
            deployment_id=x_qc_deployment_id,
        )
    except ObserverRequestAuthError as exc:
        raise HTTPException(
            status_code=exc.status_code,
            detail={"code": exc.code, "message": str(exc)},
        ) from exc

    try:
        payload = json.loads(body)
        raw_events = payload.get("events") if isinstance(payload, dict) else None
        if isinstance(raw_events, list):
            enforce_observer_request_identity(raw_events, auth)
        events, rejected = validate_observer_batch_partial(payload)
        settings = get_settings()
        await reserve_observer_request_nonce(
            db,
            auth,
            ttl_seconds=max(settings.qc_observer_max_clock_skew_seconds * 2, 1),
        )
        rejected_channel_acks = await quarantine_rejected_observer_events(
            db,
            rejected,
            request_nonce=auth.nonce,
        )
        if events:
            ack = await ingest_observer_events(db, events)
        else:
            ack = {
                "status": "rejected",
                "accepted_event_ids": [],
                "duplicate_event_ids": [],
                "channels": [],
            }
        channel_map = {
            (item["event_stream_id"], item["event_channel"]): item
            for item in [*rejected_channel_acks, *ack["channels"]]
        }
        ack["channels"] = [
            channel_map[key]
            for key in sorted(channel_map)
        ]
        if rejected:
            ack["status"] = "accepted_with_rejections" if events else "rejected"
        ack["auth_version"] = auth.auth_version
        ack["rejected_events"] = [
            {
                "event_index": item["event_index"],
                "event_id": item["event_id"],
                "code": item["rejection_code"],
            }
            for item in rejected
        ]
        await db.commit()
        background_tasks.add_task(run_observer_projection_batch)
        return ack
    except json.JSONDecodeError as exc:
        await db.rollback()
        raise HTTPException(status_code=400, detail="Invalid JSON body") from exc
    except ObserverEventValidationError as exc:
        await db.rollback()
        raise HTTPException(status_code=422, detail={"code": exc.code, "message": str(exc)}) from exc
    except ObserverEventConflict as exc:
        await db.rollback()
        raise HTTPException(status_code=409, detail={"code": exc.code, "message": str(exc)}) from exc
    except Exception:
        await db.rollback()
        raise
