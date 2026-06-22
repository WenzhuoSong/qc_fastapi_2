"""Read-only operator endpoints."""
from __future__ import annotations

from fastapi import APIRouter, HTTPException
from fastapi.responses import PlainTextResponse

from services.newbase_monitoring import (
    format_newbase_operator_snapshot_text,
    load_latest_newbase_operator_snapshot,
)
from services.weekend_review_operator_view import load_latest_weekend_review_operator_pack


router = APIRouter(prefix="/ops", tags=["ops"])


@router.get("/weekend-review/latest")
async def get_latest_weekend_review(include_full_report: bool = False) -> dict:
    """Return the latest review-only weekend operator pack."""
    pack = await load_latest_weekend_review_operator_pack(
        include_full_report=include_full_report,
    )
    if pack is None:
        raise HTTPException(status_code=404, detail="No weekend review found")
    return pack


@router.get("/weekend-review/latest/text", response_class=PlainTextResponse)
async def get_latest_weekend_review_text() -> str:
    """Return the latest weekend operator pack as compact plain text."""
    pack = await load_latest_weekend_review_operator_pack(include_full_report=False)
    if pack is None:
        raise HTTPException(status_code=404, detail="No weekend review found")
    return str(pack.get("text") or "")


@router.get("/newbase/latest")
async def get_latest_newbase_operator_snapshot(limit: int = 90) -> dict:
    """Return the latest review-only newBase operator snapshot."""
    snapshot = await load_latest_newbase_operator_snapshot(limit=limit)
    if snapshot is None:
        raise HTTPException(status_code=404, detail="No newBase live snapshots found")
    return snapshot


@router.get("/newbase/latest/text", response_class=PlainTextResponse)
async def get_latest_newbase_operator_snapshot_text(limit: int = 90) -> str:
    """Return the latest review-only newBase operator snapshot as compact text."""
    snapshot = await load_latest_newbase_operator_snapshot(limit=limit)
    if snapshot is None:
        raise HTTPException(status_code=404, detail="No newBase live snapshots found")
    return format_newbase_operator_snapshot_text(snapshot)
