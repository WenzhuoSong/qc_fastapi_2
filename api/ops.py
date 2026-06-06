"""Read-only operator endpoints."""
from __future__ import annotations

from fastapi import APIRouter, HTTPException
from fastapi.responses import PlainTextResponse

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
