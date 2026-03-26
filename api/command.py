# api/command.py
import logging
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from db.session import get_db
from db.queries import upsert_system_config, get_system_config

logger = logging.getLogger("qc_fastapi_2.command")

router = APIRouter(tags=["command"])


class PauseTradingRequest(BaseModel):
    pause: bool
    reason: str = ""


@router.post("/command/pause")
async def pause_trading(
    req: PauseTradingRequest,
    db: AsyncSession = Depends(get_db)
):
    """
    暂停或恢复交易
    """
    await upsert_system_config(
        db,
        key="trading_paused",
        value={"paused": req.pause, "reason": req.reason},
        updated_by="api"
    )

    logger.info(f"Trading {'PAUSED' if req.pause else 'RESUMED'}: {req.reason}")

    return {
        "status": "ok",
        "trading_paused": req.pause,
        "reason": req.reason
    }


@router.get("/command/status")
async def command_status(db: AsyncSession = Depends(get_db)):
    """
    查询当前交易状态
    """
    config = await get_system_config(db, "trading_paused")
    if not config:
        return {"trading_paused": False, "reason": ""}

    value = config.value
    return {
        "trading_paused": value.get("paused", False),
        "reason": value.get("reason", ""),
        "updated_at": config.updated_at.isoformat(),
        "updated_by": config.updated_by
    }
