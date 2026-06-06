# api/command.py
import logging
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from db.session import get_db
from db.queries import upsert_system_config, get_system_config
from services.operator_halt import (
    CONFIG_KEY as OPERATOR_HALT_CONFIG_KEY,
    build_operator_halt_state,
    normalize_operator_halt_state,
)

logger = logging.getLogger("qc_fastapi_2.command")

router = APIRouter(tags=["command"])


class PauseTradingRequest(BaseModel):
    pause: bool
    reason: str = ""


class OperatorHaltRequest(BaseModel):
    halt: bool
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


@router.post("/command/operator_halt")
async def operator_halt(
    req: OperatorHaltRequest,
    db: AsyncSession = Depends(get_db)
):
    """
    Set or clear the dedicated operator halt latch.
    """
    state = build_operator_halt_state(
        halted=req.halt,
        reason=req.reason or ("operator_halt" if req.halt else "operator_resume"),
        updated_by="api",
    )
    await upsert_system_config(
        db,
        key=OPERATOR_HALT_CONFIG_KEY,
        value=state,
        updated_by="api",
    )
    logger.info("Operator halt %s: %s", "ENABLED" if req.halt else "CLEARED", req.reason)
    return {
        "status": "ok",
        "operator_halt_state": normalize_operator_halt_state(state),
    }


@router.get("/command/status")
async def command_status(db: AsyncSession = Depends(get_db)):
    """
    查询当前交易状态
    """
    config = await get_system_config(db, "trading_paused")
    operator_halt_cfg = await get_system_config(db, OPERATOR_HALT_CONFIG_KEY)
    operator_halt_state = normalize_operator_halt_state(
        operator_halt_cfg.value if operator_halt_cfg else None
    )
    if not config:
        return {
            "trading_paused": False,
            "reason": "",
            "operator_halt_state": operator_halt_state,
        }

    value = config.value
    return {
        "trading_paused": value.get("paused", False),
        "reason": value.get("reason", ""),
        "updated_at": config.updated_at.isoformat(),
        "updated_by": config.updated_by,
        "operator_halt_state": operator_halt_state,
    }
