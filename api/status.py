# api/status.py
import logging
from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from db.session import get_db
from db.queries import get_latest_snapshots, get_latest_portfolio, get_system_config

logger = logging.getLogger("qc_fastapi_2.status")

router = APIRouter(tags=["status"])


@router.get("/status")
async def get_status(db: AsyncSession = Depends(get_db)):
    """
    返回系统当前状态概览
    """
    latest_snapshots = await get_latest_snapshots(db, limit=5)
    latest_portfolio = await get_latest_portfolio(db)
    trading_paused = await get_system_config(db, "trading_paused")

    return {
        "system": "QC FastAPI 2",
        "version": "1.0.0",
        "trading_paused": trading_paused.value if trading_paused else False,
        "latest_snapshots": [
            {
                "id": s.id,
                "received_at": s.received_at.isoformat(),
                "trading_date": s.trading_date.isoformat() if s.trading_date else None,
                "packet_type": s.packet_type,
                "is_processed": s.is_processed
            }
            for s in latest_snapshots
        ],
        "latest_portfolio": {
            "recorded_at": latest_portfolio.recorded_at.isoformat() if latest_portfolio else None,
            "total_value": float(latest_portfolio.total_value) if latest_portfolio and latest_portfolio.total_value else None,
            "cash_pct": float(latest_portfolio.cash_pct) if latest_portfolio and latest_portfolio.cash_pct else None,
            "daily_pnl_pct": float(latest_portfolio.daily_pnl_pct) if latest_portfolio and latest_portfolio.daily_pnl_pct else None,
            "regime_label": latest_portfolio.regime_label if latest_portfolio else None
        } if latest_portfolio else None
    }
