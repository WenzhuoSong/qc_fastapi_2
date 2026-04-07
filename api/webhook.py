# api/webhook.py
import logging
import gzip
import json
from datetime import datetime
from fastapi import APIRouter, HTTPException, Depends, Header
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from config import get_settings
from db.session import get_db
from db.models import QCSnapshot, PortfolioTimeseries, HoldingsFactor
from db.queries import upsert_system_config

logger = logging.getLogger("qc_fastapi_2.webhook")
settings = get_settings()

router = APIRouter(tags=["webhook"])


def verify_auth(x_webhook_user: str = Header(None), x_webhook_secret: str = Header(None)):
    """验证 webhook 鉴权头"""
    if x_webhook_user != settings.webhook_user or x_webhook_secret != settings.webhook_secret:
        raise HTTPException(status_code=403, detail="Invalid credentials")


@router.post("/webhook/qc")
async def receive_qc_packet(
    data: bytes = None,
    db: AsyncSession = Depends(get_db),
    _auth=Depends(verify_auth)
):
    """
    接收 QC 的 gzip 压缩 JSON 数据包
    packet_type: heartbeat | alert | emergency
    """
    try:
        # 解压
        decompressed = gzip.decompress(data)
        payload = json.loads(decompressed)

        packet_type = payload.get("packet_type", "heartbeat")
        trading_date_str = payload.get("trading_date")
        trading_date = datetime.strptime(trading_date_str, "%Y-%m-%d").date() if trading_date_str else None

        # 插入快照
        snapshot = QCSnapshot(
            received_at=datetime.utcnow(),
            trading_date=trading_date,
            packet_type=packet_type,
            trading_session=payload.get("trading_session"),
            schema_version=payload.get("schema_version"),
            checksum=payload.get("checksum"),
            raw_payload=payload,
            is_processed=False
        )
        db.add(snapshot)
        await db.commit()
        await db.refresh(snapshot)

        logger.info(f"Received {packet_type} packet, snapshot_id={snapshot.id}, trading_date={trading_date}")

        if packet_type == "heartbeat":
            await _process_heartbeat(db, snapshot.id, payload)

        return JSONResponse({"status": "ok", "snapshot_id": snapshot.id})

    except Exception as e:
        logger.error(f"Webhook error: {e}")
        raise HTTPException(status_code=400, detail=str(e))


async def _process_heartbeat(db: AsyncSession, snapshot_id: int, payload: dict):
    """解析 heartbeat payload，写入 portfolio_timeseries 和 holdings_factors。"""
    now       = datetime.utcnow()
    portfolio = payload.get("portfolio", {})
    holdings  = payload.get("holdings", [])

    # portfolio_timeseries
    db.add(PortfolioTimeseries(
        snapshot_id          = snapshot_id,
        recorded_at          = now,
        total_value          = portfolio.get("total_value"),
        cash_pct             = portfolio.get("cash_pct"),
        daily_pnl_pct        = portfolio.get("daily_pnl_pct"),
        current_drawdown_pct = portfolio.get("current_drawdown_pct"),
        vix                  = portfolio.get("vix"),
    ))

    # holdings_factors
    for h in holdings:
        db.add(HoldingsFactor(
            snapshot_id        = snapshot_id,
            recorded_at        = now,
            ticker             = h.get("ticker"),
            weight_current     = h.get("weight_current"),
            weight_target      = h.get("weight_target"),
            weight_drift       = h.get("weight_drift"),
            mom_20d            = h.get("mom_20d"),
            mom_60d            = h.get("mom_60d"),
            mom_252d           = h.get("mom_252d"),
            rsi_14             = h.get("rsi_14"),
            atr_pct            = h.get("atr_pct"),
            bb_position        = h.get("bb_position"),
            beta_vs_spy        = h.get("beta_vs_spy"),
            unrealized_pnl_pct = h.get("unrealized_pnl_pct"),
            holding_days       = h.get("holding_days"),
        ))

    # 更新 last_vix（QC Phase 2 接入后会有实际值）
    vix = portfolio.get("vix")
    if vix is not None:
        await upsert_system_config(db, "last_vix", {"value": float(vix)}, "webhook")

    await db.commit()
