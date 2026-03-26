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
from db.models import QCSnapshot

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

        return JSONResponse({"status": "ok", "snapshot_id": snapshot.id})

    except Exception as e:
        logger.error(f"Webhook error: {e}")
        raise HTTPException(status_code=400, detail=str(e))
