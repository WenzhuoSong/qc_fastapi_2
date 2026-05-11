# api/webhook.py
import hashlib
import logging
import gzip
import json
from datetime import datetime
from fastapi import APIRouter, HTTPException, Depends, Header, Request
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from config import get_settings
from db.session import get_db
from db.models import QCSnapshot, PortfolioTimeseries, HoldingsFactor, AlertLog, AgentAnalysis
from db.queries import upsert_system_config, upsert_alert, get_recent_alerts
from tools.notify_tools import tool_send_telegram
from tools.qc_tools import tool_emergency_liquidate

logger = logging.getLogger("qc_fastapi_2.webhook")
settings = get_settings()

router = APIRouter(tags=["webhook"])


def verify_auth(x_webhook_user: str = Header(None), x_webhook_secret: str = Header(None)):
    """验证 webhook 鉴权头"""
    if x_webhook_user != settings.webhook_user or x_webhook_secret != settings.webhook_secret:
        raise HTTPException(status_code=403, detail="Invalid credentials")


@router.post("/webhook/qc")
async def receive_qc_packet(
    request: Request,
    db: AsyncSession = Depends(get_db),
    _auth=Depends(verify_auth)
):
    """
    接收 QC 的 gzip 压缩 JSON 数据包
    packet_type: heartbeat | alert | emergency
    """
    try:
        # 读取原始 body 并解压
        data = await request.body()
        decompressed = gzip.decompress(data)
        payload = json.loads(decompressed)

        # 验证 checksum
        received_checksum = payload.get("checksum")
        if received_checksum:
            payload_for_check = {k: v for k, v in payload.items() if k != "checksum"}
            expected = hashlib.md5(
                json.dumps(payload_for_check, sort_keys=True).encode()
            ).hexdigest()
            if received_checksum != expected:
                logger.warning(f"Checksum mismatch: received={received_checksum} expected={expected}")
                raise HTTPException(status_code=400, detail="Checksum mismatch")

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
        elif packet_type == "alert":
            await _process_alert(db, snapshot.id, payload)
        elif packet_type == "emergency":
            await _process_emergency(db, snapshot.id, payload)

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
            hist_vol_20d       = h.get("hist_vol_20d"),
            beta_vs_spy        = h.get("beta_vs_spy"),
            unrealized_pnl_pct = h.get("unrealized_pnl_pct"),
            holding_days       = h.get("holding_days"),
        ))

    # 更新 last_vix（QC Phase 2 接入后会有实际值）
    vix = portfolio.get("vix")
    if vix is not None:
        await upsert_system_config(db, "last_vix", {"value": float(vix)}, "webhook")

    await db.commit()


async def _process_alert(db: AsyncSession, snapshot_id: int, payload: dict):
    """
    解析 alert packet，写入 alerts_log 表。
    若有未处理的 critical alert，追加到 system_config.pending_critical_alerts。
    """
    alerts = payload.get("alerts", [])
    if not alerts:
        logger.warning(f"[ALERT] packet {snapshot_id} has no alerts array")
        return

    now = datetime.utcnow()
    critical_alerts = []

    for a in alerts:
        alert_record = {
            "snapshot_id":  snapshot_id,
            "alert_id":     a.get("alert_id") or f"{snapshot_id}_{a.get('type', 'unknown')}",
            "level":        a.get("level", "warning"),
            "type":         a.get("type", "unknown"),
            "message":      a.get("message", ""),
            "ticker":       a.get("ticker"),
            "value":        a.get("value"),
            "threshold":    a.get("threshold"),
            "triggered_at": now,
            "is_handled":   False,
        }
        await upsert_alert(db, alert_record)
        logger.info(f"[ALERT] {alert_record['level']} {alert_record['type']} {alert_record.get('ticker', '')}: {alert_record['message']}")

        if alert_record["level"] == "critical":
            critical_alerts.append({
                "alert_id":    alert_record["alert_id"],
                "type":        alert_record["type"],
                "message":     alert_record["message"],
                "ticker":      alert_record["ticker"],
                "snapshot_id": snapshot_id,
                "triggered_at": now.isoformat(),
            })

    if critical_alerts:
        existing_cfg = await get_recent_alerts(db, hours=24, level="critical")
        pending = [a for a in critical_alerts]
        for ca in pending:
            await upsert_system_config(db, "pending_critical_alerts", {"alerts": pending}, "webhook")
        logger.warning(f"[ALERT] {len(critical_alerts)} critical alerts stored for downstream processing")


async def _process_emergency(db: AsyncSession, snapshot_id: int, payload: dict):
    """
    处理 emergency packet：
    1. 立即设置 circuit_state = ALERT
    2. 发送 Telegram 紧急通知
    3. 可选：自动清仓（由 emergency_auto_liquidate 配置控制）
    4. 写入 AgentAnalysis 记录（trigger_type=emergency）
    """
    now = datetime.utcnow()
    reason = payload.get("reason", "Unknown emergency")
    details = payload.get("details", {})

    # 1. 触发熔断
    await upsert_system_config(db, "circuit_state", {"value": "ALERT"}, "webhook")
    logger.critical(f"[EMERGENCY] circuit_state set to ALERT | reason={reason}")

    # 2. 发送 Telegram 通知
    urgency_emoji = "🚨" if payload.get("severity") == "critical" else "⚠️"
    text = (
        f"{urgency_emoji} <b>EMERGENCY PACKET RECEIVED</b>\n"
        f"  Reason: {reason}\n"
        f"  Snapshot: {snapshot_id}\n"
        f"  Time: {now.strftime('%Y-%m-%d %H:%M:%S')} UTC\n"
    )
    if details:
        text += f"  Details: {json.dumps(details, ensure_ascii=False)[:200]}\n"
    if settings.emergency_auto_liquidate:
        text += "  🔥 Auto-liquidate: <b>ENABLED</b>"
    else:
        text += "  🔒 Auto-liquidate: DISABLED (manual action required)"

    await tool_send_telegram({"text": text})

    # 3. 可选：自动清仓
    if settings.emergency_auto_liquidate:
        logger.warning("[EMERGENCY] emergency_auto_liquidate is True — executing liquidation")
        result = await tool_emergency_liquidate({})
        if result.get("success"):
            logger.critical("[EMERGENCY] Emergency liquidation command sent successfully")
            await tool_send_telegram({"text": "✅ 紧急清仓指令已发送"})
        else:
            logger.error(f"[EMERGENCY] Emergency liquidation failed: {result.get('error')}")
            await tool_send_telegram({"text": f"❌ 紧急清仓失败: {result.get('error')}"})

    # 4. 写入 AgentAnalysis 记录（供后续审计）
    analysis = AgentAnalysis(
        analyzed_at=now,
        trigger_type="emergency",
        snapshot_ids=[snapshot_id],
        execution_status="emergency_triggered",
        notes=f"Emergency packet received: {reason}",
    )
    db.add(analysis)
    await db.commit()
    logger.info(f"[EMERGENCY] AgentAnalysis record created: id={analysis.id}")
