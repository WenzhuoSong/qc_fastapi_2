# api/webhook.py
import hashlib
import logging
import gzip
import json
import math
from datetime import datetime
from fastapi import APIRouter, HTTPException, Depends, Header, Request
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from config import get_settings
from db.session import get_db
from db.models import QCSnapshot, PortfolioTimeseries, HoldingsFactor, AccountStateSnapshot, AlertLog, AgentAnalysis
from db.queries import upsert_system_config, upsert_alert, get_recent_alerts
from services.account_state_snapshot import build_account_state_snapshot
from services.execution_log_store import append_reconciliation_from_account_snapshot
from tools.notify_tools import tool_send_telegram
from tools.qc_tools import tool_emergency_liquidate

logger = logging.getLogger("qc_fastapi_2.webhook")
settings = get_settings()

router = APIRouter(tags=["webhook"])

_DECIMAL_RETURN_FIELDS = {
    "daily_return_pct",
    "return_5d",
    "mom_20d",
    "mom_60d",
    "mom_252d",
}


def verify_auth(x_webhook_user: str = Header(None), x_webhook_secret: str = Header(None)):
    """验证 webhook 鉴权头"""
    if x_webhook_user != settings.webhook_user or x_webhook_secret != settings.webhook_secret:
        raise HTTPException(status_code=403, detail="Invalid credentials")


def _numeric_or_none(row: dict, field: str, precision: int, scale: int, ticker: str | None = None):
    """Return a DB-safe numeric value, dropping values outside NUMERIC precision."""
    value = row.get(field)
    if value is None or value == "":
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        logger.warning("[Webhook] invalid numeric %s.%s=%r", ticker or "?", field, value)
        return None
    if not math.isfinite(number):
        logger.warning("[Webhook] non-finite numeric %s.%s=%r", ticker or "?", field, value)
        return None

    if field in _DECIMAL_RETURN_FIELDS and abs(number) >= 100:
        number = number / 100.0
        logger.warning(
            "[Webhook] normalized legacy percent-point field %s.%s from %r to %r",
            ticker or "?",
            field,
            value,
            number,
        )

    rounded = round(number, scale)
    max_abs = 10 ** (precision - scale)
    if abs(rounded) >= max_abs:
        logger.warning(
            "[Webhook] out-of-range numeric dropped %s.%s=%r for NUMERIC(%s,%s)",
            ticker or "?",
            field,
            value,
            precision,
            scale,
        )
        return None
    return rounded


@router.post("/webhook/qc")
async def receive_qc_packet(
    request: Request,
    db: AsyncSession = Depends(get_db),
    _auth=Depends(verify_auth)
):
    """
    接收 QC 的 gzip 压缩 JSON 数据包
    packet_type: heartbeat | daily_feature_snapshot | alert | emergency
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

        if packet_type in ("heartbeat", "daily_feature_snapshot"):
            await _process_market_snapshot(db, snapshot.id, payload)
        elif packet_type == "alert":
            await _process_alert(db, snapshot.id, payload)
        elif packet_type == "emergency":
            await _process_emergency(db, snapshot.id, payload)

        return JSONResponse({"status": "ok", "snapshot_id": snapshot.id})

    except Exception as e:
        logger.error(f"Webhook error: {e}")
        raise HTTPException(status_code=400, detail=str(e))


async def _process_market_snapshot(db: AsyncSession, snapshot_id: int, payload: dict):
    """解析 market snapshot payload，写入 portfolio_timeseries 和 holdings_factors。"""
    now       = datetime.utcnow()
    portfolio = payload.get("portfolio", {})
    holdings  = payload.get("holdings") or payload.get("features", [])

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
        ticker = h.get("ticker")
        open_price = _numeric_or_none(h, "open_price", 15, 4, ticker)
        high_price = _numeric_or_none(h, "high_price", 15, 4, ticker)
        low_price = _numeric_or_none(h, "low_price", 15, 4, ticker)
        if open_price is None:
            open_price = _numeric_or_none(h, "open", 15, 4, ticker)
        if high_price is None:
            high_price = _numeric_or_none(h, "high", 15, 4, ticker)
        if low_price is None:
            low_price = _numeric_or_none(h, "low", 15, 4, ticker)

        db.add(HoldingsFactor(
            snapshot_id        = snapshot_id,
            recorded_at        = now,
            ticker             = ticker,
            universe_role      = h.get("universe_role"),
            price              = _numeric_or_none(h, "price", 15, 4, ticker),
            close_price        = _numeric_or_none(h, "close_price", 15, 4, ticker),
            open_price         = open_price,
            high_price         = high_price,
            low_price          = low_price,
            volume             = h.get("volume"),
            dollar_volume      = _numeric_or_none(h, "dollar_volume", 20, 2, ticker),
            daily_return_pct   = _numeric_or_none(h, "daily_return_pct", 8, 6, ticker),
            return_5d          = _numeric_or_none(h, "return_5d", 8, 6, ticker),
            weight_current     = _numeric_or_none(h, "weight_current", 6, 4, ticker),
            weight_target      = _numeric_or_none(h, "weight_target", 6, 4, ticker),
            weight_drift       = _numeric_or_none(h, "weight_drift", 6, 4, ticker),
            mom_20d            = _numeric_or_none(h, "mom_20d", 8, 6, ticker),
            mom_60d            = _numeric_or_none(h, "mom_60d", 8, 6, ticker),
            mom_252d           = _numeric_or_none(h, "mom_252d", 8, 6, ticker),
            sma_20             = _numeric_or_none(h, "sma_20", 15, 4, ticker),
            sma_50             = _numeric_or_none(h, "sma_50", 15, 4, ticker),
            sma_200            = _numeric_or_none(h, "sma_200", 15, 4, ticker),
            rsi_14             = _numeric_or_none(h, "rsi_14", 6, 2, ticker),
            atr_pct            = _numeric_or_none(h, "atr_pct", 8, 6, ticker),
            bb_position        = _numeric_or_none(h, "bb_position", 6, 4, ticker),
            hist_vol_20d       = _numeric_or_none(h, "hist_vol_20d", 8, 6, ticker),
            beta_vs_spy        = _numeric_or_none(h, "beta_vs_spy", 6, 4, ticker),
            unrealized_pnl_pct = _numeric_or_none(h, "unrealized_pnl_pct", 8, 6, ticker),
            holding_days       = h.get("holding_days"),
        ))

    account_state = build_account_state_snapshot(payload, qc_snapshot_id=snapshot_id, received_at=now)
    db.add(AccountStateSnapshot(
        qc_snapshot_id=account_state["qc_snapshot_id"],
        recorded_at=account_state["recorded_at"],
        account_timestamp=account_state["account_timestamp"],
        source_packet_type=account_state["source_packet_type"],
        contract_version=account_state["contract_version"],
        account_status=account_state["account_status"],
        data_status=account_state["data_status"],
        policy_version=account_state["policy_version"],
        total_value=account_state["total_value"],
        cash=account_state["cash"],
        cash_pct=account_state["cash_pct"],
        buying_power=account_state["buying_power"],
        open_order_count=account_state["open_order_count"],
        has_open_orders=account_state["has_open_orders"],
        is_market_open=account_state["is_market_open"],
        last_command_id=account_state["last_command_id"],
        active_command_id=account_state["active_command_id"],
        active_execution_status=account_state["active_execution_status"],
        processed_command_count=account_state["processed_command_count"],
        holdings_weights=account_state["holdings_weights"],
        target_weights=account_state["target_weights"],
        raw_snapshot=account_state["raw_snapshot"],
    ))
    try:
        await append_reconciliation_from_account_snapshot(db, account_state)
    except Exception as exc:
        logger.warning("[Webhook] command reconciliation from account snapshot failed: %s", exc)

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
            await tool_send_telegram({"text": "✅ Emergency liquidation command sent"})
        else:
            logger.error(f"[EMERGENCY] Emergency liquidation failed: {result.get('error')}")
            await tool_send_telegram({"text": f"❌ Emergency liquidation failed: {result.get('error')}"})

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
