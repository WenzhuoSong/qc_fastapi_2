"""
services/position_manager.py

Monitors portfolio positions for: drift, holding period, and intraday moves.
Used by cron/position_monitor.py (every 30 min during trading hours).
Also called during Stage 1 market_brief for position_alerts injection.

P1-2: POSITION_MANAGER
"""
from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Optional

from sqlalchemy import select, desc

from db.session import AsyncSessionLocal
from db.models import HoldingsFactor, QCSnapshot, AlertLog
from db.queries import upsert_alert
from config import get_settings

logger = logging.getLogger("qc_fastapi_2.position_manager")
settings = get_settings()

# Default thresholds
DEFAULT_DRIFT_THRESHOLD = 0.05      # 5% drift between target and current
DEFAULT_MAX_HOLDING_DAYS = 60       # max days before flagging
DEFAULT_ATR_THRESHOLD = 2.0         # 2x ATR for intraday move threshold


async def run_position_health_check(
    drift_threshold: float = DEFAULT_DRIFT_THRESHOLD,
    max_holding_days: int = DEFAULT_MAX_HOLDING_DAYS,
    atr_threshold: float = DEFAULT_ATR_THRESHOLD,
) -> dict:
    """
    Run all position health checks.
    Returns:
        {
            "drift_alerts": [...],
            "holding_period_alerts": [...],
            "intraday_alerts": [...],
            "total_alerts": int,
        }
    """
    drift_alerts = await check_position_drift(threshold=drift_threshold)
    holding_alerts = await check_holding_periods(max_days=max_holding_days)
    intraday_alerts = await check_intraday_moves(atr_threshold=atr_threshold)

    total = len(drift_alerts) + len(holding_alerts) + len(intraday_alerts)
    logger.info(
        f"[position_manager] drift={len(drift_alerts)} "
        f"holding={len(holding_alerts)} intraday={len(intraday_alerts)} "
        f"total={total}"
    )

    return {
        "drift_alerts": drift_alerts,
        "holding_period_alerts": holding_alerts,
        "intraday_alerts": intraday_alerts,
        "total_alerts": total,
    }


async def check_position_drift(
    threshold: float = DEFAULT_DRIFT_THRESHOLD,
) -> list[dict]:
    """
    Compare target_weights vs current_weights.
    Alert if |target - current| > threshold for any held position.
    """
    async with AsyncSessionLocal() as db:
        stmt = (
            select(HoldingsFactor)
            .order_by(desc(HoldingsFactor.recorded_at))
            .limit(50)
        )
        rows = (await db.execute(stmt)).scalars().all()

    if not rows:
        return []

    # Group by ticker, keep latest
    latest_by_ticker: dict[str, HoldingsFactor] = {}
    for r in rows:
        if r.ticker and r.ticker not in latest_by_ticker:
            latest_by_ticker[r.ticker] = r

    alerts = []
    for ticker, h in latest_by_ticker.items():
        if not h.weight_target or not h.weight_current:
            continue
        try:
            target = float(h.weight_target)
            current = float(h.weight_current)
            drift = abs(target - current)
            if drift > threshold and current > 0.01:
                alerts.append({
                    "ticker": ticker,
                    "type": "drift",
                    "severity": "warning" if drift < 0.10 else "critical",
                    "current_weight": round(current, 4),
                    "target_weight": round(target, 4),
                    "drift": round(drift, 4),
                    "threshold": threshold,
                    "message": (
                        f"{ticker} drift {drift:.2%} exceeds threshold {threshold:.2%} "
                        f"(current={current:.2%}, target={target:.2%})"
                    ),
                })
        except (TypeError, ValueError):
            continue

    return alerts


async def check_holding_periods(
    max_days: int = DEFAULT_MAX_HOLDING_DAYS,
) -> list[dict]:
    """
    Scan holdings for positions held longer than max_days.
    Suggests review or rebalancing.
    """
    async with AsyncSessionLocal() as db:
        stmt = (
            select(HoldingsFactor)
            .order_by(desc(HoldingsFactor.recorded_at))
            .limit(50)
        )
        rows = (await db.execute(stmt)).scalars().all()

    if not rows:
        return []

    latest_by_ticker: dict[str, HoldingsFactor] = {}
    for r in rows:
        if r.ticker and r.ticker not in latest_by_ticker:
            latest_by_ticker[r.ticker] = r

    alerts = []
    for ticker, h in latest_by_ticker.items():
        if not h.holding_days:
            continue
        try:
            days = int(h.holding_days)
            if days > max_days and float(h.weight_current or 0) > 0.01:
                alerts.append({
                    "ticker": ticker,
                    "type": "holding_period",
                    "severity": "warning" if days < 90 else "critical",
                    "holding_days": days,
                    "max_days": max_days,
                    "current_weight": round(float(h.weight_current or 0), 4),
                    "message": (
                        f"{ticker} held {days} days (max={max_days}), "
                        f"weight={float(h.weight_current or 0):.2%} — consider review"
                    ),
                })
        except (TypeError, ValueError):
            continue

    return alerts


async def check_intraday_moves(
    atr_threshold: float = DEFAULT_ATR_THRESHOLD,
) -> list[dict]:
    """
    Detect large intraday moves based on ATR%.
    Requires holdings_factors to have atr_pct populated from QC data.
    """
    async with AsyncSessionLocal() as db:
        stmt = (
            select(HoldingsFactor)
            .order_by(desc(HoldingsFactor.recorded_at))
            .limit(50)
        )
        rows = (await db.execute(stmt)).scalars().all()

    if not rows:
        return []

    latest_by_ticker: dict[str, HoldingsFactor] = {}
    for r in rows:
        if r.ticker and r.ticker not in latest_by_ticker:
            latest_by_ticker[r.ticker] = r

    alerts = []
    for ticker, h in latest_by_ticker.items():
        if not h.atr_pct:
            continue
        try:
            atr = abs(float(h.atr_pct))
            # Large move = > atr_threshold x ATR (typically 2x ATR = significant move)
            # Note: actual daily return would need to come from portfolio_timeseries
            # Here we flag high ATR as proxy for volatile positions
            if atr > atr_threshold * 0.01:  # atr_pct stored as decimal
                alerts.append({
                    "ticker": ticker,
                    "type": "high_atr",
                    "severity": "info" if atr < 0.05 else "warning",
                    "atr_pct": round(atr, 4),
                    "atr_threshold": atr_threshold,
                    "current_weight": round(float(h.weight_current or 0), 4),
                    "message": (
                        f"{ticker} ATR {atr:.2%} elevated "
                        f"(threshold {atr_threshold:.1f}x). Volatility elevated."
                    ),
                })
        except (TypeError, ValueError):
            continue

    return alerts


async def persist_position_alerts(alerts: list[dict], source: str = "position_monitor") -> None:
    """
    Write position alerts to AlertLog table for audit trail.
    """
    if not alerts:
        return

    async with AsyncSessionLocal() as db:
        now = datetime.utcnow()
        for alert in alerts:
            # Get snapshot_id from latest heartbeat
            snapshot_stmt = (
                select(QCSnapshot)
                .where(QCSnapshot.packet_type == "heartbeat")
                .order_by(desc(QCSnapshot.received_at))
                .limit(1)
            )
            snapshot_row = (await db.execute(snapshot_stmt)).scalar_one_or_none()
            snapshot_id = snapshot_row.id if snapshot_row else None

            alert_record = {
                "snapshot_id": snapshot_id,
                "alert_id": f"{source}_{alert.get('ticker', 'unknown')}_{int(now.timestamp())}",
                "level": alert.get("severity", "warning"),
                "type": f"position_{alert.get('type', 'unknown')}",
                "message": alert.get("message", ""),
                "ticker": alert.get("ticker"),
                "triggered_at": now,
                "is_handled": False,
            }
            await upsert_alert(db, alert_record)

        await db.commit()

    logger.info(f"[position_manager] Persisted {len(alerts)} alerts to AlertLog")