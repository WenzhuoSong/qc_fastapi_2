"""
services/position_manager.py

Monitors portfolio positions for: drift, holding period, and intraday moves.
Used by cron/position_monitor.py (every 30 min during trading hours).
Also called during Stage 1 market_brief for position_alerts injection.

P1-2: POSITION_MANAGER
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, asdict
from datetime import date, datetime
from typing import Optional, Any

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


@dataclass
class PositionConstraints:
    max_new_buys_per_cycle: int = 3
    max_positions: int = 12
    max_single_trade_pct: float = 0.08
    max_turnover_per_cycle: float = 0.30
    max_daily_trades: int = 5
    min_hold_days: int = 2

    @classmethod
    def from_config(cls, cfg: dict[str, Any] | None) -> "PositionConstraints":
        if not cfg:
            return cls()
        defaults = asdict(cls())
        clean: dict[str, Any] = {}
        for key, default in defaults.items():
            value = cfg.get(key, default)
            try:
                clean[key] = int(value) if isinstance(default, int) else float(value)
            except (TypeError, ValueError):
                clean[key] = default
        return cls(**clean)


@dataclass
class PositionManagerOutput:
    adjusted_weights: dict[str, float]
    violations: list[str]
    trade_summary: dict[str, Any]
    constraints: dict[str, Any]


class PositionManager:
    """
    Pre-trade quantity/frequency control applied after Risk Manager.

    It only makes target weights more conservative: blocked or clipped buy weight
    is moved to CASH, young positions are protected from sells, and excessive
    turnover is scaled toward the current portfolio.
    """

    trade_threshold = 0.01

    def apply(
        self,
        target_weights: dict[str, float],
        current_holdings: dict[str, float],
        constraints: PositionConstraints | None = None,
        holdings_meta: list[dict] | None = None,
        actual_daily_trades: int = 0,
    ) -> PositionManagerOutput:
        constraints = constraints or PositionConstraints()
        current = _clean_weights(current_holdings)
        target = _clean_weights(target_weights)
        target.setdefault("CASH", 0.0)

        violations: list[str] = []
        hold_days = _extract_hold_days(holdings_meta or [])

        # Protect young positions from sells before applying buy/turnover caps.
        for ticker, cur_w in current.items():
            if ticker == "CASH" or cur_w <= self.trade_threshold:
                continue
            tgt_w = target.get(ticker, 0.0)
            if tgt_w < cur_w - self.trade_threshold:
                days = hold_days.get(ticker)
                if days is not None and days < constraints.min_hold_days:
                    freed_delta = cur_w - tgt_w
                    target[ticker] = cur_w
                    target["CASH"] = max(target.get("CASH", 0.0) - freed_delta, 0.0)
                    violations.append(
                        f"min_hold_days:{ticker} held {days}d < {constraints.min_hold_days}d, sell skipped"
                    )

        target = self._cap_new_buys(target, current, constraints, violations)
        target = self._cap_position_count(target, current, constraints, violations)
        target = self._cap_single_buys(target, current, constraints, violations)
        target = self._cap_trade_count(target, current, constraints, violations, actual_daily_trades)
        target = self._cap_turnover(target, current, constraints, violations)

        adjusted = _normalize_weights(target)
        final_deltas = _weight_deltas(adjusted, current)
        new_buys = [
            t for t, d in final_deltas.items()
            if t != "CASH" and d > self.trade_threshold and current.get(t, 0.0) <= self.trade_threshold
        ]
        buys = [t for t, d in final_deltas.items() if t != "CASH" and d > self.trade_threshold]
        sells = [t for t, d in final_deltas.items() if t != "CASH" and d < -self.trade_threshold]
        summary = {
            "new_buys": len(new_buys),
            "buy_trades": len(buys),
            "sell_trades": len(sells),
            "total_trades": len(buys) + len(sells),
            "total_turnover": round(_turnover(adjusted, current), 6),
            "position_count": _position_count(adjusted),
            "actual_daily_trades_before_cycle": actual_daily_trades,
        }

        return PositionManagerOutput(
            adjusted_weights=adjusted,
            violations=violations,
            trade_summary=summary,
            constraints=asdict(constraints),
        )

    def _cap_new_buys(
        self,
        target: dict[str, float],
        current: dict[str, float],
        constraints: PositionConstraints,
        violations: list[str],
    ) -> dict[str, float]:
        deltas = _weight_deltas(target, current)
        new_positions = [
            t for t, d in deltas.items()
            if t != "CASH" and d > self.trade_threshold and current.get(t, 0.0) <= self.trade_threshold
        ]
        if len(new_positions) <= constraints.max_new_buys_per_cycle:
            return target

        keep = set(sorted(new_positions, key=lambda t: deltas[t], reverse=True)[:constraints.max_new_buys_per_cycle])
        blocked = [t for t in new_positions if t not in keep]
        out = dict(target)
        freed = 0.0
        for ticker in blocked:
            old_target = out.get(ticker, 0.0)
            replacement = current.get(ticker, 0.0)
            freed += max(old_target - replacement, 0.0)
            out[ticker] = replacement
        out["CASH"] = out.get("CASH", 0.0) + freed
        violations.append(f"new_buys_capped:{blocked}")
        return out

    def _cap_position_count(
        self,
        target: dict[str, float],
        current: dict[str, float],
        constraints: PositionConstraints,
        violations: list[str],
    ) -> dict[str, float]:
        projected = [t for t, w in target.items() if t != "CASH" and w > self.trade_threshold]
        if len(projected) <= constraints.max_positions:
            return target

        existing = {t for t, w in current.items() if t != "CASH" and w > self.trade_threshold}
        if len(existing) >= constraints.max_positions:
            violations.append(
                f"position_count_exceeded:{len(projected)}>{constraints.max_positions}, existing positions preserved"
            )
            return target

        new_projected = [t for t in projected if t not in existing]
        slots = max(constraints.max_positions - len(existing), 0)
        keep_new = set(sorted(new_projected, key=lambda t: target.get(t, 0.0), reverse=True)[:slots])
        blocked = [t for t in new_projected if t not in keep_new]
        out = dict(target)
        freed = 0.0
        for ticker in blocked:
            freed += out.get(ticker, 0.0)
            out[ticker] = 0.0
        out["CASH"] = out.get("CASH", 0.0) + freed
        violations.append(f"max_positions_capped:{blocked}")
        return out

    def _cap_single_buys(
        self,
        target: dict[str, float],
        current: dict[str, float],
        constraints: PositionConstraints,
        violations: list[str],
    ) -> dict[str, float]:
        out = dict(target)
        for ticker, delta in _weight_deltas(target, current).items():
            if ticker == "CASH" or delta <= constraints.max_single_trade_pct:
                continue
            excess = delta - constraints.max_single_trade_pct
            out[ticker] = current.get(ticker, 0.0) + constraints.max_single_trade_pct
            out["CASH"] = out.get("CASH", 0.0) + excess
            violations.append(
                f"single_buy_capped:{ticker} {delta:.2%}->{constraints.max_single_trade_pct:.2%}"
            )
        return out

    def _cap_turnover(
        self,
        target: dict[str, float],
        current: dict[str, float],
        constraints: PositionConstraints,
        violations: list[str],
    ) -> dict[str, float]:
        turnover = _turnover(target, current)
        if turnover <= constraints.max_turnover_per_cycle + 1e-9:
            return target

        scale = constraints.max_turnover_per_cycle / turnover if turnover > 0 else 1.0
        out: dict[str, float] = {}
        for ticker in set(target) | set(current):
            out[ticker] = current.get(ticker, 0.0) + (target.get(ticker, 0.0) - current.get(ticker, 0.0)) * scale
        violations.append(
            f"turnover_scaled:{turnover:.2%}->{constraints.max_turnover_per_cycle:.2%}"
        )
        return out

    def _cap_trade_count(
        self,
        target: dict[str, float],
        current: dict[str, float],
        constraints: PositionConstraints,
        violations: list[str],
        actual_daily_trades: int = 0,
    ) -> dict[str, float]:
        deltas = _weight_deltas(target, current)
        sells = [t for t, d in deltas.items() if t != "CASH" and d < -self.trade_threshold]
        buys = [t for t, d in deltas.items() if t != "CASH" and d > self.trade_threshold]
        total_trades = len(sells) + len(buys)
        remaining_daily_trades = max(constraints.max_daily_trades - max(actual_daily_trades, 0), 0)
        if total_trades <= remaining_daily_trades:
            return target

        if remaining_daily_trades <= 0:
            out = dict(target)
            for ticker in buys:
                out[ticker] = current.get(ticker, 0.0)
            out["CASH"] = 1.0 - sum(w for t, w in out.items() if t != "CASH")
            violations.append(
                f"daily_trade_count_exhausted:{actual_daily_trades}>={constraints.max_daily_trades}"
            )
            return out

        if len(sells) >= remaining_daily_trades:
            violations.append(
                f"daily_trade_count_exceeded:{total_trades}+{actual_daily_trades}>{constraints.max_daily_trades}, sell trades preserved"
            )
            return target

        buy_slots = max(remaining_daily_trades - len(sells), 0)
        keep_buys = set(sorted(buys, key=lambda t: deltas[t], reverse=True)[:buy_slots])
        blocked = [t for t in buys if t not in keep_buys]
        out = dict(target)
        freed = 0.0
        for ticker in blocked:
            old_target = out.get(ticker, 0.0)
            replacement = current.get(ticker, 0.0)
            freed += max(old_target - replacement, 0.0)
            out[ticker] = replacement
        out["CASH"] = out.get("CASH", 0.0) + freed
        violations.append(f"daily_trade_count_capped:{blocked}")
        return out


def apply_position_constraints(
    target_weights: dict[str, float],
    current_holdings: dict[str, float],
    config: dict[str, Any] | None = None,
    holdings_meta: list[dict] | None = None,
    actual_daily_trades: int = 0,
) -> PositionManagerOutput:
    constraints = PositionConstraints.from_config(config)
    return PositionManager().apply(
        target_weights,
        current_holdings,
        constraints,
        holdings_meta,
        actual_daily_trades=actual_daily_trades,
    )


def _clean_weights(weights: dict[str, Any] | None) -> dict[str, float]:
    cleaned: dict[str, float] = {}
    for ticker, value in (weights or {}).items():
        key = (ticker or "").upper().strip()
        if not key:
            continue
        try:
            cleaned[key] = max(float(value or 0.0), 0.0)
        except (TypeError, ValueError):
            cleaned[key] = 0.0
    return cleaned


def _normalize_weights(weights: dict[str, float]) -> dict[str, float]:
    cleaned = _clean_weights(weights)
    total = sum(cleaned.values())
    if total <= 0:
        return {"CASH": 1.0}
    scaled = {ticker: weight / total for ticker, weight in cleaned.items()}
    out = {ticker: round(weight, 4) for ticker, weight in scaled.items() if ticker != "CASH" and weight > 0}
    out["CASH"] = round(max(1.0 - sum(out.values()), 0.0), 4)
    return out


def _weight_deltas(target: dict[str, float], current: dict[str, float]) -> dict[str, float]:
    keys = set(target) | set(current)
    return {ticker: float(target.get(ticker, 0.0)) - float(current.get(ticker, 0.0)) for ticker in keys}


def _turnover(target: dict[str, float], current: dict[str, float]) -> float:
    return sum(abs(delta) for delta in _weight_deltas(target, current).values()) / 2.0


def _position_count(weights: dict[str, float]) -> int:
    return sum(1 for ticker, weight in weights.items() if ticker != "CASH" and weight > PositionManager.trade_threshold)


def _extract_hold_days(holdings_meta: list[dict]) -> dict[str, int]:
    out: dict[str, int] = {}
    for item in holdings_meta:
        ticker = (item.get("ticker") or "").upper().strip()
        if not ticker:
            continue
        try:
            out[ticker] = int(item.get("holding_days"))
        except (TypeError, ValueError):
            continue
    return out


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
