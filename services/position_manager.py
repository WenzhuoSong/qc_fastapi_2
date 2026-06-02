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
    decay_auto_reduce_pct: float = 0.25

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
    mutation_types: list[str]


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
        asset_profiles: dict[str, Any] | None = None,
        min_hold_exempt_tickers: set[str] | None = None,
    ) -> PositionManagerOutput:
        constraints = constraints or PositionConstraints()
        current = _clean_weights(current_holdings)
        target = _clean_weights(target_weights)
        target.setdefault("CASH", 0.0)

        violations: list[str] = []
        mutation_types: list[str] = []
        hold_days = _extract_hold_days(holdings_meta or [])
        profiles = asset_profiles or _load_asset_profiles_for_weights(target, current)
        min_hold_exempt = _clean_ticker_set(min_hold_exempt_tickers)

        # Protect young positions from sells before applying buy/turnover caps.
        for ticker, cur_w in current.items():
            if ticker == "CASH" or cur_w <= self.trade_threshold:
                continue
            if ticker in min_hold_exempt:
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
                    mutation_types.append("defer_sell_due_to_min_hold_days")

        target = self._apply_decay_holding_limits(
            target,
            current,
            hold_days,
            profiles,
            constraints,
            violations,
            mutation_types,
        )
        target = self._cap_new_buys(target, current, constraints, violations, mutation_types)
        target = self._cap_position_count(target, current, constraints, violations, mutation_types)
        target = self._cap_single_buys(target, current, constraints, violations, mutation_types)
        target = self._cap_trade_count(target, current, constraints, violations, mutation_types, actual_daily_trades)
        target = self._cap_turnover(target, current, constraints, violations, mutation_types)

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
            "decay_holding_reviews": len([
                item for item in violations
                if item.startswith("decay_auto_reduce:") or item.startswith("decay_max_hold_review:")
            ]),
        }

        return PositionManagerOutput(
            adjusted_weights=adjusted,
            violations=violations,
            trade_summary=summary,
            constraints=asdict(constraints),
            mutation_types=_unique(mutation_types),
        )

    def _apply_decay_holding_limits(
        self,
        target: dict[str, float],
        current: dict[str, float],
        hold_days: dict[str, int],
        asset_profiles: dict[str, Any],
        constraints: PositionConstraints,
        violations: list[str],
        mutation_types: list[str],
    ) -> dict[str, float]:
        out = dict(target)
        for ticker, cur_w in sorted(current.items()):
            if ticker == "CASH" or cur_w <= self.trade_threshold:
                continue
            days = hold_days.get(ticker)
            if days is None:
                continue
            policy = _holding_policy_for_ticker(ticker, asset_profiles)
            auto_days = _optional_int(policy.get("auto_reduce_after_days"))
            max_days = _optional_int(policy.get("max_hold_days"))
            if auto_days is None and max_days is None:
                continue
            decay_risk = str(policy.get("decay_risk") or "").strip().lower()
            if decay_risk not in {"high", "extreme"} and not policy.get("force_decay_review"):
                continue

            review_due = max_days is not None and days >= max_days
            reduce_due = auto_days is not None and days >= auto_days
            if not review_due and not reduce_due:
                continue

            old_target = float(out.get(ticker, 0.0) or 0.0)
            reduce_pct = max(min(float(constraints.decay_auto_reduce_pct or 0.0), 1.0), 0.0)
            if review_due:
                reduce_pct = max(reduce_pct, 0.50)
                violations.append(
                    f"decay_max_hold_review:{ticker} held {days}d >= {max_days}d decay_risk={decay_risk or 'unknown'}"
                )
            if reduce_pct <= 0:
                continue

            suggested_target = max(cur_w * (1.0 - reduce_pct), 0.0)
            if old_target <= suggested_target + 1e-9:
                continue

            out[ticker] = suggested_target
            out["CASH"] = float(out.get("CASH", 0.0) or 0.0) + max(old_target - suggested_target, 0.0)
            threshold_text = (
                f"auto_reduce_after_days={auto_days}"
                if reduce_due
                else f"max_hold_days={max_days}"
            )
            violations.append(
                f"decay_auto_reduce:{ticker} held {days}d {threshold_text} "
                f"{old_target:.2%}->{suggested_target:.2%} decay_risk={decay_risk or 'unknown'}"
            )
            mutation_types.append("decay_risk_auto_reduce")
        return out

    def _cap_new_buys(
        self,
        target: dict[str, float],
        current: dict[str, float],
        constraints: PositionConstraints,
        violations: list[str],
        mutation_types: list[str],
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
        mutation_types.append("cap_new_buy_to_current")
        return out

    def _cap_position_count(
        self,
        target: dict[str, float],
        current: dict[str, float],
        constraints: PositionConstraints,
        violations: list[str],
        mutation_types: list[str],
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
        mutation_types.append("cap_new_buy_to_current")
        return out

    def _cap_single_buys(
        self,
        target: dict[str, float],
        current: dict[str, float],
        constraints: PositionConstraints,
        violations: list[str],
        mutation_types: list[str],
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
            mutation_types.append("cap_single_buy_delta")
        return out

    def _cap_turnover(
        self,
        target: dict[str, float],
        current: dict[str, float],
        constraints: PositionConstraints,
        violations: list[str],
        mutation_types: list[str],
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
        mutation_types.append("turnover_scale_toward_current")
        return out

    def _cap_trade_count(
        self,
        target: dict[str, float],
        current: dict[str, float],
        constraints: PositionConstraints,
        violations: list[str],
        mutation_types: list[str],
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
            mutation_types.append("cap_trade_count_buys")
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
        mutation_types.append("cap_trade_count_buys")
        return out


def apply_position_constraints(
    target_weights: dict[str, float],
    current_holdings: dict[str, float],
    config: dict[str, Any] | None = None,
    holdings_meta: list[dict] | None = None,
    actual_daily_trades: int = 0,
    asset_profiles: dict[str, Any] | None = None,
) -> PositionManagerOutput:
    cfg = config or {}
    constraints = PositionConstraints.from_config(config)
    return PositionManager().apply(
        target_weights,
        current_holdings,
        constraints,
        holdings_meta,
        actual_daily_trades=actual_daily_trades,
        asset_profiles=asset_profiles,
        min_hold_exempt_tickers=_clean_ticker_set(cfg.get("min_hold_exempt_tickers") or []),
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


def _clean_ticker_set(values: Any) -> set[str]:
    if isinstance(values, str):
        raw_values = [values]
    else:
        raw_values = list(values or [])
    return {
        str(value or "").upper().strip()
        for value in raw_values
        if str(value or "").strip()
    }


def _normalize_weights(weights: dict[str, float]) -> dict[str, float]:
    cleaned = _clean_weights(weights)
    non_cash = {
        ticker: weight
        for ticker, weight in cleaned.items()
        if ticker != "CASH" and weight > 0
    }
    non_cash_total = sum(non_cash.values())
    cash = max(float(cleaned.get("CASH", 0.0) or 0.0), 0.0)
    total = non_cash_total + cash
    if total <= 0:
        return {"CASH": 1.0}
    if non_cash_total > 1.0 + 1e-9:
        scale = 1.0 / non_cash_total
        non_cash = {ticker: weight * scale for ticker, weight in non_cash.items()}
    out = {ticker: round(weight, 4) for ticker, weight in non_cash.items() if weight > 0}
    out["CASH"] = round(max(1.0 - sum(out.values()), 0.0), 4)
    return out


def _weight_deltas(target: dict[str, float], current: dict[str, float]) -> dict[str, float]:
    keys = set(target) | set(current)
    return {ticker: float(target.get(ticker, 0.0)) - float(current.get(ticker, 0.0)) for ticker in keys}


def _turnover(target: dict[str, float], current: dict[str, float]) -> float:
    return sum(abs(delta) for delta in _weight_deltas(target, current).values()) / 2.0


def _position_count(weights: dict[str, float]) -> int:
    return sum(1 for ticker, weight in weights.items() if ticker != "CASH" and weight > PositionManager.trade_threshold)


def _unique(values: list[str]) -> list[str]:
    out: list[str] = []
    for value in values:
        if value and value not in out:
            out.append(value)
    return out


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


def _load_asset_profiles_for_weights(
    target: dict[str, float],
    current: dict[str, float],
) -> dict[str, Any]:
    tickers = sorted((set(target) | set(current)) - {"CASH"})
    if not tickers:
        return {}
    try:
        from services.knowledge_base import load_knowledge_base

        assets = load_knowledge_base().get("assets") or {}
        return {
            ticker: assets[ticker]
            for ticker in tickers
            if ticker in assets
        }
    except Exception as exc:
        logger.warning("[position_manager] asset profile lookup failed: %s", exc)
        return {}


def _holding_policy_for_ticker(ticker: str, asset_profiles: dict[str, Any]) -> dict[str, Any]:
    profile = asset_profiles.get(str(ticker).upper().strip()) or {}
    policy = dict(profile.get("holding_policy") or {}) if isinstance(profile.get("holding_policy"), dict) else {}
    for key in ("max_hold_days", "auto_reduce_after_days"):
        if profile.get(key) is not None and policy.get(key) is None:
            policy[key] = profile.get(key)
    if profile.get("decay_risk") is not None:
        policy["decay_risk"] = profile.get("decay_risk")
    return policy


def _optional_int(value: Any) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


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
    snapshot, rows = await _latest_heartbeat_holdings()
    diagnostics = _build_position_monitor_diagnostics(
        snapshot=snapshot,
        rows=rows,
        max_holding_days=max_holding_days,
        atr_threshold=atr_threshold,
    )

    total = len(drift_alerts) + len(holding_alerts) + len(intraday_alerts)
    logger.info(
        f"[position_manager] drift={len(drift_alerts)} "
        f"holding={len(holding_alerts)} intraday={len(intraday_alerts)} "
        f"total={total} diagnostics={diagnostics}"
    )

    return {
        "drift_alerts": drift_alerts,
        "holding_period_alerts": holding_alerts,
        "intraday_alerts": intraday_alerts,
        "total_alerts": total,
        "diagnostics": diagnostics,
    }


async def check_position_drift(
    threshold: float = DEFAULT_DRIFT_THRESHOLD,
) -> list[dict]:
    """
    Compare target_weights vs current_weights.
    Alert if |target - current| > threshold for any held position.
    """
    snapshot, rows = await _latest_heartbeat_holdings()

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
    snapshot, rows = await _latest_heartbeat_holdings()

    if not rows:
        return []
    if not _holding_days_are_trusted(snapshot):
        logger.info(
            "[position_manager] holding-period alerts skipped: heartbeat schema_version=%s",
            getattr(snapshot, "schema_version", None),
        )
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
    snapshot, rows = await _latest_heartbeat_holdings()

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
            current_weight = float(h.weight_current or 0)
            if current_weight <= 0.01:
                continue
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
                    "current_weight": round(current_weight, 4),
                    "message": (
                        f"{ticker} ATR {atr:.2%} elevated "
                        f"(threshold {atr_threshold:.1f}x). Volatility elevated."
                    ),
                })
        except (TypeError, ValueError):
            continue

    return alerts


async def _latest_heartbeat_holdings() -> tuple[QCSnapshot | None, list[HoldingsFactor]]:
    """Return holdings rows attached to the latest heartbeat snapshot only."""
    async with AsyncSessionLocal() as db:
        snapshot = (
            await db.execute(
                select(QCSnapshot)
                .where(QCSnapshot.packet_type == "heartbeat")
                .order_by(desc(QCSnapshot.received_at))
                .limit(1)
            )
        ).scalar_one_or_none()
        if not snapshot:
            return None, []
        rows = (
            await db.execute(
                select(HoldingsFactor)
                .where(HoldingsFactor.snapshot_id == snapshot.id)
            )
        ).scalars().all()
    return snapshot, list(rows)


def _holding_days_are_trusted(snapshot: QCSnapshot | None) -> bool:
    """Schema 1.3 holding_days was polluted by QC warm-up history replay."""
    version = getattr(snapshot, "schema_version", None)
    try:
        return float(str(version)) >= 1.4
    except (TypeError, ValueError):
        return False


def _build_position_monitor_diagnostics(
    *,
    snapshot: QCSnapshot | None,
    rows: list[HoldingsFactor],
    max_holding_days: int,
    atr_threshold: float,
) -> dict[str, Any]:
    schema_version = getattr(snapshot, "schema_version", None)
    holding_days_trusted = _holding_days_are_trusted(snapshot)
    held_rows = []
    unheld_rows = []
    holding_days_values: list[int] = []
    unheld_high_atr = 0

    for row in rows or []:
        try:
            weight = float(getattr(row, "weight_current", 0) or 0)
        except (TypeError, ValueError):
            weight = 0.0
        target = held_rows if weight > 0.01 else unheld_rows
        target.append(row)

        if weight > 0.01:
            try:
                holding_days_values.append(int(getattr(row, "holding_days", 0) or 0))
            except (TypeError, ValueError):
                pass
        else:
            try:
                atr = abs(float(getattr(row, "atr_pct", 0) or 0))
                if atr > atr_threshold * 0.01:
                    unheld_high_atr += 1
            except (TypeError, ValueError):
                pass

    max_observed_holding_days = max(holding_days_values) if holding_days_values else None
    return {
        "heartbeat_snapshot_id": getattr(snapshot, "id", None),
        "heartbeat_schema_version": schema_version,
        "holding_days_trusted": holding_days_trusted,
        "holding_period_alerts_enabled": holding_days_trusted,
        "holding_period_skip_reason": None if holding_days_trusted else "heartbeat schema_version < 1.4",
        "holdings_rows": len(rows or []),
        "held_positions": len(held_rows),
        "unheld_rows_filtered": len(unheld_rows),
        "unheld_high_atr_filtered": unheld_high_atr,
        "max_observed_holding_days": max_observed_holding_days,
        "max_holding_days_threshold": max_holding_days,
        "atr_threshold_x": atr_threshold,
    }


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
