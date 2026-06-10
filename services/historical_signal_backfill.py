"""One-shot historical signal replay backfill.

This module persists historical-prior signal labels from stored daily market
features. It is intentionally observe-only: it never writes execution commands,
target weights, AgentAnalysis rows, or operator decisions.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

from services.historical_signal_replay import (
    DEFAULT_HORIZONS,
    SIGNAL_SOURCE_YFINANCE_REPLAY,
    HistoricalSignalReplayResult,
    replay_historical_signals,
)
from services.market_feature_store import get_market_daily_feature_rows, model_to_feature_dict
from services.signal_ledger import persist_frozen_signals
from services.signal_outcome_labeler import persist_signal_outcomes
from services.strategy_conviction import compute_conviction_profiles, persist_conviction_profiles
from strategies import get_strategy


DEFAULT_HISTORICAL_BACKFILL_STRATEGIES = [
    "momentum_lite_v1",
    "absolute_trend_following_lite",
    "seasonality_month_end_lite",
    "sector_theme_relative_strength_lite",
    "leveraged_long_amplifier_lite",
    "dual_momentum_rotation",
    "mean_reversion_lite",
    "relative_value_reversion_lite",
    "sector_theme_relative_value_reversion_lite",
    "low_vol_factor",
    "defensive_quality_rotation_lite",
    "macro_rate_duration_lite",
    "macro_cyclical_inflation_rotation_lite",
    "carry_cash_proxy_lite",
    "volatility_hedge_lite",
    "inverse_equity_hedge_lite",
    "risk_parity_lite",
    "equal_weight_benchmark",
]


@dataclass(frozen=True)
class HistoricalSignalBackfillPlan:
    replay: HistoricalSignalReplayResult
    selected_start_date: date | None
    selected_end_date: date | None
    feature_rows_seen: int
    trading_dates_seen: int
    summary: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "selected_start_date": self.selected_start_date.isoformat() if self.selected_start_date else None,
            "selected_end_date": self.selected_end_date.isoformat() if self.selected_end_date else None,
            "feature_rows_seen": self.feature_rows_seen,
            "trading_dates_seen": self.trading_dates_seen,
            "signals_generated": len(self.replay.signals),
            "outcomes_generated": len(self.replay.outcomes),
            "summary": dict(self.summary),
        }


@dataclass(frozen=True)
class HistoricalSignalBackfillResult:
    write_enabled: bool
    plan: HistoricalSignalBackfillPlan
    frozen_inserted: int
    frozen_duplicates: int
    frozen_conflicts: list[dict[str, Any]]
    outcomes_inserted: int
    outcome_duplicates: int
    outcome_conflicts: list[dict[str, Any]]
    profiles_generated: int
    profiles_inserted: int
    profiles_updated: int
    profile_duplicates: int
    summary: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "write_enabled": self.write_enabled,
            "plan": self.plan.to_dict(),
            "frozen_inserted": self.frozen_inserted,
            "frozen_duplicates": self.frozen_duplicates,
            "frozen_conflict_count": len(self.frozen_conflicts),
            "outcomes_inserted": self.outcomes_inserted,
            "outcome_duplicates": self.outcome_duplicates,
            "outcome_conflict_count": len(self.outcome_conflicts),
            "profiles_generated": self.profiles_generated,
            "profiles_inserted": self.profiles_inserted,
            "profiles_updated": self.profiles_updated,
            "profile_duplicates": self.profile_duplicates,
            "summary": dict(self.summary),
        }


def build_historical_signal_backfill_plan(
    feature_rows: list[Any],
    *,
    strategy_names: list[str],
    horizons: tuple[int, ...] = DEFAULT_HORIZONS,
    start_date: date | None = None,
    end_date: date | None = None,
    max_dates: int | None = 120,
    source: str = "yfinance",
    generated_at: datetime | None = None,
) -> HistoricalSignalBackfillPlan:
    """Build a historical-prior replay plan from normalized daily features."""
    normalized = [model_to_feature_dict(row) if not isinstance(row, dict) else dict(row) for row in feature_rows]
    available_dates = sorted({
        _parse_date(row.get("trading_date"))
        for row in normalized
        if _parse_date(row.get("trading_date")) is not None
    })
    selected_start, selected_end = _selected_replay_window(
        available_dates,
        start_date=start_date,
        end_date=end_date,
        max_dates=max_dates,
    )
    replay = replay_historical_signals(
        normalized,
        strategy_names=strategy_names,
        horizons=horizons,
        start_date=selected_start,
        end_date=selected_end,
        max_dates=None,
        mode="historical_backfill",
        signal_source=SIGNAL_SOURCE_YFINANCE_REPLAY,
        generated_at=generated_at or datetime.now(timezone.utc),
    )
    summary = {
        **replay.summary,
        "execution_authority": "none",
        "target_weight_mutation": "none",
        "source_bucket": "historical_prior",
        "reliability": "historical_prior",
        "requires_live_confirmation": True,
        "training_authority": "limited_historical_prior",
        "feature_source": source,
        "feature_authority": "daily_research",
        "selected_start_date": selected_start.isoformat() if selected_start else None,
        "selected_end_date": selected_end.isoformat() if selected_end else None,
        "max_dates": max_dates,
    }
    return HistoricalSignalBackfillPlan(
        replay=replay,
        selected_start_date=selected_start,
        selected_end_date=selected_end,
        feature_rows_seen=len(normalized),
        trading_dates_seen=len(available_dates),
        summary=summary,
    )


async def run_historical_signal_backfill(
    db: Any,
    *,
    strategy_names: list[str] | None = None,
    horizons: tuple[int, ...] = DEFAULT_HORIZONS,
    start_date: date | None = None,
    end_date: date | None = None,
    max_dates: int | None = 120,
    source: str = "yfinance",
    write: bool = False,
    profile_signal_limit: int = 20000,
) -> HistoricalSignalBackfillResult:
    """Read stored daily features, replay signals, and optionally persist."""
    strategies = list(strategy_names or DEFAULT_HISTORICAL_BACKFILL_STRATEGIES)
    tickers = _strategy_universe(strategies)
    effective_start = start_date
    effective_end = end_date
    if effective_start is None and max_dates is not None and max_dates > 0:
        window_start, window_end = await _latest_feature_window(
            db,
            tickers=tickers,
            source=source,
            max_dates=max_dates,
            end_date=effective_end,
        )
        effective_start = window_start
        effective_end = effective_end or window_end
    rows = await get_market_daily_feature_rows(
        db,
        tickers=tickers,
        start_date=effective_start,
        end_date=effective_end,
        source=source,
    )
    plan = build_historical_signal_backfill_plan(
        rows,
        strategy_names=strategies,
        horizons=horizons,
        start_date=effective_start,
        end_date=effective_end,
        max_dates=max_dates,
        source=source,
    )
    if not write:
        return HistoricalSignalBackfillResult(
            write_enabled=False,
            plan=plan,
            frozen_inserted=0,
            frozen_duplicates=0,
            frozen_conflicts=[],
            outcomes_inserted=0,
            outcome_duplicates=0,
            outcome_conflicts=[],
            profiles_generated=0,
            profiles_inserted=0,
            profiles_updated=0,
            profile_duplicates=0,
            summary={
                **plan.summary,
                "dry_run": True,
            },
        )

    frozen_result = await persist_frozen_signals(db, plan.replay.signals)
    outcome_result = await persist_signal_outcomes(db, plan.replay.outcomes)
    persisted_signals = await _read_profile_signals(
        db,
        as_of_date=plan.selected_end_date,
        limit=profile_signal_limit,
    )
    persisted_outcomes = await _read_outcomes_for_signals(
        db,
        signal_ids=[str(getattr(row, "signal_id", "")) for row in persisted_signals],
    )
    created = datetime.now(timezone.utc)
    conviction = compute_conviction_profiles(
        persisted_signals,
        persisted_outcomes,
        as_of_date=plan.selected_end_date or datetime.now(timezone.utc).date(),
        created_at=created,
    )
    profile_result = await persist_conviction_profiles(db, conviction.profiles)
    summary = {
        **plan.summary,
        "dry_run": False,
        "signals_persisted_scope": "strategy_frozen_signals",
        "outcomes_persisted_scope": "strategy_signal_outcomes",
        "profiles_recomputed_from_persisted_signals": len(persisted_signals),
        "persisted_outcomes_seen": len(persisted_outcomes),
        "frozen_inserted": frozen_result.inserted,
        "frozen_duplicates": frozen_result.duplicates,
        "frozen_conflicts": len(frozen_result.conflicts),
        "outcomes_inserted": outcome_result.inserted,
        "outcome_duplicates": outcome_result.duplicates,
        "outcome_conflicts": len(outcome_result.conflicts),
        "profiles_generated": len(conviction.profiles),
        "profiles_inserted": profile_result.inserted,
        "profiles_updated": profile_result.updated,
        "profile_duplicates": profile_result.duplicates,
        "conviction": conviction.summary,
    }
    return HistoricalSignalBackfillResult(
        write_enabled=True,
        plan=plan,
        frozen_inserted=frozen_result.inserted,
        frozen_duplicates=frozen_result.duplicates,
        frozen_conflicts=frozen_result.conflicts,
        outcomes_inserted=outcome_result.inserted,
        outcome_duplicates=outcome_result.duplicates,
        outcome_conflicts=outcome_result.conflicts,
        profiles_generated=len(conviction.profiles),
        profiles_inserted=profile_result.inserted,
        profiles_updated=profile_result.updated,
        profile_duplicates=profile_result.duplicates,
        summary=summary,
    )


def _selected_replay_window(
    available_dates: list[date],
    *,
    start_date: date | None,
    end_date: date | None,
    max_dates: int | None,
) -> tuple[date | None, date | None]:
    dates = [item for item in available_dates if (start_date is None or item >= start_date)]
    dates = [item for item in dates if (end_date is None or item <= end_date)]
    if not dates:
        return start_date, end_date
    if max_dates is not None and max_dates > 0 and len(dates) > max_dates:
        dates = dates[-int(max_dates):]
    return dates[0], dates[-1]


def _strategy_universe(strategy_names: list[str]) -> list[str]:
    tickers = {"SPY"}
    for name in strategy_names:
        strategy = get_strategy(name)
        tickers.update(str(ticker).upper().strip() for ticker in strategy.universe_tickers if ticker)
    return sorted(ticker for ticker in tickers if ticker)


async def _read_profile_signals(
    db: Any,
    *,
    as_of_date: date | None,
    limit: int,
) -> list[Any]:
    from sqlalchemy import desc, select

    from db.models import StrategyFrozenSignal

    stmt = select(StrategyFrozenSignal).where(
        StrategyFrozenSignal.signal_source.in_(["yfinance_replay", "fastapi_live_freeze"])
    )
    if as_of_date is not None:
        stmt = stmt.where(StrategyFrozenSignal.signal_date <= as_of_date)
    stmt = stmt.order_by(desc(StrategyFrozenSignal.signal_date), desc(StrategyFrozenSignal.id)).limit(int(limit))
    result = await db.execute(stmt)
    return list(result.scalars().all())


async def _latest_feature_window(
    db: Any,
    *,
    tickers: list[str],
    source: str,
    max_dates: int,
    end_date: date | None,
) -> tuple[date | None, date | None]:
    from sqlalchemy import desc, select

    from db.models import MarketDailyFeature

    stmt = (
        select(MarketDailyFeature.trading_date)
        .where(MarketDailyFeature.source == source)
        .where(MarketDailyFeature.ticker.in_(tickers))
        .distinct()
        .order_by(desc(MarketDailyFeature.trading_date))
        .limit(int(max_dates))
    )
    if end_date is not None:
        stmt = stmt.where(MarketDailyFeature.trading_date <= end_date)
    result = await db.execute(stmt)
    dates = sorted(item for item in result.scalars().all() if item is not None)
    if not dates:
        return None, end_date
    return dates[0], dates[-1]


async def _read_outcomes_for_signals(db: Any, *, signal_ids: list[str]) -> list[Any]:
    if not signal_ids:
        return []
    from sqlalchemy import select

    from db.models import StrategySignalOutcome

    result = await db.execute(
        select(StrategySignalOutcome).where(StrategySignalOutcome.signal_id.in_(signal_ids))
    )
    return list(result.scalars().all())


def _parse_date(value: Any) -> date | None:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str) and value.strip():
        try:
            return date.fromisoformat(value[:10])
        except ValueError:
            return None
    return None
