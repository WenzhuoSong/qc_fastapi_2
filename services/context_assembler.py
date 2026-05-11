"""
services/context_assembler.py

Reads historical market memory from MemoryDaily and MemoryWeekly tables
and assembles structured context for injection into the RESEARCHER prompt.

Called during Stage 1 (market_brief) of the pipeline. Results are attached to
brief["memory_context"] and consumed by the RESEARCHER agent in Stage 3.

Failure is always silent: any exception returns an empty context dict so the
pipeline continues unaffected.
"""

from __future__ import annotations

import logging
from collections import Counter
from datetime import date, timedelta
from typing import Optional

from sqlalchemy import select

from db.models import MemoryDaily, MemoryWeekly, MemoryMonthly
from db.session import AsyncSessionLocal
from services.earnings_tracker import get_upcoming_earnings
from services.macro_watcher import get_relevant_macro_events

logger = logging.getLogger("qc_fastapi_2.context_assembler")


async def assemble_memory_context(
    today: Optional[date] = None,
    daily_lookback: int = 5,
    weekly_lookback: int = 2,
    monthly_lookback: int = 2,
) -> dict:
    """
    Read recent memory and return a structured context dict.

    Returns:
        {
            "recent_days": [...],         # last N daily memories
            "recent_weeks": [...],        # last N weekly memories
            "recent_months": [...],       # last N monthly memories (P1-4)
            "memory_prose": str,          # prose summary for LLM prompt injection
            "regime_trend": str,          # description of recent regime trend
            "has_memory": bool,           # whether any valid memory data exists
            "earnings_context": dict,     # P1-3
            "macro_events_context": dict, # P1-3
        }
    """
    if today is None:
        today = date.today()

    try:
        async with AsyncSessionLocal() as session:
            recent_days = await _get_recent_daily(session, today, daily_lookback)
            recent_weeks = await _get_recent_weekly(session, today, weekly_lookback)
            recent_months = await _get_recent_monthly(session, today, monthly_lookback)

        if not recent_days and not recent_weeks and not recent_months:
            return _empty_context()

        prose = _build_memory_prose(recent_days, recent_weeks, recent_months)
        regime_trend = _compute_regime_trend(recent_days)

        # P1-3: earnings + macro context
        earnings_context = await _get_earnings_context()
        macro_context = await _get_macro_context()

        return {
            "recent_days": [_serialize_daily(m) for m in recent_days],
            "recent_weeks": [_serialize_weekly(m) for m in recent_weeks],
            "recent_months": [_serialize_monthly(m) for m in recent_months],
            "memory_prose": prose,
            "regime_trend": regime_trend,
            "has_memory": True,
            "earnings_context": earnings_context,
            "macro_events_context": macro_context,
        }

    except Exception as e:
        logger.error(f"[CONTEXT_ASSEMBLER] Failed to read memory: {e}")
        return _empty_context()


# ── DB Helpers ────────────────────────────────────────────────────────────────


async def _get_recent_daily(session, today: date, n: int):
    """Get up to N recent MemoryDaily records before today."""
    cutoff = today - timedelta(days=n + 3)  # extra buffer for weekends/holidays
    result = await session.execute(
        select(MemoryDaily)
        .where(MemoryDaily.trading_date >= cutoff)
        .where(MemoryDaily.trading_date < today)  # exclude today (not yet complete)
        .order_by(MemoryDaily.trading_date.desc())
        .limit(n)
    )
    return list(result.scalars().all())


async def _get_recent_weekly(session, today: date, n: int):
    """Get up to N recent MemoryWeekly records."""
    cutoff = today - timedelta(weeks=n + 1)
    result = await session.execute(
        select(MemoryWeekly)
        .where(MemoryWeekly.week_start >= cutoff)
        .order_by(MemoryWeekly.week_start.desc())
        .limit(n)
    )
    return list(result.scalars().all())


async def _get_recent_monthly(session, today: date, n: int):
    """Get up to N recent MemoryMonthly records."""
    cutoff = today - timedelta(days=n * 35)
    result = await session.execute(
        select(MemoryMonthly)
        .where(MemoryMonthly.month_start >= cutoff)
        .order_by(MemoryMonthly.month_start.desc())
        .limit(n)
    )
    return list(result.scalars().all())


# ── Prose Assembly ────────────────────────────────────────────────────────────


def _build_memory_prose(
    daily_list: list, weekly_list: list, monthly_list: list
) -> str:
    """
    Build a prose summary for LLM prompt injection.
    Kept within ~800 token budget: only the most recent week, 3 days, and monthly context.
    """
    parts: list[str] = []

    # P1-4: Monthly context (most recent)
    if monthly_list:
        latest_month = monthly_list[0]
        parts.append(
            f"[Last Month Summary] {latest_month.month_start} ~ {latest_month.month_end}:\n"
            f"Dominant regime: {latest_month.dominant_regime} "
            f"(stability: {latest_month.regime_stability or 'unknown'}).\n"
            f"Key macro themes: {', '.join(latest_month.macro_themes or [])}\n"
            f"Momentum effectiveness: {latest_month.momentum_effectiveness}.\n"
            f"Next-month watch: {latest_month.next_month_watch or 'none'}."
        )

    if weekly_list:
        latest_week = weekly_list[0]
        regime_detail = ""
        if latest_week.regime_shift:
            regime_detail = f"Regime shifted this week: {latest_week.regime_shift_detail or 'details unavailable'}."
        else:
            regime_detail = "No regime shift this week."

        parts.append(
            f"[Last Week Summary] {latest_week.week_start} to {latest_week.week_end}:\n"
            f"Dominant regime: {latest_week.dominant_regime}. {regime_detail}\n"
            f"Key macro themes: {', '.join(latest_week.macro_themes or [])}\n"
            f"Momentum factor effectiveness: {latest_week.momentum_effectiveness}\n"
            f"Next-week outlook: {latest_week.next_week_watch or 'none'}"
        )

    if daily_list:
        parts.append("[Recent Daily Memories (newest first)]")
        for m in daily_list[:3]:  # only last 3 days to stay within token budget
            parts.append(
                f"  {m.trading_date}: Regime={m.regime_label}, "
                f"Stance={m.recommended_stance}, Approved={m.risk_approved}\n"
                f"  Narrative: {m.macro_narrative or 'N/A'}\n"
                f"  Key events: {', '.join(m.key_events or [])}"
            )

    return "\n\n".join(parts) if parts else "No historical memory data available."


def _compute_regime_trend(daily_list: list) -> str:
    """Simple regime distribution analysis for trend description."""
    if not daily_list:
        return "No historical data"

    regimes = [m.regime_label for m in daily_list if m.regime_label]
    if not regimes:
        return "No historical data"

    counts = Counter(regimes)
    dominant = counts.most_common(1)[0][0]
    unique = len(set(regimes))

    if unique == 1:
        return f"Past days consistently {dominant}"
    elif unique == 2:
        return f"Past days alternating between {' / '.join(sorted(set(regimes)))}"
    else:
        return f"Recent regime has been volatile (mostly {dominant})"


# ── Serialization ─────────────────────────────────────────────────────────────


def _serialize_daily(m: MemoryDaily) -> dict:
    return {
        "date": str(m.trading_date),
        "regime": m.regime_label,
        "stance": m.recommended_stance,
        "approved": m.risk_approved,
        "narrative": m.macro_narrative,
        "key_events": m.key_events or [],
        "top3_overweight": m.top3_overweight or [],
    }


def _serialize_weekly(m: MemoryWeekly) -> dict:
    return {
        "week_start": str(m.week_start),
        "week_end": str(m.week_end),
        "dominant_regime": m.dominant_regime,
        "regime_shift": m.regime_shift,
        "macro_themes": m.macro_themes or [],
        "momentum_effectiveness": m.momentum_effectiveness,
        "next_week_watch": m.next_week_watch,
        "weekly_return_pct": m.weekly_return_pct,
    }


def _serialize_monthly(m: MemoryMonthly) -> dict:
    return {
        "month_start": str(m.month_start),
        "month_end": str(m.month_end),
        "dominant_regime": m.dominant_regime,
        "regime_stability": m.regime_stability,
        "macro_themes": m.macro_themes or [],
        "momentum_effectiveness": m.momentum_effectiveness,
        "key_lessons": m.key_lessons or [],
        "next_month_watch": m.next_month_watch,
        "monthly_return_pct": m.monthly_return_pct,
    }


def _empty_context() -> dict:
    return {
        "recent_days": [],
        "recent_weeks": [],
        "memory_prose": "No historical memory data available (system may be running for the first time).",
        "regime_trend": "No historical data",
        "has_memory": False,
        "earnings_context": _empty_earnings_context(),
        "macro_events_context": _empty_macro_context(),
    }


# ── Earnings Context (P1-3) ─────────────────────────────────────────────────────


async def _get_earnings_context() -> dict:
    """Fetch upcoming earnings for held tickers (next 7 days)."""
    try:
        from constants import ETF_UNIVERSE
        tickers = list(ETF_UNIVERSE)
        upcoming = await get_upcoming_earnings(tickers, days=7)
        if not upcoming:
            return _empty_earnings_context()

        prose_parts = [f"Upcoming earnings (next 7 days):"]
        for e in upcoming:
            confirmed = "confirmed" if e.get("is_confirmed") else "estimated"
            prose_parts.append(
                f"  {e['ticker']}: {e['earnings_date']} ({confirmed}, "
                f"{e.get('days_until', '?')}d)"
            )
        return {
            "has_earnings": True,
            "upcoming": upcoming,
            "earnings_prose": "\n".join(prose_parts),
        }
    except Exception as e:
        logger.warning(f"[context_assembler] earnings context failed: {e}")
        return _empty_earnings_context()


def _empty_earnings_context() -> dict:
    return {
        "has_earnings": False,
        "upcoming": [],
        "earnings_prose": "No upcoming earnings events in the next 7 days.",
    }


# ── Macro Events Context (P1-3) ─────────────────────────────────────────────────


async def _get_macro_context() -> dict:
    """Fetch relevant macro events for the next 5 days."""
    try:
        macro = await get_relevant_macro_events(days=5)
        return macro
    except Exception as e:
        logger.warning(f"[context_assembler] macro context failed: {e}")
        return _empty_macro_context()


def _empty_macro_context() -> dict:
    return {
        "events": [],
        "key_dates": [],
        "market_watch": "No macro events data available.",
        "has_data": False,
    }