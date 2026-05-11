"""
services/earnings_tracker.py

Tracks upcoming earnings release dates for universe tickers.
Written to earnings_calendar table daily by morning_health cron.
Read by RISK MGR hard_risk_filter and context_assembler.

P1-3: EARNINGS_TRACKER
"""
from __future__ import annotations

import asyncio
import logging
from datetime import date, timedelta
from typing import Optional

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from constants import ETF_UNIVERSE
from db.session import AsyncSessionLocal
from db.models import EarningsCalendar
from services.finnhub_client import fetch_earnings_calendar_detail

logger = logging.getLogger("qc_fastapi_2.earnings_tracker")


async def update_earnings_calendar(
    tickers: Optional[list[str]] = None,
    weeks_ahead: int = 4,
) -> dict:
    """
    Fetch and upsert earnings calendar for given tickers (default: ETF_UNIVERSE).
    Returns {"updated": N, "skipped": M, "errors": [...]}.
    """
    if tickers is None:
        tickers = list(ETF_UNIVERSE)

    updated = 0
    skipped = 0
    errors = []

    for ticker in tickers:
        try:
            items = await _fetch_and_upsert_ticker(ticker, weeks_ahead)
            if items is None:
                skipped += 1
            else:
                updated += len(items)
        except Exception as e:
            logger.error(f"[earnings_tracker] {ticker} error: {e}")
            errors.append(f"{ticker}: {e}")

    logger.info(f"[earnings_tracker] updated={updated} skipped={skipped} errors={len(errors)}")
    return {"updated": updated, "skipped": skipped, "errors": errors}


async def _fetch_and_upsert_ticker(ticker: str, weeks_ahead: int) -> list[dict] | None:
    """Fetch from Finnhub and upsert into earnings_calendar. Returns rows written or None."""
    loop = asyncio.get_event_loop()
    items = await loop.run_in_executor(None, fetch_earnings_calendar_detail, ticker, weeks_ahead)
    if not items:
        return None

    async with AsyncSessionLocal() as db:
        for item in items:
            earnings_date_str = item.get("earnings_date")
            if not earnings_date_str:
                continue
            try:
                earnings_date = date.fromisoformat(earnings_date_str)
            except (ValueError, TypeError):
                continue

            stmt = insert(EarningsCalendar).values(
                ticker=ticker,
                company_name=item.get("company_name"),
                earnings_date=earnings_date,
                eps_estimate=item.get("eps_estimate"),
                eps_actual=item.get("eps_actual"),
                revenue_estimate=item.get("revenue_estimate"),
                revenue_actual=item.get("revenue_actual"),
                is_confirmed=item.get("is_confirmed", False),
            )
            stmt = stmt.on_conflict_do_update(
                constraint="uq_earnings_ticker_date",
                set_={
                    "eps_estimate": stmt.excluded.eps_estimate,
                    "eps_actual": stmt.excluded.eps_actual,
                    "revenue_estimate": stmt.excluded.revenue_estimate,
                    "revenue_actual": stmt.excluded.revenue_actual,
                    "is_confirmed": stmt.excluded.is_confirmed,
                    "updated_at": stmt.excluded.updated_at,
                },
            )
            await db.execute(stmt)
        await db.commit()

    return items


async def get_upcoming_earnings(
    tickers: list[str],
    days: int = 7,
) -> list[dict]:
    """
    Return upcoming earnings for given tickers within N days.
    Used by RISK MGR hard_risk_filter and context_assembler.
    """
    today = date.today()
    cutoff = today + timedelta(days=days)

    async with AsyncSessionLocal() as db:
        stmt = select(EarningsCalendar).where(
            EarningsCalendar.ticker.in_(tickers),
            EarningsCalendar.earnings_date >= today,
            EarningsCalendar.earnings_date <= cutoff,
        ).order_by(EarningsCalendar.earnings_date)
        rows = (await db.execute(stmt)).scalars().all()

    return [
        {
            "ticker":        r.ticker,
            "company_name":  r.company_name,
            "earnings_date": str(r.earnings_date),
            "is_confirmed":  r.is_confirmed,
            "days_until":    (r.earnings_date - today).days,
        }
        for r in rows
    ]


def get_hard_risk_earnings_tickers(
    upcoming_earnings: list[dict],
    current_weights: dict[str, float],
) -> dict[str, dict]:
    """
    Build hard_risks_map entries for tickers with upcoming earnings.
    Only flag if ticker is held (current_weights > 0).
    """
    hard_risks = {}
    for e in upcoming_earnings:
        ticker = e.get("ticker", "").upper()
        if ticker in current_weights and current_weights[ticker] > 0:
            hard_risks[ticker] = {
                "earnings_soon": (
                    f"Earnings {e['earnings_date']} "
                    f"({'confirmed' if e.get('is_confirmed') else 'estimated'}, "
                    f"{e.get('days_until', '?')}d away)"
                )
            }
    return hard_risks