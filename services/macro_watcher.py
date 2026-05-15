"""
services/macro_watcher.py

Tracks important macroeconomic events: Fed meetings, CPI, NFP, PMI, etc.
Written to macro_events_cache table daily by morning_health cron.
Read by context_assembler for injection into RESEARCHER prompt.

P1-3: MACRO_WATCHER
"""
from __future__ import annotations

import asyncio
import logging
from datetime import date, timedelta
from typing import Optional

logger = logging.getLogger("qc_fastapi_2.macro_watcher")

# Important macro event types to track and their keywords in Finnhub event names
KEY_MACRO_TYPES = {
    "fed": ["fed", "fomc", "federal reserve", "powell"],
    "cpi": ["cpi", "consumer price"],
    "nfp": ["nonfarm", "nfp", "jobs report", "unemployment"],
    "pmi": ["pmi", "purchasing managers", "manufacturing", "ism"],
    "gdp": ["gdp", "gdp estimate", "real gdp"],
    "retail": ["retail sales", "consumer spending"],
}


def _classify_event(event: dict) -> Optional[str]:
    """Classify an economic calendar event by type."""
    name = (event.get("event") or "").lower()
    for macro_type, keywords in KEY_MACRO_TYPES.items():
        if any(kw in name for kw in keywords):
            return macro_type
    return None


def _is_fomc_meeting_event(event: dict) -> bool:
    """Return True only for actual FOMC meetings, not minutes or speeches."""
    name = (event.get("event") or "").lower()
    meeting_terms = (
        "fomc",
        "federal open market committee",
        "fed interest rate decision",
        "federal funds rate",
    )
    if not any(term in name for term in meeting_terms):
        return False
    excluded_terms = (
        "minutes",
        "transcript",
        "statement",
        "press conference",
        "speech",
        "speaks",
        "remarks",
    )
    if any(term in name for term in excluded_terms):
        return False
    return (
        "meeting" in name
        or "interest rate decision" in name
        or "federal funds rate" in name
        or name.strip() in {"fomc", "fomc meeting"}
    )


def _extract_date(event: dict) -> Optional[date]:
    """Extract date from Finnhub calendar event."""
    for field in ("datetime", "date", "time"):
        val = event.get(field)
        if val:
            if isinstance(val, (int, float)):
                from datetime import datetime
                try:
                    return datetime.fromtimestamp(val).date()
                except (ValueError, OSError):
                    pass
            elif isinstance(val, str):
                try:
                    return date.fromisoformat(val[:10])
                except ValueError:
                    pass
    return None


async def update_macro_events_cache(days_ahead: int = 60) -> dict:
    """
    Fetch economic calendar from Finnhub and update macro_events_cache.
    Returns {"events": N, "by_type": {...}, "next_fomc": date, "next_cpi": date}.
    """
    from services.finnhub_client import fetch_economic_calendar

    loop = asyncio.get_event_loop()
    raw_events = await loop.run_in_executor(None, fetch_economic_calendar, days_ahead)
    if not raw_events:
        logger.warning("[macro_watcher] No events returned from Finnhub")
        return {"events": 0}

    # Classify events
    by_type: dict[str, list] = {k: [] for k in KEY_MACRO_TYPES}
    other: list = []
    next_fomc: Optional[date] = None
    next_cpi: Optional[date] = None

    for evt in raw_events:
        evt_type = _classify_event(evt)
        evt_date = _extract_date(evt)
        if evt_type:
            by_type[evt_type].append(evt)
            if evt_type == "fed" and evt_date and _is_fomc_meeting_event(evt):
                if next_fomc is None or evt_date < next_fomc:
                    next_fomc = evt_date
            if evt_type == "cpi" and evt_date:
                if next_cpi is None or evt_date < next_cpi:
                    next_cpi = evt_date
        else:
            other.append(evt)

    from sqlalchemy import select

    from db.models import MacroEventsCache
    from db.session import AsyncSessionLocal

    async with AsyncSessionLocal() as db:
        stmt = select(MacroEventsCache).where(MacroEventsCache.id == 1)
        existing = (await db.execute(stmt)).scalar_one_or_none()

        fields = {
            "economic_calendar": raw_events,
            "fed_schedule":       by_type.get("fed", []),
            "cpi_schedule":       by_type.get("cpi", []),
            "nfp_schedule":       by_type.get("nfp", []),
            "pmi_schedule":      by_type.get("pmi", []),
            "next_fomc":          next_fomc,
            "next_cpi":          next_cpi,
        }

        if existing:
            for k, v in fields.items():
                setattr(existing, k, v)
        else:
            db.add(MacroEventsCache(id=1, **fields))
        await db.commit()

    total = sum(len(v) for v in by_type.values())
    logger.info(
        f"[macro_watcher] stored {total} events | "
        f"fed={len(by_type['fed'])} cpi={len(by_type['cpi'])} nfp={len(by_type['nfp'])} pmi={len(by_type['pmi'])}"
    )
    return {
        "events":   total,
        "by_type":  {k: len(v) for k, v in by_type.items()},
        "next_fomc": str(next_fomc) if next_fomc else None,
        "next_cpi":  str(next_cpi) if next_cpi else None,
    }


async def get_relevant_macro_events(days: int = 5) -> dict:
    """
    Return relevant macro events for the next N days.
    Used by context_assembler to inject into RESEARCHER prompt.
    """
    from sqlalchemy import select

    from db.models import MacroEventsCache
    from db.session import AsyncSessionLocal

    async with AsyncSessionLocal() as db:
        row = (await db.execute(
            select(MacroEventsCache).where(MacroEventsCache.id == 1)
        )).scalar_one_or_none()

    if not row:
        return {
            "events": [],
            "key_dates": [],
            "market_watch": "No macro events data available.",
            "has_data": False,
        }

    today = date.today()
    cutoff = today + timedelta(days=days)
    all_events: list[dict] = (row.economic_calendar or []) + (row.fed_schedule or [])
    relevant = []
    for e in all_events:
        evt_date = _extract_date(e)
        if evt_date and today <= evt_date <= cutoff:
            relevant.append({
                "event": e.get("event"),
                "date":  str(evt_date),
                "impact": e.get("impact", "medium"),
            })

    key_dates = []
    if row.next_fomc:
        key_dates.append(f"FOMC: {row.next_fomc}")
    if row.next_cpi:
        key_dates.append(f"CPI: {row.next_cpi}")

    return {
        "events":    relevant,
        "key_dates": key_dates,
        "market_watch": _build_watch_prose(row),
        "has_data": True,
    }


def _build_watch_prose(row: MacroEventsCache) -> str:
    """Build short text summary of upcoming macro events."""
    parts = []
    if row.next_fomc:
        parts.append(f"Next FOMC: {row.next_fomc}")
    if row.next_cpi:
        parts.append(f"Next CPI: {row.next_cpi}")

    n_events = sum(
        len(getattr(row, f"{t}_schedule") or [])
        for t in ["fed", "cpi", "nfp", "pmi"]
    )
    if n_events > 0:
        parts.append(f"{n_events} high-impact events this period")

    return " | ".join(parts) if parts else "No major macro events scheduled."
