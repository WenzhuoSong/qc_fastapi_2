# services/dynamic_scheduler.py
"""
Dynamic event-driven pipeline scheduler.

Checks MacroEventsCache + EarningsCalendar for high-impact events within
1 trading day and triggers extra pipeline runs if debounce allows.

Called by cron/dynamic_scheduler.py every 30 min during market hours.
Uses system_config["dynamic_scheduler_state"] for debounce state.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Optional

from sqlalchemy import select

from db.session import AsyncSessionLocal
from db.models import EarningsCalendar, MacroEventsCache, HoldingsFactor, QCSnapshot
from db.queries import get_system_config, upsert_system_config, get_latest_snapshots
from services.pipeline import run_full_pipeline
from services.trading_analysis_gate import evaluate_trading_analysis_gate

logger = logging.getLogger("qc_fastapi_2.dynamic_scheduler")

DEBOUNCE_MINUTES = 90       # don't re-trigger same event within 90 min
IMMINENT_DAYS = 1           # look-ahead window for events
HIGH_IMPACT_ONLY = True     # only confirmed earnings to reduce noise


# ─────────────────────────── Public API ───────────────────────────────────────


async def check_and_trigger() -> dict:
    """
    Check for imminent high-impact events and trigger extra pipeline runs.
    Returns {"checked_at": iso_string, "actions": [{"trigger": str, "result": str}]}.
    """
    now_utc = datetime.utcnow()
    today = date.today()
    actions: list[dict] = []

    # Load debounce state
    state: dict = {}
    async with AsyncSessionLocal() as db:
        cfg = await get_system_config(db, "dynamic_scheduler_state")
    if cfg and cfg.value:
        state = cfg.value or {}

    gate = await evaluate_trading_analysis_gate()
    if not gate.get("allowed"):
        logger.warning("[dynamic_scheduler] Skipping extra pipeline checks: %s", gate.get("reason"))
        return {
            "checked_at": now_utc.isoformat(),
            "actions": actions,
            "status": "skipped",
            "reason": gate.get("reason"),
            "trading_analysis_gate": gate,
        }

    # ── Macro events (FOMC, CPI) ────────────────────────────────────────────
    macro_triggers = await _check_macro_events(today)
    for event_type, label in macro_triggers:
        trigger_key = f"last_{event_type}_trigger_at"
        if _is_debounced(state.get(trigger_key), now_utc, DEBOUNCE_MINUTES):
            logger.debug(f"[dynamic_scheduler] {event_type} debounced, skipping")
            continue

        trigger_name = f"pre_{event_type}_extra"
        logger.info(f"[dynamic_scheduler] Triggering extra pipeline: {trigger_name} ({label})")
        try:
            result = await run_full_pipeline(trigger=trigger_name)
            logger.info(f"[dynamic_scheduler] {trigger_name} result: {result.get('status')}")
            actions.append({"trigger": trigger_name, "result": result.get("status", "unknown")})
        except Exception as e:
            logger.exception(f"[dynamic_scheduler] {trigger_name} failed: {e}")
            actions.append({"trigger": trigger_name, "result": f"error: {e}"})

        state[trigger_key] = now_utc.isoformat()

    # ── Earnings events ────────────────────────────────────────────────────
    held_tickers = await _get_held_tickers()
    earnings_triggers = await _check_earnings_events(held_tickers, today)

    for ticker, label in earnings_triggers:
        event_type = f"earnings_{ticker}"
        trigger_key = f"last_{event_type}_trigger_at"
        if _is_debounced(state.get(trigger_key), now_utc, DEBOUNCE_MINUTES):
            logger.debug(f"[dynamic_scheduler] {event_type} debounced, skipping")
            continue

        trigger_name = f"pre_earnings_{ticker}"
        logger.info(f"[dynamic_scheduler] Triggering extra pipeline: {trigger_name} ({label})")
        try:
            result = await run_full_pipeline(trigger=trigger_name)
            logger.info(f"[dynamic_scheduler] {trigger_name} result: {result.get('status')}")
            actions.append({"trigger": trigger_name, "result": result.get("status", "unknown")})
        except Exception as e:
            logger.exception(f"[dynamic_scheduler] {trigger_name} failed: {e}")
            actions.append({"trigger": trigger_name, "result": f"error: {e}"})

        state[trigger_key] = now_utc.isoformat()

    # Persist updated debounce state
    if state:
        async with AsyncSessionLocal() as db:
            await upsert_system_config(db, "dynamic_scheduler_state", state, "dynamic_scheduler")

    return {"checked_at": now_utc.isoformat(), "actions": actions}


# ─────────────────────────── Helpers ──────────────────────────────────────────


async def _check_macro_events(today: date) -> list[tuple[str, str]]:
    """
    Return list of (event_type, label) for macro events within IMMINENT_DAYS.
    Checks next_fomc, next_cpi directly and scans nfp_schedule/pmi_schedule.
    """
    cutoff = today + timedelta(days=IMMINENT_DAYS)
    triggers: list[tuple[str, str]] = []

    async with AsyncSessionLocal() as db:
        row = (await db.execute(
            select(MacroEventsCache).where(MacroEventsCache.id == 1)
        )).scalar_one_or_none()

    if not row:
        return []

    # Check stale data (MacroEventsCache updated more than 25h ago)
    if row.updated_at:
        age_hours = (datetime.utcnow() - row.updated_at.replace(tzinfo=None)).total_seconds() / 3600
        if age_hours > 25:
            logger.warning(
                f"[dynamic_scheduler] MacroEventsCache is {age_hours:.1f}h old — "
                "skipping macro event checks"
            )
            return []

    # Direct next_fomc / next_cpi
    if row.next_fomc and today <= row.next_fomc <= cutoff:
        triggers.append(("fomc", f"FOMC on {row.next_fomc}"))

    if row.next_cpi and today <= row.next_cpi <= cutoff:
        triggers.append(("cpi", f"CPI on {row.next_cpi}"))

    # Scan nfp_schedule for imminent NFP
    from services.macro_watcher import _extract_date
    for schedule, evt_type in [
        (row.nfp_schedule, "nfp"),
        (row.pmi_schedule, "pmi"),
    ]:
        if not schedule:
            continue
        for evt in (schedule if isinstance(schedule, list) else []):
            evt_date = _extract_date(evt)
            if evt_date and today <= evt_date <= cutoff:
                triggers.append((evt_type, f"{evt_type.upper()} on {evt_date}"))
                break  # one trigger per type

    return triggers


async def _get_held_tickers() -> list[str]:
    """
    Return tickers currently held (weight_current > 0) from latest QCSnapshot.
    """
    async with AsyncSessionLocal() as db:
        snaps = await get_latest_snapshots(db, limit=1)
        if not snaps:
            return []
        latest = snaps[0]
        result = await db.execute(
            select(HoldingsFactor.ticker)
            .where(HoldingsFactor.snapshot_id == latest.id)
            .where(HoldingsFactor.weight_current > 0)
        )
        return [row[0] for row in result.all()]


async def _check_earnings_events(
    tickers: list[str],
    today: date,
) -> list[tuple[str, str]]:
    """
    Return list of (ticker, label) for confirmed earnings within IMMINENT_DAYS
    for currently held tickers.
    """
    if not tickers:
        return []

    cutoff = today + timedelta(days=IMMINENT_DAYS)

    async with AsyncSessionLocal() as db:
        stmt = (
            select(EarningsCalendar)
            .where(EarningsCalendar.ticker.in_(tickers))
            .where(EarningsCalendar.earnings_date >= today)
            .where(EarningsCalendar.earnings_date <= cutoff)
        )
        if HIGH_IMPACT_ONLY:
            stmt = stmt.where(EarningsCalendar.is_confirmed == True)
        rows = (await db.execute(stmt)).scalars().all()

    return [(r.ticker, f"{r.ticker} earnings on {r.earnings_date}") for r in rows]


def _is_debounced(
    last_at_str: Optional[str],
    now: datetime,
    debounce_minutes: int,
) -> bool:
    """Return True if last trigger was within debounce_minutes ago."""
    if not last_at_str:
        return False
    try:
        last_at = datetime.fromisoformat(last_at_str)
        return (now - last_at).total_seconds() < debounce_minutes * 60
    except (ValueError, TypeError):
        return False
