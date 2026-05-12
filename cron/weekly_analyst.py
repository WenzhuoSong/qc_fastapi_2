"""
cron/weekly_analyst.py

Runs each Friday at 17:30 ET (after daily_analyst).
Aggregates this week's MemoryDaily records, uses GPT-4o to distill them
into a weekly memory, and upserts memory_weekly.
"""

import asyncio
import json
import logging
from datetime import date, timedelta

from openai import AsyncOpenAI
from sqlalchemy import select, func

from config import get_settings
from db.models import MemoryDaily, MemoryWeekly, PortfolioTimeseries
from db.session import AsyncSessionLocal

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("qc_fastapi_2.cron.weekly_analyst")
settings = get_settings()

# ── LLM Distillation Prompts ──────────────────────────────────────────────────

WEEKLY_ANALYST_SYSTEM = """You are a weekly memory distillation expert for a quantitative trading system.
Based on this week's daily memories, identify higher-level market patterns,
sector rotation signals, and strategy effectiveness.
Output must be valid JSON with no markdown code fences."""

WEEKLY_ANALYST_USER_TEMPLATE = """
Please distill this week's market memory based on the following {day_count} days of daily records.

## This Week's Daily Memory Summaries
{daily_summaries}

## This Week's Portfolio Performance
- Weekly Return: {weekly_return}%
- Execution Count: {execution_count}

Output the following JSON structure:

{{
  "dominant_regime": "trending_bull | trending_bear | high_vol | mean_reverting | defensive",
  "regime_shift": true or false,
  "regime_shift_detail": "description of the shift if it happened, otherwise null",
  "macro_themes": ["theme1", "theme2", "theme3"],
  "sector_rotation_signal": "sector rotation summary, ≤100 chars",
  "momentum_effectiveness": "strong | moderate | weak | failed",
  "signal_conflicts": ["ticker1 (large bull/bear disagreement)", "ticker2"],
  "best_calls": [{{"ticker": "XLK", "detail": "accurate call description"}}],
  "worst_calls": [{{"ticker": "TLT", "detail": "inaccurate call description"}}],
  "next_week_watch": "key risks or opportunities to watch next week, ≤100 chars",
  "calendar_events": ["event1", "event2"]
}}
"""


# ── Main Entry ────────────────────────────────────────────────────────────────


async def main() -> None:
    today = date.today()
    # Monday to Friday of current week
    week_start = today - timedelta(days=today.weekday())
    week_end = today

    logger.info(f"[WEEKLY_ANALYST] Processing week: {week_start} ~ {week_end}")

    # Phase 3: Run DECAY_DETECTOR before LLM distillation
    try:
        from services.decay_detector import evaluate_decay_signal
        decay_result = await evaluate_decay_signal()
        logger.info(
            f"[WEEKLY_ANALYST] DECAY_DETECTOR: "
            f"strength={decay_result.decay_signal_strength}, "
            f"recommendation={decay_result.recommendation}"
        )
    except Exception as e:
        logger.warning(f"[WEEKLY_ANALYST] DECAY_DETECTOR failed: {e}")

    async with AsyncSessionLocal() as session:
        # 1. Read all MemoryDaily records for this week
        daily_memories = await _get_week_daily_memories(session, week_start, week_end)
        if not daily_memories:
            logger.warning("[WEEKLY_ANALYST] No daily memories this week, skipping")
            return

        # 2. Compute weekly portfolio return
        weekly_return = await _compute_weekly_return(session, week_start, week_end)

        # 3. LLM distillation
        memory_data = await _extract_weekly_memory(daily_memories, weekly_return)

        # 4. Upsert memory_weekly
        await _upsert_memory_weekly(
            session, week_start, week_end, daily_memories, weekly_return, memory_data
        )

    logger.info(f"[WEEKLY_ANALYST] Weekly memory write complete: {week_start}")


# ── Helpers ───────────────────────────────────────────────────────────────────


async def _get_week_daily_memories(session, week_start: date, week_end: date):
    """Get all MemoryDaily records for the given week."""
    result = await session.execute(
        select(MemoryDaily)
        .where(MemoryDaily.trading_date >= week_start)
        .where(MemoryDaily.trading_date <= week_end)
        .order_by(MemoryDaily.trading_date.asc())
    )
    return result.scalars().all()


async def _compute_weekly_return(session, week_start: date, week_end: date) -> float:
    """Estimate weekly return from first and last PortfolioTimeseries of the week."""
    try:
        result_start = await session.execute(
            select(PortfolioTimeseries.total_value)
            .where(func.date(PortfolioTimeseries.recorded_at) >= week_start)
            .order_by(PortfolioTimeseries.recorded_at.asc())
            .limit(1)
        )
        result_end = await session.execute(
            select(PortfolioTimeseries.total_value)
            .where(func.date(PortfolioTimeseries.recorded_at) <= week_end)
            .order_by(PortfolioTimeseries.recorded_at.desc())
            .limit(1)
        )
        v_start = result_start.scalar_one_or_none()
        v_end = result_end.scalar_one_or_none()
        if v_start and v_end and float(v_start) > 0:
            return round((float(v_end) - float(v_start)) / float(v_start) * 100, 3)
    except Exception as e:
        logger.warning(f"[WEEKLY_ANALYST] Could not compute weekly return: {e}")
    return 0.0


async def _extract_weekly_memory(daily_memories, weekly_return: float) -> dict:
    """Call GPT-4o to distill weekly memory. Returns degraded result on failure."""
    client = AsyncOpenAI(api_key=settings.openai_api_key)

    daily_summaries = "\n\n".join([
        f"[{m.trading_date}] Regime={m.regime_label} | "
        f"Stance={m.recommended_stance} | Approved={m.risk_approved}\n"
        f"Macro Narrative: {m.macro_narrative or 'N/A'}\n"
        f"Key Events: {', '.join(m.key_events or [])}"
        for m in daily_memories
    ])

    execution_count = sum(1 for m in daily_memories if m.execution_happened)

    user_msg = WEEKLY_ANALYST_USER_TEMPLATE.format(
        day_count=len(daily_memories),
        daily_summaries=daily_summaries[:3000],
        weekly_return=weekly_return,
        execution_count=execution_count,
    )

    try:
        resp = await client.chat.completions.create(
            model=settings.openai_model_heavy,  # gpt-4o
            messages=[
                {"role": "system", "content": WEEKLY_ANALYST_SYSTEM},
                {"role": "user", "content": user_msg},
            ],
            temperature=0.3,
            max_tokens=1000,
            response_format={"type": "json_object"},
        )
        return json.loads(resp.choices[0].message.content)
    except Exception as e:
        logger.error(f"[WEEKLY_ANALYST] LLM distillation failed: {e}")
        return {
            "dominant_regime": daily_memories[-1].regime_label if daily_memories else "unknown",
            "regime_shift": False,
            "regime_shift_detail": None,
            "macro_themes": [],
            "sector_rotation_signal": "LLM distillation failed",
            "momentum_effectiveness": "moderate",
            "signal_conflicts": [],
            "best_calls": [],
            "worst_calls": [],
            "next_week_watch": "Unable to generate outlook",
            "calendar_events": [],
        }


async def _upsert_memory_weekly(
    session, week_start, week_end, daily_memories, weekly_return, memory_data,
) -> None:
    """Write or update a memory_weekly record."""
    existing_result = await session.execute(
        select(MemoryWeekly).where(MemoryWeekly.week_start == week_start)
    )
    existing = existing_result.scalar_one_or_none()

    fields = dict(
        week_start=week_start,
        week_end=week_end,
        dominant_regime=str(memory_data.get("dominant_regime", "unknown"))[:50],
        regime_shift=bool(memory_data.get("regime_shift", False)),
        regime_shift_detail=memory_data.get("regime_shift_detail"),
        macro_themes=memory_data.get("macro_themes", []),
        sector_rotation_signal=memory_data.get("sector_rotation_signal"),
        momentum_effectiveness=str(memory_data.get("momentum_effectiveness", "moderate"))[:20],
        signal_conflicts=memory_data.get("signal_conflicts", []),
        best_calls=memory_data.get("best_calls", []),
        worst_calls=memory_data.get("worst_calls", []),
        weekly_return_pct=weekly_return,
        execution_count=sum(1 for m in daily_memories if m.execution_happened),
        next_week_watch=memory_data.get("next_week_watch"),
        calendar_events=memory_data.get("calendar_events", []),
        daily_count=len(daily_memories),
        source_daily_ids=[m.id for m in daily_memories],
    )

    if existing:
        for k, v in fields.items():
            setattr(existing, k, v)
    else:
        session.add(MemoryWeekly(**fields))

    await session.commit()


if __name__ == "__main__":
    asyncio.run(main())