"""
cron/monthly_analyst.py

Railway cron entry: 每月末最后一个交易日 17:00 ET 运行。
汇总本月所有 memory_weekly → LLM 提炼 → upsert memory_monthly。

P1-4: MONTHLY_ANALYST
使用方式：python -m cron.monthly_analyst
"""
import asyncio
import json
import logging
from datetime import date, timedelta
from calendar import monthrange

from db.session import AsyncSessionLocal
from db.models import MemoryWeekly, MemoryMonthly, PortfolioTimeseries
from db.queries import upsert_system_config
from config import get_settings
from openai import AsyncOpenAI
from services.openai_chat_compat import build_chat_completion_kwargs

logger = logging.getLogger("qc_fastapi_2.cron.monthly")
settings = get_settings()

MONTHLY_ANALYST_SYSTEM = """You are the monthly memory distillation analyst for a quantitative trading system.
Use this month's weekly memories to identify higher-level market patterns, sector rotation, and strategy lessons.
Output valid JSON only. Do not include markdown code fences."""

MONTHLY_ANALYST_USER_TEMPLATE = """
Distill this month's market memory from the following {week_count} weekly records.

## Monthly weekly-memory summaries
{weekly_summaries}

## Monthly portfolio performance
- Monthly return: {monthly_return}%
- Execution count: {execution_count}

Return the following JSON structure. All fields are required; use null or empty arrays when unavailable.
{{
  "dominant_regime": "dominant regime this month (bull_trend|bull_weak|neutral|bear_weak|bear_trend|high_vol)",
  "regime_stability": "stable|shifting|volatile",
  "macro_themes": ["theme1", "theme2", "theme3"],
  "sector_rotation_summary": "sector rotation summary, <=25 words",
  "momentum_effectiveness": "strong|moderate|weak|failed",
  "key_lessons": ["lesson1", "lesson2"],
  "signal_conflicts": ["ticker1 (large bull/bear disagreement)", "ticker2"],
  "best_calls": [{{"ticker": "XLK", "detail": "accurate call description"}}],
  "worst_calls": [{{"ticker": "TLT", "detail": "inaccurate call description"}}],
  "next_month_watch": "key risks or opportunities to watch next month, <=25 words",
  "calendar_events": ["event1", "event2"]
}}
"""


async def run_monthly_analyst() -> None:
    """主入口：每月末运行，提炼月度记忆并写入 memory_monthly。"""
    today = date.today()
    year = today.year
    month = today.month

    # 计算本月范围
    _, last_day = monthrange(year, month)
    month_start = date(year, month, 1)
    month_end = date(year, month, last_day)

    logger.info(f"[MONTHLY_ANALYST] Processing month: {year}-{month:02d} ({month_start} ~ {month_end})")

    async with AsyncSessionLocal() as session:
        # 1. 读取本月所有 memory_weekly
        weekly_memories = await _get_monthly_weekly(session, month_start, month_end)
        if not weekly_memories:
            logger.warning("[MONTHLY_ANALYST] No weekly memories found for this month, skipping")
            return

        # 2. 计算本月组合表现
        monthly_return = await _compute_monthly_return(session, month_start, month_end)
        execution_count = sum(1 for w in weekly_memories if w.execution_count or 0)

        # 3. LLM 提炼
        memory_data = await _extract_monthly_memory(
            weekly_memories, monthly_return, execution_count
        )

        # 4. upsert memory_monthly
        await _upsert_memory_monthly(
            session, month_start, month_end, weekly_memories,
            monthly_return, execution_count, memory_data
        )

    logger.info(f"[MONTHLY_ANALYST] Monthly memory write complete: {year}-{month:02d}")


async def _get_monthly_weekly(session, month_start: date, month_end: date):
    from sqlalchemy import select
    result = await session.execute(
        select(MemoryWeekly)
        .where(MemoryWeekly.week_start >= month_start)
        .where(MemoryWeekly.week_start <= month_end)
        .order_by(MemoryWeekly.week_start.asc())
    )
    return list(result.scalars().all())


async def _compute_monthly_return(session, month_start: date, month_end: date) -> float:
    """从 PortfolioTimeseries 估算月回报（首末净值差）。"""
    from sqlalchemy import select, func
    try:
        result_start = await session.execute(
            select(PortfolioTimeseries.total_value)
            .where(func.date(PortfolioTimeseries.recorded_at) >= month_start)
            .order_by(PortfolioTimeseries.recorded_at.asc())
            .limit(1)
        )
        result_end = await session.execute(
            select(PortfolioTimeseries.total_value)
            .where(func.date(PortfolioTimeseries.recorded_at) <= month_end)
            .order_by(PortfolioTimeseries.recorded_at.desc())
            .limit(1)
        )
        v_start = result_start.scalar_one_or_none()
        v_end = result_end.scalar_one_or_none()
        if v_start and v_end and float(v_start) > 0:
            return round((float(v_end) - float(v_start)) / float(v_start) * 100, 3)
    except Exception:
        pass
    return 0.0


async def _extract_monthly_memory(
    weekly_memories, monthly_return: float, execution_count: int
) -> dict:
    client = AsyncOpenAI(api_key=settings.openai_api_key)

    weekly_summaries = "\n\n".join([
        f"[Week {w.week_start}]: Regime={w.dominant_regime} | "
        f"Stance shift={w.regime_shift}\n"
        f"Macro themes: {', '.join(w.macro_themes or [])}\n"
        f"Momentum effectiveness: {w.momentum_effectiveness}\n"
        f"Weekly return: {w.weekly_return_pct or 'N/A'}%"
        for w in weekly_memories
    ])

    user_msg = MONTHLY_ANALYST_USER_TEMPLATE.format(
        week_count=len(weekly_memories),
        weekly_summaries=weekly_summaries[:4000],
        monthly_return=monthly_return,
        execution_count=execution_count,
    )

    try:
        resp = await client.chat.completions.create(**build_chat_completion_kwargs(
            model=settings.openai_model_heavy,
            messages=[
                {"role": "system", "content": MONTHLY_ANALYST_SYSTEM},
                {"role": "user", "content": user_msg},
            ],
            temperature=0.3,
            max_tokens=1200,
            response_format={"type": "json_object"},
        ))
        return json.loads(resp.choices[0].message.content)
    except Exception as e:
        logger.error(f"[MONTHLY_ANALYST] LLM distillation failed: {e}")
        last_week = weekly_memories[-1] if weekly_memories else None
        return {
            "dominant_regime": last_week.dominant_regime if last_week else "unknown",
            "regime_stability": "unknown",
            "macro_themes": [],
            "sector_rotation_summary": "LLM distillation failed",
            "momentum_effectiveness": "moderate",
            "key_lessons": [],
            "signal_conflicts": [],
            "best_calls": [],
            "worst_calls": [],
            "next_month_watch": "Unable to generate forward watchlist",
            "calendar_events": [],
        }


async def _upsert_memory_monthly(
    session, month_start, month_end, weekly_memories,
    monthly_return, execution_count, memory_data
):
    from sqlalchemy import select
    existing = (await session.execute(
        select(MemoryMonthly).where(MemoryMonthly.month_start == month_start)
    )).scalar_one_or_none()

    fields = dict(
        month_start=month_start,
        month_end=month_end,
        dominant_regime=memory_data.get("dominant_regime", "unknown"),
        regime_stability=memory_data.get("regime_stability", "unknown"),
        macro_themes=memory_data.get("macro_themes", []),
        sector_rotation_summary=memory_data.get("sector_rotation_summary"),
        momentum_effectiveness=memory_data.get("momentum_effectiveness", "moderate"),
        key_lessons=memory_data.get("key_lessons", []),
        signal_conflicts=memory_data.get("signal_conflicts", []),
        best_calls=memory_data.get("best_calls", []),
        worst_calls=memory_data.get("worst_calls", []),
        monthly_return_pct=monthly_return,
        execution_count=execution_count,
        next_month_watch=memory_data.get("next_month_watch"),
        calendar_events=memory_data.get("calendar_events", []),
        weekly_count=len(weekly_memories),
        source_weekly_ids=[w.id for w in weekly_memories],
    )

    if existing:
        for k, v in fields.items():
            setattr(existing, k, v)
    else:
        session.add(MemoryMonthly(**fields))

    await session.commit()


if __name__ == "__main__":
    asyncio.run(run_monthly_analyst())
