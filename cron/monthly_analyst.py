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

logger = logging.getLogger("qc_fastapi_2.cron.monthly")
settings = get_settings()

MONTHLY_ANALYST_SYSTEM = """你是一个量化交易系统的月度记忆提炼专家。
基于本月每周记忆，识别更高层次的月度市场规律、板块轮动和策略教训。
输出必须是合法 JSON，不包含任何 markdown 代码块标记。"""

MONTHLY_ANALYST_USER_TEMPLATE = """
请基于以下本月每周记忆（{week_count} 周），提炼本月市场记忆。

## 本月每周记忆摘要
{weekly_summaries}

## 本月组合表现
- 月回报: {monthly_return}%
- 执行次数: {execution_count}

请输出以下 JSON 结构（所有字段必填，无数据填 null）：
{{
  "dominant_regime": "本月主导 regime (bull_trend|bull_weak|neutral|bear_weak|bear_trend|high_vol)",
  "regime_stability": "stable|shifting|volatile",
  "macro_themes": ["主题1", "主题2", "主题3"],
  "sector_rotation_summary": "板块轮动摘要，≤100字",
  "momentum_effectiveness": "strong|moderate|weak|failed",
  "key_lessons": ["教训1", "教训2"],
  "signal_conflicts": ["ticker1 (bull/bear 分歧大)", "ticker2"],
  "best_calls": [{{"ticker": "XLK", "detail": "判断准确的描述"}}],
  "worst_calls": [{{"ticker": "TLT", "detail": "判断失误的描述"}}],
  "next_month_watch": "下月需重点关注的风险或机会，≤100字",
  "calendar_events": ["事件1", "事件2"]
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

    logger.info(f"[MONTHLY_ANALYST] 处理月份: {year}-{month:02d} ({month_start} ~ {month_end})")

    async with AsyncSessionLocal() as session:
        # 1. 读取本月所有 memory_weekly
        weekly_memories = await _get_monthly_weekly(session, month_start, month_end)
        if not weekly_memories:
            logger.warning("[MONTHLY_ANALYST] 本月无 weekly 记忆，跳过")
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

    logger.info(f"[MONTHLY_ANALYST] 月度记忆写入完成: {year}-{month:02d}")


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
        f"宏观主题: {', '.join(w.macro_themes or [])}\n"
        f"动量效果: {w.momentum_effectiveness}\n"
        f"周回报: {w.weekly_return_pct or 'N/A'}%"
        for w in weekly_memories
    ])

    user_msg = MONTHLY_ANALYST_USER_TEMPLATE.format(
        week_count=len(weekly_memories),
        weekly_summaries=weekly_summaries[:4000],
        monthly_return=monthly_return,
        execution_count=execution_count,
    )

    try:
        resp = await client.chat.completions.create(
            model=settings.openai_model_heavy,
            messages=[
                {"role": "system", "content": MONTHLY_ANALYST_SYSTEM},
                {"role": "user", "content": user_msg},
            ],
            temperature=0.3,
            max_tokens=1200,
            response_format={"type": "json_object"},
        )
        return json.loads(resp.choices[0].message.content)
    except Exception as e:
        logger.error(f"[MONTHLY_ANALYST] LLM 提炼失败: {e}")
        last_week = weekly_memories[-1] if weekly_memories else None
        return {
            "dominant_regime": last_week.dominant_regime if last_week else "unknown",
            "regime_stability": "unknown",
            "macro_themes": [],
            "sector_rotation_summary": "LLM 提炼失败",
            "momentum_effectiveness": "moderate",
            "key_lessons": [],
            "signal_conflicts": [],
            "best_calls": [],
            "worst_calls": [],
            "next_month_watch": "无法生成前瞻",
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