# agents/quarterly_analyst.py
"""
QUARTERLY_ANALYST — LLM distillation agent for quarterly strategy review.

Runs on the first trading day of each quarter. Reviews the previous quarter's
performance (MemoryMonthly + MemoryWeekly + MemoryDaily + performance data),
evaluates MomentumLiteV1 effectiveness, and generates strategy_revision_v1.

Output:
  strategy_revision_v1 JSON stored in system_config with status: "pending_approval"
  Telegram alert sent to human for /approve_strategy or /skip_strategy
"""
from __future__ import annotations

import json
import logging
from datetime import date, datetime

from openai import AsyncOpenAI
from sqlalchemy import select

from config import get_settings
from db.session import AsyncSessionLocal
from db.models import MemoryMonthly, MemoryWeekly, MemoryDaily, AgentAnalysis
from db.queries import upsert_system_config
from services.openai_chat_compat import build_chat_completion_kwargs
from tools.notify_tools import tool_send_telegram

logger = logging.getLogger("qc_fastapi_2.quarterly_analyst")
settings = get_settings()

# ─────────────────────────────── Prompts ─────────────────────────────────────

QUARTERLY_ANALYST_SYSTEM = """You are a quarterly strategy reviewer for a quantitative ETF trading system.
Review the past quarter's performance data and generate a strategy revision recommendation.
Output must be valid JSON with no markdown code fences.

Your recommendation should be conservative — only recommend changes if there is
clear evidence that the current strategy is underperforming. A stable strategy
with moderate returns is better than frequent changes.
"""

QUARTERLY_ANALYST_USER_TEMPLATE = """
Based on the following quarterly data, evaluate the MomentumLiteV1 strategy effectiveness
and recommend any parameter adjustments.

## Quarterly Performance Summary
- Quarterly Return: {quarterly_return}%
- Sharpe Ratio: {sharpe_ratio}
- Max Drawdown: {max_drawdown}%
- Execution Count: {execution_count}
- Momentum Effectiveness: {momentum_effectiveness}

## Monthly Memory Summaries (last 3 months)
{monthly_summaries}

## Weekly Memory Summaries (last quarter)
{weekly_summaries}

## Regime Distribution This Quarter
{regime_distribution}

## Decay Signal (if any)
{decay_signal}

## Decision Quality Trend
{decision_quality_trend}

Output the following JSON structure (all fields required; use null or empty arrays when no data):

{{
  "strategy_revision_v1": {{
    "current_strategy": "momentum_lite_v1",
    "version": "2.0",
    "changes_recommended": true or false,
    "change_summary": "≤100 chars description of main change",
    "parameter_changes": {{
      "w_mom_20d": {{"old": 0.30, "new": 0.xx, "reason": "..."}},
      "w_mom_60d": {{"old": 0.35, "new": 0.xx, "reason": "..."}},
      "w_mom_252d": {{"old": 0.20, "new": 0.xx, "reason": "..."}},
      "w_rsi": {{"old": 0.10, "new": 0.xx, "reason": "..."}},
      "w_atr": {{"old": 0.05, "new": 0.xx, "reason": "..."}}
    }},
    "regime_overrides": {{
      "high_vol": {{"adjustment": "increase_cash_by_x_percent", "reason": "..."}},
      "bear_weak": {{"adjustment": "...", "reason": "..."}}
    }},
    "new_strategy_candidates": ["strategy_name", ...],
    "confidence": "high" or "medium" or "low",
    "rejection_risks": ["risk1", ...],
    "rationale": "≥50 chars explanation of why changes are needed or not"
  }}
}}
"""


# ─────────────────────────────── Main Entry ──────────────────────────────────


async def run_quarterly_analyst() -> dict:
    """
    Run the quarterly strategy review. Returns the strategy_revision_v1 dict.
    """
    logger.info("[QUARTERLY_ANALYST] Starting quarterly review")
    today = date.today()

    # Determine the previous quarter
    quarter_month_start = (today.month - 1) // 3 * 3
    if quarter_month_start == 1:
        prev_year = today.year - 1
        prev_quarter_month = 10  # Q4 of previous year
    else:
        prev_year = today.year
        prev_quarter_month = quarter_month_start - 3

    from datetime import date as _date
    quarter_start = _date(prev_year, prev_quarter_month, 1)

    async with AsyncSessionLocal() as db:
        # Get MemoryMonthly for the quarter
        monthly_result = await db.execute(
            select(MemoryMonthly)
            .where(MemoryMonthly.month_start >= quarter_start)
            .order_by(MemoryMonthly.month_start.asc())
        )
        monthly_records = monthly_result.scalars().all()

        # Get MemoryWeekly for the quarter
        weekly_result = await db.execute(
            select(MemoryWeekly)
            .where(MemoryWeekly.week_start >= quarter_start)
            .order_by(MemoryWeekly.week_start.asc())
        )
        weekly_records = weekly_result.scalars().all()

        # Get decay signal if any
        decay_cfg = await db.execute(
            select(SystemConfig).where(SystemConfig.key == "decay_signal")
        )
        decay_record = decay_cfg.scalar_one_or_none()
        decay_signal = json.dumps(decay_record.value, ensure_ascii=False) if decay_record else "{}"

    # Compute quarterly stats
    quarterly_return = _avg([m.monthly_return_pct for m in monthly_records])
    sharpe_ratio = _avg([m.monthly_sharpe for m in monthly_records])
    max_drawdown = max((m.max_drawdown_pct or 0) for m in monthly_records) if monthly_records else 0
    execution_count = sum((m.execution_count or 0) for m in monthly_records)
    momentum_eff = _mode([m.momentum_effectiveness for m in weekly_records]) if weekly_records else "moderate"

    # Build regime distribution
    regime_counts: dict[str, int] = {}
    for w in weekly_records:
        r = w.dominant_regime or "unknown"
        regime_counts[r] = regime_counts.get(r, 0) + 1

    # Build monthly summaries
    monthly_summaries = "\n\n".join([
        f"[{m.month_start}] Regime={m.dominant_regime} | "
        f"Momentum={m.momentum_effectiveness} | Return={m.monthly_return_pct}% | "
        f"Sharpe={m.monthly_sharpe} | "
        f"Best: {json.dumps(m.best_calls or [])} | "
        f"Worst: {json.dumps(m.worst_calls or [])}"
        for m in monthly_records
    ]) if monthly_records else "No monthly records"

    # Build weekly summaries
    weekly_summaries = "\n".join([
        f"[{w.week_start}] {w.dominant_regime} | {w.momentum_effectiveness} | "
        f"Conflicts: {json.dumps(w.signal_conflicts or [])}"
        for w in weekly_records[-12:]]   # last 12 weeks max
    ) if weekly_records else "No weekly records"

    # Decision quality trend
    async with AsyncSessionLocal() as db:
        daily_result = await db.execute(
            select(MemoryDaily)
            .where(MemoryDaily.trading_date >= quarter_start)
            .where(MemoryDaily.decision_quality_score.isnot(None))
            .order_by(MemoryDaily.trading_date.asc())
        )
        daily_records = daily_result.scalars().all()

    if daily_records:
        dqs_values = [d.decision_quality_score for d in daily_records]
        early_avg = _avg(dqs_values[:len(dqs_values)//2]) if dqs_values else 0
        late_avg = _avg(dqs_values[len(dqs_values)//2:]) if dqs_values else 0
        dqs_trend = f"early={early_avg:.2%}, late={late_avg:.2%}, change={late_avg-early_avg:+.1%}"
    else:
        dqs_trend = "No decision quality scores available"

    # Build LLM prompt
    user_msg = QUARTERLY_ANALYST_USER_TEMPLATE.format(
        quarterly_return=f"{quarterly_return:.2f}" if quarterly_return is not None else "N/A",
        sharpe_ratio=f"{sharpe_ratio:.2f}" if sharpe_ratio is not None else "N/A",
        max_drawdown=f"{max_drawdown:.2f}" if max_drawdown else "N/A",
        execution_count=execution_count,
        momentum_effectiveness=momentum_eff,
        monthly_summaries=monthly_summaries[:3000],
        weekly_summaries=weekly_summaries[:2000],
        regime_distribution=json.dumps(regime_counts, ensure_ascii=False),
        decay_signal=decay_signal[:500],
        decision_quality_trend=dqs_trend,
    )

    # Call LLM
    client = AsyncOpenAI(api_key=settings.openai_api_key)
    try:
        resp = await client.chat.completions.create(**build_chat_completion_kwargs(
            model=settings.openai_model_heavy,
            messages=[
                {"role": "system", "content": QUARTERLY_ANALYST_SYSTEM},
                {"role": "user", "content": user_msg},
            ],
            temperature=0.3,
            max_tokens=2000,
            response_format={"type": "json_object"},
        ))
        raw = resp.choices[0].message.content
        parsed = json.loads(raw)
        revision = parsed.get("strategy_revision_v1", {})
    except Exception as e:
        logger.error(f"[QUARTERLY_ANALYST] LLM call failed: {e}")
        revision = _degraded_revision(
            quarterly_return, momentum_eff, regime_counts, decay_signal
        )

    # Build the full output record
    output = {
        "quarter_start": quarter_start.isoformat(),
        "created_at": datetime.utcnow().isoformat(),
        "quarterly_stats": {
            "quarterly_return_pct": quarterly_return,
            "sharpe_ratio": sharpe_ratio,
            "max_drawdown_pct": max_drawdown,
            "execution_count": execution_count,
            "momentum_effectiveness": momentum_eff,
            "regime_distribution": regime_counts,
        },
        "strategy_revision_v1": revision,
        "status": "pending_approval",
        "source_monthly_ids": [m.id for m in monthly_records],
        "source_weekly_ids": [w.id for w in weekly_records],
    }

    # Store in system_config
    async with AsyncSessionLocal() as db:
        await upsert_system_config(db, "strategy_revision_v1", output, "quarterly_analyst")

    # Send Telegram alert
    change_summary = revision.get("change_summary", "no changes")
    changes_rec = revision.get("changes_recommended", False)
    recommendation = (
        f"✅ QUARTERLY_ANALYST: Strategy changes recommended.\n"
        f"Summary: {change_summary}\n"
        f"Changes recommended: {changes_rec}\n"
        f"Confidence: {revision.get('confidence', 'unknown')}\n"
        f"Reply /approve_strategy to apply or /skip_strategy to reject."
    )
    if not changes_rec:
        recommendation = (
            f"ℹ️ QUARTERLY_ANALYST: Strategy review complete.\n"
            f"No changes recommended — current strategy performing adequately.\n"
            f"Summary: {change_summary}\n"
            f"Reply /approve_strategy to override and apply changes, or ignore to keep current."
        )
    await tool_send_telegram({"text": recommendation})

    logger.info(
        f"[QUARTERLY_ANALYST] Review complete: "
        f"changes={changes_rec}, confidence={revision.get('confidence')}"
    )
    return output


# ─────────────────────────────── Helpers ─────────────────────────────────────


def _avg(values: list) -> float | None:
    vals = [v for v in values if v is not None]
    return sum(vals) / len(vals) if vals else None


def _mode(values: list) -> str:
    """Most common non-None value."""
    from collections import Counter
    v = [str(vv) for vv in values if vv is not None]
    if not v:
        return "unknown"
    return Counter(v).most_common(1)[0][0]


def _degraded_revision(
    quarterly_return: float | None,
    momentum_eff: str,
    regime_counts: dict,
    decay_signal: str,
) -> dict:
    """Fallback when LLM call fails."""
    return {
        "current_strategy": "momentum_lite_v1",
        "version": "2.0",
        "changes_recommended": False,
        "change_summary": "LLM distillation failed — no changes applied",
        "parameter_changes": {},
        "regime_overrides": {},
        "new_strategy_candidates": [],
        "confidence": "low",
        "rejection_risks": ["LLM call failed — degraded output"],
        "rationale": (
            f"Quarterly return={quarterly_return:.2%}, momentum_effectiveness={momentum_eff}. "
            f"Cannot generate full recommendation due to LLM failure."
        ),
    }
