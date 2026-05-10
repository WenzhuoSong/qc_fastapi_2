"""
cron/daily_analyst.py

Runs each trading day at 16:45 ET (after post_market_report).
Reads the day's latest AgentAnalysis record, uses GPT-4o-mini to distill
it into a structured daily memory, and upserts memory_daily.
"""

import asyncio
import json
import logging
from datetime import date, datetime, timezone

from openai import AsyncOpenAI
from sqlalchemy import select, func

from config import get_settings
from db.models import AgentAnalysis, ExecutionLog, MemoryDaily, PortfolioTimeseries
from db.session import AsyncSessionLocal

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("qc_fastapi_2.cron.daily_analyst")
settings = get_settings()

# ── LLM Distillation Prompts ──────────────────────────────────────────────────

DAILY_ANALYST_SYSTEM = """You are a memory distillation expert for a quantitative trading system.
Your job is to distill the day's pipeline analysis results into a concise, structured memory
that will be used by downstream agents for historical context.
Output must be valid JSON with no markdown code fences."""

DAILY_ANALYST_USER_TEMPLATE = """
Please distill today's market memory based on the following analysis data.

## Today's Pipeline Summary
{researcher_summary}

## Today's Risk Manager Output
- Risk Approved: {risk_approved}
- Execution Happened: {execution_happened}
- Top 5 Target Weights: {top5_weights}

## Today's Market Data
- Regime: {regime_label} (confidence: {regime_confidence})
- VIX: {vix}
- SPY daily return: {spy_return}%

Output the following JSON structure (all fields required; use null or empty arrays when no data):

{{
  "macro_narrative": "≤150 chars concise macro narrative summarizing today's key market drivers",
  "key_events": ["event1", "event2", "event3"],
  "regime_assessment": "one-sentence assessment of today's regime classification",
  "top3_overweight": [{{"ticker": "XLK", "weight": 0.18, "reason": "brief reason"}}],
  "top3_underweight": [{{"ticker": "TLT", "weight": 0.05, "reason": "brief reason"}}],
  "recommended_stance": "buy|overweight|maintain|underweight|sell",
  "hard_risks_detected": ["description1", "description2"],
  "learning_note": "≤80 chars, the single most important lesson or pattern worth remembering today"
}}
"""


# ── Main Entry ────────────────────────────────────────────────────────────────


async def main() -> None:
    """Main entry: distill today's memory and write to memory_daily."""
    today = date.today()
    logger.info(f"[DAILY_ANALYST] Starting processing for {today}")

    async with AsyncSessionLocal() as session:
        # 1. Read the day's latest AgentAnalysis
        analysis = await _get_latest_analysis_today(session, today)
        if not analysis:
            logger.warning(f"[DAILY_ANALYST] No pipeline records for {today}, skipping")
            return

        # 2. Read the day's latest portfolio data (for VIX and SPY return)
        portfolio = await _get_latest_portfolio(session)

        # 3. Check whether today's memory already exists (avoid duplicates)
        existing = await _get_existing_memory(session, today)

        # 4. Check whether execution actually happened today
        execution_happened = await _check_execution_happened(session, today)

        # 5. LLM distillation
        memory_data = await _extract_memory_with_llm(analysis, portfolio, execution_happened)

        # 6. Upsert memory_daily
        await _upsert_memory_daily(
            session, today, analysis, portfolio, memory_data, existing, execution_happened
        )

    logger.info(f"[DAILY_ANALYST] Memory write complete for {today}")


# ── Helpers ───────────────────────────────────────────────────────────────────


async def _get_latest_analysis_today(session, today: date):
    """Get the most recent AgentAnalysis record for today."""
    result = await session.execute(
        select(AgentAnalysis)
        .where(func.date(AgentAnalysis.analyzed_at) == today)
        .order_by(AgentAnalysis.analyzed_at.desc())
        .limit(1)
    )
    return result.scalar_one_or_none()


async def _get_latest_portfolio(session):
    """Get the most recent PortfolioTimeseries record."""
    result = await session.execute(
        select(PortfolioTimeseries)
        .order_by(PortfolioTimeseries.recorded_at.desc())
        .limit(1)
    )
    return result.scalar_one_or_none()


async def _get_existing_memory(session, today: date):
    """Check whether a memory_daily record already exists for today."""
    result = await session.execute(
        select(MemoryDaily).where(MemoryDaily.trading_date == today)
    )
    return result.scalar_one_or_none()


async def _check_execution_happened(session, today: date) -> bool:
    """Check whether at least one successful execution happened today."""
    result = await session.execute(
        select(func.count(ExecutionLog.id))
        .where(func.date(ExecutionLog.executed_at) == today)
        .where(ExecutionLog.status == "success")
    )
    count = result.scalar_one_or_none() or 0
    return count > 0


async def _extract_memory_with_llm(analysis: AgentAnalysis, portfolio, execution_happened: bool) -> dict:
    """Call GPT-4o-mini to distill memory. Returns degraded result on failure."""
    client = AsyncOpenAI(api_key=settings.openai_api_key)

    # Extract key information from analysis JSONB fields
    # researcher_output = synthesizer output (adjusted_weights, market_judgment, recommended_stance)
    synthesizer_out = analysis.researcher_output or {}
    # allocator_output = quant_baseline (base_weights, regime_result, scoring_breakdown)
    quant_baseline = analysis.allocator_output or {}
    risk_out = analysis.risk_output or {}

    # Build top-5 weights summary from risk target_weights
    target_weights = risk_out.get("target_weights", {})
    top5 = sorted(
        [(k, v) for k, v in target_weights.items() if k != "CASH"],
        key=lambda x: x[1], reverse=True,
    )[:5]
    top5_str = ", ".join([f"{t}:{w:.1%}" for t, w in top5]) if top5 else "N/A"

    # Regime info — from allocator_output (quant_baseline.regime_result)
    regime_info = synthesizer_out.get("market_judgment") or {}
    quant_regime = quant_baseline.get("regime_result") or {}
    regime_label = regime_info.get("regime") or quant_regime.get("regime") or "unknown"
    regime_confidence = regime_info.get("confidence") or quant_regime.get("confidence") or "unknown"

    # SPY daily return — approximated from portfolio data if available
    spy_return_str = "N/A"
    try:
        if portfolio and portfolio.daily_pnl_pct is not None:
            spy_return_str = f"{float(portfolio.daily_pnl_pct) * 100:.2f}"
    except (TypeError, ValueError):
        pass

    # VIX
    vix_str = "N/A"
    try:
        if portfolio and portfolio.vix is not None:
            vix_str = f"{float(portfolio.vix):.1f}"
    except (TypeError, ValueError):
        pass

    user_msg = DAILY_ANALYST_USER_TEMPLATE.format(
        researcher_summary=json.dumps(synthesizer_out, ensure_ascii=False)[:2000],
        risk_approved=analysis.risk_approved,
        execution_happened=execution_happened,
        top5_weights=top5_str,
        regime_label=regime_label,
        regime_confidence=regime_confidence,
        vix=vix_str,
        spy_return=spy_return_str,
    )

    try:
        resp = await client.chat.completions.create(
            model=settings.openai_model,  # gpt-4o-mini
            messages=[
                {"role": "system", "content": DAILY_ANALYST_SYSTEM},
                {"role": "user", "content": user_msg},
            ],
            temperature=0.3,
            max_tokens=800,
            response_format={"type": "json_object"},
        )
        raw = resp.choices[0].message.content
        return json.loads(raw)
    except Exception as e:
        logger.error(f"[DAILY_ANALYST] LLM distillation failed, using degraded result: {e}")
        return {
            "macro_narrative": "LLM distillation failed — no macro narrative available",
            "key_events": [],
            "regime_assessment": regime_label,
            "top3_overweight": [{"ticker": t, "weight": w, "reason": ""} for t, w in top5[:3]],
            "top3_underweight": [],
            "recommended_stance": "maintain",
            "hard_risks_detected": [],
            "learning_note": "",
        }


async def _upsert_memory_daily(
    session, today: date, analysis, portfolio, memory_data: dict, existing, execution_happened: bool,
) -> None:
    """Write or update a memory_daily record."""
    risk_out = analysis.risk_output or {}

    vix_val = None
    try:
        if portfolio and portfolio.vix is not None:
            vix_val = float(portfolio.vix)
    except (TypeError, ValueError):
        pass

    spy_return_val = None
    try:
        if portfolio and portfolio.daily_pnl_pct is not None:
            spy_return_val = float(portfolio.daily_pnl_pct)
    except (TypeError, ValueError):
        pass

    fields = dict(
        trading_date=today,
        regime_label=str(memory_data.get("regime_assessment", "unknown"))[:50],
        regime_confidence=None,  # stored as string in JSON, null until we parse it
        vix_close=vix_val,
        spy_return_pct=spy_return_val,
        recommended_stance=str(memory_data.get("recommended_stance", "maintain"))[:50],
        risk_approved=bool(analysis.risk_approved),
        execution_happened=execution_happened,
        top3_overweight=memory_data.get("top3_overweight", []),
        top3_underweight=memory_data.get("top3_underweight", []),
        macro_narrative=str(memory_data.get("macro_narrative", ""))[:200],
        key_events=memory_data.get("key_events", []),
        hard_risks_detected=memory_data.get("hard_risks_detected", []),
        agent_analysis_id=analysis.id,
        raw_researcher_output=analysis.researcher_output,
    )

    if existing:
        for k, v in fields.items():
            setattr(existing, k, v)
    else:
        session.add(MemoryDaily(**fields))

    await session.commit()


if __name__ == "__main__":
    asyncio.run(main())