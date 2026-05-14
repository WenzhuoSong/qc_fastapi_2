"""
cron/daily_analyst.py

Runs each trading day at 16:45 ET (after post_market_report).
Reads the day's latest AgentAnalysis record, uses GPT-4o-mini to distill
it into a structured daily memory, and upserts memory_daily.

Phase 3 additions:
- Calls write_decision_context() to write structured decision record
- Calls calibrate_decisions() for the previous trading day (next-day backfill)
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
from services.cron_audit import audit_cron_run

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
    async with audit_cron_run("daily_analyst") as audit:
        await _main_impl(audit)


async def _main_impl(audit=None) -> None:
    """Main entry: distill today's memory and write to memory_daily."""
    today = date.today()
    logger.info(f"[DAILY_ANALYST] Starting processing for {today}")

    # Phase 3: Backfill DQS for yesterday (must run before calibrate_decisions)
    yesterday = today - __import__("datetime").timedelta(days=1)
    try:
        from services.decision_memory import (
            read_decision_context,
            compute_decision_quality_score,
            backfill_decision_quality,
        )
        yesterday_ctx = await read_decision_context(yesterday)
        if yesterday_ctx:
            portfolio_ret = await _get_yesterday_portfolio_return(yesterday)
            spy_ret = await _get_yesterday_spy_return(yesterday)
            dqs = compute_decision_quality_score(
                recommended_stance=yesterday_ctx.get("recommended_stance"),
                portfolio_return_pct=portfolio_ret,
                spy_return_pct=spy_ret,
                researcher_confidence=yesterday_ctx.get("researcher_confidence"),
                execution_happened=bool(yesterday_ctx.get("execution_happened", False)),
            )
            if dqs is not None:
                await backfill_decision_quality(
                    trading_date=yesterday,
                    decision_quality_score=dqs,
                    portfolio_return_pct=portfolio_ret,
                )
                await _write_dqs_to_memory_daily_column(yesterday, dqs, portfolio_ret)
                logger.info(f"[DAILY_ANALYST] DQS backfill: {yesterday} DQS={dqs:.3f}")
        else:
            logger.info(f"[DAILY_ANALYST] No decision context for {yesterday}, skipping DQS backfill")
    except Exception as e:
        logger.warning(f"[DAILY_ANALYST] DQS backfill failed: {e}")

    # Phase 3: Run DECISION_CALIBRATOR for the previous trading day (next-day backfill)
    try:
        from services.decision_calibrator import calibrate_decisions
        calib_result = await calibrate_decisions(trading_date=yesterday)
        logger.info(
            f"[DAILY_ANALYST] DECISION_CALIBRATOR: "
            f"bias={calib_result.bias_multipliers}, samples={calib_result.sample_size}"
        )
    except Exception as e:
        logger.warning(f"[DAILY_ANALYST] DECISION_CALIBRATOR failed: {e}")

    async with AsyncSessionLocal() as session:
        # 1. Read the day's latest AgentAnalysis
        analysis = await _get_latest_analysis_today(session, today)
        if not analysis:
            logger.warning(f"[DAILY_ANALYST] No pipeline records for {today}, skipping")
            if audit:
                audit.mark_skipped("no_pipeline_records")
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
        if audit:
            audit.add_rows(1)
            audit.set_summary(
                trading_date=today.isoformat(),
                analysis_id=analysis.id,
                execution_happened=execution_happened,
                memory_existing=bool(existing),
            )

        # Phase 3: Write structured decision context
        try:
            from services.decision_memory import write_decision_context as _write_decision
            await _write_decision(
                analysis_id=analysis.id,
                trading_date=today,
                regime=memory_data.get("regime_assessment", "unknown"),
                weights_used=_get_weights_used(analysis),
                rationale=memory_data.get("macro_narrative", ""),
                outcome={"portfolio_return_pct": None, "decision_quality_score": None},
                execution_happened=execution_happened,
                researcher_confidence=_get_researcher_confidence(analysis),
                recommended_stance=memory_data.get("recommended_stance"),
            )
        except Exception as e:
            logger.warning(f"[DAILY_ANALYST] write_decision_context failed: {e}")

    logger.info(f"[DAILY_ANALYST] Memory write complete for {today}")


# ── Phase 3 Helpers ────────────────────────────────────────────────────────────


def _get_weights_used(analysis) -> dict:
    """Extract target_weights from risk_output for decision_memory."""
    risk_out = analysis.risk_output or {}
    return risk_out.get("target_weights", {}) or {}


def _get_researcher_confidence(analysis) -> str | None:
    """Extract researcher confidence level from researcher's output."""
    researcher_out = analysis.researcher_output or {}
    market_judgment = researcher_out.get("market_judgment") or {}
    return market_judgment.get("confidence")


# ── DQS Backfill Helpers ────────────────────────────────────────────────────────


async def _get_yesterday_portfolio_return(target_date: date) -> float | None:
    """Get portfolio daily return for target_date from PortfolioTimeseries."""
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(PortfolioTimeseries.daily_pnl_pct)
            .where(func.date(PortfolioTimeseries.recorded_at) == target_date)
            .order_by(PortfolioTimeseries.recorded_at.desc())
            .limit(1)
        )
        row = result.scalar_one_or_none()
    return float(row) if row is not None else None


async def _get_yesterday_spy_return(target_date: date) -> float | None:
    """Get SPY return for target_date from MemoryDaily."""
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(MemoryDaily.spy_return_pct)
            .where(MemoryDaily.trading_date == target_date)
        )
        row = result.scalar_one_or_none()
    return float(row) if row is not None else None


async def _write_dqs_to_memory_daily_column(
    trading_date: date,
    dqs: float,
    portfolio_return_pct: float | None,
) -> None:
    """Write DQS to the flat MemoryDaily column (calibrator reads this, not JSONB)."""
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(MemoryDaily).where(MemoryDaily.trading_date == trading_date)
        )
        row = result.scalar_one_or_none()
        if row:
            row.decision_quality_score = dqs
            if portfolio_return_pct is not None:
                row.portfolio_return_pct = portfolio_return_pct
            await session.commit()


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
