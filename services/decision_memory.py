# services/decision_memory.py
"""
decision_memory — structured decision context writer for MemoryDaily.

Called from cron/daily_analyst.py after LLM distillation.
Writes a structured decision context record to MemoryDaily alongside the
LLM-distilled fields.

This structured record enables:
  - Similar case retrieval (regime + market condition matching)
  - DECISION_CALIBRATOR confidence scoring
  - Post-hoc analysis of decision accuracy

The existing LLM distillation (macro_narrative, key_events, etc.) is still
written by daily_analyst.py — this supplements it with structured fields.
"""
from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import MemoryDaily, AgentAnalysis
from db.session import AsyncSessionLocal

logger = logging.getLogger("qc_fastapi_2.decision_memory")


# ─────────────────────────────── Main Entry ───────────────────────────────


async def write_decision_context(
    analysis_id: int,
    trading_date: date,
    regime: str,
    weights_used: dict[str, float],
    rationale: str,
    outcome: dict,
    execution_happened: bool,
    researcher_confidence: Optional[str] = None,
    recommended_stance: Optional[str] = None,
) -> bool:
    """
    Write structured decision context to memory_daily.

    Called from cron/daily_analyst.py after LLM distillation.

    Args:
        analysis_id: AgentAnalysis ID for this day's pipeline run
        trading_date: The trading date
        regime: Current regime label (e.g., "trending_bull", "high_vol")
        weights_used: Final target_weights from risk_manager
        rationale: The recommended_stance or brief rationale
        outcome: Dict with portfolio_return_pct, decision_quality_score (may be null — backfilled next day)
        execution_happened: Whether a trade was executed today
        researcher_confidence: high/medium/low from researcher output
        recommended_stance: buy/overweight/maintain/underweight/sell

    Returns:
        True if written successfully, False otherwise.
    """
    try:
        async with AsyncSessionLocal() as db:
            # Check if memory_daily already exists for this date
            result = await db.execute(
                select(MemoryDaily).where(MemoryDaily.trading_date == trading_date)
            )
            existing = result.scalar_one_or_none()

            # Get the AgentAnalysis record
            analysis_result = await db.execute(
                select(AgentAnalysis).where(AgentAnalysis.id == analysis_id)
            )
            analysis = analysis_result.scalar_one_or_none()

            # Get synthesizer output for structured fields
            synthesizer_out = {}
            if analysis and analysis.researcher_output:
                synthesizer_out = analysis.researcher_output or {}

            # Build top holdings for structured record
            top_holdings = []
            if weights_used:
                sorted_weights = sorted(
                    [(t, w) for t, w in weights_used.items() if t != "CASH" and w > 0],
                    key=lambda x: x[1],
                    reverse=True,
                )
                top_holdings = [
                    {"ticker": t, "weight": round(w, 4)}
                    for t, w in sorted_weights[:5]
                ]

            # Determine decision quality (null = backfill tomorrow)
            decision_quality = outcome.get("decision_quality_score")

            # Extract market judgment from synthesizer
            market_judgment = synthesizer_out.get("market_judgment") or {}
            uncertainty_flag = market_judgment.get("uncertainty_flag", False)
            key_events_list = synthesizer_out.get("key_events") or []
            playground_assessment = synthesizer_out.get("playground_strategy_assessment") or {}
            playground_selected_strategies: list[str] = []
            if playground_assessment:
                try:
                    from services.memory_feedback import extract_playground_strategy_names

                    playground_selected_strategies = extract_playground_strategy_names(
                        playground_assessment
                    )
                except Exception:
                    playground_selected_strategies = []

            # Extract regime confidence
            regime_confidence = None
            if analysis and analysis.allocator_output:
                quant = analysis.allocator_output or {}
                regime_result = quant.get("regime_result") or {}
                regime_confidence = regime_result.get("confidence")

            # Build structured decision record
            decision_record = {
                "analysis_id": analysis_id,
                "regime": regime,
                "regime_confidence": regime_confidence,
                "uncertainty_flag": uncertainty_flag,
                "researcher_confidence": researcher_confidence,
                "recommended_stance": recommended_stance,
                "weights_used": weights_used,
                "top_holdings": top_holdings,
                "execution_happened": execution_happened,
                "rationale": rationale,
                "key_events": key_events_list if isinstance(key_events_list, list) else [],
                "market_judgment": {
                    "regime": market_judgment.get("regime"),
                    "impact_bias": market_judgment.get("impact_bias"),
                    "uncertainty_flag": uncertainty_flag,
                },
                "playground_strategy_assessment": playground_assessment,
                "playground_selected_strategies": playground_selected_strategies,
                # outcome may have portfolio_return_pct and decision_quality_score
                "outcome_portfolio_return_pct": outcome.get("portfolio_return_pct"),
                "outcome_decision_quality_score": decision_quality,
                "outcome_recorded_at": datetime.utcnow().isoformat(),
            }

            if existing:
                # Update existing record's decision context fields
                existing.decision = decision_record
                logger.info(
                    f"[decision_memory] Updated decision_context for {trading_date} "
                    f"(analysis_id={analysis_id})"
                )
            else:
                # Create new memory_daily with decision context
                # (daily_analyst usually creates this, but if it skipped, create minimal)
                new_memory = MemoryDaily(
                    trading_date=trading_date,
                    regime_label=regime or "unknown",
                    regime_confidence=regime_confidence,
                    risk_approved=bool(analysis and analysis.risk_approved) if analysis else False,
                    execution_happened=execution_happened,
                    recommended_stance=recommended_stance or "maintain",
                    top3_overweight=[{"ticker": h["ticker"], "weight": h["weight"], "reason": ""}
                                     for h in top_holdings[:3]],
                    top3_underweight=[],
                    macro_narrative=rationale or "",
                    key_events=key_events_list if isinstance(key_events_list, list) else [],
                    agent_analysis_id=analysis_id,
                    decision=decision_record,
                )
                db.add(new_memory)
                logger.info(
                    f"[decision_memory] Created decision_context for {trading_date} "
                    f"(analysis_id={analysis_id})"
                )

            await db.commit()
            return True

    except Exception as e:
        logger.error(f"[decision_memory] Failed to write decision context: {e}")
        return False


# ─────────────────────────────── DQS Computation ───────────────────────────────


def compute_decision_quality_score(
    recommended_stance: Optional[str],
    portfolio_return_pct: Optional[float],
    spy_return_pct: Optional[float],
    researcher_confidence: Optional[str],
    execution_happened: bool = True,
) -> Optional[float]:
    """
    Compute decision quality score (0.0–1.0) from decision context + actual outcome.
    Called the day after a decision to backfill MemoryDaily.

    Formula: 40% direction_accuracy + 35% magnitude_alignment + 25% confidence_calibration
    """
    if portfolio_return_pct is None:
        return None

    # Direction accuracy (40%)
    if recommended_stance in ("buy", "overweight"):
        direction_score = 1.0 if portfolio_return_pct > 0 else 0.0
    elif recommended_stance in ("sell", "underweight"):
        direction_score = 1.0 if portfolio_return_pct < 0 else 0.0
    else:
        direction_score = 0.5  # maintain = neutral

    if not execution_happened:
        direction_score = 0.5  # passive drift, can't fairly score

    # Magnitude alignment (35%)
    if spy_return_pct is not None and execution_happened:
        diff = abs(portfolio_return_pct - spy_return_pct)
        denom = abs(spy_return_pct) + 0.005
        magnitude_score = max(0.0, min(1.0, 1.0 - diff / denom))
    else:
        magnitude_score = 0.5

    # Confidence calibration (25%)
    correct = (direction_score >= 0.5)
    conf_bonus = {
        ("high", True): 1.0,
        ("medium", True): 0.75,
        ("low", True): 0.5,
        ("high", False): 0.0,
        ("medium", False): 0.25,
        ("low", False): 0.5,
    }
    confidence_score = conf_bonus.get((researcher_confidence or "low", correct), 0.5)

    return round(
        0.40 * direction_score + 0.35 * magnitude_score + 0.25 * confidence_score, 4
    )


async def read_decision_context(
    trading_date: date,
) -> dict | None:
    """
    Read the structured decision context for a given trading date.
    Returns the decision dict or None.
    """
    try:
        async with AsyncSessionLocal() as db:
            result = await db.execute(
                select(MemoryDaily.decision).where(MemoryDaily.trading_date == trading_date)
            )
            row = result.scalar_one_or_none()
            return row if row else None
    except Exception as e:
        logger.warning(f"[decision_memory] Failed to read decision context: {e}")
        return None


async def backfill_decision_quality(
    trading_date: date,
    decision_quality_score: float,
    portfolio_return_pct: Optional[float] = None,
) -> bool:
    """
    Backfill decision_quality_score for a past trading date.
    Called the next day by daily_analyst when actual portfolio return is known.

    Args:
        trading_date: The date whose quality to backfill
        decision_quality_score: 0.0-1.0 quality score
        portfolio_return_pct: Optional actual portfolio return for the day
    """
    try:
        async with AsyncSessionLocal() as db:
            result = await db.execute(
                select(MemoryDaily).where(MemoryDaily.trading_date == trading_date)
            )
            existing = result.scalar_one_or_none()
            if not existing:
                return False

            if existing.decision is None:
                existing.decision = {}

            existing.decision["outcome_decision_quality_score"] = decision_quality_score
            if portfolio_return_pct is not None:
                existing.decision["outcome_portfolio_return_pct"] = portfolio_return_pct
            existing.decision["outcome_backfilled_at"] = datetime.utcnow().isoformat()

            await db.commit()
            logger.info(
                f"[decision_memory] Backfilled DQS={decision_quality_score:.2%} for {trading_date}"
            )
            return True
    except Exception as e:
        logger.error(f"[decision_memory] Failed to backfill decision quality: {e}")
        return False
