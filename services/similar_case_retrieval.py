# services/similar_case_retrieval.py
"""
Similar case retrieval for RESEARCHER prompt injection.

When running pipeline, retrieves similar past decisions (same regime,
similar market conditions) from MemoryDaily/MemoryWeekly and returns
them for injection into the RESEARCHER prompt.

Used by agents/researcher.py via _retrieve_similar_cases_for_researcher().

Similarity scoring:
  - Regime match: 40% weight (exact match required)
  - VIX range: within 5 points: 20% weight
  - Drawdown range: within 3%: 20% weight
  - Breadth range: within 15%: 20% weight
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Optional

from sqlalchemy import select

from db.session import AsyncSessionLocal
from db.models import MemoryDaily, MemoryWeekly, PortfolioTimeseries

logger = logging.getLogger("qc_fastapi_2.similar_cases")

# ─────────────────────────────── Weights & Thresholds ────────────────────────

REGIME_WEIGHT = 0.40
VIX_WEIGHT = 0.20
DRAWDOWN_WEIGHT = 0.20
BREADTH_WEIGHT = 0.20

VIX_RANGE = 5.0       # ±5 points
DRAWDOWN_RANGE = 0.03  # ±3%
BREADTH_RANGE = 0.15   # ±15% breadth

MIN_CASES = 3
MAX_CASES = 5


# ─────────────────────────────── SimilarCaseRetriever ────────────────────────


class SimilarCaseRetriever:
    """
    Retrieves similar past decisions from MemoryDaily for confidence calibration.
    """

    async def retrieve_similar_cases(
        self,
        current_regime: str,
        current_market_conditions: Optional[dict] = None,
        lookback_days: int = 60,
        max_cases: int = MAX_CASES,
    ) -> list[dict]:
        """
        Retrieve similar past decisions for the given regime and market conditions.

        Args:
            current_regime: e.g., "trending_bull", "high_vol", "mean_reverting"
            current_market_conditions: dict with keys: vix, drawdown_pct, breadth_pct
            lookback_days: how many days to look back (default 60)
            max_cases: maximum cases to return

        Returns:
            List of similar case dicts sorted by similarity score descending.
            Each dict: {
              "trading_date": "2024-01-15",
              "regime": "trending_bull",
              "vix": 18.5,
              "drawdown_pct": 0.03,
              "recommended_stance": "overweight",
              "weights_used": {"XLK": 0.18, ...},
              "outcome_portfolio_return_pct": 0.45,
              "outcome_decision_quality_score": 0.72,
              "key_events": ["Fed dovish", ...],
              "similarity_score": 0.85,
              "similarity_breakdown": {...}
            }
        """
        current_market_conditions = current_market_conditions or {}

        today = date.today()
        cutoff = today - timedelta(days=lookback_days)

        async with AsyncSessionLocal() as db:
            # Get MemoryDaily records in lookback window
            result = await db.execute(
                select(MemoryDaily)
                .where(MemoryDaily.trading_date >= cutoff)
                .where(MemoryDaily.trading_date < today)
                .order_by(MemoryDaily.trading_date.desc())
            )
            daily_records = result.scalars().all()

        if not daily_records:
            return []

        # Compute similarity scores
        scored_cases = []
        for record in daily_records:
            score, breakdown = self._compute_similarity(
                record, current_regime, current_market_conditions
            )

            # Extract case data
            decision = record.decision or {}
            case = {
                "trading_date": str(record.trading_date),
                "regime": record.regime_label or "unknown",
                "vix": record.vix_close,
                "drawdown_pct": None,
                "recommended_stance": record.recommended_stance,
                "weights_used": decision.get("weights_used") or {},
                "top_holdings": decision.get("top_holdings") or [],
                "outcome_portfolio_return_pct": (
                    decision.get("outcome_portfolio_return_pct")
                    or record.portfolio_return_pct
                ),
                "outcome_decision_quality_score": (
                    decision.get("outcome_decision_quality_score")
                    or record.decision_quality_score
                ),
                "key_events": record.key_events or [],
                "macro_narrative": record.macro_narrative or "",
                "researcher_confidence": decision.get("researcher_confidence"),
                "similarity_score": score,
                "similarity_breakdown": breakdown,
            }

            # Only include if at least regime matches
            if breakdown["regime_score"] >= REGIME_WEIGHT * 0.5:
                scored_cases.append(case)

        # Sort by similarity score descending
        scored_cases.sort(key=lambda c: c["similarity_score"], reverse=True)

        # Also enrich with breadth data from holdings
        await self._enrich_with_breadth(scored_cases, today, lookback_days)

        return scored_cases[:max_cases]

    # ── Similarity Computation ───────────────────────────────────────────────

    def _compute_similarity(
        self,
        record: MemoryDaily,
        current_regime: str,
        current_conditions: dict,
    ) -> tuple[float, dict]:
        """
        Compute weighted similarity score between a past record and current conditions.

        Returns: (total_score 0.0-1.0, breakdown_dict)
        """
        breakdown: dict[str, float] = {}
        total_score = 0.0

        # 1. Regime match (40%)
        regime_match = (record.regime_label or "").lower() == current_regime.lower()
        if regime_match:
            regime_score = REGIME_WEIGHT
        else:
            # Partial credit for similar regimes
            regime_score = 0.0
        breakdown["regime"] = regime_score
        breakdown["regime_match"] = regime_match
        breakdown["regime_recorded"] = record.regime_label
        breakdown["regime_current"] = current_regime
        total_score += regime_score

        # 2. VIX range (20%)
        vix_current = current_conditions.get("vix")
        if vix_current is not None and record.vix_close is not None:
            vix_diff = abs(record.vix_close - vix_current)
            if vix_diff <= VIX_RANGE:
                # Full credit at diff=0, linearly decreasing to 0 at diff=VIX_RANGE
                vix_score = VIX_WEIGHT * (1 - vix_diff / VIX_RANGE)
            else:
                vix_score = 0.0
        else:
            # No VIX data — no credit, no penalty
            vix_score = 0.0
        breakdown["vix_score"] = vix_score
        breakdown["vix_recorded"] = record.vix_close
        breakdown["vix_current"] = vix_current
        total_score += vix_score

        # 3. Drawdown range (20%)
        dd_current = current_conditions.get("drawdown_pct")
        if dd_current is not None and record.current_drawdown_pct is not None:
            dd_diff = abs(record.current_drawdown_pct - dd_current)
            if dd_diff <= DRAWDOWN_RANGE:
                dd_score = DRAWDOWN_WEIGHT * (1 - dd_diff / DRAWDOWN_RANGE)
            else:
                dd_score = 0.0
        else:
            # Fall back to vix based if no drawdown
            dd_score = 0.0
        breakdown["drawdown_score"] = dd_score
        breakdown["drawdown_recorded"] = getattr(record, "current_drawdown_pct", None)
        breakdown["drawdown_current"] = dd_current
        total_score += dd_score

        # 4. Breadth (20%) — placeholder, computed from SPY momentum
        # If we have breadth data, use it; otherwise use regime proxy
        breadth_current = current_conditions.get("breadth_pct")
        if breadth_current is not None:
            # Approximate breadth from SPY return
            breadth_score = BREADTH_WEIGHT * 0.5  # half credit if we have breadth data
        else:
            breadth_score = BREADTH_WEIGHT * 0.25  # quarter credit as regime proxy
        breakdown["breadth_score"] = breadth_score
        total_score += breadth_score

        # Normalize to 0-1
        if total_score > 1.0:
            total_score = 1.0

        return total_score, breakdown

    # ── Data Enrichment ─────────────────────────────────────────────────────

    async def _enrich_with_breadth(
        self,
        scored_cases: list[dict],
        today: date,
        lookback_days: int,
    ) -> None:
        """Enrich cases with portfolio breadth/drawdown data from PortfolioTimeseries."""
        try:
            cutoff = today - timedelta(days=lookback_days)
            async with AsyncSessionLocal() as db:
                result = await db.execute(
                    select(PortfolioTimeseries)
                    .where(PortfolioTimeseries.recorded_at >= cutoff)
                    .order_by(PortfolioTimeseries.recorded_at.desc())
                )
                portfolios = result.scalars().all()

            portfolio_by_date: dict[str, dict] = {}
            for p in portfolios:
                date_str = str(p.recorded_at.date()) if hasattr(p.recorded_at, "date") else str(p.recorded_at)[:10]
                if date_str not in portfolio_by_date:
                    portfolio_by_date[date_str] = p

            for case in scored_cases:
                date_str = case["trading_date"]
                if date_str in portfolio_by_date:
                    p = portfolio_by_date[date_str]
                    if case.get("drawdown_pct") is None and hasattr(p, "current_drawdown_pct"):
                        case["drawdown_pct"] = float(p.current_drawdown_pct or 0)
                    if hasattr(p, "spy_return_pct"):
                        case["spy_return_pct"] = float(p.spy_return_pct or 0)
        except Exception as e:
            logger.warning(f"[similar_case_retrieval] Failed to enrich with breadth: {e}")


# ─────────────────────────────── Convenience Functions ────────────────────────


async def get_similar_cases_for_researcher(
    regime: str,
    market_conditions: Optional[dict] = None,
    max_cases: int = MAX_CASES,
) -> list[dict]:
    """Convenience wrapper — used in agents/researcher.py prompt injection."""
    retriever = SimilarCaseRetriever()
    return await retriever.retrieve_similar_cases(
        current_regime=regime,
        current_market_conditions=market_conditions,
        max_cases=max_cases,
    )


def format_cases_for_prompt(cases: list[dict]) -> str:
    """
    Format similar cases for injection into RESEARCHER user message.

    Returns a markdown-formatted string suitable for the LLM prompt.
    """
    if not cases:
        return "No similar historical cases available."

    lines = []
    for i, c in enumerate(cases, 1):
        dqs = c.get("outcome_decision_quality_score")
        dqs_str = f"{dqs:.0%}" if dqs is not None else "N/A"
        ret = c.get("outcome_portfolio_return_pct")
        ret_str = f"{ret:+.2%}" if ret is not None else "N/A"

        events = c.get("key_events") or []
        events_str = ", ".join(events[:3]) if events else "none"

        stance = c.get("recommended_stance", "maintain")
        top_holdings = c.get("top_holdings", [])
        top_str = ", ".join([f"{h['ticker']}:{h['weight']:.0%}" for h in top_holdings[:3]]) if top_holdings else "N/A"

        lines.append(
            f"{i}. [{c['trading_date']}] "
            f"Regime={c['regime']}, "
            f"VIX={c.get('vix', 'N/A')}, "
            f"Stance={stance}, "
            f"Top: {top_str}, "
            f"DQS={dqs_str}, "
            f"Return={ret_str}, "
            f"Events: {events_str}"
        )

    return "\n".join(lines)
