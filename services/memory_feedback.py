"""
Memory feedback helpers.

This module turns post-hoc decision quality into conservative advisory signals.
It does not change execution authority; Risk Manager remains the hard gate.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

logger = logging.getLogger("qc_fastapi_2.memory_feedback")

MIN_STRATEGY_FEEDBACK_SAMPLES = 3
LOOKBACK_DAYS = 120


@dataclass
class StrategyMemoryFeedback:
    strategy_name: str
    regime: str
    sample_size: int
    avg_decision_quality_score: float | None
    discount_multiplier: float
    confidence: str
    advisory_note: str
    can_bypass_risk_manager: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "strategy_name": self.strategy_name,
            "regime": self.regime,
            "sample_size": self.sample_size,
            "avg_decision_quality_score": self.avg_decision_quality_score,
            "discount_multiplier": self.discount_multiplier,
            "confidence": self.confidence,
            "advisory_note": self.advisory_note,
            "can_bypass_risk_manager": self.can_bypass_risk_manager,
        }


async def build_strategy_memory_feedback(
    regime: str,
    strategy_names: list[str],
    lookback_days: int = LOOKBACK_DAYS,
) -> dict[str, dict[str, Any]]:
    """Build per-strategy advisory discounts for the current regime."""
    if not strategy_names:
        return {}

    today = date.today()
    cutoff = today - timedelta(days=lookback_days)
    try:
        from sqlalchemy import select

        from db.models import MemoryDaily
        from db.session import AsyncSessionLocal

        async with AsyncSessionLocal() as db:
            result = await db.execute(
                select(MemoryDaily)
                .where(MemoryDaily.trading_date >= cutoff)
                .where(MemoryDaily.trading_date < today)
                .where(MemoryDaily.decision_quality_score.isnot(None))
                .order_by(MemoryDaily.trading_date.desc())
            )
            records = result.scalars().all()
    except Exception as exc:
        logger.warning("[memory_feedback] strategy feedback read failed: %s", exc)
        return {
            name: _neutral_strategy_feedback(name, regime, "memory feedback unavailable").to_dict()
            for name in strategy_names
        }

    return build_strategy_memory_feedback_from_records(regime, strategy_names, records)


def build_strategy_memory_feedback_from_records(
    regime: str,
    strategy_names: list[str],
    records: list[Any],
) -> dict[str, dict[str, Any]]:
    """
    Pure helper for computing strategy discounts from MemoryDaily-like records.

    A record contributes only when:
    - regime matches current regime
    - decision_quality_score is available
    - decision/raw_researcher_output names a Playground strategy used in the decision
    """
    names = list(dict.fromkeys(strategy_names))
    samples: dict[str, list[float]] = {name: [] for name in names}
    normalized_regime = _normalize_regime(regime)

    for record in records:
        if _normalize_regime(getattr(record, "regime_label", None)) != normalized_regime:
            continue
        dqs = _record_dqs(record)
        if dqs is None:
            continue
        used = _extract_playground_strategy_names(record)
        for name in used:
            if name in samples:
                samples[name].append(dqs)

    feedback: dict[str, dict[str, Any]] = {}
    for name in names:
        values = samples[name]
        if len(values) < MIN_STRATEGY_FEEDBACK_SAMPLES:
            feedback[name] = _neutral_strategy_feedback(
                name,
                regime,
                f"insufficient similar-regime strategy samples ({len(values)}/{MIN_STRATEGY_FEEDBACK_SAMPLES})",
                sample_size=len(values),
            ).to_dict()
            continue

        avg_dqs = round(sum(values) / len(values), 4)
        if avg_dqs < 0.45:
            multiplier = 0.70
            confidence = "medium" if len(values) < 8 else "high"
            note = (
                f"discounted: historical DQS {avg_dqs:.2f} in {regime} "
                f"over {len(values)} samples"
            )
        elif avg_dqs < 0.55:
            multiplier = 0.85
            confidence = "medium"
            note = (
                f"slightly discounted: historical DQS {avg_dqs:.2f} in {regime} "
                f"over {len(values)} samples"
            )
        else:
            multiplier = 1.0
            confidence = "medium" if len(values) < 8 else "high"
            note = (
                f"no discount: historical DQS {avg_dqs:.2f} in {regime} "
                f"over {len(values)} samples"
            )

        feedback[name] = StrategyMemoryFeedback(
            strategy_name=name,
            regime=regime,
            sample_size=len(values),
            avg_decision_quality_score=avg_dqs,
            discount_multiplier=multiplier,
            confidence=confidence,
            advisory_note=note,
        ).to_dict()

    return feedback


def _neutral_strategy_feedback(
    strategy_name: str,
    regime: str,
    reason: str,
    sample_size: int = 0,
) -> StrategyMemoryFeedback:
    return StrategyMemoryFeedback(
        strategy_name=strategy_name,
        regime=regime,
        sample_size=sample_size,
        avg_decision_quality_score=None,
        discount_multiplier=1.0,
        confidence="low",
        advisory_note=f"neutral memory feedback: {reason}",
    )


def _record_dqs(record: Any) -> float | None:
    dqs = getattr(record, "decision_quality_score", None)
    if dqs is None:
        decision = getattr(record, "decision", None) or {}
        dqs = decision.get("outcome_decision_quality_score")
    try:
        return float(dqs)
    except (TypeError, ValueError):
        return None


def _extract_playground_strategy_names(record: Any) -> set[str]:
    decision = getattr(record, "decision", None) or {}
    raw_output = getattr(record, "raw_researcher_output", None) or {}
    assessment = (
        decision.get("playground_strategy_assessment")
        or raw_output.get("playground_strategy_assessment")
        or {}
    )
    explicit = decision.get("playground_selected_strategies")
    names = set(_strategy_names_from_value(explicit))
    names.update(_strategy_names_from_value(assessment))
    return names


def extract_playground_strategy_names(assessment: Any) -> list[str]:
    """Public pure wrapper used when storing daily decision memory."""
    return sorted(_strategy_names_from_value(assessment))


def _strategy_names_from_value(value: Any) -> set[str]:
    names: set[str] = set()
    if isinstance(value, str):
        cleaned = value.strip()
        if cleaned:
            names.add(cleaned)
        return names
    if isinstance(value, list):
        for item in value:
            names.update(_strategy_names_from_value(item))
        return names
    if not isinstance(value, dict):
        return names

    for key in (
        "strategy_name",
        "selected_strategy",
        "best_strategy",
        "primary_strategy",
        "recommended_strategy",
    ):
        names.update(_strategy_names_from_value(value.get(key)))

    for key in (
        "selected_strategies",
        "discounted_strategies",
        "strategy_blend",
        "blend",
    ):
        names.update(_strategy_names_from_value(value.get(key)))

    return names


def _normalize_regime(value: Any) -> str:
    return str(value or "unknown").strip().lower()
