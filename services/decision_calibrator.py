# services/decision_calibrator.py
"""
DECISION_CALIBRATOR — calibrates LLM confidence scores based on historical accuracy.

After execution (or after daily_analyst backfills decision_quality_score),
computes per-confidence-level accuracy and updates researcher_confidence_bias
in system_config.

Updated bias multipliers are used by agents/researcher.py to adjust
confidence scoring when market conditions diverge from historical patterns.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Optional

from sqlalchemy import select, func

from db.session import AsyncSessionLocal
from db.models import MemoryDaily
from db.queries import get_system_config, upsert_system_config

logger = logging.getLogger("qc_fastapi_2.decision_calibrator")

# Confidence levels
CONFIDENCE_LEVELS = ["high", "medium", "low"]

# Minimum sample size before applying calibration
MIN_SAMPLE_SIZE = 10

# Decay factor for old calibrations
DECAY_WINDOW_DAYS = 90


# ─────────────────────────────── Result Dataclass ──────────────────────────────


@dataclass
class CalibrationResult:
    calibration_timestamp: str
    per_level_accuracy: dict[str, float]  # {"high": 0.72, "medium": 0.55, "low": 0.38}
    bias_multipliers: dict[str, float]     # {"high": 1.0, "medium": 0.9, "low": 1.1}
    sample_size: int
    confidence: str  # "high" / "medium" / "low"
    recommendations: list[str]  # e.g., ["high is well-calibrated", "medium is overconfident"]


# ─────────────────────────────── DecisionCalibrator ───────────────────────────


class DecisionCalibrator:
    """
    Calibrates LLM confidence scores based on historical accuracy.

    Process:
    1. Read MemoryDaily records with backfilled decision_quality_score
    2. Group by researcher_confidence (high/medium/low)
    3. Compute per-level accuracy: what % were correct (DQS > 0.6)
    4. Compare to expected accuracy: high=70%+, medium=50-70%, low=30-50%
    5. Compute bias multiplier: overconfident → <1.0, underconfident → >1.0
    6. Write to system_config.researcher_confidence_bias
    """

    async def calibrate_after_execution(
        self,
        trading_date: Optional[date] = None,
    ) -> CalibrationResult:
        """
        Run calibration on MemoryDaily records.
        If trading_date provided, uses that date as the anchor;
        otherwise uses today (backfill scenario).

        Called from:
        - daily_analyst.py after backfilling decision_quality_score (next-day)
        - cron/decision_calibrator.py as standalone next-day job
        """
        today = trading_date or date.today()
        cutoff = today - timedelta(days=DECAY_WINDOW_DAYS)

        async with AsyncSessionLocal() as db:
            # Get all MemoryDaily with backfilled DQS within window
            result = await db.execute(
                select(MemoryDaily)
                .where(MemoryDaily.trading_date >= cutoff)
                .where(MemoryDaily.trading_date <= today)
                .where(MemoryDaily.decision_quality_score.isnot(None))
                .order_by(MemoryDaily.trading_date.asc())
            )
            records = result.scalars().all()

        if len(records) < MIN_SAMPLE_SIZE:
            logger.info(
                f"[DECISION_CALIBRATOR] Only {len(records)} samples (need {MIN_SAMPLE_SIZE}), "
                f"skipping calibration"
            )
            return _degraded_result(f"insufficient_data_{len(records)}")

        # Group by researcher_confidence
        groups: dict[str, list[MemoryDaily]] = {level: [] for level in CONFIDENCE_LEVELS}
        for record in records:
            decision = record.decision or {}
            conf = decision.get("researcher_confidence") or record.regime_confidence
            conf_str = str(conf).lower().strip() if conf else "medium"
            if conf_str not in CONFIDENCE_LEVELS:
                conf_str = "medium"
            groups[conf_str].append(record)

        # Expected accuracy ranges per level
        EXPECTED = {"high": 0.70, "medium": 0.55, "low": 0.35}

        per_level_accuracy: dict[str, float] = {}
        bias_multipliers: dict[str, float] = {}
        recommendations: list[str] = []
        total_samples = 0

        for level in CONFIDENCE_LEVELS:
            group = groups[level]
            n = len(group)
            if n == 0:
                per_level_accuracy[level] = 0.0
                bias_multipliers[level] = 1.0
                recommendations.append(f"{level}: no data")
                continue

            total_samples += n

            # Accuracy: % of records where DQS > 0.6
            correct = sum(1 for r in group if (r.decision_quality_score or 0) > 0.6)
            accuracy = correct / n
            per_level_accuracy[level] = round(accuracy, 3)

            # Expected accuracy
            expected = EXPECTED[level]

            # Bias multiplier: accuracy / expected
            # > 1.0 = well-calibrated or underconfident
            # < 1.0 = overconfident
            if expected > 0:
                bias = accuracy / expected
                # Clamp to [0.5, 1.5] range
                bias = max(0.5, min(1.5, bias))
                bias_multipliers[level] = round(bias, 3)
            else:
                bias_multipliers[level] = 1.0

            # Interpretation
            if abs(bias_multipliers[level] - 1.0) < 0.05:
                recommendations.append(f"{level} ({n} samples): well-calibrated")
            elif bias_multipliers[level] < 1.0:
                recommendations.append(
                    f"{level} ({n} samples): OVERCONFIDENT — actual={accuracy:.0%}, expected={expected:.0%}, "
                    f"bias={bias_multipliers[level]:.2f}"
                )
            else:
                recommendations.append(
                    f"{level} ({n} samples): underconfident — actual={accuracy:.0%}, expected={expected:.0%}, "
                    f"bias={bias_multipliers[level]:.2f}"
                )

        # Determine confidence of this calibration
        calibration_confidence = "high" if total_samples >= MIN_SAMPLE_SIZE * 2 else "medium"

        result = CalibrationResult(
            calibration_timestamp=datetime.utcnow().isoformat(),
            per_level_accuracy=per_level_accuracy,
            bias_multipliers=bias_multipliers,
            sample_size=total_samples,
            confidence=calibration_confidence,
            recommendations=recommendations,
        )

        # Write to system_config
        await self._write_bias_to_config(result)

        logger.info(
            f"[DECISION_CALIBRATOR] Calibration complete: "
            f"{total_samples} samples, bias={bias_multipliers}"
        )

        return result

    async def get_current_bias(self) -> dict[str, float]:
        """Read current bias multipliers from system_config."""
        async with AsyncSessionLocal() as db:
            cfg = await get_system_config(db, "researcher_confidence_bias")
        if not cfg:
            return {"high": 1.0, "medium": 1.0, "low": 1.0}
        bias = (cfg.value or {}).get("bias_multipliers", {})
        return {
            "high": bias.get("high", 1.0),
            "medium": bias.get("medium", 1.0),
            "low": bias.get("low", 1.0),
        }

    async def apply_bias_to_confidence(
        self,
        base_confidence: str,  # high/medium/low
        override_multiplier: float = 1.0,
    ) -> float:
        """
        Apply bias to a base confidence level.
        Returns a float between 0.0 and 1.0 representing calibrated confidence.

        Used in researcher.py when computing per-ticker confidence with historical calibration.
        """
        bias = await self.get_current_bias()
        multiplier = bias.get(base_confidence, 1.0) * override_multiplier

        # Convert base confidence to base score
        base_scores = {"high": 0.80, "medium": 0.60, "low": 0.40}
        base_score = base_scores.get(base_confidence, 0.60)

        calibrated = min(1.0, base_score * multiplier)
        return round(calibrated, 3)

    # ── Internal Helpers ──────────────────────────────────────────────────────

    async def _write_bias_to_config(self, result: CalibrationResult) -> None:
        """Write calibration result to system_config."""
        async with AsyncSessionLocal() as db:
            await upsert_system_config(
                db,
                "researcher_confidence_bias",
                {
                    "calibration_timestamp": result.calibration_timestamp,
                    "per_level_accuracy": result.per_level_accuracy,
                    "bias_multipliers": result.bias_multipliers,
                    "sample_size": result.sample_size,
                    "confidence": result.confidence,
                    "recommendations": result.recommendations,
                    "decay_window_days": DECAY_WINDOW_DAYS,
                    "min_sample_size": MIN_SAMPLE_SIZE,
                },
                "decision_calibrator",
            )


# ─────────────────────────────── Convenience Functions ───────────────────────


async def calibrate_decisions(trading_date: Optional[date] = None) -> CalibrationResult:
    """Run calibration. Called from daily_analyst.py (next-day backfill) or standalone cron."""
    calibrator = DecisionCalibrator()
    return await calibrator.calibrate_after_execution(trading_date=trading_date)


def _degraded_result(reason: str) -> CalibrationResult:
    return CalibrationResult(
        calibration_timestamp=datetime.utcnow().isoformat(),
        per_level_accuracy={"high": 0.0, "medium": 0.0, "low": 0.0},
        bias_multipliers={"high": 1.0, "medium": 1.0, "low": 1.0},
        sample_size=0,
        confidence="low",
        recommendations=[f"Calibration skipped: {reason}"],
    )