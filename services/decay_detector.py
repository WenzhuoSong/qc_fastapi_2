# services/decay_detector.py
"""
DECAY_DETECTOR — monitors signal effectiveness decay over rolling windows.

Detects when momentum signals stop working and triggers strategy_refresh_recommendation.
Called by cron/weekly_analyst.py before LLM distillation (runs Sunday night).

Decay detection signals:
  1. Signal accuracy:  decision_quality_score trend over 4-week rolling windows
  2. Momentum effectiveness: MemoryWeekly.momentum_effectiveness over 13 weeks
  3. Bull/Bear disagreement spike: cross_exam disagreement_count increase
  4. RSI mean-reversion failure: high_vol regime + wrong-direction RSI signals
  5. Regime stability collapse: regime_shift=True for 3+ consecutive weeks

Output:
  {
    "decay_signal_strength": "strong" | "moderate" | "weak" | "none",
    "confidence": 0.0-1.0,
    "momentum_effectiveness_trend": "improving" | "stable" | "declining",
    "affected_regimes": ["high_vol", ...],
    "recommendation": "strategy_refresh_recommendation" | null,
    "details": {...}
  }
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Optional

from sqlalchemy import select, func

from db.session import AsyncSessionLocal
from db.models import MemoryDaily, MemoryWeekly, AgentStepLog, SystemConfig
from tools.notify_tools import tool_send_telegram

logger = logging.getLogger("qc_fastapi_2.decay_detector")


def _fmt_float(value: object) -> str:
    return f"{float(value):.2f}" if isinstance(value, (int, float)) else "N/A"


# ─────────────────────────────── Strength Enum ───────────────────────────────


@dataclass
class DecayResult:
    decay_signal_strength: str      # "strong" | "moderate" | "weak" | "none"
    confidence: float              # 0.0-1.0
    momentum_effectiveness_trend: str  # "improving" | "stable" | "declining"
    affected_regimes: list[str]
    recommendation: str | None    # "strategy_refresh_recommendation" | null
    details: dict


# ─────────────────────────────── DecayDetector ────────────────────────────────


class DecayDetector:
    """
    Monitors signal effectiveness decay over rolling windows.
    Triggers strategy_refresh_recommendation when decay is detected.
    """

    # Minimum weeks of data needed before declaring decay
    MIN_WEEKS = 6
    MIN_DAYS = 20  # for daily metrics

    async def evaluate_decay(self) -> DecayResult:
        """
        Evaluate all 5 decay signals and aggregate into decay_signal_strength.
        Called at start of weekly_analyst.py.
        """
        signals: list[dict] = []

        # Signal 1: Decision quality score trend (4-week rolling)
        dqs_trend = await self._check_decision_quality_trend()
        signals.append(dqs_trend)

        # Signal 2: Momentum effectiveness trend (13-week rolling)
        mom_trend = await self._check_momentum_effectiveness_trend()
        signals.append(mom_trend)

        # Signal 3: Bull/Bear disagreement increase
        disagreement_trend = await self._check_disagreement_trend()
        signals.append(disagreement_trend)

        # Signal 4: Regime stability collapse
        regime_trend = await self._check_regime_stability()
        signals.append(regime_trend)

        # Signal 5: Per-strategy health profile decay
        strategy_health = await self._check_strategy_health_profiles()
        signals.append(strategy_health)

        # Aggregate signals
        return self._aggregate_signals(signals)

    # ── Signal 1: Decision Quality Score Trend ──────────────────────────────

    async def _check_decision_quality_trend(self) -> dict:
        """
        Compare decision_quality_score over 4-week rolling windows.
        Negative trend > 15% → decaying.
        """
        today = date.today()
        four_weeks_ago = today - timedelta(weeks=4)
        eight_weeks_ago = today - timedelta(weeks=8)

        async with AsyncSessionLocal() as db:
            # Recent 4 weeks
            recent = await db.execute(
                select(func.avg(MemoryDaily.decision_quality_score))
                .where(MemoryDaily.trading_date >= four_weeks_ago)
                .where(MemoryDaily.decision_quality_score.isnot(None))
            )
            recent_avg = recent.scalar_one_or_none()

            # Previous 4 weeks
            previous = await db.execute(
                select(func.avg(MemoryDaily.decision_quality_score))
                .where(MemoryDaily.trading_date >= eight_weeks_ago)
                .where(MemoryDaily.trading_date < four_weeks_ago)
                .where(MemoryDaily.decision_quality_score.isnot(None))
            )
            previous_avg = previous.scalar_one_or_none()

        if recent_avg is None or previous_avg is None:
            return {"signal": "dqs_trend", "triggered": False, "details": "insufficient data"}

        # Positive change = improvement, negative = decay
        if previous_avg > 0:
            pct_change = (recent_avg - previous_avg) / previous_avg
        else:
            pct_change = 0.0

        # More than 15% drop = decay signal
        decay_threshold = -0.15
        triggered = pct_change < decay_threshold

        return {
            "signal": "dqs_trend",
            "triggered": triggered,
            "value": pct_change,
            "recent_avg": recent_avg,
            "previous_avg": previous_avg,
            "details": (
                f"decision_quality: recent={recent_avg:.2%}, previous={previous_avg:.2%}, "
                f"change={pct_change:+.1%} → {'DECAY' if triggered else 'ok'}"
            ),
        }

    # ── Signal 2: Momentum Effectiveness Trend ─────────────────────────────

    MOMENTUM_SCORES = {"strong": 4, "moderate": 3, "weak": 2, "failed": 1}

    async def _check_momentum_effectiveness_trend(self) -> dict:
        """
        Check MemoryWeekly.momentum_effectiveness over rolling 13-week windows.
        If "strong" drops to "moderate" or "failed" for 3+ consecutive weeks → trigger.
        """
        today = date.today()
        thirteen_weeks_ago = today - timedelta(weeks=13)

        async with AsyncSessionLocal() as db:
            result = await db.execute(
                select(MemoryWeekly)
                .where(MemoryWeekly.week_start >= thirteen_weeks_ago)
                .order_by(MemoryWeekly.week_start.asc())
            )
            weeks = result.scalars().all()

        if len(weeks) < self.MIN_WEEKS:
            return {"signal": "momentum_trend", "triggered": False, "details": "insufficient weeks"}

        # Compute rolling 4-week momentum score average
        recent_weeks = weeks[-4:]
        prev_weeks = weeks[-8:-4] if len(weeks) >= 8 else []

        def avg_momentum(ws) -> float | None:
            if not ws:
                return None
            scores = [self.MOMENTUM_SCORES.get(w.momentum_effectiveness or "moderate", 3) for w in ws]
            return sum(scores) / len(scores)

        recent_avg = avg_momentum(recent_weeks)
        prev_avg = avg_momentum(prev_weeks) if prev_weeks else None

        if recent_avg is None:
            return {"signal": "momentum_trend", "triggered": False, "details": "no recent momentum data"}

        # Check for sustained degradation: current 4 weeks should be declining vs prior 4
        triggered = False
        if prev_avg is not None and recent_avg < prev_avg - 0.5:
            triggered = True

        # Also check: consecutive "weak" or "failed" in recent 3+ weeks
        recent_effectiveness = [w.momentum_effectiveness for w in recent_weeks]
        consecutive_weak = 0
        for e in reversed(recent_effectiveness):
            if e in ("weak", "failed"):
                consecutive_weak += 1
            else:
                break
        if consecutive_weak >= 3:
            triggered = True

        return {
            "signal": "momentum_trend",
            "triggered": triggered,
            "recent_avg": recent_avg,
            "prev_avg": prev_avg,
            "consecutive_weak": consecutive_weak,
            "recent_effectiveness": recent_effectiveness,
            "details": (
                f"momentum: recent_avg={recent_avg:.2f}, prev_avg={_fmt_float(prev_avg)}, "
                f"consecutive_weak={consecutive_weak}/3"
            ),
        }

    # ── Signal 3: Bull/Bear Disagreement Increase ───────────────────────────

    async def _check_disagreement_trend(self) -> dict:
        """
        Check agent_step_log for Stage 4c cross_exam outputs where
        disagreement_count increases over time (per ticker, high disagreement).
        """
        today = date.today()
        eight_weeks_ago = today - timedelta(weeks=8)
        four_weeks_ago = today - timedelta(weeks=4)

        async with AsyncSessionLocal() as db:
            # Get recent cross_exam outputs with high disagreement
            result = await db.execute(
                select(AgentStepLog.output_data)
                .where(AgentStepLog.stage == "4c_cross_exam")
                .where(AgentStepLog.created_at >= eight_weeks_ago)
                .order_by(AgentStepLog.created_at.asc())
            )
            logs = result.scalars().all()

        if len(logs) < 2:
            return {"signal": "disagreement_trend", "triggered": False, "details": "insufficient cross_exam data"}

        # Count average disagreement items per week in recent 4 weeks vs prior 4 weeks
        def count_disagreements(output_data: dict | list) -> int:
            # cross_exam output has rebuttal dicts
            items = output_data or {}
            count = 0
            if isinstance(items, dict):
                values = items.values()
            elif isinstance(items, list):
                values = items
            else:
                return 0
            for v in values:
                if isinstance(v, dict):
                    args = v.get("arguments") or []
                    if isinstance(args, list) and len(args) > 2:
                        count += 1  # multi-argument = strong disagreement
            return count

        recent_window = logs[-4:]
        prev_window = logs[-8:-4] if len(logs) >= 8 else []
        recent_avg = sum(count_disagreements(log) for log in recent_window) / max(len(recent_window), 1)
        prev_avg = (
            sum(count_disagreements(log) for log in prev_window) / len(prev_window)
            if prev_window
            else 0
        )

        if recent_avg - prev_avg > 2.0:  # 2+ more disagreements = decay signal
            return {
                "signal": "disagreement_trend",
                "triggered": True,
                "recent_avg": recent_avg,
                "prev_avg": prev_avg,
                "details": f"disagreements: recent={recent_avg:.1f}, prev={prev_avg:.1f} → DECAY",
            }

        return {
            "signal": "disagreement_trend",
            "triggered": False,
            "recent_avg": recent_avg,
            "prev_avg": prev_avg,
            "details": f"disagreements stable: recent={recent_avg:.1f}, prev={prev_avg:.1f}",
        }

    # ── Signal 4: Regime Stability Collapse ─────────────────────────────────

    async def _check_regime_stability(self) -> dict:
        """
        Check for regime_shift=True in 3+ consecutive weeks (regime jumping).
        Also check if regime stability (from MemoryWeekly) is "volatile".
        """
        today = date.today()
        ten_weeks_ago = today - timedelta(weeks=10)

        async with AsyncSessionLocal() as db:
            result = await db.execute(
                select(MemoryWeekly)
                .where(MemoryWeekly.week_start >= ten_weeks_ago)
                .order_by(MemoryWeekly.week_start.asc())
            )
            weeks = result.scalars().all()

        if len(weeks) < 3:
            return {"signal": "regime_stability", "triggered": False, "details": "insufficient weeks"}

        # Check consecutive regime shifts
        recent_weeks = weeks[-4:]
        consecutive_shifts = 0
        for w in reversed(recent_weeks):
            if w.regime_shift:
                consecutive_shifts += 1
            else:
                break

        triggered = consecutive_shifts >= 3

        # Also check regime stability column if present
        stable_count = sum(1 for w in recent_weeks if w.regime_shift)
        regime_volatility = stable_count >= 3  # 3+ shifts in 4 weeks = volatile

        return {
            "signal": "regime_stability",
            "triggered": triggered or regime_volatility,
            "consecutive_shifts": consecutive_shifts,
            "details": (
                f"regime: {consecutive_shifts} consecutive shifts in recent 4 weeks "
                f"→ {'DECAY (unstable)' if triggered else 'ok'}"
            ),
        }

    async def _check_strategy_health_profiles(self) -> dict:
        """Check per-strategy/regime health profiles written by Playground."""
        async with AsyncSessionLocal() as db:
            result = await db.execute(
                select(SystemConfig).where(SystemConfig.key == "strategy_health_profiles")
            )
            cfg = result.scalar_one_or_none()

        if not cfg or not cfg.value:
            return {
                "signal": "strategy_health_decay",
                "triggered": False,
                "details": "no strategy health profiles",
            }

        decay_flags = (cfg.value or {}).get("decay_flags") or []
        triggered = bool(decay_flags)
        return {
            "signal": "strategy_health_decay",
            "triggered": triggered,
            "decay_flags": decay_flags,
            "approval_required": True,
            "details": (
                f"{len(decay_flags)} strategy/regime decay flags detected; "
                "parameter changes require approval"
                if triggered
                else "no strategy/regime health decay flags"
            ),
        }

    # ── Aggregation ─────────────────────────────────────────────────────────

    def _aggregate_signals(self, signals: list[dict]) -> DecayResult:
        """Aggregate all signals into decay_signal_strength and recommendation."""
        triggered_signals = [s for s in signals if s.get("triggered")]
        n_triggered = len(triggered_signals)
        n_signals = len(signals)

        # Determine decay strength
        if n_triggered >= 3:
            decay_signal_strength = "strong"
            recommendation = "strategy_refresh_recommendation"
        elif n_triggered == 2:
            decay_signal_strength = "moderate"
            recommendation = "strategy_refresh_recommendation"
        elif n_triggered == 1:
            decay_signal_strength = "weak"
            recommendation = None  # monitor but don't trigger
        else:
            decay_signal_strength = "none"
            recommendation = None

        # Confidence: more signals with more data = higher confidence
        confidence = min(1.0, n_triggered / max(n_signals, 1) * 1.5)

        # Affected regimes: infer from DQS trend if it was triggered
        affected_regimes = []
        for s in triggered_signals:
            if s.get("signal") == "dqs_trend":
                affected_regimes.append("decision_quality_decline")
            elif s.get("signal") == "momentum_trend":
                affected_regimes.append("momentum_decay")
            elif s.get("signal") == "disagreement_trend":
                affected_regimes.append("signal_conflict_increase")
            elif s.get("signal") == "regime_stability":
                affected_regimes.append("regime_instability")
            elif s.get("signal") == "strategy_health_decay":
                affected_regimes.extend(
                    f"{flag.get('strategy_name')}:{flag.get('regime')}"
                    for flag in (s.get("decay_flags") or [])[:5]
                )

        # Momentum trend direction
        mom_signal = next((s for s in signals if s.get("signal") == "momentum_trend"), None)
        if mom_signal and mom_signal.get("recent_avg") is not None:
            if mom_signal.get("prev_avg") is not None:
                if mom_signal["recent_avg"] > mom_signal["prev_avg"] + 0.3:
                    mom_effectiveness_trend = "improving"
                elif mom_signal["recent_avg"] < mom_signal["prev_avg"] - 0.3:
                    mom_effectiveness_trend = "declining"
                else:
                    mom_effectiveness_trend = "stable"
            else:
                mom_effectiveness_trend = "stable"
        else:
            mom_effectiveness_trend = "unknown"

        logger.info(
            f"[DECAY_DETECTOR] {n_triggered}/{n_signals} signals triggered | "
            f"strength={decay_signal_strength} | recommendation={recommendation}"
        )

        return DecayResult(
            decay_signal_strength=decay_signal_strength,
            confidence=round(confidence, 3),
            momentum_effectiveness_trend=mom_effectiveness_trend,
            affected_regimes=affected_regimes,
            recommendation=recommendation,
            details={
                "signals": signals,
                "n_triggered": n_triggered,
                "n_signals": n_signals,
            },
        )


# ─────────────────────────────── Convenience Functions ──────────────────────


async def evaluate_decay_signal() -> DecayResult:
    """Evaluate decay and write to system_config if recommendation triggers."""
    detector = DecayDetector()
    result = await detector.evaluate_decay()

    from db.queries import upsert_system_config
    from datetime import datetime

    async with AsyncSessionLocal() as db:
        await upsert_system_config(
            db,
            "decay_signal",
            {
                "decay_signal_strength": result.decay_signal_strength,
                "confidence": result.confidence,
                "momentum_effectiveness_trend": result.momentum_effectiveness_trend,
                "affected_regimes": result.affected_regimes,
                "recommendation": result.recommendation,
                "details": result.details,
                "evaluated_at": datetime.utcnow().isoformat(),
            },
            "decay_detector",
        )

    if result.recommendation:
        await tool_send_telegram({
            "text": (
                f"⚠️ DECAY_DETECTOR: Signal strength={result.decay_signal_strength} "
                f"(confidence={result.confidence:.0%}), "
                f"affected: {result.affected_regimes}. "
                f"Strategy refresh recommended."
            )
        })
        logger.warning(f"[DECAY_DETECTOR] Strategy refresh recommended: {result.affected_regimes}")

    return result
