# services/circuit_breaker.py
"""
4-state circuit breaker with market-signal-driven triggers.

Uses the existing 3-state model (CLOSED/ALERT/DEFENSIVE) with 5 trigger monitors:
    1. VIX spike          — >30 → ALERT, >40 → DEFENSIVE (instant)
    2. Portfolio drawdown — >10% → ALERT (rolling 1h)
    3. Consecutive rejections — >3 in 2h → ALERT
    4. LLM failure rate   — >50% in 1h → ALERT
    5. Persistent ALERT   — ALERT > 2h → auto-escalate to DEFENSIVE

State transitions:
    CLOSED → ALERT   : any trigger condition fires
    ALERT → DEFENSIVE: escalation trigger OR ALERT persists > 2h
    ALERT → CLOSED   : all triggers clear + 30 min cooldown
    DEFENSIVE → ALERT: trigger de-escalates (VIX drops, drawdown recovers)
    Any → CLOSED     : human command only (via /reset_circuit)

Data sources:
    - VIX:              system_config.last_vix
    - Drawdown:         portfolio_timeseries.current_drawdown_pct
    - Rejections:       agent_analysis (execution_status filter)
    - LLM failures:     agent_step_log (failed=True per LLM stage)
    - Circuit state:    system_config.circuit_state
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, date
from enum import Enum
from typing import Optional

import asyncio
from sqlalchemy import select, func

from db.session import AsyncSessionLocal
from db.queries import get_system_config, upsert_system_config
from db.models import AgentAnalysis, AgentStepLog, PortfolioTimeseries
from tools.notify_tools import tool_send_telegram
from config import get_settings
from services.operator_messages import format_circuit_state_change_message

logger = logging.getLogger("qc_fastapi_2.circuit_breaker")
settings = get_settings()

# ─────────────────────────────── State Enum ────────────────────────────────


class CircuitState(str, Enum):
    CLOSED = "CLOSED"    # Normal operation
    ALERT = "ALERT"      # Degraded vigilance
    DEFENSIVE = "DEFENSIVE"  # Protected mode


class CircuitTriggerClass(str, Enum):
    MARKET_RISK = "market_risk"
    ACCOUNT_RISK = "account_risk"
    EXECUTION_RISK = "execution_risk"
    CONTROL_PLANE = "control_plane"
    DIAGNOSTICS = "diagnostics"
    DERIVED = "derived"
    TECHNICAL = "technical"


TRIGGER_CLASS_MAP = {
    "vix": CircuitTriggerClass.MARKET_RISK,
    "vix_spike": CircuitTriggerClass.MARKET_RISK,
    "drawdown": CircuitTriggerClass.MARKET_RISK,
    "drawdown_threshold": CircuitTriggerClass.MARKET_RISK,
    "account_state_stale": CircuitTriggerClass.ACCOUNT_RISK,
    "account_state_guard_failure": CircuitTriggerClass.ACCOUNT_RISK,
    "account_stale": CircuitTriggerClass.ACCOUNT_RISK,
    "holdings_mismatch": CircuitTriggerClass.ACCOUNT_RISK,
    "rejections": CircuitTriggerClass.EXECUTION_RISK,
    "consecutive_qc_rejects": CircuitTriggerClass.EXECUTION_RISK,
    "policy_mismatch_timeout": CircuitTriggerClass.CONTROL_PLANE,
    "policy_sync_recovery_exhausted": CircuitTriggerClass.CONTROL_PLANE,
    "policy_version_mismatch": CircuitTriggerClass.CONTROL_PLANE,
    "llm_failure": CircuitTriggerClass.DIAGNOSTICS,
    "persistent_alert": CircuitTriggerClass.DERIVED,
}

STICKY_DEFENSIVE_TRIGGER_CLASSES = {
    CircuitTriggerClass.MARKET_RISK,
    CircuitTriggerClass.ACCOUNT_RISK,
}


# ─────────────────────────────── Dataclasses ───────────────────────────────


@dataclass
class TriggerResult:
    name: str
    value: float
    threshold: float
    triggered: bool
    direction: str  # "escalate" | "deescalate" | "clear" | "none"
    details: str = ""


@dataclass
class CircuitTransition:
    from_state: CircuitState
    to_state: CircuitState
    reason: str
    primary_trigger: str
    all_trigger_results: list[TriggerResult] = field(default_factory=list)


@dataclass
class CircuitHealthReport:
    has_issues: bool
    current_state: str
    issues: list[str]  # human-readable issue list
    triggers: list[TriggerResult]


# ─────────────────────────────── Cooldown / Window Defaults ────────────────

@dataclass
class CircuitConfig:
    vix_alert_threshold: float = 30.0
    vix_defensive_threshold: float = 40.0
    drawdown_alert_threshold: float = 0.10
    rejection_window_hours: int = 2
    rejection_count_threshold: int = 3
    llm_failure_window_hours: int = 1
    llm_failure_rate_threshold: float = 0.50
    cooldown_minutes: int = 30
    persistent_alert_hours: int = 2


def _load_config() -> CircuitConfig:
    """Load circuit breaker thresholds from settings (with defaults)."""
    return CircuitConfig(
        vix_alert_threshold=settings.vix_alert_threshold,
        vix_defensive_threshold=settings.vix_defensive_threshold,
        drawdown_alert_threshold=settings.drawdown_alert_threshold,
        rejection_window_hours=settings.rejection_window_hours,
        rejection_count_threshold=settings.rejection_count_threshold,
        llm_failure_window_hours=settings.llm_failure_window_hours,
        llm_failure_rate_threshold=settings.llm_failure_rate_threshold,
        cooldown_minutes=getattr(settings, "circuit_cooldown_minutes", 30),
        persistent_alert_hours=getattr(settings, "persistent_alert_hours", 2),
    )


# ─────────────────────────────── CircuitBreakerMonitor ────────────────────


class CircuitBreakerMonitor:
    """
    Evaluates all 5 trigger conditions and manages state transitions.
    Call evaluate_triggers() at the start of each pipeline run (before Stage 0).
    """

    def __init__(self, config: Optional[CircuitConfig] = None):
        self.config = config or _load_config()

    # ── Public Entry ─────────────────────────────────────────────────────────

    async def evaluate_triggers(self) -> Optional[CircuitTransition]:
        """
        Evaluate all 5 trigger conditions against current circuit state.
        Returns CircuitTransition if state should change, else None.

        Order: always check all triggers, but determine direction from
        current state + trigger severity.
        """
        async with AsyncSessionLocal() as db:
            circuit_cfg = await get_system_config(db, "circuit_state")

        current = self._parse_state(circuit_cfg)

        # Evaluate all triggers
        trigger_results = await self._evaluate_all_triggers()

        # Determine desired next state
        next_state, primary_trigger, reason = self._compute_next_state(
            current, trigger_results, circuit_cfg
        )

        if next_state == current:
            return None  # No transition needed

        return CircuitTransition(
            from_state=current,
            to_state=next_state,
            reason=reason,
            primary_trigger=primary_trigger,
            all_trigger_results=trigger_results,
        )

    async def run_health_check(self) -> CircuitHealthReport:
        """Called by morning_health cron — returns current circuit health."""
        trigger_results = await self._evaluate_all_triggers()

        async with AsyncSessionLocal() as db:
            circuit_cfg = await get_system_config(db, "circuit_state")
        current_state = self._parse_state(circuit_cfg)

        issues = []
        for tr in trigger_results:
            if tr.triggered and tr.direction in ("escalate",):
                issues.append(f"{tr.name}: value={tr.value:.2f}, threshold={tr.threshold:.2f}")

        return CircuitHealthReport(
            has_issues=len(issues) > 0,
            current_state=current_state.value,
            issues=issues,
            triggers=trigger_results,
        )

    async def record_llm_failure(self, stage_name: str) -> None:
        """Called after an LLM stage fails — log to system_config for failure rate tracking."""
        async with AsyncSessionLocal() as db:
            fail_cfg = await get_system_config(db, "llm_failure_log")
            failures = (fail_cfg.value if fail_cfg else {}) or {}

            now = datetime.utcnow().isoformat()
            window_h = self.config.llm_failure_window_hours

            # Prune old entries
            cutoff = (datetime.utcnow() - timedelta(hours=window_h)).isoformat()
            failures = {k: v for k, v in failures.items() if k > cutoff}
            failures[now] = stage_name

            await upsert_system_config(db, "llm_failure_log", failures, "circuit_breaker")

    async def record_rejection(self, analysis_id: int) -> None:
        """Called after RISK MGR rejects a proposal — log for rejection rate tracking."""
        async with AsyncSessionLocal() as db:
            rej_cfg = await get_system_config(db, "rejection_log")
            rejections = (rej_cfg.value if rej_cfg else {}) or {}

            now = datetime.utcnow().isoformat()
            window_h = self.config.rejection_window_hours

            cutoff = (datetime.utcnow() - timedelta(hours=window_h)).isoformat()
            rejections = {k: v for k, v in rejections.items() if k > cutoff}
            rejections[now] = analysis_id

            await upsert_system_config(db, "rejection_log", rejections, "circuit_breaker")

    async def update_circuit_state(
        self,
        new_state: CircuitState,
        reason: str,
        primary_trigger: str = "",
    ) -> None:
        """Write new state to system_config and send Telegram alert."""
        async with AsyncSessionLocal() as db:
            await upsert_system_config(
                db,
                "circuit_state",
                {
                    "value": new_state.value,
                    "reason": reason,
                    "primary_trigger": primary_trigger,
                    "updated_at": datetime.utcnow().isoformat(),
                },
                "circuit_breaker",
            )

        await tool_send_telegram({
            "text": format_circuit_state_change_message(
                state=new_state.value,
                reason=reason,
                primary_trigger=primary_trigger,
            )
        })

        logger.warning(f"[circuit_breaker] State transition: {new_state.value} | {reason}")

    # ── Internal Trigger Evaluators ──────────────────────────────────────────

    async def _evaluate_all_triggers(self) -> list[TriggerResult]:
        results = await asyncio.gather(
            self._check_vix_trigger(),
            self._check_drawdown_trigger(),
            self._check_rejection_trigger(),
            self._check_llm_failure_trigger(),
            self._check_persistent_alert_trigger(),
            return_exceptions=True,
        )
        # Filter out exceptions (logged internally), keep TriggerResult
        return [r for r in results if isinstance(r, TriggerResult)]

    async def _check_vix_trigger(self) -> TriggerResult:
        """VIX > 30 → ALERT, VIX > 40 → DEFENSIVE. Instant evaluation."""
        vix = await self._get_current_vix()
        cfg = self.config

        if vix is None:
            return TriggerResult(name="vix", value=0, threshold=cfg.vix_alert_threshold,
                                 triggered=False, direction="none",
                                 details="no VIX data available")

        if vix > cfg.vix_defensive_threshold:
            return TriggerResult(
                name="vix", value=vix, threshold=cfg.vix_defensive_threshold,
                triggered=True, direction="escalate",
                details=f"VIX={vix:.1f} > {cfg.vix_defensive_threshold} → DEFENSIVE"
            )
        elif vix > cfg.vix_alert_threshold:
            return TriggerResult(
                name="vix", value=vix, threshold=cfg.vix_alert_threshold,
                triggered=True, direction="escalate",
                details=f"VIX={vix:.1f} > {cfg.vix_alert_threshold} → ALERT"
            )
        else:
            return TriggerResult(
                name="vix", value=vix, threshold=cfg.vix_alert_threshold,
                triggered=False, direction="clear",
                details=f"VIX={vix:.1f} within normal range"
            )

    async def _check_drawdown_trigger(self) -> TriggerResult:
        """Portfolio drawdown > threshold → ALERT."""
        drawdown = await self._get_current_drawdown()
        cfg = self.config

        if drawdown is None:
            return TriggerResult(name="drawdown", value=0, threshold=cfg.drawdown_alert_threshold,
                                 triggered=False, direction="none",
                                 details="no drawdown data available")

        if drawdown > cfg.drawdown_alert_threshold:
            return TriggerResult(
                name="drawdown", value=drawdown, threshold=cfg.drawdown_alert_threshold,
                triggered=True, direction="escalate",
                details=f"Drawdown={drawdown:.2%} > {cfg.drawdown_alert_threshold:.2%} → ALERT"
            )
        else:
            return TriggerResult(
                name="drawdown", value=drawdown, threshold=cfg.drawdown_alert_threshold,
                triggered=False, direction="clear",
                details=f"Drawdown={drawdown:.2%} within normal range"
            )

    async def _check_rejection_trigger(self) -> TriggerResult:
        """>N consecutive RISK MGR rejections in window → ALERT."""
        count = await self._get_rejection_count()
        cfg = self.config

        if count is None:
            return TriggerResult(name="rejections", value=0, threshold=cfg.rejection_count_threshold,
                                 triggered=False, direction="none",
                                 details="no rejection data available")

        if count > cfg.rejection_count_threshold:
            return TriggerResult(
                name="rejections", value=count, threshold=cfg.rejection_count_threshold,
                triggered=True, direction="escalate",
                details=f"{count} rejections in {cfg.rejection_window_hours}h window → ALERT"
            )
        else:
            return TriggerResult(
                name="rejections", value=count, threshold=cfg.rejection_count_threshold,
                triggered=False, direction="clear",
                details=f"{count} rejections in {cfg.rejection_window_hours}h window (threshold={cfg.rejection_count_threshold})"
            )

    async def _check_llm_failure_trigger(self) -> TriggerResult:
        """>50% LLM stage failures in 1h window → ALERT."""
        rate, total, failed = await self._get_llm_failure_rate()
        cfg = self.config

        if rate is None:
            return TriggerResult(name="llm_failure", value=0, threshold=cfg.llm_failure_rate_threshold,
                                 triggered=False, direction="none",
                                 details="no LLM failure data available")

        if rate > cfg.llm_failure_rate_threshold:
            return TriggerResult(
                name="llm_failure", value=rate, threshold=cfg.llm_failure_rate_threshold,
                triggered=True, direction="escalate",
                details=f"LLM failure rate={rate:.0%} ({failed}/{total}) in {cfg.llm_failure_window_hours}h → ALERT"
            )
        else:
            return TriggerResult(
                name="llm_failure", value=rate, threshold=cfg.llm_failure_rate_threshold,
                triggered=False, direction="clear",
                details=f"LLM failure rate={rate:.0%} ({failed}/{total}) in {cfg.llm_failure_window_hours}h window"
            )

    async def _check_persistent_alert_trigger(self) -> TriggerResult:
        """ALERT state > persistent_alert_hours without de-escalation → auto-escalate to DEFENSIVE."""
        async with AsyncSessionLocal() as db:
            circuit_cfg = await get_system_config(db, "circuit_state")

        current = self._parse_state(circuit_cfg)
        if current != CircuitState.ALERT:
            return TriggerResult(name="persistent_alert", value=0, threshold=self.config.persistent_alert_hours,
                                 triggered=False, direction="none",
                                 details=f"current state={current.value}, not ALERT")

        cfg = circuit_cfg.value if circuit_cfg else {}
        updated_at_str = cfg.get("updated_at", "")
        if not updated_at_str:
            return TriggerResult(name="persistent_alert", value=0, threshold=self.config.persistent_alert_hours,
                                 triggered=False, direction="none",
                                 details="no timestamp for ALERT start")

        try:
            alert_start = datetime.fromisoformat(updated_at_str)
        except (ValueError, TypeError):
            return TriggerResult(name="persistent_alert", value=0, threshold=self.config.persistent_alert_hours,
                                 triggered=False, direction="none",
                                 details=f"invalid timestamp: {updated_at_str}")

        hours_in_alert = (datetime.utcnow() - alert_start).total_seconds() / 3600

        if hours_in_alert > self.config.persistent_alert_hours:
            return TriggerResult(
                name="persistent_alert", value=hours_in_alert,
                threshold=float(self.config.persistent_alert_hours),
                triggered=True, direction="escalate",
                details=f"ALERT persisted {hours_in_alert:.1f}h > {self.config.persistent_alert_hours}h → DEFENSIVE"
            )
        else:
            return TriggerResult(
                name="persistent_alert", value=hours_in_alert,
                threshold=float(self.config.persistent_alert_hours),
                triggered=False, direction="none",
                details=f"ALERT for {hours_in_alert:.1f}h (threshold={self.config.persistent_alert_hours}h)"
            )

    # ── Data Access Helpers ───────────────────────────────────────────────────

    async def _get_current_vix(self) -> Optional[float]:
        try:
            async with AsyncSessionLocal() as db:
                cfg = await get_system_config(db, "last_vix")
            if cfg:
                return float((cfg.value or {}).get("value", 0) or 0)
            # Fallback: most recent portfolio_timeseries
            async with AsyncSessionLocal() as db:
                result = await db.execute(
                    select(PortfolioTimeseries.vix)
                    .order_by(PortfolioTimeseries.recorded_at.desc())
                    .limit(1)
                )
                v = result.scalar_one_or_none()
                return float(v) if v is not None else None
        except Exception as e:
            logger.warning(f"[circuit_breaker] Failed to get VIX: {e}")
            return None

    async def _get_current_drawdown(self) -> Optional[float]:
        try:
            async with AsyncSessionLocal() as db:
                result = await db.execute(
                    select(PortfolioTimeseries.current_drawdown_pct)
                    .order_by(PortfolioTimeseries.recorded_at.desc())
                    .limit(1)
                )
                dd = result.scalar_one_or_none()
                return abs(float(dd)) if dd is not None else None
        except Exception as e:
            logger.warning(f"[circuit_breaker] Failed to get drawdown: {e}")
            return None

    async def _get_rejection_count(self) -> Optional[int]:
        """Count RISK MGR rejections in the configured time window."""
        try:
            window_h = self.config.rejection_window_hours
            cutoff = datetime.utcnow() - timedelta(hours=window_h)
            async with AsyncSessionLocal() as db:
                # Also check rejection_log from system_config as secondary source
                rej_cfg = await get_system_config(db, "rejection_log")
                rejections = (rej_cfg.value if rej_cfg else {}) or {}

                # Count rejections from log (more reliable for counting)
                cutoff_str = cutoff.isoformat()
                count = sum(1 for k in rejections if k > cutoff_str)
                return count
        except Exception as e:
            logger.warning(f"[circuit_breaker] Failed to get rejection count: {e}")
            return 0

    async def _get_llm_failure_rate(self) -> tuple[Optional[float], int, int]:
        """
        Returns (rate, total_stages, failed_stages) for the LLM failure rate trigger.
        Counts LLM stages from agent_step_log in the configured window.
        """
        try:
            window_h = self.config.llm_failure_window_hours
            cutoff = datetime.utcnow() - timedelta(hours=window_h)
            async with AsyncSessionLocal() as db:
                result = await db.execute(
                    select(func.count(AgentStepLog.id))
                    .where(AgentStepLog.created_at >= cutoff)
                    .where(AgentStepLog.model.isnot(None))  # LLM stages have model set
                )
                total = result.scalar_one_or_none() or 0

                result_fail = await db.execute(
                    select(func.count(AgentStepLog.id))
                    .where(AgentStepLog.created_at >= cutoff)
                    .where(AgentStepLog.model.isnot(None))
                    .where(AgentStepLog.failed == True)
                )
                failed = result_fail.scalar_one_or_none() or 0

                if total == 0:
                    return None, 0, 0
                return failed / total, total, failed
        except Exception as e:
            logger.warning(f"[circuit_breaker] Failed to get LLM failure rate: {e}")
            return None, 0, 0

    # ── State Machine Logic ───────────────────────────────────────────────────

    def _parse_state(self, circuit_cfg) -> CircuitState:
        """Parse circuit state from system_config."""
        if circuit_cfg:
            val = (circuit_cfg.value or {}).get("value", "CLOSED")
            try:
                return CircuitState(val)
            except ValueError:
                return CircuitState.CLOSED
        return CircuitState.CLOSED

    def _compute_next_state(
        self,
        current: CircuitState,
        trigger_results: list[TriggerResult],
        circuit_cfg,
    ) -> tuple[CircuitState, str, str]:
        """
        Determine the next circuit state based on current state + trigger results.

        Returns: (next_state, primary_trigger_name, reason_str)
        """
        escalations = [tr for tr in trigger_results if tr.triggered and tr.direction == "escalate"]
        deescalations = [tr for tr in trigger_results if tr.triggered and tr.direction == "deescalate"]
        clears = [tr for tr in trigger_results if tr.direction == "clear"]

        # CLOSED → ALERT: any escalation trigger fires
        if current == CircuitState.CLOSED:
            if escalations:
                tr = max(escalations, key=lambda t: t.value)  # worst trigger
                return CircuitState.ALERT, tr.name, tr.details

        # ALERT → DEFENSIVE or CLOSED. Keep this in one branch so the
        # cooldown close path remains reachable after escalation checks.
        elif current == CircuitState.ALERT:
            persistent_tr = next((t for t in trigger_results if t.name == "persistent_alert" and t.triggered), None)
            if persistent_tr and self._persistent_alert_can_escalate_to_defensive(circuit_cfg):
                return CircuitState.DEFENSIVE, "persistent_alert", persistent_tr.details
            if escalations:
                # Only escalate to DEFENSIVE if VIX > 40 or rejection count very high
                vix_tr = next((t for t in trigger_results if t.name == "vix" and t.triggered), None)
                if vix_tr and "DEFENSIVE" in vix_tr.details:
                    return CircuitState.DEFENSIVE, vix_tr.name, vix_tr.details
                rej_tr = next((t for t in trigger_results if t.name == "rejections" and t.triggered), None)
                if rej_tr and rej_tr.value > self.config.rejection_count_threshold + 2:
                    return CircuitState.DEFENSIVE, rej_tr.name, rej_tr.details

            # ALERT → CLOSED: no active escalation trigger remains and
            # cooldown elapsed. `persistent_alert` reports direction="none"
            # while ALERT is young, so requiring every trigger to be
            # direction="clear" would keep ALERT sticky forever.
            active_escalations = [
                t for t in trigger_results
                if t.triggered and t.direction == "escalate" and t.name != "persistent_alert"
            ]
            if not active_escalations and clears:
                # Check cooldown
                cfg = circuit_cfg.value if circuit_cfg else {}
                updated_at_str = cfg.get("updated_at", "")
                if updated_at_str:
                    try:
                        state_start = datetime.fromisoformat(updated_at_str)
                        elapsed = datetime.utcnow() - state_start
                        if elapsed.total_seconds() >= self.config.cooldown_minutes * 60:
                            return CircuitState.CLOSED, "all_clear", "All triggers cleared, cooldown elapsed"
                    except (ValueError, TypeError):
                        pass
                else:
                    # No timestamp — safe to close
                    return CircuitState.CLOSED, "all_clear", "All triggers cleared, no timestamp"

        # DEFENSIVE → ALERT: VIX drops below alert threshold and drawdown recovers
        elif current == CircuitState.DEFENSIVE:
            vix_tr = next((t for t in trigger_results if t.name == "vix"), None)
            dd_tr = next((t for t in trigger_results if t.name == "drawdown"), None)
            if vix_tr and not vix_tr.triggered and vix_tr.direction == "clear":
                if dd_tr and not dd_tr.triggered:
                    return CircuitState.ALERT, "deescalation", f"VIX={vix_tr.value:.1f} dropped, drawdown={dd_tr.value:.2%} recovered → ALERT"

        return current, "", "no state change"

    def _persistent_alert_can_escalate_to_defensive(self, circuit_cfg) -> bool:
        cfg = circuit_cfg.value if circuit_cfg else {}
        original_trigger = str((cfg or {}).get("primary_trigger") or "").strip()
        trigger_class = TRIGGER_CLASS_MAP.get(original_trigger, CircuitTriggerClass.TECHNICAL)
        return trigger_class in STICKY_DEFENSIVE_TRIGGER_CLASSES

    async def get_current_state(self) -> CircuitState:
        async with AsyncSessionLocal() as db:
            circuit_cfg = await get_system_config(db, "circuit_state")
        return self._parse_state(circuit_cfg)


# ─────────────────────────────── Convenience Functions ──────────────────────


async def evaluate_and_apply() -> Optional[CircuitTransition]:
    """Evaluate triggers and apply state transition if needed. Returns the transition or None."""
    monitor = CircuitBreakerMonitor()
    transition = await monitor.evaluate_triggers()
    if transition:
        await monitor.update_circuit_state(
            transition.to_state,
            transition.reason,
            transition.primary_trigger,
        )
    return transition


async def record_stage_failure(stage_name: str) -> None:
    """Called by pipeline when an LLM stage fails."""
    monitor = CircuitBreakerMonitor()
    await monitor.record_llm_failure(stage_name)


async def record_rejection_event(analysis_id: int) -> None:
    """Called by pipeline when RISK MGR rejects."""
    monitor = CircuitBreakerMonitor()
    await monitor.record_rejection(analysis_id)
