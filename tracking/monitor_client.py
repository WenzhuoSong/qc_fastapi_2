# tracking/monitor_client.py
"""
PipelineRunTracker — local monitor-compatible pipeline telemetry.

The system monitor uses local database artifacts: AgentStepLog for per-stage
telemetry and AgentAnalysis.risk_output for final decision state. This class
keeps the pipeline call sites stable while avoiding external tracking services.
"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Callable, Awaitable

if TYPE_CHECKING:
    from services.circuit_breaker import CircuitTransition, TriggerResult

logger = logging.getLogger("qc_fastapi_2.tracking.monitor")


class PipelineRunTracker:
    """
    Local tracker facade.

    Stage durations, token usage, failures, and final decisions are persisted by
    the pipeline's AgentStepLog/AgentAnalysis writes. Methods here intentionally
    never raise and never require network credentials.
    """

    def __init__(self) -> None:
        self._disabled = False
        self._run_context: dict[str, Any] = {}
        self._stage_metrics: list[dict[str, Any]] = []
        self._events: list[dict[str, Any]] = []
        self._final: dict[str, Any] = {}

    @property
    def is_disabled(self) -> bool:
        return self._disabled

    def start_run(self, pipeline_context: dict, regime_result: dict | None = None) -> None:
        self._run_context = {
            "trigger": pipeline_context.get("trigger", ""),
            "auth_mode": pipeline_context.get("auth_mode", ""),
            "active_strategy": pipeline_context.get("active_strategy", ""),
            "regime": (regime_result.get("regime", "") if regime_result else ""),
            "regime_confidence": (regime_result.get("confidence", "") if regime_result else ""),
        }

    def log_stage_metrics(
        self,
        stage_name: str,
        duration_ms: int,
        tokens: dict | None = None,
        **kwargs: Any,
    ) -> None:
        metric: dict[str, Any] = {
            "stage": stage_name,
            "duration_ms": duration_ms,
        }
        if tokens and isinstance(tokens, dict):
            prompt = int(tokens.get("prompt_tokens") or 0)
            completion = int(tokens.get("completion_tokens") or 0)
            metric["prompt_tokens"] = prompt
            metric["completion_tokens"] = completion
            metric["total_tokens"] = prompt + completion
        metric.update({k: v for k, v in kwargs.items() if isinstance(v, (int, float, str, bool))})
        self._stage_metrics.append(metric)

    def log_final_decision(
        self,
        synthesizer_out: dict,
        risk_out: dict,
    ) -> None:
        market = synthesizer_out.get("market_judgment") or {}
        self._final = {
            "final_regime": market.get("regime", ""),
            "final_stance": synthesizer_out.get("recommended_stance", ""),
            "degraded": bool(synthesizer_out.get("used_degraded_fallback", False)),
            "risk_approved": bool(risk_out.get("approved", False)),
            "execution_status": risk_out.get("execution_status", "unknown"),
            "target_construction_mode": risk_out.get("target_construction_mode"),
            "raw_llm_adjusted_weights_consumed": risk_out.get("raw_llm_adjusted_weights_consumed"),
        }

    def end_run(self, execution_status: str, error_reason: str | None = None) -> None:
        self._final["pipeline_status"] = execution_status
        if error_reason:
            self._final["error_reason"] = error_reason[:200]
        logger.debug(
            "[Tracker] local monitor telemetry finalized | status=%s stages=%d events=%d",
            execution_status,
            len(self._stage_metrics),
            len(self._events),
        )

    def log_circuit_transition(self, transition: "CircuitTransition") -> None:
        self._events.append({
            "type": "circuit_transition",
            "from_state": transition.from_state.value,
            "to_state": transition.to_state.value,
            "primary_trigger": transition.primary_trigger,
            "reason": transition.reason,
        })

    def log_trigger_results(self, trigger_results: "list[TriggerResult]") -> None:
        for tr in trigger_results:
            self._events.append({
                "type": "trigger_result",
                "name": tr.name,
                "value": tr.value,
                "triggered": bool(tr.triggered),
            })

    def log_retry_event(
        self,
        service_name: str,
        attempt: int,
        error_type: str,
    ) -> None:
        self._events.append({
            "type": "retry",
            "service_name": service_name,
            "attempt": attempt,
            "error_type": error_type,
        })

    def make_retry_callback(
        self,
        service_name: str,
    ) -> Callable[[int, Exception], Awaitable[None]]:
        async def _cb(attempt: int, error: Exception) -> None:
            self.log_retry_event(service_name, attempt, type(error).__name__)
        return _cb
