# tracking/wandb_client.py
"""
PipelineRunTracker — W&B experiment tracking for Agentix pipeline runs.

All methods are safe: if WANDB_API_KEY is not set, methods are no-ops and log
a warning. The pipeline never fails due to tracking issues.
"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Callable, Awaitable

import wandb

from config import get_settings

if TYPE_CHECKING:
    from services.circuit_breaker import CircuitTransition, TriggerResult

logger = logging.getLogger("qc_fastapi_2.tracking.wandb_client")
settings = get_settings()


class PipelineRunTracker:
    """
    Thin W&B client wrapper. All methods return None and never raise.

    Usage:
        tracker = PipelineRunTracker()
        tracker.start_run(pipeline_context, regime_result)
        tracker.log_stage_metrics("3_researcher", duration_ms=26130,
                                  tokens={"prompt_tokens": 8456, "completion_tokens": 957})
        tracker.end_run(execution_status="success")
    """

    def __init__(self) -> None:
        self._api_key = settings.wandb_api_key
        self._disabled = not bool(self._api_key)
        self._run = None
        self._project = settings.wandb_project or "agentix"
        self._entity = settings.wandb_entity or None

        if self._disabled:
            logger.debug("[Tracker] WANDB_API_KEY not set — tracking disabled")
            return

        try:
            wandb.login(key=self._api_key)
            logger.debug(f"[Tracker] W&B configured: project={self._project}, entity={self._entity}")
        except Exception as e:
            logger.warning(f"[Tracker] Failed to login to W&B: {e} — tracking disabled")
            self._disabled = True

    @property
    def is_disabled(self) -> bool:
        return self._disabled

    def start_run(self, pipeline_context: dict, regime_result: dict | None = None) -> None:
        """Start a W&B run and log pipeline-level config."""
        if self._disabled:
            return

        try:
            run_name = f"pipeline_{pipeline_context.get('trigger', 'unknown')}"
            self._run = wandb.init(
                project=self._project,
                entity=self._entity,
                name=run_name,
                config={
                    "trigger": pipeline_context.get("trigger", ""),
                    "auth_mode": pipeline_context.get("auth_mode", ""),
                    "active_strategy": pipeline_context.get("active_strategy", ""),
                    "regime": (regime_result.get("regime", "") if regime_result else ""),
                    "regime_confidence": (regime_result.get("confidence", "") if regime_result else ""),
                },
                reinit=True,
            )
            logger.info(f"[Tracker] W&B run started: {self._run.name}")
        except Exception as e:
            logger.warning(f"[Tracker] start_run failed: {e}")
            self._disabled = True

    def log_stage_metrics(
        self,
        stage_name: str,
        duration_ms: int,
        tokens: dict | None = None,
        **kwargs: Any,
    ) -> None:
        """Log duration + optional token counts + extra metrics for a pipeline stage."""
        if self._disabled or self._run is None:
            return

        try:
            log_dict = {f"{stage_name}_duration_ms": duration_ms}

            if tokens and isinstance(tokens, dict):
                log_dict[f"{stage_name}_prompt_tokens"] = tokens.get("prompt_tokens", 0)
                log_dict[f"{stage_name}_completion_tokens"] = tokens.get("completion_tokens", 0)
                log_dict[f"{stage_name}_total_tokens"] = (
                    tokens.get("prompt_tokens", 0) + tokens.get("completion_tokens", 0)
                )

            for k, v in kwargs.items():
                if isinstance(v, (int, float)):
                    log_dict[f"{stage_name}_{k}"] = v
                elif isinstance(v, str):
                    self._run.config[f"{stage_name}_{k}"] = v

            self._run.log(log_dict)
        except Exception as e:
            logger.warning(f"[Tracker] log_stage_metrics({stage_name}) failed: {e}")

    def log_final_decision(
        self,
        synthesizer_out: dict,
        risk_out: dict,
    ) -> None:
        """Log the PM decision outcome after Stage 5/6."""
        if self._disabled or self._run is None:
            return

        try:
            mj = synthesizer_out.get("market_judgment") or {}
            if isinstance(mj, dict):
                self._run.config["final_regime"] = mj.get("regime", "")
                self._run.config["final_stance"] = synthesizer_out.get("recommended_stance", "")
                self._run.config["degraded"] = synthesizer_out.get("used_degraded_fallback", False)

            self._run.config["risk_approved"] = bool(risk_out.get("approved", False))
            self._run.config["execution_status"] = risk_out.get("execution_status", "unknown")

            weights = synthesizer_out.get("adjusted_weights") or {}
            if isinstance(weights, dict):
                top5 = dict(sorted(weights.items(), key=lambda x: x[1], reverse=True)[:5])
                self._run.config["top5_weights"] = str(top5)

            overlays = risk_out.get("overlays") or []
            if overlays:
                self._run.config["risk_overlays"] = str(overlays[:3])
        except Exception as e:
            logger.warning(f"[Tracker] log_final_decision failed: {e}")

    def end_run(self, execution_status: str, error_reason: str | None = None) -> None:
        """Finish the W&B run."""
        if self._disabled or self._run is None:
            return

        try:
            self._run.config["pipeline_status"] = execution_status
            if error_reason:
                self._run.config["error_reason"] = error_reason[:200]
            self._run.finish()
            logger.info(f"[Tracker] W&B run {self._run.name} ended: {execution_status}")
        except Exception as e:
            logger.warning(f"[Tracker] end_run failed: {e}")

    # ── Phase D: Circuit + Retry telemetry ─────────────────────────────────

    def log_circuit_transition(self, transition: "CircuitTransition") -> None:
        """Log circuit breaker state transition as W&B config + metric."""
        if self._disabled or self._run is None:
            return
        try:
            self._run.config["circuit_from_state"]       = transition.from_state.value
            self._run.config["circuit_to_state"]         = transition.to_state.value
            self._run.config["circuit_primary_trigger"]   = transition.primary_trigger
            self._run.config["circuit_reason"]           = (transition.reason or "")[:200]
            self._run.log({
                "circuit_escalated": int(
                    transition.to_state.value in ("ALERT", "DEFENSIVE")
                )
            })
        except Exception as e:
            logger.warning(f"[Tracker] log_circuit_transition failed: {e}")

    def log_trigger_results(self, trigger_results: "list[TriggerResult]") -> None:
        """Log per-trigger metric values from circuit breaker evaluation."""
        if self._disabled or self._run is None:
            return
        try:
            log_dict: dict[str, Any] = {}
            for tr in trigger_results:
                if isinstance(tr.value, (int, float)):
                    log_dict[f"trigger_{tr.name}_value"] = tr.value
                    log_dict[f"trigger_{tr.name}_fired"] = int(tr.triggered)
            if log_dict:
                self._run.log(log_dict)
        except Exception as e:
            logger.warning(f"[Tracker] log_trigger_results failed: {e}")

    def log_retry_event(
        self,
        service_name: str,
        attempt: int,
        error_type: str,
    ) -> None:
        """Log a retry/degraded-fallback event. Accumulates per-service counts."""
        if self._disabled or self._run is None:
            return
        try:
            counter_key = f"retry_{service_name}_attempts"
            existing = self._run.summary.get(counter_key, 0)
            self._run.summary[counter_key] = existing + 1
            self._run.log({f"retry_{service_name}_attempt": attempt})
            self._run.config[f"retry_{service_name}_last_error"] = error_type[:80]
        except Exception as e:
            logger.warning(f"[Tracker] log_retry_event failed: {e}")

    def make_retry_callback(
        self,
        service_name: str,
    ) -> Callable[[int, Exception], Awaitable[None]]:
        """Return an async on_retry callback suitable for with_retry(on_retry=...)."""
        async def _cb(attempt: int, error: Exception) -> None:
            self.log_retry_event(service_name, attempt, type(error).__name__)
        return _cb