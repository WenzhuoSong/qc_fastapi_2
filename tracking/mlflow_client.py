# tracking/mlflow_client.py
"""
PipelineRunTracker — MLflow experiment tracking for Agentix pipeline runs.

All methods are safe: if MLFLOW_TRACKING_URI is not set or the server is
unreachable, methods are no-ops and log a warning. The pipeline never fails
due to tracking issues.
"""
from __future__ import annotations

import logging
from typing import Any

import mlflow

from config import get_settings

logger = logging.getLogger("qc_fastapi_2.tracking.mlflow_client")
settings = get_settings()


class PipelineRunTracker:
    """
    Thin MLflow client wrapper. All methods return None and never raise.

    Usage:
        tracker = PipelineRunTracker()
        tracker.start_run(pipeline_context, regime_result)
        tracker.log_stage_metrics("3_researcher", duration_ms=26130,
                                  tokens={"prompt_tokens": 8456, "completion_tokens": 957})
        tracker.end_run(execution_status="success")
    """

    def __init__(self) -> None:
        self._uri = settings.mlflow_tracking_uri
        self._disabled = not bool(self._uri)
        self._run_id: str | None = None
        self._experiment_name = settings.mlflow_experiment_name or "agentix"

        if self._disabled:
            logger.debug("[Tracker] MLFLOW_TRACKING_URI not set — tracking disabled")
            return

        try:
            mlflow.set_tracking_uri(self._uri)
            mlflow.set_experiment(self._experiment_name)
            logger.debug(f"[Tracker] MLflow configured: uri={self._uri}, experiment={self._experiment_name}")
        except Exception as e:
            logger.warning(f"[Tracker] Failed to configure MLflow: {e} — tracking disabled")
            self._disabled = True

    @property
    def is_disabled(self) -> bool:
        return self._disabled

    def start_run(self, pipeline_context: dict, regime_result: dict | None = None) -> None:
        """Start an MLflow run and log pipeline-level params."""
        if self._disabled:
            return

        try:
            mlflow.start_run(run_name=f"pipeline_{pipeline_context.get('trigger', 'unknown')}")
            self._run_id = mlflow.active_run().info.run_id

            mlflow.log_param("trigger", pipeline_context.get("trigger", ""))
            mlflow.log_param("auth_mode", pipeline_context.get("auth_mode", ""))
            mlflow.log_param("active_strategy", pipeline_context.get("active_strategy", ""))

            if regime_result and isinstance(regime_result, dict):
                mlflow.log_param("regime", regime_result.get("regime", ""))
                mlflow.log_param("regime_confidence", regime_result.get("confidence", ""))

            logger.info(f"[Tracker] MLflow run started: {self._run_id}")
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
        if self._disabled:
            return

        try:
            mlflow.log_metric(f"{stage_name}_duration_ms", duration_ms, step=0)

            if tokens and isinstance(tokens, dict):
                mlflow.log_metric(f"{stage_name}_prompt_tokens", tokens.get("prompt_tokens", 0), step=0)
                mlflow.log_metric(f"{stage_name}_completion_tokens", tokens.get("completion_tokens", 0), step=0)
                mlflow.log_metric(f"{stage_name}_total_tokens",
                                  tokens.get("prompt_tokens", 0) + tokens.get("completion_tokens", 0), step=0)

            for k, v in kwargs.items():
                if isinstance(v, (int, float)):
                    mlflow.log_metric(f"{stage_name}_{k}", v, step=0)
                elif isinstance(v, str):
                    mlflow.log_param(f"{stage_name}_{k}", v)
        except Exception as e:
            logger.warning(f"[Tracker] log_stage_metrics({stage_name}) failed: {e}")

    def log_final_decision(
        self,
        synthesizer_out: dict,
        risk_out: dict,
    ) -> None:
        """Log the PM decision outcome after Stage 5/6."""
        if self._disabled:
            return

        try:
            mj = synthesizer_out.get("market_judgment") or {}
            if isinstance(mj, dict):
                mlflow.log_param("final_regime", mj.get("regime", ""))
                mlflow.log_param("final_stance", synthesizer_out.get("recommended_stance", ""))
                mlflow.log_param("degraded", synthesizer_out.get("used_degraded_fallback", False))

            mlflow.log_param("risk_approved", risk_out.get("approved", False))
            mlflow.log_param("execution_status", risk_out.get("execution_status", "unknown"))

            # Log top-5 adjusted weights as a JSON param (readable in MLflow UI)
            weights = synthesizer_out.get("adjusted_weights", {})
            if isinstance(weights, dict):
                top5 = dict(sorted(weights.items(), key=lambda x: x[1], reverse=True)[:5])
                mlflow.log_param("top5_weights", str(top5))

            overlays = risk_out.get("overlays") or []
            if overlays:
                mlflow.log_param("risk_overlays", str(overlays[:3]))
        except Exception as e:
            logger.warning(f"[Tracker] log_final_decision failed: {e}")

    def end_run(self, execution_status: str, error_reason: str | None = None) -> None:
        """Close the MLflow run with final status."""
        if self._disabled or self._run_id is None:
            return

        try:
            mlflow.log_param("pipeline_status", execution_status)
            if error_reason:
                mlflow.log_param("error_reason", error_reason[:200])

            mlflow.end_run(status="FINISHED" if execution_status == "success" else "FAILED")
            logger.info(f"[Tracker] MLflow run {self._run_id} ended: {execution_status}")
        except Exception as e:
            logger.warning(f"[Tracker] end_run failed: {e}")
            try:
                mlflow.end_run(status="KILLED")
            except Exception:
                pass