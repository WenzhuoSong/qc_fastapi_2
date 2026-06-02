"""
Execution gateway for dual-track strategy and execution evidence.

Strategy validity is evaluated from historical/consensus evidence. QC live
inputs are treated as execution intel: availability, drift, turnover, and cost.
"""
from __future__ import annotations

from typing import Any


TURNOVER_THRESHOLD = 0.60


def build_execution_gateway(
    strategy_evidence: dict[str, Any] | None,
    *,
    turnover_threshold: float = TURNOVER_THRESHOLD,
) -> dict[str, Any]:
    evidence = strategy_evidence or {}
    strategy_layer = _strategy_layer(evidence)
    execution_layer = _execution_layer(evidence, turnover_threshold=turnover_threshold)

    if strategy_layer["verdict"] == "blocked":
        final_permission = "denied"
        primary_reason = strategy_layer["reason"]
        source = "strategy_layer"
    elif strategy_layer["verdict"] == "watch_only":
        final_permission = "tightened"
        primary_reason = strategy_layer["reason"]
        source = "strategy_layer"
    elif execution_layer["verdict"] in {"blocked", "tightened"}:
        final_permission = "tightened"
        primary_reason = execution_layer["reason"]
        source = "execution_intel_layer"
    else:
        final_permission = "approved"
        primary_reason = "strategy_supported_and_execution_acceptable"
        source = "gateway"

    return {
        "final_permission": final_permission,
        "primary_reason": primary_reason,
        "source": source,
        "response_class": _response_class(strategy_layer, execution_layer, source),
        "strategy_layer": strategy_layer,
        "execution_intel_layer": execution_layer,
        "thresholds": {
            "turnover_threshold": float(turnover_threshold),
        },
    }


def _strategy_layer(evidence: dict[str, Any]) -> dict[str, Any]:
    summary = evidence.get("evidence_summary") or {}
    confidence = evidence.get("strategy_confidence") or {}
    use_summary = evidence.get("strategy_use_summary") or {}
    historical = str(summary.get("historical_evidence") or "").lower()
    legacy_permission = str(summary.get("execution_permission") or "").lower()
    rows = [row for row in confidence.values() if isinstance(row, dict)]
    consensus_conflict = any(bool(row.get("consensus_conflict")) for row in rows)
    actionable_count = int(_to_float(use_summary.get("actionable_count"), 0) or 0)

    if legacy_permission == "blocked" or historical in {"missing", "weak"} and actionable_count <= 0:
        return {
            "verdict": "blocked",
            "reason": "no_actionable_strategy_confidence",
            "response_class": "data_quality",
            "historical_evidence": historical or "unknown",
        }
    if consensus_conflict or legacy_permission == "human_required":
        return {
            "verdict": "watch_only",
            "reason": "regime_consensus_mismatch",
            "response_class": "strategy_conflict",
            "historical_evidence": historical or "unknown",
        }
    if actionable_count > 0 or legacy_permission in {"allowed", "advisory"}:
        return {
            "verdict": "approved",
            "reason": "historical_strategy_support",
            "response_class": "normal",
            "historical_evidence": historical or "unknown",
        }
    return {
        "verdict": "watch_only",
        "reason": "strategy_confidence_watch_only",
        "response_class": "data_quality",
        "historical_evidence": historical or "unknown",
    }


def _execution_layer(evidence: dict[str, Any], *, turnover_threshold: float) -> dict[str, Any]:
    execution_intel = evidence.get("execution_intel") or {}
    summary = evidence.get("evidence_summary") or {}
    status = str(
        execution_intel.get("status")
        or summary.get("execution_intel_status")
        or "unknown"
    )
    max_turnover = _max_turnover(evidence)
    if max_turnover > turnover_threshold:
        return {
            "verdict": "tightened",
            "reason": "high_turnover_cost",
            "response_class": "cost_turnover",
            "execution_intel_status": status,
            "gross_turnover_pct": round(max_turnover, 6),
        }
    if status == "insufficient_data":
        return {
            "verdict": "tightened",
            "reason": "execution_intel_insufficient_data",
            "response_class": "data_quality",
            "execution_intel_status": status,
            "gross_turnover_pct": round(max_turnover, 6),
        }
    if status == "conflicted":
        return {
            "verdict": "tightened",
            "reason": "execution_intel_conflicted",
            "response_class": "strategy_conflict",
            "execution_intel_status": status,
            "gross_turnover_pct": round(max_turnover, 6),
        }
    return {
        "verdict": "acceptable",
        "reason": "execution_intel_available",
        "response_class": "normal",
        "execution_intel_status": status,
        "gross_turnover_pct": round(max_turnover, 6),
    }


def _response_class(strategy_layer: dict[str, Any], execution_layer: dict[str, Any], source: str) -> str:
    if source == "strategy_layer":
        return str(strategy_layer.get("response_class") or "data_quality")
    if source == "execution_intel_layer":
        return str(execution_layer.get("response_class") or "data_quality")
    return "normal"


def _max_turnover(evidence: dict[str, Any]) -> float:
    candidates: list[float] = []
    execution_intel = evidence.get("execution_intel") or {}
    turnover_estimate = execution_intel.get("turnover_estimate") or {}
    candidates.append(_to_float(turnover_estimate.get("gross_turnover_pct"), 0.0) or 0.0)
    candidates.append(_to_float(execution_intel.get("gross_turnover_pct"), 0.0) or 0.0)
    candidates.append(_to_float(evidence.get("max_turnover"), 0.0) or 0.0)
    for row in evidence.get("strategy_results") or []:
        if isinstance(row, dict):
            candidates.append(_to_float(row.get("turnover"), 0.0) or 0.0)
            candidates.append(_to_float(row.get("expected_turnover_pct"), 0.0) or 0.0)
    return max(candidates or [0.0])


def _to_float(value: Any, default: float | None = 0.0) -> float | None:
    try:
        return float(value) if value is not None else default
    except (TypeError, ValueError):
        return default
