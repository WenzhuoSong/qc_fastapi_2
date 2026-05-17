"""
Strategy confidence calibrator.

This is the single consumer for KnowledgeResolution.confidence_adjustments. It
applies accepted deltas exactly once and records every rejected adjustment.
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any


EXPECTED_CONSUMER = "strategy_confidence_calibrator"


def calibrate_strategy_confidence(
    *,
    strategy_confidence: dict[str, Any] | None,
    knowledge_resolution: dict[str, Any] | None,
) -> dict[str, Any]:
    """Apply resolver confidence adjustments to strategy confidence once."""
    original = deepcopy(strategy_confidence or {})
    calibrated = deepcopy(original)
    resolution = knowledge_resolution or {}
    adjustments = resolution.get("confidence_adjustments") or {}
    intended = adjustments.get("intended_consumer")
    records: list[dict[str, Any]] = []

    if intended and intended != EXPECTED_CONSUMER:
        records = [
            {
                "status": "rejected",
                "reason": "wrong_intended_consumer",
                "intended_consumer": intended,
            }
        ]
        return {
            "strategy_confidence": calibrated,
            "records": records,
            "summary": _summary(records),
        }

    blocking_missing = _blocking_missing_knowledge(resolution)
    applied_targets: set[str] = set()
    for item in adjustments.get("items") or []:
        record = _apply_adjustment(
            item=item,
            calibrated=calibrated,
            blocking_missing=blocking_missing,
            applied_targets=applied_targets,
        )
        records.append(record)

    return {
        "strategy_confidence": calibrated,
        "records": records,
        "summary": _summary(records),
    }


def _apply_adjustment(
    *,
    item: dict[str, Any],
    calibrated: dict[str, Any],
    blocking_missing: bool,
    applied_targets: set[str],
) -> dict[str, Any]:
    target_type = item.get("target_type")
    target = str(item.get("target") or "")
    delta = _to_float(item.get("delta"), 0.0)
    max_abs_delta = abs(_to_float(item.get("max_abs_delta"), 0.0))
    base_record = {
        "target_type": target_type,
        "target": target,
        "requested_delta": delta,
        "reason": item.get("reason"),
    }

    if item.get("status") == "rejected":
        return {
            **base_record,
            "status": "rejected",
            "rejection_reason": item.get("rejection_reason") or "resolver_rejected",
        }
    if blocking_missing:
        return {
            **base_record,
            "status": "rejected",
            "rejection_reason": "blocking_missing_knowledge",
        }
    if target_type != "strategy":
        return {
            **base_record,
            "status": "rejected",
            "rejection_reason": "unsupported_target_type",
        }
    if not target or target not in calibrated:
        return {
            **base_record,
            "status": "rejected",
            "rejection_reason": "unknown_strategy",
        }
    if target in applied_targets:
        return {
            **base_record,
            "status": "rejected",
            "rejection_reason": "duplicate_target_adjustment",
        }
    if max_abs_delta <= 0:
        return {
            **base_record,
            "status": "rejected",
            "rejection_reason": "invalid_max_abs_delta",
        }
    if abs(delta) > max_abs_delta:
        return {
            **base_record,
            "status": "rejected",
            "rejection_reason": "delta_exceeds_max_abs_delta",
            "max_abs_delta": max_abs_delta,
        }

    row = calibrated.get(target)
    if not isinstance(row, dict):
        return {
            **base_record,
            "status": "rejected",
            "rejection_reason": "malformed_strategy_confidence",
        }

    before = _to_float(row.get("confidence_score"), None)
    if before is None:
        return {
            **base_record,
            "status": "rejected",
            "rejection_reason": "missing_confidence_score",
        }

    after = max(0.0, min(1.0, before + delta))
    row["confidence_score"] = after
    row["confidence_score_pre_calibration"] = before
    row.setdefault("calibration_reason_codes", [])
    row["calibration_reason_codes"] = _unique(
        list(row["calibration_reason_codes"]) + [str(item.get("reason") or "knowledge_adjustment")]
    )
    applied_targets.add(target)
    return {
        **base_record,
        "status": "accepted",
        "confidence_before": before,
        "confidence_after": after,
        "applied_delta": after - before,
    }


def _blocking_missing_knowledge(resolution: dict[str, Any]) -> bool:
    return any(
        str(item.get("severity")) == "blocking"
        for item in resolution.get("missing_knowledge") or []
    )


def _summary(records: list[dict[str, Any]]) -> dict[str, int]:
    accepted = sum(1 for record in records if record.get("status") == "accepted")
    rejected = sum(1 for record in records if record.get("status") == "rejected")
    return {
        "total": len(records),
        "accepted": accepted,
        "rejected": rejected,
    }


def _to_float(value: Any, default: float | None = 0.0) -> float | None:
    try:
        return float(value) if value is not None else default
    except (TypeError, ValueError):
        return default


def _unique(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out
