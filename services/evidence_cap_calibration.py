"""Read-only calibration report for selective evidence caps.

This module summarizes observe-mode evidence cap behavior and recommends a
configuration for operator review. It never writes system_config and has no
execution authority.
"""
from __future__ import annotations

from datetime import datetime, timezone
from statistics import median
from typing import Any, Iterable

from services.conviction_decision import decision_statistical_status
from services.evidence_cap_config import (
    DEFAULT_MAX_CALIBRATION_AGE_DAYS,
    DEFAULT_MAX_WOULD_CLIP_RATE,
    DEFAULT_MIN_MULTIPLIER,
    DEFAULT_MIN_OBSERVE_CYCLES,
    default_evidence_cap_config,
)


CONTRACT_VERSION = "evidence_cap_calibration_v1"
YOUNG_ETF_HISTORY_DAYS = 252
EXPECTED_YOUNG_ETF_CAP_RANGE = {"min": 0.01, "max": 0.03}
DEFAULT_VOTE_THRESHOLDS = {
    "increase": {
        "min_voted_count": 2,
        "or_single_conviction_status": ["indicative", "statistically_meaningful"],
    },
    "reduce": {
        "min_voted_count": 1,
        "min_confidence": 0.65,
    },
    "hedge": {
        "min_voted_count": 1,
        "requires_regime": ["defensive", "alert", "high_vol", "risk_off"],
    },
}

MEANINGFUL_CONVICTION_STATUSES = {"statistically_meaningful", "indicative"}
EARLY_CONVICTION_STATUSES = {"early_signal"}
REJECT_EVENT_TYPES = {"qc_rejected", "rejected", "command_rejected"}


def build_evidence_cap_calibration_report(
    *,
    cap_cycles: Iterable[Any] | None = None,
    conviction_profiles: Iterable[Any] | None = None,
    command_events: Iterable[Any] | None = None,
    current_config: dict[str, Any] | None = None,
    operator_notes: Iterable[Any] | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    """Build a calibration report without mutating runtime configuration."""
    now = generated_at or datetime.now(timezone.utc)
    cfg = default_evidence_cap_config(current_config or {})
    cycles = [_normalize_cap_cycle(item) for item in (cap_cycles or [])]
    cycles = [item for item in cycles if item.get("cap_diagnostics")]
    cap_rows = _flatten_cap_rows(cycles)
    profiles = [_normalize_profile(item) for item in (conviction_profiles or [])]
    profiles = [row for row in profiles if row]
    events = [_normalize_event(item) for item in (command_events or [])]
    events = [row for row in events if row]
    notes = list(operator_notes or [])

    observe_summary = _observe_summary(cycles, cap_rows)
    young_etf_summary = _young_etf_summary(cap_rows)
    conviction_summary = _conviction_summary(profiles)
    execution_feedback = _execution_feedback(events)
    recommended_weights = _recommended_formula_weights(conviction_summary)
    recommended_min_multiplier = _recommended_min_multiplier(
        current_min_multiplier=float(cfg.get("min_multiplier") or DEFAULT_MIN_MULTIPLIER),
        young_etf_summary=young_etf_summary,
    )
    readiness = _gated_readiness(
        observe_summary=observe_summary,
        execution_feedback=execution_feedback,
        cfg=cfg,
    )
    recommended_config = {
        "mode": "gated" if readiness["criteria_met"] else "observe",
        "min_observe_cycles": int(cfg.get("min_observe_cycles") or DEFAULT_MIN_OBSERVE_CYCLES),
        "observe_cycles": observe_summary["observe_cycles"],
        "max_would_clip_rate": float(cfg.get("max_would_clip_rate") or DEFAULT_MAX_WOULD_CLIP_RATE),
        "would_clip_rate": observe_summary["would_clip_rate"],
        "calibration_generated_at": now.isoformat(),
        "max_calibration_age_days": float(cfg.get("max_calibration_age_days") or DEFAULT_MAX_CALIBRATION_AGE_DAYS),
        "require_fresh_calibration": True,
        "min_multiplier": recommended_min_multiplier,
        **recommended_weights,
        "enforcement_criteria_met": bool(readiness["criteria_met"]),
        "young_etf_cap_within_expected_range": (
            young_etf_summary["cap_range_status"] == "within_expected_range"
        ),
    }
    warnings = _calibration_warnings(observe_summary, young_etf_summary, conviction_summary, execution_feedback)
    status = "available" if observe_summary["observe_cycles"] > 0 else "insufficient_observe_data"
    return {
        "contract_version": CONTRACT_VERSION,
        "status": status,
        "generated_at": now.isoformat(),
        "recommendation_only": True,
        "execution_authority": "none",
        "target_weight_mutation": "none",
        "input_counts": {
            "observe_cycles": observe_summary["observe_cycles"],
            "cap_rows": observe_summary["cap_row_count"],
            "conviction_profiles": len(profiles),
            "command_events": len(events),
            "operator_notes": len(notes),
        },
        "observe_summary": observe_summary,
        "young_etf_summary": young_etf_summary,
        "conviction_summary": conviction_summary,
        "execution_feedback": execution_feedback,
        "gated_readiness": readiness,
        "recommended_config": recommended_config,
        "recommended_vote_thresholds": DEFAULT_VOTE_THRESHOLDS,
        "operator_action": _operator_action(readiness, status),
        "warnings": warnings,
    }


async def load_evidence_cap_calibration_report(
    db: Any,
    *,
    step_limit: int = 200,
    profile_limit: int = 5000,
    event_limit: int = 1000,
    current_config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Load recent DB diagnostics and render a read-only calibration report."""
    from sqlalchemy import desc, func, select

    from db.models import AgentStepLog, CommandLifecycleEvent, StrategyConvictionProfile

    step_result = await db.execute(
        select(AgentStepLog)
        .where(AgentStepLog.stage.in_(("2d_evidence_scorecard", "2c_playground")))
        .order_by(desc(AgentStepLog.created_at), desc(AgentStepLog.id))
        .limit(step_limit)
    )
    latest_profile_date_result = await db.execute(
        select(func.max(StrategyConvictionProfile.as_of_date))
    )
    latest_profile_date = latest_profile_date_result.scalar_one_or_none()
    profile_rows: list[Any] = []
    if latest_profile_date is not None:
        profile_result = await db.execute(
            select(StrategyConvictionProfile)
            .where(StrategyConvictionProfile.as_of_date == latest_profile_date)
            .order_by(
                StrategyConvictionProfile.source_bucket,
                StrategyConvictionProfile.strategy_id,
                StrategyConvictionProfile.ticker,
            )
            .limit(profile_limit)
        )
        profile_rows = list(profile_result.scalars().all())

    event_result = await db.execute(
        select(CommandLifecycleEvent)
        .order_by(desc(CommandLifecycleEvent.event_time), desc(CommandLifecycleEvent.id))
        .limit(event_limit)
    )
    report = build_evidence_cap_calibration_report(
        cap_cycles=list(step_result.scalars().all()),
        conviction_profiles=profile_rows,
        command_events=list(event_result.scalars().all()),
        current_config=current_config,
    )
    if latest_profile_date is not None:
        report["latest_conviction_profile_date"] = latest_profile_date.isoformat()
    return report


def _observe_summary(cycles: list[dict[str, Any]], rows: list[dict[str, Any]]) -> dict[str, Any]:
    would_clip_rows = [row for row in rows if row.get("would_clip")]
    degraded = [
        row for row in rows
        if _to_float(row.get("static_cap"), 0.0) - _to_float(row.get("evidence_adjusted_cap"), 0.0) > 1e-12
    ]
    multipliers = [_to_float(row.get("evidence_quality_multiplier"), None) for row in rows]
    multipliers = [value for value in multipliers if value is not None]
    adjusted_caps = [_to_float(row.get("evidence_adjusted_cap"), None) for row in rows]
    adjusted_caps = [value for value in adjusted_caps if value is not None]
    return {
        "observe_cycles": len(cycles),
        "cap_row_count": len(rows),
        "would_clip_count": len(would_clip_rows),
        "would_clip_rate": _safe_ratio(len(would_clip_rows), len(rows)),
        "degraded_ticker_count": len({row.get("ticker") for row in degraded if row.get("ticker")}),
        "mapping_error_count": sum(int(_to_float(row.get("mapping_error_count"), 0)) for row in rows),
        "median_multiplier": _rounded_median(multipliers),
        "median_evidence_adjusted_cap": _rounded_median(adjusted_caps),
        "top_would_clip_tickers": _top_counts(
            str(row.get("ticker") or "")
            for row in would_clip_rows
            if str(row.get("ticker") or "")
        ),
    }


def _young_etf_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    young = [
        row for row in rows
        if _to_float(row.get("history_days"), YOUNG_ETF_HISTORY_DAYS + 1) < YOUNG_ETF_HISTORY_DAYS
    ]
    adjusted_caps = [_to_float(row.get("evidence_adjusted_cap"), None) for row in young]
    adjusted_caps = [value for value in adjusted_caps if value is not None]
    would_clip = sum(1 for row in young if row.get("would_clip"))
    median_cap = _rounded_median(adjusted_caps)
    if median_cap is None:
        cap_range_status = "insufficient_data"
    elif median_cap < EXPECTED_YOUNG_ETF_CAP_RANGE["min"]:
        cap_range_status = "below_expected_range"
    elif median_cap > EXPECTED_YOUNG_ETF_CAP_RANGE["max"]:
        cap_range_status = "above_expected_range"
    else:
        cap_range_status = "within_expected_range"
    return {
        "history_threshold_days": YOUNG_ETF_HISTORY_DAYS,
        "expected_cap_range": dict(EXPECTED_YOUNG_ETF_CAP_RANGE),
        "row_count": len(young),
        "would_clip_count": would_clip,
        "would_clip_rate": _safe_ratio(would_clip, len(young)),
        "median_evidence_adjusted_cap": median_cap,
        "cap_range_status": cap_range_status,
        "top_young_tickers": _top_counts(str(row.get("ticker") or "") for row in young if row.get("ticker")),
    }


def _conviction_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    status_counts = _top_counts(str(row.get("status") or "unknown") for row in rows)
    statistical_status_counts = _top_counts(str(row.get("statistical_status") or "insufficient") for row in rows)
    source_counts = _top_counts(str(row.get("source_bucket") or "unknown") for row in rows)
    meaningful = sum(
        1 for row in rows if str(row.get("statistical_status") or "") in MEANINGFUL_CONVICTION_STATUSES
    )
    early = sum(1 for row in rows if str(row.get("statistical_status") or "") in EARLY_CONVICTION_STATUSES)
    insufficient = sum(
        1
        for row in rows
        if str(row.get("statistical_status") or "") not in MEANINGFUL_CONVICTION_STATUSES | EARLY_CONVICTION_STATUSES
    )
    ns = [_to_float(row.get("n"), None) for row in rows]
    ns = [value for value in ns if value is not None]
    return {
        "profile_count": len(rows),
        "meaningful_profile_count": meaningful,
        "early_profile_count": early,
        "insufficient_profile_count": insufficient,
        "meaningful_profile_ratio": _safe_ratio(meaningful, len(rows)),
        "median_n": _rounded_median(ns),
        "status_counts": status_counts,
        "statistical_status_counts": statistical_status_counts,
        "source_counts": source_counts,
    }


def _execution_feedback(rows: list[dict[str, Any]]) -> dict[str, Any]:
    rejected = [
        row for row in rows
        if str(row.get("event_type") or "").lower() in REJECT_EVENT_TYPES
        or str(row.get("event_status") or "").lower() == "rejected"
    ]
    return {
        "event_count": len(rows),
        "rejection_count": len(rejected),
        "rejection_rate": _safe_ratio(len(rejected), len(rows)),
        "top_rejection_reasons": _top_counts(str(row.get("reason") or "unknown") for row in rejected),
    }


def _recommended_formula_weights(conviction_summary: dict[str, Any]) -> dict[str, float]:
    meaningful_ratio = float(conviction_summary.get("meaningful_profile_ratio") or 0.0)
    if meaningful_ratio >= 0.50:
        return {
            "coverage_weight": 0.40,
            "conviction_weight": 0.40,
            "history_weight": 0.20,
        }
    if meaningful_ratio >= 0.20:
        return {
            "coverage_weight": 0.45,
            "conviction_weight": 0.35,
            "history_weight": 0.20,
        }
    return {
        "coverage_weight": 0.50,
        "conviction_weight": 0.25,
        "history_weight": 0.25,
    }


def _recommended_min_multiplier(*, current_min_multiplier: float, young_etf_summary: dict[str, Any]) -> float:
    status = str(young_etf_summary.get("cap_range_status") or "")
    if status == "below_expected_range":
        return round(max(current_min_multiplier, 0.15), 6)
    if status == "above_expected_range":
        return round(min(current_min_multiplier, 0.10), 6)
    return round(current_min_multiplier, 6)


def _gated_readiness(
    *,
    observe_summary: dict[str, Any],
    execution_feedback: dict[str, Any],
    cfg: dict[str, Any],
) -> dict[str, Any]:
    blockers: list[str] = []
    observe_cycles = int(observe_summary.get("observe_cycles") or 0)
    min_observe_cycles = int(cfg.get("min_observe_cycles") or DEFAULT_MIN_OBSERVE_CYCLES)
    if observe_cycles < min_observe_cycles:
        blockers.append("insufficient_observe_cycles")

    would_clip_rate = float(observe_summary.get("would_clip_rate") or 0.0)
    max_would_clip_rate = float(cfg.get("max_would_clip_rate") or DEFAULT_MAX_WOULD_CLIP_RATE)
    if would_clip_rate > max_would_clip_rate + 1e-12:
        blockers.append("would_clip_rate_too_high")

    rejection_rate = float(execution_feedback.get("rejection_rate") or 0.0)
    if rejection_rate > 0.20:
        blockers.append("recent_command_rejection_rate_high")

    return {
        "criteria_met": not blockers and observe_cycles > 0,
        "gate_blockers": blockers,
        "observe_cycles": observe_cycles,
        "min_observe_cycles": min_observe_cycles,
        "would_clip_rate": round(would_clip_rate, 6),
        "max_would_clip_rate": round(max_would_clip_rate, 6),
        "rejection_rate": round(rejection_rate, 6),
        "requires_operator_approval": True,
    }


def _operator_action(readiness: dict[str, Any], status: str) -> str:
    if status != "available":
        return "collect_observe_data"
    if readiness.get("criteria_met"):
        return "operator_review_then_optionally_enable_gated"
    return "keep_observe_and_collect_more_calibration"


def _calibration_warnings(
    observe_summary: dict[str, Any],
    young_etf_summary: dict[str, Any],
    conviction_summary: dict[str, Any],
    execution_feedback: dict[str, Any],
) -> list[str]:
    warnings: list[str] = []
    if observe_summary.get("observe_cycles", 0) < DEFAULT_MIN_OBSERVE_CYCLES:
        warnings.append("observe_cycles_below_minimum")
    if young_etf_summary.get("cap_range_status") == "below_expected_range":
        warnings.append("young_etf_caps_below_expected_range")
    if conviction_summary.get("meaningful_profile_ratio", 0.0) < 0.20:
        warnings.append("conviction_profiles_not_yet_meaningful")
    if execution_feedback.get("rejection_rate", 0.0) > 0.20:
        warnings.append("recent_command_rejection_rate_high")
    return warnings


def _flatten_cap_rows(cycles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for cycle in cycles:
        analysis_id = cycle.get("analysis_id")
        created_at = cycle.get("created_at")
        for ticker, raw in (cycle.get("cap_diagnostics") or {}).items():
            if not isinstance(raw, dict):
                continue
            clean_ticker = str(raw.get("ticker") or ticker or "").upper().strip()
            if not clean_ticker:
                continue
            rows.append({
                "analysis_id": analysis_id,
                "created_at": created_at,
                "ticker": clean_ticker,
                **raw,
            })
    return rows


def _normalize_cap_cycle(value: Any) -> dict[str, Any]:
    payload = _record_get(value, "output_data")
    if not isinstance(payload, dict):
        payload = value if isinstance(value, dict) else {}
    return {
        "analysis_id": _record_get(value, "analysis_id") or payload.get("analysis_id"),
        "created_at": _iso_or_none(_record_get(value, "created_at") or payload.get("created_at")),
        "cap_diagnostics": _extract_cap_diagnostics(payload),
    }


def _extract_cap_diagnostics(payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    candidates = [
        payload.get("evidence_cap_diagnostics"),
        ((payload.get("strategies") or {}) if isinstance(payload.get("strategies"), dict) else {}).get(
            "evidence_cap_diagnostics"
        ),
        (
            ((payload.get("evidence_bundle") or {}) if isinstance(payload.get("evidence_bundle"), dict) else {})
            .get("strategies", {})
            if isinstance(
                ((payload.get("evidence_bundle") or {}) if isinstance(payload.get("evidence_bundle"), dict) else {})
                .get("strategies", {}),
                dict,
            )
            else {}
        ).get("evidence_cap_diagnostics"),
        ((payload.get("playground_bundle") or {}) if isinstance(payload.get("playground_bundle"), dict) else {}).get(
            "evidence_cap_diagnostics"
        ),
    ]
    for candidate in candidates:
        if isinstance(candidate, dict) and candidate:
            return candidate
    return {}


def _normalize_profile(value: Any) -> dict[str, Any]:
    diagnostics = _record_get(value, "diagnostics")
    diagnostics = diagnostics if isinstance(diagnostics, dict) else {}
    n = _record_get(value, "n")
    status = _record_get(value, "status")
    return {
        "strategy_id": _record_get(value, "strategy_id"),
        "ticker": _record_get(value, "ticker"),
        "source_bucket": _record_get(value, "source_bucket"),
        "status": status,
        "statistical_status": decision_statistical_status(
            status=_record_get(value, "statistical_status") or status,
            n=_optional_int(n),
            diagnostics=diagnostics,
        ),
        "n": n,
        "conviction": _record_get(value, "conviction"),
        "as_of_date": _iso_or_none(_record_get(value, "as_of_date")),
    }


def _normalize_event(value: Any) -> dict[str, Any]:
    return {
        "command_id": _record_get(value, "command_id"),
        "event_type": _record_get(value, "event_type"),
        "event_status": _record_get(value, "event_status"),
        "reason": _record_get(value, "reason"),
        "event_time": _iso_or_none(_record_get(value, "event_time")),
    }


def _record_get(value: Any, key: str, default: Any = None) -> Any:
    if isinstance(value, dict):
        return value.get(key, default)
    return getattr(value, key, default)


def _top_counts(values: Iterable[str], *, limit: int = 8) -> dict[str, int]:
    counts: dict[str, int] = {}
    for raw in values:
        value = str(raw or "").strip()
        if not value:
            continue
        counts[value] = counts.get(value, 0) + 1
    return dict(sorted(counts.items(), key=lambda item: (-item[1], item[0]))[:limit])


def _rounded_median(values: list[float]) -> float | None:
    if not values:
        return None
    return round(float(median(values)), 6)


def _safe_ratio(numerator: int | float, denominator: int | float) -> float:
    denom = float(denominator or 0.0)
    if denom <= 0:
        return 0.0
    return round(float(numerator or 0.0) / denom, 6)


def _to_float(value: Any, default: float | None) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _optional_int(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _iso_or_none(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    text = str(value)
    return text or None
