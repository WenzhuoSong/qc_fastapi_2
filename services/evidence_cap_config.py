"""Runtime config helpers for selective evidence cap enforcement."""
from __future__ import annotations

from datetime import UTC, datetime
from typing import Any


VALID_EVIDENCE_CAP_MODES = {"off", "observe", "gated"}
DEFAULT_EVIDENCE_CAP_MODE = "observe"
DEFAULT_MIN_OBSERVE_CYCLES = 10
DEFAULT_MAX_WOULD_CLIP_RATE = 0.30
DEFAULT_MIN_MULTIPLIER = 0.10
DEFAULT_MAX_CALIBRATION_AGE_DAYS = 7.0


def default_evidence_cap_config(raw: dict[str, Any] | None = None) -> dict[str, Any]:
    """Return a normalized JSON-safe evidence cap config.

    Defaults are intentionally observe-only. A configured ``mode=gated`` still
    requires readiness metrics to pass before target_builder may clip weights.
    """
    cfg = dict(raw or {})
    mode = str(cfg.get("mode") or DEFAULT_EVIDENCE_CAP_MODE).strip().lower()
    if mode not in VALID_EVIDENCE_CAP_MODES:
        mode = DEFAULT_EVIDENCE_CAP_MODE

    out = {
        "mode": mode,
        "min_observe_cycles": _to_nonnegative_int(
            cfg.get("min_observe_cycles"),
            DEFAULT_MIN_OBSERVE_CYCLES,
        ),
        "max_would_clip_rate": _clamp(
            _to_float(cfg.get("max_would_clip_rate"), DEFAULT_MAX_WOULD_CLIP_RATE)
        ),
        "min_multiplier": _clamp(_to_float(cfg.get("min_multiplier"), DEFAULT_MIN_MULTIPLIER)),
        "observe_cycles": _to_nonnegative_int(
            cfg.get("observe_cycles", cfg.get("observed_cycles")),
            0,
        ),
        "would_clip_rate": _optional_float(cfg.get("would_clip_rate")),
        "calibration_generated_at": _optional_str(
            cfg.get("calibration_generated_at", cfg.get("generated_at"))
        ),
        "max_calibration_age_days": max(
            _to_float(cfg.get("max_calibration_age_days"), DEFAULT_MAX_CALIBRATION_AGE_DAYS),
            0.0,
        ),
        "require_fresh_calibration": bool(cfg.get("require_fresh_calibration", True)),
    }

    # Optional operator-reviewed criteria. When present and false, it blocks
    # gated enforcement; when absent, readiness is determined by metrics above.
    for key in (
        "enforcement_criteria_met",
        "no_false_positive_degradation",
        "young_etf_cap_within_expected_range",
        "young_etf_cap_reviewed",
    ):
        if key in cfg:
            out[key] = bool(cfg.get(key))
    return out


def resolve_evidence_cap_mode(
    config: dict[str, Any] | None = None,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Resolve configured mode into the effective target-builder behavior."""
    cfg = default_evidence_cap_config(config)
    configured_mode = str(cfg.get("mode") or DEFAULT_EVIDENCE_CAP_MODE)

    criteria = evidence_cap_enforcement_criteria(cfg, now=now)
    if configured_mode == "off":
        effective_mode = "off"
        blocked_reason = None
        execution_effect = "none"
    elif configured_mode == "observe":
        effective_mode = "observe"
        blocked_reason = None
        execution_effect = "diagnostic_only"
    elif criteria["criteria_met"]:
        effective_mode = "gated"
        blocked_reason = None
        execution_effect = "tighten_only"
    else:
        effective_mode = "observe"
        blocked_reason = "enforcement_criteria_not_met"
        execution_effect = "diagnostic_only"

    return {
        "configured_mode": configured_mode,
        "effective_mode": effective_mode,
        "enabled": configured_mode != "off",
        "criteria_met": bool(criteria["criteria_met"]),
        "blocked_reason": blocked_reason,
        "gate_blockers": criteria["gate_blockers"],
        "execution_effect": execution_effect,
        "calibration_freshness": criteria["calibration_freshness"],
        **cfg,
    }


def evidence_cap_enforcement_criteria(
    config: dict[str, Any] | None = None,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Evaluate readiness criteria for gated evidence cap enforcement."""
    cfg = default_evidence_cap_config(config)
    blockers: list[str] = []

    observe_cycles = int(cfg.get("observe_cycles") or 0)
    min_observe_cycles = int(cfg.get("min_observe_cycles") or DEFAULT_MIN_OBSERVE_CYCLES)
    if observe_cycles < min_observe_cycles:
        blockers.append("insufficient_observe_cycles")

    would_clip_rate = cfg.get("would_clip_rate")
    max_would_clip_rate = float(cfg.get("max_would_clip_rate") or DEFAULT_MAX_WOULD_CLIP_RATE)
    if would_clip_rate is None:
        blockers.append("missing_would_clip_rate")
    elif float(would_clip_rate) > max_would_clip_rate + 1e-12:
        blockers.append("would_clip_rate_too_high")

    for key in (
        "enforcement_criteria_met",
        "no_false_positive_degradation",
        "young_etf_cap_within_expected_range",
        "young_etf_cap_reviewed",
    ):
        if key in cfg and not bool(cfg.get(key)):
            blockers.append(key)

    freshness = calibration_freshness(
        generated_at=cfg.get("calibration_generated_at"),
        max_age_days=float(cfg.get("max_calibration_age_days") or DEFAULT_MAX_CALIBRATION_AGE_DAYS),
        now=now,
    )
    if bool(cfg.get("require_fresh_calibration", True)):
        if not freshness["present"]:
            blockers.append("missing_calibration_generated_at")
        elif not freshness["valid"]:
            blockers.append("invalid_calibration_generated_at")
        elif not freshness["fresh"]:
            blockers.append("calibration_data_stale")

    return {
        "criteria_met": not blockers,
        "gate_blockers": blockers,
        "observe_cycles": observe_cycles,
        "min_observe_cycles": min_observe_cycles,
        "would_clip_rate": would_clip_rate,
        "max_would_clip_rate": max_would_clip_rate,
        "calibration_freshness": freshness,
    }


def calibration_freshness(
    *,
    generated_at: Any,
    max_age_days: float = DEFAULT_MAX_CALIBRATION_AGE_DAYS,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return freshness metadata for an evidence-cap calibration timestamp."""
    raw = _optional_str(generated_at)
    max_age = max(float(max_age_days or DEFAULT_MAX_CALIBRATION_AGE_DAYS), 0.0)
    if not raw:
        return {
            "present": False,
            "valid": False,
            "fresh": False,
            "generated_at": None,
            "age_days": None,
            "max_age_days": max_age,
        }

    parsed = _parse_datetime(raw)
    if parsed is None:
        return {
            "present": True,
            "valid": False,
            "fresh": False,
            "generated_at": raw,
            "age_days": None,
            "max_age_days": max_age,
        }

    current = _strip_tz(now or datetime.now(UTC))
    age_days = max((current - parsed).total_seconds() / 86400.0, 0.0)
    return {
        "present": True,
        "valid": True,
        "fresh": age_days <= max_age + 1e-12,
        "generated_at": parsed.isoformat(),
        "age_days": round(age_days, 6),
        "max_age_days": max_age,
    }


def _to_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _to_nonnegative_int(value: Any, default: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = int(default)
    return max(parsed, 0)


def _clamp(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def _parse_datetime(value: str) -> datetime | None:
    try:
        return _strip_tz(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _strip_tz(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(UTC).replace(tzinfo=None)
