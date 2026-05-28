"""Runtime config helpers for selective evidence cap enforcement."""
from __future__ import annotations

from typing import Any


VALID_EVIDENCE_CAP_MODES = {"off", "observe", "gated"}
DEFAULT_EVIDENCE_CAP_MODE = "observe"
DEFAULT_MIN_OBSERVE_CYCLES = 10
DEFAULT_MAX_WOULD_CLIP_RATE = 0.30
DEFAULT_MIN_MULTIPLIER = 0.10


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


def resolve_evidence_cap_mode(config: dict[str, Any] | None = None) -> dict[str, Any]:
    """Resolve configured mode into the effective target-builder behavior."""
    cfg = default_evidence_cap_config(config)
    configured_mode = str(cfg.get("mode") or DEFAULT_EVIDENCE_CAP_MODE)

    criteria = evidence_cap_enforcement_criteria(cfg)
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
        **cfg,
    }


def evidence_cap_enforcement_criteria(config: dict[str, Any] | None = None) -> dict[str, Any]:
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

    return {
        "criteria_met": not blockers,
        "gate_blockers": blockers,
        "observe_cycles": observe_cycles,
        "min_observe_cycles": min_observe_cycles,
        "would_clip_rate": would_clip_rate,
        "max_would_clip_rate": max_would_clip_rate,
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


def _to_nonnegative_int(value: Any, default: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = int(default)
    return max(parsed, 0)


def _clamp(value: float) -> float:
    return max(0.0, min(1.0, float(value)))
