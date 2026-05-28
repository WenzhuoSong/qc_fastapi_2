"""Policy gate for alpha-decision diagnostics.

This module decides how alpha-decision diagnostics may be consumed by
recommendation and portfolio-construction layers. It has no execution authority
and cannot mutate target weights.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


ALPHA_DECISION_POLICY_CONFIG_KEY = "alpha_decision_policy_config"
ALPHA_DECISION_POLICY_CONTRACT_VERSION = "alpha_decision_policy_v1"
VALID_MODES = {"observe", "recommendation", "gated"}
DEFAULT_CONFIG: dict[str, Any] = {
    "mode": "observe",
    "min_status_for_promotion": "indicative",
    "min_status_for_allocation_full_credit": "statistically_meaningful",
    "require_positive_residual_alpha": True,
    "require_cost_adjusted_edge_positive": True,
    "max_full_credit_correlation": 0.4,
    "max_allowed_duplicate_correlation": 0.8,
    "cost_model": "ibkr_proxy",
    "min_observe_cycles_before_gated": 20,
    "operator_approval_required_for_gated": True,
    "operator_gated_approved": False,
    "raw_adjusted_diagnostics_reviewed": False,
    "dry_run_report_reviewed": False,
    "unexpected_mature_degradation_false_positive_count": 0,
    "evidence_cap_calibration_fresh": False,
    "dashboard_naked_conviction_blocked": True,
    "observe_cycles": 0,
}


def default_alpha_decision_policy_config(raw: dict[str, Any] | None = None) -> dict[str, Any]:
    """Return a normalized alpha-decision policy config.

    Invalid modes fall back to observe. The returned dict intentionally carries
    a private ``_mode_explicitly_configured`` marker so gated mode cannot be
    enabled by accidental defaults.
    """
    value = raw if isinstance(raw, dict) else {}
    cfg = dict(DEFAULT_CONFIG)
    mode = str(value.get("mode") or cfg["mode"]).strip().lower()
    cfg["mode"] = mode if mode in VALID_MODES else "observe"
    cfg["_mode_explicitly_configured"] = "mode" in value

    for key in (
        "min_status_for_promotion",
        "min_status_for_allocation_full_credit",
        "cost_model",
    ):
        if key in value:
            cfg[key] = str(value.get(key) or cfg[key]).strip() or cfg[key]

    for key in (
        "require_positive_residual_alpha",
        "require_cost_adjusted_edge_positive",
        "operator_approval_required_for_gated",
        "operator_gated_approved",
        "raw_adjusted_diagnostics_reviewed",
        "dry_run_report_reviewed",
        "evidence_cap_calibration_fresh",
        "dashboard_naked_conviction_blocked",
    ):
        if key in value:
            cfg[key] = _to_bool(value.get(key))

    for key in ("max_full_credit_correlation", "max_allowed_duplicate_correlation"):
        parsed = _to_float(value.get(key))
        if parsed is not None:
            cfg[key] = max(min(parsed, 1.0), -1.0)

    for key in (
        "min_observe_cycles_before_gated",
        "unexpected_mature_degradation_false_positive_count",
        "observe_cycles",
    ):
        parsed_int = _to_int(value.get(key))
        if parsed_int is not None:
            cfg[key] = max(parsed_int, 0)

    return cfg


def evaluate_alpha_decision_policy(
    config: dict[str, Any] | None = None,
    *,
    alpha_decision_summary: dict[str, Any] | None = None,
    evidence_cap_calibration: dict[str, Any] | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    """Evaluate alpha decision consumption mode.

    Gated mode requires explicit config and all review criteria. A blocked
    gated request degrades to recommendation effect only; allocation effect
    remains disabled.
    """
    cfg = default_alpha_decision_policy_config(config or {})
    mode = str(cfg.get("mode") or "observe")
    blockers: list[str] = []
    warnings: list[str] = []

    observe_cycles = _observe_cycles(cfg, alpha_decision_summary or {})
    evidence_cap_fresh = _evidence_cap_fresh(cfg, evidence_cap_calibration, generated_at=generated_at)
    false_positive_count = int(cfg.get("unexpected_mature_degradation_false_positive_count") or 0)

    if mode == "gated":
        if not cfg.get("_mode_explicitly_configured"):
            blockers.append("mode_not_explicitly_configured")
        if observe_cycles < int(cfg.get("min_observe_cycles_before_gated") or 20):
            blockers.append("insufficient_observe_cycles")
        if bool(cfg.get("operator_approval_required_for_gated")) and not bool(cfg.get("operator_gated_approved")):
            blockers.append("operator_gated_approval_missing")
        if false_positive_count > 0:
            blockers.append("unexpected_mature_degradation_false_positive")
        if not evidence_cap_fresh:
            blockers.append("evidence_cap_calibration_not_fresh")
        if not bool(cfg.get("dashboard_naked_conviction_blocked")):
            blockers.append("dashboard_naked_conviction_not_blocked")
        if not bool(cfg.get("raw_adjusted_diagnostics_reviewed")):
            blockers.append("raw_adjusted_allocation_diagnostics_not_reviewed")
        if not bool(cfg.get("dry_run_report_reviewed")):
            blockers.append("gated_dry_run_report_not_reviewed")

    if mode == "observe":
        effective_mode = "observe"
    elif mode == "recommendation":
        effective_mode = "recommendation"
    elif blockers:
        effective_mode = "recommendation"
        warnings.append("requested_gated_mode_blocked")
    else:
        effective_mode = "gated"

    recommendation_effect = effective_mode in {"recommendation", "gated"}
    allocation_effect = effective_mode == "gated"

    return {
        "contract_version": ALPHA_DECISION_POLICY_CONTRACT_VERSION,
        "mode": mode,
        "effective_mode": effective_mode,
        "gated_enabled": allocation_effect,
        "recommendation_effect": recommendation_effect,
        "allocation_effect": allocation_effect,
        "would_affect_allocation": True,
        "blockers": sorted(set(blockers)),
        "warnings": sorted(set(warnings)),
        "criteria": {
            "observe_cycles": observe_cycles,
            "min_observe_cycles_before_gated": int(cfg.get("min_observe_cycles_before_gated") or 20),
            "operator_approval_required_for_gated": bool(cfg.get("operator_approval_required_for_gated")),
            "operator_gated_approved": bool(cfg.get("operator_gated_approved")),
            "raw_adjusted_diagnostics_reviewed": bool(cfg.get("raw_adjusted_diagnostics_reviewed")),
            "dry_run_report_reviewed": bool(cfg.get("dry_run_report_reviewed")),
            "unexpected_mature_degradation_false_positive_count": false_positive_count,
            "evidence_cap_calibration_fresh": evidence_cap_fresh,
            "dashboard_naked_conviction_blocked": bool(cfg.get("dashboard_naked_conviction_blocked")),
        },
        "decision_rules": {
            "min_status_for_promotion": cfg.get("min_status_for_promotion"),
            "min_status_for_allocation_full_credit": cfg.get("min_status_for_allocation_full_credit"),
            "require_positive_residual_alpha": bool(cfg.get("require_positive_residual_alpha")),
            "require_cost_adjusted_edge_positive": bool(cfg.get("require_cost_adjusted_edge_positive")),
            "max_full_credit_correlation": cfg.get("max_full_credit_correlation"),
            "max_allowed_duplicate_correlation": cfg.get("max_allowed_duplicate_correlation"),
            "cost_model": cfg.get("cost_model"),
        },
        "execution_authority": "none",
        "target_weight_mutation": "none",
        "never_bypasses_target_builder": True,
        "full_auto_safety_preconditions_unchanged": True,
        "operator_review_required_for_gated": bool(cfg.get("operator_approval_required_for_gated")),
    }


def _observe_cycles(config: dict[str, Any], alpha_decision_summary: dict[str, Any]) -> int:
    for value in (
        config.get("observe_cycles"),
        config.get("alpha_decision_observe_cycles"),
        alpha_decision_summary.get("observe_cycles"),
        alpha_decision_summary.get("alpha_decision_observe_cycles"),
    ):
        parsed = _to_int(value)
        if parsed is not None:
            return max(parsed, 0)
    return 0


def _evidence_cap_fresh(
    config: dict[str, Any],
    evidence_cap_calibration: dict[str, Any] | None,
    *,
    generated_at: datetime | None,
) -> bool:
    if config.get("evidence_cap_calibration_fresh"):
        return True
    report = evidence_cap_calibration if isinstance(evidence_cap_calibration, dict) else {}
    recommended = report.get("recommended_config") if isinstance(report.get("recommended_config"), dict) else {}
    generated = recommended.get("calibration_generated_at") or report.get("generated_at")
    max_age_days = _to_float(recommended.get("max_calibration_age_days"))
    if not generated or max_age_days is None:
        return False
    try:
        produced_at = datetime.fromisoformat(str(generated).replace("Z", "+00:00"))
    except ValueError:
        return False
    if produced_at.tzinfo is None:
        produced_at = produced_at.replace(tzinfo=timezone.utc)
    now = generated_at or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    age_days = (now - produced_at).total_seconds() / 86400.0
    return 0 <= age_days <= max_age_days


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _to_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value: Any) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        return None
