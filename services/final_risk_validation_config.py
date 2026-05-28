"""Pure helpers for final risk validation runtime configuration."""
from __future__ import annotations

from typing import Any


VALID_FINAL_RISK_VALIDATION_MODES = {"observe", "blocking", "auto"}
VALID_EFFECTIVE_MODES = {"observe", "blocking"}


def default_final_risk_validation_config(raw: dict[str, Any] | None) -> dict[str, Any]:
    cfg = dict(raw or {})
    mode = str(cfg.get("mode") or "observe").strip().lower()
    if mode not in VALID_FINAL_RISK_VALIDATION_MODES:
        mode = "observe"
    cfg["mode"] = mode
    cfg.setdefault("full_auto_effective_mode", "blocking")
    cfg.setdefault("semi_auto_effective_mode", "observe")
    cfg.setdefault("material_drift_threshold", 0.015)
    cfg.setdefault("threshold_basis", "operator_default_pending_observe_mode_distribution")
    cfg.setdefault("require_human_confirmation_for_conditional_material_drift", True)
    return cfg


def resolve_final_risk_validation_mode(config: dict[str, Any] | None, *, auth_mode: str | None) -> str:
    """Resolve the runtime mode from static config and authorization mode."""
    cfg = default_final_risk_validation_config(config)
    mode = str(cfg.get("mode") or "observe").strip().lower()
    if mode in VALID_EFFECTIVE_MODES:
        return mode

    auth = str(auth_mode or "").strip().upper()
    key = "full_auto_effective_mode" if auth == "FULL_AUTO" else "semi_auto_effective_mode"
    effective = str(cfg.get(key) or "").strip().lower()
    return effective if effective in VALID_EFFECTIVE_MODES else ("blocking" if auth == "FULL_AUTO" else "observe")


def format_final_risk_validation_config(config: dict[str, Any]) -> str:
    cfg = default_final_risk_validation_config(config)
    return (
        "Final Risk Validation\n"
        f"  mode: {cfg['mode']}\n"
        f"  full_auto_effective_mode: {cfg['full_auto_effective_mode']}\n"
        f"  material_drift_threshold: {float(cfg['material_drift_threshold']):.1%}\n"
        f"  threshold_basis: {cfg['threshold_basis']}"
    )
