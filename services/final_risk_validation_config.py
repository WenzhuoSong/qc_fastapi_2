"""Pure helpers for final risk validation runtime configuration."""
from __future__ import annotations

from typing import Any


VALID_FINAL_RISK_VALIDATION_MODES = {"observe", "blocking"}


def default_final_risk_validation_config(raw: dict[str, Any] | None) -> dict[str, Any]:
    cfg = dict(raw or {})
    mode = str(cfg.get("mode") or "observe").strip().lower()
    if mode not in VALID_FINAL_RISK_VALIDATION_MODES:
        mode = "observe"
    cfg["mode"] = mode
    cfg.setdefault("material_drift_threshold", 0.015)
    cfg.setdefault("threshold_basis", "operator_default_pending_observe_mode_distribution")
    cfg.setdefault("require_human_confirmation_for_conditional_material_drift", True)
    return cfg


def format_final_risk_validation_config(config: dict[str, Any]) -> str:
    cfg = default_final_risk_validation_config(config)
    return (
        "Final Risk Validation\n"
        f"  mode: {cfg['mode']}\n"
        f"  material_drift_threshold: {float(cfg['material_drift_threshold']):.1%}\n"
        f"  threshold_basis: {cfg['threshold_basis']}"
    )
