"""Pure helpers for Portfolio Construction promotion gate configuration."""
from __future__ import annotations


VALID_PORTFOLIO_CONSTRUCTION_MODES = {"shadow", "candidate", "gated"}


def default_pc_promotion_config(raw: dict | None) -> dict:
    out = dict(raw or {})
    mode = str(out.get("portfolio_construction_mode") or "shadow").strip().lower()
    if mode not in VALID_PORTFOLIO_CONSTRUCTION_MODES:
        mode = "shadow"
    out["portfolio_construction_mode"] = mode
    out.setdefault("enabled", mode != "shadow")
    out.setdefault("require_manual_approval", False)
    if "min_shadow_cycles" not in out and "min_cycles" in out:
        out["min_shadow_cycles"] = out.get("min_cycles")
    out.setdefault("min_shadow_cycles", 20)
    out.setdefault("min_cycles", out["min_shadow_cycles"])
    out.setdefault("min_pass_rate", 0.90)
    out.setdefault("max_material_diff", 0.015)
    out.setdefault("max_turnover_diff", 0.02)
    out.setdefault("require_semi_auto_gated_before_full_auto", True)
    out.setdefault("min_gated_semi_auto_confirmed_cycles", 5)
    out.setdefault("allow_full_auto_gated", False)
    return out


def format_pc_promotion_config(config: dict) -> str:
    mode = "manual" if config.get("require_manual_approval") else "auto"
    pc_mode = str(config.get("portfolio_construction_mode") or "shadow")
    return (
        "⚙️ Portfolio Construction promotion gate\n"
        f"  construction_mode: {pc_mode}\n"
        f"  enabled: {bool(config.get('enabled'))}\n"
        f"  approval_mode: {mode}\n"
        f"  min_shadow_cycles: {_config_int(config, 'min_shadow_cycles', 20)}\n"
        f"  min_pass_rate: {_config_float(config, 'min_pass_rate', 0.90):.0%}\n"
        f"  max_material_diff: {_config_float(config, 'max_material_diff', 0.015):.1%}\n"
        f"  max_turnover_diff: {_config_float(config, 'max_turnover_diff', 0.02):.1%}\n"
        f"  min_gated_semi_auto_confirmed_cycles: {_config_int(config, 'min_gated_semi_auto_confirmed_cycles', 5)}\n"
        f"  allow_full_auto_gated: {bool(config.get('allow_full_auto_gated'))}\n"
        "  execution_authority: none"
    )


def _config_int(config: dict, key: str, default: int) -> int:
    if key not in config or config.get(key) is None:
        return default
    try:
        return int(config.get(key))
    except (TypeError, ValueError):
        return default


def _config_float(config: dict, key: str, default: float) -> float:
    if key not in config or config.get(key) is None:
        return default
    try:
        return float(config.get(key))
    except (TypeError, ValueError):
        return default
