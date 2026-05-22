"""Pure helpers for Portfolio Construction promotion gate configuration."""
from __future__ import annotations


def default_pc_promotion_config(raw: dict | None) -> dict:
    out = dict(raw or {})
    out.setdefault("enabled", True)
    out.setdefault("require_manual_approval", False)
    out.setdefault("min_cycles", 20)
    out.setdefault("min_pass_rate", 0.80)
    return out


def format_pc_promotion_config(config: dict) -> str:
    mode = "manual" if config.get("require_manual_approval") else "auto"
    return (
        "⚙️ Portfolio Construction promotion gate\n"
        f"  enabled: {bool(config.get('enabled'))}\n"
        f"  approval_mode: {mode}\n"
        f"  min_cycles: {int(config.get('min_cycles') or 20)}\n"
        f"  min_pass_rate: {float(config.get('min_pass_rate') or 0.80):.0%}\n"
        "  execution_authority: none"
    )
