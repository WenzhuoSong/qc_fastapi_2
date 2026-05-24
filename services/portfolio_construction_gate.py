"""Pure Portfolio Construction gate helpers for target-builder integration."""
from __future__ import annotations

from services.pc_promotion_config import default_pc_promotion_config


def construction_input_for_target_builder(
    *,
    portfolio_construction_payload: dict | None,
    promotion_gate: dict | None,
    config: dict | None,
) -> dict:
    """Return construction weights only when gated mode is explicitly eligible."""
    pc_config = default_pc_promotion_config(config or {})
    mode = str(pc_config.get("portfolio_construction_mode") or "shadow")
    payload = portfolio_construction_payload or {}
    gate = promotion_gate or {}
    weights = payload.get("target_weights") if isinstance(payload, dict) else None
    base = {
        "mode": mode,
        "gate_status": gate.get("status"),
        "gate_eligible": bool(gate.get("eligible")),
        "construction_weights": None,
        "construction_source": None,
        "construction_participated": False,
        "execution_effect": "none",
    }

    if mode != "gated":
        return {**base, "blocked_reason": f"mode_{mode}_not_gated"}
    if not bool(gate.get("eligible")):
        return {**base, "blocked_reason": "promotion_gate_not_eligible"}
    if not isinstance(weights, dict) or not weights:
        return {**base, "blocked_reason": "construction_weights_missing"}

    source = (
        payload.get("construction_source")
        or (payload.get("diagnostics") or {}).get("construction_source")
        or "portfolio_construction"
    )
    return {
        **base,
        "construction_weights": weights,
        "construction_source": str(source),
        "construction_participated": True,
        "execution_effect": "target_builder_input",
        "blocked_reason": None,
    }
