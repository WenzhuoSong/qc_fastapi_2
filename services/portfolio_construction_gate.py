"""Pure Portfolio Construction gate helpers for target-builder integration."""
from __future__ import annotations

from services.pc_promotion_config import default_pc_promotion_config
from services.weight_source_contract import (
    PC_CANDIDATE_KEY,
    PC_SHADOW_KEY,
    weight_source_contract_summary,
)


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
        "configured_mode": mode,
        "effective_mode": "deterministic_target_builder",
        "gate_status": gate.get("status"),
        "gate_eligible": bool(gate.get("eligible")),
        "gate_blockers": _as_string_list(gate.get("blockers")),
        "gate_reason": gate.get("reason"),
        "construction_weights": None,
        "construction_source": None,
        "construction_weight_source": PC_SHADOW_KEY,
        "target_builder_input_key": None,
        "construction_participated": False,
        "execution_effect": "none",
        "weight_source_contract": weight_source_contract_summary(),
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
        "effective_mode": "portfolio_construction_gated",
        "construction_weights": weights,
        "construction_source": str(source),
        "construction_weight_source": PC_CANDIDATE_KEY,
        "target_builder_input_key": PC_CANDIDATE_KEY,
        "construction_participated": True,
        "execution_effect": "target_builder_input",
        "blocked_reason": None,
    }


def _as_string_list(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        return [str(item) for item in value if str(item)]
    text = str(value)
    return [text] if text else []
