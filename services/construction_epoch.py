"""Portfolio-construction epoch diagnostics.

Conviction samples are path-dependent: a signal frozen under shadow
construction should not be merged with a signal frozen under gated
construction.  This module creates a small, stable epoch fingerprint that can
travel through diagnostics without requiring a schema migration.
"""
from __future__ import annotations

import hashlib
import json
from datetime import date, datetime
from typing import Any


CONSTRUCTION_EPOCH_CONTRACT_VERSION = "construction_epoch_v1"
DEFAULT_CONSTRUCTION_OBJECTIVE_VERSION = "maximize_signal_weighted_effective_n_v1"
UNKNOWN_CONSTRUCTION_EPOCH_ID = "unknown"


def build_construction_epoch(
    *,
    pc_mode: str | None = None,
    construction_objective_version: str | None = None,
    policy_version: str | None = None,
    promotion_config: dict[str, Any] | None = None,
    promotion_config_hash: str | None = None,
    source: str = "runtime",
) -> dict[str, Any]:
    """Build a deterministic epoch payload for signal/conviction diagnostics."""
    config = promotion_config if isinstance(promotion_config, dict) else {}
    normalized_pc_mode = _clean_str(
        pc_mode
        or config.get("portfolio_construction_mode")
        or "unknown"
    )
    objective_version = _clean_str(
        construction_objective_version
        or _objective_version_from_config(config)
        or DEFAULT_CONSTRUCTION_OBJECTIVE_VERSION
    )
    normalized_policy_version = _clean_str(policy_version or config.get("policy_version") or "unknown")
    config_hash = _clean_str(
        promotion_config_hash
        or (stable_hash_json(config) if config else "none")
    )
    fingerprint = {
        "pc_mode": normalized_pc_mode,
        "construction_objective_version": objective_version,
        "policy_version": normalized_policy_version,
        "promotion_config_hash": config_hash,
    }
    return {
        "contract_version": CONSTRUCTION_EPOCH_CONTRACT_VERSION,
        "epoch_id": stable_hash_json(fingerprint)[:16],
        "pc_mode": normalized_pc_mode,
        "construction_objective_version": objective_version,
        "policy_version": normalized_policy_version,
        "promotion_config_hash": config_hash,
        "source": _clean_str(source or "runtime"),
        "execution_authority": "none",
        "target_weight_mutation": "none",
    }


def build_historical_replay_construction_epoch() -> dict[str, Any]:
    """Return the fixed epoch used for yfinance historical signal replay."""
    return build_construction_epoch(
        pc_mode="historical_replay",
        construction_objective_version="historical_replay_no_pc_v1",
        policy_version="historical",
        promotion_config_hash="historical_replay",
        source="yfinance_replay",
    )


def unknown_construction_epoch(reason: str = "missing_construction_epoch") -> dict[str, Any]:
    """Return a stable fallback for legacy rows without epoch diagnostics."""
    return {
        "contract_version": CONSTRUCTION_EPOCH_CONTRACT_VERSION,
        "epoch_id": UNKNOWN_CONSTRUCTION_EPOCH_ID,
        "pc_mode": "unknown",
        "construction_objective_version": "unknown",
        "policy_version": "unknown",
        "promotion_config_hash": "unknown",
        "source": "legacy_or_missing",
        "reason": reason,
        "execution_authority": "none",
        "target_weight_mutation": "none",
    }


def construction_epoch_from_diagnostics(diagnostics: Any) -> dict[str, Any]:
    """Extract and normalize an epoch payload from a diagnostics dict."""
    if not isinstance(diagnostics, dict):
        return unknown_construction_epoch("diagnostics_not_dict")
    epoch = diagnostics.get("construction_epoch")
    if not isinstance(epoch, dict):
        return unknown_construction_epoch("missing_construction_epoch")
    epoch_id = _clean_str(epoch.get("epoch_id") or "")
    if not epoch_id:
        return unknown_construction_epoch("missing_epoch_id")
    return {
        "contract_version": _clean_str(epoch.get("contract_version") or CONSTRUCTION_EPOCH_CONTRACT_VERSION),
        "epoch_id": epoch_id,
        "pc_mode": _clean_str(epoch.get("pc_mode") or "unknown"),
        "construction_objective_version": _clean_str(
            epoch.get("construction_objective_version") or "unknown"
        ),
        "policy_version": _clean_str(epoch.get("policy_version") or "unknown"),
        "promotion_config_hash": _clean_str(epoch.get("promotion_config_hash") or "unknown"),
        "source": _clean_str(epoch.get("source") or "unknown"),
        "execution_authority": "none",
        "target_weight_mutation": "none",
    }


def construction_epoch_from_signal(signal: Any) -> dict[str, Any]:
    return construction_epoch_from_diagnostics(getattr(signal, "diagnostics", None))


def construction_epoch_id_from_profile(value: Any) -> str:
    diagnostics = _record_get(value, "diagnostics")
    epoch = construction_epoch_from_diagnostics(diagnostics)
    return str(epoch.get("epoch_id") or UNKNOWN_CONSTRUCTION_EPOCH_ID)


def stable_hash_json(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(_json_ready(value), sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _objective_version_from_config(config: dict[str, Any]) -> str | None:
    objective = config.get("construction_objective")
    if isinstance(objective, dict):
        raw = objective.get("version") or objective.get("primary")
        if raw:
            return str(raw)
    raw = config.get("construction_objective_version")
    return str(raw) if raw else None


def _record_get(value: Any, key: str) -> Any:
    if isinstance(value, dict):
        return value.get(key)
    return getattr(value, key, None)


def _clean_str(value: Any) -> str:
    return str(value or "").strip() or "unknown"


def _json_ready(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in sorted(value.items())}
    if isinstance(value, (list, tuple, set)):
        return [_json_ready(item) for item in value]
    return value
