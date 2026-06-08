"""Operator-controlled halt latch for the trading loop."""

from __future__ import annotations

from datetime import datetime
from typing import Any


CONFIG_KEY = "operator_halt_state"


def normalize_operator_halt_state(value: Any) -> dict[str, Any]:
    """Return a fail-safe normalized operator halt state."""
    if value is None:
        return _fail_safe_state("operator_halt_state_missing")
    if not isinstance(value, dict):
        return _fail_safe_state("operator_halt_state_malformed")

    raw_halted = value.get("halted")
    if isinstance(raw_halted, bool):
        halted = raw_halted
        fail_safe = False
    elif isinstance(raw_halted, str) and raw_halted.lower() in {"true", "false"}:
        halted = raw_halted.lower() == "true"
        fail_safe = False
    else:
        return _fail_safe_state("operator_halt_state_missing_or_invalid_halted")

    reason = str(value.get("reason") or "").strip()
    updated_at = value.get("updated_at")
    updated_by = str(value.get("updated_by") or value.get("operator") or "unknown").strip()
    return {
        "halted": halted,
        "reason": reason,
        "updated_at": updated_at,
        "updated_by": updated_by,
        "source": "system_config",
        "fail_safe": fail_safe,
    }


def build_operator_halt_state(
    *,
    halted: bool,
    reason: str = "",
    updated_by: str = "operator",
    now: datetime | None = None,
) -> dict[str, Any]:
    ts = (now or datetime.utcnow()).isoformat()
    return {
        "halted": bool(halted),
        "reason": str(reason or "").strip(),
        "updated_at": ts,
        "updated_by": str(updated_by or "operator"),
    }


def format_operator_halt_status(state: dict[str, Any]) -> str:
    normalized = normalize_operator_halt_state(state)
    status = "HALTED" if normalized.get("halted") else "running"
    reason = normalized.get("reason") or "none"
    updated_at = normalized.get("updated_at") or "unknown"
    updated_by = normalized.get("updated_by") or "unknown"
    fail_safe = " yes" if normalized.get("fail_safe") else " no"
    return (
        f"Operator halt: {status}\n"
        f"  reason: {reason}\n"
        f"  updated_at: {updated_at}\n"
        f"  updated_by: {updated_by}\n"
        f"  fail_safe:{fail_safe}"
    )


def _fail_safe_state(reason: str) -> dict[str, Any]:
    return {
        "halted": True,
        "reason": reason,
        "updated_at": None,
        "updated_by": "system",
        "source": "fail_safe",
        "fail_safe": True,
    }
