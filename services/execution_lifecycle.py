"""Execution lifecycle helpers shared by preflight, QC ACK handling, and tests."""
from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any


class ExecutionSkipReason(str, Enum):
    THROTTLE_DEFERRED = "throttle_deferred"
    ACTIVE_EXECUTION_WAIT = "active_execution_wait"
    PREFLIGHT_BLOCKED = "preflight_blocked"
    GUARD_BLOCKED = "guard_blocked"


ACTIVE_EXECUTION_CONTROL_REASONS = {
    "active_command_in_progress",
    "already_in_progress",
    "deferred_by_active_execution",
    "duplicate_command_id",
    "reduce_only_override_candidate",
}

DEFAULT_EXECUTION_LIFECYCLE_CONFIG: dict[str, Any] = {
    "enabled": True,
    "mode": "observe",
    "ack_wait_seconds": 10,
    "timeout_reconciliation_grace_minutes": 20,
    "block_ordinary_commands_when_active_execution": True,
    "allow_reduce_only_override": True,
    "allow_emergency_override": True,
    "same_target_tolerance": 0.005,
    "per_ticker_reconciliation_tolerance": 0.005,
    "portfolio_gross_reconciliation_tolerance": 0.02,
    "max_active_execution_minutes": 60,
    "auto_cancel_stale_open_orders": False,
}

ACTIVE_EXECUTION_STATUSES = {"accepted", "orders_submitted", "partial"}


def default_execution_lifecycle_config(config: dict[str, Any] | None = None) -> dict[str, Any]:
    out = dict(DEFAULT_EXECUTION_LIFECYCLE_CONFIG)
    out.update(config or {})
    mode = str(out.get("mode") or "observe").lower().strip()
    out["mode"] = mode if mode in {"observe", "active", "strict", "off"} else "observe"
    out["enabled"] = bool(out.get("enabled", True))
    out["block_ordinary_commands_when_active_execution"] = bool(
        out.get("block_ordinary_commands_when_active_execution", True)
    )
    out["allow_reduce_only_override"] = bool(out.get("allow_reduce_only_override", True))
    out["allow_emergency_override"] = bool(out.get("allow_emergency_override", True))
    out["auto_cancel_stale_open_orders"] = bool(out.get("auto_cancel_stale_open_orders", False))
    for key in (
        "same_target_tolerance",
        "per_ticker_reconciliation_tolerance",
        "portfolio_gross_reconciliation_tolerance",
    ):
        out[key] = _float_or_default(out.get(key), DEFAULT_EXECUTION_LIFECYCLE_CONFIG[key])
    for key in ("ack_wait_seconds", "timeout_reconciliation_grace_minutes", "max_active_execution_minutes"):
        out[key] = max(int(_float_or_default(out.get(key), DEFAULT_EXECUTION_LIFECYCLE_CONFIG[key])), 1)
    return out


def classify_new_command_vs_active(
    *,
    new_target: dict[str, Any],
    active_target: dict[str, Any],
    actual_holdings: dict[str, Any],
    active_open_orders: int | None,
    same_target_tolerance: float = 0.005,
) -> str:
    """Classify a proposed ordinary command against the current active execution.

    This function is intentionally pure so QC and Agent tests can share the same
    semantics before PR5 wires the active-execution gate into preflight.
    """
    new_clean = clean_weight_map(new_target)
    active_clean = clean_weight_map(active_target)
    actual_clean = clean_weight_map(actual_holdings)
    if is_within_target_tolerance(new_clean, active_clean, same_target_tolerance):
        return "already_in_progress"
    if is_reduce_only_vs_actual(new_clean, actual_clean):
        return "reduce_only_override_candidate"
    if int(active_open_orders or 0) > 0:
        return "active_command_in_progress"
    return "previous_command_pending_reconciliation"


def evaluate_active_execution_gate(
    *,
    target_weights: dict[str, Any],
    active_execution: dict[str, Any] | None,
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    cfg = default_execution_lifecycle_config(config)
    active = active_execution or {}
    mode = str(cfg.get("mode") or "observe")
    if not cfg.get("enabled") or mode == "off":
        return _active_gate_result(
            allowed=True,
            status="disabled",
            config=cfg,
            active_execution=active,
            classification=None,
            skip_reason=None,
            stale_active_execution=evaluate_stale_active_execution(active, cfg),
        )
    if not _is_active_execution(active):
        return _active_gate_result(
            allowed=True,
            status="pass",
            config=cfg,
            active_execution=active,
            classification=None,
            skip_reason=None,
            stale_active_execution=evaluate_stale_active_execution(active, cfg),
        )

    stale = evaluate_stale_active_execution(active, cfg)
    classification = classify_new_command_vs_active(
        new_target=target_weights,
        active_target=active.get("target_weights") or {},
        actual_holdings=active.get("holdings_weights") or {},
        active_open_orders=active.get("open_order_count"),
        same_target_tolerance=float(cfg["same_target_tolerance"]),
    )
    bypass = classification == "reduce_only_override_candidate" and bool(cfg.get("allow_reduce_only_override"))
    should_block = (
        bool(cfg.get("block_ordinary_commands_when_active_execution"))
        and not bypass
        and classification in {"already_in_progress", "active_command_in_progress", "previous_command_pending_reconciliation"}
    )
    if not should_block:
        return _active_gate_result(
            allowed=True,
            status="reduce_only_override_allowed" if bypass else "pass",
            config=cfg,
            active_execution=active,
            classification=classification,
            skip_reason=None,
            stale_active_execution=stale,
        )

    would_defer = True
    allowed = mode == "observe"
    gate_status = "would_defer_by_active_execution" if allowed else "deferred_by_active_execution"
    if not allowed and mode == "strict" and stale.get("is_stale"):
        gate_status = "stale_active_execution"
    return _active_gate_result(
        allowed=allowed,
        status=gate_status,
        config=cfg,
        active_execution=active,
        classification=classification,
        skip_reason=ExecutionSkipReason.ACTIVE_EXECUTION_WAIT.value,
        would_defer=would_defer,
        stale_active_execution=stale,
    )


def evaluate_stale_active_execution(
    active_execution: dict[str, Any] | None,
    config: dict[str, Any] | None = None,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Classify stale active execution without mutating state."""
    cfg = default_execution_lifecycle_config(config or {})
    active = active_execution or {}
    command_id = str(active.get("command_id") or active.get("active_command_id") or "").strip()
    if not command_id:
        return {
            "is_stale": False,
            "reason": "no_active_command",
            "command_id": None,
            "elapsed_minutes": None,
            "threshold_minutes": cfg["max_active_execution_minutes"],
            "auto_action": "none",
            "auto_cancel": False,
        }

    started_at = _datetime_from_any(
        active.get("started_at")
        or active.get("qc_ack_at")
        or active.get("executed_at")
        or active.get("recorded_at")
    )
    if started_at is None:
        return {
            "is_stale": False,
            "reason": "missing_started_at",
            "command_id": command_id,
            "elapsed_minutes": None,
            "threshold_minutes": cfg["max_active_execution_minutes"],
            "auto_action": "none",
            "auto_cancel": False,
        }

    checked_at = now or datetime.utcnow()
    if checked_at.tzinfo is not None:
        checked_at = checked_at.replace(tzinfo=None)
    elapsed = max((checked_at - started_at).total_seconds() / 60.0, 0.0)
    threshold = int(cfg["max_active_execution_minutes"])
    open_order_count = _int_or_none(active.get("open_order_count")) or 0
    has_open_orders = bool(active.get("has_open_orders")) or open_order_count > 0
    if elapsed < threshold:
        return {
            "is_stale": False,
            "reason": "within_threshold",
            "command_id": command_id,
            "elapsed_minutes": round(elapsed, 1),
            "threshold_minutes": threshold,
            "open_order_count": open_order_count,
            "auto_action": "none",
            "auto_cancel": False,
        }

    if has_open_orders:
        return {
            "is_stale": True,
            "reason": "open_orders_not_filling",
            "command_id": command_id,
            "elapsed_minutes": round(elapsed, 1),
            "threshold_minutes": threshold,
            "open_order_count": open_order_count,
            "auto_action": "alert_operator",
            "auto_cancel": bool(cfg.get("auto_cancel_stale_open_orders")),
            "operator_action": "check_dashboard_then_cancel_orders_if_orders_are_stuck",
        }
    return {
        "is_stale": True,
        "reason": "no_open_orders_but_unreconciled",
        "command_id": command_id,
        "elapsed_minutes": round(elapsed, 1),
        "threshold_minutes": threshold,
        "open_order_count": open_order_count,
        "auto_action": "trigger_reconciliation",
        "auto_cancel": False,
        "operator_action": "force_reconcile_if_heartbeat_does_not_close_lifecycle",
    }


async def load_active_execution_command() -> dict[str, Any] | None:
    """Load the latest active execution identity from QC account truth."""
    from sqlalchemy import desc, select

    from db.models import AccountStateSnapshot, ExecutionLog
    from db.session import AsyncSessionLocal

    async with AsyncSessionLocal() as db:
        snapshot = (
            await db.execute(
                select(AccountStateSnapshot)
                .order_by(desc(AccountStateSnapshot.recorded_at))
                .limit(1)
            )
        ).scalar_one_or_none()
        if not snapshot:
            return None
        active_command_id = (
            str(getattr(snapshot, "active_command_id", "") or "").strip()
            or str((getattr(snapshot, "raw_snapshot", None) or {}).get("active_command_id") or "").strip()
            or str((getattr(snapshot, "raw_snapshot", None) or {}).get("last_command_id") or "").strip()
        )
        if not active_command_id:
            return None
        row = (
            await db.execute(select(ExecutionLog).where(ExecutionLog.command_id == active_command_id))
        ).scalar_one_or_none()

    status = str(getattr(snapshot, "active_execution_status", "") or "").strip().lower()
    open_orders = getattr(snapshot, "open_order_count", None)
    active = {
        "command_id": active_command_id,
        "status": status or str(getattr(row, "qc_status", "") or "").lower().strip(),
        "open_order_count": int(open_orders or 0) if open_orders is not None else None,
        "has_open_orders": bool(getattr(snapshot, "has_open_orders", False)),
        "target_weights": getattr(snapshot, "target_weights", None) or {},
        "holdings_weights": getattr(snapshot, "holdings_weights", None) or {},
        "recorded_at": str(getattr(snapshot, "recorded_at", "") or ""),
        "qc_status": getattr(row, "qc_status", None),
        "executed_at": str(getattr(row, "executed_at", "") or "") if row else None,
        "qc_ack_at": str(getattr(row, "qc_ack_at", "") or "") if row else None,
    }
    return active if _is_active_execution(active) else None


def is_within_target_tolerance(
    lhs: dict[str, Any],
    rhs: dict[str, Any],
    tolerance: float = 0.005,
) -> bool:
    left = clean_weight_map(lhs)
    right = clean_weight_map(rhs)
    tickers = (set(left) | set(right)) - {"CASH"}
    if not tickers:
        return False
    return all(abs(float(left.get(t, 0.0)) - float(right.get(t, 0.0))) <= tolerance for t in tickers)


def is_reduce_only_vs_actual(
    new_target: dict[str, Any],
    actual_holdings: dict[str, Any],
    tolerance: float = 0.001,
) -> bool:
    target = clean_weight_map(new_target)
    actual = clean_weight_map(actual_holdings)
    if not target:
        return False
    for ticker, target_weight in target.items():
        if ticker == "CASH":
            continue
        current = float(actual.get(ticker, 0.0) or 0.0)
        if float(target_weight or 0.0) > current + tolerance:
            return False
    return True


def clean_weight_map(weights: dict[str, Any] | None) -> dict[str, float]:
    out: dict[str, float] = {}
    for ticker, value in (weights or {}).items():
        key = str(ticker or "").upper().strip()
        if not key:
            continue
        try:
            out[key] = max(float(value or 0.0), 0.0)
        except (TypeError, ValueError):
            out[key] = 0.0
    return out


def _is_active_execution(active: dict[str, Any]) -> bool:
    if not active:
        return False
    status = str(active.get("status") or active.get("qc_status") or "").lower().strip()
    open_orders = active.get("open_order_count")
    has_open = bool(active.get("has_open_orders"))
    if open_orders is not None:
        try:
            has_open = has_open or int(open_orders or 0) > 0
        except (TypeError, ValueError):
            pass
    return bool(active.get("command_id")) and (status in ACTIVE_EXECUTION_STATUSES or has_open)


def _active_gate_result(
    *,
    allowed: bool,
    status: str,
    config: dict[str, Any],
    active_execution: dict[str, Any],
    classification: str | None,
    skip_reason: str | None,
    would_defer: bool = False,
    stale_active_execution: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "allowed": allowed,
        "status": status,
        "would_defer": bool(would_defer),
        "skip_reason": skip_reason,
        "classification": classification,
        "active_command_id": active_execution.get("command_id"),
        "open_order_count": active_execution.get("open_order_count"),
        "active_execution": active_execution,
        "stale_active_execution": stale_active_execution or {},
        "config": config,
        "execution_effect": "active_block" if not allowed else "diagnostic_only" if would_defer else "none",
    }


def _float_or_default(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _int_or_none(value: Any) -> int | None:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _datetime_from_any(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=None) if value.tzinfo is not None else value
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed.replace(tzinfo=None) if parsed.tzinfo is not None else parsed
