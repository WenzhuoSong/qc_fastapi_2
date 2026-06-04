"""Final execution preflight checks before commands are sent to QC."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from services.execution_policy import evaluate_policy


DEFAULT_COMMAND_PREFLIGHT_CONFIG = {
    "max_daily_commands": 12,
    "max_gross_turnover_per_day": 1.50,
    "risk_reduce_reserved_commands": 4,
    "risk_reduce_gross_turnover_per_day": 0.25,
    "max_buy_delta": 0.15,
    "max_sell_delta": 0.20,
    "recent_same_target_dedupe_minutes": 5,
    "recent_same_target_dedupe_tolerance": 0.005,
}


COMMAND_PREFLIGHT_BLOCKER_LABELS = {
    "command_id_present": "missing command id",
    "analysis_id_present": "missing analysis id",
    "command_id_idempotent": "duplicate command id",
    "analysis_id_not_submitted": "analysis already submitted",
    "policy_version_present": "missing policy version",
    "policy_alignment_confirmed": "policy alignment not confirmed",
    "daily_command_count_ok": "daily command cap exceeded",
    "daily_gross_turnover_ok": "daily turnover cap exceeded",
    "buy_delta_ok": "buy delta cap exceeded",
    "sell_delta_ok": "sell delta cap exceeded",
}

_PERCENT_CHECKS = {
    "daily_gross_turnover_ok",
    "buy_delta_ok",
    "sell_delta_ok",
}


def preflight_execution_weights(weights: dict[str, Any]) -> dict[str, Any]:
    """Return blocking policy violations for a proposed execution payload."""
    policy = evaluate_policy(weights=weights)
    cap_violations = policy["cap_violations"]
    group_violations = policy["group_violations"]
    return {
        "allowed": bool(policy["allowed"]),
        "cap_violations": cap_violations,
        "group_violations": group_violations,
        "policy_version": policy["policy_version"],
        "policy_evaluation": policy,
    }


async def preflight_execution_command(
    *,
    command_id: str,
    analysis_id: int | None,
    target_weights: dict[str, Any],
    current_weights: dict[str, Any] | None,
    policy_version: str | None,
    policy_sync_result: dict[str, Any] | None,
    policy_alignment_result: dict[str, Any] | None = None,
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Return command-level hard blocks before SetWeights is sent to QC."""
    from services.execution_log_store import (
        command_submission_state,
        summarize_today_execution_activity,
    )

    cfg = _command_config(config)
    target = _clean_weights(target_weights)
    current = _clean_weights(current_weights or {})
    metrics = command_weight_delta_metrics(target, current)
    command_class = _command_class_from_metrics(metrics)
    submission_state = await command_submission_state(command_id=command_id, analysis_id=analysis_id)
    today = await summarize_today_execution_activity()
    policy_sync_ack_status = _policy_sync_ack_status(policy_sync_result)
    policy_transport_ok = _policy_alignment_ok(policy_alignment_result)
    command_count_limit = _daily_command_limit(cfg, command_class)
    gross_turnover_limit = _daily_turnover_limit(cfg, command_class)

    checks: dict[str, dict[str, Any]] = {
        "command_id_present": {
            "pass": bool(str(command_id or "").strip()),
            "actual": command_id,
            "threshold": "non-empty command_id",
        },
        "analysis_id_present": {
            "pass": analysis_id is not None,
            "actual": analysis_id,
            "threshold": "analysis_id required",
        },
        "command_id_idempotent": {
            "pass": not bool(submission_state.get("command_id_exists")),
            "actual": submission_state.get("command_id_status"),
            "threshold": "command_id not previously used",
        },
        "analysis_id_not_submitted": {
            "pass": not bool(submission_state.get("analysis_id_submitted")),
            "actual": submission_state.get("analysis_command_id"),
            "threshold": "analysis_id has no prior submitted command",
        },
        "policy_version_present": {
            "pass": bool(str(policy_version or "").strip()),
            "actual": policy_version,
            "threshold": "FastAPI policy_version required in command payload",
        },
        "policy_alignment_confirmed": {
            "pass": policy_transport_ok,
            "actual": {
                "policy_sync": policy_sync_result,
                "policy_sync_ack_status": policy_sync_ack_status,
                "policy_alignment": policy_alignment_result,
            },
            "threshold": "recent account_state_guard policy alignment required before SetWeights",
        },
        "daily_command_count_ok": {
            "pass": int(today.get("command_count") or 0) < int(command_count_limit),
            "actual": int(today.get("command_count") or 0),
            "threshold": int(command_count_limit),
            "base_threshold": int(cfg["max_daily_commands"]),
            "reserve_applied": int(command_count_limit) - int(cfg["max_daily_commands"]),
            "bucket": command_class,
        },
        "daily_gross_turnover_ok": {
            "pass": float(today.get("gross_turnover") or 0.0) + metrics["gross_turnover"] <= float(gross_turnover_limit) + 1e-12,
            "actual": round(float(today.get("gross_turnover") or 0.0) + metrics["gross_turnover"], 6),
            "threshold": float(gross_turnover_limit),
            "base_threshold": float(cfg["max_gross_turnover_per_day"]),
            "reserve_applied": round(float(gross_turnover_limit) - float(cfg["max_gross_turnover_per_day"]), 6),
            "bucket": command_class,
        },
        "buy_delta_ok": {
            "pass": metrics["buy_delta"] <= float(cfg["max_buy_delta"]) + 1e-12,
            "actual": metrics["buy_delta"],
            "threshold": float(cfg["max_buy_delta"]),
        },
        "sell_delta_ok": {
            "pass": metrics["sell_delta"] <= float(cfg["max_sell_delta"]) + 1e-12,
            "actual": metrics["sell_delta"],
            "threshold": float(cfg["max_sell_delta"]),
        },
    }
    blockers = [name for name, row in checks.items() if not row["pass"]]
    return {
        "allowed": not blockers,
        "command_id": command_id,
        "analysis_id": analysis_id,
        "policy_version": policy_version,
        "checks": checks,
        "blockers": blockers,
        "metrics": metrics,
        "today": today,
        "config": cfg,
        "command_class": command_class,
        "execution_authority": "hard_block" if blockers else "allowed",
    }


async def check_recent_same_target_dedupe(
    *,
    proposed_target: dict[str, Any],
    command_id: str,
    lookback_minutes: int = 5,
    tolerance: float = 0.005,
) -> dict[str, Any]:
    """Return a not-send decision when a recent reconciled command has the same target."""
    from sqlalchemy import desc, select

    from db.models import CommandLifecycleEvent, ExecutionLog
    from db.session import AsyncSessionLocal
    from services.execution_lifecycle import is_within_target_tolerance

    clean_command_id = str(command_id or "").strip()
    cutoff = datetime.now(UTC).replace(tzinfo=None) - timedelta(minutes=max(int(lookback_minutes or 0), 1))
    async with AsyncSessionLocal() as db:
        lifecycle_rows = (
            await db.execute(
                select(CommandLifecycleEvent)
                .where(CommandLifecycleEvent.event_type == "reconciled")
                .where(CommandLifecycleEvent.event_time >= cutoff)
                .where(CommandLifecycleEvent.command_id != clean_command_id)
                .order_by(desc(CommandLifecycleEvent.event_time), desc(CommandLifecycleEvent.id))
                .limit(5)
            )
        ).scalars().all()
        for event in lifecycle_rows:
            row = (
                await db.execute(
                    select(ExecutionLog).where(ExecutionLog.command_id == event.command_id)
                )
            ).scalar_one_or_none()
            recent_target = _target_weights_from_execution_row(row)
            if recent_target and is_within_target_tolerance(proposed_target, recent_target, tolerance):
                return {
                    "should_send": False,
                    "reason": "recent_same_target_reconciled",
                    "reference_command_id": event.command_id,
                    "reference_reconciled_at": _iso_or_none(getattr(event, "event_time", None)),
                    "lookback_minutes": int(lookback_minutes),
                    "tolerance": float(tolerance),
                }

    return {
        "should_send": True,
        "reason": None,
        "lookback_minutes": int(lookback_minutes),
        "tolerance": float(tolerance),
    }


def format_command_preflight_blockers(preflight_result: dict[str, Any]) -> str:
    """Return operator-facing failed checks with actual/threshold values."""
    blockers = list(preflight_result.get("blockers") or [])
    checks = preflight_result.get("checks") or {}
    if not blockers:
        return "No failed command preflight checks."

    lines: list[str] = []
    for name in blockers:
        check = checks.get(name) or {}
        label = COMMAND_PREFLIGHT_BLOCKER_LABELS.get(name, str(name))
        actual = _format_check_value(name, check.get("actual"))
        threshold = _format_check_value(name, check.get("threshold"))
        bucket = check.get("bucket")
        reserve = check.get("reserve_applied")
        reserve_text = ""
        if bucket == "risk_reduce" and reserve not in (None, 0, 0.0):
            reserve_text = f", reserve_applied={_format_check_value(name, reserve)}"
        lines.append(f"- {label}: actual={actual}, threshold={threshold}{reserve_text} ({name})")
    return "\n".join(lines)


def command_weight_delta_metrics(
    target_weights: dict[str, Any],
    current_weights: dict[str, Any] | None,
) -> dict[str, float]:
    target = _clean_weights(target_weights)
    current = _clean_weights(current_weights or {})
    buy_delta = 0.0
    sell_delta = 0.0
    for ticker in sorted((set(target) | set(current)) - {"CASH"}):
        delta = float(target.get(ticker, 0.0) or 0.0) - float(current.get(ticker, 0.0) or 0.0)
        if delta > 0:
            buy_delta += delta
        elif delta < 0:
            sell_delta += abs(delta)
    gross = buy_delta + sell_delta
    return {
        "buy_delta": round(buy_delta, 6),
        "sell_delta": round(sell_delta, 6),
        "gross_turnover": round(gross / 2.0, 6),
    }


def _command_class_from_metrics(metrics: dict[str, Any]) -> str:
    """Classify commands for preflight budget purposes."""
    try:
        buy_delta = float(metrics.get("buy_delta") or 0.0)
        sell_delta = float(metrics.get("sell_delta") or 0.0)
    except (TypeError, ValueError):
        return "ordinary_rebalance"
    if buy_delta <= 1e-12 and sell_delta > 1e-12:
        return "risk_reduce"
    return "ordinary_rebalance"


def _daily_command_limit(cfg: dict[str, Any], command_class: str) -> int:
    base = int(cfg["max_daily_commands"])
    if command_class == "risk_reduce":
        return base + max(int(cfg.get("risk_reduce_reserved_commands") or 0), 0)
    return base


def _daily_turnover_limit(cfg: dict[str, Any], command_class: str) -> float:
    base = float(cfg["max_gross_turnover_per_day"])
    if command_class == "risk_reduce":
        return base + max(float(cfg.get("risk_reduce_gross_turnover_per_day") or 0.0), 0.0)
    return base


def _format_check_value(check_name: str, value: Any) -> str:
    if value is None:
        return "none"
    if check_name in _PERCENT_CHECKS:
        try:
            return f"{float(value) * 100:.2f}%"
        except (TypeError, ValueError):
            return str(value)
    if isinstance(value, float):
        return f"{value:.6g}"
    return str(value)


def _policy_sync_ack_status(policy_sync_result: dict[str, Any] | None) -> str | None:
    if not isinstance(policy_sync_result, dict):
        return None
    direct = policy_sync_result.get("ack_status") or policy_sync_result.get("qc_status")
    if direct:
        return str(direct).lower().strip()
    ack = policy_sync_result.get("ack")
    if isinstance(ack, dict):
        value = ack.get("qc_status")
        return str(value).lower().strip() if value else None
    return None


def _policy_alignment_ok(policy_alignment_result: dict[str, Any] | None) -> bool:
    if not isinstance(policy_alignment_result, dict):
        return False
    return bool(policy_alignment_result.get("aligned"))


def _command_config(config: dict[str, Any] | None) -> dict[str, Any]:
    out = dict(DEFAULT_COMMAND_PREFLIGHT_CONFIG)
    for key, default in DEFAULT_COMMAND_PREFLIGHT_CONFIG.items():
        try:
            parsed = type(default)((config or {}).get(key, default))
        except (TypeError, ValueError):
            parsed = default
        out[key] = parsed
    return out


def _target_weights_from_execution_row(row: Any) -> dict[str, Any]:
    payload = getattr(row, "command_payload", None) or {}
    qc_response = getattr(row, "qc_response", None) or {}
    if isinstance(qc_response, dict):
        account_state = qc_response.get("account_state")
        for value in (
            qc_response.get("actual_target_weights"),
            account_state.get("target_weights") if isinstance(account_state, dict) else None,
        ):
            if isinstance(value, dict) and value:
                return value
    if isinstance(payload, dict):
        for key in ("sent_weights", "proposed_weights"):
            value = payload.get(key)
            if isinstance(value, dict) and value:
                return value
    return {}


def _iso_or_none(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _clean_weights(weights: dict[str, Any] | None) -> dict[str, float]:
    out: dict[str, float] = {}
    for raw_ticker, raw_weight in (weights or {}).items():
        ticker = str(raw_ticker or "").upper().strip()
        if not ticker:
            continue
        try:
            out[ticker] = max(float(raw_weight or 0.0), 0.0)
        except (TypeError, ValueError):
            out[ticker] = 0.0
    return out
