"""Structured auto-pause triggers for execution trust failures."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any


DEFAULT_AUTO_PAUSE_CONFIG: dict[str, Any] = {
    "enabled": True,
    "mode": "observe",
    "auto_pause_after_consecutive_qc_rejects": 2,
    "policy_mismatch_alert_after_minutes": 5,
    "heartbeat_stale_pause_after_minutes": 5,
    "pause_on_account_state_guard_failure": True,
    "execution_event_lookback": 10,
}

POLICY_MISMATCH_REASONS = {
    "policy_version_mismatch_with_buy",
    "policy_version_mismatch",
    "missing_policy_version",
}


def default_auto_pause_config(config: dict[str, Any] | None = None) -> dict[str, Any]:
    """Return a normalized, JSON-serializable auto-pause config."""
    merged = dict(DEFAULT_AUTO_PAUSE_CONFIG)
    merged.update(config or {})
    mode = str(merged.get("mode") or "observe").lower().strip()
    merged["mode"] = mode if mode in {"observe", "active", "off"} else "observe"
    merged["auto_pause_after_consecutive_qc_rejects"] = max(
        int(_number_or_default(merged.get("auto_pause_after_consecutive_qc_rejects"), 2)),
        1,
    )
    merged["policy_mismatch_alert_after_minutes"] = max(
        float(_number_or_default(merged.get("policy_mismatch_alert_after_minutes"), 5)),
        0.0,
    )
    merged["heartbeat_stale_pause_after_minutes"] = max(
        float(_number_or_default(merged.get("heartbeat_stale_pause_after_minutes"), 5)),
        0.0,
    )
    merged["execution_event_lookback"] = max(int(_number_or_default(merged.get("execution_event_lookback"), 10)), 1)
    return merged


async def load_auto_pause_verdict(
    *,
    config: dict[str, Any] | None = None,
    account_state_guard: dict[str, Any] | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Load recent execution state and evaluate auto-pause triggers."""
    cfg = default_auto_pause_config(config)
    events = await _load_recent_execution_events(limit=int(cfg["execution_event_lookback"]))
    return evaluate_auto_pause_triggers(
        execution_events=events,
        account_state_guard=account_state_guard,
        config=cfg,
        now=now,
    )


async def apply_auto_pause_if_needed(verdict: dict[str, Any]) -> bool:
    """Set circuit_state=ALERT when an active auto-pause verdict requires it."""
    if not verdict.get("should_pause"):
        return False

    from db.queries import upsert_system_config
    from db.session import AsyncSessionLocal
    from services.circuit_breaker import CircuitBreakerMonitor, CircuitState

    reason = str(verdict.get("reason") or "auto_pause_triggered")
    primary_trigger = str(verdict.get("primary_trigger") or "auto_pause")
    monitor = CircuitBreakerMonitor()
    await monitor.update_circuit_state(
        CircuitState.ALERT,
        reason=reason,
        primary_trigger=primary_trigger,
    )
    async with AsyncSessionLocal() as db:
        await upsert_system_config(
            db,
            "auto_pause_last_event",
            {
                "triggered_at": _utcnow().isoformat(),
                "primary_trigger": primary_trigger,
                "reason": reason,
                "verdict": verdict,
            },
            "auto_pause",
        )
    return True


def evaluate_auto_pause_triggers(
    *,
    execution_events: list[dict[str, Any]] | None = None,
    account_state_guard: dict[str, Any] | None = None,
    config: dict[str, Any] | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Evaluate execution-trust triggers from recent events and account guard."""
    cfg = default_auto_pause_config(config)
    mode = cfg["mode"]
    now = _strip_tz(now or _utcnow())
    if not bool(cfg.get("enabled", True)) or mode == "off":
        return {
            "enabled": False,
            "mode": mode,
            "status": "disabled",
            "would_pause": False,
            "should_pause": False,
            "execution_effect": "none",
            "primary_trigger": None,
            "reason": None,
            "triggers": [],
            "config": _public_config(cfg),
        }

    events = execution_events or []
    triggers = [
        _consecutive_qc_rejects_trigger(events, cfg),
        _policy_mismatch_timeout_trigger(events, cfg, now),
        _account_state_stale_trigger(account_state_guard or {}, cfg),
        _account_state_guard_failure_trigger(account_state_guard or {}, cfg),
    ]
    fired = [item for item in triggers if item["triggered"]]
    would_pause = bool(fired)
    should_pause = would_pause and mode == "active"
    primary = _primary_trigger(fired)
    status = "pause_required" if should_pause else ("would_pause" if would_pause else "pass")
    reason = primary.get("details") if primary else None

    return {
        "enabled": True,
        "mode": mode,
        "status": status,
        "would_pause": would_pause,
        "should_pause": should_pause,
        "execution_effect": "circuit_alert" if should_pause else "diagnostic_only",
        "primary_trigger": primary.get("name") if primary else None,
        "reason": reason,
        "triggers": triggers,
        "config": _public_config(cfg),
    }


def _consecutive_qc_rejects_trigger(events: list[dict[str, Any]], cfg: dict[str, Any]) -> dict[str, Any]:
    threshold = int(cfg["auto_pause_after_consecutive_qc_rejects"])
    count = 0
    evidence: list[dict[str, Any]] = []
    for event in events:
        status = str(event.get("qc_status") or "").lower().strip()
        if status == "rejected":
            count += 1
            evidence.append(_event_evidence(event))
            continue
        if status in {"accepted", "filled", "partial", "submitted", "timeout_no_ack"}:
            break
    triggered = count >= threshold
    return {
        "name": "consecutive_qc_rejects",
        "triggered": triggered,
        "value": count,
        "threshold": threshold,
        "severity": "high",
        "details": f"{count} consecutive QC rejects >= {threshold}" if triggered else f"{count} consecutive QC rejects",
        "evidence": evidence,
    }


def _policy_mismatch_timeout_trigger(
    events: list[dict[str, Any]],
    cfg: dict[str, Any],
    now: datetime,
) -> dict[str, Any]:
    threshold_minutes = float(cfg["policy_mismatch_alert_after_minutes"])
    latest = events[0] if events else None
    mismatch = latest if latest and _is_policy_mismatch_event(latest) else None
    age_minutes = None
    if mismatch:
        event_time = _parse_datetime(mismatch.get("qc_ack_at") or mismatch.get("executed_at"))
        age_minutes = (now - event_time).total_seconds() / 60 if event_time else None
    triggered = bool(mismatch and age_minutes is not None and age_minutes >= threshold_minutes)
    return {
        "name": "policy_mismatch_timeout",
        "triggered": triggered,
        "value": round(age_minutes, 3) if age_minutes is not None else 0.0,
        "threshold": threshold_minutes,
        "severity": "high",
        "details": (
            f"latest QC policy mismatch persisted {age_minutes:.1f}m >= {threshold_minutes:.1f}m"
            if triggered
            else "latest QC event is not a stale policy mismatch"
        ),
        "evidence": [_event_evidence(mismatch)] if mismatch else [],
    }


def _account_state_stale_trigger(account_guard: dict[str, Any], cfg: dict[str, Any]) -> dict[str, Any]:
    threshold_minutes = float(cfg["heartbeat_stale_pause_after_minutes"])
    snapshot = account_guard.get("snapshot") if isinstance(account_guard.get("snapshot"), dict) else {}
    age_seconds = _number_or_none(snapshot.get("age_seconds"))
    stale_blockers = {
        "account_state_snapshot_stale_or_missing_time",
        "missing_account_state_snapshot",
    }
    blockers = set(account_guard.get("blockers") or [])
    stale_by_blocker = bool(stale_blockers & blockers)
    stale_by_age = bool(age_seconds is not None and age_seconds >= threshold_minutes * 60)
    triggered = stale_by_blocker or stale_by_age
    return {
        "name": "account_state_stale",
        "triggered": triggered,
        "value": round((age_seconds or 0.0) / 60, 3),
        "threshold": threshold_minutes,
        "severity": "high",
        "details": (
            f"account state stale or missing: blockers={sorted(stale_blockers & blockers)}"
            if triggered
            else "account state freshness within tolerance"
        ),
        "evidence": [{"snapshot": snapshot, "blockers": sorted(blockers)}],
    }


def _account_state_guard_failure_trigger(account_guard: dict[str, Any], cfg: dict[str, Any]) -> dict[str, Any]:
    enabled = bool(cfg.get("pause_on_account_state_guard_failure", True))
    blockers = list(account_guard.get("blockers") or [])
    triggered = enabled and bool(account_guard.get("would_block")) and bool(blockers)
    return {
        "name": "account_state_guard_failure",
        "triggered": triggered,
        "value": len(blockers),
        "threshold": 1,
        "severity": "high",
        "details": (
            f"account_state_guard would block: {', '.join(blockers)}"
            if triggered
            else "account_state_guard did not report blocking failures"
        ),
        "evidence": [{"status": account_guard.get("status"), "blockers": blockers}],
    }


async def _load_recent_execution_events(*, limit: int) -> list[dict[str, Any]]:
    from sqlalchemy import desc, func, select

    from db.models import ExecutionLog
    from db.session import AsyncSessionLocal

    order_time = func.coalesce(ExecutionLog.qc_ack_at, ExecutionLog.executed_at)
    async with AsyncSessionLocal() as db:
        rows = (
            await db.execute(
                select(ExecutionLog)
                .where(ExecutionLog.command_id.isnot(None))
                .order_by(desc(order_time))
                .limit(limit)
            )
        ).scalars().all()
    return [
        {
            "command_id": row.command_id,
            "qc_status": row.qc_status,
            "qc_ack_at": _iso_or_none(row.qc_ack_at),
            "executed_at": _iso_or_none(row.executed_at),
            "qc_rejection_reason": row.qc_rejection_reason,
            "qc_response": row.qc_response or {},
            "status": row.status,
        }
        for row in rows
    ]


def _primary_trigger(fired: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not fired:
        return None
    priority = {
        "consecutive_qc_rejects": 0,
        "policy_mismatch_timeout": 1,
        "account_state_stale": 2,
        "account_state_guard_failure": 3,
    }
    return sorted(fired, key=lambda item: priority.get(str(item.get("name")), 99))[0]


def _is_policy_mismatch_event(event: dict[str, Any]) -> bool:
    response = event.get("qc_response") if isinstance(event.get("qc_response"), dict) else {}
    reason = str(event.get("qc_rejection_reason") or response.get("reason") or "").strip()
    if response.get("policy_mismatch") is True:
        return True
    return any(token in reason for token in POLICY_MISMATCH_REASONS)


def _event_evidence(event: dict[str, Any] | None) -> dict[str, Any]:
    if not event:
        return {}
    response = event.get("qc_response") if isinstance(event.get("qc_response"), dict) else {}
    return {
        "command_id": event.get("command_id"),
        "qc_status": event.get("qc_status"),
        "qc_ack_at": _iso_or_none(event.get("qc_ack_at")),
        "executed_at": _iso_or_none(event.get("executed_at")),
        "reason": event.get("qc_rejection_reason") or response.get("reason"),
        "policy_mismatch": response.get("policy_mismatch"),
        "policy_version": response.get("policy_version"),
    }


def _public_config(cfg: dict[str, Any]) -> dict[str, Any]:
    return {
        "mode": cfg.get("mode"),
        "auto_pause_after_consecutive_qc_rejects": cfg.get("auto_pause_after_consecutive_qc_rejects"),
        "policy_mismatch_alert_after_minutes": cfg.get("policy_mismatch_alert_after_minutes"),
        "heartbeat_stale_pause_after_minutes": cfg.get("heartbeat_stale_pause_after_minutes"),
        "pause_on_account_state_guard_failure": cfg.get("pause_on_account_state_guard_failure"),
    }


def _number_or_default(value: Any, fallback: float) -> float:
    number = _number_or_none(value)
    return number if number is not None else fallback


def _number_or_none(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number or number in {float("inf"), float("-inf")}:
        return None
    return number


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return _strip_tz(value)
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return _strip_tz(datetime.fromisoformat(text))
    except ValueError:
        return None


def _strip_tz(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(UTC).replace(tzinfo=None)


def _iso_or_none(value: Any) -> str | None:
    parsed = _parse_datetime(value)
    return parsed.isoformat() if parsed else None


def _utcnow() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)
