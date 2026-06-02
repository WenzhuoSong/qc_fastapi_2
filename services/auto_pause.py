"""Structured auto-pause triggers for execution trust failures."""
from __future__ import annotations

from datetime import UTC, datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from services.execution_lifecycle import ACTIVE_EXECUTION_CONTROL_REASONS
from services.market_calendar import is_us_equity_trading_day, us_equity_holiday_name


DEFAULT_AUTO_PAUSE_CONFIG: dict[str, Any] = {
    "enabled": True,
    "mode": "observe",
    "auto_pause_after_consecutive_qc_rejects": 2,
    "max_qc_reject_event_age_hours": 6,
    "policy_mismatch_alert_after_minutes": 5,
    "heartbeat_stale_pause_after_minutes": 5,
    "suppress_account_stale_outside_strict_market": True,
    "pause_on_account_state_guard_failure": True,
    "execution_event_lookback": 10,
}

MARKET_TZ = ZoneInfo("America/New_York")
MARKET_OPEN = time(9, 30)
HEARTBEAT_STRICT_AFTER = time(10, 0)
MARKET_CLOSE = time(16, 0)

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
    merged["max_qc_reject_event_age_hours"] = max(
        float(_number_or_default(merged.get("max_qc_reject_event_age_hours"), 6)),
        0.0,
    )
    merged["policy_mismatch_alert_after_minutes"] = max(
        float(_number_or_default(merged.get("policy_mismatch_alert_after_minutes"), 5)),
        0.0,
    )
    merged["heartbeat_stale_pause_after_minutes"] = max(
        float(_number_or_default(merged.get("heartbeat_stale_pause_after_minutes"), 5)),
        0.0,
    )
    merged["suppress_account_stale_outside_strict_market"] = bool(
        merged.get("suppress_account_stale_outside_strict_market", True)
    )
    merged["execution_event_lookback"] = max(int(_number_or_default(merged.get("execution_event_lookback"), 10)), 1)
    return merged


async def load_auto_pause_verdict(
    *,
    config: dict[str, Any] | None = None,
    account_state_guard: dict[str, Any] | None = None,
    policy_sync_recovery: dict[str, Any] | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Load recent execution state and evaluate auto-pause triggers."""
    cfg = default_auto_pause_config(config)
    events = await _load_recent_execution_events(limit=int(cfg["execution_event_lookback"]))
    return evaluate_auto_pause_triggers(
        execution_events=events,
        account_state_guard=account_state_guard,
        policy_sync_recovery=policy_sync_recovery,
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

    reason = _reason_with_evidence(verdict)
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
    policy_sync_recovery: dict[str, Any] | None = None,
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
        _consecutive_qc_rejects_trigger(events, cfg, now),
        _policy_mismatch_timeout_trigger(events, cfg, now, policy_sync_recovery),
        _policy_sync_recovery_exhausted_trigger(policy_sync_recovery or {}),
        _account_state_stale_trigger(account_state_guard or {}, cfg, now),
        _account_state_guard_failure_trigger(account_state_guard or {}, cfg, policy_sync_recovery, now),
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


def _consecutive_qc_rejects_trigger(events: list[dict[str, Any]], cfg: dict[str, Any], now: datetime) -> dict[str, Any]:
    threshold = int(cfg["auto_pause_after_consecutive_qc_rejects"])
    max_age_hours = float(cfg["max_qc_reject_event_age_hours"])
    cutoff = now - timedelta(hours=max_age_hours) if max_age_hours > 0 else None
    count = 0
    evidence: list[dict[str, Any]] = []
    for event in events:
        event_time = _parse_datetime(event.get("qc_ack_at") or event.get("executed_at"))
        if cutoff and event_time and event_time < cutoff:
            break
        command_type = str(event.get("command_type") or "").lower().strip()
        if command_type != "weight_adjustment":
            continue
        status = str(event.get("qc_status") or "").lower().strip()
        response = event.get("qc_response") if isinstance(event.get("qc_response"), dict) else {}
        reason = str(event.get("qc_rejection_reason") or response.get("reason") or "").lower().strip()
        if status in {"not_sent", "preflight_blocked"} or reason in {
            "fastapi_no_qc_command",
            "blocked_by_command_preflight",
        }:
            break
        if reason in ACTIVE_EXECUTION_CONTROL_REASONS:
            continue
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
    policy_sync_recovery: dict[str, Any] | None,
) -> dict[str, Any]:
    threshold_minutes = float(cfg["policy_mismatch_alert_after_minutes"])
    if _recoverable_policy_sync_recovery(policy_sync_recovery):
        return {
            "name": "policy_mismatch_timeout",
            "triggered": False,
            "value": 0.0,
            "threshold": threshold_minutes,
            "severity": "high",
            "details": "suppressed by policy_sync_recovery",
            "evidence": [{"policy_sync_recovery": policy_sync_recovery or {}}],
        }
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


def _policy_sync_recovery_exhausted_trigger(policy_sync_recovery: dict[str, Any]) -> dict[str, Any]:
    triggered = str(policy_sync_recovery.get("status") or "").lower().strip() == "unrecoverable"
    reason = policy_sync_recovery.get("reason") or "policy_sync_recovery_unrecoverable"
    return {
        "name": "policy_sync_recovery_exhausted",
        "triggered": triggered,
        "value": 1 if triggered else 0,
        "threshold": 1,
        "severity": "high",
        "details": f"policy sync recovery exhausted: {reason}" if triggered else "policy sync recovery not exhausted",
        "evidence": [{"policy_sync_recovery": policy_sync_recovery}],
    }


def _account_state_stale_trigger(account_guard: dict[str, Any], cfg: dict[str, Any], now: datetime) -> dict[str, Any]:
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
    raw_triggered = stale_by_blocker or stale_by_age
    suppression = _account_stale_market_suppression(now, cfg)
    triggered = raw_triggered and not suppression["suppressed"]
    return {
        "name": "account_state_stale",
        "triggered": triggered,
        "value": round((age_seconds or 0.0) / 60, 3),
        "threshold": threshold_minutes,
        "severity": "warning" if raw_triggered and suppression["suppressed"] else "high",
        "details": (
            f"account state stale or missing: blockers={sorted(stale_blockers & blockers)}"
            if triggered
            else f"account state stale suppressed: {suppression['reason']}"
            if raw_triggered and suppression["suppressed"]
            else "account state freshness within tolerance"
        ),
        "evidence": [{"snapshot": snapshot, "blockers": sorted(blockers), "market_suppression": suppression}],
    }


def _account_state_guard_failure_trigger(
    account_guard: dict[str, Any],
    cfg: dict[str, Any],
    policy_sync_recovery: dict[str, Any] | None,
    now: datetime,
) -> dict[str, Any]:
    enabled = bool(cfg.get("pause_on_account_state_guard_failure", True))
    blockers = list(account_guard.get("blockers") or [])
    suppressed = _recoverable_policy_sync_recovery(policy_sync_recovery)
    stale_only_blockers = {
        "account_state_snapshot_stale_or_missing_time",
        "missing_account_state_snapshot",
    }
    stale_market_suppression = _account_stale_market_suppression(now, cfg)
    stale_only_suppressed = (
        bool(blockers)
        and set(blockers).issubset(stale_only_blockers)
        and stale_market_suppression["suppressed"]
    )
    triggered = (
        enabled
        and not suppressed
        and not stale_only_suppressed
        and bool(account_guard.get("would_block"))
        and bool(blockers)
    )
    return {
        "name": "account_state_guard_failure",
        "triggered": triggered,
        "value": len(blockers),
        "threshold": 1,
        "severity": "high",
        "details": (
            f"account_state_guard would block: {', '.join(blockers)}"
            if triggered
            else "account_state_guard failure suppressed by policy_sync_recovery"
            if suppressed
            else f"account_state_guard stale failure suppressed: {stale_market_suppression['reason']}"
            if stale_only_suppressed
            else "account_state_guard did not report blocking failures"
        ),
        "evidence": [{
            "status": account_guard.get("status"),
            "blockers": blockers,
            "policy_sync_recovery": policy_sync_recovery or {},
            "market_suppression": stale_market_suppression,
        }],
    }


def _account_stale_market_suppression(now: datetime, cfg: dict[str, Any]) -> dict[str, Any]:
    if not bool(cfg.get("suppress_account_stale_outside_strict_market", True)):
        return {"suppressed": False, "reason": "disabled_by_config"}
    market_now = _strip_tz(now).replace(tzinfo=UTC).astimezone(MARKET_TZ)
    market_date = market_now.date()
    market_time = market_now.time()
    if not is_us_equity_trading_day(market_date):
        holiday = us_equity_holiday_name(market_date)
        return {
            "suppressed": True,
            "reason": f"market_closed:{holiday}" if holiday else "market_closed",
            "market_time": market_now.isoformat(),
        }
    if market_time < MARKET_OPEN or market_time > MARKET_CLOSE:
        return {"suppressed": True, "reason": "market_closed", "market_time": market_now.isoformat()}
    if market_time < HEARTBEAT_STRICT_AFTER:
        return {"suppressed": True, "reason": "opening_grace", "market_time": market_now.isoformat()}
    return {"suppressed": False, "reason": "strict_market_hours", "market_time": market_now.isoformat()}


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
            "command_type": row.command_type,
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
        "policy_sync_recovery_exhausted": 2,
        "account_state_stale": 3,
        "account_state_guard_failure": 4,
    }
    return sorted(fired, key=lambda item: priority.get(str(item.get("name")), 99))[0]


def _reason_with_evidence(verdict: dict[str, Any]) -> str:
    base = str(verdict.get("reason") or "auto_pause_triggered")
    primary = str(verdict.get("primary_trigger") or "auto_pause")
    trigger_class = {
        "consecutive_qc_rejects": "execution_risk",
        "policy_mismatch_timeout": "control_plane",
        "policy_sync_recovery_exhausted": "control_plane",
        "account_state_stale": "account_risk",
        "account_state_guard_failure": "account_risk",
    }.get(primary, "technical")
    evidence_lines: list[str] = []
    for trigger in verdict.get("triggers") or []:
        if trigger.get("name") != primary:
            continue
        for item in trigger.get("evidence") or []:
            command_id = item.get("command_id")
            if not command_id:
                continue
            event_time = _parse_datetime(item.get("qc_ack_at") or item.get("executed_at"))
            age_hours = None
            if event_time:
                age_hours = max((_utcnow() - event_time).total_seconds() / 3600, 0.0)
            age_text = f" age={age_hours:.1f}h" if age_hours is not None else ""
            evidence_lines.append(
                f"- {str(command_id)[:32]} type={item.get('command_type') or 'unknown'} "
                f"status={item.get('qc_status') or 'unknown'}{age_text} "
                f"reason={item.get('reason') or 'n/a'}"
            )
            if len(evidence_lines) >= 3:
                break
        break
    if not evidence_lines:
        return f"{base}\nTrigger class: {trigger_class}"
    return (
        f"{base}\n"
        f"Trigger class: {trigger_class}\n"
        "Evidence:\n"
        + "\n".join(evidence_lines)
    )


def _recoverable_policy_sync_recovery(recovery: dict[str, Any] | None) -> bool:
    try:
        from services.policy_sync_recovery import policy_sync_recovery_suppresses_auto_pause

        return policy_sync_recovery_suppresses_auto_pause(recovery)
    except Exception:
        return False


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
        "command_type": event.get("command_type"),
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
        "max_qc_reject_event_age_hours": cfg.get("max_qc_reject_event_age_hours"),
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
