"""Operational alert evaluation and Telegram delivery.

This module turns the read-only operational health snapshot into actionable
notifications. It deliberately does not expose dashboard routes or mutate
trading state.
"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any


ALERT_STATE_KEY = "operational_alert_state_v1"
DEFAULT_COOLDOWN_HOURS = 6

ALERT_CHECKS = {
    "qc_heartbeat": "critical",
    "daily_feature_snapshot": "warning",
    "yfinance_backfill": "warning",
}

PIPELINE_BAD_STATUSES = {
    "failed",
    "error",
    "timeout",
}

EXECUTION_BAD_STATUSES = {
    "failed",
    "timeout",
    "skipped",
    "error",
}


def evaluate_operational_alerts(
    snapshot: dict[str, Any],
    state: dict[str, Any] | None = None,
    *,
    now: datetime | None = None,
    cooldown_hours: int = DEFAULT_COOLDOWN_HOURS,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Return alerts that should be sent now and updated alert state.

    Repeated active alerts are suppressed until the cooldown expires. A changed
    fingerprint, for example a different cron failure message, is sent
    immediately.
    """
    now = (now or datetime.now(UTC)).replace(tzinfo=None)
    previous = (state or {}).get("alerts") or {}
    current = _active_alerts(snapshot, now)
    alerts_to_send: list[dict[str, Any]] = []
    next_alert_state: dict[str, Any] = {}

    for alert in current:
        key = alert["key"]
        existing = previous.get(key) or {}
        last_sent_at = _parse_dt(existing.get("last_sent_at"))
        same_fingerprint = existing.get("fingerprint") == alert["fingerprint"]
        in_cooldown = (
            last_sent_at is not None
            and now - last_sent_at < timedelta(hours=cooldown_hours)
        )
        should_send = not same_fingerprint or not in_cooldown
        if should_send:
            alerts_to_send.append(alert)

        next_alert_state[key] = {
            "fingerprint": alert["fingerprint"],
            "last_seen_at": now.isoformat(),
            "last_sent_at": now.isoformat() if should_send else existing.get("last_sent_at"),
            "level": alert["level"],
            "title": alert["title"],
        }

    return alerts_to_send, {
        "updated_at": now.isoformat(),
        "cooldown_hours": cooldown_hours,
        "alerts": next_alert_state,
    }


def format_operational_alert_message(alerts: list[dict[str, Any]]) -> str:
    """Format operational alerts for Telegram."""
    if not alerts:
        return ""
    highest = "critical" if any(a.get("level") == "critical" for a in alerts) else "warning"
    lines = [f"Ops alert: {highest} ({len(alerts)} active)"]
    for alert in alerts[:8]:
        detail = alert.get("detail") or alert.get("title") or alert.get("key")
        lines.append(f"- [{alert.get('level', 'warning')}] {detail}")
    return "\n".join(lines)


async def send_operational_alerts(
    snapshot: dict[str, Any],
    *,
    cooldown_hours: int = DEFAULT_COOLDOWN_HOURS,
) -> dict[str, Any]:
    """Send deduped Telegram alerts for a health snapshot."""
    from db.queries import get_system_config, upsert_system_config
    from db.session import AsyncSessionLocal
    from tools.notify_tools import tool_send_telegram

    async with AsyncSessionLocal() as db:
        row = await get_system_config(db, ALERT_STATE_KEY)
        prior_state = (row.value if row else {}) or {}

    alerts, next_state = evaluate_operational_alerts(
        snapshot,
        prior_state,
        cooldown_hours=cooldown_hours,
    )

    sent = False
    send_error = None
    if alerts:
        result = await tool_send_telegram({
            "text": format_operational_alert_message(alerts),
            "parse_mode": "",
        })
        sent = bool(result.get("sent"))
        send_error = result.get("error")

    async with AsyncSessionLocal() as db:
        await upsert_system_config(db, ALERT_STATE_KEY, next_state, "operational_alerts")

    return {
        "active_alerts": len(next_state.get("alerts") or {}),
        "sent_alerts": len(alerts) if sent else 0,
        "send_attempted": bool(alerts),
        "sent": sent,
        "error": send_error,
    }


def _active_alerts(snapshot: dict[str, Any], now: datetime) -> list[dict[str, Any]]:
    checks = snapshot.get("checks") or {}
    alerts: list[dict[str, Any]] = []

    for key, level in ALERT_CHECKS.items():
        check = checks.get(key) or {}
        state = check.get("state")
        if state is None or state == "ok":
            continue
        reason = str(check.get("reason") or check.get("state") or "not ok")
        title = str(check.get("label") or key)
        alerts.append({
            "key": key,
            "level": "critical" if check.get("blocking") else level,
            "title": title,
            "detail": f"{title}: {reason}",
            "fingerprint": f"{key}:{check.get('state')}:{reason}",
            "as_of": check.get("as_of"),
            "created_at": now.isoformat(),
        })

    pipeline = checks.get("pipeline_status") or {}
    pipeline_status = str(pipeline.get("status") or "unknown").lower()
    if pipeline_status in PIPELINE_BAD_STATUSES:
        alerts.append({
            "key": "pipeline_status",
            "level": "critical",
            "title": "Pipeline",
            "detail": f"Pipeline: {pipeline_status}",
            "fingerprint": f"pipeline_status:{pipeline_status}:{pipeline.get('as_of')}",
            "as_of": pipeline.get("as_of"),
            "created_at": now.isoformat(),
        })

    for row in (snapshot.get("failed_crons") or [])[:3]:
        job_name = str(row.get("job_name") or "unknown")
        error = str(row.get("error_message") or row.get("status") or "failed")[:120]
        started_at = row.get("started_at")
        alerts.append({
            "key": f"cron_failed:{job_name}",
            "level": "warning",
            "title": f"Cron {job_name}",
            "detail": f"Cron {job_name}: {error}",
            "fingerprint": f"cron_failed:{job_name}:{started_at}:{error}",
            "as_of": started_at,
            "created_at": now.isoformat(),
        })

    execution = snapshot.get("execution") or {}
    if execution.get("available"):
        execution_status = str(execution.get("status") or "unknown").lower()
        if execution_status in EXECUTION_BAD_STATUSES:
            executed_at = execution.get("executed_at")
            alerts.append({
                "key": "execution_status",
                "level": "critical" if execution_status in {"failed", "timeout", "error"} else "warning",
                "title": "Execution",
                "detail": f"Execution: {execution_status}",
                "fingerprint": f"execution_status:{execution_status}:{execution.get('analysis_id')}:{executed_at}",
                "as_of": executed_at,
                "created_at": now.isoformat(),
            })

    return alerts


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError:
        return None
