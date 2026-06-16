"""Append-only command lifecycle events."""
from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from services.reconciliation_guard import calculate_reconciliation_drift
from services.json_safety import json_safe


VALID_EVENT_TYPES = {
    "created",
    "preflight_passed",
    "preflight_blocked",
    "submitted_to_qc",
    "qc_accepted",
    "qc_rejected",
    "qc_timeout",
    "execution_result",
    "orders_submitted",
    "filled",
    "partial",
    "canceled",
    "superseded",
    "reconciled",
    "reconciliation_drift",
    "failed_no_fill",
    "timeout_reconciled_no_execution",
    "deferred_by_active_execution",
    "force_reconciled_by_operator",
    "cancel_orders_requested_by_operator",
    "unknown_command_feedback",
}

DEFAULT_RECONCILIATION_TOLERANCE = 0.0025
DEFAULT_RECONCILIATION_MAX_AGE_MINUTES = 30
RECONCILIATION_TERMINAL_EVENTS = {
    "reconciled",
    "reconciliation_drift",
    "failed_no_fill",
    "superseded",
    "timeout_reconciled_no_execution",
}
RECONCILIATION_ACTIVE_QC_STATUSES = {
    "accepted",
    "orders_submitted",
    "partial",
    "timeout_no_ack",
}
COMMAND_LIFECYCLE_STATES = {
    "created",
    "pending_ack",
    "accepted",
    "rejected",
    "orders_submitted",
    "partial",
    "filled",
    "noop_reconciled",
    "pending_reconcile",
    "diverged",
}
TERMINAL_LIFECYCLE_STATES = {
    "filled",
    "rejected",
    "noop_reconciled",
    "diverged",
}
LIFECYCLE_STATE_RANK = {
    "created": 0,
    "pending_ack": 10,
    "accepted": 20,
    "orders_submitted": 30,
    "partial": 40,
    "pending_reconcile": 45,
    "filled": 60,
    "noop_reconciled": 70,
    "diverged": 80,
    "rejected": 90,
}
QC_STATUS_TO_LIFECYCLE_STATE = {
    "submitted": "pending_ack",
    "accepted": "accepted",
    "orders_submitted": "orders_submitted",
    "partial": "partial",
    "filled": "filled",
    "reconciled": "filled",
    "reconciliation_drift": "diverged",
    "rejected": "rejected",
    "canceled": "rejected",
    "failed_no_fill": "rejected",
    "not_sent": "created",
    "timeout_no_ack": "pending_reconcile",
    "timeout_no_execution_confirmed": "rejected",
}


def next_lifecycle_state(
    current_state: str | None,
    proposed_state: str | None,
) -> str:
    """Return a monotonic lifecycle state for the shared command row.

    Late/duplicate ACKs are common. A stale accepted ACK must not move a command
    backward after account truth has already marked it filled, noop, or
    diverged. The one allowed terminal escalation is filled -> diverged when a
    later trusted reconciliation check proves drift.
    """
    current = str(current_state or "").lower().strip()
    proposed = str(proposed_state or "").lower().strip()
    if proposed not in COMMAND_LIFECYCLE_STATES:
        return current if current in COMMAND_LIFECYCLE_STATES else "created"
    if current not in COMMAND_LIFECYCLE_STATES:
        return proposed
    if current == proposed:
        return current
    if current == "filled" and proposed == "diverged":
        return "diverged"
    if current in TERMINAL_LIFECYCLE_STATES:
        return current
    if LIFECYCLE_STATE_RANK.get(proposed, -1) >= LIFECYCLE_STATE_RANK.get(current, -1):
        return proposed
    return current


def build_command_lifecycle_event(
    *,
    command_id: str,
    event_type: str,
    analysis_id: int | None = None,
    event_status: str | None = None,
    source: str = "fastapi",
    reason: str | None = None,
    payload: dict[str, Any] | None = None,
    event_time: datetime | None = None,
) -> dict[str, Any]:
    """Return a normalized lifecycle event payload."""
    command = str(command_id or "").strip()
    if not command:
        raise ValueError("command_id is required for lifecycle events")
    event = str(event_type or "").strip()
    if event not in VALID_EVENT_TYPES:
        raise ValueError(f"unknown command lifecycle event_type: {event}")
    return {
        "command_id": command,
        "analysis_id": analysis_id,
        "event_type": event,
        "event_status": str(event_status or event).strip(),
        "event_time": _strip_tz(event_time or datetime.now(UTC)),
        "source": str(source or "fastapi").strip(),
        "reason": reason,
        "payload": json_safe(payload or {}),
    }


async def append_command_lifecycle_event(
    db,
    *,
    command_id: str,
    event_type: str,
    analysis_id: int | None = None,
    event_status: str | None = None,
    source: str = "fastapi",
    reason: str | None = None,
    payload: dict[str, Any] | None = None,
    event_time: datetime | None = None,
) -> None:
    """Append an event to the current DB session."""
    from db.models import CommandLifecycleEvent

    event = build_command_lifecycle_event(
        command_id=command_id,
        analysis_id=analysis_id,
        event_type=event_type,
        event_status=event_status,
        source=source,
        reason=reason,
        payload=payload,
        event_time=event_time,
    )
    db.add(CommandLifecycleEvent(**event))


def lifecycle_state_from_status(
    *,
    status: str | None = None,
    qc_status: str | None = None,
    qc_response: dict[str, Any] | None = None,
) -> str:
    """Return the command-row lifecycle state for current status fields."""
    response = qc_response if isinstance(qc_response, dict) else {}
    execution_state = str(
        response.get("execution_state")
        or (response.get("order_summary") or {}).get("execution_state")
        or ""
    ).lower().strip()
    if execution_state == "noop_reconciled":
        return "noop_reconciled"

    clean_qc = str(qc_status or "").lower().strip()
    if clean_qc in QC_STATUS_TO_LIFECYCLE_STATE:
        return QC_STATUS_TO_LIFECYCLE_STATE[clean_qc]

    clean_status = str(status or "").lower().strip()
    if clean_status in {"sent", "submitted", "pending_send"}:
        return "pending_ack"
    if clean_status in {"accepted"}:
        return "accepted"
    if clean_status in {"rejected", "failed", "skipped"}:
        return "rejected"
    if clean_status in COMMAND_LIFECYCLE_STATES:
        return clean_status
    return "created"


def build_command_reconciliation_events(
    *,
    command_id: str,
    command_payload: dict[str, Any] | None = None,
    qc_response: dict[str, Any] | None = None,
    account_state: dict[str, Any] | None = None,
    analysis_id: int | None = None,
    event_time: datetime | None = None,
    tolerance: float = DEFAULT_RECONCILIATION_TOLERANCE,
) -> list[dict[str, Any]]:
    """Derive fill/reconciliation lifecycle events from QC facts.

    This is intentionally deterministic and conservative. It does not infer a
    full fill from target acceptance alone; it needs either QC order summary
    evidence or account holdings that match the accepted target within
    tolerance.
    """
    response = qc_response or {}
    status = str(response.get("status") or "").lower().strip()
    if status and status != "accepted":
        return []

    account = account_state or _account_state_from_response(response)
    order_summary = _order_summary_from_response(response)
    event_time = event_time or _event_time_from_response(response)
    events: list[dict[str, Any]] = []

    if _orders_submitted_evidence(order_summary, response):
        events.append(build_command_lifecycle_event(
            command_id=command_id,
            analysis_id=analysis_id,
            event_type="orders_submitted",
            event_status="orders_submitted",
            source="qc",
            reason="qc_ack_reports_orders_submitted",
            payload={
                "execution_state": response.get("execution_state"),
                "order_summary": order_summary,
                "account_state": _account_reconciliation_payload(account),
            },
            event_time=event_time,
        ))

    partial_reason = _partial_reason(order_summary, account)
    if partial_reason:
        events.append(build_command_lifecycle_event(
            command_id=command_id,
            analysis_id=analysis_id,
            event_type="partial",
            event_status="partial_or_open_orders",
            source="qc",
            reason=partial_reason,
            payload={
                "order_summary": order_summary,
                "account_state": _account_reconciliation_payload(account),
            },
            event_time=event_time,
        ))
    elif _has_order_fill_evidence(order_summary):
        events.append(build_command_lifecycle_event(
            command_id=command_id,
            analysis_id=analysis_id,
            event_type="filled",
            event_status="filled",
            source="qc",
            reason="qc_order_summary_reports_no_open_orders",
            payload={
                "order_summary": order_summary,
                "account_state": _account_reconciliation_payload(account),
            },
            event_time=event_time,
        ))
    elif _failed_no_fill_evidence(order_summary, response, account):
        events.append(build_command_lifecycle_event(
            command_id=command_id,
            analysis_id=analysis_id,
            event_type="failed_no_fill",
            event_status="failed_no_fill",
            source="qc",
            reason="qc_reports_command_completed_without_fills",
            payload={
                "order_summary": order_summary,
                "account_state": _account_reconciliation_payload(account),
            },
            event_time=event_time,
        ))

    target = _target_weights_for_reconciliation(command_payload or {}, response, account)
    holdings = _holdings_weights_for_reconciliation(response, account)
    if not target or not holdings:
        return events

    drift = _reconciliation_drift(target, holdings, account=account, tolerance=tolerance)
    payload = {
        "tolerance": tolerance,
        "threshold": drift["threshold"],
        "max_abs_diff": drift["raw_max_abs_diff"],
        "max_drift": drift["max_drift"],
        "diffs": drift["diffs"],
        "drift_tickers": drift["drift_tickers"],
        "target_weights": target,
        "actual_holdings_weights": holdings,
        "order_summary": order_summary,
        "account_state": _account_reconciliation_payload(account),
    }
    if not drift["drift_tickers"] and not _has_open_orders(order_summary, account):
        events.append(build_command_lifecycle_event(
            command_id=command_id,
            analysis_id=analysis_id,
            event_type="reconciled",
            event_status="reconciled",
            source="fastapi",
            reason="actual_holdings_match_target",
            payload=payload,
            event_time=event_time,
        ))
    elif drift["drift_tickers"] and not _has_open_orders(order_summary, account):
        events.append(build_command_lifecycle_event(
            command_id=command_id,
            analysis_id=analysis_id,
            event_type="reconciliation_drift",
            event_status="drift",
            source="fastapi",
            reason="actual_holdings_deviate_from_target",
            payload=payload,
            event_time=event_time,
        ))
    return events


async def load_command_lifecycle(command_id: str) -> list[dict[str, Any]]:
    """Load ordered lifecycle events for one command."""
    from sqlalchemy import select

    from db.models import CommandLifecycleEvent
    from db.session import AsyncSessionLocal

    async with AsyncSessionLocal() as db:
        rows = (
            await db.execute(
                select(CommandLifecycleEvent)
                .where(CommandLifecycleEvent.command_id == command_id)
                .order_by(CommandLifecycleEvent.event_time, CommandLifecycleEvent.id)
            )
        ).scalars().all()
    return [_event_to_dict(row) for row in rows]


def build_reconciliation_lag_report(
    *,
    commands: list[dict[str, Any]],
    events: list[dict[str, Any]],
    now: datetime | None = None,
    max_age_minutes: int = DEFAULT_RECONCILIATION_MAX_AGE_MINUTES,
) -> dict[str, Any]:
    """Report accepted commands that are not yet reconciled after a threshold."""
    checked_at = _strip_tz(now or datetime.now(UTC))
    threshold = max(int(max_age_minutes or DEFAULT_RECONCILIATION_MAX_AGE_MINUTES), 1)
    events_by_command: dict[str, list[dict[str, Any]]] = {}
    for event in events or []:
        command_id = str(event.get("command_id") or "").strip()
        if command_id:
            events_by_command.setdefault(command_id, []).append(event)

    rows: list[dict[str, Any]] = []
    for command in commands or []:
        command_id = str(command.get("command_id") or "").strip()
        if not command_id:
            continue
        if str(command.get("command_type") or "").strip() not in {"weight_adjustment", ""}:
            continue
        if str(command.get("qc_status") or "").lower().strip() not in RECONCILIATION_ACTIVE_QC_STATUSES:
            continue
        accepted_at = _datetime_from_any(command.get("qc_ack_at") or command.get("executed_at"))
        if accepted_at is None:
            continue
        lifecycle = events_by_command.get(command_id, [])
        terminal = _latest_event_of_type(lifecycle, RECONCILIATION_TERMINAL_EVENTS)
        if terminal:
            continue
        latest = _latest_event(lifecycle)
        age_minutes = max((checked_at - accepted_at).total_seconds() / 60.0, 0.0)
        status = "overdue" if age_minutes >= threshold else "pending"
        rows.append({
            "command_id": command_id,
            "analysis_id": command.get("analysis_id"),
            "qc_status": command.get("qc_status"),
            "accepted_at": accepted_at.isoformat(),
            "age_minutes": round(age_minutes, 1),
            "max_age_minutes": threshold,
            "status": status,
            "latest_event_type": latest.get("event_type") if latest else None,
            "latest_event_status": latest.get("event_status") if latest else None,
            "reason": "accepted_without_reconciled_event",
        })
    rows.sort(key=lambda row: (row["status"] != "overdue", -float(row["age_minutes"])))
    return {
        "contract_version": "command_reconciliation_lag_v1",
        "checked_at": checked_at.isoformat(),
        "max_age_minutes": threshold,
        "accepted_without_reconciled_count": len(rows),
        "overdue_count": sum(1 for row in rows if row["status"] == "overdue"),
        "pending_count": sum(1 for row in rows if row["status"] == "pending"),
        "rows": rows,
        "execution_effect": "diagnostic_only",
    }


def _event_to_dict(row) -> dict[str, Any]:
    return {
        "id": row.id,
        "command_id": row.command_id,
        "analysis_id": row.analysis_id,
        "event_type": row.event_type,
        "event_status": row.event_status,
        "event_time": row.event_time.isoformat() if row.event_time else None,
        "source": row.source,
        "reason": row.reason,
        "payload": row.payload or {},
    }


def _latest_event(events: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not events:
        return None
    return max(
        events,
        key=lambda event: _datetime_from_any(event.get("event_time")) or datetime.min,
    )


def _latest_event_of_type(events: list[dict[str, Any]], event_types: set[str]) -> dict[str, Any] | None:
    matching = [
        event for event in events or []
        if str(event.get("event_type") or "").strip() in event_types
    ]
    return _latest_event(matching)


def _order_summary_from_response(response: dict[str, Any]) -> dict[str, Any]:
    for key in ("order_summary", "fill_summary"):
        value = response.get(key)
        if isinstance(value, dict):
            return value
    return {}


def _account_state_from_response(response: dict[str, Any]) -> dict[str, Any]:
    value = response.get("account_state")
    return value if isinstance(value, dict) else {}


def _event_time_from_response(response: dict[str, Any]) -> datetime | None:
    value = response.get("qc_timestamp") or response.get("timestamp_utc")
    return _datetime_from_any(value)


def _datetime_from_any(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return _strip_tz(value)
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return _strip_tz(datetime.fromisoformat(text))
    except ValueError:
        return None


def _partial_reason(order_summary: dict[str, Any], account: dict[str, Any]) -> str | None:
    if _int_or_none(order_summary.get("partial_fill_count")):
        return "qc_order_summary_reports_partial_fills"
    if _has_open_orders(order_summary, account):
        return "qc_reports_open_orders_after_command"
    return None


def _has_order_fill_evidence(order_summary: dict[str, Any]) -> bool:
    if not order_summary:
        return False
    actual = _int_or_none(order_summary.get("actual_order_count"))
    if actual == 0:
        return False
    filled = _int_or_none(order_summary.get("filled_order_count"))
    submitted = _int_or_none(order_summary.get("submitted_order_count"))
    action_count = _int_or_none(order_summary.get("action_count"))
    all_filled = _bool_or_none(order_summary.get("all_filled"))
    if all_filled is True:
        return True
    if filled and filled > 0:
        return True
    return bool((submitted and submitted > 0) or (action_count and action_count > 0)) and not _has_open_orders(order_summary, {})


def _orders_submitted_evidence(order_summary: dict[str, Any], response: dict[str, Any]) -> bool:
    execution_state = str(response.get("execution_state") or "").lower().strip()
    if execution_state == "noop_reconciled":
        return False
    if execution_state == "orders_submitted":
        return True
    actual = _int_or_none(order_summary.get("actual_order_count"))
    if actual == 0:
        return False
    submitted = _int_or_none(order_summary.get("submitted_order_count"))
    action_count = _int_or_none(order_summary.get("action_count"))
    return bool((submitted and submitted > 0) or (action_count and action_count > 0))


def _failed_no_fill_evidence(
    order_summary: dict[str, Any],
    response: dict[str, Any],
    account: dict[str, Any],
) -> bool:
    """Return true only when QC explicitly says the command completed with no fill."""
    if _has_open_orders(order_summary, account):
        return False
    for source in (order_summary, response, account):
        if not isinstance(source, dict):
            continue
        if _bool_or_none(source.get("failed_no_fill")) is True:
            return True
        status = str(
            source.get("active_execution_status")
            or source.get("execution_state")
            or source.get("status")
            or ""
        ).lower().strip()
        if status in {"failed_no_fill", "no_fill"}:
            return True
    return False


def _has_open_orders(order_summary: dict[str, Any], account: dict[str, Any]) -> bool:
    for source in (order_summary, account):
        if not isinstance(source, dict):
            continue
        value = _int_or_none(
            source.get("open_order_count_after", source.get("open_order_count"))
        )
        if value is not None:
            return value > 0
        has_open = _bool_or_none(source.get("has_open_orders"))
        if has_open is not None:
            return has_open
    return False


def _target_weights_for_reconciliation(
    command_payload: dict[str, Any],
    response: dict[str, Any],
    account: dict[str, Any],
) -> dict[str, float]:
    for value in (
        response.get("actual_target_weights"),
        account.get("target_weights"),
        command_payload.get("sent_weights"),
        command_payload.get("proposed_weights"),
    ):
        weights = _clean_weights(value)
        if weights:
            return weights
    return {}


def _holdings_weights_for_reconciliation(
    response: dict[str, Any],
    account: dict[str, Any],
) -> dict[str, float]:
    for value in (
        account.get("holdings_weights"),
        response.get("actual_holdings_weights"),
    ):
        weights = _clean_weights(value)
        if weights:
            return weights
    return {}


def _account_reconciliation_payload(account: dict[str, Any]) -> dict[str, Any]:
    return {
        "timestamp_utc": account.get("timestamp_utc"),
        "algorithm_time": account.get("algorithm_time"),
        "policy_version": account.get("policy_version"),
        "total_value": account.get("total_value"),
        "open_order_count": account.get("open_order_count"),
        "has_open_orders": account.get("has_open_orders"),
        "last_command_id": account.get("last_command_id"),
        "active_command_id": account.get("active_command_id"),
        "active_execution_status": account.get("active_execution_status"),
        "processed_command_count": account.get("processed_command_count"),
    }


def _reconciliation_drift(
    target: dict[str, float],
    actual: dict[str, float],
    *,
    account: dict[str, Any],
    tolerance: float,
) -> dict[str, Any]:
    drift = calculate_reconciliation_drift(
        target,
        actual,
        total_value=_float_or_none(account.get("total_value")),
        prices=_prices_from_account_state(account),
        config={"relative_weight_tolerance": tolerance},
    )
    return {
        **drift,
        "diffs": _weight_diff(target, actual)["diffs"],
    }


def _prices_from_account_state(account: dict[str, Any]) -> dict[str, float]:
    if not isinstance(account, dict):
        return {}
    explicit = account.get("prices")
    prices = _clean_prices(explicit)
    if prices:
        return prices
    for key in ("holdings_detail_rows", "holdings"):
        rows = account.get(key)
        if isinstance(rows, list):
            prices.update(_prices_from_holdings_rows(rows))
    raw = account.get("raw_snapshot")
    if isinstance(raw, dict):
        rows = raw.get("holdings_detail_rows")
        if isinstance(rows, list):
            prices.update(_prices_from_holdings_rows(rows))
    return prices


def _prices_from_holdings_rows(rows: list[Any]) -> dict[str, float]:
    out: dict[str, float] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        ticker = str(row.get("ticker") or row.get("symbol") or "").upper().strip()
        price = _float_or_none(row.get("market_price") or row.get("price"))
        if ticker and price and price > 0:
            out[ticker] = price
    return out


def _clean_prices(value: Any) -> dict[str, float]:
    out: dict[str, float] = {}
    if not isinstance(value, dict):
        return out
    for ticker, raw in value.items():
        key = str(ticker or "").upper().strip()
        price = _float_or_none(raw)
        if key and price and price > 0:
            out[key] = price
    return out


def _weight_diff(target: dict[str, float], actual: dict[str, float]) -> dict[str, Any]:
    tickers = sorted((set(target) | set(actual)) - {"CASH"})
    diffs = []
    max_abs = 0.0
    for ticker in tickers:
        target_weight = float(target.get(ticker, 0.0) or 0.0)
        actual_weight = float(actual.get(ticker, 0.0) or 0.0)
        diff = round(actual_weight - target_weight, 6)
        max_abs = max(max_abs, abs(diff))
        if abs(diff) > 1e-9:
            diffs.append({
                "ticker": ticker,
                "target": round(target_weight, 6),
                "actual": round(actual_weight, 6),
                "diff": diff,
            })
    return {"max_abs_diff": round(max_abs, 6), "diffs": diffs}


def _clean_weights(value: Any) -> dict[str, float]:
    if not isinstance(value, dict):
        return {}
    out: dict[str, float] = {}
    for ticker, raw in value.items():
        key = str(ticker or "").upper().strip()
        number = _float_or_none(raw)
        if key and number is not None:
            out[key] = round(max(number, 0.0), 6)
    return out


def _int_or_none(value: Any) -> int | None:
    number = _float_or_none(value)
    return int(number) if number is not None else None


def _float_or_none(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number or number in {float("inf"), float("-inf")}:
        return None
    return number


def _bool_or_none(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        clean = value.strip().lower()
        if clean in {"true", "1", "yes"}:
            return True
        if clean in {"false", "0", "no"}:
            return False
    return bool(value)


def _strip_tz(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(UTC).replace(tzinfo=None)
