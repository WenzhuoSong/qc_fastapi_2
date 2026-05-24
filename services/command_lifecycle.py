"""Append-only command lifecycle events."""
from __future__ import annotations

from datetime import UTC, datetime
from typing import Any


VALID_EVENT_TYPES = {
    "created",
    "preflight_passed",
    "preflight_blocked",
    "submitted_to_qc",
    "qc_accepted",
    "qc_rejected",
    "qc_timeout",
    "execution_result",
    "filled",
    "partial",
    "reconciled",
    "reconciliation_drift",
}

DEFAULT_RECONCILIATION_TOLERANCE = 0.01


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
        "payload": payload or {},
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

    target = _target_weights_for_reconciliation(command_payload or {}, response, account)
    holdings = _holdings_weights_for_reconciliation(response, account)
    if not target or not holdings:
        return events

    diff = _weight_diff(target, holdings)
    payload = {
        "tolerance": tolerance,
        "max_abs_diff": diff["max_abs_diff"],
        "diffs": diff["diffs"],
        "target_weights": target,
        "actual_holdings_weights": holdings,
        "order_summary": order_summary,
        "account_state": _account_reconciliation_payload(account),
    }
    if diff["max_abs_diff"] <= float(tolerance) and not _has_open_orders(order_summary, account):
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
    elif diff["max_abs_diff"] > float(tolerance):
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
    filled = _int_or_none(order_summary.get("filled_order_count"))
    submitted = _int_or_none(order_summary.get("submitted_order_count"))
    action_count = _int_or_none(order_summary.get("action_count"))
    all_filled = _bool_or_none(order_summary.get("all_filled"))
    if all_filled is True:
        return True
    if filled and filled > 0:
        return True
    return bool((submitted and submitted > 0) or (action_count and action_count > 0)) and not _has_open_orders(order_summary, {})


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
        "open_order_count": account.get("open_order_count"),
        "has_open_orders": account.get("has_open_orders"),
        "last_command_id": account.get("last_command_id"),
    }


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
