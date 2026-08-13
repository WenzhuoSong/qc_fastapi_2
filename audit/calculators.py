"""Pure observer audit calculations with no trading side effects."""
from __future__ import annotations

from decimal import Decimal
from typing import Any, Iterable


ZERO = Decimal("0")
TERMINAL_STATUSES = frozenset({"filled", "canceled", "cancelled", "invalid", "rejected"})
REJECTED_STATUSES = frozenset({"invalid", "rejected"})


def decimal(value: Any) -> Decimal:
    if value is None:
        return ZERO
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def target_reconciliation(
    targets: dict[str, Any],
    *,
    equity: Any | None,
    cash: Any | None,
    holdings: Iterable[dict[str, Any]],
) -> dict[str, Any]:
    target_map = {str(key): decimal(value) for key, value in targets.items()}
    target_exposure = sum((abs(value) for value in target_map.values()), ZERO)
    if equity is None or decimal(equity) <= ZERO:
        return {
            "target_exposure": target_exposure,
            "actual_exposure": None,
            "cash_weight": None,
            "tracking_error_l1": None,
            "target_weights": target_map,
            "actual_weights": {},
            "unmatched_actual": [],
        }

    nav = decimal(equity)
    target_keys = set(target_map)
    actual_map: dict[str, Decimal] = {}
    unmatched: list[str] = []
    for holding in holdings:
        ticker = str(holding.get("ticker") or "")
        security_id = str(holding.get("security_id") or "")
        key = ticker if ticker in target_keys else security_id if security_id in target_keys else ticker or security_id
        if not key:
            continue
        actual_map[key] = actual_map.get(key, ZERO) + decimal(holding.get("holdings_value")) / nav
        if key not in target_keys:
            unmatched.append(key)

    all_keys = set(target_map) | set(actual_map)
    tracking_error = sum(
        (abs(target_map.get(key, ZERO) - actual_map.get(key, ZERO)) for key in all_keys),
        ZERO,
    )
    return {
        "target_exposure": target_exposure,
        "actual_exposure": sum((abs(value) for value in actual_map.values()), ZERO),
        "cash_weight": decimal(cash) / nav,
        "tracking_error_l1": tracking_error,
        "target_weights": target_map,
        "actual_weights": actual_map,
        "unmatched_actual": sorted(set(unmatched)),
    }


def summarize_intent(intent: dict[str, Any], lifecycle: Iterable[dict[str, Any]]) -> dict[str, Any]:
    events = sorted(lifecycle, key=lambda item: item["event_time"])
    requested = decimal(intent.get("submitted_quantity")) or decimal(intent.get("requested_quantity"))
    incremental_fill = ZERO
    cumulative_fill: Decimal | None = None
    first_fill_time = None
    terminal_time = None
    first_event_time = events[0]["event_time"] if events else None
    fees = ZERO
    rejected = False
    terminal = False
    remaining_quantity: Decimal | None = None

    for event in events:
        status = str(event.get("status") or "").lower()
        fill = decimal(event.get("fill_quantity"))
        if event.get("fill_quantity_semantics") == "cumulative_reconstructed":
            cumulative_fill = fill
        else:
            incremental_fill += fill
        if first_fill_time is None and (fill != ZERO or status in {"partiallyfilled", "partial", "filled"}):
            first_fill_time = event["event_time"]
        if status in TERMINAL_STATUSES:
            terminal = True
            terminal_time = event["event_time"]
        if status in REJECTED_STATUSES:
            rejected = True
        if event.get("remaining_quantity") is not None:
            remaining_quantity = decimal(event["remaining_quantity"])
        fees += decimal(event.get("fee"))

    filled = cumulative_fill if cumulative_fill is not None else incremental_fill
    denominator = abs(requested)
    fill_ratio = min(abs(filled) / denominator, Decimal("1")) if denominator else None
    if remaining_quantity is None and denominator:
        remaining_quantity = max(denominator - abs(filled), ZERO)
    partial = bool(fill_ratio is not None and ZERO < fill_ratio < Decimal("0.999999"))
    return {
        "requested_quantity": requested,
        "filled_quantity": filled,
        "fill_ratio": fill_ratio,
        "remaining_quantity": remaining_quantity,
        "first_event_time": first_event_time,
        "first_fill_time": first_fill_time,
        "terminal_time": terminal_time,
        "terminal": terminal,
        "partial": partial,
        "rejected": rejected,
        "fees": fees,
        "event_count": len(events),
        "evidence_raw_event_ids": [int(item["raw_event_id"]) for item in events],
    }


def risk_intent_candidate(risk: dict[str, Any], intent: dict[str, Any]) -> bool:
    if risk["event_stream_id"] != intent["event_stream_id"]:
        return False
    if risk["security_id"] != intent["security_id"]:
        return False
    if intent["event_time"] < risk["event_time"]:
        return False
    age_seconds = (intent["event_time"] - risk["event_time"]).total_seconds()
    if age_seconds > 7 * 24 * 60 * 60:
        return False
    marker = f"{intent.get('risk_class', '')} {intent.get('reason', '')}".lower()
    return any(token in marker for token in ("risk", "atr", "stop", "exit", "liquidat"))
