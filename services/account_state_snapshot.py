"""Normalize QC account-state facts into a stable execution guard contract."""
from __future__ import annotations

from datetime import UTC, datetime
from typing import Any


ACCOUNT_STATE_CONTRACT_VERSION = "v1"


def build_account_state_snapshot(
    payload: dict[str, Any],
    *,
    qc_snapshot_id: int | None = None,
    received_at: datetime | None = None,
) -> dict[str, Any]:
    """Build a versioned account-state snapshot from QC heartbeat/ACK payloads.

    New QC payloads should send top-level ``account_state``. Legacy heartbeats
    are still useful, so this function derives the same contract from
    ``portfolio`` and ``holdings`` when explicit account state is missing.
    """
    payload = payload or {}
    explicit = payload.get("account_state") if isinstance(payload.get("account_state"), dict) else {}
    portfolio = payload.get("portfolio") if isinstance(payload.get("portfolio"), dict) else {}
    holdings_rows = _rows(payload.get("holdings") or payload.get("features"))

    total_value = _first_number(
        explicit,
        portfolio,
        ("total_portfolio_value", "total_value", "portfolio_value"),
    )
    cash = _first_number(explicit, portfolio, ("cash", "cash_value"))
    cash_pct = _first_number(explicit, portfolio, ("cash_pct", "cash_weight"))
    if cash_pct is None and cash is not None and total_value and total_value > 0:
        cash_pct = cash / total_value

    open_order_count = _int_or_none(
        explicit.get("open_order_count", explicit.get("open_orders_count"))
    )
    has_open_orders = explicit.get("has_open_orders")
    if has_open_orders is None and open_order_count is not None:
        has_open_orders = open_order_count > 0

    holdings_weights = explicit.get("holdings_weights")
    if not isinstance(holdings_weights, dict):
        holdings_weights = _holdings_weights_from_rows(holdings_rows)
    else:
        holdings_weights = _clean_weight_map(holdings_weights)

    target_weights = explicit.get("target_weights")
    if not isinstance(target_weights, dict):
        target_weights = payload.get("target_weights") if isinstance(payload.get("target_weights"), dict) else {}
    target_weights = _clean_weight_map(target_weights)

    timestamp = explicit.get("timestamp_utc") or payload.get("timestamp_utc") or explicit.get("as_of")
    account_timestamp = _parse_timestamp(timestamp)
    recorded_at = _strip_tz(received_at) or account_timestamp or datetime.now(UTC).replace(tzinfo=None)

    warnings: list[str] = []
    if not explicit:
        warnings.append("legacy_payload_without_explicit_account_state")
    if total_value is None:
        warnings.append("missing_total_value")
    if cash is None:
        warnings.append("missing_cash")
    if _first_number(explicit, portfolio, ("buying_power", "margin_remaining")) is None:
        warnings.append("missing_buying_power")
    if open_order_count is None:
        warnings.append("missing_open_order_count")

    policy_version = (
        explicit.get("policy_version")
        or payload.get("policy_version")
        or _nested(payload, ("policy", "version"))
        or "unknown"
    )

    return {
        "contract_version": ACCOUNT_STATE_CONTRACT_VERSION,
        "qc_snapshot_id": qc_snapshot_id,
        "source_packet_type": str(payload.get("packet_type") or "unknown"),
        "recorded_at": recorded_at,
        "account_timestamp": account_timestamp,
        "account_status": str(explicit.get("account_status") or "unknown"),
        "data_status": str(explicit.get("data_status") or "unknown"),
        "policy_version": str(policy_version),
        "total_value": _round_or_none(total_value, 2),
        "cash": _round_or_none(cash, 2),
        "cash_pct": _round_or_none(cash_pct, 6),
        "buying_power": _round_or_none(_first_number(explicit, portfolio, ("buying_power", "margin_remaining")), 2),
        "open_order_count": open_order_count,
        "has_open_orders": bool(has_open_orders) if has_open_orders is not None else None,
        "is_market_open": _bool_or_none(explicit.get("is_market_open", portfolio.get("is_market_open"))),
        "holdings_weights": holdings_weights,
        "target_weights": target_weights,
        "raw_snapshot": {
            "contract_version": ACCOUNT_STATE_CONTRACT_VERSION,
            "explicit_account_state": bool(explicit),
            "timestamp_utc": timestamp,
            "policy_source": explicit.get("policy_source"),
            "last_command_id": explicit.get("last_command_id"),
            "processed_command_count": _int_or_none(explicit.get("processed_command_count")),
            "open_order_count": open_order_count,
            "has_open_orders": bool(has_open_orders) if has_open_orders is not None else None,
            "warnings": warnings,
        },
    }


def _rows(value: Any) -> list[dict[str, Any]]:
    return [row for row in value or [] if isinstance(row, dict)]


def _holdings_weights_from_rows(rows: list[dict[str, Any]]) -> dict[str, float]:
    out: dict[str, float] = {}
    for row in rows:
        ticker = str(row.get("ticker") or "").upper().strip()
        if not ticker:
            continue
        weight = _number_or_none(row.get("weight_current"))
        if weight is not None:
            out[ticker] = round(weight, 6)
    return out


def _clean_weight_map(weights: dict[str, Any]) -> dict[str, float]:
    out: dict[str, float] = {}
    for ticker, raw in weights.items():
        key = str(ticker or "").upper().strip()
        value = _number_or_none(raw)
        if key and value is not None:
            out[key] = round(value, 6)
    return out


def _first_number(primary: dict[str, Any], fallback: dict[str, Any], keys: tuple[str, ...]) -> float | None:
    for key in keys:
        value = _number_or_none(primary.get(key))
        if value is not None:
            return value
    for key in keys:
        value = _number_or_none(fallback.get(key))
        if value is not None:
            return value
    return None


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


def _int_or_none(value: Any) -> int | None:
    number = _number_or_none(value)
    return int(number) if number is not None else None


def _bool_or_none(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes"}:
            return True
        if lowered in {"false", "0", "no"}:
            return False
    return bool(value)


def _round_or_none(value: float | None, digits: int) -> float | None:
    return round(value, digits) if value is not None else None


def _parse_timestamp(value: Any) -> datetime | None:
    if not value:
        return None
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


def _strip_tz(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value
    return value.astimezone(UTC).replace(tzinfo=None)


def _nested(data: dict[str, Any], path: tuple[str, ...]) -> Any:
    cur: Any = data
    for key in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
    return cur
