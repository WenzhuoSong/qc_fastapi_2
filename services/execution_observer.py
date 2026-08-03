"""Observer-only execution tape for QC-owned strategies.

This module records what QC actually did. It never creates targets, submits
orders, cancels orders, or recommends an automatic trade response.
"""
from __future__ import annotations

import hashlib
import json
import math
from datetime import date, datetime
from typing import Any

from sqlalchemy import desc, func, select
from sqlalchemy.dialects.postgresql import insert

from db.models import ExecutionObservedFill, ExecutionObservedOrder


EXECUTION_TAPE_SCHEMA_VERSION = "execution_tape_observer_v1"
EXECUTION_AUTHORITY = "none"
TARGET_WEIGHT_MUTATION = "none"


async def record_observed_execution_tape(
    db: Any,
    *,
    live_snapshot_id: int | None,
    snapshot_record: dict[str, Any],
) -> dict[str, Any]:
    """Persist observed orders/fills from a StrategyLiveSnapshot payload.

    The input is the already-normalized StrategyLiveSnapshot record. The
    function is idempotent: each order/fill event has a deterministic UID and
    is upserted.
    """
    orders = _rows(snapshot_record.get("orders"))
    fills = _rows(snapshot_record.get("fills"))
    order_count = 0
    fill_count = 0

    for order in orders:
        record = _order_record(
            order,
            live_snapshot_id=live_snapshot_id,
            snapshot_record=snapshot_record,
        )
        if not record:
            continue
        stmt = insert(ExecutionObservedOrder).values(record)
        update_cols = {
            key: getattr(stmt.excluded, key)
            for key in record
            if key not in {"id", "event_uid", "created_at"}
        }
        await db.execute(
            stmt.on_conflict_do_update(
                constraint="uq_execution_observed_order_event_uid",
                set_=update_cols,
            )
        )
        order_count += 1

    for fill in fills:
        record = _fill_record(
            fill,
            live_snapshot_id=live_snapshot_id,
            snapshot_record=snapshot_record,
        )
        if not record:
            continue
        stmt = insert(ExecutionObservedFill).values(record)
        update_cols = {
            key: getattr(stmt.excluded, key)
            for key in record
            if key not in {"id", "fill_uid", "created_at"}
        }
        await db.execute(
            stmt.on_conflict_do_update(
                constraint="uq_execution_observed_fill_uid",
                set_=update_cols,
            )
        )
        fill_count += 1

    return {
        "schema_version": EXECUTION_TAPE_SCHEMA_VERSION,
        "execution_authority": EXECUTION_AUTHORITY,
        "target_weight_mutation": TARGET_WEIGHT_MUTATION,
        "orders_seen": len(orders),
        "fills_seen": len(fills),
        "orders_upserted": order_count,
        "fills_upserted": fill_count,
    }


async def build_execution_tape_summary(
    db: Any,
    *,
    strategy_id: str | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    strategy_filter = str(strategy_id or "").lower().strip()
    fills_stmt = select(ExecutionObservedFill).order_by(
        desc(ExecutionObservedFill.event_time),
        desc(ExecutionObservedFill.id),
    ).limit(max(int(limit), 1))
    orders_stmt = select(ExecutionObservedOrder).order_by(
        desc(ExecutionObservedOrder.event_time),
        desc(ExecutionObservedOrder.id),
    ).limit(max(int(limit), 1))
    if strategy_filter:
        fills_stmt = fills_stmt.where(ExecutionObservedFill.strategy_id == strategy_filter)
        orders_stmt = orders_stmt.where(ExecutionObservedOrder.strategy_id == strategy_filter)

    fills = list((await db.execute(fills_stmt)).scalars().all())
    orders = list((await db.execute(orders_stmt)).scalars().all())

    aggregate_stmt = select(
        func.count(ExecutionObservedFill.id),
        func.coalesce(func.sum(ExecutionObservedFill.notional), 0.0),
        func.coalesce(func.sum(ExecutionObservedFill.fee), 0.0),
        func.coalesce(func.avg(ExecutionObservedFill.fee_bps), 0.0),
        func.max(ExecutionObservedFill.event_time),
    )
    if strategy_filter:
        aggregate_stmt = aggregate_stmt.where(ExecutionObservedFill.strategy_id == strategy_filter)
    aggregate = (await db.execute(aggregate_stmt)).one()

    by_side = _by_side(fills)
    return {
        "schema_version": EXECUTION_TAPE_SCHEMA_VERSION,
        "execution_authority": EXECUTION_AUTHORITY,
        "target_weight_mutation": TARGET_WEIGHT_MUTATION,
        "strategy_id": strategy_filter or "all",
        "fill_count": int(aggregate[0] or 0),
        "gross_notional": _round(aggregate[1]),
        "total_fees": _round(aggregate[2]),
        "avg_fee_bps": _round(aggregate[3], 4),
        "latest_fill_at": _iso(aggregate[4]),
        "by_side": by_side,
        "recent_orders": [_order_payload(row) for row in orders],
        "recent_fills": [_fill_payload(row) for row in fills],
        "operator_contract": {
            "read_only": True,
            "automatic_trade_response": "forbidden",
        },
    }


def _order_record(
    order: dict[str, Any],
    *,
    live_snapshot_id: int | None,
    snapshot_record: dict[str, Any],
) -> dict[str, Any] | None:
    ticker = _ticker(order)
    if not ticker:
        return None
    event_time = _parse_datetime(order.get("time_utc") or order.get("time") or order.get("event_time"))
    order_id = _clean(order.get("order_id") or order.get("id"))
    event_uid = _event_uid(
        "order",
        snapshot_record,
        ticker,
        order_id,
        _clean(order.get("status")),
        _clean(order.get("direction")),
        _num(order.get("quantity")),
        event_time,
        order,
    )
    return {
        **_base_event_fields(snapshot_record, live_snapshot_id=live_snapshot_id),
        "event_uid": event_uid,
        "event_time": event_time,
        "order_id": order_id,
        "ticker": ticker,
        "direction": _direction(order),
        "quantity": _num(order.get("quantity")),
        "status": _clean(order.get("status")),
        "order_type": _clean(order.get("order_type") or order.get("type")),
        "limit_price": _num(order.get("limit_price")),
        "stop_price": _num(order.get("stop_price")),
        "raw_order": _json_safe(order),
    }


def _fill_record(
    fill: dict[str, Any],
    *,
    live_snapshot_id: int | None,
    snapshot_record: dict[str, Any],
) -> dict[str, Any] | None:
    ticker = _ticker(fill)
    if not ticker:
        return None
    event_time = _parse_datetime(fill.get("time_utc") or fill.get("time") or fill.get("event_time"))
    order_id = _clean(fill.get("order_id") or fill.get("id"))
    quantity = _num(fill.get("quantity") or fill.get("fill_quantity"))
    fill_price = _num(fill.get("fill_price") or fill.get("price"))
    fee = _num(fill.get("fee") or fill.get("order_fee"))
    notional = abs(quantity * fill_price) if quantity is not None and fill_price is not None else None
    signed_notional = _signed_notional(
        direction=_direction(fill),
        quantity=quantity,
        fill_price=fill_price,
    )
    fee_bps = (abs(fee) / notional * 10000.0) if fee is not None and notional and notional > 0 else None
    fill_uid = _event_uid(
        "fill",
        snapshot_record,
        ticker,
        order_id,
        _clean(fill.get("direction")),
        quantity,
        fill_price,
        event_time,
        fill,
    )
    return {
        **_base_event_fields(snapshot_record, live_snapshot_id=live_snapshot_id),
        "fill_uid": fill_uid,
        "event_time": event_time,
        "order_id": order_id,
        "ticker": ticker,
        "direction": _direction(fill),
        "quantity": quantity,
        "fill_price": fill_price,
        "fee": fee,
        "notional": notional,
        "signed_notional": signed_notional,
        "fee_bps": fee_bps,
        "raw_fill": _json_safe(fill),
    }


def _base_event_fields(snapshot_record: dict[str, Any], *, live_snapshot_id: int | None) -> dict[str, Any]:
    observed_at = snapshot_record.get("recorded_at")
    if not isinstance(observed_at, datetime):
        observed_at = _parse_datetime(observed_at) or datetime.utcnow()
    trading_date = snapshot_record.get("trading_date")
    if not isinstance(trading_date, date):
        trading_date = _parse_date(trading_date) or observed_at.date()
    return {
        "strategy_live_snapshot_id": live_snapshot_id,
        "qc_snapshot_id": snapshot_record.get("qc_snapshot_id"),
        "snapshot_uid": _clean(snapshot_record.get("snapshot_uid")),
        "strategy_id": str(snapshot_record.get("strategy_id") or "unknown").lower().strip(),
        "algorithm_version": _clean(snapshot_record.get("algorithm_version")),
        "trading_date": trading_date,
        "observed_at": observed_at,
    }


def _event_uid(*parts: Any) -> str:
    if len(parts) >= 2 and isinstance(parts[1], dict):
        snapshot_record = parts[1]
        stable_parts = (
            parts[0],
            {
                "strategy_id": str(snapshot_record.get("strategy_id") or "").lower().strip(),
                "algorithm_version": snapshot_record.get("algorithm_version"),
                "trading_date": _iso(snapshot_record.get("trading_date")),
            },
            *parts[2:],
        )
    else:
        stable_parts = parts
    blob = json.dumps(_json_safe(stable_parts), sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _by_side(fills: list[Any]) -> dict[str, dict[str, float | int]]:
    out: dict[str, dict[str, float | int]] = {}
    for row in fills:
        side = str(getattr(row, "direction", None) or "unknown").lower()
        bucket = out.setdefault(side, {"count": 0, "notional": 0.0, "fees": 0.0})
        bucket["count"] = int(bucket["count"]) + 1
        bucket["notional"] = float(bucket["notional"]) + float(getattr(row, "notional", None) or 0.0)
        bucket["fees"] = float(bucket["fees"]) + float(getattr(row, "fee", None) or 0.0)
    for bucket in out.values():
        bucket["notional"] = _round(bucket["notional"])
        bucket["fees"] = _round(bucket["fees"])
    return out


def _order_payload(row: Any) -> dict[str, Any]:
    return {
        "event_time": _iso(getattr(row, "event_time", None)),
        "trading_date": _iso(getattr(row, "trading_date", None)),
        "strategy_id": getattr(row, "strategy_id", None),
        "algorithm_version": getattr(row, "algorithm_version", None),
        "order_id": getattr(row, "order_id", None),
        "ticker": getattr(row, "ticker", None),
        "direction": getattr(row, "direction", None),
        "quantity": getattr(row, "quantity", None),
        "status": getattr(row, "status", None),
        "order_type": getattr(row, "order_type", None),
    }


def _fill_payload(row: Any) -> dict[str, Any]:
    return {
        "event_time": _iso(getattr(row, "event_time", None)),
        "trading_date": _iso(getattr(row, "trading_date", None)),
        "strategy_id": getattr(row, "strategy_id", None),
        "algorithm_version": getattr(row, "algorithm_version", None),
        "order_id": getattr(row, "order_id", None),
        "ticker": getattr(row, "ticker", None),
        "direction": getattr(row, "direction", None),
        "quantity": getattr(row, "quantity", None),
        "fill_price": getattr(row, "fill_price", None),
        "fee": getattr(row, "fee", None),
        "notional": _round(getattr(row, "notional", None)),
        "fee_bps": _round(getattr(row, "fee_bps", None), 4),
    }


def _signed_notional(*, direction: str | None, quantity: float | None, fill_price: float | None) -> float | None:
    if quantity is None or fill_price is None:
        return None
    side = str(direction or "").lower()
    sign = -1.0 if side in {"sell", "sold", "short"} or quantity < 0 else 1.0
    return sign * abs(quantity) * fill_price


def _ticker(row: dict[str, Any]) -> str | None:
    ticker = _clean(row.get("ticker") or row.get("symbol"))
    return ticker.upper() if ticker else None


def _direction(row: dict[str, Any]) -> str | None:
    direction = _clean(row.get("direction") or row.get("side"))
    return direction.lower() if direction else None


def _rows(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [row for row in value if isinstance(row, dict)]


def _clean(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _num(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    if not value:
        return None
    text = str(value).replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(text).replace(tzinfo=None)
    except ValueError:
        return None


def _parse_date(value: Any) -> date | None:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if not value:
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return str(value)


def _round(value: Any, digits: int = 2) -> float | None:
    number = _num(value)
    return round(number, digits) if number is not None else None


def _iso(value: Any) -> str | None:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return str(value) if value is not None else None
