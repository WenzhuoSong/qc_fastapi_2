"""Persist account-state snapshots from non-heartbeat QC packets."""
from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from services.account_state_snapshot import build_account_state_snapshot


_PROCESSED_ACK_STATUSES = {
    "accepted",
    "orders_submitted",
    "partial",
    "filled",
    "reconciled",
    "reconciliation_drift",
    "failed_no_fill",
}


def is_usable_execution_ack_account_state(account_state: dict[str, Any] | None) -> bool:
    """Return true when an ACK carries enough account truth to store safely."""
    if not isinstance(account_state, dict) or not account_state:
        return False
    return (
        isinstance(account_state.get("holdings_weights"), dict)
        or account_state.get("open_order_count") is not None
        or account_state.get("open_orders_count") is not None
    )


def build_execution_ack_account_snapshot(
    *,
    account_state: dict[str, Any],
    command_id: str,
    ack_status: str,
    holdings_weights: dict[str, Any] | None = None,
    target_weights: dict[str, Any] | None = None,
    fallback_account_state: dict[str, Any] | None = None,
    received_at: datetime | None = None,
) -> dict[str, Any]:
    """Build an AccountStateSnapshot row payload from a QC execution ACK."""
    state = _merge_defined(fallback_account_state or {}, account_state or {})
    normalized_status = str(ack_status or "").lower().strip()
    clean_command_id = str(command_id or "").strip()

    if isinstance(holdings_weights, dict) and holdings_weights:
        state["holdings_weights"] = holdings_weights
    if isinstance(target_weights, dict) and target_weights:
        state["target_weights"] = target_weights

    if clean_command_id and normalized_status in _PROCESSED_ACK_STATUSES:
        state["last_command_id"] = clean_command_id
        state.setdefault("active_execution_status", normalized_status)

    payload = {
        "packet_type": "execution_ack",
        "timestamp_utc": state.get("timestamp_utc") or state.get("as_of") or datetime.now(UTC).isoformat(),
        "policy_version": state.get("policy_version"),
        "last_command_id": state.get("last_command_id"),
        "active_command_id": state.get("active_command_id"),
        "active_execution_status": state.get("active_execution_status"),
        "processed_command_count": state.get("processed_command_count"),
        "target_weights": state.get("target_weights") or target_weights or {},
        "account_state": state,
    }
    return build_account_state_snapshot(payload, received_at=received_at or datetime.now(UTC))


async def ingest_execution_ack_snapshot(
    *,
    account_state: dict[str, Any] | None,
    command_id: str,
    ack_status: str,
    holdings_weights: dict[str, Any] | None = None,
    target_weights: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Persist a source=execution_ack AccountStateSnapshot when ACK state is usable."""
    if not is_usable_execution_ack_account_state(account_state):
        return {
            "ingested": False,
            "reason": "ack_account_state_not_usable",
            "command_id": str(command_id or "").strip(),
        }

    from db.models import AccountStateSnapshot
    from db.session import AsyncSessionLocal
    from sqlalchemy import desc, select

    async with AsyncSessionLocal() as db:
        latest = (
            await db.execute(
                select(AccountStateSnapshot)
                .order_by(desc(AccountStateSnapshot.recorded_at), desc(AccountStateSnapshot.id))
                .limit(1)
            )
        ).scalar_one_or_none()
        snapshot = build_execution_ack_account_snapshot(
            account_state=account_state or {},
            command_id=command_id,
            ack_status=ack_status,
            holdings_weights=holdings_weights,
            target_weights=target_weights,
            fallback_account_state=_fallback_account_state_from_snapshot(latest),
        )
        row = AccountStateSnapshot(**_account_state_snapshot_model_kwargs(snapshot))
        db.add(row)
        await db.commit()
        await db.refresh(row)
        return {
            "ingested": True,
            "snapshot_id": row.id,
            "command_id": str(command_id or "").strip(),
            "source_packet_type": snapshot["source_packet_type"],
            "last_command_id": snapshot["last_command_id"],
            "active_execution_status": snapshot["active_execution_status"],
        }


def _account_state_snapshot_model_kwargs(snapshot: dict[str, Any]) -> dict[str, Any]:
    keys = (
        "qc_snapshot_id",
        "recorded_at",
        "account_timestamp",
        "source_packet_type",
        "contract_version",
        "account_status",
        "data_status",
        "policy_version",
        "total_value",
        "cash",
        "cash_pct",
        "buying_power",
        "open_order_count",
        "has_open_orders",
        "is_market_open",
        "last_command_id",
        "active_command_id",
        "active_execution_status",
        "processed_command_count",
        "holdings_weights",
        "target_weights",
        "raw_snapshot",
    )
    return {key: snapshot.get(key) for key in keys}


def _fallback_account_state_from_snapshot(snapshot: Any) -> dict[str, Any]:
    if snapshot is None:
        return {}
    return {
        "timestamp_utc": _iso_or_none(getattr(snapshot, "recorded_at", None)),
        "account_status": getattr(snapshot, "account_status", None),
        "data_status": getattr(snapshot, "data_status", None),
        "policy_version": getattr(snapshot, "policy_version", None),
        "total_value": _float_or_none(getattr(snapshot, "total_value", None)),
        "cash": _float_or_none(getattr(snapshot, "cash", None)),
        "cash_pct": _float_or_none(getattr(snapshot, "cash_pct", None)),
        "buying_power": _float_or_none(getattr(snapshot, "buying_power", None)),
        "open_order_count": getattr(snapshot, "open_order_count", None),
        "has_open_orders": getattr(snapshot, "has_open_orders", None),
        "is_market_open": getattr(snapshot, "is_market_open", None),
        "last_command_id": getattr(snapshot, "last_command_id", None),
        "active_command_id": getattr(snapshot, "active_command_id", None),
        "active_execution_status": getattr(snapshot, "active_execution_status", None),
        "processed_command_count": getattr(snapshot, "processed_command_count", None),
        "holdings_weights": getattr(snapshot, "holdings_weights", None) or {},
        "target_weights": getattr(snapshot, "target_weights", None) or {},
    }


def _merge_defined(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    out = dict(base or {})
    for key, value in (override or {}).items():
        if value is not None:
            out[key] = value
    return out


def _iso_or_none(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
