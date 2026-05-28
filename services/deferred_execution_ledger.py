"""Deferred execution ledger for staged execution throttle deltas.

This ledger is diagnostic and operational bookkeeping. It records the portion
of a desired target that was intentionally deferred by the execution throttle,
then reviews open carryover items on the next pipeline cycle.
"""
from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from typing import Any


DEFERRED_EXECUTION_CONTRACT_VERSION = "v1"
OPEN_DEFERRED_STATUSES = {"open"}


def build_deferred_execution_items(
    *,
    analysis_id: int,
    command_id: str,
    throttle: dict[str, Any],
) -> list[dict[str, Any]]:
    """Build current-cycle deferred items from an execution throttle result."""
    deferred = _clean_weights(throttle.get("deferred_delta") or {}, keep_negative=True)
    if not deferred:
        return []
    desired = _clean_weights(throttle.get("desired_target_weights") or {})
    staged = _clean_weights(throttle.get("staged_target_weights") or {})
    current = _clean_weights(throttle.get("current_weights") or {})
    items: list[dict[str, Any]] = []
    for ticker, delta in sorted(deferred.items()):
        if ticker == "CASH" or abs(delta) <= 1e-9:
            continue
        side = "buy" if delta > 0 else "sell"
        item = {
            "deferred_id": _deferred_id(command_id, ticker, side, delta),
            "analysis_id": int(analysis_id),
            "command_id": command_id,
            "source": "execution_throttle",
            "status": "open",
            "side": side,
            "ticker": ticker,
            "original_delta": round(delta, 6),
            "remaining_delta": round(delta, 6),
            "current_weight": round(float(current.get(ticker, 0.0) or 0.0), 6),
            "desired_weight": round(float(desired.get(ticker, 0.0) or 0.0), 6),
            "staged_weight": round(float(staged.get(ticker, 0.0) or 0.0), 6),
            "reason": throttle.get("reason") or "execution_delta_deferred",
            "raw_payload": {
                "contract_version": DEFERRED_EXECUTION_CONTRACT_VERSION,
                "execution_throttle_contract_version": throttle.get("contract_version"),
                "metrics_before": throttle.get("metrics_before") or {},
                "metrics_after": throttle.get("metrics_after") or {},
                "limits": throttle.get("limits") or {},
                "buy_scale": throttle.get("buy_scale"),
                "violations": throttle.get("violations") or [],
            },
        }
        items.append(item)
    return items


def review_deferred_execution_items(
    *,
    open_items: list[dict[str, Any]],
    desired_target_weights: dict[str, Any],
    staged_target_weights: dict[str, Any],
    current_weights: dict[str, Any],
    tolerance: float = 0.0005,
) -> list[dict[str, Any]]:
    """Classify prior deferred items against this cycle's current plan.

    Status meanings:
    - executed: current holdings already reached the old deferred desired level.
    - cancelled: the current desired target no longer asks for that deferred side.
    - still_valid: the deferred side still exists and may be carried by the
      current staged command or a future command.
    """
    desired = _clean_weights(desired_target_weights or {})
    staged = _clean_weights(staged_target_weights or {})
    current = _clean_weights(current_weights or {})
    reviews: list[dict[str, Any]] = []
    for item in open_items or []:
        ticker = str(item.get("ticker") or "").upper().strip()
        if not ticker:
            continue
        side = str(item.get("side") or "").lower().strip()
        if side not in {"buy", "sell"}:
            side = "buy" if float(item.get("remaining_delta") or 0.0) >= 0 else "sell"
        current_w = float(current.get(ticker, 0.0) or 0.0)
        desired_w = float(desired.get(ticker, 0.0) or 0.0)
        staged_w = float(staged.get(ticker, 0.0) or 0.0)
        previous_desired = float(item.get("desired_weight") or 0.0)

        if side == "buy":
            if current_w >= previous_desired - tolerance:
                status = "executed"
                reason = "holdings_reached_deferred_desired_weight"
                remaining = 0.0
            elif desired_w <= current_w + tolerance:
                status = "cancelled"
                reason = "current_plan_no_longer_requires_buy_delta"
                remaining = 0.0
            elif staged_w > current_w + tolerance:
                status = "still_valid"
                reason = "included_in_current_staged_command"
                remaining = max(desired_w - staged_w, 0.0)
            else:
                status = "still_valid"
                reason = "waiting_for_buy_capacity"
                remaining = max(desired_w - current_w, 0.0)
        else:
            if current_w <= previous_desired + tolerance:
                status = "executed"
                reason = "holdings_reached_deferred_desired_weight"
                remaining = 0.0
            elif desired_w >= current_w - tolerance:
                status = "cancelled"
                reason = "current_plan_no_longer_requires_sell_delta"
                remaining = 0.0
            elif staged_w < current_w - tolerance:
                status = "still_valid"
                reason = "included_in_current_staged_command"
                remaining = min(desired_w - staged_w, 0.0)
            else:
                status = "still_valid"
                reason = "waiting_for_sell_capacity"
                remaining = min(desired_w - current_w, 0.0)

        reviews.append({
            "deferred_id": item.get("deferred_id"),
            "ticker": ticker,
            "side": side,
            "status": status,
            "reason": reason,
            "previous_remaining_delta": _round_float(item.get("remaining_delta")),
            "remaining_delta": round(float(remaining or 0.0), 6),
            "current_weight": round(current_w, 6),
            "desired_weight": round(desired_w, 6),
            "staged_weight": round(staged_w, 6),
        })
    return reviews


def summarize_deferred_execution_pressure(items: list[dict[str, Any]]) -> dict[str, Any]:
    """Summarize deferred pressure for dashboards and risk_out diagnostics."""
    rows = [
        item for item in (items or [])
        if str(item.get("status") or "open") in {"open", "still_valid"}
    ]
    buy_delta = 0.0
    sell_delta = 0.0
    for item in rows:
        delta = float(item.get("remaining_delta") or 0.0)
        if delta > 0:
            buy_delta += delta
        else:
            sell_delta += abs(delta)
    return {
        "open_count": len(rows),
        "open_buy_delta": round(buy_delta, 6),
        "open_sell_delta": round(sell_delta, 6),
        "open_gross_delta": round(buy_delta + sell_delta, 6),
        "tickers": sorted({str(item.get("ticker") or "").upper() for item in rows if item.get("ticker")}),
    }


async def record_deferred_execution_plan(
    *,
    analysis_id: int,
    command_id: str,
    throttle: dict[str, Any],
) -> dict[str, Any]:
    """Review prior carryover and persist this cycle's deferred deltas."""
    from sqlalchemy import select

    from db.models import DeferredExecutionLedger
    from db.session import AsyncSessionLocal

    now = _utcnow_db_naive()
    desired = throttle.get("desired_target_weights") or {}
    staged = throttle.get("staged_target_weights") or {}
    current = throttle.get("current_weights") or {}
    new_items = build_deferred_execution_items(
        analysis_id=analysis_id,
        command_id=command_id,
        throttle=throttle,
    )
    new_item_keys = {
        (str(item.get("ticker") or "").upper(), str(item.get("side") or "").lower())
        for item in new_items
    }
    async with AsyncSessionLocal() as db:
        open_rows = (
            await db.execute(
                select(DeferredExecutionLedger)
                .where(DeferredExecutionLedger.status.in_(sorted(OPEN_DEFERRED_STATUSES)))
                .order_by(DeferredExecutionLedger.created_at, DeferredExecutionLedger.id)
            )
        ).scalars().all()
        open_items = [_row_to_item(row) for row in open_rows]
        reviews = review_deferred_execution_items(
            open_items=open_items,
            desired_target_weights=desired,
            staged_target_weights=staged,
            current_weights=current,
        )
        review_by_id = {str(item.get("deferred_id")): item for item in reviews}
        for row in open_rows:
            review = review_by_id.get(str(row.deferred_id))
            if not review:
                continue
            row.latest_current_weight = review.get("current_weight")
            row.latest_desired_weight = review.get("desired_weight")
            row.latest_staged_weight = review.get("staged_weight")
            row.remaining_delta = review.get("remaining_delta")
            row.review_count = int(row.review_count or 0) + 1
            row.review_payload = review
            row.resolution_reason = review.get("reason")
            row.updated_at = now
            if review.get("status") in {"executed", "cancelled"}:
                row.status = review["status"]
                row.resolved_at = now
            elif (
                review.get("status") == "still_valid"
                and (str(row.ticker or "").upper(), str(row.side or "").lower()) in new_item_keys
            ):
                row.status = "carried_forward"
                row.resolution_reason = "carried_forward_to_current_deferred_item"
                row.resolved_at = now

        created_items: list[dict[str, Any]] = []
        for item in new_items:
            existing = (
                await db.execute(
                    select(DeferredExecutionLedger)
                    .where(DeferredExecutionLedger.deferred_id == item["deferred_id"])
                    .limit(1)
                )
            ).scalar_one_or_none()
            if existing:
                continue
            row = DeferredExecutionLedger(
                deferred_id=item["deferred_id"],
                analysis_id=item["analysis_id"],
                command_id=item["command_id"],
                source=item["source"],
                status=item["status"],
                side=item["side"],
                ticker=item["ticker"],
                original_delta=item["original_delta"],
                remaining_delta=item["remaining_delta"],
                current_weight=item["current_weight"],
                desired_weight=item["desired_weight"],
                staged_weight=item["staged_weight"],
                latest_current_weight=item["current_weight"],
                latest_desired_weight=item["desired_weight"],
                latest_staged_weight=item["staged_weight"],
                reason=item["reason"],
                raw_payload=item["raw_payload"],
                review_payload={},
            )
            db.add(row)
            created_items.append(item)
        await db.commit()

    open_after: list[dict[str, Any]] = []
    for item in open_items:
        review = review_by_id.get(str(item.get("deferred_id"))) or {}
        key = (str(item.get("ticker") or "").upper(), str(item.get("side") or "").lower())
        if not review:
            open_after.append(item)
            continue
        if review.get("status") == "still_valid" and key not in new_item_keys:
            open_after.append({
                **item,
                "status": "open",
                "remaining_delta": review.get("remaining_delta", item.get("remaining_delta")),
            })
    open_after.extend(created_items)
    return {
        "contract_version": DEFERRED_EXECUTION_CONTRACT_VERSION,
        "execution_effect": "diagnostic_only",
        "analysis_id": analysis_id,
        "command_id": command_id,
        "reviewed_count": len(reviews),
        "created_count": len(created_items),
        "reviews": reviews,
        "created_items": created_items,
        "open_pressure": summarize_deferred_execution_pressure(open_after),
    }


async def load_deferred_execution_dashboard(limit: int = 50) -> dict[str, Any]:
    """Load recent deferred execution rows for the read-only dashboard."""
    from sqlalchemy import desc, select

    from db.models import DeferredExecutionLedger
    from db.session import AsyncSessionLocal

    limit = max(min(int(limit or 50), 200), 1)
    async with AsyncSessionLocal() as db:
        rows = (
            await db.execute(
                select(DeferredExecutionLedger)
                .order_by(desc(DeferredExecutionLedger.created_at), desc(DeferredExecutionLedger.id))
                .limit(limit)
            )
        ).scalars().all()
    compact = [_row_to_item(row) for row in rows]
    open_rows = [row for row in compact if row.get("status") == "open"]
    return {
        "available": True,
        "contract_version": DEFERRED_EXECUTION_CONTRACT_VERSION,
        "summary": summarize_deferred_execution_pressure(open_rows),
        "recent_rows": compact,
    }


def _row_to_item(row: Any) -> dict[str, Any]:
    return {
        "deferred_id": getattr(row, "deferred_id", None),
        "analysis_id": getattr(row, "analysis_id", None),
        "command_id": getattr(row, "command_id", None),
        "source": getattr(row, "source", None),
        "status": getattr(row, "status", None),
        "side": getattr(row, "side", None),
        "ticker": getattr(row, "ticker", None),
        "original_delta": _round_float(getattr(row, "original_delta", None)),
        "remaining_delta": _round_float(getattr(row, "remaining_delta", None)),
        "current_weight": _round_float(getattr(row, "current_weight", None)),
        "desired_weight": _round_float(getattr(row, "desired_weight", None)),
        "staged_weight": _round_float(getattr(row, "staged_weight", None)),
        "latest_current_weight": _round_float(getattr(row, "latest_current_weight", None)),
        "latest_desired_weight": _round_float(getattr(row, "latest_desired_weight", None)),
        "latest_staged_weight": _round_float(getattr(row, "latest_staged_weight", None)),
        "reason": getattr(row, "reason", None),
        "resolution_reason": getattr(row, "resolution_reason", None),
        "review_count": getattr(row, "review_count", None),
        "raw_payload": getattr(row, "raw_payload", None) or {},
        "review_payload": getattr(row, "review_payload", None) or {},
        "created_at": _iso(getattr(row, "created_at", None)),
        "updated_at": _iso(getattr(row, "updated_at", None)),
        "resolved_at": _iso(getattr(row, "resolved_at", None)),
    }


def _deferred_id(command_id: str, ticker: str, side: str, delta: float) -> str:
    raw = f"{command_id}|{ticker}|{side}|{float(delta):.8f}"
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]
    return f"{command_id}_{ticker}_{side}_{digest}"[:96]


def _clean_weights(weights: dict[str, Any], *, keep_negative: bool = False) -> dict[str, float]:
    out: dict[str, float] = {}
    for ticker, value in (weights or {}).items():
        key = str(ticker or "").upper().strip()
        if not key:
            continue
        try:
            parsed = float(value or 0.0)
        except (TypeError, ValueError):
            continue
        if not keep_negative:
            parsed = max(parsed, 0.0)
        if abs(parsed) > 1e-12 or key == "CASH":
            out[key] = parsed
    return out


def _round_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return round(float(value), 6)
    except (TypeError, ValueError):
        return None


def _utcnow_db_naive() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)
