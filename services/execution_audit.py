"""Execution audit helpers."""
from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any


ACTION_STATUSES = {"proposed", "sent", "accepted", "rejected", "filled", "failed", "skipped"}


def build_execution_audit_payload(
    *,
    action_status: str,
    proposed_weights: dict[str, Any] | None = None,
    sent_weights: dict[str, Any] | None = None,
    command_id: str | None = None,
    rebalance_actions: list[dict[str, Any]] | None = None,
    estimated_cost_pct: float | None = None,
    reason: str | None = None,
) -> dict[str, Any]:
    status = action_status if action_status in ACTION_STATUSES else "failed"
    return {
        "action_status": status,
        "proposed_weights": proposed_weights or {},
        "sent_weights": sent_weights or {},
        "command_id": command_id,
        "rebalance_actions": rebalance_actions or [],
        "estimated_cost_pct": estimated_cost_pct,
        "reason": reason,
        "recorded_at": datetime.now(UTC).isoformat(),
    }


def count_execution_actions_from_payload(payload: dict[str, Any] | None) -> int:
    """Count actual non-cash order-level actions when available."""
    payload = payload or {}
    actions = payload.get("rebalance_actions") or []
    if actions:
        return sum(1 for action in actions if str(action.get("ticker") or "").upper() != "CASH")

    weights = payload.get("sent_weights") or payload.get("weights_sent") or {}
    return sum(1 for ticker, weight in weights.items() if str(ticker).upper() != "CASH" and float(weight or 0) > 0)


async def count_today_actual_execution_actions(target_date: date | None = None) -> int:
    """Count sent/accepted/filled execution actions from today's execution_log rows."""
    from sqlalchemy import func, select

    from db.models import ExecutionLog
    from db.session import AsyncSessionLocal

    day = target_date or date.today()
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(ExecutionLog.command_payload)
            .where(func.date(ExecutionLog.executed_at) == day)
            .where(ExecutionLog.status.in_(("sent", "accepted", "filled", "success")))
        )
        rows = result.scalars().all()

    return sum(count_execution_actions_from_payload(row) for row in rows)
