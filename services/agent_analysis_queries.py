"""Shared AgentAnalysis query helpers.

These helpers keep review-only records from being mistaken for executable
trade-decision analyses by dashboard, health, and recommendation readers.
"""
from __future__ import annotations

from datetime import date
from typing import Any

from sqlalchemy import desc, func, or_, select

from db.models import AgentAnalysis


REVIEW_ONLY_TRIGGER_TYPES = {"weekend_review"}
REVIEW_ONLY_EXECUTION_STATUSES = {"review_only"}


def is_review_only_analysis(row: Any) -> bool:
    """Return True when an AgentAnalysis row is a non-trading review artifact."""
    trigger_type = str(getattr(row, "trigger_type", "") or "").strip().lower()
    if trigger_type in REVIEW_ONLY_TRIGGER_TYPES:
        return True

    execution_status = str(getattr(row, "execution_status", "") or "").strip().lower()
    if execution_status in REVIEW_ONLY_EXECUTION_STATUSES:
        return True

    for payload_name in ("planner_output", "decision", "risk_output"):
        payload = getattr(row, payload_name, None)
        if isinstance(payload, dict) and bool(payload.get("review_only")):
            return True
        if isinstance(payload, dict) and str(payload.get("execution_authority") or "").lower() == "none":
            if bool(payload.get("target_weight_mutation") == "none" and payload.get("review_only")):
                return True
    return False


async def load_latest_trade_decision_analysis(
    db: Any,
    *,
    today: date | None = None,
    row_limit: int = 50,
) -> AgentAnalysis | None:
    """Load the latest AgentAnalysis row that represents a trading decision.

    The SQL predicate excludes the obvious review-only rows. A Python-level
    predicate then handles JSON flags without relying on dialect-specific JSON
    operators, keeping tests and local SQLite-like environments simple.
    """
    stmt = (
        select(AgentAnalysis)
        .where(
            or_(AgentAnalysis.trigger_type.is_(None), AgentAnalysis.trigger_type != "weekend_review"),
            or_(AgentAnalysis.execution_status.is_(None), AgentAnalysis.execution_status != "review_only"),
        )
        .order_by(desc(AgentAnalysis.analyzed_at), desc(AgentAnalysis.id))
        .limit(max(int(row_limit or 50), 1))
    )
    if today is not None:
        stmt = stmt.where(func.date(AgentAnalysis.analyzed_at) == today)

    rows = (await db.execute(stmt)).scalars().all()
    for row in rows:
        if not is_review_only_analysis(row):
            return row
    return None
