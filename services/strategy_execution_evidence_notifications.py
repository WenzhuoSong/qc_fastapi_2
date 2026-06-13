"""Operator notifications for strategy execution-evidence certification.

This module is observability-only. It reads frozen decision-funnel diagnostic
artifacts and records notification state; it never changes strategy evidence,
target weights, or execution authority.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Awaitable, Callable


STATE_KEY = "strategy_execution_evidence_notification_state"
STATE_SCHEMA_VERSION = "strategy_execution_evidence_notification_state_v1"


Notifier = Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]


def extract_strategy_execution_evidence_rows(
    diagnostic_artifacts: list[dict[str, Any]] | None,
) -> list[dict[str, Any]]:
    """Extract frozen strategy execution-evidence rows from decision artifacts."""
    rows: list[dict[str, Any]] = []
    for artifact in diagnostic_artifacts or []:
        if not isinstance(artifact, dict):
            continue
        if artifact.get("schema_version") != "decision_funnel_observability_v1":
            continue
        flags = artifact.get("data_quality_flags")
        if not isinstance(flags, dict):
            continue
        summary = flags.get("strategy_execution_evidence")
        if not isinstance(summary, dict):
            continue
        for row in summary.get("rows") or []:
            if isinstance(row, dict) and row.get("strategy_name"):
                rows.append(dict(row))
    return rows


def extract_buy_intent_tickers(
    diagnostic_artifacts: list[dict[str, Any]] | None,
    *,
    limit: int = 12,
) -> list[str]:
    tickers: list[str] = []
    for artifact in diagnostic_artifacts or []:
        if not isinstance(artifact, dict):
            continue
        if artifact.get("schema_version") != "decision_funnel_observability_v1":
            continue
        for row in artifact.get("buy_intents") or []:
            if not isinstance(row, dict):
                continue
            ticker = str(row.get("ticker") or "").upper().strip()
            if ticker and ticker not in tickers:
                tickers.append(ticker)
            if len(tickers) >= limit:
                return tickers
    return tickers


def prepare_strategy_execution_evidence_notification(
    *,
    analysis_id: int,
    diagnostic_artifacts: list[dict[str, Any]] | None,
    previous_state: dict[str, Any] | None,
    now: datetime | None = None,
) -> tuple[str | None, dict[str, Any]]:
    """Return a Telegram message for newly validated strategies and next state."""
    timestamp = (now or datetime.utcnow()).isoformat()
    state = dict(previous_state or {})
    latest_status_by_strategy = dict(state.get("latest_status_by_strategy") or {})
    notified_validated_by_strategy = dict(state.get("notified_validated_by_strategy") or {})

    rows = extract_strategy_execution_evidence_rows(diagnostic_artifacts)
    newly_validated: list[dict[str, Any]] = []
    for row in rows:
        strategy = str(row.get("strategy_name") or "").strip()
        if not strategy:
            continue
        status = str(row.get("execution_evidence_status") or "").strip()
        already_notified = strategy in notified_validated_by_strategy
        if status == "execution_grade_validated" and not already_notified:
            newly_validated.append(row)
        latest_status_by_strategy[strategy] = status

    next_state = {
        "schema_version": STATE_SCHEMA_VERSION,
        "last_seen_analysis_id": analysis_id,
        "last_seen_at": timestamp,
        "latest_status_by_strategy": latest_status_by_strategy,
        "notified_validated_by_strategy": notified_validated_by_strategy,
    }
    if not newly_validated:
        return None, next_state

    buy_intent_tickers = extract_buy_intent_tickers(diagnostic_artifacts)
    lines = [
        "🧪 Strategy execution evidence certified",
        f"Analysis: {analysis_id}",
        "This is observability-only; sizing limits and execution gates are unchanged.",
        (
            "Current buy-intent tickers: "
            + (", ".join(buy_intent_tickers) if buy_intent_tickers else "none")
        ),
        "",
    ]
    for row in newly_validated:
        strategy = str(row.get("strategy_name") or "")
        failed = [str(item) for item in (row.get("failed_checks") or [])]
        checks = row.get("evidence_checks") if isinstance(row.get("evidence_checks"), dict) else {}
        check_rows = checks.get("checks") if isinstance(checks.get("checks"), dict) else {}
        passed_count = sum(1 for item in check_rows.values() if bool((item or {}).get("pass")))
        total_count = len(check_rows)
        suggested = str(row.get("suggested_use") or "unknown")
        approved = str(row.get("approved_use") or "unknown")
        certification_status = str(row.get("certification_status") or "unknown")
        lines.extend([
            f"- {strategy}",
            f"  status: {certification_status} / execution_grade_validated",
            f"  use: suggested={suggested} approved={approved}",
            f"  evidence_checks: {passed_count}/{total_count} passed" if total_count else "  evidence_checks: not included",
            f"  failed_checks: {', '.join(failed) if failed else 'none'}",
        ])
        notified_validated_by_strategy[strategy] = {
            "first_notified_at": timestamp,
            "analysis_id": analysis_id,
            "execution_evidence_status": "execution_grade_validated",
        }

    lines.extend([
        "",
        "First-certification checklist:",
        "1. Inspect frozen evidence checks.",
        "2. Watch the first small add through QC ACK and reconciliation.",
        "3. Confirm the next outcome label lands on schedule.",
    ])
    next_state["notified_validated_by_strategy"] = notified_validated_by_strategy
    return "\n".join(lines), next_state


async def notify_strategy_execution_evidence_certification(
    *,
    db,
    analysis_id: int,
    diagnostic_artifacts: list[dict[str, Any]] | None,
    notifier: Notifier,
) -> dict[str, Any]:
    """Send one Telegram notification when a strategy newly becomes execution-grade."""
    from db.queries import get_system_config, upsert_system_config

    row = await get_system_config(db, STATE_KEY)
    previous_state = row.value if row and isinstance(row.value, dict) else {}
    message, next_state = prepare_strategy_execution_evidence_notification(
        analysis_id=analysis_id,
        diagnostic_artifacts=diagnostic_artifacts,
        previous_state=previous_state,
    )
    if not message:
        await upsert_system_config(db, STATE_KEY, next_state, "strategy_execution_evidence_notifications")
        return {"sent": False, "reason": "no_new_execution_grade_strategy"}

    result = await notifier({"text": message, "parse_mode": ""})
    if bool(result.get("sent")):
        await upsert_system_config(db, STATE_KEY, next_state, "strategy_execution_evidence_notifications")
        return {
            "sent": True,
            "strategy_count": len(next_state.get("notified_validated_by_strategy") or {}),
        }
    return {"sent": False, "reason": "telegram_send_failed", "error": result.get("error")}
