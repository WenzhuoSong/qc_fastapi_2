"""Operator-first Telegram message formatters.

These helpers keep trading decisions out of the copy layer. They only turn
already-computed state into concise operator-facing summaries.
"""
from __future__ import annotations

from typing import Any


def command_label(command_id: str | None) -> str:
    return str(command_id or "unknown")


def format_circuit_state_change_message(
    *,
    state: str,
    reason: str,
    primary_trigger: str | None = None,
) -> str:
    state_text = str(state or "unknown").upper()
    emoji = {"CLOSED": "🟢", "ALERT": "🟡", "DEFENSIVE": "🔴"}.get(state_text, "⚪")
    paused = state_text != "CLOSED"
    recommended = (
        "wait for the condition to resolve, then use /reset_circuit"
        if paused
        else "no action needed"
    )
    override = "DEFENSIVE" if paused else "none"
    return "\n".join(
        [
            f"{emoji} Circuit {state_text}",
            f"Status: {'pipeline paused' if paused else 'pipeline active'}",
            f"Trigger: {primary_trigger or 'n/a'}",
            f"Reason: {reason or 'n/a'}",
            f"Override mode: {override}",
            f"Recommended: {recommended}",
        ]
    )


def format_market_closed_stale_info(account_guard: dict[str, Any]) -> str:
    snapshot = account_guard.get("snapshot") if isinstance(account_guard.get("snapshot"), dict) else {}
    freshness = account_guard.get("freshness") if isinstance(account_guard.get("freshness"), dict) else {}
    market = freshness.get("market_status") if isinstance(freshness.get("market_status"), dict) else {}
    recorded_at = snapshot.get("account_timestamp") or snapshot.get("recorded_at") or "unknown"
    age = snapshot.get("age_seconds")
    age_text = f"{float(age) / 60:.1f}m" if isinstance(age, (int, float)) else "unknown"
    return "\n".join(
        [
            "ℹ️ Account snapshot stale because market is closed",
            "Status: no action needed",
            f"Latest snapshot: {recorded_at}",
            f"Snapshot age: {age_text}",
            f"Market: {market.get('phase') or 'closed'}",
            "Recommended: wait for the next market heartbeat",
        ]
    )


def format_reconciliation_guard_alert_message(verdict: dict[str, Any]) -> str:
    status = str((verdict or {}).get("status") or "unknown")
    reason = str((verdict or {}).get("reason") or "unknown")
    command = (verdict or {}).get("command") if isinstance((verdict or {}).get("command"), dict) else {}
    lines = [
        f"⛔ Reconciliation guard: {status}",
        "Status: new command blocked",
        f"Reason: {reason}",
    ]
    if command.get("command_id"):
        lines.append(
            f"Command: {command.get('command_id')} | state={command.get('lifecycle_state') or 'unknown'}"
        )
    if status == "diverged":
        lines.append(f"Max drift: {float((verdict or {}).get('max_drift') or 0.0):.2%}")
        drift = (verdict or {}).get("drift_tickers") or []
        if drift:
            lines.append("Changed tickers:")
        for row in drift[:5]:
            lines.append(
                f"- {row.get('ticker')}: expected={float(row.get('expected') or 0.0):.2%} "
                f"actual={float(row.get('actual') or 0.0):.2%} "
                f"diff={float(row.get('diff') or 0.0):+.2%}"
            )
        if len(drift) > 5:
            lines.append(f"... +{len(drift) - 5} more")
        lines.append("Next action: inspect account truth and reconcile before trading.")
    elif status == "stuck_in_flight":
        lines.append(
            f"Elapsed: {float(command.get('age_seconds') or 0.0):.0f}s | "
            f"threshold={command.get('timeout_threshold_seconds')}s"
        )
        lines.append("Next action: check QC order state; force reconcile or cancel if stuck.")
    else:
        lines.append("Next action: wait for reliable account truth.")
    lines.append("No command sent to QC.")
    return "\n".join(lines)


def format_qc_lifecycle_ack_message(command_id: str, qc_ack: dict[str, Any]) -> str:
    status = str((qc_ack or {}).get("qc_status") or "").lower().strip()
    response = (qc_ack or {}).get("qc_response") if isinstance((qc_ack or {}).get("qc_response"), dict) else {}
    order_summary = response.get("order_summary") if isinstance(response.get("order_summary"), dict) else {}
    label = command_label(command_id)
    execution_state = str(response.get("execution_state") or "").lower().strip()
    lifecycle_context = _format_lifecycle_context(qc_ack)
    if execution_state == "noop_reconciled" or order_summary.get("is_noop") is True:
        return "\n".join(
            [
                f"✅ No-op reconciled `{label}`",
                "Status: no orders needed",
                "Execution truth: target already matches current holdings",
                f"SetHoldings actions: {order_summary.get('action_count', 0)}",
                f"Actual orders: {order_summary.get('actual_order_count', 0)}",
                "Next action: wait for normal account heartbeat.",
            ]
        ) + lifecycle_context
    if status == "accepted":
        return "\n".join(
            [
                f"✅ QC accepted ownership `{label}`",
                "Status: execution in progress",
                f"Execution truth: {response.get('execution_state') or 'accepted'}",
                "Next action: wait for fill/account reconciliation.",
            ]
        ) + lifecycle_context
    if status == "orders_submitted":
        return "\n".join(
            [
                f"📤 QC submitted orders `{label}`",
                "Status: awaiting fills",
                f"Actual orders: {order_summary.get('actual_order_count', order_summary.get('submitted_order_count', 'n/a'))}",
                "Next action: wait for heartbeat reconciliation.",
            ]
        ) + lifecycle_context
    if status == "partial":
        return "\n".join(
            [
                f"⏳ Partial execution `{label}`",
                "Status: command still in flight",
                f"Filled: {order_summary.get('filled_order_count', 'n/a')}/"
                f"{order_summary.get('actual_order_count', order_summary.get('submitted_order_count', 'n/a'))}",
                f"Open orders: {order_summary.get('open_order_count_after', order_summary.get('open_order_count', 'n/a'))}",
                "Next action: wait; do not submit overlapping target unless manually overriding.",
            ]
        ) + lifecycle_context
    if status == "filled":
        return (
            f"✅ QC reports fills `{label}`\n"
            "Status: account reconciliation pending\n"
            "Next action: wait for account snapshot confirmation."
            f"{lifecycle_context}"
        )
    if status == "reconciled":
        return (
            f"✅ Reconciled `{label}`\n"
            "Status: actual holdings match target within tolerance\n"
            "Next action: no action needed."
            f"{lifecycle_context}"
        )
    if status == "reconciliation_drift":
        return (
            f"⚠️ Reconciliation drift `{label}`\n"
            "Status: account truth differs from target\n"
            "Next action: inspect account and reconciliation guard before the next command."
            f"{lifecycle_context}"
        )
    if status == "failed_no_fill":
        return (
            f"⚠️ QC accepted `{label}` but reports no fill\n"
            "Status: positions should be verified from account truth\n"
            "Next action: check QC order logs."
            f"{lifecycle_context}"
        )
    if status == "superseded":
        return (
            f"ℹ️ Command `{label}` superseded\n"
            "Status: later command or override took precedence\n"
            "Next action: inspect lifecycle if this was not expected."
            f"{lifecycle_context}"
        )
    return (
        f"⚠️ QC ACK timeout `{label}`\n"
        "Status: ownership not confirmed inside wait window\n"
        "Execution truth: unknown until heartbeat reconciliation\n"
        "Next action: do not assume positions changed."
        f"{lifecycle_context}"
    )


def _format_lifecycle_context(qc_ack: dict[str, Any]) -> str:
    state = str((qc_ack or {}).get("lifecycle_state") or "").strip()
    trust = (qc_ack or {}).get("feedback_trust") if isinstance((qc_ack or {}).get("feedback_trust"), dict) else {}
    trust_status = str((trust or {}).get("status") or "").strip()
    if not state and not trust_status:
        return ""
    parts = []
    if state:
        parts.append(f"state={state}")
    if trust_status:
        parts.append(f"feedback={trust_status}")
    return "\nLifecycle: " + " | ".join(parts)
