"""DB access helpers for execution command lifecycle records."""
from __future__ import annotations

from datetime import UTC, datetime, time, timedelta
from typing import Any

from sqlalchemy import select
from sqlalchemy import desc

from db.models import AccountStateSnapshot, CommandLifecycleEvent, ExecutionLog
from db.session import AsyncSessionLocal
from services.command_lifecycle import append_command_lifecycle_event, build_command_reconciliation_events

RECONCILIATION_ACTIVE_QC_STATUSES = {
    "accepted",
    "orders_submitted",
    "partial",
    "timeout_no_ack",
}
RECONCILIATION_TERMINAL_QC_STATUSES = {
    "rejected",
    "canceled",
    "reconciled",
    "reconciliation_drift",
    "failed_no_fill",
    "superseded",
    "timeout_no_execution_confirmed",
}


def _utcnow_db_naive() -> datetime:
    """Return UTC time in the naive form expected by DateTime columns."""
    return datetime.now(UTC).replace(tzinfo=None)


async def create_or_update_submitted_log(
    *,
    command_id: str,
    target_weights: dict[str, Any],
    analysis_id: int | None = None,
    policy_version: str | None = None,
    preflight_result: dict[str, Any] | None = None,
    policy_sync_result: dict[str, Any] | None = None,
    qc_response: dict[str, Any] | None = None,
) -> None:
    async with AsyncSessionLocal() as db:
        result = await db.execute(select(ExecutionLog).where(ExecutionLog.command_id == command_id))
        row = result.scalar_one_or_none()
        payload = {
            "action_status": "sent",
            "command_id": command_id,
            "sent_weights": target_weights,
            "policy_version": policy_version,
            "command_preflight": preflight_result or {},
            "policy_sync": policy_sync_result or {},
            "recorded_at": datetime.now(UTC).isoformat(),
        }
        if row:
            row.analysis_id = analysis_id or row.analysis_id
            row.command_type = row.command_type or "weight_adjustment"
            row.command_payload = payload
            row.qc_response = qc_response or row.qc_response
            row.status = "sent"
            row.qc_status = row.qc_status or "submitted"
        else:
            db.add(
                ExecutionLog(
                    analysis_id=analysis_id,
                    command_id=command_id,
                    command_type="weight_adjustment",
                    command_payload=payload,
                    qc_response=qc_response,
                    status="sent",
                    qc_status="submitted",
                )
            )
        await append_command_lifecycle_event(
            db,
            command_id=command_id,
            analysis_id=analysis_id,
            event_type="created",
            event_status="sent",
            source="fastapi",
            payload={
                "target_weights": target_weights,
                "policy_version": policy_version,
                "preflight_result": preflight_result or {},
            },
        )
        await append_command_lifecycle_event(
            db,
            command_id=command_id,
            analysis_id=analysis_id,
            event_type="submitted_to_qc",
            event_status="submitted",
            source="fastapi",
            payload={
                "policy_sync_result": policy_sync_result or {},
                "qc_response": qc_response or {},
            },
        )
        await db.commit()


async def create_or_update_policy_sync_log(
    *,
    command_id: str,
    analysis_id: int | None = None,
    policy_version: str | None = None,
    policy_payload: dict[str, Any] | None = None,
    qc_response: dict[str, Any] | None = None,
    status: str = "pending_send",
    qc_status: str = "pending",
) -> None:
    """Create a local row before PolicySync is sent so fast QC ACKs are not lost."""
    payload = {
        "action_status": status,
        "command_id": command_id,
        "policy_version": policy_version,
        "policy_payload": policy_payload or {},
        "qc_response": qc_response or {},
        "recorded_at": datetime.now(UTC).isoformat(),
    }
    async with AsyncSessionLocal() as db:
        result = await db.execute(select(ExecutionLog).where(ExecutionLog.command_id == command_id))
        row = result.scalar_one_or_none()
        if row:
            row.analysis_id = analysis_id or row.analysis_id
            row.command_type = row.command_type or "policy_sync"
            row.command_payload = payload
            row.qc_response = qc_response or row.qc_response
            row.status = status
            if row.qc_status not in {"accepted", "rejected"}:
                row.qc_status = qc_status
        else:
            db.add(
                ExecutionLog(
                    analysis_id=analysis_id,
                    command_id=command_id,
                    command_type="policy_sync",
                    command_payload=payload,
                    qc_response=qc_response,
                    status=status,
                    qc_status=qc_status,
                )
            )
        await append_command_lifecycle_event(
            db,
            command_id=command_id,
            analysis_id=analysis_id,
            event_type="created" if status == "pending_send" else "submitted_to_qc",
            event_status=status,
            source="fastapi",
            payload=payload,
        )
        await db.commit()


async def update_execution_result(
    *,
    command_id: str,
    analysis_id: int | None,
    audit_payload: dict[str, Any],
    qc_response: dict[str, Any] | None,
    status: str,
) -> None:
    async with AsyncSessionLocal() as db:
        result = await db.execute(select(ExecutionLog).where(ExecutionLog.command_id == command_id))
        row = result.scalar_one_or_none()
        if row:
            row.analysis_id = analysis_id or row.analysis_id
            row.command_payload = audit_payload
            row.qc_response = qc_response or row.qc_response
            row.status = status
            if status in {"failed", "rejected", "skipped"} and row.qc_ack_at is None and row.qc_status in {None, "submitted"}:
                row.qc_status = "not_sent"
        else:
            db.add(
                ExecutionLog(
                    analysis_id=analysis_id,
                    command_id=command_id,
                    command_type="weight_adjustment",
                    command_payload=audit_payload,
                    qc_response=qc_response,
                    status=status,
                    qc_status="not_sent" if status in {"failed", "rejected", "skipped"} else "submitted",
                )
            )
        event_type = "execution_result"
        if status == "rejected":
            event_type = "preflight_blocked"
        await append_command_lifecycle_event(
            db,
            command_id=command_id,
            analysis_id=analysis_id,
            event_type=event_type,
            event_status=status,
            source="fastapi",
            reason=audit_payload.get("reason") if isinstance(audit_payload, dict) else None,
            payload={
                "audit_payload": audit_payload,
                "qc_response": qc_response or {},
            },
        )
        await db.commit()


async def record_preflight_block(
    *,
    command_id: str,
    analysis_id: int | None,
    target_weights: dict[str, Any],
    preflight_result: dict[str, Any],
    policy_version: str | None,
    policy_sync_result: dict[str, Any] | None = None,
) -> None:
    payload = {
        "action_status": "rejected",
        "command_id": command_id,
        "sent_weights": {},
        "proposed_weights": target_weights,
        "policy_version": policy_version,
        "command_preflight": preflight_result,
        "policy_sync": policy_sync_result or {},
        "reason": "blocked_by_command_preflight",
        "recorded_at": datetime.now(UTC).isoformat(),
    }
    await update_execution_result(
        command_id=command_id,
        analysis_id=analysis_id,
        audit_payload=payload,
        qc_response=None,
        status="rejected",
    )


async def record_active_execution_wait(
    *,
    command_id: str,
    analysis_id: int | None,
    target_weights: dict[str, Any],
    active_execution_gate: dict[str, Any],
    policy_version: str | None,
) -> None:
    payload = {
        "action_status": "deferred_by_active_execution",
        "command_id": command_id,
        "sent_weights": {},
        "proposed_weights": target_weights,
        "policy_version": policy_version,
        "reason": "active_execution_wait",
        "active_execution_gate": active_execution_gate,
        "recorded_at": datetime.now(UTC).isoformat(),
    }
    async with AsyncSessionLocal() as db:
        result = await db.execute(select(ExecutionLog).where(ExecutionLog.command_id == command_id))
        row = result.scalar_one_or_none()
        if row:
            row.analysis_id = analysis_id or row.analysis_id
            row.command_type = row.command_type or "weight_adjustment"
            row.command_payload = payload
            row.status = "deferred_by_active_execution"
            row.qc_status = "not_sent"
            row.qc_rejection_reason = "active_execution_wait"
        else:
            row = ExecutionLog(
                analysis_id=analysis_id,
                command_id=command_id,
                command_type="weight_adjustment",
                command_payload=payload,
                qc_response=None,
                status="deferred_by_active_execution",
                qc_status="not_sent",
                qc_rejection_reason="active_execution_wait",
            )
            db.add(row)
        await append_command_lifecycle_event(
            db,
            command_id=command_id,
            analysis_id=analysis_id,
            event_type="deferred_by_active_execution",
            event_status="active_execution_wait",
            source="fastapi",
            reason="active_execution_wait",
            payload=payload,
        )
        await db.commit()


async def force_reconcile_command(
    *,
    command_id: str,
    operator: str = "telegram",
    reason: str = "operator_force_reconcile",
) -> dict[str, Any]:
    """Close a stale command lifecycle using latest account truth."""
    clean_command_id = str(command_id or "").strip()
    if not clean_command_id:
        return {"success": False, "error": "missing_command_id"}
    now = _utcnow_db_naive()
    async with AsyncSessionLocal() as db:
        row = (
            await db.execute(select(ExecutionLog).where(ExecutionLog.command_id == clean_command_id))
        ).scalar_one_or_none()
        if not row:
            return {"success": False, "error": "command_not_found", "command_id": clean_command_id}
        snapshot = (
            await db.execute(
                select(AccountStateSnapshot)
                .order_by(desc(AccountStateSnapshot.recorded_at), desc(AccountStateSnapshot.id))
                .limit(1)
            )
        ).scalar_one_or_none()
        if not snapshot:
            return {"success": False, "error": "account_snapshot_not_found", "command_id": clean_command_id}

        target_weights = getattr(snapshot, "target_weights", None) or {}
        actual_weights = getattr(snapshot, "holdings_weights", None) or {}
        drift = _weight_diff_for_force_reconcile(target_weights, actual_weights)
        terminal_status = "reconciled" if float(drift.get("max_abs_diff") or 0.0) <= 0.01 else "reconciliation_drift"
        row.qc_status = terminal_status
        payload = dict(row.command_payload or {})
        payload["force_reconciliation"] = {
            "operator": operator,
            "reason": reason,
            "terminal_status": terminal_status,
            "snapshot_id": getattr(snapshot, "id", None),
            "snapshot_recorded_at": str(getattr(snapshot, "recorded_at", "") or ""),
            "target_weights": target_weights,
            "actual_holdings_weights": actual_weights,
            **drift,
        }
        row.command_payload = payload
        await append_command_lifecycle_event(
            db,
            command_id=clean_command_id,
            analysis_id=getattr(row, "analysis_id", None),
            event_type="force_reconciled_by_operator",
            event_status=terminal_status,
            source="telegram",
            reason=reason,
            payload=payload["force_reconciliation"],
            event_time=now,
        )
        await db.commit()
    return {
        "success": True,
        "command_id": clean_command_id,
        "status": terminal_status,
        "max_abs_diff": drift.get("max_abs_diff"),
        "diff_count": len(drift.get("diffs") or []),
        "snapshot_id": payload["force_reconciliation"].get("snapshot_id"),
    }


async def record_cancel_orders_requested(
    *,
    active_command_id: str,
    cancel_command_id: str,
    operator: str = "telegram",
    qc_result: dict[str, Any] | None = None,
) -> None:
    """Record operator-requested order cancellation against the active command."""
    clean_active = str(active_command_id or "").strip()
    if not clean_active:
        return
    payload = {
        "operator": operator,
        "cancel_command_id": str(cancel_command_id or "").strip(),
        "qc_result": qc_result or {},
        "requested_at": datetime.now(UTC).isoformat(),
    }
    async with AsyncSessionLocal() as db:
        row = (
            await db.execute(select(ExecutionLog).where(ExecutionLog.command_id == clean_active))
        ).scalar_one_or_none()
        await append_command_lifecycle_event(
            db,
            command_id=clean_active,
            analysis_id=getattr(row, "analysis_id", None),
            event_type="cancel_orders_requested_by_operator",
            event_status="requested",
            source="telegram",
            reason="operator_cancel_orders",
            payload=payload,
        )
        await db.commit()


async def update_qc_status(
    command_id: str,
    qc_status: str,
    rejection_reason: str | None = None,
    qc_response: dict[str, Any] | None = None,
) -> None:
    ack_time = _utcnow_db_naive()
    values: dict[str, Any] = {
        "qc_status": qc_status,
        "qc_ack_at": ack_time,
    }
    if rejection_reason is not None:
        values["qc_rejection_reason"] = rejection_reason
    if qc_response is not None:
        values["qc_response"] = qc_response
    async with AsyncSessionLocal() as db:
        result = await db.execute(select(ExecutionLog).where(ExecutionLog.command_id == command_id))
        row = result.scalar_one_or_none()
        if row:
            row.qc_status = qc_status
            row.qc_ack_at = ack_time
            if rejection_reason is not None:
                row.qc_rejection_reason = rejection_reason
            if qc_response is not None:
                row.qc_response = qc_response
        else:
            row = ExecutionLog(
                analysis_id=_analysis_id_from_command_id(command_id),
                command_id=command_id,
                command_type=_command_type_from_id(command_id),
                command_payload={
                    "action_status": "qc_ack_without_local_row",
                    "command_id": command_id,
                    "reason": rejection_reason,
                    "qc_response": qc_response or {},
                    "recorded_at": datetime.now(UTC).isoformat(),
                },
                qc_response=qc_response,
                status="ack_received",
                qc_status=qc_status,
                qc_ack_at=ack_time,
                qc_rejection_reason=rejection_reason,
            )
            db.add(row)
        event_type = _event_type_for_qc_status(qc_status)
        await append_command_lifecycle_event(
            db,
            command_id=command_id,
            event_type=event_type,
            event_status=qc_status,
            source=_event_source_for_qc_status(qc_status),
            reason=rejection_reason,
            payload=qc_response or {},
            event_time=ack_time,
        )
        superseded = _superseded_lifecycle_payload_from_qc_response(command_id, qc_response or {})
        if superseded:
            await append_command_lifecycle_event(
                db,
                command_id=superseded["command_id"],
                analysis_id=_analysis_id_from_command_id(superseded["command_id"]),
                event_type="superseded",
                event_status="superseded",
                source="qc",
                reason=superseded["reason"],
                payload=superseded["payload"],
                event_time=ack_time,
            )
        await _append_reconciliation_events(
            db,
            command_id=command_id,
            analysis_id=getattr(row, "analysis_id", None),
            command_payload=(getattr(row, "command_payload", None) or {}),
            qc_response=qc_response or {},
            event_time=ack_time,
        )
        await db.commit()


async def get_qc_status(command_id: str) -> str | None:
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(ExecutionLog.qc_status).where(ExecutionLog.command_id == command_id)
        )
        return result.scalar_one_or_none()


def _event_type_for_qc_status(qc_status: str) -> str:
    return {
        "accepted": "qc_accepted",
        "rejected": "qc_rejected",
        "timeout_no_ack": "qc_timeout",
        "orders_submitted": "orders_submitted",
        "partial": "partial",
        "filled": "filled",
        "canceled": "canceled",
        "reconciled": "reconciled",
        "reconciliation_drift": "reconciliation_drift",
        "failed_no_fill": "failed_no_fill",
        "superseded": "superseded",
        "timeout_no_execution_confirmed": "timeout_reconciled_no_execution",
    }.get(str(qc_status or "").lower().strip(), "execution_result")


def _event_source_for_qc_status(qc_status: str) -> str:
    status = str(qc_status or "").lower().strip()
    if status in {"timeout_no_ack", "timeout_no_execution_confirmed", "deferred_by_active_execution"}:
        return "fastapi"
    return "qc"


async def get_execution_log_by_command_id(command_id: str) -> ExecutionLog | None:
    async with AsyncSessionLocal() as db:
        result = await db.execute(select(ExecutionLog).where(ExecutionLog.command_id == command_id))
        return result.scalar_one_or_none()


async def command_submission_state(command_id: str, analysis_id: int | None = None) -> dict[str, Any]:
    async with AsyncSessionLocal() as db:
        command_row = None
        if command_id:
            command_row = (
                await db.execute(select(ExecutionLog).where(ExecutionLog.command_id == command_id))
            ).scalar_one_or_none()
        analysis_row = None
        if analysis_id is not None:
            analysis_row = (
                await db.execute(
                    select(ExecutionLog)
                    .where(ExecutionLog.analysis_id == analysis_id)
                    .where(ExecutionLog.command_type == "weight_adjustment")
                    .order_by(desc(ExecutionLog.executed_at))
                    .limit(1)
                )
            ).scalar_one_or_none()
    return {
        "command_id_exists": command_row is not None,
        "command_id_status": getattr(command_row, "status", None),
        "command_id_qc_status": getattr(command_row, "qc_status", None),
        "analysis_id_submitted": analysis_row is not None,
        "analysis_command_id": getattr(analysis_row, "command_id", None),
        "analysis_status": getattr(analysis_row, "status", None),
        "analysis_qc_status": getattr(analysis_row, "qc_status", None),
    }


async def summarize_today_execution_activity(now: datetime | None = None) -> dict[str, Any]:
    now = now or _utcnow_db_naive()
    await reconcile_timeout_no_ack_commands(now=now)
    start = datetime.combine(now.date(), time.min)
    async with AsyncSessionLocal() as db:
        rows = (
            await db.execute(
                select(ExecutionLog)
                .where(ExecutionLog.command_type == "weight_adjustment")
                .where(ExecutionLog.executed_at >= start)
            )
        ).scalars().all()
    command_rows = [row for row in rows if _counts_toward_daily_command(row)]
    gross_turnover = 0.0
    for row in command_rows:
        if not _counts_toward_daily_turnover(row):
            continue
        payload = getattr(row, "command_payload", None) or {}
        preflight = payload.get("command_preflight") or {}
        metrics = preflight.get("metrics") or {}
        try:
            gross_turnover += float(metrics.get("gross_turnover") or 0.0)
        except (TypeError, ValueError):
            continue
    return {
        "command_count": len(command_rows),
        "gross_turnover": round(gross_turnover, 6),
    }


def _counts_toward_daily_command(row: Any) -> bool:
    """Count only weight commands that were actually sent or are pending/accepted."""
    if getattr(row, "command_type", None) != "weight_adjustment":
        return False
    status = str(getattr(row, "status", "") or "").lower()
    qc_status = str(getattr(row, "qc_status", "") or "").lower()
    if qc_status in {"not_sent", "rejected", "timeout_no_execution_confirmed"}:
        return False
    return status in {"sent", "accepted", "timeout_no_ack"} or qc_status in {
        "submitted",
        "accepted",
        "orders_submitted",
        "partial",
        "filled",
        "reconciled",
        "reconciliation_drift",
        "failed_no_fill",
        "timeout_no_ack",
    }


def _counts_toward_daily_turnover(row: Any) -> bool:
    if not _counts_toward_daily_command(row):
        return False
    qc_status = str(getattr(row, "qc_status", "") or "").lower()
    return qc_status in {
        "submitted",
        "accepted",
        "orders_submitted",
        "partial",
        "filled",
        "reconciled",
        "reconciliation_drift",
        "timeout_no_ack",
    }


async def mark_timeout(command_id: str) -> None:
    await update_qc_status(command_id, "timeout_no_ack", rejection_reason="no QC ack before timeout")


async def reconcile_timeout_no_ack_commands(
    *,
    now: datetime | None = None,
    grace_minutes: int = 20,
) -> int:
    """Release stale ACK timeouts only when account snapshots prove QC did not process them.

    A timeout remains pending during the grace window because the command may
    still be accepted or partially filled. After a later account snapshot, we
    can safely release it from daily command/turnover caps if QC reports no
    open orders, no active target weights, and no matching last_command_id.
    """
    now = now or _utcnow_db_naive()
    cutoff = now - timedelta(minutes=max(int(grace_minutes or 0), 1))
    reconciled = 0

    async with AsyncSessionLocal() as db:
        timeout_rows = (
            await db.execute(
                select(ExecutionLog)
                .where(ExecutionLog.command_type == "weight_adjustment")
                .where(ExecutionLog.qc_status == "timeout_no_ack")
                .where(ExecutionLog.executed_at <= cutoff)
                .order_by(ExecutionLog.executed_at)
            )
        ).scalars().all()

        for row in timeout_rows:
            snapshot = (
                await db.execute(
                    select(AccountStateSnapshot)
                    .where(AccountStateSnapshot.recorded_at > (row.qc_ack_at or row.executed_at))
                    .order_by(desc(AccountStateSnapshot.recorded_at))
                    .limit(1)
                )
            ).scalar_one_or_none()
            decision = _timeout_reconciliation_decision(row, snapshot)
            if decision.get("status") != "timeout_no_execution_confirmed":
                continue

            row.qc_status = "timeout_no_execution_confirmed"
            row.qc_rejection_reason = decision["reason"]
            payload = dict(row.command_payload or {})
            payload["timeout_reconciliation"] = decision
            row.command_payload = payload
            await append_command_lifecycle_event(
                db,
                command_id=row.command_id,
                analysis_id=row.analysis_id,
                event_type="timeout_reconciled_no_execution",
                event_status="timeout_no_execution_confirmed",
                source="fastapi",
                reason=decision["reason"],
                payload=decision,
                event_time=now,
            )
            reconciled += 1

        if reconciled:
            await db.commit()

    return reconciled


def _timeout_reconciliation_decision(row: Any, snapshot: Any | None) -> dict[str, Any]:
    command_id = str(getattr(row, "command_id", "") or "")
    if not command_id:
        return {"status": "pending", "reason": "missing_command_id"}
    if snapshot is None:
        return {"status": "pending", "reason": "no_later_account_snapshot"}

    raw = getattr(snapshot, "raw_snapshot", None) or {}
    last_command_id = str(raw.get("last_command_id") or "").strip()
    active_command_id = str(
        getattr(snapshot, "active_command_id", None)
        or raw.get("active_command_id")
        or ""
    ).strip()
    target_weights = getattr(snapshot, "target_weights", None) or raw.get("target_weights") or {}
    has_open_orders = getattr(snapshot, "has_open_orders", None)
    open_order_count = getattr(snapshot, "open_order_count", None)

    if last_command_id == command_id or active_command_id == command_id:
        return {
            "status": "pending",
            "reason": "account_snapshot_reports_command_processed",
            "snapshot_id": getattr(snapshot, "id", None),
            "snapshot_recorded_at": str(getattr(snapshot, "recorded_at", "") or ""),
            "last_command_id": last_command_id,
            "active_command_id": active_command_id,
        }
    if bool(has_open_orders) or (open_order_count is not None and int(open_order_count or 0) > 0):
        return {
            "status": "pending",
            "reason": "account_snapshot_has_open_orders",
            "snapshot_id": getattr(snapshot, "id", None),
            "snapshot_recorded_at": str(getattr(snapshot, "recorded_at", "") or ""),
            "open_order_count": open_order_count,
        }
    if target_weights:
        return {
            "status": "pending",
            "reason": "account_snapshot_has_active_target_weights",
            "snapshot_id": getattr(snapshot, "id", None),
            "snapshot_recorded_at": str(getattr(snapshot, "recorded_at", "") or ""),
            "last_command_id": last_command_id,
        }

    return {
        "status": "timeout_no_execution_confirmed",
        "reason": "later_account_snapshot_has_no_matching_command_no_targets_no_open_orders",
        "snapshot_id": getattr(snapshot, "id", None),
        "snapshot_recorded_at": str(getattr(snapshot, "recorded_at", "") or ""),
        "last_command_id": last_command_id,
        "active_command_id": active_command_id,
        "open_order_count": open_order_count,
    }


def _superseded_lifecycle_payload_from_qc_response(
    new_command_id: str,
    qc_response: dict[str, Any],
) -> dict[str, Any] | None:
    superseded_command_id = str((qc_response or {}).get("superseded_command_id") or "").strip()
    if not superseded_command_id:
        return None
    reason = str((qc_response or {}).get("reason") or "superseded_by_override").strip()
    return {
        "command_id": superseded_command_id,
        "reason": reason,
        "payload": {
            "superseded_by_command_id": new_command_id,
            "reason": reason,
            "canceled_order_count": (qc_response or {}).get("canceled_order_count"),
            "order_summary": (qc_response or {}).get("order_summary") or {},
        },
    }


def _command_type_from_id(command_id: str) -> str:
    command = str(command_id or "")
    if command.endswith("_policy"):
        return "policy_sync"
    return "weight_adjustment" if command.startswith("analysis_") else "unknown"


def _analysis_id_from_command_id(command_id: str) -> int | None:
    command = str(command_id or "")
    if command.endswith("_policy"):
        command = command[:-7]
    if not command.startswith("analysis_"):
        return None
    try:
        return int(command.split("_", 1)[1])
    except (TypeError, ValueError, IndexError):
        return None


async def append_reconciliation_from_account_snapshot(db, account_state: dict[str, Any]) -> int:
    """Append delayed reconciliation events from a QC heartbeat account state."""
    raw = account_state.get("raw_snapshot") if isinstance(account_state.get("raw_snapshot"), dict) else {}
    command_id = _account_state_command_id(account_state)
    if not command_id:
        return 0
    result = await db.execute(select(ExecutionLog).where(ExecutionLog.command_id == command_id))
    row = result.scalar_one_or_none()
    if not row:
        return 0
    qc_status = str(row.qc_status or "").lower().strip()
    if qc_status in RECONCILIATION_TERMINAL_QC_STATUSES:
        return 0
    if qc_status not in RECONCILIATION_ACTIVE_QC_STATUSES:
        return 0
    reconciliation_account_state = {
        "timestamp_utc": raw.get("timestamp_utc") or account_state.get("account_timestamp"),
        "policy_version": account_state.get("policy_version"),
        "open_order_count": account_state.get("open_order_count"),
        "has_open_orders": account_state.get("has_open_orders"),
        "holdings_weights": account_state.get("holdings_weights") or {},
        "target_weights": account_state.get("target_weights") or {},
        "last_command_id": account_state.get("last_command_id") or raw.get("last_command_id"),
        "active_command_id": account_state.get("active_command_id") or raw.get("active_command_id"),
        "active_execution_status": account_state.get("active_execution_status") or raw.get("active_execution_status"),
        "processed_command_count": account_state.get("processed_command_count") or raw.get("processed_command_count"),
    }
    qc_response = _qc_response_for_heartbeat_reconciliation(
        row.qc_response or {},
        reconciliation_account_state,
    )
    event_types = await _append_reconciliation_events(
        db,
        command_id=command_id,
        analysis_id=row.analysis_id,
        command_payload=row.command_payload or {},
        qc_response=qc_response,
        account_state=reconciliation_account_state,
        event_time=account_state.get("recorded_at"),
    )
    next_status = _qc_status_from_reconciliation_event_types(event_types, qc_status)
    if next_status != qc_status:
        row.qc_status = next_status
        if row.qc_ack_at is None and next_status in {"accepted", "orders_submitted", "partial"}:
            row.qc_ack_at = account_state.get("recorded_at") or _utcnow_db_naive()
        payload = dict(row.command_payload or {})
        payload["heartbeat_reconciliation"] = {
            "status": next_status,
            "event_types": event_types,
            "account_state": reconciliation_account_state,
            "recorded_at": str(account_state.get("recorded_at") or ""),
        }
        row.command_payload = payload
    return len(event_types)


async def _append_reconciliation_events(
    db,
    *,
    command_id: str,
    analysis_id: int | None,
    command_payload: dict[str, Any],
    qc_response: dict[str, Any],
    account_state: dict[str, Any] | None = None,
    event_time: datetime | None = None,
) -> list[str]:
    events = build_command_reconciliation_events(
        command_id=command_id,
        analysis_id=analysis_id,
        command_payload=command_payload,
        qc_response=qc_response,
        account_state=account_state,
        event_time=event_time,
    )
    if not events:
        return []

    existing = (
        await db.execute(
            select(CommandLifecycleEvent.event_type)
            .where(CommandLifecycleEvent.command_id == command_id)
            .where(CommandLifecycleEvent.event_type.in_((
                "orders_submitted",
                "filled",
                "partial",
                "reconciled",
                "reconciliation_drift",
                "failed_no_fill",
                "superseded",
                "timeout_reconciled_no_execution",
            )))
        )
    ).scalars().all()
    existing_types = set(existing)
    if existing_types & {"reconciled", "reconciliation_drift", "failed_no_fill", "superseded", "timeout_reconciled_no_execution"}:
        return []

    appended: list[str] = []
    for event in events:
        event_type = event["event_type"]
        if event_type in existing_types and event_type in {
            "orders_submitted",
            "filled",
            "partial",
            "reconciliation_drift",
            "failed_no_fill",
        }:
            continue
        await append_command_lifecycle_event(
            db,
            command_id=event["command_id"],
            analysis_id=event.get("analysis_id"),
            event_type=event_type,
            event_status=event.get("event_status"),
            source=event.get("source") or "fastapi",
            reason=event.get("reason"),
            payload=event.get("payload") or {},
            event_time=event.get("event_time"),
        )
        appended.append(event_type)
    return appended


def _account_state_command_id(account_state: dict[str, Any]) -> str:
    raw = account_state.get("raw_snapshot") if isinstance(account_state.get("raw_snapshot"), dict) else {}
    for value in (
        account_state.get("active_command_id"),
        raw.get("active_command_id"),
        account_state.get("last_command_id"),
        raw.get("last_command_id"),
    ):
        command_id = str(value or "").strip()
        if command_id:
            return command_id
    return ""


def _qc_response_for_heartbeat_reconciliation(
    qc_response: dict[str, Any],
    account_state: dict[str, Any],
) -> dict[str, Any]:
    response = dict(qc_response or {})
    response["status"] = "accepted"
    response.setdefault("execution_state", account_state.get("active_execution_status") or "orders_submitted")
    response.setdefault("actual_target_weights", account_state.get("target_weights") or {})
    response.setdefault("actual_holdings_weights", account_state.get("holdings_weights") or {})
    response.setdefault("account_state", account_state)
    open_order_count = account_state.get("open_order_count")
    has_open_orders = bool(account_state.get("has_open_orders")) if account_state.get("has_open_orders") is not None else False
    order_summary = dict(response.get("order_summary") or {})
    if open_order_count is not None:
        order_summary["open_order_count_after"] = open_order_count
    order_summary["has_open_orders"] = has_open_orders
    response["order_summary"] = order_summary
    return response


def _qc_status_from_reconciliation_event_types(
    event_types: list[str],
    current_status: str | None,
) -> str:
    normalized = [str(event or "").lower().strip() for event in event_types or []]
    for terminal in ("reconciled", "reconciliation_drift", "failed_no_fill"):
        if terminal in normalized:
            return terminal
    if "filled" in normalized:
        return "filled"
    if "partial" in normalized:
        return "partial"
    if "orders_submitted" in normalized:
        return "orders_submitted"
    return str(current_status or "").lower().strip()


def _weight_diff_for_force_reconcile(
    target_weights: dict[str, Any],
    actual_weights: dict[str, Any],
) -> dict[str, Any]:
    target = _clean_weight_dict_for_force_reconcile(target_weights)
    actual = _clean_weight_dict_for_force_reconcile(actual_weights)
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
    diffs.sort(key=lambda row: (-abs(float(row.get("diff") or 0.0)), str(row.get("ticker") or "")))
    return {"max_abs_diff": round(max_abs, 6), "diffs": diffs}


def _clean_weight_dict_for_force_reconcile(weights: dict[str, Any] | None) -> dict[str, float]:
    out: dict[str, float] = {}
    if not isinstance(weights, dict):
        return out
    for ticker, raw in weights.items():
        key = str(ticker or "").upper().strip()
        if not key:
            continue
        try:
            out[key] = max(float(raw or 0.0), 0.0)
        except (TypeError, ValueError):
            out[key] = 0.0
    return out
