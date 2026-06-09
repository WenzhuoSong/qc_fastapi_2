"""DB access helpers for execution command lifecycle records."""
from __future__ import annotations

from datetime import UTC, datetime, time, timedelta
from typing import Any

from sqlalchemy import select
from sqlalchemy import desc

from db.models import AccountStateSnapshot, AgentAnalysis, CommandLifecycleEvent, ExecutionLog
from db.session import AsyncSessionLocal
from services.command_lifecycle import (
    append_command_lifecycle_event,
    build_command_reconciliation_events,
    lifecycle_state_from_status,
    next_lifecycle_state,
)
from services.execution_lifecycle import classify_qc_feedback_trust
from services.json_safety import json_safe
from services.target_fingerprint import build_target_fingerprint

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
_ANALYSIS_EXECUTED_AT_SKIP_STATUSES = {"", "pending", "proposed", "running"}
_NOT_SENT_DB_STATUSES = {"failed", "rejected", "skipped", "deferred", "deduped"}


def _execution_log_db_status(status: str | None) -> str:
    """Return a short status that fits execution_log.status varchar(20).

    Detailed business reasons belong in command_payload.reason/action_status and
    lifecycle events, not in the narrow DB status column.
    """
    clean = str(status or "unknown").lower().strip()
    if len(clean) <= 20:
        return clean
    if clean.startswith("skipped"):
        return "skipped"
    if clean.startswith("deferred"):
        return "deferred"
    if clean.startswith("rejected"):
        return "rejected"
    if clean.startswith("failed"):
        return "failed"
    if clean.startswith("timeout"):
        return "timeout"
    return "failed"


def _event_status_for_db(status: str | None) -> str:
    clean = str(status or "unknown").lower().strip()
    if len(clean) <= 40:
        return clean
    return _execution_log_db_status(clean)


def _utcnow_db_naive() -> datetime:
    """Return UTC time in the naive form expected by DateTime columns."""
    return datetime.now(UTC).replace(tzinfo=None)


def _safe_json_payload(value: Any) -> Any:
    """Normalize Python-only containers before JSONB persistence."""
    return json_safe(value)


def _apply_command_lifecycle_skeleton(
    row: Any,
    *,
    command_id: str,
    analysis_id: int | None = None,
    command_type: str | None = None,
    policy_version: str | None = None,
    status: str | None = None,
    qc_status: str | None = None,
    qc_response: dict[str, Any] | None = None,
    submitted_at: datetime | None = None,
    latest_qc_ack_at: datetime | None = None,
    metadata: dict[str, Any] | None = None,
) -> None:
    """Populate the shared command lifecycle row fields on execution_log."""
    clean_command_id = str(command_id or getattr(row, "command_id", "") or "").strip()
    if clean_command_id:
        row.command_id = clean_command_id
        row.correlation_id = getattr(row, "correlation_id", None) or clean_command_id
    if analysis_id is not None:
        row.analysis_id = analysis_id
        row.source_analysis_id = getattr(row, "source_analysis_id", None) or analysis_id
    elif getattr(row, "analysis_id", None) is not None:
        row.source_analysis_id = getattr(row, "source_analysis_id", None) or getattr(row, "analysis_id", None)
    if command_type:
        row.command_type = command_type
    if policy_version:
        row.policy_version = policy_version
    if submitted_at is not None:
        row.submitted_at = submitted_at
    elif getattr(row, "submitted_at", None) is None:
        row.submitted_at = getattr(row, "executed_at", None) or _utcnow_db_naive()
    if latest_qc_ack_at is not None:
        row.latest_qc_ack_at = latest_qc_ack_at
    lifecycle_metadata = dict(getattr(row, "lifecycle_metadata", None) or {})
    if metadata:
        lifecycle_metadata.update(_safe_json_payload(metadata))
    if lifecycle_metadata:
        row.lifecycle_metadata = lifecycle_metadata
    proposed_lifecycle_state = lifecycle_state_from_status(
        status=status if status is not None else getattr(row, "status", None),
        qc_status=qc_status if qc_status is not None else getattr(row, "qc_status", None),
        qc_response=qc_response if qc_response is not None else getattr(row, "qc_response", None),
    )
    row.lifecycle_state = next_lifecycle_state(
        getattr(row, "lifecycle_state", None),
        proposed_lifecycle_state,
    )


def _store_feedback_trust(row: Any, feedback_trust: dict[str, Any]) -> None:
    """Store PR2a trust classification on the shared command lifecycle row."""
    lifecycle_metadata = dict(getattr(row, "lifecycle_metadata", None) or {})
    lifecycle_metadata["feedback_trust"] = _safe_json_payload(feedback_trust)
    row.lifecycle_metadata = lifecycle_metadata
    lifecycle_hint = str((feedback_trust or {}).get("lifecycle_state_hint") or "").strip()
    if lifecycle_hint:
        row.lifecycle_state = next_lifecycle_state(getattr(row, "lifecycle_state", None), lifecycle_hint)


def _set_target_fingerprint(row: Any, fingerprint_payload: dict[str, Any] | None) -> None:
    fingerprint = str((fingerprint_payload or {}).get("fingerprint") or "").strip()
    if fingerprint and not str(getattr(row, "target_fingerprint", "") or "").strip():
        row.target_fingerprint = fingerprint


def _analysis_execution_status_from_row(row: Any) -> str:
    """Derive the user-facing AgentAnalysis execution status from lifecycle truth."""
    qc_status = str(getattr(row, "qc_status", "") or "").lower().strip()
    lifecycle_state = str(getattr(row, "lifecycle_state", "") or "").lower().strip()
    status = str(getattr(row, "status", "") or "").lower().strip()
    payload = getattr(row, "command_payload", None)
    if isinstance(payload, dict) and qc_status == "not_sent":
        action_status = str(payload.get("action_status") or "").lower().strip()
        reason = str(payload.get("reason") or "").lower().strip()
        if action_status == "deferred_by_active_execution":
            return action_status
        if status == "deferred" and reason == "active_execution_wait":
            return "deferred_by_active_execution"
        if status == "skipped" and reason == "broker_order_filter_no_executable_delta":
            return "skipped_broker_order_filter"

    if qc_status in {
        "reconciled",
        "reconciliation_drift",
        "failed_no_fill",
        "timeout_no_execution_confirmed",
        "rejected",
        "canceled",
        "superseded",
    }:
        return qc_status
    if lifecycle_state in {
        "filled",
        "noop_reconciled",
        "partial",
        "orders_submitted",
        "accepted",
        "deduped",
        "deferred_by_active_execution",
        "rejected",
    }:
        return lifecycle_state
    if status and status != "sent":
        return status
    return qc_status or status or "unknown"


async def _sync_agent_analysis_execution_state(db: Any, row: Any) -> None:
    """Keep AgentAnalysis execution fields aligned with the lifecycle row.

    ``execution_log`` is the command lifecycle source of truth.  AgentAnalysis is
    an operator-facing decision row, so stale ``pending`` values there make daily
    review and validation observations misleading.
    """
    analysis_id = getattr(row, "analysis_id", None)
    if analysis_id is None:
        return
    analysis = await db.get(AgentAnalysis, analysis_id)
    if analysis is None:
        return
    existing_status = str(getattr(analysis, "execution_status", "") or "").lower().strip()
    if existing_status == "review_only":
        return
    next_status = _analysis_execution_status_from_row(row)
    if not next_status:
        return
    analysis.execution_status = next_status
    if next_status not in _ANALYSIS_EXECUTED_AT_SKIP_STATUSES:
        analysis.executed_at = (
            getattr(row, "latest_qc_ack_at", None)
            or getattr(row, "executed_at", None)
            or _utcnow_db_naive()
        )


def _target_fingerprint_tolerance_from_preflight(preflight_result: dict[str, Any] | None) -> float | None:
    config = (preflight_result or {}).get("config") if isinstance(preflight_result, dict) else None
    if not isinstance(config, dict):
        return None
    try:
        return float(config.get("recent_same_target_dedupe_tolerance"))
    except (TypeError, ValueError):
        return None


def _build_setweights_target_fingerprint(
    target_weights: dict[str, Any] | None,
    *,
    policy_version: str | None,
    command_id: str | None = None,
    analysis_id: int | None = None,
    correlation_id: str | None = None,
    tolerance: float | None = None,
) -> dict[str, Any] | None:
    if not isinstance(target_weights, dict) or not target_weights:
        return None
    return build_target_fingerprint(
        target_weights,
        command_type="SetWeights",
        policy_version=policy_version,
        tolerance=tolerance,
        metadata={
            "command_id": command_id,
            "analysis_id": analysis_id,
            "correlation_id": correlation_id,
        },
    )


def _target_fingerprint_from_command_payload(
    payload: dict[str, Any] | None,
    *,
    command_id: str | None = None,
    analysis_id: int | None = None,
    row: Any | None = None,
) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    existing = payload.get("target_fingerprint")
    if isinstance(existing, dict) and existing.get("fingerprint"):
        return existing
    target_weights = None
    for key in ("sent_weights", "proposed_weights", "weights"):
        value = payload.get(key)
        if isinstance(value, dict) and value:
            target_weights = value
            break
    if not target_weights:
        return None
    policy_version = payload.get("policy_version")
    if not policy_version and row is not None:
        policy_version = getattr(row, "policy_version", None)
    return _build_setweights_target_fingerprint(
        target_weights,
        policy_version=policy_version,
        command_id=command_id or payload.get("command_id") or getattr(row, "command_id", None),
        analysis_id=analysis_id if analysis_id is not None else getattr(row, "analysis_id", None),
        correlation_id=getattr(row, "correlation_id", None),
    )


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
    recorded_at = _utcnow_db_naive()
    target_fingerprint = _build_setweights_target_fingerprint(
        target_weights,
        policy_version=policy_version,
        command_id=command_id,
        analysis_id=analysis_id,
        tolerance=_target_fingerprint_tolerance_from_preflight(preflight_result),
    )
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
            "target_fingerprint": target_fingerprint or {},
            "recorded_at": recorded_at.isoformat(),
        }
        payload = _safe_json_payload(payload)
        safe_qc_response = _safe_json_payload(qc_response) if qc_response is not None else None
        if row:
            row.analysis_id = analysis_id or row.analysis_id
            row.command_type = row.command_type or "weight_adjustment"
            row.command_payload = payload
            row.qc_response = safe_qc_response or row.qc_response
            row.status = "sent"
            row.qc_status = row.qc_status or "submitted"
            _set_target_fingerprint(row, target_fingerprint)
            _apply_command_lifecycle_skeleton(
                row,
                command_id=command_id,
                analysis_id=analysis_id,
                command_type="weight_adjustment",
                policy_version=policy_version,
                status=row.status,
                qc_status=row.qc_status,
                qc_response=row.qc_response,
                submitted_at=recorded_at,
                metadata={"source": "create_or_update_submitted_log"},
            )
        else:
            row = ExecutionLog(
                analysis_id=analysis_id,
                command_id=command_id,
                command_type="weight_adjustment",
                command_payload=payload,
                qc_response=safe_qc_response,
                status="sent",
                qc_status="submitted",
            )
            _set_target_fingerprint(row, target_fingerprint)
            _apply_command_lifecycle_skeleton(
                row,
                command_id=command_id,
                analysis_id=analysis_id,
                command_type="weight_adjustment",
                policy_version=policy_version,
                status="sent",
                qc_status="submitted",
                qc_response=safe_qc_response,
                submitted_at=recorded_at,
                metadata={"source": "create_or_update_submitted_log"},
            )
            db.add(row)
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
                "target_fingerprint": target_fingerprint or {},
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
                "qc_response": safe_qc_response or {},
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
    recorded_at = _utcnow_db_naive()
    payload = {
        "action_status": status,
        "command_id": command_id,
        "policy_version": policy_version,
        "policy_payload": policy_payload or {},
        "qc_response": qc_response or {},
        "recorded_at": recorded_at.isoformat(),
    }
    payload = _safe_json_payload(payload)
    safe_qc_response = _safe_json_payload(qc_response) if qc_response is not None else None
    async with AsyncSessionLocal() as db:
        result = await db.execute(select(ExecutionLog).where(ExecutionLog.command_id == command_id))
        row = result.scalar_one_or_none()
        if row:
            row.analysis_id = analysis_id or row.analysis_id
            row.command_type = row.command_type or "policy_sync"
            row.command_payload = payload
            row.qc_response = safe_qc_response or row.qc_response
            row.status = status
            if row.qc_status not in {"accepted", "rejected"}:
                row.qc_status = qc_status
            _apply_command_lifecycle_skeleton(
                row,
                command_id=command_id,
                analysis_id=analysis_id,
                command_type="policy_sync",
                policy_version=policy_version,
                status=row.status,
                qc_status=row.qc_status,
                qc_response=row.qc_response,
                submitted_at=recorded_at,
                metadata={"source": "create_or_update_policy_sync_log"},
            )
        else:
            row = ExecutionLog(
                analysis_id=analysis_id,
                command_id=command_id,
                command_type="policy_sync",
                command_payload=payload,
                qc_response=safe_qc_response,
                status=status,
                qc_status=qc_status,
            )
            _apply_command_lifecycle_skeleton(
                row,
                command_id=command_id,
                analysis_id=analysis_id,
                command_type="policy_sync",
                policy_version=policy_version,
                status=status,
                qc_status=qc_status,
                qc_response=safe_qc_response,
                submitted_at=recorded_at,
                metadata={"source": "create_or_update_policy_sync_log"},
            )
            db.add(row)
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
    raw_status = str(status or "unknown").lower().strip()
    db_status = _execution_log_db_status(raw_status)
    if isinstance(audit_payload, dict):
        audit_payload = dict(audit_payload)
        if raw_status != db_status:
            audit_payload.setdefault("raw_execution_status", raw_status)
            audit_payload.setdefault("execution_status_db", db_status)
        fingerprint_payload = _target_fingerprint_from_command_payload(
            audit_payload,
            command_id=command_id,
            analysis_id=analysis_id,
        )
        if fingerprint_payload:
            audit_payload["target_fingerprint"] = fingerprint_payload
    else:
        fingerprint_payload = None
    audit_payload = _safe_json_payload(audit_payload)
    qc_response = _safe_json_payload(qc_response) if qc_response is not None else None
    policy_version = audit_payload.get("policy_version") if isinstance(audit_payload, dict) else None
    async with AsyncSessionLocal() as db:
        result = await db.execute(select(ExecutionLog).where(ExecutionLog.command_id == command_id))
        row = result.scalar_one_or_none()
        if row:
            row.analysis_id = analysis_id or row.analysis_id
            row.command_payload = audit_payload
            row.qc_response = qc_response or row.qc_response
            row.status = db_status
            if db_status in _NOT_SENT_DB_STATUSES and row.qc_ack_at is None and row.qc_status in {None, "submitted"}:
                row.qc_status = "not_sent"
            _set_target_fingerprint(row, fingerprint_payload)
            _apply_command_lifecycle_skeleton(
                row,
                command_id=command_id,
                analysis_id=analysis_id,
                command_type=row.command_type or "weight_adjustment",
                policy_version=policy_version,
                status=row.status,
                qc_status=row.qc_status,
                qc_response=row.qc_response,
                metadata={"source": "update_execution_result"},
            )
        else:
            qc_status = "not_sent" if db_status in _NOT_SENT_DB_STATUSES else "submitted"
            row = ExecutionLog(
                analysis_id=analysis_id,
                command_id=command_id,
                command_type="weight_adjustment",
                command_payload=audit_payload,
                qc_response=qc_response,
                status=db_status,
                qc_status=qc_status,
            )
            _set_target_fingerprint(row, fingerprint_payload)
            _apply_command_lifecycle_skeleton(
                row,
                command_id=command_id,
                analysis_id=analysis_id,
                command_type="weight_adjustment",
                policy_version=policy_version,
                status=db_status,
                qc_status=qc_status,
                qc_response=qc_response,
                metadata={"source": "update_execution_result"},
            )
            db.add(row)
        event_type = "execution_result"
        if db_status == "rejected":
            event_type = "preflight_blocked"
        await append_command_lifecycle_event(
            db,
            command_id=command_id,
            analysis_id=analysis_id,
            event_type=event_type,
            event_status=_event_status_for_db(raw_status),
            source="fastapi",
            reason=audit_payload.get("reason") if isinstance(audit_payload, dict) else None,
            payload={
                "audit_payload": audit_payload,
                "qc_response": qc_response or {},
            },
        )
        await _sync_agent_analysis_execution_state(db, row)
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


async def record_recent_same_target_dedupe(
    *,
    command_id: str,
    analysis_id: int | None,
    target_weights: dict[str, Any],
    dedupe_result: dict[str, Any],
    policy_version: str | None,
    preflight_result: dict[str, Any] | None = None,
) -> None:
    """Record a same-target duplicate as not_sent without consuming daily caps."""
    target_fingerprint = _build_setweights_target_fingerprint(
        target_weights,
        policy_version=policy_version,
        command_id=command_id,
        analysis_id=analysis_id,
        tolerance=_target_fingerprint_tolerance_from_preflight(preflight_result),
    )
    payload = {
        "action_status": "deduped",
        "command_id": command_id,
        "sent_weights": {},
        "proposed_weights": target_weights,
        "policy_version": policy_version,
        "command_preflight": preflight_result or {},
        "reason": "recent_same_target_reconciled",
        "same_target_dedupe": dedupe_result,
        "target_fingerprint": target_fingerprint or {},
        "recorded_at": datetime.now(UTC).isoformat(),
    }
    payload = _safe_json_payload(payload)
    async with AsyncSessionLocal() as db:
        result = await db.execute(select(ExecutionLog).where(ExecutionLog.command_id == command_id))
        row = result.scalar_one_or_none()
        if row:
            row.analysis_id = analysis_id or row.analysis_id
            row.command_type = row.command_type or "weight_adjustment"
            row.command_payload = payload
            row.status = "deduped"
            row.qc_status = "not_sent"
            row.qc_rejection_reason = "recent_same_target_reconciled"
            _set_target_fingerprint(row, target_fingerprint)
            _apply_command_lifecycle_skeleton(
                row,
                command_id=command_id,
                analysis_id=analysis_id,
                command_type="weight_adjustment",
                policy_version=policy_version,
                status=row.status,
                qc_status=row.qc_status,
                qc_response=row.qc_response,
                metadata={"source": "record_recent_same_target_dedupe"},
            )
        else:
            row = ExecutionLog(
                analysis_id=analysis_id,
                command_id=command_id,
                command_type="weight_adjustment",
                command_payload=payload,
                qc_response=None,
                status="deduped",
                qc_status="not_sent",
                qc_rejection_reason="recent_same_target_reconciled",
            )
            _set_target_fingerprint(row, target_fingerprint)
            _apply_command_lifecycle_skeleton(
                row,
                command_id=command_id,
                analysis_id=analysis_id,
                command_type="weight_adjustment",
                policy_version=policy_version,
                status="deduped",
                qc_status="not_sent",
                qc_response=None,
                metadata={"source": "record_recent_same_target_dedupe"},
            )
            db.add(row)
        await append_command_lifecycle_event(
            db,
            command_id=command_id,
            analysis_id=analysis_id,
            event_type="execution_result",
            event_status="deduped",
            source="fastapi",
            reason="recent_same_target_reconciled",
            payload=payload,
        )
        await _sync_agent_analysis_execution_state(db, row)
        await db.commit()


async def record_active_execution_wait(
    *,
    command_id: str,
    analysis_id: int | None,
    target_weights: dict[str, Any],
    active_execution_gate: dict[str, Any],
    policy_version: str | None,
) -> None:
    target_fingerprint = _build_setweights_target_fingerprint(
        target_weights,
        policy_version=policy_version,
        command_id=command_id,
        analysis_id=analysis_id,
    )
    payload = {
        "action_status": "deferred_by_active_execution",
        "command_id": command_id,
        "sent_weights": {},
        "proposed_weights": target_weights,
        "policy_version": policy_version,
        "reason": "active_execution_wait",
        "active_execution_gate": active_execution_gate,
        "target_fingerprint": target_fingerprint or {},
        "recorded_at": datetime.now(UTC).isoformat(),
    }
    payload = _safe_json_payload(payload)
    async with AsyncSessionLocal() as db:
        result = await db.execute(select(ExecutionLog).where(ExecutionLog.command_id == command_id))
        row = result.scalar_one_or_none()
        if row:
            row.analysis_id = analysis_id or row.analysis_id
            row.command_type = row.command_type or "weight_adjustment"
            row.command_payload = payload
            row.status = "deferred"
            row.qc_status = "not_sent"
            row.qc_rejection_reason = "active_execution_wait"
            _set_target_fingerprint(row, target_fingerprint)
            _apply_command_lifecycle_skeleton(
                row,
                command_id=command_id,
                analysis_id=analysis_id,
                command_type="weight_adjustment",
                policy_version=policy_version,
                status=row.status,
                qc_status=row.qc_status,
                qc_response=row.qc_response,
                metadata={"source": "record_active_execution_wait"},
            )
        else:
            row = ExecutionLog(
                analysis_id=analysis_id,
                command_id=command_id,
                command_type="weight_adjustment",
                command_payload=payload,
                qc_response=None,
                status="deferred",
                qc_status="not_sent",
                qc_rejection_reason="active_execution_wait",
            )
            _set_target_fingerprint(row, target_fingerprint)
            _apply_command_lifecycle_skeleton(
                row,
                command_id=command_id,
                analysis_id=analysis_id,
                command_type="weight_adjustment",
                policy_version=policy_version,
                status="deferred",
                qc_status="not_sent",
                qc_response=None,
                metadata={"source": "record_active_execution_wait"},
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
        await _sync_agent_analysis_execution_state(db, row)
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
        row.qc_ack_at = row.qc_ack_at or now
        row.latest_qc_ack_at = now
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
        payload = _safe_json_payload(payload)
        row.command_payload = payload
        _apply_command_lifecycle_skeleton(
            row,
            command_id=clean_command_id,
            analysis_id=getattr(row, "analysis_id", None),
            command_type=getattr(row, "command_type", None) or _command_type_from_id(clean_command_id),
            policy_version=getattr(row, "policy_version", None),
            status=getattr(row, "status", None),
            qc_status=terminal_status,
            qc_response=getattr(row, "qc_response", None),
            latest_qc_ack_at=now,
            metadata={"source": "force_reconcile_command"},
        )
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
        await _sync_agent_analysis_execution_state(db, row)
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
) -> dict[str, Any]:
    ack_time = _utcnow_db_naive()
    safe_qc_response = _safe_json_payload(qc_response) if qc_response is not None else None
    response_policy_version = (
        safe_qc_response.get("policy_version")
        if isinstance(safe_qc_response, dict)
        else None
    )
    async with AsyncSessionLocal() as db:
        result = await db.execute(select(ExecutionLog).where(ExecutionLog.command_id == command_id))
        row = result.scalar_one_or_none()
        command_known = row is not None
        feedback_trust = classify_qc_feedback_trust(
            qc_response=safe_qc_response or {"status": qc_status},
            command_known=command_known,
        )
        if row:
            row.qc_status = qc_status
            row.qc_ack_at = ack_time
            row.latest_qc_ack_at = ack_time
            if rejection_reason is not None:
                row.qc_rejection_reason = rejection_reason
            if safe_qc_response is not None:
                row.qc_response = safe_qc_response
            _apply_command_lifecycle_skeleton(
                row,
                command_id=command_id,
                analysis_id=getattr(row, "analysis_id", None),
                command_type=getattr(row, "command_type", None) or _command_type_from_id(command_id),
                policy_version=getattr(row, "policy_version", None) or response_policy_version,
                status=getattr(row, "status", None),
                qc_status=qc_status,
                qc_response=row.qc_response,
                latest_qc_ack_at=ack_time,
                metadata={"source": "update_qc_status"},
            )
            _store_feedback_trust(row, feedback_trust)
        else:
            command_payload = _safe_json_payload(
                {
                    "action_status": "qc_ack_without_local_row",
                    "command_id": command_id,
                    "reason": rejection_reason,
                    "qc_response": safe_qc_response or {},
                    "feedback_trust": feedback_trust,
                    "recorded_at": datetime.now(UTC).isoformat(),
                }
            )
            row = ExecutionLog(
                analysis_id=_analysis_id_from_command_id(command_id),
                command_id=command_id,
                command_type=_command_type_from_id(command_id),
                command_payload=command_payload,
                qc_response=safe_qc_response,
                status="ack_received",
                qc_status=qc_status,
                qc_ack_at=ack_time,
                qc_rejection_reason=rejection_reason,
            )
            _apply_command_lifecycle_skeleton(
                row,
                command_id=command_id,
                analysis_id=_analysis_id_from_command_id(command_id),
                command_type=_command_type_from_id(command_id),
                policy_version=response_policy_version,
                status="ack_received",
                qc_status=qc_status,
                qc_response=safe_qc_response,
                latest_qc_ack_at=ack_time,
                metadata={"source": "update_qc_status_without_local_row"},
            )
            _store_feedback_trust(row, feedback_trust)
            db.add(row)
        event_type = "unknown_command_feedback" if not command_known else _event_type_for_qc_status(qc_status)
        await append_command_lifecycle_event(
            db,
            command_id=command_id,
            event_type=event_type,
            event_status=qc_status,
            source=_event_source_for_qc_status(qc_status),
            reason=rejection_reason or feedback_trust.get("reason"),
            payload={
                **(safe_qc_response or {}),
                "feedback_trust": feedback_trust,
            },
            event_time=ack_time,
        )
        superseded = _superseded_lifecycle_payload_from_qc_response(command_id, safe_qc_response or {})
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
        event_types: list[str] = []
        if _feedback_trust_allows_reconciliation_event_derivation(feedback_trust):
            event_types = await _append_reconciliation_events(
                db,
                command_id=command_id,
                analysis_id=getattr(row, "analysis_id", None),
                command_payload=(getattr(row, "command_payload", None) or {}),
                qc_response=safe_qc_response or {},
                event_time=ack_time,
            )
            all_event_types = event_types or await _read_reconciliation_event_types(db, command_id)
            if all_event_types:
                _sync_execution_log_from_reconciliation_events(
                    row,
                    command_id=command_id,
                    event_types=all_event_types,
                    event_time=ack_time,
                    source="update_qc_status",
                )
        await _sync_agent_analysis_execution_state(db, row)
        await db.commit()
        return {
            "command_id": command_id,
            "known_command": command_known,
            "feedback_trust": feedback_trust,
            "lifecycle_state": getattr(row, "lifecycle_state", None),
            "reconciliation_event_types": event_types,
        }


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


def _feedback_trust_allows_reconciliation_event_derivation(feedback_trust: dict[str, Any] | None) -> bool:
    """Only derive hard reconciliation events from trusted or explicitly in-flight ACKs."""
    status = str((feedback_trust or {}).get("status") or "").strip()
    if status in {
        "trusted_for_reconciliation",
        "trusted_noop_reconciled",
        "trusted_terminal_no_fill",
        "partial",
    }:
        return True
    return False


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
    return summarize_execution_activity_rows(rows)


def summarize_execution_activity_rows(rows: list[Any]) -> dict[str, Any]:
    """Summarize execution rows using the same daily-cap counting rules."""
    command_rows = [row for row in rows if _counts_toward_daily_command(row)]
    gross_turnover = 0.0
    risk_reduce_command_count = 0
    risk_reduce_gross_turnover = 0.0
    for row in command_rows:
        if not _counts_toward_daily_turnover(row):
            continue
        metrics = _command_preflight_metrics(row)
        try:
            row_turnover = float(metrics.get("gross_turnover") or 0.0)
        except (TypeError, ValueError):
            continue
        gross_turnover += row_turnover
        if _metrics_are_risk_reduce(metrics):
            risk_reduce_command_count += 1
            risk_reduce_gross_turnover += row_turnover
    return {
        "command_count": len(command_rows),
        "gross_turnover": round(gross_turnover, 6),
        "risk_reduce_command_count": risk_reduce_command_count,
        "risk_reduce_gross_turnover": round(risk_reduce_gross_turnover, 6),
        "ordinary_command_count": len(command_rows) - risk_reduce_command_count,
        "ordinary_gross_turnover": round(gross_turnover - risk_reduce_gross_turnover, 6),
    }


def _command_preflight_metrics(row: Any) -> dict[str, Any]:
    payload = getattr(row, "command_payload", None) or {}
    preflight = payload.get("command_preflight") or {}
    return preflight.get("metrics") or {}


def _metrics_are_risk_reduce(metrics: dict[str, Any]) -> bool:
    try:
        buy_delta = float(metrics.get("buy_delta") or 0.0)
        sell_delta = float(metrics.get("sell_delta") or 0.0)
    except (TypeError, ValueError):
        return False
    return buy_delta <= 1e-12 and sell_delta > 1e-12


def _counts_toward_daily_command(row: Any) -> bool:
    """Count only weight commands that were actually sent or are pending/accepted."""
    if getattr(row, "command_type", None) != "weight_adjustment":
        return False
    if _is_noop_execution(row):
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


def _is_noop_execution(row: Any) -> bool:
    response = getattr(row, "qc_response", None) or {}
    if not isinstance(response, dict):
        return False
    summary = response.get("order_summary") if isinstance(response.get("order_summary"), dict) else {}
    execution_state = str(response.get("execution_state") or summary.get("execution_state") or "").lower().strip()
    if execution_state == "noop_reconciled" or summary.get("is_noop") is True:
        return True
    try:
        actual_order_count = int(summary.get("actual_order_count"))
    except (TypeError, ValueError):
        actual_order_count = None
    return actual_order_count == 0 and execution_state == "noop_reconciled"


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
            snapshots = (
                await db.execute(
                    select(AccountStateSnapshot)
                    .where(AccountStateSnapshot.recorded_at > (row.qc_ack_at or row.executed_at))
                    .order_by(AccountStateSnapshot.recorded_at, AccountStateSnapshot.id)
                )
            ).scalars().all()
            decision = _timeout_reconciliation_decision_from_snapshots(row, snapshots)
            if decision.get("status") != "timeout_no_execution_confirmed":
                continue

            row.qc_status = "timeout_no_execution_confirmed"
            row.qc_rejection_reason = decision["reason"]
            row.latest_qc_ack_at = now
            payload = dict(row.command_payload or {})
            payload["timeout_reconciliation"] = decision
            payload = _safe_json_payload(payload)
            row.command_payload = payload
            _apply_command_lifecycle_skeleton(
                row,
                command_id=row.command_id,
                analysis_id=getattr(row, "analysis_id", None),
                command_type=getattr(row, "command_type", None) or _command_type_from_id(row.command_id),
                policy_version=getattr(row, "policy_version", None),
                status=getattr(row, "status", None),
                qc_status=row.qc_status,
                qc_response=getattr(row, "qc_response", None),
                latest_qc_ack_at=now,
                metadata={"source": "reconcile_timeout_no_ack_commands"},
            )
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
            await _sync_agent_analysis_execution_state(db, row)
            reconciled += 1

        if reconciled:
            await db.commit()

    return reconciled


def _timeout_reconciliation_decision_from_snapshots(
    row: Any,
    snapshots: list[Any],
) -> dict[str, Any]:
    """Return the first conclusive no-execution snapshot for a timeout row."""
    if not snapshots:
        return _timeout_reconciliation_decision(row, None)

    last_pending: dict[str, Any] | None = None
    for snapshot in snapshots:
        decision = _timeout_reconciliation_decision(row, snapshot)
        if decision.get("status") == "timeout_no_execution_confirmed":
            return decision
        last_pending = decision
        if decision.get("reason") == "account_snapshot_reports_command_processed":
            return decision
    return last_pending or {"status": "pending", "reason": "no_later_account_snapshot"}


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
        event_types = await _read_reconciliation_event_types(db, command_id)
        if event_types:
            _sync_execution_log_from_reconciliation_events(
                row,
                command_id=command_id,
                event_types=event_types,
                event_time=account_state.get("recorded_at") or _utcnow_db_naive(),
                source="append_reconciliation_from_account_snapshot_terminal_resync",
            )
            await _sync_agent_analysis_execution_state(db, row)
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
    all_event_types = event_types or await _read_reconciliation_event_types(db, command_id)
    if all_event_types:
        _sync_execution_log_from_reconciliation_events(
            row,
            command_id=command_id,
            event_types=all_event_types,
            event_time=account_state.get("recorded_at") or _utcnow_db_naive(),
            source="append_reconciliation_from_account_snapshot",
            account_state=reconciliation_account_state,
        )
        await _sync_agent_analysis_execution_state(db, row)
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
            .where(CommandLifecycleEvent.event_type.in_(_RECONCILIATION_EVENT_TYPES_FOR_STATUS()))
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


async def _read_reconciliation_event_types(db, command_id: str) -> list[str]:
    rows = (
        await db.execute(
            select(CommandLifecycleEvent.event_type)
            .where(CommandLifecycleEvent.command_id == command_id)
            .where(CommandLifecycleEvent.event_type.in_(_RECONCILIATION_EVENT_TYPES_FOR_STATUS()))
        )
    ).scalars().all()
    return [str(row or "").lower().strip() for row in rows if str(row or "").strip()]


def _sync_execution_log_from_reconciliation_events(
    row: Any,
    *,
    command_id: str,
    event_types: list[str],
    event_time: datetime | None,
    source: str,
    account_state: dict[str, Any] | None = None,
) -> bool:
    current_status = str(getattr(row, "qc_status", "") or "").lower().strip()
    next_status = _qc_status_from_reconciliation_event_types(event_types, current_status)
    lifecycle_state = str(getattr(row, "lifecycle_state", "") or "").lower().strip()
    if not next_status or (next_status == current_status and lifecycle_state not in {"", "created"}):
        return False

    ack_time = event_time or _utcnow_db_naive()
    row.qc_status = next_status
    if getattr(row, "qc_ack_at", None) is None and next_status in {"accepted", "orders_submitted", "partial"}:
        row.qc_ack_at = ack_time
    row.latest_qc_ack_at = ack_time
    payload = dict(getattr(row, "command_payload", None) or {})
    payload["reconciliation_row_cache_sync"] = {
        "status": next_status,
        "event_types": [str(event or "") for event in event_types or []],
        "source": source,
        "account_state": account_state or {},
        "recorded_at": str(ack_time or ""),
    }
    row.command_payload = _safe_json_payload(payload)
    _apply_command_lifecycle_skeleton(
        row,
        command_id=command_id,
        analysis_id=getattr(row, "analysis_id", None),
        command_type=getattr(row, "command_type", None) or _command_type_from_id(command_id),
        policy_version=getattr(row, "policy_version", None),
        status=getattr(row, "status", None),
        qc_status=next_status,
        qc_response=getattr(row, "qc_response", None),
        latest_qc_ack_at=ack_time,
        metadata={"source": source},
    )
    return True


def _RECONCILIATION_EVENT_TYPES_FOR_STATUS() -> tuple[str, ...]:
    return (
        "orders_submitted",
        "filled",
        "partial",
        "reconciled",
        "reconciliation_drift",
        "failed_no_fill",
        "superseded",
        "timeout_reconciled_no_execution",
    )


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
    if "timeout_reconciled_no_execution" in normalized:
        return "timeout_no_execution_confirmed"
    if "superseded" in normalized:
        return "superseded"
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
