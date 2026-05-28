"""Manual/diagnostic recovery for FastAPI/QC execution policy drift.

Runtime trading should rely on the QC compiled policy matching FastAPI via
deployment/CI. PolicySync remains available as an explicit diagnostic tool, but
automatic recovery is disabled by default and must never allow SetWeights in
the same pipeline cycle.
"""
from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Awaitable, Callable


POLICY_SYNC_RECOVERY_CONFIG_KEY = "policy_sync_recovery_config"
POLICY_SYNC_RECOVERY_STATE_KEY = "policy_sync_recovery_state"

DEFAULT_POLICY_SYNC_RECOVERY_CONFIG: dict[str, Any] = {
    "enabled": False,
    "max_recovery_attempts": 3,
    "max_consecutive_mismatch_cycles": 5,
    "fire_and_forget": True,
    "expected_policy_version_source": "execution_policy",
    "sync_protocol_version": "v2_payload_json",
}

RECOVERABLE_BLOCKER = "policy_version_mismatch"
RECOVERY_PENDING_STATUSES = {"pending_confirmation", "send_in_progress"}

PolicySyncSender = Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]


def default_policy_sync_recovery_config(config: dict[str, Any] | None = None) -> dict[str, Any]:
    """Return normalized JSON-safe policy sync recovery config."""
    merged = dict(DEFAULT_POLICY_SYNC_RECOVERY_CONFIG)
    merged.update(config or {})
    merged["enabled"] = bool(merged.get("enabled", True))
    merged["max_recovery_attempts"] = max(_int_or_default(merged.get("max_recovery_attempts"), 3), 1)
    merged["max_consecutive_mismatch_cycles"] = max(
        _int_or_default(merged.get("max_consecutive_mismatch_cycles"), 5),
        1,
    )
    merged["fire_and_forget"] = bool(merged.get("fire_and_forget", True))
    source = str(merged.get("expected_policy_version_source") or "execution_policy").strip()
    merged["expected_policy_version_source"] = source or "execution_policy"
    protocol = str(merged.get("sync_protocol_version") or "v2_payload_json").strip()
    merged["sync_protocol_version"] = protocol or "v2_payload_json"
    return merged


def evaluate_policy_sync_recovery(
    *,
    account_guard_result: dict[str, Any] | None,
    recovery_state: dict[str, Any] | None,
    execution_policy_version: str,
    config: dict[str, Any] | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Classify policy drift into pass, recoverable, or unrecoverable."""
    cfg = default_policy_sync_recovery_config(config)
    now = _strip_tz(now or _utcnow())
    guard = account_guard_result or {}
    state = dict(recovery_state or {})
    expected = str(execution_policy_version or "").strip()
    observed = _observed_policy_version(guard)

    if not cfg["enabled"]:
        return _decision(
            status="disabled",
            action="none",
            reason="policy_sync_recovery_disabled",
            expected=expected,
            observed=observed,
            state=state,
            config=cfg,
            now=now,
        )

    if not expected:
        return _decision(
            status="unrecoverable",
            action="none",
            reason="missing_expected_policy_version",
            expected=expected,
            observed=observed,
            state=_with_status(state, "unrecoverable", now, "missing_expected_policy_version"),
            config=cfg,
            now=now,
        )

    if observed == expected and not bool(guard.get("would_block")):
        return _decision(
            status="recovered" if state.get("status") else "pass",
            action="mark_recovered" if state.get("status") not in {None, "", "recovered"} else "none",
            reason="policy_version_confirmed",
            expected=expected,
            observed=observed,
            state=_recovered_state(state, expected, observed, now),
            config=cfg,
            now=now,
        )

    blockers = [str(item) for item in (guard.get("blockers") or [])]
    if set(blockers) != {RECOVERABLE_BLOCKER}:
        reason = "non_recoverable_account_guard_blockers" if blockers else "no_recoverable_policy_mismatch"
        status = "unrecoverable" if bool(guard.get("would_block")) else "pass"
        next_state = state if status == "pass" else _with_status(state, "unrecoverable", now, reason)
        return _decision(
            status=status,
            action="none",
            reason=reason,
            expected=expected,
            observed=observed,
            state=next_state,
            config=cfg,
            now=now,
            blockers=blockers,
        )

    if not _account_safe_for_policy_recovery(guard):
        reason = "account_state_not_safe_for_policy_sync_recovery"
        return _decision(
            status="unrecoverable",
            action="none",
            reason=reason,
            expected=expected,
            observed=observed,
            state=_with_status(state, "unrecoverable", now, reason),
            config=cfg,
            now=now,
            blockers=blockers,
        )

    if _last_sync_rejected(state, cfg):
        reason = "policy_sync_rejected"
        return _decision(
            status="unrecoverable",
            action="none",
            reason=reason,
            expected=expected,
            observed=observed,
            state=_with_status(state, "unrecoverable", now, reason),
            config=cfg,
            now=now,
            blockers=blockers,
        )

    prior_attempts = _int_or_default(state.get("attempt_count"), 0)
    prior_cycles = _int_or_default(state.get("consecutive_mismatch_cycles"), 0)
    next_cycles = prior_cycles + 1
    if prior_attempts >= int(cfg["max_recovery_attempts"]):
        reason = "max_recovery_attempts_exhausted"
        return _decision(
            status="unrecoverable",
            action="none",
            reason=reason,
            expected=expected,
            observed=observed,
            state=_with_status(_cycle_state(state, expected, observed, next_cycles, now), "unrecoverable", now, reason),
            config=cfg,
            now=now,
            blockers=blockers,
        )
    if next_cycles >= int(cfg["max_consecutive_mismatch_cycles"]):
        reason = "max_consecutive_mismatch_cycles_exhausted"
        return _decision(
            status="unrecoverable",
            action="none",
            reason=reason,
            expected=expected,
            observed=observed,
            state=_with_status(_cycle_state(state, expected, observed, next_cycles, now), "unrecoverable", now, reason),
            config=cfg,
            now=now,
            blockers=blockers,
        )

    if str(state.get("status") or "").strip() in RECOVERY_PENDING_STATUSES:
        return _decision(
            status="recoverable",
            action="waiting_for_confirmation",
            reason="waiting_for_policy_version_confirmation",
            expected=expected,
            observed=observed,
            state=_cycle_state(state, expected, observed, next_cycles, now),
            config=cfg,
            now=now,
            blockers=blockers,
        )

    return _decision(
        status="recoverable",
        action="send_sync",
        reason="policy_version_mismatch_recoverable",
        expected=expected,
        observed=observed,
        state=_cycle_state(
            {
                **state,
                "attempt_count": prior_attempts + 1,
                "first_detected_at": state.get("first_detected_at") or now.isoformat(),
            },
            expected,
            observed,
            next_cycles,
            now,
        ),
        config=cfg,
        now=now,
        blockers=blockers,
    )


async def run_policy_sync_recovery(
    *,
    account_guard_result: dict[str, Any],
    config: dict[str, Any] | None = None,
    sender: PolicySyncSender | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Evaluate and, when explicitly enabled, send PolicySync without waiting for QC ACK."""
    from services.execution_policy import policy_snapshot

    policy = policy_snapshot()
    expected = str(policy.get("version") or "").strip()
    now = _strip_tz(now or _utcnow())

    async with _locked_state_session() as locked:
        state = await _hydrate_state_with_last_qc_status(await locked.get_state())
        decision = evaluate_policy_sync_recovery(
            account_guard_result=account_guard_result,
            recovery_state=state,
            execution_policy_version=expected,
            config=config,
            now=now,
        )
        if decision["action"] == "send_sync":
            command_id = _recovery_command_id(now, int(decision["next_state"].get("attempt_count") or 1))
            in_progress = {
                **decision["next_state"],
                "status": "send_in_progress",
                "last_sync_command_id": command_id,
                "last_attempted_at": now.isoformat(),
                "last_result": "pending_send",
                "last_sync_protocol_version": decision["config"].get("sync_protocol_version"),
            }
            await locked.set_state(in_progress)
            decision["next_state"] = in_progress
            decision["sync_command_id"] = command_id
        elif decision["action"] in {"waiting_for_confirmation", "mark_recovered"} or decision["status"] == "unrecoverable":
            await locked.set_state(decision["next_state"])

    if decision["action"] != "send_sync":
        return decision

    command_id = decision["sync_command_id"]
    sender = sender or _default_policy_sync_sender
    await _record_policy_sync_pending(command_id, policy)
    try:
        result = await sender({"command_id": command_id, "payload": policy})
    except Exception as exc:  # pragma: no cover - covered via injected sender behavior
        result = {"success": False, "error": _short_error(exc)}
    await _record_policy_sync_sent(command_id, policy, result)

    finished_at = _utcnow()
    final_state = {
        **decision["next_state"],
        "status": "pending_confirmation" if result.get("success") else "send_failed",
        "last_result": "sent" if result.get("success") else "send_failed",
        "last_error": None if result.get("success") else str(result.get("error") or "policy_sync_send_failed"),
        "last_response": result.get("response") or {},
        "last_attempt_finished_at": finished_at.isoformat(),
        "last_sync_protocol_version": decision["config"].get("sync_protocol_version"),
    }
    await store_policy_sync_recovery_state(final_state)
    return {
        **decision,
        "action": "sync_sent" if result.get("success") else "sync_send_failed",
        "status": "recoverable",
        "sync_command_id": command_id,
        "send_result": result,
        "next_state": final_state,
    }


async def load_policy_sync_recovery_config() -> dict[str, Any]:
    from db.queries import get_system_config
    from db.session import AsyncSessionLocal

    async with AsyncSessionLocal() as db:
        row = await get_system_config(db, POLICY_SYNC_RECOVERY_CONFIG_KEY)
    return default_policy_sync_recovery_config((row.value if row else {}) or {})


async def load_policy_sync_recovery_state() -> dict[str, Any]:
    from db.queries import get_system_config
    from db.session import AsyncSessionLocal

    async with AsyncSessionLocal() as db:
        row = await get_system_config(db, POLICY_SYNC_RECOVERY_STATE_KEY)
    return (row.value if row else {}) or {}


async def store_policy_sync_recovery_state(state: dict[str, Any]) -> None:
    async with _locked_state_session() as locked:
        await locked.set_state(state or {})


def policy_sync_recovery_suppresses_auto_pause(recovery: dict[str, Any] | None) -> bool:
    """Return true when a recoverable policy mismatch is already being handled."""
    if not isinstance(recovery, dict):
        return False
    if recovery.get("status") != "recoverable":
        return False
    return recovery.get("action") in {
        "send_sync",
        "sync_sent",
        "sync_send_failed",
        "waiting_for_confirmation",
    }


async def _default_policy_sync_sender(payload: dict[str, Any]) -> dict[str, Any]:
    from tools.qc_tools import tool_send_policy_sync

    return await tool_send_policy_sync(payload)


async def _record_policy_sync_pending(command_id: str, policy: dict[str, Any]) -> None:
    from services.execution_log_store import create_or_update_policy_sync_log

    await create_or_update_policy_sync_log(
        command_id=command_id,
        analysis_id=None,
        policy_version=policy.get("version"),
        policy_payload=policy,
        status="pending_send",
        qc_status="pending",
    )


async def _record_policy_sync_sent(command_id: str, policy: dict[str, Any], result: dict[str, Any]) -> None:
    from services.execution_log_store import create_or_update_policy_sync_log

    await create_or_update_policy_sync_log(
        command_id=command_id,
        analysis_id=None,
        policy_version=policy.get("version"),
        policy_payload=policy,
        qc_response=result.get("response") or result,
        status="sent" if result.get("success") else "failed",
        qc_status="submitted" if result.get("success") else "not_sent",
    )


async def _hydrate_state_with_last_qc_status(state: dict[str, Any]) -> dict[str, Any]:
    command_id = str((state or {}).get("last_sync_command_id") or "").strip()
    if not command_id:
        return state or {}
    try:
        from services.execution_log_store import get_execution_log_by_command_id

        row = await get_execution_log_by_command_id(command_id)
    except Exception:
        return state or {}
    if not row:
        return state or {}
    out = dict(state or {})
    if getattr(row, "qc_status", None):
        out["last_qc_status"] = row.qc_status
    if getattr(row, "qc_rejection_reason", None):
        out["last_qc_rejection_reason"] = row.qc_rejection_reason
    return out


class _LockedState:
    def __init__(self, session: Any, row: Any):
        self._session = session
        self._row = row

    async def get_state(self) -> dict[str, Any]:
        return (getattr(self._row, "value", None) or {}) if self._row else {}

    async def set_state(self, state: dict[str, Any]) -> None:
        from db.models import SystemConfig

        if self._row:
            self._row.value = state or {}
            self._row.updated_by = "policy_sync_recovery"
            self._row.updated_at = _utcnow()
        else:
            self._row = SystemConfig(
                key=POLICY_SYNC_RECOVERY_STATE_KEY,
                value=state or {},
                updated_by="policy_sync_recovery",
            )
            self._session.add(self._row)
        await self._session.commit()


class _LockedStateSession:
    def __init__(self):
        self._session_cm = None
        self._session = None

    async def __aenter__(self) -> _LockedState:
        from sqlalchemy import select

        from db.models import SystemConfig
        from db.session import AsyncSessionLocal

        self._session_cm = AsyncSessionLocal()
        self._session = await self._session_cm.__aenter__()
        result = await self._session.execute(
            select(SystemConfig)
            .where(SystemConfig.key == POLICY_SYNC_RECOVERY_STATE_KEY)
            .with_for_update()
        )
        row = result.scalar_one_or_none()
        return _LockedState(self._session, row)

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._session_cm:
            await self._session_cm.__aexit__(exc_type, exc, tb)


def _locked_state_session() -> _LockedStateSession:
    return _LockedStateSession()


def _account_safe_for_policy_recovery(guard: dict[str, Any]) -> bool:
    checks = guard.get("checks") if isinstance(guard.get("checks"), dict) else {}
    required_checks = (
        "snapshot_fresh",
        "explicit_account_state",
        "account_status_ok",
        "data_status_ok",
        "no_open_orders",
        "buying_power_present",
        "holdings_weights_present",
        "holdings_match_snapshot_rows",
    )
    return all(bool((checks.get(name) or {}).get("pass")) for name in required_checks)


def _observed_policy_version(guard: dict[str, Any]) -> str:
    checks = guard.get("checks") if isinstance(guard.get("checks"), dict) else {}
    policy_match = checks.get("policy_version_matches_expected") or {}
    if policy_match.get("actual"):
        return str(policy_match.get("actual") or "").strip()
    policy_present = checks.get("policy_version_present") or {}
    if policy_present.get("actual"):
        return str(policy_present.get("actual") or "").strip()
    snapshot = guard.get("snapshot") if isinstance(guard.get("snapshot"), dict) else {}
    return str(snapshot.get("policy_version") or "").strip()


def _last_sync_rejected(state: dict[str, Any], config: dict[str, Any]) -> bool:
    current_protocol = str((config or {}).get("sync_protocol_version") or "").strip()
    state_protocol = str((state or {}).get("last_sync_protocol_version") or "").strip()
    if current_protocol and state_protocol != current_protocol:
        return False
    return str((state or {}).get("last_qc_status") or "").lower().strip() == "rejected"


def _cycle_state(
    state: dict[str, Any],
    expected: str,
    observed: str,
    cycles: int,
    now: datetime,
) -> dict[str, Any]:
    return {
        **(state or {}),
        "status": state.get("status") or "recoverable",
        "expected_policy_version": expected,
        "observed_policy_version": observed,
        "consecutive_mismatch_cycles": cycles,
        "last_detected_at": now.isoformat(),
    }


def _with_status(state: dict[str, Any], status: str, now: datetime, reason: str) -> dict[str, Any]:
    return {
        **(state or {}),
        "status": status,
        "last_result": reason,
        "last_detected_at": now.isoformat(),
    }


def _recovered_state(state: dict[str, Any], expected: str, observed: str, now: datetime) -> dict[str, Any]:
    return {
        **(state or {}),
        "status": "recovered",
        "expected_policy_version": expected,
        "observed_policy_version": observed,
        "attempt_count": 0,
        "consecutive_mismatch_cycles": 0,
        "last_result": "policy_version_confirmed",
        "recovered_at": now.isoformat(),
    }


def _decision(
    *,
    status: str,
    action: str,
    reason: str,
    expected: str,
    observed: str,
    state: dict[str, Any],
    config: dict[str, Any],
    now: datetime,
    blockers: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "enabled": bool(config.get("enabled", True)),
        "status": status,
        "action": action,
        "reason": reason,
        "expected_policy_version": expected,
        "observed_policy_version": observed,
        "blockers": blockers or [],
        "trading_blocked": status in {"recoverable", "unrecoverable"},
        "next_state": state or {},
        "config": config,
        "evaluated_at": now.isoformat(),
    }


def _recovery_command_id(now: datetime, attempt: int) -> str:
    return f"policy_recovery_{now.strftime('%Y%m%d_%H%M%S')}_{attempt}"


def _int_or_default(value: Any, fallback: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return fallback


def _short_error(exc: Exception) -> str:
    text = str(exc) or type(exc).__name__
    return text.splitlines()[0][:240]


def _strip_tz(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(UTC).replace(tzinfo=None)


def _utcnow() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)
