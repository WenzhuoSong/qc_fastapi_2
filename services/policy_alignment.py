"""Policy-version alignment helpers shared by execution entry points."""
from __future__ import annotations

from datetime import UTC, datetime
from typing import Any


DEFAULT_MANUAL_CONFIRM_POLICY_ALIGNMENT_CONFIG: dict[str, Any] = {
    "enabled": True,
    "max_age_seconds": 300,
}


def default_manual_confirm_policy_alignment_config(config: dict[str, Any] | None = None) -> dict[str, Any]:
    """Return a normalized manual-confirm policy-alignment config."""
    merged = dict(DEFAULT_MANUAL_CONFIRM_POLICY_ALIGNMENT_CONFIG)
    merged.update(config or {})
    merged["enabled"] = bool(merged.get("enabled", True))
    try:
        merged["max_age_seconds"] = max(float(merged.get("max_age_seconds", 300)), 0.0)
    except (TypeError, ValueError):
        merged["max_age_seconds"] = 300.0
    return merged


def policy_alignment_from_account_guard(
    account_guard: dict[str, Any] | None,
    *,
    expected_policy_version: str | None,
    now: datetime | None = None,
    max_age_seconds: float | None = None,
) -> dict[str, Any]:
    """Return policy alignment from an account_state_guard result.

    This is intentionally a read-only assertion. It never sends PolicySync.
    PolicySync repair belongs to the control-plane recovery stage.
    """
    guard = account_guard or {}
    checks = guard.get("checks") if isinstance(guard.get("checks"), dict) else {}
    version_match = checks.get("policy_version_matches_expected") or {}
    version_present = checks.get("policy_version_present") or {}
    snapshot = guard.get("snapshot") if isinstance(guard.get("snapshot"), dict) else {}
    expected = str(expected_policy_version or "").strip()
    actual = str(snapshot.get("policy_version") or version_present.get("actual") or "").strip()
    age_seconds = _guard_age_seconds(snapshot, now=now)
    age_ok = True if max_age_seconds is None else age_seconds is not None and age_seconds <= float(max_age_seconds)
    aligned = (
        bool(guard.get("enabled", True))
        and str(guard.get("status") or "").lower().strip() == "pass"
        and not bool(guard.get("would_block"))
        and bool(version_present.get("pass", bool(actual)))
        and (bool(version_match.get("pass")) if expected else bool(actual))
        and age_ok
    )
    return {
        "source": "account_state_guard",
        "aligned": aligned,
        "expected_policy_version": expected or None,
        "actual_policy_version": actual or None,
        "guard_status": guard.get("status"),
        "guard_blockers": guard.get("blockers") or [],
        "age_seconds": age_seconds,
        "max_age_seconds": max_age_seconds,
        "age_ok": age_ok,
        "version_check": version_match,
        "present_check": version_present,
    }


def _guard_age_seconds(snapshot: dict[str, Any], *, now: datetime | None = None) -> float | None:
    age = snapshot.get("age_seconds")
    if isinstance(age, (int, float)):
        return float(age)

    recorded_at = _parse_datetime(snapshot.get("recorded_at"))
    if not recorded_at:
        return None
    current = _strip_tz(now or datetime.now(UTC))
    return max((current - recorded_at).total_seconds(), 0.0)


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return _strip_tz(value)
    if isinstance(value, str) and value:
        try:
            return _strip_tz(datetime.fromisoformat(value.replace("Z", "+00:00")))
        except ValueError:
            return None
    return None


def _strip_tz(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(UTC).replace(tzinfo=None)
