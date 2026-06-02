"""Pure helpers for code-enforced FULL_AUTO safety preconditions."""
from __future__ import annotations

from typing import Any

from services.final_risk_validation_config import resolve_final_risk_validation_mode


def full_auto_safety_precondition_violations(
    *,
    auth_mode: str,
    account_state_guard_config: dict[str, Any],
    final_risk_validation_config: dict[str, Any],
    auto_pause_config: dict[str, Any],
    execution_lifecycle_config: dict[str, Any] | None = None,
) -> list[str]:
    """Return configuration violations that make FULL_AUTO unsafe to enter."""
    if str(auth_mode or "").upper().strip() != "FULL_AUTO":
        return []

    violations: list[str] = []
    account_guard_mode = str(account_state_guard_config.get("mode") or "").lower().strip()
    if account_guard_mode != "blocking":
        violations.append("account_state_guard.mode must be blocking in FULL_AUTO")

    final_mode = resolve_final_risk_validation_mode(
        final_risk_validation_config,
        auth_mode="FULL_AUTO",
    )
    if final_mode != "blocking":
        violations.append("final_risk_validation effective mode must be blocking in FULL_AUTO")

    auto_pause_mode = str(auto_pause_config.get("mode") or "").lower().strip()
    if auto_pause_mode != "active":
        violations.append("auto_pause.mode must be active in FULL_AUTO")

    lifecycle_mode = str((execution_lifecycle_config or {}).get("mode") or "observe").lower().strip()
    if lifecycle_mode not in {"active", "strict"}:
        violations.append(
            "execution_lifecycle_config.mode must be active or strict in FULL_AUTO "
            f"(current: {lifecycle_mode})"
        )

    return violations
