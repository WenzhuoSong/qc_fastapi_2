"""Read-only safety invariant scans for configuration fail-safe posture."""

from __future__ import annotations

from typing import Any

from services.operator_halt import normalize_operator_halt_state


SCHEMA_VERSION = "safety_config_fail_safe_report_v1"


def build_config_fail_safe_report(configs: dict[str, Any] | None) -> dict[str, Any]:
    """Return a read-only report of safety config paths that need attention."""
    configs = configs or {}
    findings: list[dict[str, Any]] = []

    operator_state = normalize_operator_halt_state(configs.get("operator_halt_state"))
    if operator_state.get("fail_safe"):
        findings.append(
            _finding(
                "operator_halt_state",
                "operator_halt_state_missing_or_invalid",
                "halted",
                "missing or malformed operator halt state must fail closed",
            )
        )

    circuit_state = _normalize_circuit_state(configs.get("circuit_state"))
    if circuit_state["fail_safe"]:
        findings.append(
            _finding(
                "circuit_state",
                "circuit_state_missing_or_invalid",
                "ALERT",
                "missing or malformed circuit state must fail closed",
            )
        )

    emergency_enabled = _bool_value(configs.get("emergency_auto_liquidate"), default=False)
    if emergency_enabled:
        findings.append(
            _finding(
                "emergency_auto_liquidate",
                "emergency_auto_liquidate_enabled_requires_lifecycle",
                "disabled_until_lifecycle_and_reconciliation_wrapped",
                "automatic liquidation is highest-impact and must not bypass lifecycle controls",
            )
        )

    _require_explicit_mode(
        findings,
        configs,
        key="account_state_guard_config",
        allowed={"blocking", "observe", "off"},
        preferred_for_full_auto={"blocking"},
    )
    _require_explicit_mode(
        findings,
        configs,
        key="auto_pause_config",
        allowed={"active", "observe", "off"},
        preferred_for_full_auto={"active"},
    )
    _require_explicit_mode(
        findings,
        configs,
        key="execution_lifecycle_config",
        allowed={"strict", "active", "observe", "off"},
        preferred_for_full_auto={"strict", "active"},
    )
    _require_explicit_mode(
        findings,
        configs,
        key="reconciliation_guard_config",
        allowed={"blocking", "observe", "off"},
        preferred_for_full_auto={"blocking"},
    )

    return {
        "schema_version": SCHEMA_VERSION,
        "execution_authority": "none",
        "target_weight_mutation": "none",
        "finding_count": len(findings),
        "fail_safe_required": bool(findings),
        "findings": findings,
        "effective_states": {
            "operator_halt": {
                "halted": bool(operator_state.get("halted")),
                "fail_safe": bool(operator_state.get("fail_safe")),
            },
            "circuit_state": circuit_state,
            "emergency_auto_liquidate": {"enabled": emergency_enabled},
        },
    }


def _require_explicit_mode(
    findings: list[dict[str, Any]],
    configs: dict[str, Any],
    *,
    key: str,
    allowed: set[str],
    preferred_for_full_auto: set[str],
) -> None:
    payload = configs.get(key)
    if not isinstance(payload, dict):
        findings.append(
            _finding(
                key,
                f"{key}_missing_or_malformed",
                f"explicit mode in {sorted(preferred_for_full_auto)} before FULL_AUTO",
                "safety-critical config must be explicit, not inferred from a missing row",
            )
        )
        return
    mode = str(payload.get("mode") or "").lower().strip()
    if mode not in allowed:
        findings.append(
            _finding(
                key,
                f"{key}_invalid_mode",
                f"one of {sorted(allowed)}",
                "invalid safety mode must not silently run with ambiguous posture",
                actual=mode or None,
            )
        )


def _normalize_circuit_state(payload: Any) -> dict[str, Any]:
    valid = {"CLOSED", "ALERT", "DEFENSIVE"}
    if not isinstance(payload, dict):
        return {"value": "ALERT", "fail_safe": True, "reason": "circuit_state_missing_or_malformed"}
    raw = str(payload.get("value") or "").upper().strip()
    if raw not in valid:
        return {"value": "ALERT", "fail_safe": True, "reason": "circuit_state_missing_or_invalid"}
    return {"value": raw, "fail_safe": False, "reason": None}


def _bool_value(value: Any, *, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, dict):
        return _bool_value(value.get("enabled"), default=default)
    if isinstance(value, str):
        lowered = value.lower().strip()
        if lowered in {"1", "true", "yes", "on"}:
            return True
        if lowered in {"0", "false", "no", "off"}:
            return False
    return default


def _finding(
    key: str,
    code: str,
    expected_safe_behavior: str,
    detail: str,
    *,
    actual: Any | None = None,
) -> dict[str, Any]:
    out = {
        "key": key,
        "code": code,
        "severity": "high",
        "expected_safe_behavior": expected_safe_behavior,
        "detail": detail,
    }
    if actual is not None:
        out["actual"] = actual
    return out
