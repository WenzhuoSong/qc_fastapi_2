"""Process-level execution fence for every FastAPI-originated QC command.

The observer-first build intentionally supports no executable authority. The
environment value is still recorded so a poisoned or stale deployment can be
diagnosed, but no value can enable a command in this release.
"""
from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Any


EXECUTION_AUTHORITY_ENV = "FASTAPI_EXECUTION_AUTHORITY"
FROZEN_EXECUTION_AUTHORITY = "none"


def evaluate_execution_authority_fence(
    command_name: str,
    *,
    environ: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Return the immutable observer-first admission decision.

    This function deliberately does not read database configuration. Missing,
    malformed, legacy, and future-looking values all remain non-executable.
    """
    source = os.environ if environ is None else environ
    configured = str(source.get(EXECUTION_AUTHORITY_ENV, "") or "").strip().lower()
    reason = (
        "fastapi_execution_authority_none"
        if configured in {"", FROZEN_EXECUTION_AUTHORITY}
        else "unsupported_fastapi_execution_authority"
    )
    return {
        "allowed": False,
        "blocked": True,
        "command": str(command_name or "unknown"),
        "execution_authority": FROZEN_EXECUTION_AUTHORITY,
        "configured_execution_authority": configured or FROZEN_EXECUTION_AUTHORITY,
        "source": "environment_or_code_default",
        "reason": reason,
    }


def execution_authority_status(
    *,
    environ: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Return the public, secret-free status of the process-level fence."""
    decision = evaluate_execution_authority_fence("health_check", environ=environ)
    return {
        "execution_authority": decision["execution_authority"],
        "configured_execution_authority": decision["configured_execution_authority"],
        "fence_enforced": True,
        "executable_commands_enabled": False,
        "reason": decision["reason"],
    }
