"""Final execution preflight checks before commands are sent to QC."""
from __future__ import annotations

from typing import Any

from services.execution_policy import evaluate_policy


def preflight_execution_weights(weights: dict[str, Any]) -> dict[str, Any]:
    """Return blocking policy violations for a proposed execution payload."""
    policy = evaluate_policy(weights=weights)
    cap_violations = policy["cap_violations"]
    group_violations = policy["group_violations"]
    return {
        "allowed": bool(policy["allowed"]),
        "cap_violations": cap_violations,
        "group_violations": group_violations,
        "policy_version": policy["policy_version"],
        "policy_evaluation": policy,
    }
