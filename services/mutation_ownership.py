"""Ownership registry for legacy and post-risk target mutation paths."""
from __future__ import annotations

from typing import Any


REGIME_CONSTRAINT_MUTATION_TYPE = "regime_constraint_tighten"

UNCLASSIFIED_MUTATION_OBSERVE_CRITERIA: dict[str, Any] = {
    "min_cycles_before_decision": 20,
    "decision_deadline": "2026-07-01",
}

LEGACY_MUTATION_CLASSIFICATIONS: dict[str, dict[str, Any]] = {
    "enforce_pm_constraints": {
        "status": "deprecated_inactive",
        "owner": "none",
        "stage": "legacy_stage5",
        "execution_authority": "none",
        "target_weight_mutation": "none",
        "decision": "remove_after_no_references_confirmed",
        "observe_criteria": UNCLASSIFIED_MUTATION_OBSERVE_CRITERIA,
    },
    "enforce_pm_constraints_v2": {
        "status": "classified",
        "owner": "diagnostic_guardrail",
        "stage": "stage5_synthesizer_diagnostics",
        "execution_authority": "none",
        "target_weight_mutation": "none",
        "decision": "keep_as_non_execution_legacy_adjusted_weights_bound",
        "observe_criteria": None,
    },
    "apply_regime_constraints": {
        "status": "classified",
        "owner": "post_risk_tighten_only",
        "stage": "stage6_to_7_regime_constraint",
        "execution_authority": "tighten_only",
        "target_weight_mutation": "tighten_only",
        "mutation_type": REGIME_CONSTRAINT_MUTATION_TYPE,
        "decision": "registered_final_validation_allowed_mutation",
        "observe_criteria": UNCLASSIFIED_MUTATION_OBSERVE_CRITERIA,
    },
}


def legacy_mutation_classification_summary() -> dict[str, Any]:
    """Return a JSON-safe classification snapshot for diagnostics and review."""
    rows = {
        name: dict(payload)
        for name, payload in sorted(LEGACY_MUTATION_CLASSIFICATIONS.items())
    }
    unresolved = [
        name
        for name, payload in rows.items()
        if str(payload.get("status") or "") not in {"classified", "deprecated_inactive"}
    ]
    return {
        "contract_version": "legacy_mutation_ownership_v1",
        "unclassified_observe_criteria": dict(UNCLASSIFIED_MUTATION_OBSERVE_CRITERIA),
        "unresolved_count": len(unresolved),
        "unresolved": unresolved,
        "classifications": rows,
    }
