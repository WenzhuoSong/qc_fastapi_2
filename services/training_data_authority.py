"""Training/validation data source authority checks.

Legacy raw JSON remains useful for debugging, but it is not authoritative for
training or validation. This module gives future dataset builders a small,
explicit gate so they cannot silently read unversioned LLM or pipeline blobs.
"""
from __future__ import annotations

from typing import Any


AUTHORITATIVE_SOURCE_TYPES = {
    "diagnostic_artifact",
    "validation_observation",
    "outcome_label",
}

LEGACY_SOURCE_TYPES = {
    "agent_step_log",
    "agent_analysis_raw",
    "agent_analysis.risk_output",
    "llm_raw_output",
    "legacy_json",
    "raw_json",
}


def evaluate_training_data_source(
    *,
    source_type: str,
    payload: dict[str, Any] | None,
) -> dict[str, Any]:
    """Return whether a payload can be used as training/validation data."""
    clean_source = str(source_type or "").strip()
    data = payload if isinstance(payload, dict) else {}
    reasons: list[str] = []

    if clean_source in LEGACY_SOURCE_TYPES or clean_source not in AUTHORITATIVE_SOURCE_TYPES:
        reasons.append("non_authoritative_source_type")

    if clean_source == "diagnostic_artifact":
        if not data.get("schema_version"):
            reasons.append("missing_schema_version")
        if not data.get("artifact_id"):
            reasons.append("missing_artifact_id")
        if data.get("execution_authority") != "none":
            reasons.append("execution_authority_not_none")

    elif clean_source == "validation_observation":
        observation_payload = data.get("observation_payload") if isinstance(data.get("observation_payload"), dict) else {}
        if not data.get("observation_id"):
            reasons.append("missing_observation_id")
        if not data.get("observation_type"):
            reasons.append("missing_observation_type")
        if data.get("execution_authority") != "none":
            reasons.append("execution_authority_not_none")
        if not (observation_payload.get("schema_version") or observation_payload.get("contract_version")):
            reasons.append("missing_observation_payload_version")

    elif clean_source == "outcome_label":
        if data.get("label_schema_version") != "outcome_label_v1":
            reasons.append("missing_or_invalid_label_schema_version")
        if data.get("training_authority") != "eligible":
            reasons.append("label_training_authority_not_eligible")

    return {
        "source_type": clean_source,
        "training_data_authority": "eligible" if not reasons else "not_authoritative",
        "allowed": not reasons,
        "reasons": sorted(set(reasons)),
    }


def assert_training_data_source_authority(
    *,
    source_type: str,
    payload: dict[str, Any] | None,
) -> dict[str, Any]:
    """Raise when a future dataset builder tries to consume non-authoritative data."""
    verdict = evaluate_training_data_source(source_type=source_type, payload=payload)
    if not verdict["allowed"]:
        reasons = ",".join(verdict["reasons"])
        raise ValueError(f"training data source is not authoritative: {source_type} ({reasons})")
    return verdict
