"""Weight source authority contract.

This module is the single naming contract for executable, advisory, and
reference weight outputs.  It keeps legacy diagnostic weights out of the
target-builder path while giving the dashboard structured labels for display.
"""
from __future__ import annotations

from typing import Any


CONTRACT_VERSION = "weight_source_contract_v1"

EXECUTABLE_TARGET_KEY = "target_weights"
PC_CANDIDATE_KEY = "pc_candidate_weights"
PC_SHADOW_KEY = "pc_shadow_weights"
LLM_ADJUSTED_KEY = "llm_adjusted_weights"
BASELINE_REFERENCE_KEY = "baseline_reference_weights"

FORBIDDEN_TARGET_BUILDER_INPUT_KEYS = frozenset(
    {
        "adjusted_weights",
        "raw_llm_adjusted_weights",
        LLM_ADJUSTED_KEY,
        "diagnostic_llm_target",
        PC_SHADOW_KEY,
        BASELINE_REFERENCE_KEY,
        "baseline_reference_target",
        "baseline_weights_reference",
        "reference_weights",
    }
)

WEIGHT_SOURCE_LABELS: dict[str, dict[str, Any]] = {
    "final_target": {
        "label": "Executable final target",
        "authority": "executable",
        "visual_class": "weight-executable",
        "may_enter_target_builder": False,
        "display_note": "post-risk target sent toward execution",
    },
    "target_builder_target": {
        "label": "Executable target-builder target",
        "authority": "executable",
        "visual_class": "weight-executable",
        "may_enter_target_builder": False,
        "display_note": "deterministic target-builder output",
    },
    "portfolio_construction_target": {
        "label": "PC candidate/advisory target",
        "authority": "candidate_or_advisory",
        "visual_class": "weight-advisory",
        "may_enter_target_builder": "gated_only",
        "display_note": "may affect execution only through pc_candidate_weights in gated mode",
    },
    "diagnostic_llm_target": {
        "label": "LLM diagnostic target",
        "authority": "advisory_only",
        "visual_class": "weight-advisory",
        "may_enter_target_builder": False,
        "display_note": "display/review only; never target-builder input",
    },
    "validated_advisory_delta": {
        "label": "Validated advisory delta",
        "authority": "advisory_delta",
        "visual_class": "weight-advisory",
        "may_enter_target_builder": False,
        "display_note": "validator-bounded explanation delta, not a target weight",
    },
    "baseline_reference_weights": {
        "label": "Baseline reference weights",
        "authority": "reference_only",
        "visual_class": "weight-reference",
        "may_enter_target_builder": False,
        "display_note": "scorecard/reference only",
    },
}


def weight_source_contract_summary() -> dict[str, Any]:
    """Return the compact runtime contract summary."""
    return {
        "contract_version": CONTRACT_VERSION,
        "executable_target_key": EXECUTABLE_TARGET_KEY,
        "pc_candidate_key": PC_CANDIDATE_KEY,
        "pc_shadow_key": PC_SHADOW_KEY,
        "llm_adjusted_key": LLM_ADJUSTED_KEY,
        "baseline_reference_key": BASELINE_REFERENCE_KEY,
        "forbidden_target_builder_input_keys": sorted(FORBIDDEN_TARGET_BUILDER_INPUT_KEYS),
    }


def dashboard_weight_source_labels() -> list[dict[str, Any]]:
    """Return dashboard rows for the weight source visual legend."""
    return [
        {"column": key, **dict(value)}
        for key, value in sorted(WEIGHT_SOURCE_LABELS.items())
    ]


def classify_weight_column(column: str) -> dict[str, Any]:
    """Return dashboard metadata for a weight-like column."""
    return dict(
        WEIGHT_SOURCE_LABELS.get(
            column,
            {
                "label": column,
                "authority": "unknown",
                "visual_class": "weight-unknown",
                "may_enter_target_builder": False,
                "display_note": "unregistered weight source",
            },
        )
    )


def assert_no_forbidden_target_builder_inputs(payload: Any, *, label: str = "target_builder") -> None:
    """Assert that legacy diagnostic weight keys are absent from target-builder input."""
    matches: list[str] = []
    _collect_forbidden_paths(payload, path=label, matches=matches)
    if matches:
        joined = ", ".join(matches)
        raise AssertionError(
            "Forbidden target_builder input weight source detected: "
            f"{joined}. Advisory/reference weights must not enter target_builder."
        )


def _collect_forbidden_paths(payload: Any, *, path: str, matches: list[str]) -> None:
    if isinstance(payload, dict):
        for key, value in payload.items():
            clean_key = str(key).strip()
            lowered = clean_key.lower()
            child_path = f"{path}.{clean_key}" if clean_key else path
            if lowered in FORBIDDEN_TARGET_BUILDER_INPUT_KEYS:
                matches.append(child_path)
            _collect_forbidden_paths(value, path=child_path, matches=matches)
    elif isinstance(payload, (list, tuple)):
        for index, value in enumerate(payload):
            _collect_forbidden_paths(value, path=f"{path}[{index}]", matches=matches)
