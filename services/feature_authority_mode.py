"""Runtime switch for feature-authority rollout mode."""
from __future__ import annotations

from typing import Any


LEGACY_OVERLAY = "legacy_overlay"
AUDIT_ONLY = "audit_only"
YFINANCE_RESEARCH = "yfinance_research"

VALID_FEATURE_AUTHORITY_MODES = frozenset({
    LEGACY_OVERLAY,
    AUDIT_ONLY,
    YFINANCE_RESEARCH,
})

DEFAULT_FEATURE_AUTHORITY_MODE = AUDIT_ONLY
ROLLBACK_REASON_DEFAULT = "new_merge_pipeline_impact"


def normalize_feature_authority_mode(raw: Any) -> str:
    if isinstance(raw, dict):
        raw = raw.get("value") or raw.get("mode")
    mode = str(raw or DEFAULT_FEATURE_AUTHORITY_MODE).strip().lower()
    if mode not in VALID_FEATURE_AUTHORITY_MODES:
        return DEFAULT_FEATURE_AUTHORITY_MODE
    return mode


def feature_authority_rollback_config(
    *,
    previous_mode: Any = None,
    reason: str = ROLLBACK_REASON_DEFAULT,
) -> dict[str, Any]:
    """Return the system_config payload for a non-destructive rollback."""
    return {
        "value": LEGACY_OVERLAY,
        "rollback": {
            "reason": str(reason or ROLLBACK_REASON_DEFAULT),
            "previous_mode": normalize_feature_authority_mode(previous_mode),
            "preserve_audit_report": True,
            "no_database_rollback": True,
            "preserve_provenance_fields": True,
        },
    }
