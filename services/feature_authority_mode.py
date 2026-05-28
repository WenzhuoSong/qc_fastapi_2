"""Feature-authority runtime mode.

The rollout has completed: yfinance is the daily research authority, QC daily
snapshots are audit/fallback data, and QC heartbeat is live account state.
"""
from __future__ import annotations

from typing import Any


YFINANCE_RESEARCH = "yfinance_research"

VALID_FEATURE_AUTHORITY_MODES = frozenset({
    YFINANCE_RESEARCH,
})

DEFAULT_FEATURE_AUTHORITY_MODE = YFINANCE_RESEARCH


def normalize_feature_authority_mode(raw: Any) -> str:
    if isinstance(raw, dict):
        raw = raw.get("value") or raw.get("mode")
    mode = str(raw or DEFAULT_FEATURE_AUTHORITY_MODE).strip().lower()
    if mode not in VALID_FEATURE_AUTHORITY_MODES:
        return DEFAULT_FEATURE_AUTHORITY_MODE
    return mode
