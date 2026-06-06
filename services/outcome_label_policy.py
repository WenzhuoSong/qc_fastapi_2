"""Dependency-light outcome label policy metadata."""
from __future__ import annotations

from typing import Any


LABEL_PRICE_SOURCE_MAP: dict[str, set[str]] = {
    "qc_execution": {"fill_price"},
    "qc_snapshot": {"qc_market_price"},
    "yfinance": {"yfinance_adjusted_close"},
}


def outcome_label_contract_summary() -> dict[str, Any]:
    """Return the stable point-in-time label contract for operator/audit logs."""
    return {
        "label_schema_version": "outcome_label_v1",
        "execution_authority": "none",
        "horizons": ["1d", "5d", "20d"],
        "label_source_price_sources": {
            source: sorted(prices)
            for source, prices in sorted(LABEL_PRICE_SOURCE_MAP.items())
        },
        "preferred_training_source": "qc_execution",
        "preferred_training_price_source": "fill_price",
        "fallback_sources": ["qc_snapshot", "yfinance"],
        "fallback_training_authority": "feature_scope_limited",
        "training_authority_requires": [
            "decision_feature_snapshot_id",
            "decision_feature_snapshot_schema_version=decision_feature_snapshot_v1",
            "decision_feature_snapshot_as_of_time<=decision_time",
            "non_mixed_feature_authority",
            "non_mixed_price_source",
            "preferred_label_source=qc_execution",
            "preferred_price_source=fill_price",
        ],
        "point_in_time_rule": (
            "features must be observed at or before decision_time; outcomes are "
            "measured after decision_time"
        ),
    }
