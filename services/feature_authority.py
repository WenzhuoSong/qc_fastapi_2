"""Field authority policy for QC live data and yfinance research features.

This module defines source-of-truth semantics only. It does not merge data or
change downstream behavior by itself.
"""
from __future__ import annotations

from enum import Enum
from typing import Any


class FeatureAuthority(str, Enum):
    LIVE_STATE = "live_state"
    INTRADAY = "intraday"
    DAILY_RESEARCH = "daily_research"
    QC_EOD_AUDIT = "qc_eod_audit"
    LEGACY_DEBUG = "legacy_debug"
    UNKNOWN = "unknown"


QC_HEARTBEAT_SOURCE = "qc_heartbeat"
QC_DAILY_SOURCE = "qc_daily_snapshot"
YFINANCE_SOURCE = "yfinance"


LIVE_STATE_FIELDS = frozenset({
    "price",
    "last_price",
    "weight_current",
    "weight_target",
    "weight_drift",
    "unrealized_pnl_pct",
    "holding_days",
    "total_value",
    "cash",
    "cash_pct",
    "daily_pnl_pct",
    "current_drawdown_pct",
    "trading_session",
    "target_weights",
    "is_market_open",
    "minutes_since_open",
    "last_trade_time",
})

INTRADAY_FIELDS = frozenset({
    "intraday_open_price",
    "intraday_high_price",
    "intraday_low_price",
    "intraday_volume",
    "intraday_return_pct",
})

DAILY_OHLCV_FIELDS = frozenset({
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "adj_close_price",
    "volume",
    "dollar_volume",
})

CANONICAL_RETURN_FIELDS = frozenset({
    "return_1d",
    "return_5d",
    "return_20d",
    "return_60d",
    "return_252d",
})

DAILY_RESEARCH_FIELDS = frozenset({
    *DAILY_OHLCV_FIELDS,
    *CANONICAL_RETURN_FIELDS,
    "sma_20",
    "sma_50",
    "sma_200",
    "hist_vol_20d",
    "rsi_14",
    "atr_pct",
    "bb_position",
    "beta_vs_spy",
})

LEGACY_RETURN_ALIASES = {
    "daily_return_pct": "return_1d",
    "mom_20d": "return_20d",
    "mom_60d": "return_60d",
    "mom_252d": "return_252d",
}

INDICATOR_FIELD_ALIASES = {
    "rsi": "rsi_14",
    "atr": "atr_pct",
    "hist_vol": "hist_vol_20d",
    "unrealized_pnl": "unrealized_pnl_pct",
    "current_drawdown": "current_drawdown_pct",
}

CANONICAL_FIELD_ALIASES = {
    **LEGACY_RETURN_ALIASES,
    **INDICATOR_FIELD_ALIASES,
}

LEGACY_QC_INDICATOR_FIELDS = frozenset({
    *LEGACY_RETURN_ALIASES.keys(),
    "sma_20",
    "sma_50",
    "sma_200",
    "rsi_14",
    "atr_pct",
    "bb_position",
    "hist_vol_20d",
})

CANONICAL_TOP_LEVEL_FIELDS = frozenset({
    *LIVE_STATE_FIELDS,
    *INTRADAY_FIELDS,
    *DAILY_RESEARCH_FIELDS,
})


def canonical_field_aliases() -> dict[str, str]:
    """Return accepted legacy/shorthand aliases for canonical field names."""
    return dict(CANONICAL_FIELD_ALIASES)


def canonical_field_name(field: str) -> str:
    clean = _clean_field(field)
    return CANONICAL_FIELD_ALIASES.get(clean, clean)


def authority_for_field(field: str, source: str | None) -> FeatureAuthority:
    field = _clean_field(field)
    source = _clean_source(source)
    canonical = canonical_field_name(field)

    if source == QC_HEARTBEAT_SOURCE:
        if canonical in LIVE_STATE_FIELDS:
            return FeatureAuthority.LIVE_STATE
        if canonical in INTRADAY_FIELDS:
            return FeatureAuthority.INTRADAY
        if field in LEGACY_QC_INDICATOR_FIELDS:
            return FeatureAuthority.LEGACY_DEBUG
        if canonical in DAILY_RESEARCH_FIELDS:
            return FeatureAuthority.LEGACY_DEBUG
        return FeatureAuthority.UNKNOWN

    if source == YFINANCE_SOURCE:
        if canonical in DAILY_RESEARCH_FIELDS:
            return FeatureAuthority.DAILY_RESEARCH
        return FeatureAuthority.UNKNOWN

    if source == QC_DAILY_SOURCE:
        if canonical in DAILY_RESEARCH_FIELDS:
            return FeatureAuthority.QC_EOD_AUDIT
        return FeatureAuthority.UNKNOWN

    return FeatureAuthority.UNKNOWN


def is_authoritative(field: str, source: str | None) -> bool:
    return authority_for_field(field, source) in {
        FeatureAuthority.LIVE_STATE,
        FeatureAuthority.INTRADAY,
        FeatureAuthority.DAILY_RESEARCH,
    }


def is_canonical_top_level_field(field: str) -> bool:
    field = _clean_field(field)
    return field in CANONICAL_TOP_LEVEL_FIELDS and field not in LEGACY_RETURN_ALIASES


def legacy_debug_namespace(row: dict[str, Any], fields: set[str] | None = None) -> dict[str, Any]:
    """Extract legacy QC indicator fields without mutating the source row."""
    candidates = fields or LEGACY_QC_INDICATOR_FIELDS
    return {
        field: row.get(field)
        for field in sorted(candidates)
        if row.get(field) is not None
    }


def source_of_truth_policy() -> dict[str, Any]:
    """Compact machine-readable source-of-truth policy for downstream agents."""
    return {
        "live_state": {
            "source": QC_HEARTBEAT_SOURCE,
            "authority": FeatureAuthority.LIVE_STATE.value,
            "fields": sorted(LIVE_STATE_FIELDS),
        },
        "intraday": {
            "source": QC_HEARTBEAT_SOURCE,
            "authority": FeatureAuthority.INTRADAY.value,
            "fields": sorted(INTRADAY_FIELDS),
            "namespace": "intraday_*",
        },
        "daily_research": {
            "source": YFINANCE_SOURCE,
            "authority": FeatureAuthority.DAILY_RESEARCH.value,
            "fields": sorted(DAILY_RESEARCH_FIELDS),
            "fallback_source": QC_DAILY_SOURCE,
            "fallback_authority": FeatureAuthority.QC_EOD_AUDIT.value,
        },
        "legacy_debug": {
            "source": QC_HEARTBEAT_SOURCE,
            "authority": FeatureAuthority.LEGACY_DEBUG.value,
            "fields": sorted(LEGACY_QC_INDICATOR_FIELDS),
            "namespace": "legacy_qc_indicators",
        },
        "fallback_semantics": "fallbacks may tighten risk but must not increase execution permission",
    }


def build_feature_source_summary(
    feature_provenance: dict[str, Any] | None = None,
    schema_capabilities: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Summarize feature authority for RiskMgr, governance, dashboard, and copy."""
    provenance = feature_provenance or {}
    capabilities = schema_capabilities or {}
    daily_research_source = capabilities.get("daily_research_authority") or "unknown"
    intraday_state = capabilities.get("intraday_live_state") or "unknown"
    return {
        "live_state_source": QC_HEARTBEAT_SOURCE,
        "daily_research_source": daily_research_source,
        "daily_research_authority": FeatureAuthority.DAILY_RESEARCH.value
        if daily_research_source == YFINANCE_SOURCE
        else FeatureAuthority.QC_EOD_AUDIT.value
        if daily_research_source == "qc_daily_fallback"
        else FeatureAuthority.UNKNOWN.value,
        "intraday_source": QC_HEARTBEAT_SOURCE,
        "intraday_live_state": intraday_state,
        "legacy_debug_namespace": "legacy_qc_indicators",
        "fallback_policy": "tighten_only",
        "source_counts": provenance.get("source_counts") or {},
        "authority_counts": provenance.get("authority_counts") or {},
        "stale_fields": provenance.get("stale_fields") or {},
        "has_stale_fields": bool(provenance.get("has_stale_fields")),
    }


def _clean_field(field: str) -> str:
    return str(field or "").strip()


def _clean_source(source: str | None) -> str:
    return str(source or "").strip().lower()
