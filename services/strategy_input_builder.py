"""Strategy input construction for research/playground scoring.

This module is intentionally read-only. Callers preload live rows and daily
research features, then this builder decides which tickers are scorable for one
strategy and which tickers should be isolated with structured reasons.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum
from typing import Any

from services.feature_authority import authority_for_field, canonical_field_name, is_authoritative


class ExclusionReason(str, Enum):
    INSUFFICIENT_HISTORY = "insufficient_history"
    STALE_DATA = "stale_data"
    MISSING_FIELD = "missing_field"
    NON_AUTHORITATIVE_FIELD = "non_authoritative_field"
    WATCHLIST_ROLE = "watchlist_role"
    HARD_RISK_BLOCK = "hard_risk_block"


@dataclass(frozen=True)
class StrategyInputResult:
    strategy_name: str
    status: str
    scorable_rows: list[dict[str, Any]]
    excluded_tickers: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    readiness_summary: dict[str, Any] = field(default_factory=dict)
    field_provenance: dict[str, dict[str, Any]] = field(default_factory=dict)
    not_scored_reason: str | None = None

    @property
    def can_score(self) -> bool:
        return self.status in {"scored", "partially_scored"} and bool(self.scorable_rows)


LONG_LOOKBACK_REQUIRED_DAYS = {
    "mom_60d": 60,
    "return_60d": 60,
    "mom_252d": 252,
    "return_252d": 252,
    "sma_200": 200,
    "beta_vs_spy": 60,
}


def build_strategy_input(
    *,
    strategy: Any,
    live_rows: list[dict[str, Any]],
    feature_matrix: dict[str, dict[str, Any]] | None = None,
    as_of: date | None = None,
    stale_after_days: int = 5,
) -> StrategyInputResult:
    """Build per-strategy scoring rows and isolate bad ticker inputs."""
    as_of_date = as_of or date.today()
    matrix = {
        str(ticker or "").upper().strip(): dict(row or {})
        for ticker, row in (feature_matrix or {}).items()
        if str(ticker or "").strip()
    }
    candidate_rows = [
        _merge_daily_research_features(row, matrix.get(_ticker(row)) or {})
        for row in strategy.eligible_rows(live_rows)
        if _ticker(row)
    ]

    required = tuple(strategy.required_fields or ())
    scorable_rows: list[dict[str, Any]] = []
    excluded: dict[str, list[dict[str, Any]]] = {}
    provenance: dict[str, dict[str, Any]] = {}

    for row in candidate_rows:
        ticker = _ticker(row)
        ticker_reasons = _exclusion_reasons_for_row(
            row=row,
            required_fields=required,
            as_of=as_of_date,
            stale_after_days=stale_after_days,
        )
        provenance[ticker] = _provenance_by_field(row, required)
        if ticker_reasons:
            excluded[ticker] = ticker_reasons
        else:
            scorable_rows.append(row)

    candidate_count = len(candidate_rows)
    scorable_count = len(scorable_rows)
    coverage = (scorable_count / candidate_count) if candidate_count else 0.0
    min_coverage = float(getattr(strategy, "min_required_coverage", 0.70) or 0.70)
    coverage_below_min_required = (
        bool(candidate_count)
        and bool(scorable_count)
        and coverage < min_coverage
    )

    if not candidate_rows:
        status = "not_scored"
        not_scored_reason = "no_strategy_candidate_tickers"
    elif scorable_count == 0:
        status = "not_scored"
        not_scored_reason = "no_scorable_tickers"
    elif excluded:
        status = "partially_scored"
        not_scored_reason = None
    else:
        status = "scored"
        not_scored_reason = None

    partial_scoring_reason = (
        "scorable_coverage_below_min_required"
        if coverage_below_min_required
        else ("excluded_tickers_isolated" if excluded and scorable_count else None)
    )
    selection_policy = (
        "partial_scoring_with_ticker_isolation"
        if scorable_count
        else "not_scored_until_any_ticker_scorable"
    )
    readiness = {
        "ready": status in {"scored", "partially_scored"},
        "status": status,
        "candidate_ticker_count": candidate_count,
        "scorable_ticker_count": scorable_count,
        "excluded_ticker_count": len(excluded),
        "coverage": round(coverage, 4),
        "min_required_coverage": min_coverage,
        "coverage_below_min_required": coverage_below_min_required,
        "coverage_shortfall": round(max(min_coverage - coverage, 0.0), 4),
        "partial_scoring_reason": partial_scoring_reason,
        "selection_policy": selection_policy,
        "excluded_tickers": excluded,
        "exclusion_counts": _exclusion_counts(excluded),
        "not_scored_reason": not_scored_reason,
        "required_fields": list(required),
    }
    return StrategyInputResult(
        strategy_name=strategy.name,
        status=status,
        scorable_rows=scorable_rows,
        excluded_tickers=excluded,
        readiness_summary=readiness,
        field_provenance=provenance,
        not_scored_reason=not_scored_reason,
    )


def _merge_daily_research_features(row: dict[str, Any], feature: dict[str, Any]) -> dict[str, Any]:
    if not feature:
        return dict(row)
    merged = dict(row)
    source = str(feature.get("source") or "yfinance")
    filled_fields: list[str] = []
    for field, value in _feature_row_to_holding_fields(feature).items():
        if _should_merge_feature_value(merged, field, value, source):
            merged[field] = value
            filled_fields.append(field)
    if filled_fields:
        sources = list(merged.get("feature_sources") or [])
        sources.append({
            "source": source,
            "filled_fields": sorted(filled_fields),
            "authority_by_field": {
                field: authority_for_field(field, source).value
                for field in sorted(filled_fields)
            },
            "canonical_aliases": {
                field: canonical_field_name(field)
                for field in sorted(filled_fields)
                if canonical_field_name(field) != field
            },
            "trading_date": feature.get("trading_date"),
        })
        merged["feature_sources"] = sources
    return merged


def _feature_row_to_holding_fields(feature: dict[str, Any]) -> dict[str, Any]:
    return {
        "price": feature.get("close_price") or feature.get("adj_close_price"),
        "close_price": feature.get("close_price") or feature.get("adj_close_price"),
        "open_price": feature.get("open_price"),
        "high_price": feature.get("high_price"),
        "low_price": feature.get("low_price"),
        "volume": feature.get("volume"),
        "dollar_volume": feature.get("dollar_volume"),
        "daily_return_pct": feature.get("return_1d"),
        "return_1d": feature.get("return_1d"),
        "return_5d": feature.get("return_5d"),
        "mom_20d": feature.get("return_20d"),
        "mom_60d": feature.get("return_60d"),
        "mom_252d": feature.get("return_252d"),
        "sma_20": feature.get("sma_20"),
        "sma_50": feature.get("sma_50"),
        "sma_200": feature.get("sma_200"),
        "hist_vol_20d": feature.get("hist_vol_20d"),
        "rsi_10": feature.get("rsi_10"),
        "rsi_14": feature.get("rsi_14"),
        "atr_pct": feature.get("atr_pct"),
        "bb_position": feature.get("bb_position"),
        "beta_vs_spy": feature.get("beta_vs_spy"),
    }


def _should_merge_feature_value(row: dict[str, Any], field: str, value: Any, source: str) -> bool:
    if value is None:
        return False
    if row.get(field) is None:
        return True
    if not is_authoritative(field, source):
        return False
    return not _row_has_authoritative_source(row, field)


def _exclusion_reasons_for_row(
    *,
    row: dict[str, Any],
    required_fields: tuple[str, ...],
    as_of: date,
    stale_after_days: int,
) -> list[dict[str, Any]]:
    reasons: list[dict[str, Any]] = []
    for field in required_fields:
        value = _field_value(row, field)
        if value is None:
            reasons.append(_missing_reason(row, field))
            continue
        provenance = _source_for_field(row, field)
        if not _provenance_is_authoritative(field, provenance):
            reasons.append({
                "type": ExclusionReason.NON_AUTHORITATIVE_FIELD.value,
                "field": field,
                "source": provenance.get("source"),
                "authority": provenance.get("authority"),
            })
            continue
        source_date = _parse_date(provenance.get("trading_date"))
        if source_date and (as_of - source_date).days > stale_after_days:
            reasons.append({
                "type": ExclusionReason.STALE_DATA.value,
                "field": field,
                "source": provenance.get("source"),
                "trading_date": source_date.isoformat(),
                "age_days": (as_of - source_date).days,
                "threshold_days": stale_after_days,
            })
    return reasons


def _missing_reason(row: dict[str, Any], field: str) -> dict[str, Any]:
    if field in LONG_LOOKBACK_REQUIRED_DAYS and _has_price_history(row):
        return {
            "type": ExclusionReason.INSUFFICIENT_HISTORY.value,
            "field": field,
            "required_days": LONG_LOOKBACK_REQUIRED_DAYS[field],
        }
    return {
        "type": ExclusionReason.MISSING_FIELD.value,
        "field": field,
    }


def _field_value(row: dict[str, Any], field: str) -> Any:
    if row.get(field) is not None:
        return row.get(field)
    canonical = canonical_field_name(field)
    if canonical != field:
        return row.get(canonical)
    return None


def _source_for_field(row: dict[str, Any], field: str) -> dict[str, Any]:
    canonical = canonical_field_name(field)
    matches: list[dict[str, Any]] = []
    for source_info in row.get("feature_sources") or []:
        filled = set(source_info.get("filled_fields") or [])
        aliases = source_info.get("canonical_aliases") or {}
        by_field = source_info.get("authority_by_field") or {}
        source = str(source_info.get("source") or "unknown")
        matched = field in filled or canonical in filled or aliases.get(field) == canonical
        if not matched:
            continue
        matches.append({
            "source": source,
            "authority": by_field.get(field) or by_field.get(canonical) or authority_for_field(field, source).value,
            "trading_date": source_info.get("trading_date"),
        })
    for match in matches:
        if _provenance_is_authoritative(field, match):
            return match
    if matches:
        return matches[-1]
    return {
        "source": "live_row",
        "authority": authority_for_field(field, "qc_heartbeat").value,
        "trading_date": None,
    }


def _provenance_by_field(row: dict[str, Any], fields: tuple[str, ...]) -> dict[str, Any]:
    return {
        field: _source_for_field(row, field)
        for field in fields
        if _field_value(row, field) is not None
    }


def _provenance_is_authoritative(field: str, provenance: dict[str, Any]) -> bool:
    authority = str(provenance.get("authority") or "")
    if authority in {"daily_research", "live_state", "intraday"}:
        return True
    return is_authoritative(field, provenance.get("source"))


def _row_has_authoritative_source(row: dict[str, Any], field: str) -> bool:
    return _provenance_is_authoritative(field, _source_for_field(row, field))


def _has_price_history(row: dict[str, Any]) -> bool:
    return any(
        row.get(field) is not None
        for field in ("close_price", "return_1d", "return_5d", "mom_20d", "hist_vol_20d")
    )


def _exclusion_counts(excluded: dict[str, list[dict[str, Any]]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for reasons in excluded.values():
        for reason in reasons:
            key = str(reason.get("type") or "unknown")
            counts[key] = counts.get(key, 0) + 1
    return counts


def _ticker(row: dict[str, Any]) -> str:
    return str((row or {}).get("ticker") or "").upper().strip()


def _parse_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None
