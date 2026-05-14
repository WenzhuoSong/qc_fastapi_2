"""
Strategy feature contract and data-quality verdicts.

Strategies declare what they need. This module evaluates whether the current
market data can safely support each strategy and explains source/freshness for
downstream agents.
"""
from __future__ import annotations

from datetime import date, datetime
from typing import Any

from services.universe_policy import filter_tradable_research_rows
from strategies.base import Strategy


DEFAULT_STALE_AFTER_DAYS = 5


def build_strategy_feature_contract(
    strategy: Strategy,
    holdings: list[dict[str, Any]],
    *,
    as_of: date | None = None,
    stale_after_days: int = DEFAULT_STALE_AFTER_DAYS,
) -> dict[str, Any]:
    """Return a machine-readable readiness and provenance verdict."""
    as_of_date = as_of or date.today()
    valid_holdings = filter_tradable_research_rows(holdings)
    required_fields = tuple(strategy.required_fields or ())
    optional_fields = tuple(strategy.optional_fields or ())
    field_contracts = [
        _field_contract(
            field,
            valid_holdings,
            required=field in required_fields,
            as_of=as_of_date,
            stale_after_days=stale_after_days,
        )
        for field in [*required_fields, *optional_fields]
    ]

    required_contracts = [item for item in field_contracts if item["required"]]
    missing_required = [
        item["field"]
        for item in required_contracts
        if item["coverage"] < float(strategy.min_required_coverage)
    ]
    stale_required = [
        item["field"]
        for item in required_contracts
        if item["freshness"] == "stale"
    ]
    readiness = strategy.data_readiness(valid_holdings)
    eligible_tickers = readiness.get("eligible_tickers") or []
    ready = (
        bool(readiness.get("ready"))
        and not missing_required
        and not stale_required
        and bool(eligible_tickers or not required_fields)
    )

    if ready:
        verdict = "ready"
        can_influence_allocation = True
    elif missing_required:
        verdict = "blocked_missing_required_fields"
        can_influence_allocation = False
    elif stale_required:
        verdict = "blocked_stale_required_fields"
        can_influence_allocation = False
    else:
        verdict = "blocked_no_eligible_tickers"
        can_influence_allocation = False

    return {
        "strategy_name": strategy.name,
        "strategy_version": strategy.version,
        "as_of": as_of_date.isoformat(),
        "required_fields": list(required_fields),
        "optional_fields": list(optional_fields),
        "min_required_coverage": float(strategy.min_required_coverage),
        "eligible_tickers": eligible_tickers,
        "field_contracts": field_contracts,
        "missing_required_fields": missing_required,
        "stale_required_fields": stale_required,
        "verdict": verdict,
        "ready": ready,
        "can_influence_allocation": can_influence_allocation,
    }


def _field_contract(
    field: str,
    holdings: list[dict[str, Any]],
    *,
    required: bool,
    as_of: date,
    stale_after_days: int,
) -> dict[str, Any]:
    total = len(holdings)
    covered_rows = [row for row in holdings if row.get(field) is not None]
    coverage = (len(covered_rows) / total) if total else 0.0
    source_counts: dict[str, int] = {}
    source_dates: list[date] = []

    for row in covered_rows:
        source, source_date = _source_for_field(row, field)
        source_counts[source] = source_counts.get(source, 0) + 1
        if source_date:
            source_dates.append(source_date)

    oldest_age_days = None
    if source_dates:
        oldest_age_days = max((as_of - item).days for item in source_dates)

    freshness = "unknown"
    if covered_rows:
        freshness = "fresh"
        if oldest_age_days is not None and oldest_age_days > stale_after_days:
            freshness = "stale"

    return {
        "field": field,
        "required": required,
        "coverage": round(coverage, 4),
        "covered_tickers": [
            (row.get("ticker") or "").upper().strip()
            for row in covered_rows
            if row.get("ticker")
        ],
        "missing_tickers": [
            (row.get("ticker") or "").upper().strip()
            for row in holdings
            if row.get("ticker") and row.get(field) is None
        ],
        "source_counts": source_counts,
        "freshness": freshness,
        "oldest_age_days": oldest_age_days,
    }


def _source_for_field(row: dict[str, Any], field: str) -> tuple[str, date | None]:
    """Infer field provenance from feature_sources, otherwise QC snapshot."""
    for source_info in row.get("feature_sources") or []:
        filled_fields = set(source_info.get("filled_fields") or [])
        if field not in filled_fields:
            continue
        return (
            str(source_info.get("source") or "unknown"),
            _parse_date(source_info.get("trading_date")),
        )
    return "qc_snapshot", None


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
