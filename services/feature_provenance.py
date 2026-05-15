"""
Feature provenance helpers for market snapshots.

The execution path still trusts QC live portfolio state for weights. These
helpers annotate research fields so downstream agents can see source and
freshness before relying on a signal.
"""
from __future__ import annotations

from datetime import date, datetime
from typing import Any


PROVENANCE_FIELDS = {
    "price",
    "close_price",
    "open_price",
    "high_price",
    "low_price",
    "volume",
    "dollar_volume",
    "daily_return_pct",
    "return_1d",
    "return_5d",
    "return_20d",
    "return_60d",
    "return_252d",
    "mom_20d",
    "mom_60d",
    "mom_252d",
    "sma_20",
    "sma_50",
    "sma_200",
    "rsi_14",
    "atr_pct",
    "bb_position",
    "hist_vol_20d",
    "beta_vs_spy",
    "weight_current",
    "weight_target",
    "weight_drift",
    "unrealized_pnl_pct",
    "holding_days",
}


def annotate_snapshot_row_provenance(
    row: dict[str, Any],
    *,
    source: str,
    as_of: Any = None,
    trading_date: Any = None,
    fields: set[str] | None = None,
) -> dict[str, Any]:
    """Return row with a feature_sources entry for present research fields."""
    out = dict(row)
    present_fields = sorted(
        field
        for field in (fields or PROVENANCE_FIELDS)
        if out.get(field) is not None
    )
    if not present_fields:
        return out

    sources = list(out.get("feature_sources") or [])
    sources.append({
        "source": source,
        "filled_fields": present_fields,
        "as_of": _iso_or_none(as_of),
        "trading_date": _date_iso_or_none(trading_date or as_of),
    })
    out["feature_sources"] = _dedupe_source_entries(sources)
    return out


def merge_feature_sources(*rows: dict[str, Any]) -> list[dict[str, Any]]:
    """Combine feature_sources from rows while preserving source order."""
    entries: list[dict[str, Any]] = []
    for row in rows:
        entries.extend(row.get("feature_sources") or [])
    return _dedupe_source_entries(entries)


def summarize_feature_provenance(
    holdings: list[dict[str, Any]],
    *,
    as_of: date | None = None,
    stale_after_days: int = 5,
) -> dict[str, Any]:
    """Build a compact agent-facing source/freshness summary."""
    as_of_date = as_of or date.today()
    source_counts: dict[str, int] = {}
    stale_fields: dict[str, list[str]] = {}
    yfinance_filled_fields: dict[str, list[str]] = {}

    for row in holdings:
        ticker = (row.get("ticker") or "").upper().strip()
        if not ticker:
            continue
        for source_info in row.get("feature_sources") or []:
            source = str(source_info.get("source") or "unknown")
            fields = sorted(set(source_info.get("filled_fields") or []))
            if not fields:
                continue
            source_counts[source] = source_counts.get(source, 0) + len(fields)
            source_date = _parse_date(source_info.get("trading_date") or source_info.get("as_of"))
            if source == "yfinance":
                yfinance_filled_fields.setdefault(ticker, [])
                yfinance_filled_fields[ticker] = sorted(set(yfinance_filled_fields[ticker]) | set(fields))
            if source_date is not None and (as_of_date - source_date).days > stale_after_days:
                stale_fields.setdefault(ticker, [])
                stale_fields[ticker] = sorted(set(stale_fields[ticker]) | set(fields))

    return {
        "as_of": as_of_date.isoformat(),
        "stale_after_days": stale_after_days,
        "source_counts": source_counts,
        "yfinance_filled_fields": yfinance_filled_fields,
        "stale_fields": stale_fields,
        "has_stale_fields": bool(stale_fields),
    }


def _dedupe_source_entries(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_key: dict[tuple[str, str | None, str | None], dict[str, Any]] = {}
    for entry in entries:
        source = str(entry.get("source") or "unknown")
        as_of = _iso_or_none(entry.get("as_of"))
        trading_date = _date_iso_or_none(entry.get("trading_date"))
        key = (source, as_of, trading_date)
        fields = sorted(set(entry.get("filled_fields") or []))
        existing = by_key.get(key)
        if existing:
            existing["filled_fields"] = sorted(set(existing.get("filled_fields") or []) | set(fields))
        else:
            by_key[key] = {
                "source": source,
                "filled_fields": fields,
                "as_of": as_of,
                "trading_date": trading_date,
            }
    return list(by_key.values())


def _iso_or_none(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _date_iso_or_none(value: Any) -> str | None:
    parsed = _parse_date(value)
    return parsed.isoformat() if parsed else None


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
