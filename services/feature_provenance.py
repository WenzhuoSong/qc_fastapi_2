"""
Feature provenance helpers for market snapshots.

The execution path still trusts QC live portfolio state for weights. These
helpers annotate research fields so downstream agents can see source and
freshness before relying on a signal.
"""
from __future__ import annotations

from datetime import date, datetime
from typing import Any

from services.feature_authority import authority_for_field, canonical_field_name


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
    "last_price",
    "intraday_open_price",
    "intraday_high_price",
    "intraday_low_price",
    "intraday_volume",
    "intraday_return_pct",
    "last_trade_time",
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
    authority_map = {
        field: authority_for_field(field, source).value
        for field in present_fields
    }
    canonical_aliases = {
        field: canonical
        for field in present_fields
        if (canonical := canonical_field_name(field)) != field
    }
    sources.append({
        "source": source,
        "filled_fields": present_fields,
        "authority": _dominant_authority(authority_map),
        "authority_by_field": authority_map,
        "canonical_aliases": canonical_aliases,
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
        "authority_counts": _authority_counts(holdings),
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
            existing["authority_by_field"] = {
                **(existing.get("authority_by_field") or {}),
                **(entry.get("authority_by_field") or {}),
            }
            existing["canonical_aliases"] = {
                **(existing.get("canonical_aliases") or {}),
                **(entry.get("canonical_aliases") or {}),
            }
            existing["authority"] = _dominant_authority(existing.get("authority_by_field") or {})
        else:
            by_key[key] = {
                "source": source,
                "filled_fields": fields,
                "authority": entry.get("authority") or _dominant_authority(entry.get("authority_by_field") or {}),
                "authority_by_field": dict(entry.get("authority_by_field") or {}),
                "canonical_aliases": dict(entry.get("canonical_aliases") or {}),
                "as_of": as_of,
                "trading_date": trading_date,
            }
    return list(by_key.values())


def _authority_counts(holdings: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in holdings:
        for source_info in row.get("feature_sources") or []:
            by_field = source_info.get("authority_by_field") or {}
            if by_field:
                for authority in by_field.values():
                    key = str(authority or "unknown")
                    counts[key] = counts.get(key, 0) + 1
                continue
            fields = list(source_info.get("filled_fields") or [])
            if fields:
                source = str(source_info.get("source") or "unknown")
                for field in fields:
                    key = authority_for_field(field, source).value
                    counts[key] = counts.get(key, 0) + 1
                continue
            key = str(source_info.get("authority") or "unknown")
            counts[key] = counts.get(key, 0) + 1
    return counts


def _dominant_authority(authority_by_field: dict[str, Any]) -> str:
    values = [str(value or "unknown") for value in authority_by_field.values()]
    if not values:
        return "unknown"
    priority = {
        "live_state": 0,
        "intraday": 1,
        "daily_research": 2,
        "qc_eod_audit": 3,
        "legacy_debug": 4,
        "unknown": 5,
    }
    return sorted(values, key=lambda item: priority.get(item, 9))[0]


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
