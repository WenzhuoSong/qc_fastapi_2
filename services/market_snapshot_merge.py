"""Market snapshot normalization and merge helpers."""
from __future__ import annotations

from typing import Any

from services.feature_authority import (
    DAILY_RESEARCH_FIELDS,
    INTRADAY_FIELDS,
    LIVE_STATE_FIELDS,
    canonical_field_name,
    legacy_debug_namespace,
)
from services.feature_authority_mode import YFINANCE_RESEARCH, normalize_feature_authority_mode
from services.feature_provenance import (
    annotate_snapshot_row_provenance,
    merge_feature_sources,
)


def normalize_feature_snapshot(snapshot: dict[str, Any]) -> dict[str, Any]:
    """Use features as holdings when only a daily feature snapshot exists."""
    out = dict(snapshot)
    if not out.get("holdings") and out.get("features"):
        as_of = out.get("timestamp_utc") or out.get("received_at") or out.get("trading_date")
        trading_date = out.get("trading_date") or as_of
        out["holdings"] = [
            _merge_ticker_rows(
                {},
                annotate_snapshot_row_provenance(
                    row,
                    source="qc_daily_snapshot",
                    as_of=as_of,
                    trading_date=trading_date,
                ),
                {},
            )
            for row in (out.get("features") or [])
        ]
        out["schema_capabilities"] = {
            "heartbeat_schema_version": "missing",
            "intraday_live_state": "unavailable",
            "daily_research_authority": "qc_daily_fallback",
        }
    return out


def merge_market_snapshots(
    heartbeat: dict[str, Any],
    feature_snapshot: dict[str, Any],
    yfinance_feature_map: dict[str, dict[str, Any]] | None = None,
    *,
    mode: str = YFINANCE_RESEARCH,
) -> dict[str, Any]:
    """Merge live heartbeat state with QC EOD audit and yfinance research fields."""
    mode = normalize_feature_authority_mode(mode)
    return _with_mode(_merge_yfinance_research(heartbeat, feature_snapshot, yfinance_feature_map), mode)


def _merge_yfinance_research(
    heartbeat: dict[str, Any],
    feature_snapshot: dict[str, Any],
    yfinance_feature_map: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    merged = dict(heartbeat)
    heartbeat_as_of = heartbeat.get("timestamp_utc") or heartbeat.get("received_at") or heartbeat.get("trading_date")
    heartbeat_trading_date = heartbeat.get("trading_date") or heartbeat_as_of
    feature_as_of = feature_snapshot.get("timestamp_utc") or feature_snapshot.get("received_at") or feature_snapshot.get("trading_date")
    feature_trading_date = feature_snapshot.get("trading_date") or feature_as_of
    feature_rows = feature_snapshot.get("features") or feature_snapshot.get("holdings") or []
    features_by_ticker = {
        (row.get("ticker") or "").upper().strip(): annotate_snapshot_row_provenance(
            row,
            source="qc_daily_snapshot",
            as_of=feature_as_of,
            trading_date=feature_trading_date,
        )
        for row in feature_rows
        if row.get("ticker")
    }
    yfinance_by_ticker = {}
    stale_yfinance_by_ticker = {}
    for row in (yfinance_feature_map or {}).values():
        if not row.get("ticker"):
            continue
        ticker = (row.get("ticker") or "").upper().strip()
        annotated = annotate_snapshot_row_provenance(
            row,
            source="yfinance",
            as_of=row.get("trading_date") or row.get("updated_at") or row.get("created_at"),
            trading_date=row.get("trading_date"),
        )
        if _is_stale_yfinance_row(row):
            stale_yfinance_by_ticker[ticker] = annotated
        else:
            yfinance_by_ticker[ticker] = annotated

    enriched_holdings = []
    for row in heartbeat.get("holdings") or []:
        ticker = (row.get("ticker") or "").upper().strip()
        feature_row = features_by_ticker.get(ticker) or {}
        yfinance_row = yfinance_by_ticker.get(ticker) or {}
        live_row = annotate_snapshot_row_provenance(
            row,
            source="qc_heartbeat",
            as_of=heartbeat_as_of,
            trading_date=heartbeat_trading_date,
        )
        enriched_holdings.append(_merge_ticker_rows(live_row, feature_row, yfinance_row))

    heartbeat_tickers = {
        (row.get("ticker") or "").upper().strip()
        for row in heartbeat.get("holdings") or []
    }
    remaining_tickers = sorted((set(features_by_ticker) | set(yfinance_by_ticker)) - heartbeat_tickers)
    for ticker in remaining_tickers:
        feature_row = features_by_ticker.get(ticker) or {}
        yfinance_row = yfinance_by_ticker.get(ticker) or {}
        if ticker and ticker not in heartbeat_tickers:
            enriched_holdings.append(_merge_ticker_rows({}, feature_row, yfinance_row))

    merged["holdings"] = enriched_holdings
    merged["latest_feature_snapshot_at"] = feature_snapshot.get("timestamp_utc")
    merged["schema_capabilities"] = _schema_capabilities(
        heartbeat,
        has_yfinance=bool(yfinance_by_ticker),
        has_qc_daily=bool(features_by_ticker),
    )
    if stale_yfinance_by_ticker:
        merged["stale_yfinance_tickers"] = sorted(stale_yfinance_by_ticker)
    return merged


def _with_mode(snapshot: dict[str, Any], mode: str) -> dict[str, Any]:
    snapshot["feature_authority_mode"] = mode
    return snapshot


def _merge_ticker_rows(
    live_row: dict[str, Any],
    qc_daily_row: dict[str, Any],
    yfinance_row: dict[str, Any],
) -> dict[str, Any]:
    ticker = live_row.get("ticker") or qc_daily_row.get("ticker") or yfinance_row.get("ticker")
    merged: dict[str, Any] = {"ticker": str(ticker).upper().strip()} if ticker else {}

    _copy_fields(merged, qc_daily_row, DAILY_RESEARCH_FIELDS, overwrite=False)
    _copy_canonical_aliases(merged, qc_daily_row, overwrite=False)
    _copy_fields(merged, yfinance_row, DAILY_RESEARCH_FIELDS, overwrite=True)
    _copy_canonical_aliases(merged, yfinance_row, overwrite=True)
    _copy_fields(merged, live_row, LIVE_STATE_FIELDS | INTRADAY_FIELDS, overwrite=True)
    if live_row.get("price") is not None:
        merged["price"] = live_row.get("price")

    legacy = {}
    for row in (qc_daily_row, live_row):
        legacy.update(legacy_debug_namespace(row))
    if legacy:
        merged["legacy_qc_indicators"] = legacy

    merged["feature_sources"] = merge_feature_sources(qc_daily_row, yfinance_row, live_row)
    return {key: value for key, value in merged.items() if value is not None}


def _copy_fields(
    target: dict[str, Any],
    source: dict[str, Any],
    fields: frozenset[str] | set[str],
    *,
    overwrite: bool,
) -> None:
    for field in sorted(fields):
        if source.get(field) is None:
            continue
        if overwrite or field not in target:
            target[field] = source.get(field)


def _copy_canonical_aliases(target: dict[str, Any], source: dict[str, Any], *, overwrite: bool) -> None:
    for field, value in source.items():
        canonical = canonical_field_name(field)
        if canonical == field or value is None or canonical not in DAILY_RESEARCH_FIELDS:
            continue
        if overwrite or canonical not in target:
            target[canonical] = value


def _schema_capabilities(heartbeat: dict[str, Any], *, has_yfinance: bool, has_qc_daily: bool) -> dict[str, Any]:
    version = str(heartbeat.get("schema_version") or "legacy")
    live_rows = heartbeat.get("holdings") or []
    has_intraday = any(
        any(row.get(field) is not None for field in INTRADAY_FIELDS)
        for row in live_rows
    )
    has_price = any(row.get("price") is not None for row in live_rows)
    if has_intraday:
        intraday = "available"
    elif has_price:
        intraday = "partial"
    else:
        intraday = "unavailable"
    if has_yfinance:
        research = "yfinance"
    elif has_qc_daily:
        research = "qc_daily_fallback"
    else:
        research = "missing"
    return {
        "heartbeat_schema_version": version,
        "intraday_live_state": intraday,
        "daily_research_authority": research,
    }


def _is_stale_yfinance_row(row: dict[str, Any]) -> bool:
    return bool(row.get("is_stale")) or str(row.get("data_quality_flag") or "").lower() == "stale"
