"""Market snapshot normalization and merge helpers."""
from __future__ import annotations

from typing import Any

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
            annotate_snapshot_row_provenance(
                row,
                source="qc_daily_snapshot",
                as_of=as_of,
                trading_date=trading_date,
            )
            for row in (out.get("features") or [])
        ]
    return out


def merge_market_snapshots(
    heartbeat: dict[str, Any],
    feature_snapshot: dict[str, Any],
) -> dict[str, Any]:
    """Overlay richer daily feature fields onto live heartbeat holdings."""
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

    enriched_holdings = []
    for row in heartbeat.get("holdings") or []:
        ticker = (row.get("ticker") or "").upper().strip()
        feature_row = features_by_ticker.get(ticker) or {}
        live_row = annotate_snapshot_row_provenance(
            row,
            source="qc_heartbeat",
            as_of=heartbeat_as_of,
            trading_date=heartbeat_trading_date,
        )
        merged_row = {**feature_row, **live_row}
        merged_row["feature_sources"] = merge_feature_sources(feature_row, live_row)
        enriched_holdings.append(merged_row)

    heartbeat_tickers = {
        (row.get("ticker") or "").upper().strip()
        for row in heartbeat.get("holdings") or []
    }
    for ticker, feature_row in features_by_ticker.items():
        if ticker and ticker not in heartbeat_tickers:
            enriched_holdings.append(feature_row)

    merged["holdings"] = enriched_holdings
    merged["latest_feature_snapshot_at"] = feature_snapshot.get("timestamp_utc")
    return merged


# Backward-compatible aliases for existing focused tests/imports.
_normalize_feature_snapshot = normalize_feature_snapshot
_merge_market_snapshots = merge_market_snapshots
