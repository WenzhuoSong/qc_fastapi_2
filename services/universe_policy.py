"""Shared ETF universe policy for research and allocation layers."""
from __future__ import annotations

from typing import Any


HEDGE_RESEARCH_TICKERS = {
    # Leveraged / inverse / volatility products: useful as stress indicators,
    # and tightly capped hedges, not eligible for ordinary strategy scoring.
    "TQQQ", "SQQQ", "SOXL", "SOXS", "SPXL", "SPXS", "UVXY", "VIXY",
    "SH", "PSQ", "RWM", "DOG", "MYY", "SBB", "SEF", "REK", "EUM", "EFZ", "YXI",
    "SJB", "TBF", "TBX",
}


def is_tradable_research_row(row: dict[str, Any]) -> bool:
    ticker = (row.get("ticker") or "").upper().strip()
    if not ticker or ticker == "CASH":
        return False
    if ticker in HEDGE_RESEARCH_TICKERS:
        return False
    if str(row.get("universe_role") or "").lower().strip() in {"watchlist", "hedge"}:
        return False
    return True


def filter_tradable_research_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Rows eligible for strategy scoring and consensus weights."""
    return [row for row in rows if is_tradable_research_row(row)]
