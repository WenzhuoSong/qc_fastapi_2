"""Shared ETF universe policy for research and allocation layers."""
from __future__ import annotations

from typing import Any


BLOCKED_RESEARCH_TICKERS = {
    # Leveraged / inverse / volatility products: useful as stress indicators,
    # not eligible for strategy weights in this system.
    "TQQQ", "SQQQ", "SOXL", "SOXS", "SPXL", "SPXS", "UVXY", "VIXY",
}


def is_tradable_research_row(row: dict[str, Any]) -> bool:
    ticker = (row.get("ticker") or "").upper().strip()
    if not ticker or ticker == "CASH":
        return False
    if ticker in BLOCKED_RESEARCH_TICKERS:
        return False
    if str(row.get("universe_role") or "").lower().strip() == "watchlist":
        return False
    return True


def filter_tradable_research_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Rows eligible for strategy scoring and consensus weights."""
    return [row for row in rows if is_tradable_research_row(row)]
