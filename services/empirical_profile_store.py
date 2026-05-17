"""
Feature-store adapter for empirical ETF profiles.

Reads yfinance rows from market_daily_features and builds derived profiles. This
module owns DB access; empirical_profiles.py remains pure computation.
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from services.empirical_profiles import build_empirical_profiles


DEFAULT_LOOKBACK_DAYS = 420
DEFAULT_SOURCE = "yfinance"


async def build_empirical_profiles_from_feature_store(
    db: Any,
    *,
    tickers: list[str],
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    source: str = DEFAULT_SOURCE,
    benchmark_ticker: str = "SPY",
) -> dict[str, dict[str, Any]]:
    """Read feature-store rows and build empirical profiles for requested tickers."""
    clean_tickers = _clean_tickers(tickers)
    if not clean_tickers:
        return {}

    read_tickers = sorted(set(clean_tickers) | {benchmark_ticker.upper()})
    start_date = date.today() - timedelta(days=max(int(lookback_days), 30))
    from services.market_feature_store import get_market_daily_feature_rows

    rows = await get_market_daily_feature_rows(
        db,
        tickers=read_tickers,
        start_date=start_date,
        source=source,
        limit=max(len(read_tickers) * max(int(lookback_days), 30), 100),
    )
    return build_empirical_profiles(
        rows,
        tickers=clean_tickers,
        source=source,
        lookback_days=lookback_days,
        benchmark_ticker=benchmark_ticker.upper(),
    )


def collect_empirical_profile_tickers(
    *,
    brief: dict[str, Any] | None,
    quant_baseline: dict[str, Any] | None,
    playground_bundle: dict[str, Any] | None,
) -> list[str]:
    """Collect current profile universe from holdings, quant, and playground."""
    brief = brief or {}
    quant = quant_baseline or {}
    playground = playground_bundle or {}
    tickers: list[str] = []

    tickers.extend((brief.get("current_weights") or {}).keys())
    for row in brief.get("holdings") or []:
        if isinstance(row, dict):
            tickers.append(str(row.get("ticker") or row.get("symbol") or ""))
    tickers.extend((quant.get("base_weights") or {}).keys())
    tickers.extend(str(ticker) for ticker in quant.get("selected_tickers") or [])
    consensus_weights = playground.get("consensus_weights") or {}
    if isinstance(consensus_weights, dict):
        tickers.extend(consensus_weights.keys())
    for strategy in playground.get("strategies") or []:
        if isinstance(strategy, dict):
            tickers.extend(strategy.get("selected_tickers") or [])

    return _clean_tickers(tickers)


def _clean_tickers(tickers) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for ticker in tickers or []:
        clean = str(ticker or "").upper().strip()
        if not clean or clean == "CASH" or clean in seen:
            continue
        seen.add(clean)
        out.append(clean)
    return out
