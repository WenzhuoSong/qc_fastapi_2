"""Daily bar storage: backfill, incremental refresh with split detection, load."""
from __future__ import annotations

import logging
from datetime import date, timedelta

import pandas as pd

from scout import db
from scout.data.provider import BAR_COLUMNS, MarketDataProvider

logger = logging.getLogger("scout.bars")

INCREMENTAL_DAYS = 10
# An overlapping close that moved this much means the vendor re-adjusted
# history (split or large dividend); the whole series is then refetched.
READJUST_TOLERANCE = 0.02

_UPSERT = """
INSERT INTO scout.bars_daily (ticker, date, open, high, low, close, volume)
VALUES (:ticker, :date, :open, :high, :low, :close, :volume)
ON CONFLICT (ticker, date) DO UPDATE SET
    open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
    close = EXCLUDED.close, volume = EXCLUDED.volume
"""


def _rows(ticker: str, frame: pd.DataFrame):
    for d, r in frame.iterrows():
        yield {
            "ticker": ticker, "date": d, "open": r["open"], "high": r["high"],
            "low": r["low"], "close": r["close"], "volume": r["volume"],
        }


async def store(frames: dict[str, pd.DataFrame], replace: bool = False) -> int:
    if replace and frames:
        await db.execute(
            "DELETE FROM scout.bars_daily WHERE ticker = ANY(:t)", {"t": list(frames)}
        )
    # One session for all tickers (per-ticker transactions cost a round trip each), committed
    # per chunk so a full backfill never holds its locks for minutes.
    return await db.execute_many(
        _UPSERT, (row for ticker, frame in frames.items() for row in _rows(ticker, frame)), atomic=False
    )


async def backfill(provider: MarketDataProvider, tickers: list[str], years: int, replace: bool = True) -> dict:
    """Full history. `replace` first deletes the stored series (a re-adjusted vendor series must not
    keep stale dates); without it every returned date is upserted, so pages never see a gap."""
    start = date.today() - timedelta(days=int(365.25 * years) + 10)
    frames = await provider.daily_bars(tickers, start)
    written = await store(frames, replace=replace)
    return {"requested": len(tickers), "received": len(frames), "rows": written}


def needs_readjust(stored_close: dict[date, float], fresh: pd.DataFrame) -> bool:
    for d, close in fresh["close"].items():
        old = stored_close.get(d)
        if old and abs(close / old - 1) > READJUST_TOLERANCE:
            return True
    return False


async def refresh(provider: MarketDataProvider, tickers: list[str], years: int) -> dict:
    """Append recent bars; refetch full history for new or re-adjusted tickers."""
    latest = await db.fetch_all(
        "SELECT ticker, max(date) AS last FROM scout.bars_daily WHERE ticker = ANY(:t) GROUP BY ticker",
        {"t": tickers},
    )
    have = {r["ticker"]: r["last"] for r in latest}
    missing = [t for t in tickers if t not in have]

    start = date.today() - timedelta(days=INCREMENTAL_DAYS)
    fresh = await provider.daily_bars([t for t in tickers if t in have], start)
    overlap = await db.fetch_all(
        "SELECT ticker, date, close FROM scout.bars_daily WHERE ticker = ANY(:t) AND date >= :d",
        {"t": list(fresh), "d": start},
    )
    stored: dict[str, dict[date, float]] = {}
    for r in overlap:
        # The newest stored bar may have been written mid-session (a partial bar): its close
        # differing from the final one is not a vendor re-adjustment.
        if r["date"] < have[r["ticker"]]:
            stored.setdefault(r["ticker"], {})[r["date"]] = r["close"]

    readjust = [t for t, f in fresh.items() if needs_readjust(stored.get(t, {}), f)]
    incremental = {t: f for t, f in fresh.items() if t not in readjust}
    rows = await store(incremental)

    full_summary = {"rows": 0}
    if missing or readjust:
        logger.info("full refetch: %d new, %d re-adjusted", len(missing), len(readjust))
        full_summary = await backfill(provider, missing + readjust, years)
    return {
        "incremental_tickers": len(incremental),
        "incremental_rows": rows,
        "new_tickers": len(missing),
        "readjusted": readjust[:50],
        "full_rows": full_summary["rows"],
    }


async def load(tickers: list[str] | None = None, since: date | None = None) -> dict[str, pd.DataFrame]:
    where, params = [], {}
    if tickers is not None:
        where.append("ticker = ANY(:t)")
        params["t"] = tickers
    if since is not None:
        where.append("date >= :d")
        params["d"] = since
    sql = "SELECT ticker, date, open, high, low, close, volume FROM scout.bars_daily"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY ticker, date"
    rows = await db.fetch_all(sql, params)
    if not rows:
        return {}
    df = pd.DataFrame(rows)
    return {
        t: g.set_index("date")[BAR_COLUMNS].astype(float)
        for t, g in df.groupby("ticker", sort=False)
    }
