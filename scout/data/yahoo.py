"""Free daily bars via yfinance (best-effort; adequate for EOD scanning)."""
from __future__ import annotations

import asyncio
import logging
from datetime import date, timedelta

import pandas as pd

from scout.data.provider import BAR_COLUMNS

logger = logging.getLogger("scout.data.yahoo")

CHUNK = 150


class YahooProvider:
    name = "yahoo"

    async def daily_bars(
        self, tickers: list[str], start: date, end: date | None = None
    ) -> dict[str, pd.DataFrame]:
        out: dict[str, pd.DataFrame] = {}
        unique = sorted(set(tickers))
        for i in range(0, len(unique), CHUNK):
            chunk = unique[i : i + CHUNK]
            try:
                raw = await asyncio.to_thread(_download, chunk, start, end)
            except Exception as exc:
                logger.warning("yahoo chunk %d failed (%s); retrying once", i // CHUNK, exc)
                await asyncio.sleep(5)
                try:
                    raw = await asyncio.to_thread(_download, chunk, start, end)
                except Exception as exc2:
                    logger.error("yahoo chunk %d failed twice: %s", i // CHUNK, exc2)
                    continue
            out.update(split_download(raw, chunk))
        return out


def _download(tickers: list[str], start: date, end: date | None) -> pd.DataFrame:
    import yfinance as yf

    return yf.download(
        tickers,
        start=start.isoformat(),
        end=(end + timedelta(days=1)).isoformat() if end else None,
        group_by="ticker",
        auto_adjust=True,
        progress=False,
        threads=True,
    )


def split_download(raw: pd.DataFrame, tickers: list[str]) -> dict[str, pd.DataFrame]:
    out: dict[str, pd.DataFrame] = {}
    if raw is None or raw.empty:
        return out
    multi = isinstance(raw.columns, pd.MultiIndex)
    for ticker in tickers:
        if multi:
            if ticker not in raw.columns.get_level_values(0):
                continue
            frame = raw[ticker]
        else:
            frame = raw
        frame = frame.rename(columns=str.lower)
        if not set(BAR_COLUMNS).issubset(frame.columns):
            continue
        frame = frame[BAR_COLUMNS].dropna(subset=["close"])
        frame = frame[(frame["close"] > 0) & (frame["high"] >= frame["low"])]
        if frame.empty:
            continue
        frame.index = pd.to_datetime(frame.index).date
        out[ticker] = frame.sort_index().astype(float)
    return out
