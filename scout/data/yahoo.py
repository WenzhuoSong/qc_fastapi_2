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


# ── Latest quotes incl. pre-market / overnight futures (for the pre-market brief) ──

def quote_from(intraday: pd.Series, daily: pd.Series, session: date) -> dict | None:
    """Last 5-minute print vs the previous session's close.

    `intraday` is indexed by tz-aware timestamps (pre/post-market included),
    `daily` by date. A last print older than `session` is marked stale (e.g. a
    cash index that has not traded yet today) and gets no change.
    """
    intraday, daily = intraday.dropna(), daily.dropna()
    if intraday.empty:
        return None
    ts = pd.Timestamp(intraday.index[-1])
    ts = ts.tz_convert("America/New_York") if ts.tzinfo else ts.tz_localize("UTC").tz_convert("America/New_York")
    prior = daily[[pd.Timestamp(d).date() < session for d in daily.index]]
    last = float(intraday.iloc[-1])
    prev = float(prior.iloc[-1]) if len(prior) else None
    stale = ts.date() < session
    return {"last": last, "prev_close": prev, "change": None if stale or not prev else last / prev - 1,
            "time_et": ts.strftime("%m-%d %H:%M"), "stale": stale}


def _quotes(symbols: list[str], session: date) -> dict[str, dict]:
    import yfinance as yf

    kw = dict(group_by="ticker", auto_adjust=False, progress=False, threads=True)
    intraday = yf.download(symbols, period="5d", interval="5m", prepost=True, **kw)
    daily = yf.download(symbols, period="10d", interval="1d", **kw)
    out = {}
    for s in symbols:
        try:
            q = quote_from(intraday[s]["Close"], daily[s]["Close"], session)
        except KeyError:
            continue
        if q:
            out[s] = q
    return out


async def latest_quotes(symbols: list[str], session: date) -> dict[str, dict]:
    """Best effort: an empty dict when Yahoo fails, never an exception."""
    if not symbols:
        return {}
    try:
        return await asyncio.to_thread(_quotes, sorted(set(symbols)) + ["SPY"], session)
    except Exception as exc:
        logger.warning("yahoo quotes failed: %s", exc)
        return {}


# ── One live quote for a page view (cached briefly) ──
_LIVE_TTL_S = 60
_live_cache: dict[str, tuple[float, dict | None]] = {}


def session_label(time_et: str) -> str:
    """'MM-DD HH:MM' (US Eastern) → 盘前 / 盘中 / 盘后."""
    hm = time_et[-5:]
    return "盘前" if hm < "09:30" else "盘中" if hm < "16:00" else "盘后"


async def live_quote(ticker: str) -> dict | None:
    """Latest print incl. pre/post market vs the previous session's close; None when unavailable."""
    import time

    from scout.market_calendar import is_us_equity_trading_day, previous_us_equity_trading_day, us_equity_market_status

    hit = _live_cache.get(ticker)
    if hit and time.monotonic() - hit[0] < _LIVE_TTL_S:
        return hit[1]
    today = date.fromisoformat(us_equity_market_status().market_date)
    session = today if is_us_equity_trading_day(today) else previous_us_equity_trading_day(today)
    q = (await latest_quotes([ticker], session)).get(ticker)
    if q and not q["stale"]:
        q = {**q, "session": session_label(q["time_et"])}
    else:
        q = None
    _live_cache[ticker] = (time.monotonic(), q)
    return q
