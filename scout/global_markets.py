"""Markets outside the US for the market read and the pre-market brief (user's request 2026-11-25: many large US days
start abroad — the yen carry unwind, Chinese AI news, European banks). Read-only Yahoo data.

Each market's own daily closes, dated by its local session: Asia has closed before New York opens, Europe is still
trading during the US pre-market. Yahoo leaves the latest Asian daily close empty for hours after the close (high
and low are there), so a missing last close is filled from the last 5-minute print of that local session.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import date

import pandas as pd

logger = logging.getLogger("scout.global_markets")

# (symbol, name, region, meaning); regions: 亚洲 closes before New York opens, 欧洲 overlaps the US morning
INDICES = (("^N225", "日经 225", "亚洲", "日本大盘；日元走弱时通常上涨"), ("^HSI", "恒生指数", "亚洲", "香港，含大量中国科技公司"),
           ("000001.SS", "上证指数", "亚洲", "中国 A 股大盘"), ("^STOXX50E", "欧洲斯托克 50", "欧洲", "欧元区大公司"),
           ("^GDAXI", "德国 DAX", "欧洲", "德国大公司，出口和工业比重高"))
OTHERS = (("JPY=X", "美元兑日元", "汇率", "上升 = 日元走弱；日元急升常伴随全球套息交易平仓、风险资产下跌"),
          ("CNY=X", "美元兑人民币", "汇率", "上升 = 人民币走弱"),
          ("HG=F", "铜", "商品", "工业需求的温度计，对全球经济预期敏感"))
ALL = INDICES + OTHERS


def _download(symbols: list[str], start: date) -> tuple[pd.DataFrame, pd.DataFrame]:
    import yfinance as yf

    kw = dict(group_by="ticker", auto_adjust=False, progress=False, threads=True)
    return yf.download(symbols, start=start, interval="1d", **kw), yf.download(symbols, period="5d", interval="5m", **kw)


def fill_last(daily: pd.DataFrame, intraday: pd.Series) -> pd.Series:
    """Daily closes indexed by local date; an empty last close (with a high) filled from that day's last 5-minute print."""
    close = daily["Close"].copy()
    close.index = pd.to_datetime(close.index).date
    if len(close) and pd.isna(close.iloc[-1]) and pd.notna(daily["High"].iloc[-1]):
        day = close.index[-1]
        ticks = intraday.dropna()
        ticks = ticks[[pd.Timestamp(t).date() == day for t in ticks.index]]
        if len(ticks):
            close.iloc[-1] = float(ticks.iloc[-1])
    return close.dropna()


async def closes(start: date, symbols: tuple = ALL) -> dict[str, pd.Series]:
    syms = [s for s, *_ in symbols]
    try:
        daily, intraday = await asyncio.to_thread(_download, syms, start)
    except Exception as exc:
        logger.warning("global markets failed: %s", exc)
        return {}
    out = {}
    for s in syms:
        try:
            out[s] = fill_last(daily[s], intraday[s]["Close"] if s in intraday.columns.get_level_values(0) else pd.Series(dtype=float))
        except KeyError:
            continue
    return out
