"""Market sentiment and positioning, described against its own history (phase 2 of the market read, user's request
2026-11-25): put/call ratios (CBOE, from 2019-10), the AAII survey (from 1987), VIX and its term structure (Yahoo,
from 2006), net new 52-week highs and the share of stocks above their 50-day average (Scout's bars).

Each reading says where today stands in its history (the share of past days at or below it) and calls the outer
tenths 偏恐慌 / 偏乐观. Descriptive only: Scout's own tests found no edge in such readings for the next week's direction
(knowledge entries `weekly_direction_v1`, `weekly_up_v2`, `risk_warning_market`), so the text never turns them into a
call. Stored in `scout.macro_series` (series SNT_*); the research job keeps them current (CBOE backfills
CBOE_PER_RUN sessions a run until it reaches 2019-10).
"""
from __future__ import annotations

import asyncio
import logging
from datetime import date, timedelta

import httpx
import pandas as pd

from scout import db
from scout.data import sentiment as src

logger = logging.getLogger("scout.sentiment")

CBOE_PER_RUN = 400
CBOE_PACING_S = 0.2
AAII_STALE_DAYS = 6
FRESH_DAYS = 7                      # a reading older than this is not today's
EXTREME = 0.1                       # the outer tenth of the history counts as extreme
PC_AVG = 10                         # sessions averaged for the put/call reading (single days are noisy)
VIX_START = date(2006, 7, 17)       # ^VIX3M history on Yahoo starts here
NOTE = ("情绪读数只描述现在处在历史的什么位置；Scout 检验过，这类读数对下周涨跌没有预测力（知识库 weekly_direction_v1、weekly_up_v2）")


async def _last(series: str) -> date | None:
    row = await db.fetch_one("SELECT max(date) AS d FROM scout.macro_series WHERE series = :s", {"s": series})
    return (row or {}).get("d")


async def _store(rows: list[tuple[str, date, float]]) -> int:
    if not rows:
        return 0
    return await db.execute_many(
        "INSERT INTO scout.macro_series (series, date, value) VALUES (:s, :d, :v) ON CONFLICT (series, date) DO UPDATE SET value = EXCLUDED.value",
        [{"s": s, "d": d, "v": v} for s, d, v in rows])


async def refresh(today: date) -> dict:
    """New CBOE sessions (at most CBOE_PER_RUN, oldest first) and the AAII survey when a week old."""
    from scout.market_calendar import is_us_equity_trading_day

    out: dict = {}
    last = await _last("SNT_PC_EQUITY")
    d = (last + timedelta(days=1)) if last else src.CBOE_START
    days = []
    while d <= today and len(days) < CBOE_PER_RUN:
        if is_us_equity_trading_day(d):
            days.append(d)
        d += timedelta(days=1)
    rows = []
    async with httpx.AsyncClient(timeout=20) as client:
        for d in days:
            r = await src.cboe_day(client, d)
            if r:
                rows += [(f"SNT_PC_{k.upper()}", d, v) for k, v in r.items()]
            await asyncio.sleep(CBOE_PACING_S)
    out["cboe"] = {"requested": len(days), "stored": await _store(rows)}
    last = await _last("SNT_AAII_BULL")
    if not last or (today - last).days >= AAII_STALE_DAYS:
        survey = [x for x in await src.aaii() if not last or x[0] > last]
        out["aaii"] = await _store([(s, d, v) for d, b, n, r in survey
                                    for s, v in (("SNT_AAII_BULL", b), ("SNT_AAII_NEUTRAL", n), ("SNT_AAII_BEAR", r))])
    return out


# ── readings ──

def position(history: pd.Series, value: float) -> float:
    """Share of the history at or below `value`."""
    h = history.dropna()
    return float((h <= value).mean()) if len(h) else float("nan")


def tone(pos: float, fear_high: bool) -> str:
    """偏恐慌 / 偏乐观 / 正常范围 from a history position; `fear_high`: a high reading means fear (put/call, VIX)."""
    if pos != pos:
        return "历史不足"
    if pos >= 1 - EXTREME:
        return "偏恐慌" if fear_high else "偏乐观"
    if pos <= EXTREME:
        return "偏乐观" if fear_high else "偏恐慌"
    return "正常范围"


def item(key: str, name: str, value: str, pos: float, since: date, fear_high: bool, meaning: str) -> dict:
    return {"key": key, "名称": name, "数值": value, "历史位置": f"高于 {since.year} 年以来 {pos * 100:.0f}% 的时间" if pos == pos else "历史不足",
            "读数": tone(pos, fear_high), "含义": meaning}


async def _series(name: str, until: date) -> pd.Series:
    rows = await db.fetch_all("SELECT date, value FROM scout.macro_series WHERE series = :s AND date <= :d ORDER BY date",
                              {"s": name, "d": until})
    return pd.Series({r["date"]: r["value"] for r in rows}, dtype=float)


BREADTH_YEARS = 5


def breadth_series(close: pd.DataFrame, liquid: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    """Daily (net new 252-session highs − lows) / liquid stocks, and the share above the 50-day average."""
    hi, lo = close.rolling(252, min_periods=200).max(), close.rolling(252, min_periods=200).min()
    n = liquid.sum(axis=1).replace(0, float("nan"))
    net = (((close >= hi) & liquid).sum(axis=1) - ((close <= lo) & liquid).sum(axis=1)) / n
    above = ((close > close.rolling(50).mean()) & liquid).sum(axis=1) / n
    valid = hi.notna().sum(axis=1) > 0.5 * close.shape[1]
    return net[valid].dropna(), above[valid].dropna()


async def breadth_history(asof: date) -> tuple[pd.Series, pd.Series]:
    """From the stored bars of today's stocks (survivors: a description, not a test), BREADTH_YEARS back; liquid =
    the scan's price and 20-day dollar-volume floors."""
    from scout.config import get_scout_settings

    st = get_scout_settings()
    rows = await db.fetch_all(
        "SELECT b.ticker, b.date, b.close, b.volume FROM scout.bars_daily b JOIN scout.instruments i USING (ticker) "
        "WHERE i.kind = 'stock' AND b.date BETWEEN :a AND :d", {"a": asof - timedelta(days=int(365.25 * BREADTH_YEARS) + 400), "d": asof})
    df = pd.DataFrame(rows)
    close = df.pivot(index="date", columns="ticker", values="close").sort_index()
    dvol = (df.assign(dv=df["close"] * df["volume"]).pivot(index="date", columns="ticker", values="dv").sort_index())
    liquid = (close >= st.scout_min_price) & (dvol.rolling(20, min_periods=10).mean() >= st.scout_min_dollar_volume)
    return breadth_series(close, liquid)


async def readings(asof: date) -> list[dict]:
    out = []
    pc = (await _series("SNT_PC_EQUITY", asof)).rolling(PC_AVG).mean().dropna()
    if len(pc) > 250 and (asof - pc.index[-1]).days <= FRESH_DAYS:      # still backfilling: the last value is years old
        out.append(item("snt.pc", f"个股期权看跌 / 看涨比（{PC_AVG} 日平均）", f"{pc.iloc[-1]:.2f}（{pc.index[-1]}）", position(pc, pc.iloc[-1]),
                        pc.index[0], True, "买看跌期权（保护或押注下跌）相对看涨期权的多少；高 = 投资者在买保护，偏谨慎"))
    bull, bear = await _series("SNT_AAII_BULL", asof), await _series("SNT_AAII_BEAR", asof)
    spread = (bull - bear).dropna()
    if len(spread) > 100 and (asof - spread.index[-1]).days <= FRESH_DAYS + 7:   # weekly survey
        out.append(item("snt.aaii", "AAII 散户调查：看多减看空", f"{spread.iloc[-1] * 100:+.0f} 个百分点（看多 {bull.iloc[-1] * 100:.0f}%、"
                        f"看空 {bear.iloc[-1] * 100:.0f}%，{spread.index[-1]}）", position(spread, spread.iloc[-1]), spread.index[0], False,
                        "美国个人投资者协会每周问会员对未来 6 个月的看法；看空远多于看多 = 散户悲观"))
    try:
        from scout.data.yahoo import YahooProvider

        bars = await YahooProvider().daily_bars(["^VIX", "^VIX3M"], VIX_START, asof)
    except Exception as exc:
        logger.warning("vix history failed: %s", exc)
        bars = {}
    vix, vix3 = (bars.get(s) for s in ("^VIX", "^VIX3M"))
    if vix is not None and len(vix) > 250:
        v = vix["close"]
        out.append(item("snt.vix", "VIX", f"{v.iloc[-1]:.1f}（{v.index[-1]}）", position(v, v.iloc[-1]), v.index[0], True,
                        "标普期权隐含的未来 30 天波动；高 = 市场紧张"))
        if vix3 is not None and len(vix3) > 250:
            ratio = (v / vix3["close"]).dropna()
            r = ratio.iloc[-1]
            out.append({**item("snt.vix_term", "VIX ÷ 三个月 VIX", f"{r:.2f}", position(ratio, r), ratio.index[0], True,
                               "低于 1 是常态（远期波动更高）；高于 1 = 期限倒挂，短期恐慌重于长期"),
                        "倒挂": bool(r > 1)})
    try:
        net, above = await breadth_history(asof)
        if len(net) > 120:
            out.append(item("snt.nhnl", "52 周新高减新低（占流动性足够的股票）", f"{net.iloc[-1] * 100:+.1f}%", position(net, net.iloc[-1]),
                            net.index[0], False, "创一年新高的股票减去创一年新低的；负 = 新低多于新高"))
            out.append(item("snt.above50", "站上 50 日均线的股票比例", f"{above.iloc[-1] * 100:.0f}%", position(above, above.iloc[-1]),
                            above.index[0], False, "低 = 多数股票在短期下跌趋势里"))
    except Exception as exc:
        logger.warning("breadth history failed: %s", exc)
    return out
