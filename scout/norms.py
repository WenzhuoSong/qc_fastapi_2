"""Historical norms ('历史常态', user's request 2026-11-06): what usually happens over the next week, month and quarter.

For any ticker, from its own stored daily closes (Scout keeps about 12 years): over every past window of 5, 21
and 63 sessions, the share that ended up, the median return, the 10th / 90th percentiles and the share that
lost 5 % or more; plus the normal range for the same horizon from the last 63 sessions' volatility (±1σ, as
`position_risk.move_range`). Descriptive only, never a forecast: Scout's direction tests (weekly_direction_v1,
weekly_up_v2, sector_forecast_v1) could not beat these base rates, which mostly reflect the market's long drift.
Market-wide rows (SPY, sector and theme ETFs) are stored after each close as brief `norms`.
"""
from __future__ import annotations

import math
from datetime import date

import numpy as np
import pandas as pd

from scout import db, universe

HORIZONS = {5: "1 周", 21: "1 个月", 63: "3 个月"}
DEFAULT_H = 21
VOL_DAYS = 63
DROP = -0.05
MIN_WINDOWS = 250                         # about a year of daily windows before a share is shown
SOURCE_NOTE = "过去各个时间段的统计，不是预测：Scout 检验过用各种指标判断涨跌，都没能比这些历史比例更准"


def stats(close: pd.Series) -> dict | None:
    """Norms for one close series (daily, ascending). None when the history is too short."""
    c = close.dropna()
    if len(c) < VOL_DAYS + MIN_WINDOWS:
        return None
    lr = np.log(c).diff()
    vol = float(lr.iloc[-VOL_DAYS:].std() * math.sqrt(252))
    vol_long = float(lr.std() * math.sqrt(252))
    last = float(c.iloc[-1])
    out = {"asof": str(c.index[-1]), "close": last, "vol": vol, "vol_long": vol_long,
           "since": str(c.index[0]), "years": round(len(c) / 252, 1), "h": {}}
    for h in HORIZONS:
        r = (c.shift(-h) / c - 1).dropna()
        if len(r) < MIN_WINDOWS:
            continue
        sigma = vol * math.sqrt(h / 252)
        out["h"][str(h)] = {"up": float((r > 0).mean()), "median": float(r.median()), "p10": float(r.quantile(0.1)),
                            "p90": float(r.quantile(0.9)), "drop5": float((r <= DROP).mean()), "windows": int(len(r)),
                            "sigma": sigma, "lo": last * (1 - sigma), "hi": last * (1 + sigma)}
    return out if out["h"] else None


def calm_note(s: dict) -> str | None:
    """Says when recent volatility is far from the long-run level (the range is then narrower / wider than usual)."""
    ratio = s["vol"] / s["vol_long"] if s.get("vol_long") else None
    if ratio is None:
        return None
    if ratio <= 0.75:
        return f"最近 3 个月的波动（年化 {s['vol'] * 100:.0f}%）低于长期平均（{s['vol_long'] * 100:.0f}%），眼下的正常范围比历史上更窄"
    if ratio >= 1.33:
        return f"最近 3 个月的波动（年化 {s['vol'] * 100:.0f}%）高于长期平均（{s['vol_long'] * 100:.0f}%），眼下的正常范围比历史上更宽"
    return None


BEAR_YEAR = 2009                          # a history starting after this has not seen a 2000- or 2008-like bear market


def history_note(s: dict | None) -> str | None:
    """Warns when the history misses the big bear markets (Scout stores about 12 years: from late 2014)."""
    if not s or int(s["since"][:4]) < BEAR_YEAR:
        return None
    return (f"统计只覆盖 {s['since'][:4]} 年以来约 {s['years']:.0f} 年，没有经历 2000、2008 年那样的熊市，上涨比例可能偏乐观"
            "（例如 SPY 1 个月上涨比例，1999 年以来约 64%）")


def pct(x: float | None, signed: bool = True, d: int = 1) -> str:
    if x is None:
        return "—"
    return f"{x * 100:+.{d}f}%" if signed else f"{x * 100:.{d}f}%"


def line(s: dict | None, h: int = DEFAULT_H, to_stop: float | None = None) -> str | None:
    """One sentence: up share, the bad one-in-ten outcome, the normal range, and the stop against it."""
    x = (s or {}).get("h", {}).get(str(h))
    if not x:
        return None
    text = (f"{HORIZONS[h]}：历史上 {x['up'] * 100:.0f}% 的时候上涨，十次里较差的一次 {pct(x['p10'])}；"
            f"按最近波动，正常范围 ±{x['sigma'] * 100:.1f}%")
    if to_stop is not None and to_stop < 0:
        where = "在正常波动范围之内，平常的波动就可能碰到" if -to_stop <= x["sigma"] else "超出正常波动范围"
        text += f"。离止损 {pct(to_stop)}，{where}"
    return text


def agent_view(ticker: str, s: dict | None) -> dict:
    """Pre-formatted for the Q&A agent (numbers already as text, each with its meaning)."""
    if not s:
        return {"代码": ticker, "说明": "历史数据不足一年，无法给出历史常态"}
    rows = {HORIZONS[int(h)]: {"历史上涨比例": pct(x["up"], False, 0), "中位涨跌": pct(x["median"]),
                               "十次里较差的一次（10% 分位）": pct(x["p10"]), "十次里较好的一次（90% 分位）": pct(x["p90"]),
                               "跌 5% 以上的比例": pct(x["drop5"], False, 0),
                               "按最近波动的正常范围（±1σ，约三次中两次落在其中）": f"±{x['sigma'] * 100:.1f}%（{x['lo']:.2f}–{x['hi']:.2f}）"}
            for h, x in s["h"].items()}
    return {"代码": ticker, "收盘": f"{s['close']:.2f}（{s['asof']}）", "统计区间": f"{s['since']} 起约 {s['years']} 年",
            "最近 63 日年化波动": pct(s["vol"], False, 0), "长期年化波动": pct(s["vol_long"], False, 0), "各期限": rows,
            "含义": SOURCE_NOTE + "；上涨比例主要来自股市长期向上的趋势。", **({"波动提示": calm_note(s)} if calm_note(s) else {}),
            **({"统计区间提示": history_note(s)} if history_note(s) else {})}


# ── storage and lookup ──

async def for_tickers(tickers: list[str]) -> dict[str, dict]:
    from scout import bars

    frames = await bars.load(sorted(set(tickers)))
    out = {}
    for t, f in frames.items():
        s = stats(f["close"])
        if s:
            out[t] = s
    return out


async def for_ticker(ticker: str) -> dict | None:
    t = universe.yahoo_symbol(ticker)
    return (await for_tickers([t])).get(t)


def market_tickers() -> list[str]:
    from scout import sectors

    return ["SPY", *universe.SECTOR_ETFS.values(), *sectors.THEME_RRG]


async def store(asof: date) -> int:
    """After the close: SPY, sector and theme ETFs (brief `norms`)."""
    rows = await for_tickers(market_tickers())
    await db.execute("INSERT INTO scout.briefs (kind, asof, content, payload) VALUES ('norms', :d, :c, CAST(:p AS JSONB))",
                     {"d": asof, "c": f"历史常态 {len(rows)} 个", "p": db.jsonb({"rows": rows})})
    return len(rows)


async def latest() -> tuple[date | None, dict[str, dict]]:
    row = await db.fetch_one("SELECT asof, payload FROM scout.briefs WHERE kind = 'norms' ORDER BY asof DESC, id DESC LIMIT 1")
    return (row["asof"], (row["payload"] or {}).get("rows") or {}) if row else (None, {})
