"""Market timing playbooks on index and sector ETFs (catalogue section 4).

The 10-month rule is the user's market filter (`scout.market_filter`, tested); the others are computed on the same
stored bars. Rules with many published versions (O'Neil's follow-through day, Zweig's breadth thrust) use one fixed
version, stated in their docstrings, never tuned.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from scout.playbooks.base import BUY, HOLD, SELL, Ctx, Verdict, num, pct, weekly_closes

INDEXES = ("SPY", "QQQ", "IWM", "RSP", "DIA")


def _etfs(ctx: Ctx) -> list[str]:
    return [t for t in ctx.etfs if ctx.close[t].notna().sum() > 230]


def faber(ctx: Ctx) -> dict[str, Verdict]:
    """Month-end close against the mean of the last 10 month-end closes (`scout.market_filter`, the same function)."""
    from scout import market_filter

    out = {}
    for t in _etfs(ctx):
        s = ctx.close[t].dropna()
        st = market_filter.summary([d.date() for d in s.index], s.tolist())
        if not st:
            continue
        since = f"，自 {st['since']} 起" if st["since"] else ""
        fresh = st["since"] is not None and (ctx.asof - st["since"]).days <= 35
        if st["off"]:
            out[t] = Verdict(SELL, f"月末收盘 {num(st['close'])} 低于 10 个月均值 {num(st['mean'])}（{pct(st['gap'])}）{since}")
        else:
            out[t] = Verdict(BUY if fresh else HOLD, f"月末收盘 {num(st['close'])} 高于 10 个月均值 {num(st['mean'])}（{pct(st['gap'])}）{since}",
                             score=float(st["gap"]))
    return out


def golden_cross_etf(ctx: Ctx) -> dict[str, Verdict]:
    from scout.playbooks.technical import golden_cross

    return golden_cross(ctx, _etfs(ctx))


def zweig_state(weekly: pd.Series, move: float = 0.04) -> dict | None:
    """Zweig's 4 % model on weekly closes: buy when a close is 4 % above the lowest close since the last sell, sell
    when 4 % below the highest close since the last buy. The first signal starts the record."""
    w = weekly.dropna()
    if len(w) < 10:
        return None
    state, lo, hi, last = None, w.iloc[0], w.iloc[0], None
    for d, x in w.items():
        if state != "buy" and x >= lo * (1 + move):
            state, hi, last = "buy", x, d
        elif state != "sell" and x <= hi * (1 - move):
            state, lo, last = "sell", x, d
        elif state == "buy":
            hi = max(hi, x)                    # the peak since the buy sets the next sell level
        elif state == "sell":
            lo = min(lo, x)
        else:
            lo, hi = min(lo, x), max(hi, x)
    return {"state": state, "since": last, "low": lo, "high": hi}


def zweig_four_percent(ctx: Ctx) -> dict[str, Verdict]:
    """On RSP (equal-weight S&P 500, the nearest stored stand-in for the Value Line geometric index) and SPY."""
    out = {}
    for t in ("RSP", "SPY"):
        if t not in ctx.close:
            continue
        w = weekly_closes(ctx.close[t])
        st = zweig_state(w)
        if not st or not st["state"]:
            continue
        weeks = int((w.index > st["since"]).sum())
        fresh = weeks <= 1
        if st["state"] == "buy":
            out[t] = Verdict(BUY if fresh else HOLD, f"周收盘比卖出后最低点上涨 4% 以上，{weeks} 周前转为买入；"
                                                     f"跌到最高周收盘 {num(st['high'])} 的 96%（{num(st['high'] * 0.96)}）时卖出")
        else:
            out[t] = Verdict(SELL, f"周收盘比买入后最高点下跌 4% 以上，{weeks} 周前转为卖出；"
                                   f"涨到最低周收盘 {num(st['low'])} 的 104%（{num(st['low'] * 1.04)}）时买回")
    return out


def oneil_state(ctx: Ctx, t: str) -> dict | None:
    """O'Neil's market direction, one fixed version: a distribution day = the index closes down ≥ 0.2 % on higher
    volume than the day before, dropped after 25 sessions or once the index closes 5 % above that day's close;
    a correction starts when the index closes 8 % below its highest close of the last 60 sessions; a rally attempt
    starts at the first up close after the correction's lowest close; a follow-through day = from the 4th day of the
    attempt, a gain ≥ 1.25 % on higher volume than the day before; the attempt fails when the index closes below its
    low (it starts over) and an uptrend ends at a close 8 % below its highest close since the follow-through or a
    close below the attempt's low. States: 确认上升 (fewer than 5 distribution days) · 上升承压 (≥ 5) · 调整中."""
    key = ("oneil", t)
    if key in ctx.cache:
        return ctx.cache[key]
    if t not in ctx.close:
        return None
    c, v = ctx.close[t].dropna(), ctx.volume[t].dropna()
    c, v = c.iloc[-260:], v.reindex(c.index).iloc[-260:]
    if len(c) < 120:
        return None
    r = c.pct_change()
    state, peak, low, attempt, ftd, ftd_low = "确认上升", float(c.iloc[:61].max()), None, None, None, None
    for i in range(61, len(c)):
        x = float(c.iloc[i])
        if state == "确认上升":
            peak = max(peak, x)
            if x <= 0.92 * peak or (ftd_low is not None and x < ftd_low):
                state, low, attempt = "调整中", x, None
            continue
        if x < low:
            low, attempt = x, None
        elif attempt is None and r.iloc[i] > 0:
            attempt = i
        elif attempt is not None and i - attempt >= 3 and r.iloc[i] >= 0.0125 and v.iloc[i] > v.iloc[i - 1]:
            state, ftd, ftd_low, peak = "确认上升", i, low, x
    dd = [i for i in range(len(c) - 25, len(c)) if r.iloc[i] <= -0.002 and v.iloc[i] > v.iloc[i - 1]
          and c.iloc[i:].max() < c.iloc[i] * 1.05]
    if state == "确认上升" and len(dd) >= 5:
        state = "上升承压"
    out = {"state": state, "distribution": len(dd), "ftd": c.index[ftd].date() if ftd is not None else None,
           "ftd_ago": len(c) - 1 - ftd if ftd is not None else None, "low": float(low) if low is not None else None}
    ctx.cache[key] = out
    return out


def oneil_market(ctx: Ctx) -> dict[str, Verdict]:
    out = {}
    for t in ("SPY", "QQQ"):
        st = oneil_state(ctx, t)
        if not st:
            continue
        ftd = f"，跟进日 {st['ftd']}" if st["ftd"] else ""
        why = f"{st['state']}：近 25 个交易日派发日 {st['distribution']} 个{ftd}"
        if st["state"] == "确认上升":
            out[t] = Verdict(BUY if (st["ftd_ago"] or 99) < ctx.fresh else HOLD, why)
        else:
            out[t] = Verdict(SELL, why + ("（O'Neil：此时减仓、不买新股）" if st["state"] == "上升承压" else "（等跟进日再买）"))
    return out


def breadth_thrust(ctx: Ctx) -> dict[str, Verdict]:
    """Zweig's breadth thrust on Scout's own universe (S&P 500 / 400 / Nasdaq-100 members, not the NYSE): the 10-day
    EMA of advancers ÷ (advancers + decliners) rises from below 0.40 to above 0.615 within 10 sessions."""
    r = ctx.close[[t for t in ctx.close.columns if t not in ctx.etfs]].pct_change()
    adv, dec = (r > 0).sum(axis=1), (r < 0).sum(axis=1)
    ema = (adv / (adv + dec).replace(0, np.nan)).ewm(span=10, adjust=False).mean().dropna()
    fired = None
    for i in range(10, len(ema)):
        if ema.iloc[i] > 0.615 and (ema.iloc[i - 10:i] < 0.40).any():
            fired = i
    now = float(ema.iloc[-1])
    if fired is not None and len(ema) - 1 - fired <= 20:
        return {"SPY": Verdict(BUY, f"广度冲刺：上涨家数比例的 10 日均值 10 天内从 40% 以下升到 61.5% 以上（{ema.index[fired].date()}）；现在 {pct(now, 0)[1:]}")}
    last = f"上次出现在 {ema.index[fired].date()}" if fired is not None else "最近一年没有出现"
    return {"SPY": Verdict(HOLD, f"没有新的广度冲刺（{last}）；上涨家数比例 10 日均值 {pct(now, 0)[1:]}")}


def dow_theory(ctx: Ctx) -> dict[str, Verdict]:
    """Scout's approximation of Dow's confirmation: the industrials (DIA) and transports (IYT) both closing at a
    one-year high within 20 sessions = bull market confirmed; both at a one-year low within 20 sessions = bear."""
    if not all(t in ctx.close and ctx.close[t].notna().sum() > 260 for t in ("DIA", "IYT")):
        return {}
    def when(t: str, hi: bool) -> int | None:
        c = ctx.close[t].dropna()
        ref = c.rolling(252).max() if hi else c.rolling(252).min()
        hit = (c >= ref) if hi else (c <= ref)
        days = np.flatnonzero(hit.iloc[-20:].to_numpy())
        return int(19 - days[-1]) if len(days) else None
    up = [when(t, True) for t in ("DIA", "IYT")]
    dn = [when(t, False) for t in ("DIA", "IYT")]
    if all(x is not None for x in up):
        return {"DIA": Verdict(BUY if min(up) < ctx.fresh else HOLD, f"工业（DIA）和运输（IYT）20 个交易日内都创一年新高（相互确认）")}
    if all(x is not None for x in dn):
        return {"DIA": Verdict(SELL, "工业和运输 20 个交易日内都创一年新低（熊市确认）")}
    one = "工业" if up[0] is not None else "运输" if up[1] is not None else None
    return {"DIA": Verdict(HOLD, f"只有{one}创一年新高，另一个没有确认" if one else "两个指数都没有创一年新高或新低")}


def liquidity(ctx: Ctx) -> dict[str, Verdict]:
    """Scout's approximation of "don't fight the Fed" / Druckenmiller's liquidity view: easing when the federal funds
    rate fell ≥ 0.25 pt over six months or the 2-year yield fell ≥ 0.5 pt over three months; tightening the reverse."""
    ff, y2 = ctx.macro.get("DFF"), ctx.macro.get("DGS2")
    if ff is None or y2 is None or len(ff) < 130 or len(y2) < 70:
        return {}
    ff_ch = float(ff.iloc[-1] - ff[ff.index <= ff.index[-1] - pd.Timedelta(days=182)].iloc[-1])
    y2_ch = float(y2.iloc[-1] - y2[y2.index <= y2.index[-1] - pd.Timedelta(days=91)].iloc[-1])
    why = f"联邦基金利率 6 个月变化 {ff_ch:+.2f} 个百分点，2 年期收益率 3 个月变化 {y2_ch:+.2f}"
    if ff_ch <= -0.25 or y2_ch <= -0.5:
        return {"SPY": Verdict(BUY, "流动性放松：" + why)}
    if ff_ch >= 0.25 or y2_ch >= 0.5:
        return {"SPY": Verdict(SELL, "流动性收紧：" + why)}
    return {"SPY": Verdict(HOLD, "流动性中性：" + why)}


def valuation_tilt(ctx: Ctx) -> dict[str, Verdict]:
    """Asness's "sin a little": the S&P 500's trailing P/E (scout.valuation, month ends since 2015) in its top fifth =
    hold a little less, bottom fifth = a little more; never all in or out."""
    v = ctx.cache.get("valuation")
    if not v:
        return {}
    spx = next((b for b in v.get("baskets", []) if b["key"] == "sp500"), None)
    h = (spx or {}).get("history") or {}
    p = h.get("share_below")
    if p is None:
        return {}
    why = f"标普 500 滚动市盈率 {num(spx['pe'], 1)}，在 2015 年以来月末值的 {pct(p, 0)[1:]} 分位"
    if p >= 0.8:
        return {"SPY": Verdict(SELL, why + "：偏贵，按这个打法小幅减仓（不是清仓）")}
    if p <= 0.2:
        return {"SPY": Verdict(BUY, why + "：偏便宜，小幅加仓")}
    return {"SPY": Verdict(HOLD, why + "：在常态范围")}
