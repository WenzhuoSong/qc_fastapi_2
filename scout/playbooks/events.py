"""Event and speculation playbooks (catalogue section 6)."""
from __future__ import annotations

import numpy as np

from scout.playbooks.base import BUY, SELL, Ctx, Verdict, num, pct
from scout.playbooks.technical import ranked


def insider_cluster(ctx: Ctx) -> dict[str, Verdict]:
    """At least two officers / directors bought on the open market (not under 10b5-1) in the last 90 days."""
    return {t: Verdict(BUY, f"近 90 天 {int(v)} 位高管 / 董事在公开市场买入", score=v)
            for t, v in ctx.angle.get("insider_cluster", {}).items() if v >= 2 and t in ctx.stocks}


def earnings_drift(ctx: Ctx) -> dict[str, Verdict]:
    """Stocks that reported in the last month, the top tenth of those by their 3-day announcement return vs SPY."""
    return ranked(ctx, "ear_drift", 0.1, "财报公告 3 日反应")


def short_squeeze(ctx: Ctx) -> dict[str, Verdict]:
    """Scout's approximation (research pool only, Yahoo's latest short interest): short interest ≥ 15 % of the float
    and the stock rising (above its 50-day average, up ≥ 10 % in 20 sessions) = squeeze watch; ≥ 15 % short while
    falling below the 50-day average = the crowd is right so far (Asquith et al.: high short interest underperforms)."""
    c, s50, r20 = ctx.close.iloc[-1], ctx.f["sma50"].iloc[-1], ctx.f["ret_20d"].iloc[-1]
    out = {}
    for t, s in ctx.snaps.items():
        short = s.get("shortPercentOfFloat")
        if t not in ctx.stocks or not short or short < 0.15:
            continue
        days = s.get("shortRatio")
        why = f"空头占流通股 {pct(short, 0)[1:]}，回补天数 {num(days, 1)}"
        if c[t] > s50[t] and r20[t] >= 0.10:
            out[t] = Verdict(BUY, why + f"；股价在 50 日均线上方、20 日涨 {pct(r20[t])}（轧空观察）", score=float(short))
        elif c[t] < s50[t]:
            out[t] = Verdict(SELL, why + "；股价在 50 日均线下方（空头目前是对的）")
    return out
