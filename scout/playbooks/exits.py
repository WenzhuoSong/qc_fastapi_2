"""Exit playbooks on the user's open trades (catalogue section 7): each author's sell rule from the fill.

The user's own plan (sell at the next open after a close below 80 % of the highest close since the buy) is managed
by `scout.portfolio`; it is shown here beside the others for comparison, computed the same way.
"""
from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd

from scout.playbooks.base import HOLD, SELL, Ctx, Verdict, num, pct
from scout.playbooks.technical import _tr, darvas_box


def _since(ctx: Ctx, t: str, opened: date) -> pd.DataFrame | None:
    if t not in ctx.close:
        return None
    df = pd.DataFrame({"c": ctx.close[t], "h": ctx.high[t], "l": ctx.low[t]}).dropna()
    df = df[df.index >= pd.Timestamp(opened)]
    return df if len(df) else None


def _each(ctx: Ctx, rule) -> dict[str, Verdict]:
    """The rule on each open satellite trade; core ETFs (`levels.CORE_TICKERS`) follow the market filter instead."""
    from scout.levels import CORE_TICKERS

    out = {}
    for tr in ctx.holdings:
        if tr["ticker"] in CORE_TICKERS:
            continue
        df = _since(ctx, tr["ticker"], tr["opened_on"])
        if df is None:
            continue
        v = rule(ctx, tr, df)
        if v:
            out[tr["ticker"]] = v
    return out


def fixed_stop(ctx: Ctx) -> dict[str, Verdict]:
    """O'Neil / Minervini: sell when the close is 8 % under the buy price."""
    def rule(ctx, tr, df):
        e = float(tr["entry_price"])
        px = float(df["c"].iloc[-1])
        stop = e * 0.92
        return Verdict(SELL if px <= stop else HOLD, f"买入价 {num(e)}，止损 {num(stop)}（−8%），现价 {num(px)}（{pct(px / e - 1)}）")
    return _each(ctx, rule)


def user_trail(ctx: Ctx) -> dict[str, Verdict]:
    def rule(ctx, tr, df):
        top, px = float(df["c"].max()), float(df["c"].iloc[-1])
        return Verdict(SELL if px < 0.8 * top else HOLD, f"买入后最高收盘 {num(top)}，止损 {num(0.8 * top)}（−20%），现价 {num(px)}")
    return _each(ctx, rule)


def chandelier(ctx: Ctx) -> dict[str, Verdict]:
    """LeBeau: highest high since the buy − 3 × ATR(22)."""
    atr = _tr(ctx).rolling(22).mean().iloc[-1]

    def rule(ctx, tr, df):
        stop = float(df["h"].max()) - 3 * float(atr.get(tr["ticker"], np.nan))
        px = float(df["c"].iloc[-1])
        return Verdict(SELL if px < stop else HOLD, f"买入后最高价 − 3 × ATR22 = {num(stop)}，现价 {num(px)}")
    return _each(ctx, rule)


def wilcox_stop(ctx: Ctx) -> dict[str, Verdict]:
    atr = _tr(ctx).rolling(42).mean().iloc[-1]

    def rule(ctx, tr, df):
        stop = float(df["c"].max()) - 10 * float(atr.get(tr["ticker"], np.nan))
        px = float(df["c"].iloc[-1])
        return Verdict(SELL if px < stop else HOLD, f"买入后最高收盘 − 10 × ATR42 = {num(stop)}，现价 {num(px)}")
    return _each(ctx, rule)


def turtle_exit(ctx: Ctx) -> dict[str, Verdict]:
    """System 2's exit: a close below the lowest low of the previous 20 sessions."""
    def rule(ctx, tr, df):
        lo = float(ctx.low[tr["ticker"]].iloc[-21:-1].min())
        px = float(df["c"].iloc[-1])
        return Verdict(SELL if px < lo else HOLD, f"前 20 日最低 {num(lo)}，现价 {num(px)}")
    return _each(ctx, rule)


def weinstein_exit(ctx: Ctx) -> dict[str, Verdict]:
    d = ctx.f["wk_dist_sma30"].iloc[-1]

    def rule(ctx, tr, df):
        x = d.get(tr["ticker"], np.nan)
        if not np.isfinite(x):
            return None
        return Verdict(SELL if x < 0 else HOLD, f"周收盘相对 30 周均线 {pct(x)}")
    return _each(ctx, rule)


def darvas_exit(ctx: Ctx) -> dict[str, Verdict]:
    def rule(ctx, tr, df):
        t = tr["ticker"]
        box = darvas_box(ctx.high[t].to_numpy(dtype=float), ctx.low[t].to_numpy(dtype=float))
        if not box:
            return None
        px = float(df["c"].iloc[-1])
        return Verdict(SELL if px < box[1] else HOLD, f"最近箱体 {num(box[1])} – {num(box[0])}，现价 {num(px)}")
    return _each(ctx, rule)
