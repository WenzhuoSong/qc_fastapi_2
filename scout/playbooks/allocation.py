"""Asset allocation playbooks (catalogue section 5): target weights at the last completed month end.

Each returns a verdict per ETF it can hold: 买入 when its weight rose at the last month end, 持有 when held unchanged,
卖出 when it dropped to zero. The ETFs come from `scout.universe.ALLOCATION_ETFS`; until their bars are stored the
playbook reports the missing ones (`Ctx.cache["missing"]`).
"""
from __future__ import annotations

import pandas as pd

from scout.playbooks.base import BUY, HOLD, SELL, Ctx, Verdict, month_end_closes, pct

STATIC = {
    "all_weather": {"VTI": 0.30, "TLT": 0.40, "IEI": 0.15, "GLD": 0.075, "DBC": 0.075},
    "permanent": {"VTI": 0.25, "TLT": 0.25, "GLD": 0.25, "BIL": 0.25},
    "sixty_forty": {"SPY": 0.60, "AGG": 0.40},
}


def _months(ctx: Ctx, tickers: tuple[str, ...], key: str, need: int) -> pd.DataFrame | None:
    m = pd.DataFrame({t: month_end_closes(ctx.close[t]) for t in tickers if t in ctx.close})
    missing = [t for t in tickers if t not in m.columns or m[t].notna().sum() < need]
    if missing:
        ctx.cache.setdefault("missing", {})[key] = missing
        return None
    return m.dropna()


def _verdicts(now: dict[str, float], before: dict[str, float], why: dict[str, str]) -> dict[str, Verdict]:
    out = {}
    for t in sorted(set(now) | set(before)):
        w0, w1 = before.get(t, 0.0), now.get(t, 0.0)
        if w1 > w0 + 1e-9:
            out[t] = Verdict(BUY, f"目标权重 {w0:.0%} → {w1:.0%}：{why.get(t, '')}", score=w1)
        elif w1 > 0:
            out[t] = Verdict(HOLD, f"目标权重 {w1:.0%}：{why.get(t, '')}", score=w1)
        elif w0 > 0:
            out[t] = Verdict(SELL, f"目标权重 {w0:.0%} → 0：{why.get(t, '')}")
    return out


def gem(ctx: Ctx) -> dict[str, Verdict]:
    """Antonacci's Global Equities Momentum, absolute momentum first: when the S&P 500's 12-month return beats
    T-bills (BIL), hold the stronger of US (SPY) and non-US (EFA) stocks over 12 months; otherwise aggregate bonds (AGG)."""
    m = _months(ctx, ("SPY", "EFA", "AGG", "BIL"), "gem", 14)
    if m is None:
        return {}

    def pick(row: int) -> tuple[str, dict[str, float]]:
        r = m.iloc[row] / m.iloc[row - 12] - 1
        if r["SPY"] <= r["BIL"]:
            return "AGG", r
        return ("SPY" if r["SPY"] >= r["EFA"] else "EFA"), r

    now, r = pick(-1)
    before, _ = pick(-2)
    why = {t: f"12 个月：SPY {pct(r['SPY'])}、EFA {pct(r['EFA'])}、国库券 {pct(r['BIL'])}" for t in ("SPY", "EFA", "AGG")}
    return _verdicts({now: 1.0}, {before: 1.0}, why)


def m13612w(m: pd.DataFrame, row: int) -> pd.Series:
    """Keller's 13612W momentum: (12 × 1-month + 4 × 3-month + 2 × 6-month + 12-month returns) / 4."""
    r = lambda k: m.iloc[row] / m.iloc[row - k] - 1  # noqa: E731
    return (12 * r(1) + 4 * r(3) + 2 * r(6) + r(12)) / 4


def vaa(ctx: Ctx) -> dict[str, Verdict]:
    """Keller & Keuning's VAA-G4: if every offensive asset (SPY, EFA, EEM, AGG) has positive 13612W momentum, hold
    the best of them; otherwise the best defensive one (SHY, IEF, LQD)."""
    off, dfn = ("SPY", "EFA", "EEM", "AGG"), ("SHY", "IEF", "LQD")
    m = _months(ctx, off + dfn, "vaa", 14)
    if m is None:
        return {}

    def pick(row: int) -> tuple[str, pd.Series]:
        s = m13612w(m, row)
        return (s[list(off)].idxmax() if (s[list(off)] > 0).all() else s[list(dfn)].idxmax()), s

    now, s = pick(-1)
    before, _ = pick(-2)
    bad = [t for t in off if s[t] <= 0]
    why = {t: f"13612W 动量 {pct(s[t])}" + (f"；动量为负的进攻资产：{'、'.join(bad)}" if bad else "；进攻资产动量都为正")
           for t in off + dfn}
    return _verdicts({now: 1.0}, {before: 1.0}, why)


def gtaa(ctx: Ctx) -> dict[str, Verdict]:
    """Faber's GTAA 5: SPY, EFA, IEF, VNQ, DBC at 20 % each, each one in cash while below its 10-month average."""
    assets = ("SPY", "EFA", "IEF", "VNQ", "DBC")
    m = _months(ctx, assets, "gtaa", 12)
    if m is None:
        return {}

    def weights(row: int) -> tuple[dict[str, float], dict[str, float]]:
        sma = m.iloc[row - 9: row + 1 if row != -1 else None].mean()
        gap = m.iloc[row] / sma - 1
        return {t: 0.2 for t in assets if gap[t] >= 0}, gap

    now, gap = weights(-1)
    before, _ = weights(-2)
    why = {t: f"月末相对 10 个月均值 {pct(gap[t])}" for t in assets}
    return _verdicts(now, before, why)


def static(key: str):
    def fn(ctx: Ctx) -> dict[str, Verdict]:
        w = STATIC[key]
        m = _months(ctx, tuple(w), key, 2)
        if m is None:
            return {}
        return {t: Verdict(HOLD, f"固定权重 {x:.1%}，每年（或偏离过大时）再平衡", score=x) for t, x in w.items()}
    fn.__name__ = key
    return fn
