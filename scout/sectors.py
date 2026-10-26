"""Sector / theme overview: rotation, breadth, participation, attention, heat.

Everything here is computed from daily bars of the scan universe. The
"buyers vs sellers" measures (up/down volume, CMF, breadth, new highs/lows)
are proxies built from daily OHLCV — not order flow — and are labelled so.

Rotation follows the idea of the JdK Relative Rotation Graph with an open
approximation (the original formula is proprietary):
    rs        = ETF close / SPY close
    rs_ratio  = 100 × rs / SMA(rs, 50)          > 100: beating its own relative trend
    rs_mom    = 100 × rs_ratio / rs_ratio[-10]   > 100: relative strength accelerating
    quadrant  = leading (≥100, ≥100) → weakening (≥100, <100) → lagging (<100, <100) → improving (<100, ≥100)
"""
from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd

from scout import db, universe
from scout.features import FeaturePanel

BENCH = "SPY"
RATIO_WINDOW = 50
MOMENTUM_LAG = 10
TAIL_POINTS = 8
TAIL_STEP = 5            # one point per week

QUADRANTS = {
    "leading": "领先", "weakening": "转弱", "lagging": "落后", "improving": "改善",
}
# Heat = mean percentile rank (across sectors) of these components.
HEAT_COMPONENTS = ("rel_20", "rs_ratio", "rs_momentum", "pct_above_50", "updown_vol_20", "rel_dollar_vol")
HEAT_LABELS = (("热", 0.67), ("温", 0.34), ("冷", -1.0))
THEME_RRG = ("SMH", "IGV", "XBI", "KRE", "ITA", "URA", "ARKK", "IBIT", "GLD", "TLT", "USO", "IWM", "QQQ", "RSP")


def quadrant(ratio: float, mom: float) -> str:
    if ratio >= 100:
        return "leading" if mom >= 100 else "weakening"
    return "improving" if mom >= 100 else "lagging"


def rotation(p: FeaturePanel, tickers: list[str], asof: date) -> dict[str, dict]:
    c = p["close"]
    end = p.dates.get_loc(asof)
    out = {}
    for t in tickers:
        if t not in c.columns or BENCH not in c.columns:
            continue
        rs = c[t] / c[BENCH]
        ratio = 100 * rs / rs.rolling(RATIO_WINDOW, min_periods=RATIO_WINDOW).mean()
        mom = 100 * ratio / ratio.shift(MOMENTUM_LAG)
        tail = []
        for k in range(TAIL_POINTS - 1, -1, -1):
            i = end - k * TAIL_STEP
            if i >= 0 and np.isfinite(ratio.iloc[i]) and np.isfinite(mom.iloc[i]):
                tail.append([round(float(ratio.iloc[i]), 3), round(float(mom.iloc[i]), 3), str(p.dates[i])])
        if not tail:
            continue
        r, m = tail[-1][0], tail[-1][1]
        prev = tail[-2] if len(tail) >= 2 else None
        out[t] = {
            "rs_ratio": r, "rs_momentum": m, "quadrant": quadrant(r, m),
            "quadrant_prev": quadrant(prev[0], prev[1]) if prev else None, "tail": tail,
        }
    return out


def _cmf(p: FeaturePanel, members: list[str], end: int, n: int = 20) -> float | None:
    h, l, c, v = (p[k][members].iloc[end - n + 1 : end + 1] for k in ("high", "low", "close", "volume"))
    rng = (h - l).replace(0, np.nan)
    mfv = (((c - l) - (h - c)) / rng).fillna(0) * v
    per_stock = mfv.sum() / v.sum().replace(0, np.nan)
    per_stock = per_stock.dropna()
    return float(per_stock.mean()) if len(per_stock) else None


def breadth(p: FeaturePanel, members: list[str], asof: date) -> dict:
    """Participation of a sector's liquid members (proxies from daily bars)."""
    end = p.dates.get_loc(asof)
    c, h, l, v = (p[k][members] for k in ("close", "high", "low", "volume"))
    last = c.iloc[end]
    hh20 = h.iloc[end - 20 : end].max()
    ll20 = l.iloc[end - 20 : end].min()
    up = c.diff() > 0
    win = slice(end - 19, end + 1)
    upv = float(v.iloc[win].where(up.iloc[win]).sum().sum())
    dnv = float(v.iloc[win].where(~up.iloc[win] & (c.diff().iloc[win] < 0)).sum().sum())
    dv = c * v
    dv5, dv60 = dv.iloc[end - 4 : end + 1].mean().sum(), dv.iloc[end - 59 : end + 1].mean().sum()
    n = len(members)
    new_highs = int((last > hh20).sum())
    new_lows = int((last < ll20).sum())
    return {
        "members": n,
        "pct_above_50": float((last > p["sma50"][members].iloc[end]).mean()),
        "pct_above_200": float((last > p["sma200"][members].iloc[end]).mean()),
        "new_highs_20": new_highs,
        "new_lows_20": new_lows,
        "net_new_highs": (new_highs - new_lows) / n if n else None,
        "updown_vol_20": upv / dnv if dnv else None,
        "cmf_20": _cmf(p, members, end),
        "rel_dollar_vol": float(dv5 / dv60) if dv60 else None,
        "dollar_vol_5d": float(dv5),
        "median_ret_20d": float(p["ret_20d"][members].iloc[end].median()),
    }


def compute(p: FeaturePanel, asof: date) -> list[dict]:
    """One row per GICS sector (with breadth from its members) and per theme ETF."""
    liquid = p["liquid"].loc[asof]
    rows = []
    rot = rotation(p, list(universe.SECTOR_ETFS.values()) + list(THEME_RRG), asof)
    total_dv5 = 0.0
    for sector, etf in universe.SECTOR_ETFS.items():
        members = [t for t in p.stocks if p.sectors.get(t) == sector and bool(liquid.get(t))]
        if not members or etf not in p["close"].columns:
            continue
        row = {"key": etf, "kind": "sector", "sector": sector, "name": universe.SECTOR_ZH.get(sector, sector),
               **_etf_returns(p, etf, asof), **rot.get(etf, {}), **breadth(p, members, asof)}
        total_dv5 += row["dollar_vol_5d"]
        rows.append(row)
    for r in rows:
        r["dollar_vol_share"] = r["dollar_vol_5d"] / total_dv5 if total_dv5 else None
        r["flow_label"] = flow_label(r.get("cmf_20"), r.get("updown_vol_20"))
    _heat(rows)
    for etf in THEME_RRG:
        if etf in p["close"].columns and etf in rot:
            rows.append({"key": etf, "kind": "theme", "name": universe.THEME_ETFS.get(etf, etf),
                         **_etf_returns(p, etf, asof), **rot[etf]})
    return rows


def _etf_returns(p: FeaturePanel, etf: str, asof: date) -> dict:
    out = {}
    for n in (1, 5, 20, 63):
        v = p[f"ret_{n}d"].at[asof, etf]
        out[f"ret_{n}d"] = None if pd.isna(v) else float(v)
    spy20 = p["ret_20d"].at[asof, BENCH]
    if out.get("ret_20d") is not None and not pd.isna(spy20):
        out["rel_20"] = out["ret_20d"] - float(spy20)
    return out


def flow_label(cmf: float | None, updown: float | None) -> str:
    """Money-flow direction only when both daily-bar proxies agree."""
    if cmf is None or updown is None:
        return "数据不足"
    if cmf > 0.02 and updown > 1.05:
        return "流入"
    if cmf < -0.02 and updown < 0.95:
        return "流出"
    return "信号分歧"


def attention_label(attention: float | None) -> str:
    if attention is None:
        return "基线不足"
    return "上升" if attention >= 1.2 else "下降" if attention <= 0.8 else "正常"


def _heat(rows: list[dict]) -> None:
    """Mean percentile rank across sectors of HEAT_COMPONENTS → 热 / 温 / 冷."""
    if not rows:
        return
    ranks = {}
    for comp in HEAT_COMPONENTS:
        vals = pd.Series([r.get(comp) for r in rows], dtype="float64")
        ranks[comp] = vals.rank(pct=True)
    for i, r in enumerate(rows):
        parts = {comp: float(ranks[comp].iloc[i]) for comp in HEAT_COMPONENTS if not pd.isna(ranks[comp].iloc[i])}
        r["heat_parts"] = {k: round(v, 3) for k, v in parts.items()}
        r["heat"] = round(float(np.mean(list(parts.values()))), 3) if parts else None
        r["heat_label"] = next(lbl for lbl, floor in HEAT_LABELS if (r["heat"] or 0) >= floor)


async def store(asof: date, rows: list[dict]) -> int:
    await db.execute("DELETE FROM scout.sector_daily WHERE asof = :d", {"d": asof})
    return await db.execute_many(
        "INSERT INTO scout.sector_daily (asof, key, kind, payload) VALUES (:d, :k, :kind, CAST(:p AS JSONB))",
        [{"d": asof, "k": r["key"], "kind": r["kind"], "p": db.jsonb(r)} for r in rows],
    )


async def latest(kind: str | None = None) -> tuple[date | None, list[dict]]:
    row = await db.fetch_one("SELECT max(asof) AS d FROM scout.sector_daily")
    d = (row or {}).get("d")
    if not d:
        return None, []
    sql = "SELECT payload FROM scout.sector_daily WHERE asof = :d"
    params: dict = {"d": d}
    if kind:
        sql += " AND kind = :k"
        params["k"] = kind
    return d, [r["payload"] for r in await db.fetch_all(sql, params)]


async def context_for(ticker_sector: str | None) -> dict | None:
    """Compact sector context for a stock card / thesis."""
    if not ticker_sector:
        return None
    _, rows = await latest("sector")
    r = next((x for x in rows if x.get("sector") == ticker_sector), None)
    if not r:
        return None
    return {"sector": r["name"], "etf": r["key"], "heat": r.get("heat_label"),
            "quadrant": QUADRANTS.get(r.get("quadrant"), r.get("quadrant")),
            "pct_above_50": r.get("pct_above_50"), "rel_20": r.get("rel_20")}
