"""Recheck 2 on Sharadar data: the `exit_vs_hold` and `exit_redeploy` designs, unchanged, on the
point-in-time universe (market-cap top 1000, later-delisted companies included), 1999 to date.

Pre-registered in `scout/knowledge/external_specs/sharadar_rechecks.yaml#recheck_2_exits`. Prices are
put on the dividend-adjusted scale (like Scout's own bars), Scout's feature formulas are recomputed
per stock, and a stock that stops trading is settled at its last close (−30 % for a performance
delisting) and then held flat, so stops, holdings and the pool all see the delisting.
"""
from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor

import numpy as np
import pandas as pd

from scout import exit_redeploy as er
from scout import exit_vs_hold as ev
from scout import sharadar_recheck as rc
from scout import sharadar_research as sr

SECTOR_ETF = {"Technology": "XLK", "Financial Services": "XLF", "Healthcare": "XLV", "Energy": "XLE",
              "Industrials": "XLI", "Consumer Defensive": "XLP", "Consumer Cyclical": "XLY", "Utilities": "XLU",
              "Basic Materials": "XLB", "Real Estate": "XLRE", "Communication Services": "XLC"}
BEAR_YEARS = (2000, 2001, 2002, 2007, 2008, 2009, 2020, 2022)


def settle(p: sr.Prices, j: int) -> dict[str, np.ndarray]:
    """One stock's OHLC on the dividend-adjusted scale; after a delisting, flat at the settled price."""
    f = p.closeadj[:, j] / p.close[:, j]
    o, h, lo, c = (x[:, j] * f for x in (p.open, p.high, p.low, p.close))
    v = p.volume[:, j].copy()
    ok = np.flatnonzero(np.isfinite(c))
    if ok.size and ok[-1] < len(c) - 1:
        last = ok[-1]
        px = c[last] * (1 + p.haircut[j])
        for x in (o, h, lo, c):
            x[last + 1:] = px
        v[last + 1:] = 0.0
    return {"open": o, "high": h, "low": lo, "close": c, "volume": v}


def stock_arrays(p: sr.Prices, j: int) -> dict:
    """The arrays `exit_vs_hold.trades` reads, with Scout's feature formulas."""
    a = settle(p, j)
    c, h = pd.Series(a["close"]), pd.Series(a["high"])
    for n in (20, 50, 200):
        a[f"sma{n}"] = c.rolling(n, min_periods=n).mean().to_numpy()
    a["atr14"] = ev.true_range_mean(a["high"], a["low"], a["close"], 14)
    a["atr22"] = ev.true_range_mean(a["high"], a["low"], a["close"], 22)
    a["atr42"] = ev.true_range_mean(a["high"], a["low"], a["close"], 42)
    a["dist_52w_high"] = (c / h.rolling(252, min_periods=200).max() - 1).to_numpy()
    a["dates"] = list(p.dates.date)
    return a


def market_off(spy: np.ndarray, dates) -> np.ndarray:
    return er.market_off(spy, dates)


def growth_series(p: sr.Prices, members_by_day: dict[int, list[int]], picks_by_day: dict[int, list[int]],
                  strong: np.ndarray, off: np.ndarray, rf: np.ndarray) -> dict[tuple[str, str], np.ndarray]:
    """Daily return of redeployed money, as `exit_redeploy.series`: the pool (equal weight over the latest
    cohort's universe) or the latest asset-growth picks, from the previous close; market filter → T-bills;
    sector filter → only stocks whose sector ETF beat SPY over 63 sessions."""
    n = len(p.dates)
    c = np.column_stack([settle(p, j)["close"] for j in range(len(p.tickers))])
    ret = np.full_like(c, np.nan)
    ret[1:] = c[1:] / c[:-1] - 1
    out = {k: rf.copy() for k in [(d, f) for d in er.DESTS for f in er.FILTERS]}
    pool_days, pick_days = sorted(members_by_day), sorted(picks_by_day)
    for t in range(1, n):
        ip = np.searchsorted(pool_days, t - 1, side="right") - 1
        ik = np.searchsorted(pick_days, t - 1, side="right") - 1
        for dest, cols in (("pool", members_by_day[pool_days[ip]] if ip >= 0 else []),
                           ("picks", picks_by_day[pick_days[ik]] if ik >= 0 else [])):
            cols = np.asarray(cols, dtype=int)
            if not cols.size:
                continue
            r = ret[t, cols]
            ok = np.isfinite(r)
            if ok.any():
                out[(dest, "none")][t] = out[(dest, "market")][t] = float(r[ok].mean())
            sel = ok & strong[t - 1, cols]
            if sel.any():
                out[(dest, "market_sector")][t] = float(r[sel].mean())
        if off[t - 1]:
            for dest in er.DESTS:
                out[(dest, "market")][t] = out[(dest, "market_sector")][t] = rf[t]
    return out


_G: dict = {}


def _init(shared: dict) -> None:
    _G.update(shared)


def _stock(args: tuple[dict, list[int]]) -> tuple[list[dict], dict[int, dict[str, np.ndarray]]]:
    a, days = args
    rf, off, growth = _G["rf"], _G["off"], _G["growth"]
    rows, sums = [], {}
    for i in days:
        base = ev.trades(a, i + 1, rf)
        if base is None:
            continue
        got = er.variants(base, i + 1, bool(off[i]), rf, growth)
        for k in ev.POLICIES:
            if k not in got:
                got[k] = (*base[k], True)
        rows.append({"i": i, **{f"r_{k}": float(v[-1] - 1 - (ev.COST if inv else 0.0)) for k, (v, _, inv) in got.items()},
                     **{f"d_{k}": d for k, (_, d, _) in got.items()}})
        sums[i] = {k: v for k, (v, _, _) in got.items()}
    return rows, sums


def calendar_returns(nav: pd.Series, dates: pd.DatetimeIndex, years=BEAR_YEARS) -> dict[int, float | None]:
    s = pd.Series(nav.to_numpy(), index=dates[nav.index])
    out = {}
    for y in years:
        x = s[s.index.year == y]
        prev = s[s.index.year < y]
        out[y] = None if x.empty or prev.empty else round(float(x.iloc[-1] / prev.iloc[-1] - 1), 4)
    return out


def run(rf_series: list, workers: int | None = None) -> dict:
    """Load the local data, build the universe, run both designs; no database writes."""
    dates = sr.trading_days()
    n = len(dates)
    cohorts = rc.rebalance_days(dates, hold=ev.WINDOW + 1, every=ev.EVERY)
    pick_days = rc.rebalance_days(dates)
    elig = sr.eligible()
    uni = sr.universe(dates[sorted(set(cohorts) | set(pick_days))], elig)
    names = sorted({t for v in uni.values() for t in v["large"]})
    p = sr.prices(names, dates)
    members = {i: [p.col[t] for t in uni.get(dates[i], {}).get("large", []) if t in p.col] for i in cohorts}
    growth = sr.asset_growth(dates[pick_days], names)
    picks = {}
    for i in pick_days:
        cols = [p.col[t] for t in uni.get(dates[i], {}).get("large", []) if t in p.col]
        sig = {j: growth[dates[i]].get(p.tickers[j]) for j in cols}
        sig = {j: v for j, v in sig.items() if v is not None and np.isfinite(v)}
        picks[i] = sorted(sig, key=sig.get)[: max(1, int(round(len(cols) * rc.FRAC)))]
    funds = sr._read("funds", ["ticker", "date", "closeadj"], filters=[("ticker", "in", ["SPY", *SECTOR_ETF.values()])])
    funds["date"] = pd.to_datetime(funds["date"])
    fw = funds.pivot(index="date", columns="ticker", values="closeadj").reindex(dates)
    spy = fw["SPY"].to_numpy()
    rf = ev.rf_daily(dates, rf_series)
    off = market_off(spy, dates)
    spy63 = pd.Series(spy).pct_change(63).to_numpy()
    strong = np.zeros((n, len(names)), dtype=bool)
    sector = elig["sector"].to_dict()
    for j, t in enumerate(names):
        etf = SECTOR_ETF.get(sector.get(t))
        if etf in fw.columns:
            r63 = fw[etf].pct_change(63).to_numpy()
            strong[:, j] = np.nan_to_num(r63 - spy63, nan=-1.0) > 0
    g = growth_series(p, members, picks, strong, off, rf)
    by_stock: dict[int, list[int]] = {}
    for i, cols in members.items():
        for j in cols:
            by_stock.setdefault(j, []).append(i)
    jobs = [(stock_arrays(p, j), ds) for j, ds in by_stock.items()]
    rows, sums, counts = [], {}, {}
    labels = {**ev.POLICIES, **er.LABELS}
    with ProcessPoolExecutor(max_workers=workers, initializer=_init, initargs=({"rf": rf, "off": off, "growth": g},)) as ex:
        for rs, ss in ex.map(_stock, jobs, chunksize=4):
            rows += rs
            for i, paths in ss.items():
                acc = sums.setdefault(i, {k: np.zeros(ev.WINDOW) for k in labels})
                for k, v in paths.items():
                    acc[k] += v
                counts[i] = counts.get(i, 0) + 1
    for r in rows:
        r["date"] = dates[r["i"]]
    means = {i: {k: v / counts[i] for k, v in acc.items()} for i, acc in sums.items()}
    hold = ev.evaluate(rows, means, rf, n, dates, ev.POLICIES)
    hold["decision"] = ev.decide(hold["policies"])
    red = ev.evaluate(rows, means, rf, n, dates, er.LABELS)
    red["decision"] = er.decide(red["policies"])
    red["risk_off_share"] = round(float(off[sorted(means)].mean()), 3)
    years = {k: calendar_returns(ev.portfolio(means, n, k), dates) for k in labels}
    return {"exit_vs_hold": hold, "exit_redeploy": red, "bear_years": years, "stocks": len(names)}


def bear_text(years: dict, labels: dict) -> str:
    pct = lambda x: "—" if x is None else f"{x:+.1%}"  # noqa: E731
    head = "方案 | " + " | ".join(str(y) for y in BEAR_YEARS)
    return "\n".join([head] + [f"{labels[k]} | " + " | ".join(pct(v.get(y)) for y in BEAR_YEARS) for k, v in years.items()])
