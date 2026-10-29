"""B0 signal replay on Sharadar data (local only; spec in git-ignored exports/b0/sharadar_replay/spec.yaml,
its hash in `scout/knowledge/external_specs/sharadar_rechecks.yaml`).

B0's parameters come only from SCOUT_B0_SCORE (`scout.b0.MomentumScore`) and are never written to a
file; results go only to exports/b0/sharadar_replay/. A signal-level approximation: B0's own
portfolio process (4–6 holdings in bull markets) cannot be reproduced by a simple rule.
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from scout import b0
from scout import exit_vs_hold as ev
from scout import sharadar_exits as sx
from scout import sharadar_research as sr

OUT = b0.EXPORT_ROOT / "sharadar_replay"
EVERY = 21
COST_SIDE = 0.0005
FIRST = "1999-01-04"
PERIODS = {"1999–2009": ("1999-01-01", "2009-12-31"), "2010 至今": ("2010-01-01", "2100-01-01"),
           "2000-03..2002-09": ("2000-03-01", "2002-09-30"), "2007-10..2009-02": ("2007-10-01", "2009-02-28"),
           "2009-03..2009-05（动量崩盘）": ("2009-03-01", "2009-05-31"), "2020-02..2020-03": ("2020-02-01", "2020-03-31"),
           "2022-01..2022-09": ("2022-01-01", "2022-09-30")}


def sp500_members(days: pd.DatetimeIndex) -> dict[pd.Timestamp, set[str]]:
    """Membership on each day: the latest quarterly snapshot, then the additions and removals since."""
    s = sr._read("sp500", ["date", "action", "ticker"])
    s["date"] = pd.to_datetime(s["date"])
    snaps = {d: set(g["ticker"]) for d, g in s[s["action"] == "historical"].groupby("date")}
    snap_days = sorted(snaps)
    ev = s[s["action"].isin(["added", "removed"])].sort_values("date")
    out = {}
    for d in days:
        k = np.searchsorted(snap_days, d, side="right") - 1
        if k < 0:
            continue
        base = set(snaps[snap_days[k]])
        for r in ev[(ev["date"] > snap_days[k]) & (ev["date"] <= d)].itertuples():
            (base.add if r.action == "added" else base.discard)(r.ticker)
        out[d] = base
    return out


def ndx_proxy(days: pd.DatetimeIndex, elig: pd.DataFrame) -> dict[pd.Timestamp, list[str]]:
    """The 100 largest Nasdaq-listed non-financial stocks by that day's market cap (no history of the index)."""
    nas = elig[(elig["exchange"] == "NASDAQ") & (elig["sector"] != "Financial Services")].index
    cap = sr._read("daily", ["ticker", "date", "marketcap"], filters=[("date", "in", [d.date() for d in days])])
    cap = cap[cap["ticker"].isin(nas) & cap["marketcap"].notna()]
    return {pd.Timestamp(d): g.sort_values("marketcap", ascending=False)["ticker"].head(100).tolist() for d, g in cap.groupby("date")}


def scores(c: np.ndarray, v: np.ndarray, i: int, cols: list[int], cfg: b0.MomentumScore) -> list[tuple[int, float]]:
    """`b0.b0_scores` on arrays at day i: weighted log returns over trading-day horizons, trend filter, dollar-volume floor."""
    out = []
    for j in cols:
        hist = np.flatnonzero(np.isfinite(c[: i + 1, j]))
        if len(hist) < cfg.longest + 1 or hist[-1] != i:
            continue
        dv = float(np.nanmean(v[hist[-20:], j]) * c[i, j])
        if not dv >= cfg.min_dollar_volume:
            continue
        now = c[i, j]
        past = {n: c[hist[-1 - n], j] for n, _ in cfg.weights}
        if min(now, *past.values()) <= 0:
            continue
        r = {n: float(np.log(now / past[n])) for n in past}
        if r[cfg.trend_horizon] <= 0:
            continue
        out.append((j, sum(w * r[n] for n, w in cfg.weights)))
    return sorted(out, key=lambda x: -x[1])


def gate_on(etf: dict[str, np.ndarray], i: int, cfg: b0.MomentumScore) -> bool:
    """Every gate ETF above its moving average; one without enough history yet is left out (SPY only before QQQ)."""
    states = []
    for t in cfg.gate:
        x = etf.get(t)
        if x is None:
            continue
        h = x[: i + 1]
        h = h[np.isfinite(h)]
        if len(h) >= cfg.gate_sma:
            states.append(h[-1] > h[-cfg.gate_sma:].mean())
    return bool(states) and all(states)


def simulate(o: np.ndarray, c: np.ndarray, rf: np.ndarray, pick, gate: np.ndarray, start: int) -> np.ndarray:
    """Daily value: at each close decide (rebalance every EVERY sessions, or the gate switching), trade at
    the next open, equal weight; cash earns the bill rate; COST_SIDE per side on what is traded."""
    n = len(c)
    nav = np.full(n, np.nan)
    w: dict[int, float] = {}          # value per holding at the last close
    cash = 1.0
    target = None
    for t in range(start, n):
        if t == start:
            pass                       # the first decision is taken at this close
        elif target is not None:       # trade at this open
            val = {j: x * o[t, j] / c[t - 1, j] if np.isfinite(o[t, j]) else x for j, x in w.items()}
            total = sum(val.values()) + cash * (1 + rf[t])
            new = {j: total / len(target) for j in target} if target else {}
            turnover = sum(abs(new.get(j, 0) - val.get(j, 0)) for j in set(new) | set(val))
            total -= turnover * COST_SIDE
            w = {j: total / len(target) for j in target} if target else {}
            cash = 0.0 if target else total
            w = {j: x * c[t, j] / o[t, j] if np.isfinite(c[t, j]) and np.isfinite(o[t, j]) else x for j, x in w.items()}
            target = None
        else:
            w = {j: x * c[t, j] / c[t - 1, j] if np.isfinite(c[t, j]) else x for j, x in w.items()}
            cash *= 1 + rf[t]
        nav[t] = sum(w.values()) + cash
        held = set(w)
        if not gate[t]:
            if held:
                target = []
        elif (t - start) % EVERY == 0 or not held:
            want = pick(t)
            if set(want) != held:
                target = want
    return nav


def stats(nav: pd.Series, rf: pd.Series) -> dict:
    nav = nav.dropna()
    if len(nav) < 30:
        return {}
    r = nav.pct_change().dropna()
    ex = r - rf.reindex(r.index).fillna(0)
    years = len(r) / 252
    return {"cagr": round(float((nav.iloc[-1] / nav.iloc[0]) ** (1 / years) - 1), 4),
            "vol": round(float(r.std() * np.sqrt(252)), 4),
            "sharpe": round(float(ex.mean() / ex.std() * np.sqrt(252)), 3),
            "max_dd": round(float((nav / nav.cummax() - 1).min()), 4),
            "worst_63": round(float((nav / nav.shift(63) - 1).min()), 4)}


def run(rf_series: list) -> dict:
    cfg = b0.score_config()
    if cfg is None:
        raise SystemExit("SCOUT_B0_SCORE not set")
    dates = sr.trading_days()
    start = int(np.searchsorted(dates, pd.Timestamp(FIRST)))
    reb = list(range(start, len(dates), EVERY))
    elig = sr.eligible()
    spx = sp500_members(dates[reb])
    ndx = ndx_proxy(dates[reb], elig)
    names = sorted({t for s in spx.values() for t in s} | {t for s in ndx.values() for t in s})
    p = sr.prices(names, dates)
    arr = [sx.settle(p, j) for j in range(len(names))]
    o = np.column_stack([a["open"] for a in arr])
    c = np.column_stack([a["close"] for a in arr])
    v = np.column_stack([a["volume"] for a in arr])
    funds = sr._read("funds", ["ticker", "date", "closeadj"], filters=[("ticker", "in", ["SPY", "QQQ"])])
    funds["date"] = pd.to_datetime(funds["date"])
    fw = funds.pivot(index="date", columns="ticker", values="closeadj").reindex(dates)
    etf = {t: fw[t].to_numpy() for t in fw.columns}
    rf = ev.rf_daily(dates, rf_series)
    gate = np.array([gate_on(etf, i, cfg) for i in range(len(dates))])

    def universe_at(t: int) -> list[int]:
        k = reb[max(0, np.searchsorted(reb, t, side="right") - 1)]
        d = dates[k]
        dv = np.nanmean(c[max(0, t - 19):t + 1] * v[max(0, t - 19):t + 1], axis=0)
        cols = set()
        for members in (spx.get(d, set()), ndx.get(d, [])):
            idx = [p.col[x] for x in members if x in p.col and np.isfinite(dv[p.col[x]])]
            cols |= set(sorted(idx, key=lambda j: -dv[j])[: cfg.per_index])
        return sorted(cols)

    def pick(t: int) -> list[int]:
        return [j for j, _ in scores(c, v, t, universe_at(t), cfg)[: cfg.target_holdings]]

    nav = pd.Series(simulate(o, c, rf, pick, gate, start), index=dates)
    rfs = pd.Series(rf, index=dates)
    bench = {}
    for t in ("SPY", "QQQ"):
        x = pd.Series(etf[t], index=dates)
        bench[f"{t} 买入持有"] = x / x.dropna().iloc[0]
        on = pd.Series(gate, index=dates).shift(1).fillna(False).astype(bool)
        r = x.pct_change().where(on, rfs)
        bench[f"{t} + 同一过滤"] = (1 + r.fillna(0)).cumprod()
    series = {"B0 信号（近似）": nav, **bench}
    out = {"full": {k: stats(s[s.index >= dates[start]], rfs) for k, s in series.items()},
           "periods": {name: {k: (round(float(s[(s.index >= a) & (s.index <= b)].dropna().iloc[-1] /
                                                s[(s.index >= a) & (s.index <= b)].dropna().iloc[0] - 1), 4)
                                  if s[(s.index >= a) & (s.index <= b)].dropna().size > 1 else None)
                              for k, s in series.items()} for name, (a, b) in PERIODS.items()},
           "sub_stats": {name: {k: stats(s[(s.index >= a) & (s.index <= b)], rfs) for k, s in series.items()}
                         for name, (a, b) in list(PERIODS.items())[:2]},
           "years": {k: {int(y): round(float(g.dropna().iloc[-1] / g.dropna().iloc[0] - 1), 4)
                         for y, g in s[s.index >= dates[start]].groupby(s[s.index >= dates[start]].index.year) if g.dropna().size > 1}
                     for k, s in series.items()},
           "gate_on_share": round(float(gate[start:].mean()), 3), "universe_names": len(names)}
    return out


def report_md(res: dict) -> str:
    p = lambda x: "—" if x is None else f"{x:+.1%}"  # noqa: E731
    lines = ["# B0 信号回放（Sharadar，1999 至今）", "", "仅供线下参考：信号层面的近似，B0 实际的组合过程无法用简单规则复现。", "",
             "| 组合 | 年化 | 波动 | 夏普 | 最大回撤 | 最差 63 日 |", "|---|---|---|---|---|---|"]
    for k, s in res["full"].items():
        lines.append(f"| {k} | {p(s.get('cagr'))} | {p(s.get('vol'))} | {s.get('sharpe')} | {p(s.get('max_dd'))} | {p(s.get('worst_63'))} |")
    lines += ["", "## 分时期收益", "", "| 时期 | " + " | ".join(res["full"]) + " |", "|---|" + "---|" * len(res["full"])]
    for name, v in res["periods"].items():
        lines.append(f"| {name} | " + " | ".join(p(v[k]) for k in res["full"]) + " |")
    lines += ["", "## 两个大时期", ""]
    for name, v in res["sub_stats"].items():
        lines.append(f"**{name}**：" + "；".join(f"{k} 年化 {p(s.get('cagr'))}、夏普 {s.get('sharpe')}、回撤 {p(s.get('max_dd'))}" for k, s in v.items()))
    lines += ["", f"过滤开启的时间占比：{res['gate_on_share']:.0%}；涉及股票 {res['universe_names']} 只。"]
    lines += ["", "## 逐年", "", "| 年 | " + " | ".join(res["years"]) + " |", "|---|" + "---|" * len(res["years"])]
    for y in sorted(next(iter(res["years"].values()))):
        lines.append(f"| {y} | " + " | ".join(p(res["years"][k].get(y)) for k in res["years"]) + " |")
    return "\n".join(lines)


def write(res: dict) -> Path:
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "result.json").write_text(json.dumps(res, ensure_ascii=False, indent=2, default=str))
    path = OUT / "report.md"
    path.write_text(report_md(res))
    return path
