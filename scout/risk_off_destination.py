"""Where the core money goes while the market filter is off (pre-registered in
`scout/knowledge/external_specs/risk_off_destination_v1.yaml`).

Local only: daily `closeadj` of SPY and four destinations (SHY, IEF, TLT, GLD) from the git-ignored Sharadar
files, cash at the French daily T-bill rate. Each month-end close decides the filter (SPY below the mean of the
last 10 month-end closes = off); on, the money is in SPY, off, in the destination (or cash, the control); traded
at the next session's close with a one-way cost on turnover. Simulation and metrics are `scout.cross_asset`'s.
"""
from __future__ import annotations

import math

import numpy as np
import pandas as pd

from scout import cross_asset as ca

KEY = "risk_off_destination_v1"
FAMILY = "risk_off_destination"
ASSETS = ("SPY", "SHY", "IEF", "TLT", "GLD")
TRIALS = {"SHY": "短期国债（1–3 年）", "IEF": "中期国债（7–10 年）", "TLT": "长期国债（20 年以上）", "GLD": "黄金"}
CONTROL = "cash"
START, BOND_START = "2004-12-01", "2002-08-01"
MONTHS = 10
COST, COST_HIGH = 0.0005, 0.002
MAX_DD_WORSE = 0.02
NW_LAGS = 3
REQUIRED_T = 3.40


# ── signal and schedule ──

def risk_off(spy: pd.Series, ends: list[int]) -> dict[int, bool]:
    """Per month-end row: True when the close is below the mean of the last MONTHS month-end closes (this one
    included); rows before MONTHS month ends exist are left out."""
    closes = spy.to_numpy(dtype=float)
    out, seen = {}, []
    for i in ends:
        seen.append(closes[i])
        if len(seen) >= MONTHS:
            out[i] = bool(closes[i] < np.mean(seen[-MONTHS:]))
    return out


def schedule(off: dict[int, bool], dest: str | None, first_row: int) -> dict[int, dict[str, float]]:
    """Targets per decision row from `first_row` on: SPY when on, the destination when off (None = cash)."""
    return {i: ({dest: 1.0} if dest else {}) if o else {"SPY": 1.0} for i, o in off.items() if i >= first_row}


def spells(off: dict[int, bool], first_row: int, n_rows: int) -> list[tuple[int, int]]:
    """(first held row, last held row) of each risk-off stretch, in trade rows (decision row + 1)."""
    rows = sorted(i for i in off if i >= first_row)
    out, start = [], None
    for i in rows:
        if off[i] and start is None:
            start = i + 1
        elif not off[i] and start is not None:
            out.append((start, min(i + 1, n_rows - 1)))
            start = None
    if start is not None and start < n_rows - 1:
        out.append((start, n_rows - 1))
    return out


def spell_returns(px: pd.DataFrame, rf: pd.Series, sp: list[tuple[int, int]], assets) -> list[dict]:
    """Each asset's return from the close of the first held row to the close of the last (the switch rows)."""
    out = []
    for a, b in sp:
        row = {"start": str(px.index[a].date()), "end": str(px.index[b].date()), "months": round((b - a) / 21, 1),
               "cash": float(np.prod(1 + rf.iloc[a + 1:b + 1].to_numpy()) - 1)}
        for t in assets:
            p0, p1 = px[t].iloc[a], px[t].iloc[b]
            row[t] = float(p1 / p0 - 1) if np.isfinite(p0) and np.isfinite(p1) else None
        out.append(row)
    return out


# ── statistics ──

def nw_t(x: pd.Series, lags: int = NW_LAGS) -> float | None:
    """Newey-West t of the mean (Bartlett weights)."""
    x = x.dropna().to_numpy(dtype=float)
    n = len(x)
    if n < 3:
        return None
    e = x - x.mean()
    v = e @ e / n
    for k in range(1, lags + 1):
        v += 2 * (1 - k / (lags + 1)) * (e[k:] @ e[:-k]) / n
    return float(x.mean() / math.sqrt(v / n)) if v > 0 else None


def versus(m: pd.Series, c: pd.Series, half) -> dict:
    d = (m - c).dropna()
    out = {}
    for part, s in (("all", d), ("first", d[d.index < half]), ("second", d[d.index >= half])):
        out[part] = {**ca.tstat(s), "nw_t": nw_t(s)}
    return out


def evaluate(results: dict[str, dict], rf: pd.Series) -> dict:
    months = ca.monthly(results[CONTROL]["ret"]).index
    half = months[len(months) // 2]
    out = {"first_month": str(months[0]), "last_month": str(months[-1]), "split": str(half), "series": {}}
    for n, r in results.items():
        ret = r["ret"]
        per = ret.index.to_period("M")
        yrs = (1 + ret).groupby(ret.index.year).prod() - 1
        out["series"][n] = {"all": ca.stats(ret, rf), "first": ca.stats(ret[per < half], rf), "second": ca.stats(ret[per >= half], rf),
                            "turnover_yr": float(sum(r["turnover"]) / (len(ret) / ca.YEAR)),
                            "years": {int(y): float(v) for y, v in yrs.items()}}
    cm = ca.monthly(results[CONTROL]["ret"])
    for n in TRIALS:
        if n in results:
            out["series"][n]["vs_cash"] = versus(ca.monthly(results[n]["ret"]), cm, half)
    return out


def decide(ev: dict, required_t: float = REQUIRED_T) -> dict:
    s, c = ev["series"], ev["series"][CONTROL]
    out, ok_names = {}, []
    for n in TRIALS:
        if n not in s:
            continue
        x, v = s[n], s[n]["vs_cash"]["all"]
        ts = [t for t in (v.get("t"), v.get("nw_t")) if t is not None]
        checks = {"sharpe_halves": all(x[h]["sharpe"] > c[h]["sharpe"] for h in ("first", "second")),
                  "drawdown": all(x[h]["max_dd"] >= c[h]["max_dd"] - MAX_DD_WORSE for h in ("all", "first", "second")),
                  "t": bool(ts) and min(ts) >= required_t}
        out[n] = {"checks": checks, "pass": all(checks.values()), "t_used": min(ts) if ts else None}
        if out[n]["pass"]:
            ok_names.append(n)
    pick = max(ok_names, key=lambda n: s[n]["all"]["sharpe"]) if ok_names else None
    return {"trials": out, "pick": pick, "required_t": required_t}


# ── run ──

def run_sample(px: pd.DataFrame, rf: pd.Series, start: str, dests, cost: float) -> tuple[dict, list[tuple[int, int]]]:
    ends = ca.month_end_rows(px.index)
    off = risk_off(px["SPY"], ends)
    first = max(i for i in ends if px.index[i] < pd.Timestamp(start))      # the month end before the start; trades the next session
    res = {CONTROL: ca.simulate(px, rf, schedule(off, None, first), cost)}
    for d in dests:
        res[d] = ca.simulate(px, rf, schedule(off, d, first), cost)
    res["spy"] = ca.simulate(px, rf, {first: {"SPY": 1.0}}, cost)
    return res, spells(off, first, len(px))


def run() -> dict:
    px, rf = ca.load(ASSETS)
    main, sp = run_sample(px, rf, START, list(TRIALS), COST)
    ev = evaluate(main, rf)
    high, _ = run_sample(px, rf, START, list(TRIALS), COST_HIGH)
    bonds, bsp = run_sample(px, rf, BOND_START, ["SHY", "IEF", "TLT"], COST)
    return {"key": KEY, "main": ev, "decision": decide(ev),
            "spells": spell_returns(px, rf, sp, ASSETS), "risk_off_share": float(sum(b - a for a, b in sp) / max(1, len(main[CONTROL]["ret"]))),
            "cost_high": {n: {"cagr": ca.stats(r["ret"], rf)["cagr"], "sharpe": ca.stats(r["ret"], rf)["sharpe"]} for n, r in high.items()},
            "bonds_from_2002": {"series": {n: ca.stats(r["ret"], rf) for n, r in bonds.items()},
                                "spells": spell_returns(px, rf, bsp, ("SPY", "SHY", "IEF", "TLT"))}}


# ── report ──

def _p(x: float | None, d: int = 1) -> str:
    return "—" if x is None else f"{x * 100:+.{d}f}%"


NAMES = {CONTROL: "现金（现行）", **TRIALS, "spy": "SPY 买入持有"}


def report_text(res: dict) -> str:
    ev, dec = res["main"], res["decision"]
    lines = [f"避险时钱放哪里（{KEY}）：{ev['first_month']} 至 {ev['last_month']}，前后两半以 {ev['split']} 切分；"
             f"避险期占 {res['risk_off_share'] * 100:.0f}% 的交易日，共 {len(res['spells'])} 段",
             "方案 | 年化 | 夏普 全部/前半/后半 | 最大回撤 全部/前半/后半 | 最差 12 个月 | 相对现金每月（t / NW t）"]
    for n, s in ev["series"].items():
        v = s.get("vs_cash", {}).get("all")
        vs = f"{_p(v['mean'], 2)}（{v['t']:.2f} / {v['nw_t']:.2f}）" if v and v.get("t") is not None and v.get("nw_t") is not None else "—"
        lines.append(f"{NAMES.get(n, n)} | {_p(s['all']['cagr'])} | {s['all']['sharpe']:.2f} / {s['first']['sharpe']:.2f} / {s['second']['sharpe']:.2f} | "
                     f"{_p(s['all']['max_dd'])} / {_p(s['first']['max_dd'])} / {_p(s['second']['max_dd'])} | {_p(s['all']['worst_12m'])} | {vs}")
    for n, d in dec["trials"].items():
        c = d["checks"]
        lines.append(f"{TRIALS[n]}：{'✓' if c['sharpe_halves'] else '✗'} 前后两半夏普高于现金；{'✓' if c['drawdown'] else '✗'} 回撤不差 2 个百分点以上；"
                     f"{'✓' if c['t'] else '✗'} t ≥ {dec['required_t']:.2f}（{d['t_used']:.2f}）" if d["t_used"] is not None else f"{TRIALS[n]}：t 无法计算")
    lines.append("每段避险期（SPY / 现金 / " + " / ".join(TRIALS) + "）：")
    for sp in res["spells"]:
        lines.append(f"  {sp['start']} → {sp['end']}（{sp['months']} 个月）：SPY {_p(sp['SPY'])} / 现金 {_p(sp['cash'])} / "
                     + " / ".join(_p(sp[t]) for t in TRIALS))
    lines.append("成本 0.20% 时：" + "；".join(f"{NAMES.get(n, n)} 年化 {_p(x['cagr'])} 夏普 {x['sharpe']:.2f}" for n, x in res["cost_high"].items()))
    b = res["bonds_from_2002"]["series"]
    lines.append("只含国债、2002-08 起（描述）：" + "；".join(f"{NAMES.get(n, n)} 年化 {_p(x['cagr'])} 夏普 {x['sharpe']:.2f} 回撤 {_p(x['max_dd'])}"
                                                for n, x in b.items()))
    lines.append(f"结论：{'通过：' + TRIALS[dec['pick']] if dec['pick'] else '没有目的地满足事先定的条件，避险时继续拿现金'}")
    return "\n".join(lines)
