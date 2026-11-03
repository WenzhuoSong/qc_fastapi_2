"""Sector contribution rotation (pre-registered in `scout/knowledge/external_specs/sector_contribution_v1.yaml`).

Local only. Each month-end: each sector's weight in the 500 largest US stocks (Sharadar, point in time) times its
SPDR ETF's 63-session return = its contribution to the market; hold the 3 largest positive contributions, weighted by
contribution (the rest in cash), traded at the next session's close (`cross_asset.simulate`). Control: 3 random
sectors weighted by their market weight.
"""
from __future__ import annotations

import math

import numpy as np
import pandas as pd

from scout import cross_asset as ca
from scout import sharadar_research as sr
from scout.sharadar_exits import SECTOR_ETF

KEY = "sector_contribution_v1"
SECTORS = tuple(SECTOR_ETF.values())
K = 3
LOOKBACK = 63
TOP = 500
COST, COST_HIGH = 0.0005, 0.0020
REPLICATIONS = 1000
SEED = 20261103
T_BAR = 3.5
SPLIT = pd.Timestamp("2013-01-01")
BEARS = {"2000-03..2002-09": ("2000-03-01", "2002-09-30"), "2007-10..2009-02": ("2007-10-01", "2009-02-28"),
         "2020-02..2020-03": ("2020-02-01", "2020-03-31"), "2022-01..2022-09": ("2022-01-01", "2022-09-30")}


# ── data ──

def sector_weights(days: list[pd.Timestamp]) -> dict[pd.Timestamp, dict[str, float]]:
    """{day: {etf: share of the TOP largest stocks' market cap}} (Sharadar sectors mapped to SPDR ETFs)."""
    elig = sr.eligible()
    want = [d.date() for d in days]
    cap = sr._read("daily", ["ticker", "date", "marketcap"], filters=[("date", "in", want)])
    px = sr._read("stocks", ["ticker", "date", "closeunadj"], filters=[("date", "in", want)])
    df = cap.merge(px, on=["ticker", "date"])
    df = df[df["ticker"].isin(elig.index) & (df["closeunadj"] >= sr.MIN_PRICE) & df["marketcap"].notna()]
    df["etf"] = df["ticker"].map(elig["sector"]).map(SECTOR_ETF)
    out = {}
    for d, g in df.groupby("date"):
        g = g.sort_values("marketcap", ascending=False).head(TOP)
        w = g.groupby("etf")["marketcap"].sum()
        out[pd.Timestamp(d)] = (w / w.sum()).to_dict()
    return out


# ── rule (data up to row i only) ──

def targets(weights: dict[str, float], rets: dict[str, float]) -> dict[str, float]:
    """The 3 largest positive contributions (weight × return), weighted by contribution among the top 3;
    with fewer than 3 positive, the missing part is cash."""
    contrib = {s: weights.get(s, 0.0) * r for s, r in rets.items() if np.isfinite(r) and weights.get(s, 0.0) > 0}
    top = sorted(contrib.items(), key=lambda kv: -kv[1])[:K]
    denom = sum(abs(v) for _, v in top)
    if denom <= 0:
        return {}
    return {s: v / denom for s, v in top if v > 0}


def random_targets(rng: np.random.Generator, weights: dict[str, float], avail: list[str]) -> dict[str, float]:
    pick = list(rng.choice(avail, size=min(K, len(avail)), replace=False))
    tot = sum(weights.get(s, 0.0) for s in pick)
    return {s: weights.get(s, 0.0) / tot for s in pick} if tot > 0 else {}


def schedules(px: pd.DataFrame, weights: dict[pd.Timestamp, dict[str, float]], ends: list[int]) -> dict[str, dict[int, dict]]:
    out: dict[str, dict[int, dict]] = {"rule": {}, "replicate": {}}
    for i in ends:
        if i < LOOKBACK:
            continue
        w = weights.get(px.index[i])
        if not w:
            continue
        avail = [s for s in SECTORS if np.isfinite(px.at[px.index[i], s]) and np.isfinite(px.at[px.index[i - LOOKBACK], s])]
        rets = {s: px.at[px.index[i], s] / px.at[px.index[i - LOOKBACK], s] - 1 for s in avail}
        out["rule"][i] = targets(w, rets)
        tot = sum(w.get(s, 0.0) for s in avail)
        out["replicate"][i] = {s: w.get(s, 0.0) / tot for s in avail}
        out.setdefault("_avail", {})[i] = (w, avail)
    return out


# ── evaluation ──

def halves(ret: pd.Series, rf: pd.Series) -> dict:
    return {"all": ca.stats(ret, rf), "first": ca.stats(ret[ret.index < SPLIT], rf), "second": ca.stats(ret[ret.index >= SPLIT], rf)}


def bears(ret: pd.Series) -> dict:
    nav = (1 + ret).cumprod()
    out = {}
    for k, (a, b) in BEARS.items():
        s = nav[(nav.index >= a) & (nav.index <= b)]
        out[k] = float(s.iloc[-1] / s.iloc[0] - 1) if len(s) > 1 else None
    return out


def run() -> dict:
    px, rf = ca.load(SECTORS + ("SPY",))
    ends = ca.month_end_rows(px.index)
    weights = sector_weights([px.index[i] for i in ends])
    sch = schedules(px, weights, ends)
    first = min(sch["rule"])
    rule = ca.simulate(px, rf, sch["rule"], COST)
    ret = rule["ret"]
    spy = ca.simulate(px, rf, {i: {"SPY": 1.0} for i in sch["rule"]}, COST)["ret"]
    core = ca.schedules(px, rf, ["core_now"], 1)["core_now"]
    filt = ca.simulate(px, rf, {i: w for i, w in core.items() if i >= first}, COST)["ret"]
    rep = ca.simulate(px, rf, sch["replicate"], COST)["ret"]
    rng = np.random.default_rng(SEED)
    ctrl = []
    for _ in range(REPLICATIONS):
        s = {i: random_targets(rng, *sch["_avail"][i]) for i in sch["rule"]}
        ctrl.append(ca.monthly(ca.simulate(px, rf, s, COST)["ret"]))
    cm = pd.concat(ctrl, axis=1).mean(axis=1)
    mine = ca.monthly(ret)
    diff = (mine - cm.reindex(mine.index)).dropna()
    held = pd.DataFrame([{s: 1.0 for s in w} for w in sch["rule"].values()]).reindex(columns=list(SECTORS)).fillna(0.0)
    years = (1 + ret).groupby(ret.index.year).prod() - 1
    spy_years = (1 + spy).groupby(spy.index.year).prod() - 1
    res = {
        "key": KEY, "start": str(ret.index[0].date()), "end": str(ret.index[-1].date()), "months": len(sch["rule"]),
        "rule": {**halves(ret, rf), "bears": bears(ret), "high_cost_cagr": ca.stats(ca.simulate(px, rf, sch["rule"], COST_HIGH)["ret"], rf)["cagr"],
                 "cash": float(rule["cash"].mean()), "avg_held": float(held.sum(axis=1).mean()),
                 "held_share": {s: float(held[s].mean()) for s in SECTORS},
                 "turnover_year": float(sum(rule["turnover"]) / (len(ret) / ca.YEAR)),
                 "years": {int(y): float(v) for y, v in years.items()}},
        "vs_random": {"all": ca.tstat(diff), "first": float(diff[diff.index < SPLIT.to_period("M")].mean()) if isinstance(diff.index, pd.PeriodIndex)
                      else float(diff[diff.index < SPLIT].mean()),
                      "second": float(diff[diff.index >= SPLIT.to_period("M")].mean()) if isinstance(diff.index, pd.PeriodIndex)
                      else float(diff[diff.index >= SPLIT].mean()),
                      "control_monthly": float(cm.mean())},
        "benches": {"spy": {**halves(spy, rf), "bears": bears(spy), "years": {int(y): float(v) for y, v in spy_years.items()}},
                    "spy_filter": {**halves(filt, rf), "bears": bears(filt)},
                    "replicate": {**halves(rep, rf), "bears": bears(rep)}},
    }
    res["decision"] = decide(res)
    return res


def decide(res: dict) -> dict:
    r, spy, vr = res["rule"], res["benches"]["spy"], res["vs_random"]
    checks = {"sharpe_halves": all(r[h]["sharpe"] > spy[h]["sharpe"] for h in ("first", "second")),
              "vs_random": bool(vr["first"] > 0 and vr["second"] > 0 and (vr["all"].get("t") or 0) >= T_BAR),
              "drawdown": r["all"]["max_dd"] >= spy["all"]["max_dd"]}
    return {**checks, "pass": all(checks.values())}


def _p(x, d: int = 1) -> str:
    return "—" if x is None or (isinstance(x, float) and math.isnan(x)) else f"{x * 100:+.{d}f}%"


def report_text(res: dict) -> str:
    r, b, vr = res["rule"], res["benches"], res["vs_random"]

    def row(name, x):
        return (f"{name}：年化 {_p(x['all']['cagr'])}，夏普 {x['all']['sharpe']:.2f}（前 {x['first']['sharpe']:.2f} / 后 {x['second']['sharpe']:.2f}），"
                f"最大回撤 {_p(x['all']['max_dd'], 0)}，最差 12 个月 {_p(x['all']['worst_12m'], 0)}")
    lines = [f"板块贡献轮动（预登记 {KEY}，只跑一次）：{res['start']} 到 {res['end']}，{res['months']} 次月度调仓；前半 1999–2012，后半 2013–", "",
             row("规则 · 贡献最大的 3 个板块，按贡献加权", r),
             f"  平均持有 {r['avg_held']:.1f} 个板块，平均现金 {r['cash'] * 100:.0f}%，年换手 {r['turnover_year']:.1f} 倍，成本每边 0.2% 时年化 {_p(r['high_cost_cagr'])}",
             "  各板块被持有的月份占比：" + "、".join(f"{s} {v * 100:.0f}%" for s, v in sorted(r["held_share"].items(), key=lambda kv: -kv[1]) if v > 0),
             f"  相对随机 3 个板块（按 SPY 权重）每月 {_p(vr['all']['mean'], 2)}（t {vr['all']['t']:.2f}；前 {_p(vr['first'], 2)} / 后 {_p(vr['second'], 2)}）",
             "  熊市：" + "；".join(f"{k} {_p(v)}" for k, v in r["bears"].items()),
             row("对照 · SPY 买入持有", b["spy"]),
             row("对照 · SPY + 大盘过滤", b["spy_filter"]),
             row("对照 · 板块按 SPY 权重一直持有", b["replicate"]),
             "  熊市（SPY）：" + "；".join(f"{k} {_p(v)}" for k, v in b["spy"]["bears"].items()),
             "逐年（规则 / SPY）：" + "；".join(f"{y} {_p(v, 0)} / {_p(b['spy']['years'].get(y), 0)}" for y, v in r["years"].items())]
    d = res["decision"]
    lines.append(f"\n判定：{'通过' if d['pass'] else '不通过'}（两半夏普高于 SPY {'✓' if d['sharpe_halves'] else '✗'}，"
                 f"好于随机选板块 t ≥ {T_BAR} {'✓' if d['vs_random'] else '✗'}，回撤不深于 SPY {'✓' if d['drawdown'] else '✗'}）")
    return "\n".join(lines)
