"""Cross-asset trend on ETFs (pre-registered in `scout/knowledge/external_specs/cross_asset_trend_v1.yaml`).

Local only: daily `closeadj` of 12 asset-class ETFs from the git-ignored Sharadar files, cash at the
French daily T-bill rate. Each month-end close gives target weights (data up to that close only),
traded at the next session's close; weights drift between rebalances; one-way cost on turnover.
Three trials from the literature (time-series momentum, Faber's GTAA, dual momentum top 3) against
the current core rule (SPY + the 10-month filter), static versions of the same assets, SPY and 60/40.
"""
from __future__ import annotations

import math
from pathlib import Path

import numpy as np
import pandas as pd

KEY = "cross_asset_trend_v1"
FAMILY = "cross_asset_trend"
SHARADAR = Path("exports/sharadar/parquet/funds.parquet")
FRENCH = Path("exports/research_data/french/csv")
ASSETS = ("SPY", "IWM", "EFA", "EEM", "VNQ", "TLT", "IEF", "LQD", "TIP", "GLD", "DBC", "UUP")
FABER5 = ("SPY", "EFA", "IEF", "VNQ", "DBC")
YEAR, VOL_DAYS, SMA_MONTHS, TOP = 252, 63, 10, 3
MIN_ASSETS = 6
COST, COST_HIGH = 0.0005, 0.002
TRIALS = {"tsmom": "时间序列动量（波动倒数权重）", "faber_gtaa": "Faber 资产配置（10 个月均线）",
          "dual_top3": "双动量前 3"}
BENCHES = {"core_now": "现行核心规则（SPY + 大盘过滤）", "static_iv": "同样资产 · 波动倒数 · 一直持有",
           "static_ew": "同样资产 · 等权 · 一直持有", "spy": "SPY 买入持有", "sixty_forty": "60% SPY + 40% IEF"}
CONTROL = {"tsmom": "static_iv", "faber_gtaa": "static_ew", "dual_top3": "static_ew"}
MIN_DD_GAIN = 0.05
MIN_T_VS_CORE = -2.0


# ── data ──

def load(assets=ASSETS) -> tuple[pd.DataFrame, pd.Series]:
    """(closeadj on SPY's sessions × assets, daily RF as a decimal on the same sessions, ending at
    the last day both exist)."""
    f = pd.read_parquet(SHARADAR, columns=["ticker", "date", "closeadj"], filters=[("ticker", "in", list(set(assets) | {"SPY"}))])
    px = f.pivot(index="date", columns="ticker", values="closeadj").sort_index()
    px.index = pd.to_datetime(px.index)
    px = px[px["SPY"].notna()][list(assets)]
    rf = french_daily_rf()
    px = px[px.index <= rf.index[-1]]
    return px, rf.reindex(px.index).fillna(0.0)


def french_daily_rf() -> pd.Series:
    d = pd.read_csv(FRENCH / "F-F_Research_Data_Factors_daily.csv", skiprows=4, index_col=0)
    d = d[pd.to_numeric(d.index, errors="coerce").notna()]
    d.index = pd.to_datetime(d.index.astype(str).str.strip(), format="%Y%m%d")
    return d["RF"].astype(float) / 100


def french_monthly() -> pd.DataFrame:
    """Monthly market total return and RF (decimals) from 1926-07."""
    d = pd.read_csv(FRENCH / "F-F_Research_Data_Factors.csv", skiprows=3, index_col=0)
    idx = d.index.astype(str).str.strip()
    d = d[idx.str.fullmatch(r"\d{6}")]
    d.index = pd.PeriodIndex(d.index.astype(str).str.strip(), freq="M")
    d = d.astype(float) / 100
    return pd.DataFrame({"mkt": d["Mkt-RF"] + d["RF"], "rf": d["RF"]})


# ── signals (data up to row i only) ──

def month_end_rows(index: pd.DatetimeIndex) -> list[int]:
    """Rows that are the last session of their month; the final row only if the month is complete
    (the next calendar month has started is unknown here, so the last row is excluded)."""
    m = index.to_period("M")
    return [i for i in range(len(index) - 1) if m[i] != m[i + 1]]


def features(px: pd.DataFrame, rf: pd.Series, i: int, ends: list[int]) -> pd.DataFrame:
    """Per asset at row i: eligible, ret12, rf12, vol63, above (close > mean of the last 10 month-end closes)."""
    out = {}
    rf12 = float(np.prod(1 + rf.iloc[i - YEAR + 1:i + 1].to_numpy()) - 1) if i >= YEAR else None
    prior_ends = [e for e in ends if e <= i][-SMA_MONTHS:]
    for a in px.columns:
        col = px[a].to_numpy()
        ok = i >= YEAR and np.isfinite(col[i]) and np.isfinite(col[i - YEAR]) and np.isfinite(col[:i + 1]).sum() >= YEAR
        r = np.diff(col[max(0, i - VOL_DAYS):i + 1]) / col[max(0, i - VOL_DAYS):i]
        r = r[np.isfinite(r)]
        vol = float(np.std(r, ddof=1) * math.sqrt(YEAR)) if len(r) >= VOL_DAYS - 3 else None
        sma_vals = [col[e] for e in prior_ends]
        above = (col[i] > np.mean(sma_vals)) if len(sma_vals) == SMA_MONTHS and np.all(np.isfinite(sma_vals)) else None
        ok = bool(ok and vol and above is not None)
        out[a] = {"eligible": ok, "ret12": col[i] / col[i - YEAR] - 1 if ok else None, "rf12": rf12, "vol63": vol,
                  "above": above}
    return pd.DataFrame(out).T


def targets(name: str, ft: pd.DataFrame) -> dict[str, float]:
    """Target weights (the rest is cash) for a trial or benchmark, from one row's features."""
    el = ft[ft["eligible"].astype(bool)]
    if el.empty:
        return {}
    inv = {a: 1 / el.at[a, "vol63"] for a in el.index}
    iv = {a: v / sum(inv.values()) for a, v in inv.items()}
    ew = {a: 1 / len(el) for a in el.index}
    up = {a for a in el.index if el.at[a, "ret12"] > el.at[a, "rf12"]}
    if name == "tsmom":
        return {a: w for a, w in iv.items() if a in up}
    if name == "faber_gtaa":
        return {a: w for a, w in ew.items() if bool(el.at[a, "above"])}
    if name == "dual_top3":
        top = el["ret12"].astype(float).sort_values(ascending=False).index[:TOP]
        return {a: 1 / len(top) for a in top if a in up}
    if name == "static_iv":
        return iv
    if name == "static_ew":
        return ew
    raise ValueError(name)


def bench_targets(name: str, ft: pd.DataFrame) -> dict[str, float]:
    if name == "spy":
        return {"SPY": 1.0}
    if name == "sixty_forty":
        return {"SPY": 0.6, "IEF": 0.4}
    if name == "core_now":                  # scout.market_filter: off while the month-end close is below the 10-month mean
        return {"SPY": 1.0} if ft.at["SPY", "above"] else {}
    return targets(name, ft)


# ── simulation ──

def simulate(px: pd.DataFrame, rf: pd.Series, schedule: dict[int, dict[str, float]], cost: float) -> dict:
    """Daily NAV for targets decided at rows `schedule` (traded at the next row's close).
    Returns {ret: daily returns from the first trade, turnover: per trade, cash: daily cash share}."""
    rets = px.pct_change().to_numpy()
    rfa = rf.to_numpy()
    cols = list(px.columns)
    trade_rows = {i + 1: w for i, w in schedule.items() if i + 1 < len(px)}
    start = min(trade_rows)
    w = np.zeros(len(cols))
    out, cash, turns = [], [], []
    for t in range(start, len(px)):
        if t > start:
            r = np.nan_to_num(rets[t])
            c = 1 - w.sum()
            gross = w @ (1 + r) + c * (1 + rfa[t])
            out.append(gross - 1)
            w = w * (1 + r) / gross
        if t in trade_rows:
            tgt = np.array([trade_rows[t].get(a, 0.0) for a in cols])
            turn = float(np.abs(tgt - w).sum())
            turns.append(turn)
            if t > start:
                out[-1] = (1 + out[-1]) * (1 - cost * turn) - 1
            w = tgt
        if t > start:
            cash.append(1 - w.sum())
    idx = px.index[start + 1:]
    first_cost = cost * turns[0]                   # the first purchase is charged on the first day
    ret = pd.Series(out, index=idx)
    ret.iloc[0] = (1 + ret.iloc[0]) * (1 - first_cost) - 1
    return {"ret": ret, "turnover": turns, "cash": pd.Series(cash, index=idx)}


def schedules(px: pd.DataFrame, rf: pd.Series, names: list[str], min_assets: int) -> dict[str, dict[int, dict]]:
    ends = month_end_rows(px.index)
    out: dict[str, dict[int, dict]] = {n: {} for n in names}
    started = False
    for i in ends:
        ft = features(px, rf, i, ends)
        if not started and int(ft["eligible"].astype(bool).sum()) < min_assets:
            continue
        started = True
        for n in names:
            out[n][i] = bench_targets(n, ft)
    return out


# ── metrics ──

def monthly(ret: pd.Series) -> pd.Series:
    return (1 + ret).groupby(ret.index.to_period("M")).prod() - 1


def stats(ret: pd.Series, rf: pd.Series) -> dict:
    ex = ret - rf.reindex(ret.index).fillna(0.0)
    nav = (1 + ret).cumprod()
    yrs = len(ret) / YEAR
    down = ex[ex < 0]
    m = monthly(ret)
    roll12 = (1 + m).rolling(12).apply(np.prod, raw=True) - 1
    return {"cagr": float(nav.iloc[-1] ** (1 / yrs) - 1), "vol": float(ret.std(ddof=1) * math.sqrt(YEAR)),
            "sharpe": float(ex.mean() / ex.std(ddof=1) * math.sqrt(YEAR)),
            "sortino": float(ex.mean() * YEAR / (math.sqrt((down ** 2).sum() / len(ex)) * math.sqrt(YEAR))) if len(down) else None,
            "max_dd": float((nav / nav.cummax() - 1).min()), "worst_12m": float(roll12.min())}


def tstat(x: pd.Series) -> dict:
    x = x.dropna()
    return {"mean": float(x.mean()), "t": float(x.mean() / (x.std(ddof=1) / math.sqrt(len(x)))) if len(x) > 2 and x.std() > 0 else None,
            "months": int(len(x))}


def evaluate(results: dict[str, dict], rf: pd.Series, spy_month: pd.Series) -> dict:
    months = monthly(results["spy"]["ret"]).index
    half = months[len(months) // 2]
    out = {"first_month": str(months[0]), "last_month": str(months[-1]), "split": str(half), "series": {}}
    spy_month = spy_month.reindex(months)
    crisis = spy_month[spy_month <= spy_month.quantile(0.10)].index
    for n, r in results.items():
        ret = r["ret"]
        per = ret.index.to_period("M")
        m = monthly(ret)
        yrs = (1 + ret).groupby(ret.index.year).prod() - 1
        out["series"][n] = {
            "all": stats(ret, rf), "first": stats(ret[per < half], rf), "second": stats(ret[per >= half], rf),
            "cash": float(r["cash"].mean()), "turnover_yr": float(sum(r["turnover"]) / (len(ret) / YEAR)),
            "years": {int(y): float(v) for y, v in yrs.items()}, "crisis_month": float(m.reindex(crisis).mean()),
            "corr_spy": float(m.corr(spy_month.reindex(m.index)))}
    for n in TRIALS:
        m = monthly(results[n]["ret"])
        vs = {}
        for b in ("core_now", CONTROL[n]):
            d = m - monthly(results[b]["ret"])
            vs[b] = {"all": tstat(d), "first": tstat(d[d.index < half]), "second": tstat(d[d.index >= half])}
        out["series"][n]["vs"] = vs
    return out


def decide(ev: dict, required_t: float) -> dict:
    s, core = ev["series"], ev["series"]["core_now"]
    out = {}
    for n in TRIALS:
        x, ctl = s[n], s[CONTROL[n]]
        checks = {
            "sharpe_vs_core": all(x[h]["sharpe"] > core[h]["sharpe"] for h in ("first", "second")),
            "sharpe_vs_static": all(x[h]["sharpe"] > ctl[h]["sharpe"] for h in ("first", "second")),
            "drawdown": x["all"]["max_dd"] >= core["all"]["max_dd"] + MIN_DD_GAIN,
            "not_behind_core": all((x["vs"]["core_now"][h]["t"] or 0) >= MIN_T_VS_CORE for h in ("first", "second")),
        }
        v = x["vs"][CONTROL[n]]
        edge = ((v["all"]["t"] or 0) >= required_t and v["first"]["mean"] > 0 and v["second"]["mean"] > 0)
        out[n] = {"checks": checks, "candidate": all(checks.values()), "edge_claim": edge}
    cands = [n for n in TRIALS if out[n]["candidate"]]
    pick = max(cands, key=lambda n: s[n]["all"]["sharpe"]) if cands else None
    return {"trials": out, "pick": pick, "required_t": required_t}


def run_set(px: pd.DataFrame, rf: pd.Series, min_assets: int, cost: float) -> dict[str, dict]:
    names = [*TRIALS, *BENCHES]
    sch = schedules(px, rf, names, min_assets)
    return {n: simulate(px, rf, sch[n], cost) for n in names}


# ── long history on the US market (descriptive) ──

def french_long() -> dict:
    """12-month momentum and the 10-month average rule on the US market, monthly, 1926-07.., vs holding."""
    d = french_monthly()
    idx = (1 + d["mkt"]).cumprod()
    mom = (1 + d["mkt"]).rolling(12).apply(np.prod, raw=True) > (1 + d["rf"]).rolling(12).apply(np.prod, raw=True)
    sma = idx > idx.rolling(SMA_MONTHS).mean()
    out = {}
    for name, sig in (("hold", pd.Series(True, index=d.index)), ("mom12", mom), ("sma10", sma)):
        held = sig.shift(1)
        ok = held.notna()
        held = held[ok].astype(bool)
        r = np.where(held, d["mkt"][ok], d["rf"][ok])
        switch = held.astype(int).diff().abs().fillna(0).to_numpy()
        r = pd.Series(r - COST * switch, index=held.index)
        r = r[r.index >= pd.Period("1927-07", "M")]
        out[name] = {"all": _mstats(r, d["rf"]), "decades": {
            f"{y}s": _mstats(r[(r.index.year >= y) & (r.index.year < y + 10)], d["rf"]) for y in range(1920, 2030, 10)
            if ((r.index.year >= y) & (r.index.year < y + 10)).sum() >= 24}}
    return out


def _mstats(r: pd.Series, rf: pd.Series) -> dict:
    ex = r - rf.reindex(r.index)
    nav = (1 + r).cumprod()
    return {"cagr": float(nav.iloc[-1] ** (12 / len(r)) - 1), "sharpe": float(ex.mean() / ex.std(ddof=1) * math.sqrt(12)),
            "max_dd": float((nav / nav.cummax() - 1).min()), "months": int(len(r))}


def run() -> dict:
    from scout import gate

    px, rf = load()
    spy_m = monthly(px["SPY"].pct_change().dropna())
    main = run_set(px, rf, MIN_ASSETS, COST)
    ev = evaluate(main, rf, spy_m)
    res = {"key": KEY, "main": ev, "decision": decide(ev, gate.required_t(len(TRIALS)))}
    high = run_set(px, rf, MIN_ASSETS, COST_HIGH)
    res["cost_high"] = {n: stats(high[n]["ret"], rf) for n in [*TRIALS, "core_now"]}
    px5, rf5 = load(FABER5)
    f5 = run_set(px5, rf5, len(FABER5), COST)
    ev5 = evaluate(f5, rf5, monthly(px5["SPY"].pct_change().dropna()))
    res["faber5"] = {"first_month": ev5["first_month"], "last_month": ev5["last_month"],
                     "series": {n: {"all": s["all"], "first": s["first"], "second": s["second"]} for n, s in ev5["series"].items()}}
    res["french_long"] = french_long()
    return res


# ── report ──

def _p(x: float | None, d: int = 1) -> str:
    return "—" if x is None else f"{x * 100:+.{d}f}%"


def report_text(res: dict) -> str:
    ev, dec = res["main"], res["decision"]
    s = ev["series"]
    lines = [f"跨资产趋势（{KEY}），{ev['first_month']} 至 {ev['last_month']}，前后两半以 {ev['split']} 分界",
             "方案 | 年化 | 波动 | 夏普 全部/前半/后半 | 最大回撤 | 最差 12 个月 | 2008 / 2020 / 2022 | 平均现金 | 年换手"]
    for n in [*TRIALS, *BENCHES]:
        x = s[n]
        y = {int(k): v for k, v in x["years"].items()}        # JSON keys are strings
        lines.append(f"{TRIALS.get(n) or BENCHES[n]} | {_p(x['all']['cagr'])} | {x['all']['vol'] * 100:.1f}% | "
                     f"{x['all']['sharpe']:.2f} / {x['first']['sharpe']:.2f} / {x['second']['sharpe']:.2f} | "
                     f"{_p(x['all']['max_dd'])} | {_p(x['all']['worst_12m'])} | "
                     f"{_p(y.get(2008))} / {_p(y.get(2020))} / {_p(y.get(2022))} | {x['cash'] * 100:.0f}% | {x['turnover_yr']:.1f}")
    for n in TRIALS:
        t = dec["trials"][n]
        vs = s[n]["vs"]
        c = vs["core_now"]
        k = vs[CONTROL[n]]
        lines.append(f"{TRIALS[n]}：对现行核心每月 {_p(c['all']['mean'], 2)}（t {c['all']['t']:.2f}；前半 {c['first']['t']:.2f}，后半 {c['second']['t']:.2f}）；"
                     f"对静态 {_p(k['all']['mean'], 2)}（t {k['all']['t']:.2f}）；条件 "
                     + "，".join(f"{a}={'✓' if b else '✗'}" for a, b in t["checks"].items())
                     + f" → {'核心仓候选' if t['candidate'] else '不满足'}" + ("；择时带来可靠超额" if t["edge_claim"] else ""))
    lines.append(f"结论：{'推荐 ' + TRIALS[dec['pick']] + '（是否采用由用户决定）' if dec['pick'] else '没有变体满足全部条件，现行核心规则不变'}")
    ch = res["cost_high"]
    lines.append("单边成本 0.2%：" + "；".join(f"{TRIALS.get(n) or BENCHES[n]} 年化 {_p(ch[n]['cagr'])}、夏普 {ch[n]['sharpe']:.2f}" for n in ch))
    f5 = res["faber5"]
    lines.append(f"Faber 原版 5 类资产（{f5['first_month']} 起，只作参考）：" + "；".join(
        f"{TRIALS.get(n) or BENCHES[n]} 夏普 {f5['series'][n]['all']['sharpe']:.2f}、回撤 {_p(f5['series'][n]['all']['max_dd'])}"
        for n in [*TRIALS, "core_now", "static_ew"]))
    fl = res["french_long"]
    lines.append("美股 1927 年起（French，单一资产，只作描述）：" + "；".join(
        f"{n} 年化 {_p(fl[n]['all']['cagr'])}、夏普 {fl[n]['all']['sharpe']:.2f}、回撤 {_p(fl[n]['all']['max_dd'])}" for n in fl))
    return "\n".join(lines)
