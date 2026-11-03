"""Two low-turnover selections (pre-registered in `scout/knowledge/external_specs/slow_factors_v1.yaml`).

Local only (Sharadar, git-ignored `exports/slow_factors/`). Every 21 sessions a cohort is formed from what is known at
that close and held 252 sessions (12 overlapping cohorts, 1/12 of capital each, equal weight within a cohort, daily
rebalanced): value + quality on Sharadar ranks 1001–3000 and low volatility on the top 1000. The pool portfolio is
built the same way from every filled pool stock, without cost. Costs come from `small_cap_costs_v1`: one way on the
entry day, one way on the exit day.
"""
from __future__ import annotations

import json
import math
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

from scout import cross_asset as ca
from scout import exit_redeploy as er
from scout import sharadar_research as sr
from scout import small_cap_rankers as scr

KEY = "slow_factors_v1"
OUT = Path("exports/slow_factors")
FIRST = "1999-07-01"
EVERY, HOLD, FRAC = 21, 252, 0.10
VOL_DAYS, VOL_MIN = 252, 200
FUND_STALE_DAYS = 400
T_BAR, NW_BAR, NW_LAGS = 3.0, 2.0, 2
SPLIT = pd.Timestamp("2013-01-01")
BEARS = {"2000-03..2002-09": ("2000-03-01", "2002-09-30"), "2007-10..2009-02": ("2007-10-01", "2009-02-28"),
         "2020-02..2020-03": ("2020-02-01", "2020-03-31"), "2022-01..2022-09": ("2022-01-01", "2022-09-30")}
TRIALS = {"value_quality": {"primary": "small", "zh": "价值 + 质量"}, "low_volatility": {"primary": "large", "zh": "低波动"}}
COST_VARIANTS = ("main", "spread2", "impact2", "gross")


# ── returns ──

def daily_returns(p: sr.Prices) -> np.ndarray:
    """Close-to-close returns on forward-filled adjusted closes (a halt earns 0); the session after a delisted stock's
    last close carries its delisting adjustment, cash afterwards."""
    c = pd.DataFrame(p.closeadj).ffill().to_numpy()
    r = np.full_like(c, np.nan)
    r[1:] = c[1:] / c[:-1] - 1
    finite = np.isfinite(p.closeadj)
    for j in np.flatnonzero(p.delisted if p.delisted is not None else np.zeros(len(p.tickers), dtype=bool)):
        ok = np.flatnonzero(finite[:, j])
        if ok.size and ok[-1] + 1 < len(p.dates):
            r[ok[-1] + 1, j] = p.haircut[j]
    return r


def cohort_path(p: sr.Prices, r: np.ndarray, i: int, cols: list[int]) -> np.ndarray:
    """Daily returns of an equal-weight cohort bought at the open after day i, for sessions i+1 … i+HOLD."""
    a = i + 1
    first = p.close[a, cols] / p.open[a, cols] - 1                    # entry day: open → close
    rest = np.nan_to_num(r[a + 1:i + HOLD + 1][:, cols], nan=0.0)
    return np.r_[[first.mean()], rest.mean(axis=1)]


def filled(p: sr.Prices, i: int, cols: list[int]) -> list[int]:
    a = i + 1
    return [j for j in cols if np.isfinite(p.open[a, j]) and p.open[a, j] > 0 and np.isfinite(p.close[a, j]) and p.close[a, j] > 0]


# ── signals ──

def volatility(r_raw: np.ndarray, i: int, cols: list[int]) -> dict[int, float]:
    """Std of daily returns over the 252 sessions to day i (actual trading days only, at least 200)."""
    if i < VOL_DAYS:
        return {}
    w = r_raw[i - VOL_DAYS + 1:i + 1][:, cols]
    n = np.isfinite(w).sum(axis=0)
    sd = np.nanstd(w, axis=0, ddof=1)
    return {j: float(s) for j, s, k in zip(cols, sd, n) if k >= VOL_MIN and np.isfinite(s)}


def book_equity(names: list[str]) -> pd.DataFrame:
    f = sr._read("fundamentals", ["ticker", "dimension", "date", "reportperiod", "equity"],
                 filters=[("dimension", "=", "ARQ"), ("ticker", "in", names)])
    f = f.rename(columns={"date": "datekey"})
    f["datekey"], f["reportperiod"] = pd.to_datetime(f["datekey"]), pd.to_datetime(f["reportperiod"])
    f = sr.first_filed(f, ("ticker", "dimension", "reportperiod"))
    return f.rename(columns={"equity": "value"})[["ticker", "datekey", "value"]]


def market_caps(days: list[pd.Timestamp], names: list[str]) -> dict[pd.Timestamp, dict[str, float]]:
    d = sr._read("daily", ["ticker", "date", "marketcap"], filters=[("date", "in", [x.date() for x in days]), ("ticker", "in", names)])
    out: dict[pd.Timestamp, dict[str, float]] = {}
    for day, g in d.groupby("date"):
        out[pd.Timestamp(day)] = dict(zip(g["ticker"], g["marketcap"].astype(float) * 1e6))
    return out


def pct_rank(v: dict[int, float]) -> dict[int, float]:
    s = pd.Series(v)
    return s.rank(pct=True).to_dict() if len(s) else {}


def value_quality(book: dict[str, float], gp: dict[str, float], cap: dict[str, float], tickers: list[str], cols: list[int]) -> dict[int, float]:
    """Mean of the pool percentile ranks of book-to-market (positive equity only) and gross profit / assets."""
    bm, q = {}, {}
    for j in cols:
        t = tickers[j]
        if t in book and t in gp and cap.get(t, 0) > 0 and book[t] > 0:
            bm[j], q[j] = book[t] / cap[t], gp[t]
    rb, rq = pct_rank(bm), pct_rank(q)
    return {j: (rb[j] + rq[j]) / 2 for j in bm}


# ── portfolio ──

def run_part(p: sr.Prices, r: np.ndarray, days: list[int], pool_of, scores_of, lowest: bool) -> dict:
    """Daily return series of the picks (per cost variant) and the pool, and bookkeeping."""
    n = len(p.dates)
    acc = {v: np.zeros(n) for v in COST_VARIANTS}
    pool_acc, active = np.zeros(n), np.zeros(n)
    picks_n, missed, total_picks = [], 0, 0
    for i in days:
        pool, costs = pool_of(i)
        if len(pool) < 10:
            continue
        sc = scores_of(i, pool)
        picks = scr.freeze(pool, sc, FRAC, lowest, "pool")
        got = filled(p, i, picks)
        pool_f = filled(p, i, pool)
        missed += len(picks) - len(got)
        total_picks += len(picks)
        if not got or len(pool_f) < 2:
            continue
        picks_n.append(len(got))
        span = slice(i + 1, i + HOLD + 1)
        path = cohort_path(p, r, i, got)
        pool_acc[span] += cohort_path(p, r, i, pool_f)
        active[span] += 1
        for v in COST_VARIANTS:
            one = float(np.mean([costs[j][v] for j in got]))
            x = path.copy()
            x[0] -= one
            x[-1] -= one
            acc[v][span] += x
    live = active >= HOLD // EVERY                        # all 12 cohorts held
    idx = p.dates[live]
    out = {v: pd.Series(acc[v][live] / active[live], index=idx) for v in COST_VARIANTS}
    out["pool"] = pd.Series(pool_acc[live] / active[live], index=idx)
    return {"daily": out, "picks_mean": float(np.mean(picks_n)) if picks_n else 0.0, "not_filled": missed,
            "not_filled_share": missed / total_picks if total_picks else 0.0}


def monthly(daily: pd.Series) -> pd.Series:
    return (1 + daily).groupby(daily.index.to_period("M")).prod() - 1


def nw_alpha(y: pd.Series, x: pd.Series, lags: int = NW_LAGS) -> dict:
    """OLS y = a + b x with Newey-West standard errors."""
    df = pd.concat([y, x], axis=1).dropna()
    Y, X = df.iloc[:, 0].to_numpy(), np.c_[np.ones(len(df)), df.iloc[:, 1].to_numpy()]
    if len(df) < 12:
        return {"alpha": None, "beta": None, "t": None}
    xtx_inv = np.linalg.inv(X.T @ X)
    b = xtx_inv @ X.T @ Y
    e = Y - X @ b
    s = (X * e[:, None]).T @ (X * e[:, None])
    for k in range(1, lags + 1):
        w = 1 - k / (lags + 1)
        g = (X[k:] * e[k:, None]).T @ (X[:-k] * e[:-k, None])
        s += w * (g + g.T)
    cov = xtx_inv @ s @ xtx_inv
    return {"alpha": float(b[0]), "beta": float(b[1]), "t": float(b[0] / math.sqrt(cov[0, 0]))}


def perf(m: pd.Series, rf: pd.Series) -> dict:
    if len(m) < 12:
        return {}
    ex = m - rf.reindex(m.index).fillna(0)
    nav = (1 + m).cumprod()
    return {"cagr": float(nav.iloc[-1] ** (12 / len(m)) - 1), "vol": float(m.std(ddof=1) * math.sqrt(12)),
            "sharpe": float(ex.mean() / ex.std(ddof=1) * math.sqrt(12)) if ex.std(ddof=1) > 0 else None,
            "max_dd": float((nav / nav.cummax() - 1).min()),
            "worst_12m": float(((1 + m).rolling(12).apply(np.prod, raw=True) - 1).min()), "months": int(len(m))}


def halves(m: pd.Series, rf: pd.Series) -> dict:
    ts = m.index.to_timestamp()
    return {"all": perf(m, rf), "first": perf(m[ts < SPLIT], rf), "second": perf(m[ts >= SPLIT], rf),
            "since_2016": perf(m[ts >= "2016-01-01"], rf)}


def diff_stats(d: pd.Series) -> dict:
    ts = d.index.to_timestamp()
    return {"mean": float(d.mean()), "t": scr.tstat(d), "nw_t": scr.nw_t(d), "months": int(len(d)),
            "first": float(d[ts < SPLIT].mean()), "second": float(d[ts >= SPLIT].mean()),
            "since_2016": float(d[ts >= "2016-01-01"].mean())}


def bears(daily: pd.Series) -> dict:
    nav = (1 + daily).cumprod()
    out = {}
    for k, (a, b) in BEARS.items():
        s = nav[(nav.index >= a) & (nav.index <= b)]
        out[k] = float(s.iloc[-1] / s.iloc[0] - 1) if len(s) > 1 else None
    return out


def evaluate(part: dict, rf_m: pd.Series) -> dict:
    d = part["daily"]
    m = {k: monthly(v) for k, v in d.items()}
    xs = m["pool"] - rf_m.reindex(m["pool"].index).fillna(0)
    out = {"first_month": str(m["main"].index[0]) if len(m["main"]) else None, "picks_mean": part["picks_mean"], "not_filled": part["not_filled"], "not_filled_share": part["not_filled_share"],
           "pool": {**halves(m["pool"], rf_m), "bears": bears(d["pool"])}, "variants": {}}
    for v in COST_VARIANTS:
        ys = m[v] - rf_m.reindex(m[v].index).fillna(0)
        out["variants"][v] = {**halves(m[v], rf_m), "bears": bears(d[v]), "vs_pool": diff_stats(m[v] - m["pool"]),
                              "alpha": nw_alpha(ys, xs),
                              "years": {int(y): float(g) for y, g in ((1 + m[v]).groupby(m[v].index.year).prod() - 1).items()}}
    out["pool"]["years"] = {int(y): float(g) for y, g in ((1 + m["pool"]).groupby(m["pool"].index.year).prod() - 1).items()}
    return out


def decide(trial: str, e: dict) -> dict:
    v, s2, pool = e["variants"]["main"], e["variants"]["spread2"], e["pool"]
    if trial == "value_quality":
        d = v["vs_pool"]
        checks = {"t": bool(d["t"] is not None and d["t"] >= T_BAR and d["nw_t"] is not None and d["nw_t"] >= NW_BAR),
                  "halves": bool(d["first"] > 0 and d["second"] > 0), "conservative_cost": bool(s2["vs_pool"]["mean"] > 0)}
    else:
        checks = {"sharpe_halves": all((v[h].get("sharpe") or -9) > (pool[h].get("sharpe") or 9) for h in ("first", "second")),
                  "alpha_t": bool(v["alpha"]["t"] is not None and v["alpha"]["t"] >= T_BAR),
                  "conservative_cost": bool((s2["alpha"]["alpha"] or 0) > 0)}
    return {**checks, "pass": all(checks.values())}


def run() -> dict:
    dates = sr.trading_days()
    elig = sr.eligible()
    start = int(np.searchsorted(dates, pd.Timestamp(FIRST)))
    days = list(range(start, len(dates) - HOLD - 1, EVERY))
    uni = sr.universe(dates[days], elig)
    names = sorted({t for v in uni.values() for t in v["large"] + v["small"]})
    p = sr.prices(names, dates)
    r = daily_returns(p)
    r_raw = np.full_like(p.closeadj, np.nan)
    r_raw[1:] = p.closeadj[1:] / p.closeadj[:-1] - 1
    costs = scr.cost_table()
    book = book_equity(names)
    gp = scr.fundamentals_signals(names)["gross_profitability"]
    caps = market_caps(list(dates[days]), names)
    rf = ca.french_daily_rf().reindex(dates).ffill().fillna(0)
    rf_m = monthly(rf)

    def pool_of(part: str):
        def f(i: int):
            d = dates[i]
            month = d.to_period("M")
            c = {p.col[t]: costs[(t, month)] for t in uni.get(d, {}).get(part, []) if t in p.col and (t, month) in costs}
            return sorted(c), c
        return f

    def vq(i: int, pool: list[int]) -> dict[int, float]:
        d = dates[i]
        return value_quality(scr.asof_values(book, d), scr.asof_values(gp, d), caps.get(d, {}), p.tickers, pool)

    def lv(i: int, pool: list[int]) -> dict[int, float]:
        return volatility(r_raw, i, pool)

    spy = sr._read("funds", ["ticker", "date", "closeadj"], filters=[("ticker", "=", "SPY")])
    spy = spy.assign(date=pd.to_datetime(spy["date"])).set_index("date")["closeadj"].reindex(dates).ffill()
    off = pd.Series(er.market_off(spy.to_numpy(), dates), index=dates).shift(1).fillna(False).astype(bool)
    spy_d = spy.pct_change().fillna(0)
    filt_d = spy_d.where(~off, rf)
    out = {"key": KEY, "asof": str(date.today()), "stocks": len(names), "trials": {}}
    for trial, fn, lowest in (("value_quality", vq, False), ("low_volatility", lv, True)):
        res = {}
        for part in ("small", "large"):
            res[part] = evaluate(run_part(p, r, days, pool_of(part), fn, lowest), rf_m)
        prim = res[TRIALS[trial]["primary"]]
        m_spy, m_filt = monthly(spy_d), monthly(filt_d)
        first_month = pd.Period(prim["first_month"], "M") if prim["first_month"] else m_spy.index[0]
        res["benches"] = {"spy": halves(m_spy[m_spy.index >= first_month], rf_m),
                          "spy_filter": halves(m_filt[m_filt.index >= first_month], rf_m)}
        res["decision"] = decide(trial, prim)
        out["trials"][trial] = res
    return out


def save(res: dict) -> Path:
    OUT.mkdir(parents=True, exist_ok=True)
    path = OUT / "result.json"
    if path.exists():
        raise FileExistsError(f"{path} exists: the pre-registered test runs once")
    path.write_text(json.dumps(res, ensure_ascii=False, indent=1, default=str))
    return path


def _p(x, d: int = 1) -> str:
    return "—" if x is None else f"{x * 100:+.{d}f}%"


def _f(x) -> str:
    return "—" if x is None else f"{x:.2f}"


def report_text(res: dict) -> str:
    lines = [f"慢变量选股（预登记 {KEY}，只跑一次）：每 21 个交易日形成一组、每组持有 252 个交易日（12 组重叠），组内等权；成本 small_cap_costs_v1", ""]
    zh_part = {"small": "1001–3000", "large": "前 1000"}

    def row(name, x):
        a = x["all"]
        return (f"{name}：年化 {_p(a.get('cagr'))}，波动 {_p(a.get('vol'))}，夏普 {_f(a.get('sharpe'))}（前 {_f(x['first'].get('sharpe'))} / 后 {_f(x['second'].get('sharpe'))}），"
                f"最大回撤 {_p(a.get('max_dd'), 0)}，最差 12 个月 {_p(a.get('worst_12m'), 0)}")
    for trial, res in res["trials"].items():
        prim = TRIALS[trial]["primary"]
        lines.append(f"【{TRIALS[trial]['zh']}】主检验在 {zh_part[prim]}")
        for part in (prim, "large" if prim == "small" else "small"):
            e = res[part]
            v = e["variants"]
            tag = "主" if part == prim else "只报告"
            lines.append(f" {zh_part[part]}（{tag}）：每组约 {e['picks_mean']:.0f} 只，无法成交 {e['not_filled_share'] * 100:.2f}%")
            lines.append("  " + row("组合（扣成本）", v["main"]))
            lines.append("  " + row("股票池组合（不扣成本）", e["pool"]))
            d = v["main"]["vs_pool"]
            lines.append(f"  组合 − 股票池：每月 {_p(d['mean'], 2)}（t {_f(d['t'])}，NW t {_f(d['nw_t'])}；前 {_p(d['first'], 2)} / 后 {_p(d['second'], 2)}；2016 起 {_p(d['since_2016'], 2)}）")
            al = v["main"]["alpha"]
            lines.append(f"  相对股票池 alpha 每月 {_p(al['alpha'], 2)}（NW t {_f(al['t'])}），贝塔 {_f(al['beta'])}")
            lines.append(f"  成本敏感性（组合 − 股票池，每月）：不扣 {_p(v['gross']['vs_pool']['mean'], 2)}，价差 × 2 {_p(v['spread2']['vs_pool']['mean'], 2)}，"
                         f"冲击 × 2 {_p(v['impact2']['vs_pool']['mean'], 2)}；价差 × 2 时 alpha {_p(v['spread2']['alpha']['alpha'], 2)}")
            lines.append("  熊市（组合 / 股票池）：" + "；".join(f"{k} {_p(v['main']['bears'][k], 0)} / {_p(e['pool']['bears'][k], 0)}" for k in BEARS))
        lines.append("  " + row("对照 · SPY", res["benches"]["spy"]))
        lines.append("  " + row("对照 · SPY + 大盘过滤", res["benches"]["spy_filter"]))
        dc = res["decision"]
        lines.append("  判定：" + ("通过" if dc["pass"] else "不通过") + "（" + "，".join(f"{k} {'✓' if ok else '✗'}" for k, ok in dc.items() if k != "pass") + "）")
        lines.append("")
    return "\n".join(lines)
