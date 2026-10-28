"""Exit rules against simply holding (pre-registered in `scout/knowledge/exit_specs/exit_vs_hold.yaml`).

Every 20 sessions every stock of the pool is bought at the next open and held for at most
60 sessions under each policy; money that leaves early earns the 3-month T-bill rate until
the window ends, so every policy covers the same window and the same trades. Results are
per trade (vs holding, t on monthly means) and per portfolio (three staggered sleeves of
60-session cohorts): Sharpe, drawdown, worst trades. Daily-bar conventions follow
`scout.exit_study`: intraday stops before targets, gaps fill at the open, a close-based stop
sells at the next open.
"""
from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor
from datetime import date

import numpy as np
import pandas as pd

from scout import exit_study, plan_exit, rankers
from scout.gate import monthly_t
from scout.simulate import ROUND_TRIP_COST

SPEC_KEY = "exit_vs_hold"
WINDOW = 60
EVERY = 20
SLEEVES = WINDOW // EVERY
COST = ROUND_TRIP_COST
POLICIES = {
    "hold": "一直持有（基准）",
    "plan": "现行持仓计划",
    "plan_wide": "持仓计划，初始止损至少 3×ATR",
    "atr10": "最高收盘 − 10×ATR42（Wilcox）",
    "chandelier": "最高价 − 3×ATR22（LeBeau 吊灯）",
    "trail20": "最高收盘回落 20%",
}
STOPS = [k for k in POLICIES if k != "hold"]
MIN_DD_GAIN = 0.05            # the candidate's max drawdown must be this much smaller than holding's
WORST = 0.05


def true_range_mean(high: np.ndarray, low: np.ndarray, close: np.ndarray, n: int) -> np.ndarray:
    """Simple n-session mean of the true range, known at each close."""
    prev = np.concatenate([[np.nan], close[:-1]])
    tr = np.fmax(high - low, np.fmax(np.abs(high - prev), np.abs(low - prev)))
    return pd.Series(tr).rolling(n, min_periods=n).mean().to_numpy()


def _trail(a: dict, s: int, level) -> tuple[int, float] | None:
    """Close-based trailing stop (only rising): (bar of the sale, price) or None when held to the end.
    `level(k)` is the stop level after the close of bar k (absolute index)."""
    stop = -np.inf
    for k in range(s, s + WINDOW - 1):
        lv = level(k)
        if np.isfinite(lv):
            stop = max(stop, lv)
        if a["close"][k] < stop:
            return k + 1 - s, float(a["open"][k + 1])
    return None


def _plan_exit(a: dict, s: int, fill: float, plan: dict, stop0: float) -> tuple[int, float] | None:
    path = exit_study.Path(a["open"], a["high"], a["low"], a["close"], a["atr14"], a["sma20"], a["sma50"], s)
    ret, days = exit_study.manage(path, fill, stop0, plan["target1"], max(plan["target2"], plan["target1"]), plan_exit.POLICY,
                                  max_hold=WINDOW)
    return days - 1, fill * (1 + ret)


def path(a: dict, s: int, fill: float, sale: tuple[int, float] | None, rf: np.ndarray) -> np.ndarray:
    """Position value at each close of the window (1 = the fill); after a sale, cash at the T-bill rate."""
    v = a["close"][s:s + WINDOW] / fill
    if sale is not None:
        k, px = sale
        grow = np.cumprod(np.concatenate([[1.0], 1 + rf[s + k + 1:s + WINDOW]]))
        v = v.copy()
        v[k:] = px / fill * grow
    return v


def trades(a: dict, s: int, rf: np.ndarray) -> dict[str, tuple[np.ndarray, int]] | None:
    """{policy: (value path, sessions until the sale)} for a buy at the open of bar s; None if any policy has no result."""
    fill = float(a["open"][s])
    if not (np.isfinite(fill) and fill > 0) or s + WINDOW > len(a["close"]) or not np.isfinite(a["close"][s:s + WINDOW]).all():
        return None
    plan = plan_exit.plan_for(a, s - 1, fill)
    atr = a["atr14"][s - 1]
    if not plan or not np.isfinite(atr) or not np.isfinite(a["atr42"][s - 1]):
        return None
    c, h = a["close"], a["high"]
    top_c = np.maximum.accumulate(c[s:s + WINDOW])
    top_h = np.maximum.accumulate(h[s:s + WINDOW])
    sales = {
        "hold": None,
        "plan": _plan_exit(a, s, fill, plan, plan["stop"]),
        "plan_wide": _plan_exit(a, s, fill, plan, min(plan["stop"], fill - 3 * atr)),
        "atr10": _trail(a, s, lambda k: top_c[k - s] - 10 * a["atr42"][k]),
        "chandelier": _trail(a, s, lambda k: top_h[k - s] - 3 * a["atr22"][k]),
        "trail20": _trail(a, s, lambda k: 0.8 * top_c[k - s]),
    }
    out = {}
    for name, sale in sales.items():
        if sale is not None and sale[0] >= WINDOW - 1:      # sold on the window's last bar: same as the window end
            sale = (WINDOW - 1, sale[1])
        out[name] = (path(a, s, fill, sale, rf), WINDOW if sale is None else sale[0] + 1)
    return out


def _stock(args: tuple[dict, list[int], np.ndarray]) -> tuple[list[dict], dict[int, dict[str, np.ndarray]]]:
    """One stock: per trade the final returns and holding days; per cohort day the summed paths."""
    a, days, rf = args
    rows, sums = [], {}
    for i in days:
        got = trades(a, i + 1, rf)
        if got is None:
            continue
        rows.append({"i": i, **{f"r_{k}": float(v[-1] - 1 - COST) for k, (v, _) in got.items()},
                     **{f"d_{k}": d for k, (_, d) in got.items()}})
        sums[i] = {k: v for k, (v, _) in got.items()}
    return rows, sums


def stock_arrays(p, ticker: str) -> dict:
    a = plan_exit.stock_arrays(p, ticker)
    a["atr22"] = true_range_mean(a["high"], a["low"], a["close"], 22)
    a["atr42"] = true_range_mean(a["high"], a["low"], a["close"], 42)
    return a


def cohort_days(n: int) -> list[int]:
    return list(range(rankers.YEAR + rankers.MONTH, n - WINDOW - 1, EVERY))


def run(p, rf: np.ndarray, workers: int | None = None) -> tuple[list[dict], dict[int, dict[str, np.ndarray]]]:
    """All trades and the mean value path per cohort and policy."""
    arr, stocks = rankers.arrays(p)
    days = cohort_days(len(p.dates))
    members = {t: [] for t in stocks}
    for i in days:
        for j in rankers._universe(arr, i):
            members[stocks[int(j)]].append(i)
    jobs = [(stock_arrays(p, t), ds, rf) for t, ds in members.items() if ds]
    rows, sums, counts = [], {}, {}
    with ProcessPoolExecutor(max_workers=workers) as ex:
        for rs, ss in ex.map(_stock, jobs, chunksize=4):
            rows += rs
            for i, paths in ss.items():
                acc = sums.setdefault(i, {k: np.zeros(WINDOW) for k in POLICIES})
                for k, v in paths.items():
                    acc[k] += v
                counts[i] = counts.get(i, 0) + 1
    for r in rows:
        r["date"] = p.dates[r["i"]]
    means = {i: {k: v / counts[i] for k, v in acc.items()} for i, acc in sums.items()}
    return rows, means


def portfolio(means: dict[int, dict[str, np.ndarray]], n: int, policy: str) -> pd.Series:
    """Daily value of three staggered sleeves (each rolls 60-session cohorts, cost paid at each buy),
    from the first day all sleeves are invested; indexed by panel position."""
    days = sorted(means)
    vals = np.full((SLEEVES, n), np.nan)
    for j in range(SLEEVES):
        nav = 1.0
        for i in days[j::SLEEVES]:
            v = means[i][policy] * (1 - COST)
            vals[j, i + 1:i + 1 + WINDOW] = nav * v
            nav *= v[-1]
    covered = np.isfinite(vals).all(axis=0)
    idx = np.flatnonzero(covered)
    if not idx.size:
        return pd.Series(dtype=float)
    lo, hi = idx[0], idx[-1]
    block = vals[:, lo:hi + 1]
    return pd.Series((block / block[:, :1]).mean(axis=0), index=np.arange(lo, hi + 1))


def stats(nav: pd.Series, rf: np.ndarray) -> dict:
    """Portfolio statistics of a daily value series (index = panel positions)."""
    if len(nav) < 30:
        return {}
    r = nav.pct_change().dropna()
    ex = r - rf[r.index]
    down = ex[ex < 0]
    years = len(r) / 252
    dd = nav / nav.cummax() - 1
    roll = nav / nav.shift(63) - 1
    return {"cagr": round(float(nav.iloc[-1] / nav.iloc[0]) ** (1 / years) - 1, 5),
            "vol": round(float(r.std(ddof=1) * np.sqrt(252)), 5),
            "sharpe": round(float(ex.mean() / ex.std(ddof=1) * np.sqrt(252)), 3),
            "sortino": round(float(ex.mean() / np.sqrt((down ** 2).mean()) * np.sqrt(252)), 3) if len(down) else None,
            "max_dd": round(float(dd.min()), 5), "worst_63": round(float(roll.min()), 5)}


def trade_stats(rows: list[dict], k: str) -> dict:
    x = np.array([r[f"r_{k}"] for r in rows])
    days = np.array([r[f"d_{k}"] for r in rows])
    worst = np.sort(x)[: max(1, int(len(x) * WORST))]
    sd = x.std(ddof=1)
    return {"n": len(x), "mean": round(float(x.mean()), 5), "median": round(float(np.median(x)), 5),
            "worst5": round(float(worst.mean()), 5), "skew": round(float(((x - x.mean()) ** 3).mean() / sd ** 3), 3),
            "win": round(float((x > 0).mean()), 3), "early": round(float((days < WINDOW).mean()), 3),
            "days": round(float(days.mean()), 1)}


def vs_hold(rows: list[dict], k: str) -> dict:
    mean, t, months = monthly_t([r["date"] for r in rows], [r[f"r_{k}"] - r["r_hold"] for r in rows])
    return {"edge": None if mean is None else round(mean, 5), "t": None if t is None else round(t, 2), "months": months}


def summarize(rows: list[dict], means: dict, rf: np.ndarray, n: int, dates) -> dict:
    cohorts = sorted(means)
    split = cohorts[len(cohorts) // 2]
    halves = {"first": [r for r in rows if r["i"] < split], "second": [r for r in rows if r["i"] >= split]}
    out: dict = {"n": len(rows), "cohorts": len(cohorts), "first": str(dates[cohorts[0]]), "split": str(dates[split]),
                 "last": str(dates[cohorts[-1]]), "policies": {}}
    for k in POLICIES:
        nav = portfolio(means, n, k)
        cut = split + 1
        out["policies"][k] = {
            "label": POLICIES[k], "portfolio": stats(nav, rf),
            "portfolio_first": stats(nav[nav.index < cut], rf), "portfolio_second": stats(nav[nav.index >= cut], rf),
            "trades": trade_stats(rows, k),
            **({"vs_hold": vs_hold(rows, k), "vs_hold_first": vs_hold(halves["first"], k),
                "vs_hold_second": vs_hold(halves["second"], k)} if k != "hold" else {})}
    out["decision"] = decide(out["policies"])
    return out


def decide(pol: dict) -> dict:
    """The pre-registered rule: Sharpe not below holding in both halves, max drawdown at least
    MIN_DD_GAIN smaller, worst 5% of trades smaller losses; the highest full-sample Sharpe wins."""
    h = pol["hold"]
    checks = {}
    for k in STOPS:
        x = pol[k]
        checks[k] = {
            "sharpe_halves": x["portfolio_first"]["sharpe"] >= h["portfolio_first"]["sharpe"]
                             and x["portfolio_second"]["sharpe"] >= h["portfolio_second"]["sharpe"],
            "drawdown": x["portfolio"]["max_dd"] >= h["portfolio"]["max_dd"] + MIN_DD_GAIN,
            "worst_trades": x["trades"]["worst5"] > h["trades"]["worst5"],
        }
    ok = [k for k in STOPS if all(checks[k].values())]
    pick = max(ok, key=lambda k: pol[k]["portfolio"]["sharpe"]) if ok else None
    return {"checks": checks, "candidates": ok, "pick": pick}


def rf_daily(dates, series: list[tuple[date, float]]) -> np.ndarray:
    """Per-session T-bill return, from the yield known the day before (percent, annual / 252)."""
    s = pd.Series({d: v for d, v in series}, dtype=float).sort_index()
    s.index = pd.to_datetime(s.index)
    y = s.reindex(pd.to_datetime(dates), method="ffill").shift(1).fillna(0.0)
    return (y / 100 / 252).to_numpy()


def report_text(res: dict) -> str:
    pct = lambda x: "—" if x is None else f"{x:+.2%}"  # noqa: E731
    lines = [f"卖出方式 vs 一直持有：{res['first']} 至 {res['last']}，{res['cohorts']} 批、{res['n']} 笔买入（每 {EVERY} 个交易日全池买入，"
             f"窗口 {WINDOW} 日），按 {res['split']} 分前后两半；提前卖出的钱按 3 个月美债计息，每笔扣 {COST:.1%}",
             "方案 | 年化 | 夏普 全部/前半/后半 | 最大回撤 全部/前半/后半 | 最差 63 日 | 每笔均值 | 最差 5% | 偏度 | 提前卖出 | 平均天数 | 相对持有（t）"]
    for k, v in res["policies"].items():
        pf, a, b, t = v["portfolio"], v["portfolio_first"], v["portfolio_second"], v["trades"]
        vh = v.get("vs_hold")
        lines.append(f"{v['label']} | {pct(pf['cagr'])} | {pf['sharpe']:.2f} / {a['sharpe']:.2f} / {b['sharpe']:.2f} | "
                     f"{pct(pf['max_dd'])} / {pct(a['max_dd'])} / {pct(b['max_dd'])} | {pct(pf['worst_63'])} | {pct(t['mean'])} | "
                     f"{pct(t['worst5'])} | {t['skew']:.2f} | {t['early']:.0%} | {t['days']:.0f} | "
                     + (f"{pct(vh['edge'])}（t {vh['t']}）" if vh else "—"))
    d = res["decision"]
    zh = {"sharpe_halves": "前后两半夏普不低于持有", "drawdown": f"最大回撤至少小 {MIN_DD_GAIN:.0%}", "worst_trades": "最差 5% 交易亏得更少"}
    for k, c in d["checks"].items():
        lines.append(f"{POLICIES[k]}：" + "；".join(f"{'✓' if ok else '✗'} {zh[n]}" for n, ok in c.items()))
    lines.append(f"结论：推荐 {POLICIES[d['pick']]}（需用户确认后才改持仓计划）" if d["pick"] else "结论：没有方案满足事先定的条件，不推荐替换")
    return "\n".join(lines)
