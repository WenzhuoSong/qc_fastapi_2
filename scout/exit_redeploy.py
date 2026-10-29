"""Exit, then redeploy (pre-registered in `scout/knowledge/exit_specs/exit_redeploy.yaml`).

The same buys as `scout.exit_vs_hold`; when an exit rule sells, the money goes into the pool
(equal weight) or the latest picks of the low-asset-growth ranking until the window ends,
optionally filtered by the market (Faber's 10-month rule on SPY: no new buys and no
redeployment while risk is off) and by sector strength (redeploy only into stocks whose
sector ETF beat SPY over 63 sessions). Every series is built from data known at the previous
close; the redeployed basket is rebalanced daily and carries no stop.
"""
from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor

import numpy as np
import pandas as pd

from scout import exit_vs_hold as ev
from scout import rankers
from scout.universe import BENCHMARK, SECTOR_ETFS

SPEC_KEY = "exit_redeploy"
PICKS_SPEC = {"key": "rank_low_asset_growth", "ranker": "low_asset_growth", "top_frac": 0.1, "hold": 63, "every": 21}
MONTHS = 10
SECTOR_DAYS = 63
MIN_T = -2.0
EXITS = {"plan": "现行计划", "atr10": "10×ATR42", "trail20": "回落 20%"}
DESTS = {"pool": "换入股票池", "picks": "换入资产增长选股"}
FILTERS = {"none": "不过滤", "market": "大盘过滤", "market_sector": "大盘 + 行业过滤"}
VARIANTS = [f"{e}|{d}|{f}" for e in EXITS for d in DESTS for f in FILTERS]
LABELS = {"hold": "一直持有（基准）", "hold_market": "一直持有 + 大盘过滤",
          **{v: " · ".join((EXITS[e], DESTS[d], FILTERS[f])) for v in VARIANTS for e, d, f in [v.split("|")]}}


def market_off(close: np.ndarray, dates) -> np.ndarray:
    """Risk off after the close of each session: the latest month-end close below the mean of the
    last MONTHS month-end closes (Faber 2007); False until MONTHS month ends exist."""
    d = pd.to_datetime(pd.Index(dates))
    month_end = np.r_[d.month[1:] != d.month[:-1], False]
    off = np.zeros(len(close), dtype=bool)
    ends: list[float] = []
    state = False
    for t in range(len(close)):
        if month_end[t] and np.isfinite(close[t]):
            ends.append(float(close[t]))
            if len(ends) >= MONTHS:
                state = close[t] < np.mean(ends[-MONTHS:])
        off[t] = state
    return off


def series(p, angle: dict, rf: np.ndarray) -> tuple[np.ndarray, dict[tuple[str, str], np.ndarray]]:
    """(risk off after each close, {(destination, filter): daily return of the redeployed money})."""
    a, stocks = rankers.arrays(p)
    a["angle"] = angle
    c = a["close"]
    n = len(p.dates)
    ret = np.full_like(c, np.nan)
    ret[1:] = c[1:] / c[:-1] - 1
    spy = p["close"][BENCHMARK].to_numpy(dtype=float)
    off = market_off(spy, p.dates)
    spy63 = spy / np.roll(spy, SECTOR_DAYS) - 1
    spy63[:SECTOR_DAYS] = np.nan
    strong_sector = {}
    for sector, etf in SECTOR_ETFS.items():
        x = p["close"][etf].to_numpy(dtype=float) if etf in p["close"].columns else np.full(n, np.nan)
        r63 = x / np.roll(x, SECTOR_DAYS) - 1
        r63[:SECTOR_DAYS] = np.nan
        strong_sector[sector] = np.nan_to_num(r63 - spy63, nan=-1.0) > 0      # too little history = not strong
    sector_of = [p.sectors.get(t) for t in stocks]
    strong = np.zeros_like(c, dtype=bool)
    for j, sec in enumerate(sector_of):
        if sec in strong_sector:
            strong[:, j] = strong_sector[sec]
    rebal = rankers.rebalance_dates(n, PICKS_SPEC)
    fn = rankers.RANKERS[PICKS_SPEC["ranker"]]
    picks_on = {i: fn(a, i, PICKS_SPEC["top_frac"]) for i in rebal}
    out = {k: rf.copy() for k in [(d, f) for d in DESTS for f in FILTERS]}
    current: list[int] = []
    nxt = 0
    for t in range(1, n):
        while nxt < len(rebal) and rebal[nxt] <= t - 1:
            current = picks_on[rebal[nxt]]
            nxt += 1
        uni = rankers._universe(a, t - 1)
        for dest, cols in (("pool", uni), ("picks", np.array(current, dtype=int))):
            if not len(cols):
                continue
            r = ret[t, cols]
            ok = np.isfinite(r)
            if ok.any():
                out[(dest, "none")][t] = out[(dest, "market")][t] = float(r[ok].mean())
            sel = ok & strong[t - 1, cols]
            if sel.any():
                out[(dest, "market_sector")][t] = float(r[sel].mean())
        if off[t - 1]:
            for dest in DESTS:
                out[(dest, "market")][t] = out[(dest, "market_sector")][t] = rf[t]
    return off, out


_G: dict = {}


def _init(shared: dict) -> None:
    _G.update(shared)


def _cash(rf: np.ndarray, s: int) -> np.ndarray:
    """Uninvested money over the window, grossed up so the portfolio's per-buy cost cancels."""
    return np.cumprod(1 + rf[s:s + ev.WINDOW]) / (1 - ev.COST)


def variants(base: dict[str, tuple[np.ndarray, int]], s: int, off_at_buy: bool, rf: np.ndarray,
             growth: dict[tuple[str, str], np.ndarray]) -> dict[str, tuple[np.ndarray, int, bool]]:
    """{name: (value path, sessions in the stock, invested)} for one buy on bar s."""
    w = ev.WINDOW
    out = {"hold": (*base["hold"], True)}
    out["hold_market"] = (_cash(rf, s), 0, False) if off_at_buy else out["hold"]
    for name in VARIANTS:
        e, d, f = name.split("|")
        if f != "none" and off_at_buy:
            out[name] = (_cash(rf, s), 0, False)
            continue
        v, days = base[e]
        if days < w:
            k = days - 1
            tail = v[k] * (1 - ev.COST) * np.cumprod(1 + growth[(d, f)][s + k + 1:s + w])
            v = np.concatenate([v[:k + 1], tail])
        out[name] = (v, days, True)
    return out


def _stock(args: tuple[dict, list[int]]) -> tuple[list[dict], dict[int, dict[str, np.ndarray]]]:
    a, days = args
    rf, off, growth = _G["rf"], _G["off"], _G["growth"]
    rows, sums = [], {}
    for i in days:
        base = ev.trades(a, i + 1, rf)
        if base is None:
            continue
        got = variants(base, i + 1, bool(off[i]), rf, growth)
        rows.append({"i": i, **{f"r_{k}": float(v[-1] - 1 - (ev.COST if inv else 0.0)) for k, (v, _, inv) in got.items()},
                     **{f"d_{k}": d for k, (_, d, _) in got.items()}})
        sums[i] = {k: v for k, (v, _, _) in got.items()}
    return rows, sums


def run(p, rf: np.ndarray, angle: dict, workers: int | None = None) -> tuple[list[dict], dict, np.ndarray]:
    off, growth = series(p, angle, rf)
    arr, stocks = rankers.arrays(p)
    days = ev.cohort_days(len(p.dates))
    members = {t: [] for t in stocks}
    for i in days:
        for j in rankers._universe(arr, i):
            members[stocks[int(j)]].append(i)
    jobs = [(ev.stock_arrays(p, t), ds) for t, ds in members.items() if ds]
    rows, sums, counts = [], {}, {}
    with ProcessPoolExecutor(max_workers=workers, initializer=_init, initargs=({"rf": rf, "off": off, "growth": growth},)) as ex:
        for rs, ss in ex.map(_stock, jobs, chunksize=4):
            rows += rs
            for i, paths in ss.items():
                acc = sums.setdefault(i, {k: np.zeros(ev.WINDOW) for k in LABELS})
                for k, v in paths.items():
                    acc[k] += v
                counts[i] = counts.get(i, 0) + 1
    for r in rows:
        r["date"] = p.dates[r["i"]]
    return rows, {i: {k: v / counts[i] for k, v in acc.items()} for i, acc in sums.items()}, off


def decide(pol: dict) -> dict:
    """exit_vs_hold's three conditions, plus: not significantly behind holding in either half (t ≥ MIN_T)."""
    d = ev.decide(pol, VARIANTS)
    for k in VARIANTS:
        ts = (pol[k]["vs_hold_first"]["t"], pol[k]["vs_hold_second"]["t"])
        d["checks"][k]["not_behind"] = all(t is not None and t >= MIN_T for t in ts)
    d["candidates"] = [k for k in VARIANTS if all(d["checks"][k].values())]
    d["pick"] = max(d["candidates"], key=lambda k: pol[k]["portfolio"]["sharpe"]) if d["candidates"] else None
    return d


def summarize(rows: list[dict], means: dict, rf: np.ndarray, n: int, dates, off: np.ndarray) -> dict:
    out = ev.evaluate(rows, means, rf, n, dates, LABELS)
    out["decision"] = decide(out["policies"])
    days = sorted(means)
    out["risk_off_share"] = round(float(off[days].mean()), 3)
    return out


def report_text(res: dict) -> str:
    pct = lambda x: "—" if x is None else f"{x:+.2%}"  # noqa: E731
    lines = [f"卖出后换仓：{res['first']} 至 {res['last']}，{res['cohorts']} 批、{res['n']} 笔买入，按 {res['split']} 分前后两半；"
             f"大盘过滤在 {res['risk_off_share']:.0%} 的批次处于避险",
             "方案 | 年化 | 夏普 全部/前半/后半 | 最大回撤 | 最差 63 日 | 每笔均值 | 最差 5% | 相对持有 前半 / 后半（t）"]
    for k, v in res["policies"].items():
        pf, a, b, t = v["portfolio"], v["portfolio_first"], v["portfolio_second"], v["trades"]
        rel = (f"{pct(v['vs_hold_first']['edge'])}（{v['vs_hold_first']['t']}）/ {pct(v['vs_hold_second']['edge'])}（{v['vs_hold_second']['t']}）"
               if "vs_hold" in v else "—")
        lines.append(f"{v['label']} | {pct(pf['cagr'])} | {pf['sharpe']:.3f} / {a['sharpe']:.3f} / {b['sharpe']:.3f} | {pct(pf['max_dd'])} | "
                     f"{pct(pf['worst_63'])} | {pct(t['mean'])} | {pct(t['worst5'])} | {rel}")
    d = res["decision"]
    zh = {"sharpe_halves": "两半夏普", "drawdown": "回撤", "worst_trades": "最差 5%", "not_behind": "不显著落后"}
    for k, c in d["checks"].items():
        lines.append(f"{LABELS[k]}：" + " ".join(f"{'✓' if ok else '✗'}{zh[n]}" for n, ok in c.items()))
    lines.append(f"结论：推荐 {LABELS[d['pick']]}（需用户确认后才改持仓计划）" if d["pick"]
                 else "结论：没有方案满足事先定的条件，不推荐替换")
    return "\n".join(lines)
