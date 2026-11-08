"""Strong stocks bought directly and held (pre-registered in `scout/knowledge/external_specs/strong_stock_hold_v1.yaml`).

Local only, on the Sharadar files (`scout_system.load`: point-in-time top 1000 from 1999, dividend-adjusted bars,
delistings settled). Every 21 sessions, while the market filter is on, the members with relative strength in the
top 20 %, close above the 200-day average, 50-day above 200-day and a 63-day gain over 10 % (the strength half of
`rules.D2_PULLBACK`, Scout's formulas) are bought at the next open, equal weight, and held under the 20 % trailing
stop (`scout_system.trail_exit`) for at most 252 sessions; money that left earns nothing until the window ends.
Select control = as many random non-qualifying members, bought and sold the same way, mean of 20 draws.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from scout import exit_redeploy as er
from scout import gate
from scout import pullback_calm as pc
from scout import scout_system as ss
from scout import simulate as simm
from scout.small_cap_rankers import nw_t

KEY = "strong_stock_hold_v1"
EVERY, WINDOW, SHORT_WINDOW = 21, 252, 126
RS_MIN, RS_TOP10 = 0.80, 0.90
REPLICATIONS = 20
SEED = 20261108
SPLIT = pd.Timestamp("2013-01-01")
NW_LAGS = 12
TRIALS = 3                                    # family trend_template: rank_trend_template, its plan version, then this
COST = simm.ROUND_TRIP_COST
CRISIS = (pd.Timestamp("2008-01-01"), pd.Timestamp("2009-12-31"))
BEARS = ss.BEARS


def strong(f: dict[str, np.ndarray], i: int, j: int, rs_min: float = RS_MIN) -> bool:
    """The strength conditions of rules.D2_PULLBACK on day i (NaN = not met)."""
    return bool(f["rs_pct"][i, j] >= rs_min and f["close"][i, j] > f["sma200"][i, j] and f["sma50"][i, j] > f["sma200"][i, j]
                and f["ret_63d"][i, j] > 0.10)


def trade(d: dict, j: int, i: int, window: int) -> dict | None:
    """Bought at the open after day i, held under the trailing stop until the window's last close (day i + window):
    {ret (net), hold (net, no stop), days, stopped}; None when the stock cannot be bought."""
    o, c = d["o"], d["c"]
    f, end = i + 1, i + window
    if end >= len(c) or not (np.isfinite(o[f, j]) and o[f, j] > 0 and np.isfinite(c[end, j])):
        return None
    last = int(d["last_day"][j])
    out = ss.trail_exit(c[:end + 1], o[:end + 1], j, f, last if last < end else None)
    if out is None:
        t, px, stopped = end, float(c[end, j]), False
    else:
        t, px = out
        stopped = not (last < end and t == last + 1)
    return {"ret": px / o[f, j] - 1 - COST, "hold": c[end, j] / o[f, j] - 1 - COST, "days": t - f + (0 if out else 1),
            "stopped": stopped}


def formation(d: dict, f: dict, i: int, rng: np.random.Generator, window: int, rs_min: float) -> dict | None:
    pool = np.flatnonzero(d["member"][i])
    trades = {int(j): trade(d, int(j), i, window) for j in pool}
    trades = {j: t for j, t in trades.items() if t}
    picks = [j for j in trades if strong(f, i, j, rs_min)]
    chosen = set(picks)
    rest = np.array([j for j in trades if j not in chosen])
    if not picks or len(rest) < 2:
        return None
    k = min(len(picks), len(rest))
    ctl = [np.mean([trades[int(j)]["ret"] for j in rng.choice(rest, size=k, replace=False)]) for _ in range(REPLICATIONS)]
    ctl_hold = float(np.mean([trades[int(j)]["hold"] for j in rest]))
    sel = [trades[j] for j in picks]
    return {"date": d["dates"][i], "n": len(picks), "pool": len(trades), "ret": float(np.mean([t["ret"] for t in sel])),
            "control": float(np.mean(ctl)), "hold_same": float(np.mean([t["hold"] for t in sel])), "control_hold": ctl_hold,
            "trades": [{"ticker": d["names"][j], "ret": trades[j]["ret"], "hold": trades[j]["hold"], "days": trades[j]["days"],
                        "stopped": trades[j]["stopped"]} for j in picks]}


def run() -> dict:
    d = ss.load()
    f = pc.features(d)
    off = er.market_off(d["spy"], d["dates"])
    n = len(d["c"])
    days = list(range(d["start"], n, EVERY))
    out = {}
    for name, window, rs_min in (("main", WINDOW, RS_MIN), ("top10", WINDOW, RS_TOP10), ("w126", SHORT_WINDOW, RS_MIN)):
        rng = np.random.default_rng(SEED)
        rows, skipped = [], 0
        for i in days:
            if i + window >= n:
                break
            if off[i]:
                skipped += 1
                continue
            r = formation(d, f, i, rng, window, rs_min)
            if r:
                spy = d["spy"]
                r["spy"] = float(spy[i + window] / spy[i] - 1) if np.isfinite(spy[i]) and np.isfinite(spy[i + window]) else None
                rows.append(r)
        out[name] = {"rows": rows, "filter_off": skipped}
    spy = d["spy"]
    out["spy_all"] = [{"date": d["dates"][i], "spy": float(spy[i + WINDOW] / spy[i] - 1), "off": bool(off[i])}
                      for i in days if i + WINDOW < n and np.isfinite(spy[i]) and np.isfinite(spy[i + WINDOW])]
    return out


# ── evaluation ──

def edge(rows: list[dict], a: str = "ret", b: str = "control") -> dict:
    if not rows:
        return {"n": 0, "mean": None, "t": None}
    x = pd.Series([r[a] - r[b] for r in rows], index=pd.DatetimeIndex([r["date"] for r in rows]))
    return {"n": len(x), "mean": float(x.mean()), "t": nw_t(x, NW_LAGS)}


def evaluate(part: dict) -> dict:
    rows = part["rows"]
    first = [r for r in rows if pd.Timestamp(r["date"]) < SPLIT]
    second = [r for r in rows if pd.Timestamp(r["date"]) >= SPLIT]
    calm = [r for r in rows if not CRISIS[0] <= pd.Timestamp(r["date"]) <= CRISIS[1]]
    trades = [t for r in rows for t in r["trades"]]
    rets = np.array([t["ret"] for t in trades]) if trades else np.array([np.nan])
    holds = np.array([t["hold"] for t in trades]) if trades else np.array([np.nan])
    years = {}
    for r in rows:
        years.setdefault(pd.Timestamp(r["date"]).year, []).append(r)
    bears = {k: edge([r for r in rows if pd.Timestamp(a) <= pd.Timestamp(r["date"]) <= pd.Timestamp(b)]) for k, (a, b) in BEARS.items()}
    q = lambda x, p: float(np.nanpercentile(x, p))  # noqa: E731
    return {"formations": len(rows), "filter_off": part["filter_off"],
            "picks_median": float(np.median([r["n"] for r in rows])) if rows else None,
            "all": edge(rows), "first": edge(first), "second": edge(second), "ex_2008_09": edge(calm),
            "ret": float(np.mean([r["ret"] for r in rows])) if rows else None,
            "control": float(np.mean([r["control"] for r in rows])) if rows else None,
            "hold_same": float(np.mean([r["hold_same"] for r in rows])) if rows else None,
            "hold_same_vs_control_hold": edge(rows, "hold_same", "control_hold"),
            "stop_cost": edge(rows, "ret", "hold_same"),
            "spy": float(np.nanmean([r["spy"] for r in rows if r["spy"] is not None])) if rows else None,
            "vs_spy": edge([r for r in rows if r["spy"] is not None], "ret", "spy"),
            "days_median": float(np.median([t["days"] for t in trades])) if trades else None,
            "stopped": float(np.mean([t["stopped"] for t in trades])) if trades else None,
            "trade_ret": {"p10": q(rets, 10), "p50": q(rets, 50), "p90": q(rets, 90), "p1": q(rets, 1)},
            "trade_hold": {"p10": q(holds, 10), "p50": q(holds, 50), "p90": q(holds, 90), "p1": q(holds, 1)},
            "years": {y: edge(v) for y, v in sorted(years.items())}, "bears": bears}


def spy_portfolios(spy_all: list[dict]) -> dict:
    """Average 252-session SPY return over every formation day, and with the market filter (cash = 0 while off)."""
    if not spy_all:
        return {}
    return {"spy": float(np.mean([r["spy"] for r in spy_all])),
            "spy_filter": float(np.mean([0.0 if r["off"] else r["spy"] for r in spy_all]))}


def decide(ev: dict) -> dict:
    req = gate.required_t(TRIALS)
    a, f, s, x = ev["all"], ev["first"], ev["second"], ev["ex_2008_09"]
    checks = {"both_halves": bool((f["mean"] or 0) > 0 and (s["mean"] or 0) > 0),
              "nw_t": bool(a["t"] is not None and a["t"] >= req),
              "ex_2008_09": bool((x["mean"] or 0) > 0)}
    return {"passed": all(checks.values()), "checks": checks, "required_t": req}


def _p(x: float | None, dg: int = 2) -> str:
    return "—" if x is None else f"{x:+.{dg}%}"


def _t(x: float | None) -> str:
    return "—" if x is None else f"{x:.2f}"


def report_text(res: dict) -> str:
    ev, dec = res["evaluation"]["main"], res["decision"]
    e = lambda x: f"{_p(x['mean'])}（t {_t(x['t'])}，{x['n']} 期）"  # noqa: E731
    lines = [f"强势股直接买入、20% 移动止损持有（Sharadar 点时市值前 1000，含退市；每 21 个交易日建仓，最长 252 日；对照 = 同日不满足条件的随机成员 × 20）",
             f"本族（trend_template）第 {TRIALS} 次检验，Newey-West t 门槛 {dec['required_t']}；{'通过' if dec['passed'] else '未通过'} {dec['checks']}",
             f"建仓 {ev['formations']} 期（大盘过滤关闭跳过 {ev['filter_off']} 期），每期入选中位数 {ev['picks_median']}",
             f"入选平均 {_p(ev['ret'])}，对照 {_p(ev['control'])}；差值 全样本 {e(ev['all'])}；前半（–2012）{e(ev['first'])}；后半（2013–）{e(ev['second'])}；"
             f"去掉 2008–2009 {e(ev['ex_2008_09'])}",
             f"不止损持有满 252 日：入选 {_p(ev['hold_same'])}，对同样持有的对照 {e(ev['hold_same_vs_control_hold'])}；止损本身的影响（止损 − 不止损）{e(ev['stop_cost'])}",
             f"对同窗口 SPY {e(ev['vs_spy'])}（SPY 平均 {_p(ev['spy'])}）；所有建仓日 SPY {_p(res['spy'].get('spy'))}，SPY + 大盘过滤 {_p(res['spy'].get('spy_filter'))}",
             f"持有天数中位数 {ev['days_median']}，被止损 {ev['stopped']:.0%}；单笔（止损）10% / 50% / 90% 分位 {_p(ev['trade_ret']['p10'], 1)} / "
             f"{_p(ev['trade_ret']['p50'], 1)} / {_p(ev['trade_ret']['p90'], 1)}，最差 1% {_p(ev['trade_ret']['p1'], 1)}；"
             f"不止损 {_p(ev['trade_hold']['p10'], 1)} / {_p(ev['trade_hold']['p50'], 1)} / {_p(ev['trade_hold']['p90'], 1)}，最差 1% {_p(ev['trade_hold']['p1'], 1)}",
             "熊市建仓：" + "；".join(f"{k} {_p(v['mean'])}（{v['n']} 期）" for k, v in ev["bears"].items()),
             "逐年：" + "，".join(f"{y} {_p(v['mean'], 1)}" for y, v in ev["years"].items())]
    for name, label in (("top10", "只报告 · 相对强度前 10%"), ("w126", "只报告 · 持有窗口 126 日")):
        v = res["evaluation"][name]
        lines.append(f"{label}：差值 {e(v['all'])}，前半 {_p(v['first']['mean'])} / 后半 {_p(v['second']['mean'])}")
    return "\n".join(lines)
