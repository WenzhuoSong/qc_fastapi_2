"""Scout's rule-based system v2 (pre-registered in `scout/knowledge/external_specs/scout_system_v2.yaml`).

Same entries, slots, sizing, filter and simulation as v1 (`scout.scout_system`); a position is held while its relative
strength stays in the top half of the day's universe (the score the entry uses), and sold at the next open once it
drops below 0.5 or leaves the universe (variant rank_trail: or the 20 % trailing stop fires first).
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from scout import cross_asset as ca
from scout import exit_redeploy as er
from scout import scout_system as ss
from scout import simulate as simm
from scout.detectors import FIXED_EXIT
from scout.detectors.rules import _breakout_retest_levels

KEY = "scout_system_v2"
KEEP_RS = 0.5
T_BAR = 2.5
EXITS = {"rank": "相对强度掉出前一半卖出", "rank_trail": "同上，加 20% 移动止损（先触发为准）"}


def rank_exit(rs: np.ndarray, c: np.ndarray, o: np.ndarray, j: int, f: int, last: int | None = None) -> tuple[int, float] | None:
    """From the fill day f: the first close with relative strength < KEEP_RS or outside the universe (NaN) → sale at the
    next open; a stock that stopped trading is settled the day after its last session."""
    seg = rs[f:, j]
    hit = np.flatnonzero(~(seg >= KEEP_RS))            # NaN (left the universe) counts as out
    t = f + hit[0] + 1 if hit.size else None
    if last is not None and f <= last < len(c) - 1 and (t is None or t > last + 1):
        return last + 1, float(c[last + 1, j])
    if t is None or t >= len(c):
        return None
    return t, float(o[t, j] if np.isfinite(o[t, j]) else c[t - 1, j])


def first(*exits) -> tuple[int, float] | None:
    got = [e for e in exits if e is not None]
    return min(got, key=lambda e: e[0]) if got else None


def exit_for(key: str, rs, c, o, j: int, f: int, last: int | None):
    r = rank_exit(rs, c, o, j, f, last)
    return r if key == "rank" else first(r, ss.trail_exit(c, o, j, f, last))


def signals(d: dict, rs_out: list) -> list[dict]:
    """v1's breakout-retest fills, with the v2 exits."""
    cond, pivot, atr, rs = ss.conditions(d)
    rs_out.append(rs)
    o, h, lo, c = d["o"], d["h"], d["l"], d["c"]
    n = len(c)
    out = []
    for b, j in zip(*np.nonzero(cond)):
        if b < d["start"] or b + 1 >= n or not (np.isfinite(atr[b, j]) and np.isfinite(pivot[b, j]) and pivot[b, j] > 0):
            continue
        k = 100.0 / pivot[b, j]
        lv = _breakout_retest_levels({"atr14": atr[b, j] * k, "high_55_prior": pivot[b, j] * k})
        if not lv.valid():
            continue
        span = range(b + 1, min(n, b + 1 + ss.TRIGGER + 1))
        bars = [simm.Bar(i, o[i, j] * k, h[i, j] * k, lo[i, j] * k, c[i, j] * k) for i in span
                if np.isfinite(o[i, j]) and np.isfinite(h[i, j]) and np.isfinite(lo[i, j]) and np.isfinite(c[i, j])]
        res = simm.simulate(bars, lv, ss.HORIZON, trigger_window=ss.TRIGGER, exit_policy=FIXED_EXIT)
        if res.entry_date is None:
            continue
        f, last = int(res.entry_date), int(d["last_day"][j])
        out.append({"j": int(j), "signal": int(b), "fill": f, "price": res.entry_price / k, "rs": float(rs[b, j]),
                    "exit": {e: exit_for(e, rs, c, o, j, f, last) for e in EXITS}})
    return sorted(out, key=lambda s: (s["fill"], -s["rs"]))


def run() -> dict:
    d = ss.load()
    dates = d["dates"]
    rfs = pd.Series(d["rf"], index=dates)
    off = er.market_off(d["spy"], dates)
    holder: list = []
    sig = signals(d, holder)
    rs = holder[0]
    fills: dict[int, list[dict]] = {}
    for s in sig:
        fills.setdefault(s["fill"], []).append(s)
    cf = pd.DataFrame(d["c"]).ffill().to_numpy()
    spy = pd.Series(d["spy"], index=dates)[dates >= dates[d["start"]]]
    spy_nav = spy / spy.iloc[0]
    on = pd.Series(~off, index=dates).shift(1).fillna(False).astype(bool)
    filt = (1 + spy.pct_change().where(on[spy.index], rfs[spy.index]).fillna(0)).cumprod()
    out = {"key": KEY, "start": str(dates[d["start"]].date()), "end": str(dates[-1].date()), "names": len(d["names"]),
           "signals": len(sig), "benches": {"spy": {**ss.halves(spy_nav, rfs), "bears": ss.period_returns(spy_nav)},
                                            "spy_filter": {**ss.halves(filt, rfs), "bears": ss.period_returns(filt)}},
           "variants": {}}
    for e in EXITS:
        nav, trades, taken = ss.run_portfolio(d, fills, e, ss.COST, off)
        navs = pd.Series(nav, index=dates)
        high = pd.Series(ss.run_portfolio(d, fills, e, ss.COST_HIGH, off)[0], index=dates)
        ctrl = []
        for r in range(ss.REPLICATIONS):
            fn = (lambda j, t, e=e: exit_for(e, rs, cf, d["o"], j, t, int(d["last_day"][j])))
            cn = pd.Series(ss.run_portfolio(d, fills, e, ss.COST, off, {"rng": np.random.default_rng([ss.SEED, r, len(e)]),
                                                                       "buys": taken, "exit_fn": fn})[0], index=dates)
            ctrl.append(ss.monthly(cn))
        cm = pd.concat(ctrl, axis=1).mean(axis=1)
        mine = ss.monthly(navs)
        diff = (mine - cm.reindex(mine.index)).dropna()
        held = pd.Series(0.0, index=dates)
        for t in trades:
            held.iloc[t["entry"]:t["exit"]] += 1
        rets = np.array([t["ret"] for t in trades])
        yrs = (len(dates) - d["start"]) / 252
        ctrl_nav = (1 + cm).cumprod()
        live = navs[navs.index >= dates[d["start"]]]
        out["variants"][e] = {
            **ss.halves(navs, rfs), "bears": ss.period_returns(navs), "high_cost_cagr": ss.stats(high, rfs).get("cagr"),
            "max_abs_day": float(navs.pct_change().abs().max()),          # bookkeeping check: one slot is 20 %
            "control": {"cagr": float(ctrl_nav.iloc[-1] ** (12 / len(cm)) - 1), "monthly_mean": float(cm.mean())},
            "vs_control": {"all": ca.tstat(diff), "first": float(diff[diff.index < ss.SPLIT].mean()),
                           "second": float(diff[diff.index >= ss.SPLIT].mean())},
            "trades": {"n": len(trades), "per_year": len(trades) / yrs, "mean": float(rets.mean()) if len(rets) else None,
                       "win": float((rets > 0).mean()) if len(rets) else None,
                       "median_days": float(np.median([t["exit"] - t["entry"] for t in trades])) if trades else None,
                       "avg_held": float(held.iloc[d["start"]:].mean())},
            "years": {int(y): float(g.dropna().iloc[-1] / g.dropna().iloc[0] - 1)
                      for y, g in live.groupby(live.index.year) if g.dropna().size > 1},
        }
    out["decision"] = decide(out)
    return out


def decide(res: dict) -> dict:
    sf, spy = res["benches"]["spy_filter"], res["benches"]["spy"]
    out = {}
    for e, v in res["variants"].items():
        vc = v["vs_control"]
        checks = {"sharpe_halves": all(v[h]["sharpe"] > sf[h]["sharpe"] for h in ("first", "second")),
                  "vs_random": bool(vc["first"] > 0 and vc["second"] > 0 and (vc["all"].get("t") or 0) >= T_BAR),
                  "drawdown": v["all"]["max_dd"] >= spy["all"]["max_dd"]}
        out[e] = {**checks, "pass": all(checks.values())}
    return out


def b0_correlation(res: dict, b0: dict | None) -> dict:
    if not b0:
        return {}
    by = b0["years"].get("B0 信号（近似）", {})
    out = {}
    for e, v in res["variants"].items():
        ys = sorted(set(map(int, by)) & set(map(int, v["years"])))
        a = [by[str(y)] if str(y) in by else by[y] for y in ys]
        b = [v["years"][str(y)] if str(y) in v["years"] else v["years"][y] for y in ys]
        out[e] = {"years": len(ys), "corr": float(np.corrcoef(a, b)[0, 1]) if len(ys) > 2 else None}
    return out


def report_text(res: dict, b0: dict | None = None) -> str:
    p = ss._p
    lines = [f"Scout 系统 v2（预登记 {KEY}，只跑一次）：{res['start']} 到 {res['end']}，突破回踩成交 {res['signals']} 次；"
             "买入同 v1，持有到相对强度掉出股票池前一半", ""]

    def row(name, x):
        return (f"{name}：年化 {p(x['all'].get('cagr'))}，夏普 {x['all'].get('sharpe', 0):.2f}（前 {x['first'].get('sharpe', 0):.2f} / "
                f"后 {x['second'].get('sharpe', 0):.2f}），最大回撤 {p(x['all'].get('max_dd'), 0)}，最差 63 日 {p(x['all'].get('worst_63'), 0)}")
    corr = b0_correlation(res, b0)
    for e, v in res["variants"].items():
        tr, vc = v["trades"], v["vs_control"]
        lines.append(row(f"系统 · {EXITS[e]}", v))
        lines.append(f"  每年 {tr['per_year']:.0f} 笔，平均持有 {tr['avg_held']:.1f} 只，持有中位数 {tr['median_days']:.0f} 个交易日，"
                     f"每笔 {p(tr['mean'])}、赚钱比例 {tr['win'] * 100:.0f}%；成本每边 0.2% 时年化 {p(v['high_cost_cagr'])}")
        lines.append(f"  相对随机选股（同样规则）每月 {p(vc['all']['mean'], 2)}（t {vc['all']['t']:.2f}；前 {p(vc['first'], 2)} / 后 {p(vc['second'], 2)}）；"
                     f"随机对照年化 {p(v['control']['cagr'])}")
        lines.append("  熊市：" + "；".join(f"{k} {p(x)}" for k, x in v["bears"].items()))
        if corr.get(e, {}).get("corr") is not None:
            lines.append(f"  与 B0 回放逐年收益的相关系数 {corr[e]['corr']:.2f}（{corr[e]['years']} 年）")
    lines.append(row("对照 · SPY 买入持有", res["benches"]["spy"]))
    lines.append(row("对照 · SPY + 大盘过滤", res["benches"]["spy_filter"]))
    if b0:
        f = b0["full"].get("B0 信号（近似）", {})
        lines.append(f"参考 · B0 回放（信号近似，股票池不同）：年化 {p(f.get('cagr'))}，夏普 {f.get('sharpe')}，最大回撤 {p(f.get('max_dd'), 0)}")
    lines.append("\n判定：" + "；".join(
        f"{EXITS[e]} → {'通过' if x['pass'] else '不通过'}（两半夏普高于 SPY + 过滤 {'✓' if x['sharpe_halves'] else '✗'}，"
        f"好于随机选股 t ≥ {T_BAR} {'✓' if x['vs_random'] else '✗'}，回撤不深于 SPY {'✓' if x['drawdown'] else '✗'}）"
        for e, x in res["decision"].items()))
    return "\n".join(lines)
