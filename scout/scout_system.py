"""Scout's first complete rule-based system (pre-registered in `scout/knowledge/external_specs/scout_system_v1.yaml`).

Local only, on the Sharadar files. Every breakout-retest signal among the 1000 largest stocks (Scout's rule on
dividend-adjusted bars) gets the rule's limit order and fill (`scout.simulate`); a portfolio of at most 5
positions takes the fills in order of relative strength while slots are free and the market filter is on, sized
at 1/5 of the previous close's value; exits are the user's 20 % trailing stop (a) or the rule's own bracket (b).
A random-selection control buys the same number of random members on the same days with the same rules.
"""
from __future__ import annotations

import math
from datetime import date

import numpy as np
import pandas as pd

from scout import cross_asset as ca
from scout import exit_redeploy as er
from scout import exit_vs_hold as ev
from scout import sharadar_exits as sx
from scout import sharadar_research as sr
from scout import simulate as simm
from scout.detectors import FIXED_EXIT
from scout.detectors.rules import _breakout_retest_levels

KEY = "scout_system_v1"
FIRST = "1999-01-04"
EVERY = 21
SLOTS = 5
TRAIL_KEEP = 0.80
TRIGGER, HORIZON = 10, 20
COST, COST_HIGH = 0.0005, 0.0020
REPLICATIONS = 20
SEED = 20261103
SPLIT = pd.Timestamp("2013-01-01")
BEARS = {"2000-03..2002-09": ("2000-03-01", "2002-09-30"), "2007-10..2009-02": ("2007-10-01", "2009-02-28"),
         "2020-02..2020-03": ("2020-02-01", "2020-03-31"), "2022-01..2022-09": ("2022-01-01", "2022-09-30")}
EXITS = {"a": "20% 移动止损（你的持仓计划）", "b": "规则自己的出场（止损 / 2R 目标 / 20 日）"}


# ── data ──

def load() -> dict:
    dates = sr.trading_days()
    start = int(np.searchsorted(dates, pd.Timestamp(FIRST)))
    reb = list(range(start, len(dates), EVERY))
    uni = sr.universe(dates[reb], sr.eligible())
    names = sorted({t for d in uni.values() for t in d["large"]})
    p = sr.prices(names, dates)
    arr = [sx.settle(p, j) for j in range(len(names))]
    traded = np.isfinite(p.close)
    last_day = np.where(traded.any(axis=0), len(dates) - 1 - np.argmax(traded[::-1], axis=0), -1)
    o, h, lo, c, v = (np.column_stack([a[k] for a in arr]) for k in ("open", "high", "low", "close", "volume"))
    member = np.zeros(c.shape, dtype=bool)
    for k, i in enumerate(reb):
        end = reb[k + 1] if k + 1 < len(reb) else len(dates)
        cols = [p.col[t] for t in uni.get(dates[i], {}).get("large", []) if t in p.col]
        member[i:end, cols] = True
    spy = sr._read("funds", ["ticker", "date", "closeadj"], filters=[("ticker", "=", "SPY")])
    spy = pd.Series(spy["closeadj"].to_numpy(), index=pd.to_datetime(spy["date"])).reindex(dates)
    rf = ca.french_daily_rf().reindex(dates).fillna(0.0).to_numpy()
    last = int(np.flatnonzero(ca.french_daily_rf().reindex(dates).notna().to_numpy())[-1])   # rf known to here
    return {"dates": dates[:last + 1], "start": start, "o": o[:last + 1], "h": h[:last + 1], "l": lo[:last + 1],
            "c": c[:last + 1], "v": v[:last + 1], "member": member[:last + 1], "spy": spy.to_numpy()[:last + 1],
            "rf": rf[:last + 1], "names": names, "last_day": last_day}


# ── signals and fills (data up to each day only) ──

def conditions(d: dict) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """(signal mask, pivot = prior 55-day high, ATR14, relative-strength percentile within the members)."""
    o, h, lo, c, v = (pd.DataFrame(d[k]) for k in ("o", "h", "l", "c", "v"))
    prev = c.shift(1)
    tr = np.fmax(h - lo, np.fmax((h - prev).abs(), (lo - prev).abs()))
    atr = tr.rolling(14, min_periods=14).mean()
    hi55 = h.rolling(55, min_periods=55).max().shift(1)
    sma50, sma200 = c.rolling(50, min_periods=50).mean(), c.rolling(200, min_periods=200).mean()
    blend = 0.4 * (c / c.shift(63) - 1) + 0.3 * (c / c.shift(126) - 1) + 0.3 * (c / c.shift(252) - 1)
    rs = blend.where(d["member"]).rank(axis=1, pct=True)
    rng = (h - lo).replace(0, np.nan)
    cond = ((c > hi55) & (v / v.rolling(50, min_periods=20).mean().shift(1) >= 1.8) & (c > sma50) & (sma50 > sma200)
            & (rs >= 0.8) & (((c - lo) / rng).fillna(0.5) >= 0.6) & (o / prev - 1 < 0.04)) & d["member"]
    return cond.to_numpy(), hi55.to_numpy(), atr.to_numpy(), rs.to_numpy()


def trail_exit(c: np.ndarray, o: np.ndarray, j: int, f: int, last: int | None = None) -> tuple[int, float] | None:
    """The 20 % trailing stop from the fill day f: (day of the sale at the open, price), None while still held.
    A stock that stopped trading on day `last` is settled the next day at its settled price (`sharadar_exits.settle`:
    last close, with the delisting adjustment)."""
    seg = c[f:, j]
    ok = np.isfinite(seg)
    hw = np.maximum.accumulate(np.where(ok, seg, -np.inf))
    hit = np.flatnonzero(ok & (seg < TRAIL_KEEP * hw))
    t = f + hit[0] + 1 if hit.size else None
    if last is not None and f <= last < len(c) - 1 and (t is None or t > last + 1):
        return last + 1, float(c[last + 1, j])
    if t is None or t >= len(c):
        return None
    px = o[t, j] if np.isfinite(o[t, j]) else c[t - 1, j]
    return t, float(px)


def signals(d: dict) -> list[dict]:
    cond, pivot, atr, rs = conditions(d)
    o, h, lo, c = d["o"], d["h"], d["l"], d["c"]
    n = len(c)
    out = []
    for b, j in zip(*np.nonzero(cond)):
        if b < d["start"] or b + 1 >= n or not (np.isfinite(atr[b, j]) and np.isfinite(pivot[b, j]) and pivot[b, j] > 0):
            continue
        k = 100.0 / pivot[b, j]                          # levels round to cents: work near 100
        lv = _breakout_retest_levels({"atr14": atr[b, j] * k, "high_55_prior": pivot[b, j] * k})
        if not lv.valid():
            continue
        span = range(b + 1, min(n, b + 1 + TRIGGER + simm.FIXED_EXIT.holding_limit(HORIZON) + 1))
        bars = [simm.Bar(i, o[i, j] * k, h[i, j] * k, lo[i, j] * k, c[i, j] * k) for i in span
                if np.isfinite(o[i, j]) and np.isfinite(h[i, j]) and np.isfinite(lo[i, j]) and np.isfinite(c[i, j])]
        res = simm.simulate(bars, lv, HORIZON, trigger_window=TRIGGER, exit_policy=FIXED_EXIT)
        if res.entry_date is None:
            continue
        f = int(res.entry_date)
        rule = (int(res.exit_date), res.exit_price / k) if res.exit_date is not None and res.state != "open" else None
        out.append({"j": int(j), "signal": int(b), "fill": f, "price": res.entry_price / k, "rs": float(rs[b, j]),
                    "exit": {"a": trail_exit(c, o, j, f, int(d["last_day"][j])), "b": rule}})
    return sorted(out, key=lambda s: (s["fill"], -s["rs"]))


# ── portfolio ──

def run_portfolio(d: dict, fills: dict[int, list[dict]], exit_key: str, cost: float, off: np.ndarray,
                  control: dict | None = None) -> tuple[np.ndarray, list[dict], dict[int, list[dict]]]:
    """Daily value from `start`. `control` = {rng, buys: {day: [taken system trades]}, exit_fn?}: buy random members
    instead (exit_fn(j, day) gives their exit; default: v1's rules).
    Returns (nav, closed trades, taken: {day: [system trades taken]})."""
    o, c, rf = d["o"], pd.DataFrame(d["c"]).ffill().to_numpy(), d["rf"]
    n, start = len(c), d["start"]
    nav = np.full(n, np.nan)
    cash, pos = 1.0, {}                                  # pos: j -> {units, exit_day, exit_px}
    taken: dict[int, list[dict]] = {}
    trades = []
    for t in range(start, n):
        cash *= 1 + rf[t]
        held_today = len(pos)
        prev_nav = nav[t - 1] if t > start else 1.0
        free = SLOTS - held_today
        if free > 0 and not off[t - 1]:
            if control is None:
                for s in fills.get(t, []):
                    if free <= 0:
                        break
                    if s["j"] in pos:                 # held, or a second signal on the same stock filling today
                        continue
                    free -= 1
                    ex = s["exit"][exit_key]
                    size = min(prev_nav / SLOTS, cash)
                    if size <= 0:
                        break
                    cash -= size
                    pos[s["j"]] = {"units": size * (1 - cost) / s["price"], "px": s["price"], "entry": t,
                                   "exit_day": ex[0] if ex else n, "exit_px": ex[1] if ex else np.nan}
                    taken.setdefault(t, []).append(s)
            else:
                k = len(control["buys"].get(t, []))
                pool = [j for j in np.flatnonzero(d["member"][t - 1]) if j not in pos and np.isfinite(o[t, j]) and o[t, j] > 0]
                picks = control["rng"].choice(pool, size=min(k, free, len(pool)), replace=False) if k and pool else []
                for j, s in zip(picks, control["buys"].get(t, [])):
                    j = int(j)
                    if control.get("exit_fn"):
                        ex = control["exit_fn"](j, t)
                    elif exit_key == "a":
                        ex = trail_exit(c, o, j, t, int(d["last_day"][j]) if "last_day" in d else None)
                    else:
                        e = s["exit"]["b"]
                        ex = (e[0], float(c[e[0], j])) if e else None
                    size = min(prev_nav / SLOTS, cash)
                    if size <= 0:
                        break
                    cash -= size
                    pos[j] = {"units": size * (1 - cost) / o[t, j], "px": float(o[t, j]), "entry": t,
                              "exit_day": ex[0] if ex else n, "exit_px": ex[1] if ex else np.nan}
        # sales after the day's buys: a sale at the open or intraday is known by the close, and a position can be
        # sold on its own fill day; the money and the slot are used from the next day
        for j in [j for j, x in pos.items() if x["exit_day"] == t]:
            x = pos.pop(j)
            cash += x["units"] * x["exit_px"] * (1 - cost)
            trades.append({"j": j, "entry": x["entry"], "exit": t, "ret": x["exit_px"] / x["px"] - 1})
        nav[t] = cash + sum(x["units"] * c[t, j] for j, x in pos.items())
    return nav, trades, taken


# ── statistics ──

def stats(nav: pd.Series, rf: pd.Series) -> dict:
    nav = nav.dropna()
    r = nav.pct_change().dropna()
    if len(r) < 60:
        return {}
    ex = r - rf.reindex(r.index).fillna(0)
    return {"cagr": float((nav.iloc[-1] / nav.iloc[0]) ** (252 / len(r)) - 1), "vol": float(r.std() * math.sqrt(252)),
            "sharpe": float(ex.mean() / ex.std() * math.sqrt(252)), "max_dd": float((nav / nav.cummax() - 1).min()),
            "worst_63": float((nav / nav.shift(63) - 1).min())}


def halves(nav: pd.Series, rf: pd.Series) -> dict:
    return {"all": stats(nav, rf), "first": stats(nav[nav.index < SPLIT], rf), "second": stats(nav[nav.index >= SPLIT], rf)}


def period_returns(nav: pd.Series) -> dict:
    out = {}
    for k, (a, b) in BEARS.items():
        s = nav[(nav.index >= a) & (nav.index <= b)].dropna()
        out[k] = float(s.iloc[-1] / s.iloc[0] - 1) if len(s) > 1 else None
    return out


def monthly(nav: pd.Series) -> pd.Series:
    return nav.dropna().resample("ME").last().pct_change().dropna()


def run() -> dict:
    d = load()
    dates = d["dates"]
    rfs = pd.Series(d["rf"], index=dates)
    off = er.market_off(d["spy"], dates)
    sig = signals(d)
    fills: dict[int, list[dict]] = {}
    for s in sig:
        fills.setdefault(s["fill"], []).append(s)
    spy = pd.Series(d["spy"], index=dates)[dates >= dates[d["start"]]]
    spy_nav = spy / spy.iloc[0]
    on = pd.Series(~off, index=dates).shift(1).fillna(False).astype(bool)
    filt = (1 + spy.pct_change().where(on[spy.index], rfs[spy.index]).fillna(0)).cumprod()
    out = {"key": KEY, "start": str(dates[d["start"]].date()), "end": str(dates[-1].date()), "names": len(d["names"]),
           "signals": len(sig), "benches": {"spy": {**halves(spy_nav, rfs), "bears": period_returns(spy_nav)},
                                            "spy_filter": {**halves(filt, rfs), "bears": period_returns(filt)}},
           "variants": {}}
    for e in EXITS:
        nav, trades, taken = run_portfolio(d, fills, e, COST, off)
        navs = pd.Series(nav, index=dates)
        high = pd.Series(run_portfolio(d, fills, e, COST_HIGH, off)[0], index=dates)
        ctrl = []
        for r in range(REPLICATIONS):
            rng = np.random.default_rng([SEED, r, ord(e)])
            cn = pd.Series(run_portfolio(d, fills, e, COST, off, {"rng": rng, "buys": taken})[0], index=dates)
            ctrl.append(monthly(cn))
        cm = pd.concat(ctrl, axis=1).mean(axis=1)
        mine = monthly(navs)
        diff = (mine - cm.reindex(mine.index)).dropna()
        held = pd.Series([0] * len(dates), index=dates, dtype=float)
        for t in trades:
            held.iloc[t["entry"]:t["exit"]] += 1
        rets = np.array([t["ret"] for t in trades])
        yrs = (len(dates) - d["start"]) / 252
        ctrl_nav = (1 + cm).cumprod()
        out["variants"][e] = {
            **halves(navs, rfs), "bears": period_returns(navs), "high_cost_cagr": stats(high, rfs).get("cagr"),
            "control": {"cagr": float(ctrl_nav.iloc[-1] ** (12 / len(cm)) - 1), "monthly_mean": float(cm.mean())},
            "vs_control": {"all": ca.tstat(diff), "first": float(diff[diff.index < SPLIT].mean()),
                           "second": float(diff[diff.index >= SPLIT].mean())},
            "trades": {"n": len(trades), "per_year": len(trades) / yrs, "mean": float(rets.mean()) if len(rets) else None,
                       "win": float((rets > 0).mean()) if len(rets) else None,
                       "median_days": float(np.median([t["exit"] - t["entry"] for t in trades])) if trades else None,
                       "avg_held": float(held[d["start"]:].mean())},
            "years": {int(y): float(g.dropna().iloc[-1] / g.dropna().iloc[0] - 1)
                      for y, g in navs[navs.index >= dates[d["start"]]].groupby(navs[navs.index >= dates[d["start"]]].index.year)
                      if g.dropna().size > 1},
        }
    out["decision"] = decide(out)
    return out


def decide(res: dict) -> dict:
    sf, spy = res["benches"]["spy_filter"], res["benches"]["spy"]
    out = {}
    for e, v in res["variants"].items():
        vc = v["vs_control"]
        checks = {"sharpe_halves": all(v[h]["sharpe"] > sf[h]["sharpe"] for h in ("first", "second")),
                  "vs_random": bool(vc["first"] > 0 and vc["second"] > 0 and (vc["all"].get("t") or 0) >= 2),
                  "drawdown": v["all"]["max_dd"] >= spy["all"]["max_dd"]}
        out[e] = {**checks, "pass": all(checks.values())}
    return out


def _p(x, dgt: int = 1) -> str:
    return "—" if x is None else f"{x * 100:+.{dgt}f}%"


def report_text(res: dict, b0: dict | None = None) -> str:
    lines = [f"Scout 卫星系统 v1（预登记 {KEY}，只跑一次）：{res['start']} 到 {res['end']}，股票池涉及 {res['names']} 只，"
             f"突破回踩成交 {res['signals']} 次；前半 1999–2012，后半 2013–", ""]

    def row(name, x):
        return (f"{name}：年化 {_p(x['all'].get('cagr'))}，夏普 {x['all'].get('sharpe', 0):.2f}（前 {x['first'].get('sharpe', 0):.2f} / "
                f"后 {x['second'].get('sharpe', 0):.2f}），最大回撤 {_p(x['all'].get('max_dd'), 0)}，最差 63 日 {_p(x['all'].get('worst_63'), 0)}")
    for e, v in res["variants"].items():
        tr, vc = v["trades"], v["vs_control"]
        lines.append(row(f"系统 · {EXITS[e]}", v))
        lines.append(f"  每年 {tr['per_year']:.0f} 笔，平均持有 {tr['avg_held']:.1f} 只，持有中位数 {tr['median_days']:.0f} 个交易日，"
                     f"每笔 {_p(tr['mean'])}、赚钱比例 {tr['win'] * 100:.0f}%；成本每边 0.2% 时年化 {_p(v['high_cost_cagr'])}")
        lines.append(f"  相对随机选股（同样规则）每月 {_p(vc['all']['mean'], 2)}（t {vc['all']['t']:.2f}；前 {_p(vc['first'], 2)} / 后 {_p(vc['second'], 2)}）；"
                     f"随机对照年化 {_p(v['control']['cagr'])}")
        lines.append("  熊市：" + "；".join(f"{k} {_p(x)}" for k, x in v["bears"].items()))
    lines.append(row("对照 · SPY 买入持有", res["benches"]["spy"]))
    lines.append(row("对照 · SPY + 大盘过滤", res["benches"]["spy_filter"]))
    lines.append("  熊市（SPY / + 过滤）：" + "；".join(f"{k} {_p(res['benches']['spy']['bears'][k])} / {_p(res['benches']['spy_filter']['bears'][k])}"
                                                for k in BEARS))
    if b0:
        f = b0["full"].get("B0 信号（近似）", {})
        lines.append(f"参考 · B0 回放（信号近似，股票池不同）：年化 {_p(f.get('cagr'))}，夏普 {f.get('sharpe')}，最大回撤 {_p(f.get('max_dd'), 0)}")
    lines.append("\n判定：" + "；".join(
        f"{EXITS[e]} → {'通过' if x['pass'] else '不通过'}（两半夏普高于 SPY + 过滤 {'✓' if x['sharpe_halves'] else '✗'}，"
        f"好于随机选股 {'✓' if x['vs_random'] else '✗'}，回撤不深于 SPY {'✓' if x['drawdown'] else '✗'}）" for e, x in res["decision"].items()))
    return "\n".join(lines)
