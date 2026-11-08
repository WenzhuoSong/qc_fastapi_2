"""Core rule plus a 20 % trend sleeve (pre-registered in `scout/knowledge/external_specs/trend_sleeve_v1.yaml`).

Local only: the sleeves' monthly returns from carver_trend_v1's run (`exports/carver_trend/result.json`, not
recomputed), the core rule (SPY + the 10-month filter) from `scout.cross_asset`, cash at the French T-bill rate.
Blends are rebalanced to their weights at each month end, paying one-way cost on the drift back.
"""
from __future__ import annotations

import json
import math
from pathlib import Path

import numpy as np
import pandas as pd

from scout import cross_asset as ca

KEY = "trend_sleeve_v1"
CARVER = Path("exports/carver_trend/result.json")
WEIGHT, WEIGHT_LOW = 0.2, 0.1
COST = 0.0005
MIN_DD_GAIN = 0.03
MIN_T = -2.0
PRIMARY = ("core_system", "core_long_only")
LABELS = {"core_system": "80% 核心 + 20% 趋势系统（做多做空）", "core_long_only": "80% 核心 + 20% 只做多版本",
          "core_now": "现行核心规则（SPY + 大盘过滤）", "core_cash": "80% 核心 + 20% 现金", "core_hold": "80% 核心 + 20% 风险平价一直做多",
          "core_system_10": "90% 核心 + 10% 趋势系统", "core_long_only_10": "90% 核心 + 10% 只做多版本", "spy": "SPY 买入持有"}


def blend(a: pd.Series, b: pd.Series, w: float, cost: float = COST) -> pd.Series:
    """Monthly returns of (1 − w) a + w b, rebalanced at each month end; the drift back costs one way on both legs."""
    gross = (1 - w) * a + w * b
    drift = ((1 - w) * (1 + a) / (1 + gross) - (1 - w)).abs()
    return gross - cost * 2 * drift * (1 + gross)


def monthly_stats(r: pd.Series, rf: pd.Series) -> dict:
    ex = r - rf.reindex(r.index).fillna(0.0)
    nav = (1 + r).cumprod()
    return {"cagr": float(nav.iloc[-1] ** (12 / len(r)) - 1), "vol": float(r.std(ddof=1) * math.sqrt(12)),
            "sharpe": float(ex.mean() / ex.std(ddof=1) * math.sqrt(12)), "max_dd": float((nav / nav.cummax() - 1).min()),
            "worst_12m": float(((1 + r).rolling(12).apply(np.prod, raw=True) - 1).min())}


def load() -> tuple[dict[str, pd.Series], pd.Series]:
    res = json.loads(CARVER.read_text())
    sleeves = {k: pd.Series({pd.Period(m, "M"): x for m, x in res["monthly"][k].items()}) for k in ("system", "long_only", "hold")}
    px, rf = ca.load()
    sch = ca.schedules(px, rf, ["core_now", "spy"], ca.MIN_ASSETS)
    core = {k: ca.monthly(ca.simulate(px, rf, sch[k], ca.COST)["ret"]) for k in ("core_now", "spy")}
    rf_m = (1 + rf).groupby(rf.index.to_period("M")).prod() - 1
    complete = {px.index[i].to_period("M") for i in ca.month_end_rows(px.index)}     # the last month may be partial
    idx = pd.PeriodIndex(sorted(set(sleeves["system"].index) & set(core["core_now"].index) & complete), freq="M")
    return {**{k: v.reindex(idx) for k, v in sleeves.items()}, **{k: v.reindex(idx) for k, v in core.items()}}, rf_m.reindex(idx).fillna(0.0)


def portfolios(s: dict[str, pd.Series], rf: pd.Series) -> dict[str, pd.Series]:
    core = s["core_now"]
    return {"core_system": blend(core, s["system"], WEIGHT), "core_long_only": blend(core, s["long_only"], WEIGHT),
            "core_now": core, "core_cash": blend(core, rf, WEIGHT), "core_hold": blend(core, s["hold"], WEIGHT),
            "core_system_10": blend(core, s["system"], WEIGHT_LOW), "core_long_only_10": blend(core, s["long_only"], WEIGHT_LOW),
            "spy": s["spy"]}


def evaluate(p: dict[str, pd.Series], rf: pd.Series, split: str = "2015-03") -> dict:
    mid = pd.Period(split, "M")
    spy = p["spy"]
    worst = spy[spy <= spy.quantile(0.10)].index
    out = {"from": str(p["core_now"].index[0]), "to": str(p["core_now"].index[-1]), "split": split, "variants": {}, "vs_core": {}}
    for k, r in p.items():
        s = monthly_stats(r, rf)
        s.update({"sharpe_first": monthly_stats(r[r.index < mid], rf)["sharpe"], "sharpe_second": monthly_stats(r[r.index >= mid], rf)["sharpe"],
                  "years": {str(y): float((1 + r[r.index.year == y]).prod() - 1) for y in (2008, 2020, 2022)},
                  "crisis": float(r.reindex(worst).mean())})
        out["variants"][k] = s
        d = r - p["core_now"]
        out["vs_core"][k] = {"all": ca.tstat(d), "first": ca.tstat(d[d.index < mid]), "second": ca.tstat(d[d.index >= mid])}
    return out


def decide(ev: dict) -> dict:
    v, vs = ev["variants"], ev["vs_core"]
    checks = {}
    for k in PRIMARY:
        s, c, z = v[k], v["core_now"], v["core_cash"]
        checks[k] = {"sharpe_vs_core": all(s[f"sharpe_{h}"] > c[f"sharpe_{h}"] for h in ("first", "second")),
                     "sharpe_vs_cash": all(s[f"sharpe_{h}"] > z[f"sharpe_{h}"] for h in ("first", "second")),
                     "drawdown": bool(s["max_dd"] - c["max_dd"] >= MIN_DD_GAIN),
                     "not_behind": all((vs[k][h]["t"] is None) or vs[k][h]["t"] >= MIN_T for h in ("first", "second"))}
    ok = [k for k in PRIMARY if all(checks[k].values())]
    pick = max(ok, key=lambda k: v[k]["sharpe"]) if ok else None
    return {"passed": pick is not None, "pick": pick, "checks": checks}


def _p(x: float | None, d: int = 1) -> str:
    return "—" if x is None else f"{x:+.{d}%}"


def report_text(res: dict) -> str:
    ev, dec = res["evaluation"], res["decision"]
    lines = [f"核心仓 + 趋势配置（trend_sleeve_v1），{ev['from']} 至 {ev['to']}，前后两半以 {ev['split']} 分界；每月末再平衡",
             "组合 | 年化 | 波动 | 夏普 全部/前半/后半 | 最大回撤 | 最差 12 个月 | 2008 / 2020 / 2022 | SPY 最差 10% 月份 | 对现行规则每月（t）"]
    for k in ("core_system", "core_long_only", "core_now", "core_cash", "core_hold", "core_system_10", "core_long_only_10", "spy"):
        s, d, y = ev["variants"][k], ev["vs_core"][k]["all"], ev["variants"][k]["years"]
        t = "—" if d["t"] is None else f"{d['t']:.2f}"
        lines.append(f"{LABELS[k]} | {_p(s['cagr'])} | {s['vol']:.1%} | {s['sharpe']:.2f} / {s['sharpe_first']:.2f} / {s['sharpe_second']:.2f} | "
                     f"{_p(s['max_dd'])} | {_p(s['worst_12m'])} | {_p(y['2008'])} / {_p(y['2020'])} / {_p(y['2022'])} | {_p(s['crisis'], 2)} | "
                     f"{_p(d['mean'], 2)}（{t}）")
    for k in PRIMARY:
        lines.append(f"{LABELS[k]}：{dec['checks'][k]}")
    lines.append(f"结论：{'通过，选 ' + LABELS[dec['pick']] if dec['passed'] else '两个组合都未满足全部条件，现行核心规则不变'}")
    return "\n".join(lines)
