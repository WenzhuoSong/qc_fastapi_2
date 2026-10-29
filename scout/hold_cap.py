"""Holding cap of the holdings plan (pre-registered in `scout/knowledge/exit_specs/hold_cap.yaml`).

Picks of the passed strategies are bought (low asset growth: next open; breakout retest: the
rule's entry) and managed for up to 60, 126 or 252 sessions by the plan or the 10×ATR42
trailing stop; after the sale the money is in the pool (equal weight) until the 252-session
window ends. Each longer cap is compared trade by trade with the 60-session cap.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from scout import exit_study, exit_vs_hold as ev, gate, horizons, plan_exit, rankers
from scout.detectors import Detector

SPEC_KEY = "hold_cap"
WINDOW = 252
CAPS = (60, 126, 252)
EXITS = {"plan": "现行计划", "atr10": "10×ATR42"}
STRATEGIES = {"asset_growth": "资产增长最慢 10%", "breakout_retest": "突破回踩"}
MIN_T = 2.0
MAX_DD_LOSS = 0.05
COST = ev.COST


def sale_plan(a: dict, s: int, fill: float, cap: int) -> tuple[int, float] | None:
    plan = plan_exit.plan_for(a, s - 1, fill)
    if not plan:
        return None
    path = exit_study.Path(a["open"], a["high"], a["low"], a["close"], a["atr14"], a["sma20"], a["sma50"], s)
    ret, days = exit_study.manage(path, fill, plan["stop"], plan["target1"], max(plan["target2"], plan["target1"]),
                                  plan_exit.POLICY, max_hold=cap)
    return days - 1, fill * (1 + ret)


def sale_atr10(a: dict, s: int, fill: float, cap: int) -> tuple[int, float]:
    """Close below (highest close since the buy − 10×ATR42, only rising) → next open; else the cap's last close."""
    c = a["close"]
    stop, top = -np.inf, -np.inf
    for k in range(s, s + cap - 1):
        top = max(top, c[k])
        lv = top - 10 * a["atr42"][k]
        if np.isfinite(lv):
            stop = max(stop, lv)
        if c[k] < stop:
            return k + 1 - s, float(a["open"][k + 1])
    return cap - 1, float(c[s + cap - 1])


def value(a: dict, s: int, fill: float, sale: tuple[int, float] | None, pool: np.ndarray) -> np.ndarray:
    """Value at each close of the window: the stock until the sale bar, then the pool (one more cost)."""
    v = a["close"][s:s + WINDOW] / fill
    if sale is None or sale[0] >= WINDOW - 1:
        return v
    k, px = sale
    tail = px / fill * (1 - COST) * np.cumprod(1 + pool[s + k + 1:s + WINDOW])
    return np.concatenate([v[:k], [px / fill], tail])


def trade(a: dict, s: int, fill: float, pool: np.ndarray) -> dict[str, np.ndarray] | None:
    """{variant: value path} for one buy (None when any variant has no result)."""
    if s < plan_exit.WARMUP or s + WINDOW > len(a["close"]) or not np.isfinite(a["close"][s:s + WINDOW]).all():
        return None
    if not np.isfinite(a["atr42"][s - 1]):
        return None
    out = {"hold": value(a, s, fill, None, pool)}
    for cap in CAPS:
        sp = sale_plan(a, s, fill, cap)
        if sp is None:
            return None
        out[f"plan|{cap}"] = value(a, s, fill, sp, pool)
        out[f"atr10|{cap}"] = value(a, s, fill, sale_atr10(a, s, fill, cap), pool)
    return out


def buys(p, angle: dict, det: Detector, spec: dict) -> dict[str, list[tuple[str, int, int, float]]]:
    """{strategy: [(ticker, signal day, fill bar, fill price)]}."""
    a, stocks = rankers.arrays(p)
    a["angle"] = angle
    fn = rankers.RANKERS[spec["ranker"]]
    out = {"asset_growth": [], "breakout_retest": []}
    n = len(p.dates)
    for i in horizons.rank_days(n, spec):
        uni = set(rankers._universe(a, i).tolist())
        for j in fn(a, i, float(spec.get("top_frac", 0.1))):
            if j in uni and np.isfinite(a["open"][i + 1, j]) and a["open"][i + 1, j] > 0:
                out["asset_growth"].append((stocks[j], i, i + 1, float(a["open"][i + 1, j])))
    from scout import base_rates

    for ticker, i in base_rates.hit_positions(det.mask(p)):
        got = horizons._fill(p, det, ticker, i)
        if got:
            out["breakout_retest"].append((ticker, i, got[0], got[1]))
    return out


def run(p, buys_by: dict, pool: np.ndarray) -> dict[str, list[dict]]:
    """Per strategy: rows {date, s, r_<variant>, path_<variant>} (paths only to build the portfolio)."""
    cache: dict[str, dict] = {}
    out = {}
    for strat, items in buys_by.items():
        rows = []
        for ticker, i, s, fill in items:
            a = cache.get(ticker)
            if a is None:
                a = cache[ticker] = ev.stock_arrays(p, ticker)
            got = trade(a, s, fill, pool)
            if got is None:
                continue
            rows.append({"date": p.dates[i], "s": s, **{f"r_{k}": float(v[-1] - 1 - COST) for k, v in got.items()},
                         "paths": got})
        out[strat] = rows
    return out


def max_drawdown(rows: list[dict], key: str, n: int) -> float | None:
    """Equal weight across the positions open each day (each position's daily return in its window)."""
    tot, cnt = np.zeros(n), np.zeros(n)
    for r in rows:
        v = r["paths"][key]
        d = np.concatenate([[v[0] - 1], v[1:] / v[:-1] - 1])
        tot[r["s"]:r["s"] + WINDOW] += d
        cnt[r["s"]:r["s"] + WINDOW] += 1
    ok = cnt > 0
    if not ok.any():
        return None
    nav = pd.Series(np.cumprod(1 + tot[ok] / cnt[ok]))
    return round(float((nav / nav.cummax() - 1).min()), 5)


def compare(rows: list[dict], key: str, base: str) -> dict:
    dates = sorted(r["date"] for r in rows)
    split = dates[len(dates) // 2]
    out = {}
    for name, part in (("all", rows), ("first", [r for r in rows if r["date"] < split]), ("second", [r for r in rows if r["date"] >= split])):
        diffs = [r[f"r_{key}"] - r[f"r_{base}"] for r in part]
        mean, t, months = gate.monthly_t([r["date"] for r in part], diffs)
        out[name] = {"edge": None if mean is None else round(mean, 5), "t": None if t is None else round(t, 2), "months": months,
                     "t_nw": horizons.nw_t([r["date"] for r in part], diffs, WINDOW) if part else None}
    out["split"] = str(split)
    return out


def summarize(results: dict[str, list[dict]], n: int) -> dict:
    out: dict = {}
    for strat, rows in results.items():
        if not rows:
            out[strat] = {"n": 0}
            continue
        keys = ["hold", *(f"{e}|{c}" for e in EXITS for c in CAPS)]
        s = {"n": len(rows), "variants": {k: {"mean": round(float(np.mean([r[f"r_{k}"] for r in rows])), 5),
                                              "max_dd": max_drawdown(rows, k, n)} for k in keys},
             "vs_hold": {k: compare(rows, k, "hold") for k in keys if k != "hold"}, "decision": {}}
        for e in EXITS:
            base = f"{e}|60"
            for cap in CAPS[1:]:
                k = f"{e}|{cap}"
                c = compare(rows, k, base)
                checks = {"halves": all((c[h]["edge"] or 0) > 0 for h in ("first", "second")),
                          "t": c["all"]["t"] is not None and c["all"]["t"] >= MIN_T,
                          "drawdown": s["variants"][k]["max_dd"] >= s["variants"][base]["max_dd"] - MAX_DD_LOSS}
                s["decision"][k] = {"vs_cap60": c, "checks": checks, "ok": all(checks.values())}
            ok = [f"{e}|{cap}" for cap in CAPS[1:] if s["decision"][f"{e}|{cap}"]["ok"]]
            s["decision"][f"{e}|pick"] = max(ok, key=lambda k: s["decision"][k]["vs_cap60"]["all"]["edge"]) if ok else None
        out[strat] = s
    return out


def report_text(res: dict) -> str:
    pct = lambda x: "—" if x is None else f"{x:+.2%}"  # noqa: E731
    lines = []
    for strat, s in res.items():
        lines.append(f"{STRATEGIES[strat]}：{s.get('n', 0)} 笔（窗口 {WINDOW} 日，卖出后换入股票池）")
        if not s.get("n"):
            continue
        lines.append("方案 | 每笔均值 | 最大回撤 | 相对一直持有 全部（t / 重叠校正 t）")
        for k, v in s["variants"].items():
            vh = s["vs_hold"].get(k)
            label = "一直持有" if k == "hold" else f"{EXITS[k.split('|')[0]]} · 上限 {k.split('|')[1]} 日"
            lines.append(f"{label} | {pct(v['mean'])} | {pct(v['max_dd'])} | "
                         + (f"{pct(vh['all']['edge'])}（{vh['all']['t']} / {vh['all']['t_nw']}）" if vh else "—"))
        for e in EXITS:
            for cap in CAPS[1:]:
                d = s["decision"][f"{e}|{cap}"]
                c = d["vs_cap60"]
                lines.append(f"{EXITS[e]} 上限 {cap} vs 60：全部 {pct(c['all']['edge'])}（t {c['all']['t']}，重叠校正 {c['all']['t_nw']}），"
                             f"前半 {pct(c['first']['edge'])}，后半 {pct(c['second']['edge'])}；"
                             + " ".join(f"{'✓' if ok else '✗'}{ {'halves': '两半为正', 't': 't≥2', 'drawdown': '回撤'}[n]}" for n, ok in d["checks"].items()))
            pick = s["decision"][f"{e}|pick"]
            lines.append(f"{EXITS[e]}：" + (f"上限 {pick.split('|')[1]} 日满足条件（需用户确认）" if pick else "较长的上限都不满足条件，保持 60 日"))
        lines.append("")
    return "\n".join(lines).strip()
