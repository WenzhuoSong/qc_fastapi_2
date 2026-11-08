"""Pullback in calm markets (pre-registered in `scout/knowledge/external_specs/pullback_calm_market_v1.yaml`).

Local only, on the Sharadar files (`scout_system.load`: point-in-time top 1000 from 1999, dividend-adjusted bars,
delistings settled). Scout's pullback-in-uptrend rule (`rules.D2_PULLBACK`, Scout's feature formulas) with its own
levels and bracket (`scout.simulate`), one signal per stock per 10 sessions; each signal is inside the condition
when the market filter is on (`exit_redeploy.market_off`) and SPY's 20-day volatility is at or below its one-year
median (`gate.vol_states` = low). Select control = the mean of the first 3 random same-day members that fill with
the same levels formula and bracket; hold = the same stock from the fill price to the exit day's close.
Judged on 1999-01-04..2014-09-30 only (Scout's own data, seen before, starts 2014-10).
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from scout import exit_redeploy as er
from scout import gate
from scout import scout_system as ss
from scout import simulate as simm
from scout.detectors import FIXED_EXIT
from scout.detectors.rules import _pullback_levels
from scout.features import rsi

KEY = "pullback_calm_market_v1"
TRIGGER, HORIZON = 3, 10                      # the rule's trigger window and horizon (rules.DETECTORS)
COOLDOWN = 10                                 # base_rates.COOLDOWN_SESSIONS
CONTROLS, TRIES = 3, 60
SEED = 20261108
PRIMARY_END = pd.Timestamp("2014-09-30")
SPLIT = pd.Timestamp("2007-01-01")
TRIALS = 2                                    # family pullback_in_uptrend: pullback_in_uptrend_plan, then this
T_SAME = 2.0                                  # condition in − out, month-clustered
COST = simm.ROUND_TRIP_COST
BEARS = {"2000-03..2002-09": ("2000-03-01", "2002-09-30"), "2007-10..2009-02": ("2007-10-01", "2009-02-28")}


def features(d: dict) -> dict[str, np.ndarray]:
    """Scout's formulas (scout/features.py) on the Sharadar arrays; RS percentile within the day's members."""
    h, lo, c, v = (pd.DataFrame(d[k]) for k in ("h", "l", "c", "v"))
    prev = c.shift(1)
    tr = np.fmax(h - lo, np.fmax((h - prev).abs(), (lo - prev).abs()))
    f = {f"sma{n}": c.rolling(n, min_periods=n).mean() for n in (20, 50, 200)}
    f["atr14"] = tr.rolling(14, min_periods=14).mean()
    f["vol_ratio"] = v / v.rolling(50, min_periods=20).mean().shift(1).replace(0, np.nan)
    f["high_20"] = h.rolling(20, min_periods=20).max()
    f["low_5"] = lo.rolling(5, min_periods=5).min()
    f["ret_63d"] = c / c.shift(63) - 1
    blend = 0.4 * f["ret_63d"] + 0.3 * (c / c.shift(126) - 1) + 0.3 * (c / c.shift(252) - 1)
    f["rs_pct"] = blend.where(d["member"]).rank(axis=1, pct=True)
    f["rsi2"] = rsi(c, 2)
    f["close"] = c
    return {k: x.to_numpy(dtype=float) for k, x in f.items()}


def mask(f: dict[str, np.ndarray], member: np.ndarray) -> np.ndarray:
    """rules.D2_PULLBACK, condition by condition."""
    with np.errstate(invalid="ignore"):
        return (member & (f["rs_pct"] >= 0.80) & (f["close"] > f["sma200"]) & (f["sma50"] > f["sma200"])
                & (f["ret_63d"] > 0.10) & (f["rsi2"] <= 10) & (f["close"] >= 0.97 * f["sma50"])
                & (f["close"] <= f["sma20"]) & (f["vol_ratio"] <= 1.2))


def cooldown(hits: np.ndarray, gap: int = COOLDOWN) -> list[tuple[int, int]]:
    """(day, stock) of each first signal, then none for that stock within `gap` sessions."""
    out, last = [], {}
    for b, j in zip(*np.nonzero(hits)):            # row-major: days ascending
        if b - last.get(j, -10 ** 9) < gap:
            continue
        last[j] = b
        out.append((int(b), int(j)))
    return out


def place(d: dict, f: dict, b: int, j: int) -> dict | None:
    """The rule's bracket placed after day b on stock j: {fill, exit, ret (gross), hold} or None when it does not fill."""
    o, h, lo, c = d["o"], d["h"], d["l"], d["c"]
    close = f["close"][b, j]
    need = ("atr14", "low_5", "sma50", "high_20")
    if not np.isfinite(close) or close <= 0 or not all(np.isfinite(f[k][b, j]) for k in need):
        return None
    k = 100.0 / close                              # levels round to cents: work near 100
    lv = _pullback_levels({"atr14": f["atr14"][b, j] * k, "close": 100.0, "low_5": f["low_5"][b, j] * k,
                           "sma50": f["sma50"][b, j] * k, "high_20": f["high_20"][b, j] * k})
    if not lv.valid():
        return None
    n = len(c)
    span = range(b + 1, min(n, b + 1 + TRIGGER + FIXED_EXIT.holding_limit(HORIZON) + 1))
    bars = [simm.Bar(i, o[i, j] * k, h[i, j] * k, lo[i, j] * k, c[i, j] * k) for i in span
            if np.isfinite(o[i, j]) and np.isfinite(h[i, j]) and np.isfinite(lo[i, j]) and np.isfinite(c[i, j])]
    res = simm.simulate(bars, lv, HORIZON, trigger_window=TRIGGER, exit_policy=FIXED_EXIT)
    if res.entry_date is None or res.exit_date is None or res.state == "open":
        return None
    e = int(res.exit_date)
    return {"fill": int(res.entry_date), "exit": e, "ret": res.exit_price / res.entry_price - 1,
            "hold": c[e, j] * k / res.entry_price - 1, "state": res.state}


def control(d: dict, f: dict, b: int, j: int, rng: np.random.Generator) -> float | None:
    names = np.flatnonzero(d["member"][b])
    names = names[names != j]
    got = []
    for _ in range(TRIES):
        if len(got) == CONTROLS or not names.size:
            break
        r = place(d, f, b, int(rng.choice(names)))
        if r:
            got.append(r["ret"])
    return float(np.mean(got)) if got else None


def run() -> dict:
    d = ss.load()
    f = features(d)
    sig = cooldown(mask(f, d["member"]) & (np.arange(len(d["c"]))[:, None] >= d["start"]))
    off = er.market_off(d["spy"], d["dates"])
    vol = gate.vol_states(d["spy"])
    rng = np.random.default_rng(SEED)
    rows = []
    for b, j in sig:
        r = place(d, f, b, j)
        if not r:
            continue
        ctl = control(d, f, b, j, rng)
        if ctl is None:
            continue
        rows.append({"date": d["dates"][b], "ticker": d["names"][j], "in": bool(not off[b] and vol[b] == "low"),
                     "filter_on": bool(not off[b]), "calm": vol[b] == "low", "state": r["state"],
                     "ret": r["ret"] - COST, "select": r["ret"] - ctl, "hold": r["ret"] - COST - r["hold"]})
    return {"signals": len(sig), "rows": rows}


# ── evaluation ──

def monthly(rows: list[dict], key: str) -> pd.Series:
    s = pd.Series([r[key] for r in rows], index=pd.to_datetime([r["date"] for r in rows]))
    return s.groupby(s.index.to_period("M")).mean() if len(s) else pd.Series(dtype=float)


def t_of(m: pd.Series) -> float | None:
    return float(m.mean() / (m.std(ddof=1) / np.sqrt(len(m)))) if len(m) >= 3 and m.std(ddof=1) > 0 else None


def side(rows: list[dict]) -> dict:
    if not rows:
        return {"n": 0}
    m = monthly(rows, "select")
    first = [r for r in rows if pd.Timestamp(r["date"]) < SPLIT]
    second = [r for r in rows if pd.Timestamp(r["date"]) >= SPLIT]
    states = pd.Series([r["state"] for r in rows]).value_counts(normalize=True).round(3).to_dict()
    return {"n": len(rows), "months": len(m), "select": float(np.mean([r["select"] for r in rows])), "select_t": t_of(m),
            "select_first": float(monthly(first, "select").mean()) if first else None,
            "select_second": float(monthly(second, "select").mean()) if second else None,
            "hold": float(np.mean([r["hold"] for r in rows])), "ret": float(np.mean([r["ret"] for r in rows])),
            "states": states}


def difference(a: list[dict], b: list[dict]) -> dict:
    """Condition in − out on the select excess, months with both sides."""
    ma, mb = monthly(a, "select"), monthly(b, "select")
    both = ma.index.intersection(mb.index)
    diff = ma[both] - mb[both]
    return {"months": len(diff), "mean": float(diff.mean()) if len(diff) else None, "t": t_of(diff)}


def evaluate(rows: list[dict]) -> dict:
    prim = [r for r in rows if pd.Timestamp(r["date"]) <= PRIMARY_END]
    later = [r for r in rows if pd.Timestamp(r["date"]) > PRIMARY_END]

    def block(rs: list[dict]) -> dict:
        inn, out = [r for r in rs if r["in"]], [r for r in rs if not r["in"]]
        return {"in": side(inn), "out": side(out), "all": side(rs), "in_minus_out": difference(inn, out),
                "filter_only": side([r for r in rs if r["filter_on"]]), "calm_only": side([r for r in rs if r["calm"]]),
                "per_year": {str(y): side([r for r in inn if pd.Timestamp(r["date"]).year == y])
                             for y in sorted({pd.Timestamp(r["date"]).year for r in rs})},
                "bears_in": {k: sum(1 for r in inn if a <= str(pd.Timestamp(r["date"]).date()) <= z) for k, (a, z) in BEARS.items()}}

    return {"primary": block(prim), "later": block(later)}


def decide(ev: dict) -> dict:
    p = ev["primary"]
    inn, diff = p["in"], p["in_minus_out"]
    bar = gate.required_t(TRIALS)
    checks = {
        "1 条件内对 select：前后两半月均值 > 0": bool(inn.get("n") and (inn.get("select_first") or 0) > 0 and (inn.get("select_second") or 0) > 0),
        f"1 条件内对 select：全样本按月 t ≥ {bar}": bool((inn.get("select_t") or 0) >= bar),
        f"2 条件内减条件外：按月 t ≥ {T_SAME}": bool((diff.get("t") or 0) >= T_SAME),
        "3 条件内对直接持有：均值 > 0": bool((inn.get("hold") or -1) > 0),
    }
    return {"passed": all(checks.values()), "checks": checks, "required_t": bar}


def _p(x: float | None, dg: int = 2) -> str:
    return "—" if x is None else f"{x * 100:+.{dg}f}%"


def _t(x: float | None) -> str:
    return "—" if x is None else f"{x:.2f}"


def report_text(res: dict) -> str:
    ev, dec = res["evaluation"], res["decision"]
    lines = [f"强势回调 · 只在平稳多头市况（{KEY}）", f"信号（冷却后）{res['signals']}，有对照的成交 {len(res['rows'])}"]
    for name, b in (("判定样本 1999-01..2014-09", ev["primary"]), ("2014-10 以后（已看过，只报告）", ev["later"])):
        lines.append(f"\n{name}")
        for lab, k in (("条件内", "in"), ("条件外", "out"), ("全部", "all"), ("只用大盘过滤", "filter_only"), ("只用低波动", "calm_only")):
            s = b[k]
            if not s.get("n"):
                lines.append(f"  {lab}：无")
                continue
            lines.append(f"  {lab}：{s['n']} 笔 · 对同日其他股票 {_p(s['select'])}（t {_t(s['select_t'])}；前半 {_p(s['select_first'])}，"
                         f"后半 {_p(s['select_second'])}）· 对直接持有 {_p(s['hold'])} · 扣费收益 {_p(s['ret'])} · {s['states']}")
        dif = b["in_minus_out"]
        lines.append(f"  条件内减条件外：{_p(dif['mean'])}（t {_t(dif['t'])}，{dif['months']} 个月）")
        lines.append(f"  熊市里条件内信号数：{b['bears_in']}")
    lines.append("\n判定：" + ("通过" if dec["passed"] else "未通过"))
    lines += [f"  {'✓' if ok else '✗'} {k}" for k, ok in dec["checks"].items()]
    return "\n".join(lines)
