"""Conditional base rates (pre-registered in `scout/knowledge/external_specs/conditional_rates_v1.yaml`).

Every rule's historical fills (`gate.backtest`, same entries and exits as the fair benchmarks) are split into
context cells — rule × sector × market regime, and one-dimension layers — and each cell's edge over the same-day
selection control is shrunk toward the rule's overall edge (normal–normal empirical Bayes, month-clustered
standard errors). A small cell that looks great by luck is pulled back; only a cell whose shrunk 90 % interval
stays above zero with enough trades and months, positive in both halves, becomes a candidate — and a candidate
only earns a forward test. Shown on the opportunity cards as history, labelled unconfirmed.
"""
from __future__ import annotations

import math
from datetime import date

import numpy as np
import pandas as pd

from scout import db, gate
from scout.simulate import ROUND_TRIP_COST

KEY = "conditional_rates_v1"
MIN_CELL = 15
CAND_N, CAND_MONTHS = 40, 12
Z90 = 1.645
REGIME_ZH = {"uptrend": "上升市", "mixed": "震荡市", "downtrend": "下降市"}
LAYERS = {"sector_regime": ("sector", "regime"), "sector": ("sector",), "regime": ("regime",), "vol": ("vol",)}


def cell_stats(rows: list[dict], split: date) -> dict | None:
    """Raw edge (ret − select), month-clustered se, halves and win rates of one cell."""
    rs = [r for r in rows if r.get("select") is not None]
    if len(rs) < MIN_CELL:
        return None
    d = pd.Series([r["ret"] - r["select"] for r in rs], index=pd.to_datetime([r["date"] for r in rs]))
    monthly = d.groupby(d.index.to_period("M")).mean()
    if len(monthly) >= 3 and monthly.std(ddof=1) > 0:
        se = float(monthly.std(ddof=1) / math.sqrt(len(monthly)))
    else:
        se = float(d.std(ddof=1) / math.sqrt(len(d)) * 1.5) if len(d) > 1 else float("nan")
    first = [x for r, x in zip(rs, d) if r["date"] < split]
    second = [x for r, x in zip(rs, d) if r["date"] >= split]
    return {"n": len(rs), "months": int(len(monthly)), "raw_edge": float(d.mean()), "se": se,
            "first_edge": float(np.mean(first)) if first else None, "second_edge": float(np.mean(second)) if second else None,
            "win_rate": float(np.mean([r["ret"] - ROUND_TRIP_COST > 0 for r in rs])),
            "control_win": float(np.mean([r["select"] > 0 for r in rs]))}


def shrink(cells: list[dict], prior: float) -> list[dict]:
    """Normal–normal empirical Bayes within one rule and layer: τ² by moments, w = τ² / (τ² + se²)."""
    ok = [c for c in cells if c["se"] and math.isfinite(c["se"])]
    if not ok:
        return cells
    means = np.array([c["raw_edge"] for c in ok])
    tau2 = max(0.0, float(np.var(means, ddof=1)) - float(np.mean([c["se"] ** 2 for c in ok]))) if len(ok) > 1 else 0.0
    for c in cells:
        if not (c["se"] and math.isfinite(c["se"])):
            c.update(shrunk_edge=prior, lo90=None, hi90=None, weight=0.0)
            continue
        w = tau2 / (tau2 + c["se"] ** 2) if tau2 > 0 else 0.0
        est, sd = w * c["raw_edge"] + (1 - w) * prior, math.sqrt(w) * c["se"]
        c.update(shrunk_edge=est, lo90=est - Z90 * sd, hi90=est + Z90 * sd, weight=w)
    return cells


def candidate(c: dict) -> bool:
    return bool(c.get("lo90") is not None and c["lo90"] > 0 and c["n"] >= CAND_N and c["months"] >= CAND_MONTHS
                and (c.get("first_edge") or 0) > 0 and (c.get("second_edge") or 0) > 0)


def rule_cells(rows: list[dict]) -> list[dict]:
    """All layers for one rule's fills."""
    if not rows:
        return []
    dates = sorted(r["date"] for r in rows)
    split = dates[len(dates) // 2]
    overall = cell_stats(rows, split)
    if overall is None:
        return []
    out = [{**overall, "layer": "all", "sector": "", "regime": "", "vol": "", "shrunk_edge": overall["raw_edge"],
            "lo90": overall["raw_edge"] - Z90 * overall["se"], "hi90": overall["raw_edge"] + Z90 * overall["se"]}]
    for layer, keys in LAYERS.items():
        groups: dict[tuple, list[dict]] = {}
        for r in rows:
            k = tuple(r.get(x) or "" for x in keys)
            if all(k):
                groups.setdefault(k, []).append(r)
        cells = []
        for k, rs in groups.items():
            st = cell_stats(rs, split)
            if st:
                cells.append({**st, "layer": layer, "sector": "", "regime": "", "vol": "", **dict(zip(keys, k))})
        out += shrink(cells, overall["raw_edge"])
    for c in out:
        c["candidate"] = c["layer"] == "sector_regime" and candidate(c)
    return out


async def run() -> dict:
    from scout.detectors import registry
    from scout.pipeline import load_panel

    if await db.fetch_one("SELECT 1 FROM scout.conditional_rates LIMIT 1"):
        raise SystemExit("already run (one run per pre-registration)")
    p = await load_panel(lookback_days=None)
    m = gate.Market(p)
    today, summary = date.today(), {}
    for det in registry(include_candidates=True):
        cells = rule_cells(gate.backtest(p, det, m))
        if not cells:
            continue
        await db.execute_many(
            "INSERT INTO scout.conditional_rates (run_on, detector, layer, sector, regime, vol, n, months, raw_edge, se, shrunk_edge, "
            "lo90, hi90, first_edge, second_edge, win_rate, control_win, candidate) VALUES (:d, :det, :layer, :sector, :regime, :vol, "
            ":n, :months, :raw_edge, :se, :shrunk_edge, :lo90, :hi90, :first_edge, :second_edge, :win_rate, :control_win, :candidate)",
            [{"d": today, "det": det.name, **{k: c.get(k) for k in ("layer", "sector", "regime", "vol", "n", "months", "raw_edge", "se",
                                                                    "shrunk_edge", "lo90", "hi90", "first_edge", "second_edge",
                                                                    "win_rate", "control_win", "candidate")}} for c in cells])
        summary[det.name] = {"fills": cells[0]["n"], "cells": len(cells) - 1,
                             "candidates": [f"{c['sector']} / {c['regime']}" for c in cells if c["candidate"]]}
    return summary


# ── reading (cards, agent) ──

async def for_signal(detector: str, sector: str | None, regime: str | None) -> dict | None:
    """The latest run's cell for a signal (the primary cell if estimated, else the best one-dimension layer),
    with the rule's overall row attached as `rule`."""
    rows = await db.fetch_all(
        "SELECT * FROM scout.conditional_rates WHERE run_on = (SELECT max(run_on) FROM scout.conditional_rates) AND detector = :d "
        "AND ((layer = 'sector_regime' AND sector = :s AND regime = :r) OR (layer = 'sector' AND sector = :s) "
        "OR (layer = 'regime' AND regime = :r) OR layer = 'all')", {"d": detector, "s": sector or "", "r": regime or ""})
    order = {"sector_regime": 0, "sector": 1, "regime": 2, "all": 3}
    if not rows:
        return None
    clean = [{**r, "run_on": str(r["run_on"])} for r in rows]          # stored in the evidence JSON
    best = min(clean, key=lambda r: order.get(r["layer"], 9))
    rule = next((r for r in clean if r["layer"] == "all"), None)
    return {**best, "rule": rule} if rule and best is not rule else best


def pooled(c: dict) -> bool:
    """τ² = 0 within the rule: the cell is pulled fully to the rule's average and its interval collapses to a point.
    v1's posterior sd (√w · se) leaves out the uncertainty of that average, so such a cell carries no information of
    its own (all but 2 of the 21 v1 'candidates', 2026-11-03)."""
    lo, hi, est = c.get("lo90"), c.get("hi90"), c.get("shrunk_edge")
    return c.get("layer") != "all" and lo is not None and hi is not None and abs(hi - lo) < 1e-9 and abs(est - lo) < 1e-9


def text(c: dict | None) -> str | None:
    from scout.universe import SECTOR_ZH

    if not c:
        return None
    sector, regime = c.get("sector") or "", c.get("regime") or ""
    where = {"sector_regime": f"{SECTOR_ZH.get(sector, sector)} · {REGIME_ZH.get(regime, regime)}",
             "sector": SECTOR_ZH.get(sector, sector), "regime": REGIME_ZH.get(regime, regime),
             "all": "全部情况"}.get(c["layer"], c["layer"])
    seen = (f"同类信号在「{where}」历史 {c['n']} 次，扣费后上涨 {c['win_rate'] * 100:.0f}%（同日随机股票 {c['control_win'] * 100:.0f}%）")
    rule = c.get("rule") or (c if c.get("layer") == "all" else None)
    overall = (f"该规则整体相对同日随机股票 {rule['raw_edge'] * 100:+.1f}%，90% 区间 {rule['lo90'] * 100:+.1f}% 到 {rule['hi90'] * 100:+.1f}%"
               if rule and rule.get("lo90") is not None else "")
    if c.get("layer") == "all":
        return f"历史统计（未经前向验证）：{seen}；{overall}"
    if pooled(c):
        return f"历史条件统计（未经前向验证）：{seen}；这个组合与规则整体没有可区分的差异" + (f"，{overall}" if overall else "")
    return (f"历史条件统计（收缩后，未经前向验证）：{seen}，相对随机股票 {c['shrunk_edge'] * 100:+.1f}%，"
            f"90% 区间 {c['lo90'] * 100:+.1f}% 到 {c['hi90'] * 100:+.1f}%（未计入规则均值本身的误差，偏窄）")
