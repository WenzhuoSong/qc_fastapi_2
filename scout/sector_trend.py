"""Trend confirmation for sectors and theme ETFs (user's request 2026-11-05): is a sector's move against SPY
beyond its usual noise, steady, and broad — over the last 5, 10, 20 and 63 sessions.

Per horizon n, on the relative line rs = ETF close / SPY close:
- strength z = ln(rs_t / rs_t−n) / (σ × √n), σ = daily std of ln rs changes over the 252 sessions before the
  window (ex ante; at least 60). The volatility-normalised trend signal of Lempérière et al. (2014),
  "Two centuries of trend following".
- quality = Kaufman's efficiency ratio of the relative line: |net change| / sum of |step changes|, on about
  ten steps per window (Kaufman's own 10-bar lookback: daily for 5 / 10 sessions, 2-day steps for 20, 6-day
  for 63 — on daily steps a longer window always reads choppier); 0 = back and forth, 1 = a straight line,
  below 0.3 is read as choppy (the common convention).
- breadth = share of the sector's liquid members beating SPY over the same n sessions (StockCharts /
  Investors Intelligence style participation); themes have no members here.

Label per horizon, thresholds by statistical convention and fixed (never tuned to returns):
- 确认: |z| ≥ 2, breadth on the same side of one half (when there are members), and a neighbouring horizon
  pointing the same way with |z| ≥ 1;
- 初现: |z| ≥ 1 otherwise (what is missing is listed);
- 噪音: |z| < 1.

Descriptive only. Sector rotation as a trading rule has failed Scout's gate six times and sector-ETF momentum
after 2000 is weak in the literature: a confirmed trend says the move was not chance, not that it continues.
Each new confirmation goes to the judgment ledger (kind `sector_trend`) so that question gets answered.
"""
from __future__ import annotations

import math

import numpy as np
import pandas as pd

from scout.features import FeaturePanel

BENCH = "SPY"
HORIZONS = (5, 10, 20, 63)
NAMES = {5: "最新 5 日", 10: "短期 10 日", 20: "中期 20 日", 63: "较长 63 日"}
SHORT = {5: "5 日", 10: "短期", 20: "中期", 63: "较长"}
NEIGHBOURS = {5: (10,), 10: (5, 20), 20: (10, 63), 63: (20,)}
LEDGER_HORIZONS = (10, 20, 63)        # 5 sessions is shown as the latest move, not recorded
Z_CONFIRM = 2.0
Z_EMERGE = 1.0
BREADTH_SIDE = 0.5
ER_CHOPPY = 0.3
ER_STEPS = 10
VOL_WINDOW = 252
VOL_MIN = 60
MIN_MEMBERS = 5

STATE_ZH = {"confirmed": "确认", "emerging": "初现", "noise": "噪音"}


def stats(log_rs: pd.Series, end: int, n: int) -> dict | None:
    """Strength, quality and relative change of the relative line over the n sessions ending at `end`."""
    if end - n < 0:
        return None
    d = log_rs.diff()
    before = d.iloc[max(1, end - n - VOL_WINDOW + 1): end - n + 1].dropna()
    window = d.iloc[end - n + 1: end + 1]
    move = log_rs.iloc[end] - log_rs.iloc[end - n]
    if len(before) < VOL_MIN or window.isna().any() or not np.isfinite(move):
        return None
    sigma = float(before.std())
    step = max(1, round(n / ER_STEPS))
    grid = list(range(end - n, end + 1, step))
    if grid[-1] != end:
        grid.append(end)
    path = float(np.abs(np.diff(log_rs.iloc[grid].to_numpy())).sum())
    if not sigma > 0:
        return None
    return {"z": float(move / (sigma * math.sqrt(n))), "er": float(abs(move) / path) if path > 0 else None,
            "rel": float(math.expm1(move))}


def breadth(close: pd.DataFrame, members: list[str], end: int, n: int) -> float | None:
    """Share of members beating SPY over n sessions; None below MIN_MEMBERS with data."""
    if not members or end - n < 0 or BENCH not in close.columns:
        return None
    bench = close[BENCH].iloc[end] / close[BENCH].iloc[end - n] - 1
    ret = (close[members].iloc[end] / close[members].iloc[end - n] - 1).dropna()
    if len(ret) < MIN_MEMBERS or not np.isfinite(bench):
        return None
    return float((ret > bench).mean())


def label(cells: dict[int, dict]) -> dict[int, dict]:
    """State per horizon from the raw cells {n: {z, er, rel, breadth}}."""
    out = {}
    for n, c in cells.items():
        z = c["z"]
        direction = 1 if z > 0 else -1
        missing = []
        if abs(z) >= Z_CONFIRM:
            b = c.get("breadth")
            if b is not None and (b - BREADTH_SIDE) * direction <= 0:
                missing.append("成分股跑赢 SPY 的比例未过半" if direction > 0 else "成分股跑输 SPY 的比例未过半")
            if not any(m in cells and cells[m]["z"] * direction >= Z_EMERGE for m in NEIGHBOURS[n]):
                missing.append("相邻周期没有同向")
            state = "emerging" if missing else "confirmed"
        else:
            state = "emerging" if abs(z) >= Z_EMERGE else "noise"
        word = "跑赢" if direction > 0 else "跑输"
        out[n] = {**c, "h": n, "state": state, "dir": direction if state != "noise" else 0,
                  "label": STATE_ZH[state] + (word if state != "noise" else ""),
                  "choppy": state != "noise" and c.get("er") is not None and c["er"] < ER_CHOPPY, "missing": missing}
    return out


def compute(p: FeaturePanel, etf: str, members: list[str], asof) -> list[dict]:
    """The four horizons for one ETF (members = the sector's liquid stocks, [] for a theme)."""
    c = p["close"]
    if etf not in c.columns or BENCH not in c.columns:
        return []
    end = p.dates.get_loc(asof)
    log_rs = np.log(c[etf] / c[BENCH])
    cells = {}
    for n in HORIZONS:
        s = stats(log_rs, end, n)
        if s is not None:
            cells[n] = {**s, "breadth": breadth(c, members, end, n)}
    return [_round(x) for _, x in sorted(label(cells).items())]


def _round(x: dict) -> dict:
    return {k: (round(v, 4) if isinstance(v, float) else v) for k, v in x.items()}


def by_h(row: dict) -> dict[int, dict]:
    return {x["h"]: x for x in row.get("trend") or []}


def summary(row: dict) -> str | None:
    """'短期确认跑赢 · 中期确认跑赢 · 较长噪音' (5 days left out: it is the latest move, not a trend)."""
    t = by_h(row)
    parts = [f"{SHORT[n]}{t[n]['label']}{'（震荡）' if t[n]['choppy'] else ''}" for n in (10, 20, 63) if n in t]
    return " · ".join(parts) or None


def changes(prev: dict | None, row: dict) -> list[dict] | None:
    """Moves into or out of a confirmed state, or a confirmed direction flip, since the previous session.
    None when the previous row has no trend (first run: a baseline, not a change)."""
    if not prev or not prev.get("trend"):
        return None
    before, now = by_h(prev), by_h(row)
    out = []
    for n in LEDGER_HORIZONS:
        a, b = before.get(n), now.get(n)
        if not a or not b:
            continue
        was, became = (a["state"], a["dir"]), (b["state"], b["dir"])
        if was != became and "confirmed" in (a["state"], b["state"]):
            out.append({"h": n, "from": a["label"], "to": b["label"], "entered": b["state"] == "confirmed", "dir": b["dir"]})
    return out


def change_text(name: str, ch: dict) -> str:
    return f"{name}：{SHORT[ch['h']]}由{ch['from']}转为{ch['to']}"


def new_confirmations(prev: dict | None, row: dict) -> list[dict]:
    """Confirmed horizons that were not confirmed in the same direction the previous session (a missing
    previous row counts as new: the first run records the states it finds)."""
    before = by_h(prev or {})
    out = []
    for n in LEDGER_HORIZONS:
        b = by_h(row).get(n)
        if not b or b["state"] != "confirmed":
            continue
        a = before.get(n)
        if a and a["state"] == "confirmed" and a["dir"] == b["dir"]:
            continue
        out.append(b)
    return out
