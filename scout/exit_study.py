"""Trailing-stop study for the holdings plan: same entries, different ways to trail the stop.

The holdings plan sells a third at target 1, a third at target 2 and trails the rest.
This replays that plan over every historical rule signal (same trigger and fill rule
as `scout/simulate.py`) with different trailing stops, and reports each net of
`ROUND_TRIP_COST`, against just holding the stock, in both halves of the sample.
An exit changes only when it is better in both halves (CLAUDE.md).

Conventions (daily bars, conservative): the stop is checked before targets on every
bar; a gap through a level fills at the open; a trailing level is computed from the
close of a bar and applies from the next bar; a close-based stop exits at the next open.
"""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from scout.simulate import ROUND_TRIP_COST

MAX_HOLD = 60                 # sessions after the fill; whatever is left is sold at that close
SCALE = (1 / 3, 1 / 3)        # sold at target 1 and target 2; the rest trails
SWING_K = 3                   # a swing low needs 3 higher lows on each side (confirmed 3 bars later)
BUFFER_ATR = 0.25


@dataclass(frozen=True)
class Policy:
    name: str
    label: str
    trail: str = "none"       # none | chandelier | swing | chand_swing | low5 | ma20 | ma50
    after_t1: bool = False    # trail only once target 1 traded (before: the initial stop)
    close_based: bool = False # the trailing stop triggers on a close below it (exit next open)
    scale: tuple[float, ...] = SCALE   # shares sold at target 1 (and target 2); () = no partial sells


POLICIES = (
    Policy("fixed", "不移动止损（初始止损 + 分批止盈，余下持有到 60 日）"),
    Policy("chandelier", "吊灯止损：最高收盘 − 3×ATR（当前方案）", "chandelier"),
    Policy("chandelier_t1", "吊灯止损，到目标 1 后才启用", "chandelier", after_t1=True),
    Policy("chandelier_close", "吊灯止损，收盘跌破才离场", "chandelier", close_based=True),
    Policy("swing", "结构止损：最近确认的波段低点 − 0.25 ATR", "swing"),
    Policy("swing_t1", "结构止损，到目标 1 后才启用", "swing", after_t1=True),
    Policy("chand_swing_t1", "到目标 1 后取吊灯与结构止损中较高者", "chand_swing", after_t1=True),
    Policy("low5", "5 日最低价", "low5"),
    Policy("ma20", "收盘跌破 20 日线离场", "ma20"),
    Policy("ma50", "收盘跌破 50 日线离场", "ma50"),
    Policy("chandelier_third", "只在目标 1 卖 1/3，其余用吊灯止损", "chandelier", scale=(1 / 3,)),
    Policy("chandelier_all", "不分批，全部仓位用吊灯止损", "chandelier", scale=()),
    Policy("chandelier_all_t1", "不分批，到目标 1 后才启用吊灯止损", "chandelier", after_t1=True, scale=()),
)


@dataclass
class Path:
    """One stock's bars from `pre` sessions before the fill bar (index `start`) onward."""
    open: np.ndarray
    high: np.ndarray
    low: np.ndarray
    close: np.ndarray
    atr: np.ndarray
    sma20: np.ndarray
    sma50: np.ndarray
    start: int


def swing_low(low: np.ndarray, upto: int, k: int = SWING_K) -> float | None:
    """The latest swing low confirmed by bar `upto` (its k bars after must exist by then)."""
    for i in range(upto - k, k - 1, -1):
        w = low[i - k : i + k + 1]
        if np.isfinite(w).all() and low[i] == w.min():
            return float(low[i])
    return None


def trail_level(p: Path, pol: Policy, k: int, top_close: float) -> float | None:
    """The trailing level after the close of bar k (absolute index), or None."""
    atr = p.atr[k]
    if pol.trail in ("chandelier", "chand_swing") and np.isfinite(atr):
        chand = top_close - 3.0 * atr
    else:
        chand = None
    if pol.trail == "chandelier":
        return chand
    if pol.trail in ("swing", "chand_swing"):
        sw = swing_low(p.low, k)
        sw = sw - BUFFER_ATR * atr if sw is not None and np.isfinite(atr) else None
        if pol.trail == "swing":
            return sw
        vals = [v for v in (chand, sw) if v is not None]
        return max(vals) if vals else None
    if pol.trail == "low5":
        return float(np.nanmin(p.low[k - 4 : k + 1]))
    return None


def manage(p: Path, entry: float, stop0: float, t1: float, t2: float, pol: Policy) -> tuple[float, int]:
    """(gross return of the whole position, sessions held) for one filled trade."""
    left, proceeds, stop, trail = 1.0, 0.0, stop0, stop0
    t1_done = t2_done = False
    exit_next_open = False
    top_close = entry
    end = min(p.start + MAX_HOLD, len(p.close))
    k = p.start
    for k in range(p.start, end):
        o, h, lo, c = p.open[k], p.high[k], p.low[k], p.close[k]
        first = k == p.start
        if exit_next_open:
            proceeds += left * o
            left = 0.0
            break
        if lo <= stop:                                    # stop first (worst case on a two-sided day)
            proceeds += left * (stop if (first or o > stop) else o)
            left = 0.0
            break
        if not t1_done and h >= t1:
            t1_done = True
            if pol.scale:
                proceeds += pol.scale[0] * (t1 if (first or o < t1) else o)
                left -= pol.scale[0]
        if t1_done and not t2_done and h >= t2:
            t2_done = True
            if len(pol.scale) > 1:
                proceeds += pol.scale[1] * (t2 if (first or o < t2) else o)
                left -= pol.scale[1]
        top_close = max(top_close, c)
        active = not pol.after_t1 or t1_done
        if active and pol.trail in ("ma20", "ma50"):
            ma = p.sma20[k] if pol.trail == "ma20" else p.sma50[k]
            if np.isfinite(ma) and c < ma:
                exit_next_open = True
        elif active and pol.trail != "none":
            if pol.close_based and c < trail:              # closed under yesterday's trailing level
                exit_next_open = True
            level = trail_level(p, pol, k, top_close)
            if level is not None:
                if pol.close_based:                        # the intraday stop stays the initial one
                    trail = max(trail, level)
                else:
                    stop = max(stop, level)
    else:
        if left > 0:
            proceeds += left * p.close[end - 1]
            left = 0.0
    if left > 0:                                          # ran out of bars on an exit-next-open
        proceeds += left * p.close[k]
    return proceeds / entry - 1, k - p.start + 1


def summarize(rows: list[dict]) -> dict:
    """rows: {ret, days, hold, r} per trade."""
    if not rows:
        return {"n": 0}
    ret = np.array([r["ret"] for r in rows]) - ROUND_TRIP_COST
    vs_hold = np.array([r["ret"] - ROUND_TRIP_COST - r["hold"] for r in rows if r["hold"] is not None])
    return {"n": len(rows), "avg_net": round(float(ret.mean()), 5), "median_net": round(float(np.median(ret)), 5),
            "win_rate": round(float((ret > 0).mean()), 4), "avg_r": round(float(np.mean([r["r"] for r in rows])), 3),
            "edge_vs_hold": round(float(vs_hold.mean()), 5) if vs_hold.size else None,
            "avg_days": round(float(np.mean([r["days"] for r in rows])), 1)}
