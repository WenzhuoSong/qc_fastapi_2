"""Price structure from one ticker's daily bars (Edwards & Magee / Dow theory).

- Swing pivots: a bar whose high (low) is the extreme of the K bars on each side.
- Trend by swings: higher highs + higher lows = up; lower highs + lower lows = down.
- Support / resistance: the nearest swing low below / swing high above the close.
- Volume follows the trend: up-day volume vs down-day volume over 20 sessions.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

PIVOT_K = 5          # swing structure (trend)
MAJOR_K = 10         # support / resistance: only swings that dominate ±2 weeks
LOOKBACK = 252


@dataclass
class Pivot:
    index: int
    date: object
    price: float


def pivots(highs: Sequence[float], lows: Sequence[float], dates: Sequence, k: int = PIVOT_K) -> tuple[list[Pivot], list[Pivot]]:
    """Confirmed swing highs/lows (needs k bars after the pivot)."""
    hs, ls = [], []
    for i in range(k, len(highs) - k):
        window_h = highs[i - k : i + k + 1]
        window_l = lows[i - k : i + k + 1]
        # Ties (equal highs on consecutive days) count once, at their first bar.
        if window_h.index(max(window_h)) == k:
            hs.append(Pivot(i, dates[i], highs[i]))
        if window_l.index(min(window_l)) == k:
            ls.append(Pivot(i, dates[i], lows[i]))
    return hs, ls


def swing_trend(hs: list[Pivot], ls: list[Pivot]) -> str:
    if len(hs) < 2 or len(ls) < 2:
        return "数据不足"
    higher_high, higher_low = hs[-1].price > hs[-2].price, ls[-1].price > ls[-2].price
    if higher_high and higher_low:
        return "上升：高点抬高、低点抬高"
    if not higher_high and not higher_low:
        return "下降：高点降低、低点降低"
    if higher_low:
        return "收敛：低点抬高但高点未创新高"
    return "扩散：高点抬高但低点降低（波动加大）"


def compute(bars: list[dict]) -> dict:
    """`bars`: ascending dicts with date/open/high/low/close/volume (≥ 30 rows)."""
    bars = bars[-LOOKBACK:]
    if len(bars) < 30:
        return {}
    highs = [b["high"] for b in bars]
    lows = [b["low"] for b in bars]
    closes = [b["close"] for b in bars]
    dates = [b["date"] for b in bars]
    close = closes[-1]
    hs, ls = pivots(highs, lows, dates)

    out: dict = {"swing_trend": swing_trend(hs, ls)}
    # The leg in progress has no confirmed pivot yet; a close beyond the last
    # confirmed swing is the Dow-theory signal that the structure is changing.
    # Kept as its own field so the swing shape and the break can't be conflated.
    if ls and close < ls[-1].price:
        out["swing_break"] = f"收盘跌破最近波段低点 {ls[-1].price:.2f}（{ls[-1].date}）：结构转弱"
    elif hs and close > hs[-1].price:
        out["swing_break"] = f"收盘突破最近波段高点 {hs[-1].price:.2f}（{hs[-1].date}）：结构转强"
    major_h, major_l = pivots(highs, lows, dates, MAJOR_K)
    above = [p for p in major_h if p.price > close]
    below = [p for p in major_l if p.price < close]
    if above:
        r = min(above, key=lambda p: p.price)
        out.update(resistance=r.price, resistance_date=str(r.date), dist_resistance=r.price / close - 1)
    if below:
        s = max(below, key=lambda p: p.price)
        out.update(support=s.price, support_date=str(s.date), dist_support=s.price / close - 1)
    if hs:
        out["last_swing_high"] = {"date": str(hs[-1].date), "price": hs[-1].price}
    if ls:
        out["last_swing_low"] = {"date": str(ls[-1].date), "price": ls[-1].price}

    up = down = 0.0
    for prev, cur in zip(bars[-21:-1], bars[-20:]):
        if cur["close"] > prev["close"]:
            up += cur["volume"]
        elif cur["close"] < prev["close"]:
            down += cur["volume"]
    if down > 0:
        out["updown_vol_20"] = up / down
    return out
