"""Reference price levels for any stock, recomputed after every close.

Rule opportunities carry their own plan (fixed on the signal day, part of
the rule and of its replay). Every other stock gets reference scenarios built
the same way from structure and volatility: a pullback entry near support
and a breakout entry above resistance, each with a protective stop at least
one ATR from the entry and targets in R multiples (or the next resistance).
These are reference levels, not rule signals: they have no replay statistics,
and the page says so. Holdings get trailing-stop suggestions.
"""
from __future__ import annotations

ZONE_ATR = 0.5            # entry zone width
STOP_ATR = 1.0            # stop below the anchor
MAX_ANCHOR_ATR = 4.0      # a support further than this is not a pullback level any more
MAX_BREAKOUT = 0.15       # resistance further than 15 % above is not a near-term trigger
CHANDELIER_ATR = 3.0
CHANDELIER_DAYS = 22


def trend_of(f: dict) -> str:
    close, s50, s200 = f.get("close"), f.get("sma50"), f.get("sma200")
    if not (close and s50 and s200):
        return "unknown"
    if close > s200 and s50 > s200:
        return "up"
    if close < s200 and s50 < s200:
        return "down"
    return "mixed"


def _scenario(name: str, lo: float, hi: float, stop: float, ceiling: float | None, price: float, note: str) -> dict:
    mid = (lo + hi) / 2
    risk = mid - stop
    t1 = mid + 2 * risk
    if ceiling and ceiling > mid + risk:          # the next resistance is a natural first target when it is past 1R
        t1 = min(t1, ceiling)
    t2 = max(mid + 3 * risk, t1)
    return {"name": name, "entry_lo": round(lo, 2), "entry_hi": round(hi, 2), "stop": round(stop, 2),
            "target1": round(t1, 2), "target2": round(t2, 2), "rr": round((t1 - mid) / risk, 2),
            "stop_pct": round(stop / mid - 1, 4), "in_zone": lo <= price <= hi,
            "distance": round(mid / price - 1, 4), "note": note}


def reference_plans(f: dict, s: dict, price: float | None = None) -> dict:
    """Pullback and breakout scenarios from the latest features (`f`) and structure (`s`)."""
    atr = f.get("atr14")
    price = price or f.get("close")
    if not atr or not price:
        return {}
    trend = trend_of(f)
    out: dict = {"trend": trend, "atr": atr, "scenarios": []}
    support, resistance = s.get("support"), s.get("resistance")

    if trend != "down":
        anchors = [(n, v) for n, v in (("支撑位", support), ("20 日线", f.get("sma20")), ("50 日线", f.get("sma50")))
                   if v and v < price and (price - v) <= MAX_ANCHOR_ATR * atr]
        if anchors:
            name, anchor = max(anchors, key=lambda a: a[1])          # the nearest level below the price
            out["scenarios"].append(_scenario(
                "回调买入", anchor, min(anchor + ZONE_ATR * atr, price), anchor - STOP_ATR * atr, resistance, price,
                f"等价格回落到{name}附近并企稳（缩量回调、收盘守住区间）再考虑；有效跌破止损位说明支撑失效。"))

    trigger = resistance if resistance and resistance / price - 1 <= MAX_BREAKOUT else None
    if trigger:
        label = "突破买入" if trend != "down" else "反转确认"
        note = ("放量（量比 ≥ 1.5 倍）收盘站上阻力位才算有效突破；冲高回落、缩量站上都不算。"
                if trend != "down" else "下降趋势中只在放量收复阻力位、结构转强后才考虑，否则不做多。")
        out["scenarios"].append(_scenario(label, trigger, trigger + ZONE_ATR * atr, trigger - STOP_ATR * atr, None, price, note))
    if trend == "down" and not trigger:
        out["note"] = "下降趋势中，近处没有可确认的反转位置，不提供做多参考价位。"
    return out


def trailing_stops(f: dict, closes: list[float], lows: list[float]) -> list[dict]:
    """Ways to raise a stop under a holding, each with what it protects."""
    atr = f.get("atr14")
    out = []
    if atr and len(closes) >= CHANDELIER_DAYS:
        top = max(closes[-CHANDELIER_DAYS:])
        out.append({"name": f"吊灯止损（{CHANDELIER_DAYS} 日最高收盘 − {CHANDELIER_ATR:g}×ATR）", "price": round(top - CHANDELIER_ATR * atr, 2),
                    "note": "给趋势留足波动空间，适合想拿住趋势的仓位"})
    if len(lows) >= 5:
        out.append({"name": "5 日最低价", "price": round(min(lows[-5:]), 2), "note": "最紧，适合短线或已有大幅浮盈"})
    if f.get("sma50"):
        out.append({"name": "50 日线", "price": round(f["sma50"], 2), "note": "中期趋势线，跌破说明中期趋势转弱"})
    # No "move to breakeven after 1R": in Scout's exit replay (2026-10-26) it lowered every rule's results.
    return sorted(out, key=lambda x: -x["price"])
