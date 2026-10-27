"""Price map for any stock: the levels a professional desk would hand you.

Computed once per data date (after the close) and stored, so the levels do
not move during the session; the live price only changes the *status* (in a
buy zone, extended, not reached, below the pivot). Modelled on how research
platforms present levels:

- key levels: supports / resistances 1–3 (major swing pivots), moving
  averages, the 52-week high; the options-implied ±1σ range to expiry and the
  analysts' target range as reality checks;
- a pivot (where the preferred view is wrong) with a preferred and an
  alternative scenario (Trading Central style);
- buy points with a buy zone from the point up to min(0.5 ATR, 5 %) above it,
  beyond which the stock is extended and not chased (O'Neil); a protective
  stop at least one ATR from the zone (its percentage shown next to fixed-%
  rules); target 1 at the first resistance (or 2R) to take part of the
  position, target 2 at the next resistance (or 3R) with a trailing stop for
  the rest; a time stop.

Rule opportunities keep their own plan (fixed on the signal day, scored by
the replay). These levels are references, not replayed rules; the page says so.
"""
from __future__ import annotations

from datetime import date, datetime, timezone

ZONE_ATR = 0.5
MAX_ZONE_PCT = 0.05          # O'Neil: more than ~5 % above the buy point is extended
STOP_BUFFER_ATR = 0.25       # a stop sits a little below the level it protects
MIN_STOP_ATR = 1.0           # closer than one ATR is inside normal daily noise
MAX_STOP_ATR = 3.5
FALLBACK_STOP_ATR = 2.0
MAX_PULLBACK_ATR = 4.0       # a level further below is not a pullback buy point
MAX_BREAKOUT_PCT = 0.15      # a resistance further above is not a near-term buy point
MIN_RR = 1.0                 # less reward to target 1 than risk to the stop is flagged
TIME_STOP = {"回调买点": 10, "突破买点": 20, "反转确认": 20}
CHANDELIER_ATR = 3.0
CHANDELIER_DAYS = 22


def trend_of(f: dict) -> str:
    close, s50, s200 = f.get("close"), f.get("sma50"), f.get("sma200")
    if not (close and s50 and s200):
        return "unknown"
    if close > s50 > s200:
        return "up"
    if close < s200 and s50 < s200:
        return "down"
    return "mixed"


def key_levels(f: dict, s: dict) -> tuple[list[dict], list[dict]]:
    """(resistances, supports) nearest first: swing levels plus moving averages and the 52-week high."""
    close = f["close"]
    pts = [{"price": r["price"], "label": f"阻力 {i + 1}", "kind": "swing"} for i, r in enumerate(s.get("resistances") or [])]
    pts += [{"price": r["price"], "label": f"支撑 {i + 1}", "kind": "swing"} for i, r in enumerate(s.get("supports") or [])]
    for key, label in (("sma20", "20 日线"), ("sma50", "50 日线"), ("sma200", "200 日线")):
        if f.get(key):
            pts.append({"price": f[key], "label": label, "kind": "ma"})
    if f.get("dist_52w_high") is not None:
        pts.append({"price": close / (1 + f["dist_52w_high"]), "label": "52 周高点", "kind": "high"})
    above = sorted((p for p in pts if p["price"] > close), key=lambda p: p["price"])
    below = sorted((p for p in pts if p["price"] < close), key=lambda p: -p["price"])
    return above, below


def stop_for(mid: float, buy_point: float, below: list[dict], atr: float) -> tuple[float, str]:
    """Tightest stop under the zone that is 1–3.5 ATR away: one ATR under the buy point,
    or just under a lower level; two ATR when nothing fits."""
    cands = [(buy_point - MIN_STOP_ATR * atr, "买点下方 1 个 ATR")]
    cands += [(p["price"] - STOP_BUFFER_ATR * atr, f"{p['label']} 下方") for p in below if p["price"] < buy_point]
    valid = [(v, b) for v, b in cands if MIN_STOP_ATR * atr <= mid - v <= MAX_STOP_ATR * atr]
    if valid:
        return max(valid)
    return mid - FALLBACK_STOP_ATR * atr, f"{FALLBACK_STOP_ATR:g} 个 ATR（附近没有合适的结构位）"


def entry(name: str, buy_point: float, f: dict, above: list[dict], below: list[dict], note: str) -> dict:
    atr = f["atr14"]
    hi = buy_point + min(ZONE_ATR * atr, MAX_ZONE_PCT * buy_point)
    mid = (buy_point + hi) / 2
    stop, basis = stop_for(mid, buy_point, below, atr)
    risk = mid - stop
    over = [p for p in above if p["price"] > hi]
    if over and over[0]["price"] < mid + 2 * risk:
        t1, t1_basis = over[0]["price"], over[0]["label"]
    else:
        t1, t1_basis = mid + 2 * risk, "2R"
    nxt = [p for p in over if p["price"] > t1 * 1.005]
    if nxt and nxt[0]["price"] < mid + 4 * risk:
        t2, t2_basis = nxt[0]["price"], nxt[0]["label"]
    else:
        t2, t2_basis = max(mid + 3 * risk, t1), "3R"
    rr = round((t1 - mid) / risk, 1)
    return {"name": name, "buy_point": round(buy_point, 2), "zone_lo": round(buy_point, 2), "zone_hi": round(hi, 2),
            "stop": round(stop, 2), "stop_basis": basis, "stop_pct": round(stop / mid - 1, 4),
            "target1": round(t1, 2), "target1_basis": t1_basis, "target2": round(t2, 2), "target2_basis": t2_basis,
            "rr": rr, "time_stop_days": TIME_STOP.get(name, 15), "note": note,
            "warning": f"到目标 1 的空间小于止损距离（R:R {rr:.1f}），性价比低" if rr < MIN_RR else None}


def price_map(f: dict, s: dict, options: dict | None = None, estimates: dict | None = None) -> dict:
    """Levels, pivot and scenarios, entries, and reality checks from one data date."""
    atr, close = f.get("atr14"), f.get("close")
    if not atr or not close:
        return {}
    trend = trend_of(f)
    above, below = key_levels(f, s)
    out: dict = {"close": round(close, 2), "atr": round(atr, 2), "trend": trend,
                 "resistances": [{**p, "price": round(p["price"], 2)} for p in above[:5]],
                 "supports": [{**p, "price": round(p["price"], 2)} for p in below[:5]], "entries": []}
    swing_above = [p for p in above if p["kind"] == "swing"]
    r1 = swing_above[0] if swing_above else None
    # The pivot is the first level whose break changes the picture: a swing low or the 50 / 200-day line
    # (the 20-day line is crossed too often to mean much).
    structural = [p for p in below if p["kind"] == "swing" or p["label"] in ("50 日线", "200 日线")]
    s1 = structural[0] if structural else None

    if trend == "down":
        # the nearer of the first swing resistance and the 50-day line: where a downtrend is first in question
        piv = min((p for p in above if p["kind"] == "swing" or p["label"] == "50 日线"), key=lambda p: p["price"], default=None)
        if piv:
            out["pivot"] = {"price": round(piv["price"], 2), "label": piv["label"], "side": "above"}
            out["preferred"] = f"偏空：价格在 {piv['label']} {piv['price']:.2f} 之下，趋势向下" + (
                f"，下方先看 {below[0]['label']} {below[0]['price']:.2f}" if below else "") + "。"
            nxt = [p for p in above if p["price"] > piv["price"] * 1.005]
            out["alternative"] = f"放量收盘站上 {piv['price']:.2f}，结构转强，转为看多" + (
                f"，上方看 {nxt[0]['label']} {nxt[0]['price']:.2f}" if nxt else "") + "。"
    elif s1:
        out["pivot"] = {"price": round(s1["price"], 2), "label": s1["label"], "side": "below"}
        bias = "偏多" if trend == "up" else "震荡（均线方向不一致）"
        up_next = above[0] if above else None
        out["preferred"] = (f"{bias}：守住 {s1['label']} {s1['price']:.2f} 之上，" +
                            (f"上方先看 {up_next['label']} {up_next['price']:.2f}" if up_next else "上方没有明显阻力") + "。")
        lower = [p for p in structural if p["price"] < s1["price"] * 0.995]
        out["alternative"] = f"收盘跌破 {s1['price']:.2f}，判断失效，转为偏弱" + (
            f"，下方看 {lower[0]['label']} {lower[0]['price']:.2f}" if lower else "") + "。"

    if trend != "down":
        near = [p for p in below if close - p["price"] <= MAX_PULLBACK_ATR * atr and p["kind"] in ("swing", "ma")]
        if near:
            bp = near[0]
            out["entries"].append(entry("回调买点", bp["price"], f, above, below,
                                        f"回落到 {bp['label']} 附近并企稳（缩量回调、收盘守住）再考虑；有效跌破止损说明支撑失效。"))
    if r1 and r1["price"] / close - 1 <= MAX_BREAKOUT_PCT:
        name = "突破买点" if trend != "down" else "反转确认"
        note = ("放量（量比 ≥ 1.5 倍）收盘站上阻力才算有效突破；冲高回落、缩量站上都不算。" if trend != "down"
                else "下降趋势中只在放量收复阻力、结构转强后才考虑做多。")
        out["entries"].append(entry(name, r1["price"], f, [p for p in above if p["price"] > r1["price"]], below, note))
    if trend == "down" and not out["entries"]:
        out["note"] = "下降趋势中，近处没有可确认的反转位置，不给做多的买点。"

    front = ((options or {}).get("expiries") or {}).get("front") or {}
    if (options or {}).get("quality") == "ok" and front.get("straddle_move"):
        m = front["straddle_move"]
        out["expected_move"] = {"move": m, "expiry": front.get("expiry"), "low": round(close * (1 - m), 2),
                                "high": round(close * (1 + m), 2)}
    pt = (estimates or {}).get("price_targets") or {}
    if pt.get("mean"):
        out["analyst_targets"] = {k: pt.get(k) for k in ("low", "median", "mean", "high")}
    return out


def status(pm: dict, price: float) -> dict:
    """Live state against the stored map: per entry, and against the pivot. Levels never move here."""
    out: dict = {"entries": []}
    for e in pm.get("entries") or []:
        lo, hi = e["zone_lo"], e["zone_hi"]
        if lo <= price <= hi:
            st, text = "in_zone", "现价在买入区间内" + ("（仍需放量收盘确认）" if e["name"] != "回调买点" else "（看是否企稳）")
        elif e["name"] == "回调买点":
            if price > hi:
                st, text = "waiting", f"等回调：距买点 {(lo / price - 1) * 100:+.1f}%"
            elif price > e["stop"]:
                st, text = "below", f"跌到买点下方 {(1 - price / lo) * 100:.1f}%，等收盘重新站回 {lo:.2f}"
            else:
                st, text = "failed", f"已跌破止损 {e['stop']:.2f}，这个回调买点失效"
        elif price < lo:
            st, text = "waiting", f"未突破：距买点 {(lo / price - 1) * 100:+.1f}%"
        else:
            st, text = "extended", f"已脱离买入区间（高于上沿 {(price / hi - 1) * 100:.1f}%），不追；回到 {hi:.2f} 以下再看"
        out["entries"].append({"name": e["name"], "state": st, "text": text})
    piv = pm.get("pivot")
    if piv:
        broken = price < piv["price"] if piv["side"] == "below" else price > piv["price"]
        out["pivot_broken"] = broken
        if broken:
            out["pivot_text"] = ("现价已跌破多空分界，若收盘确认，按备选情景处理" if piv["side"] == "below"
                                 else "现价已站上多空分界，若收盘确认，反转情景开始生效")
    return out


def locked_plan(pm: dict, fill: float, opened_on: date) -> dict | None:
    """The plan a recorded buy inherits: the entry whose zone holds the fill, else the nearest;
    stop and targets are kept, R is measured from the fill."""
    entries = pm.get("entries") or []
    if not entries:
        return None
    e = next((x for x in entries if x["zone_lo"] <= fill <= x["zone_hi"]),
             min(entries, key=lambda x: abs(fill - (x["zone_lo"] + x["zone_hi"]) / 2)))
    return {"from": e["name"], "stop": e["stop"], "stop_basis": e["stop_basis"], "target1": e["target1"],
            "target2": e["target2"], "time_stop_days": e["time_stop_days"], "opened_on": opened_on.isoformat(),
            "risk_pct": round(e["stop"] / fill - 1, 4), "in_zone": e["zone_lo"] <= fill <= e["zone_hi"]}


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


# ── stored per data date ──

async def get_map(ticker: str, refresh: bool = False) -> dict | None:
    """The price map for the latest data date; computed and stored on first use that day."""
    from scout import db, research_data, structure

    feat = await db.fetch_one("SELECT date, payload FROM scout.features_daily WHERE ticker = :t ORDER BY date DESC LIMIT 1",
                              {"t": ticker})
    if not feat:
        return None
    if not refresh:
        row = await db.fetch_one("SELECT payload FROM scout.price_levels WHERE ticker = :t AND asof = :d",
                                 {"t": ticker, "d": feat["date"]})
        if row:
            return row["payload"]
    bars = await db.fetch_all(
        "SELECT * FROM (SELECT date, open, high, low, close, volume FROM scout.bars_daily WHERE ticker = :t AND date <= :d "
        "ORDER BY date DESC LIMIT 260) x ORDER BY date", {"t": ticker, "d": feat["date"]})
    opt = await research_data.latest(ticker, "options")
    est = await research_data.latest(ticker, "estimates")
    pm = price_map(feat["payload"] or {}, structure.compute(bars), (opt or {}).get("payload"), (est or {}).get("payload"))
    if not pm:
        return None
    pm.update(asof=str(feat["date"]), computed_at=datetime.now(timezone.utc).isoformat(timespec="minutes"))
    await db.execute(
        "INSERT INTO scout.price_levels (ticker, asof, payload) VALUES (:t, :d, CAST(:p AS JSONB)) "
        "ON CONFLICT (ticker, asof) DO UPDATE SET payload = EXCLUDED.payload, created_at = now()",
        {"t": ticker, "d": feat["date"], "p": db.jsonb(pm)})
    return pm
