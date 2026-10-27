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


def stop_for(mid: float, buy_point: float, below: list[dict], atr: float, ref: str = "买点") -> tuple[float, str]:
    """Tightest stop under the zone that is 1–3.5 ATR away: one ATR under the buy point,
    or just under a lower level; two ATR when nothing fits."""
    cands = [(buy_point - MIN_STOP_ATR * atr, f"{ref}下方 1 个 ATR")]
    cands += [(p["price"] - STOP_BUFFER_ATR * atr, f"{p['label']} 下方") for p in below if p["price"] < buy_point]
    valid = [(v, b) for v, b in cands if MIN_STOP_ATR * atr <= mid - v <= MAX_STOP_ATR * atr]
    if valid:
        return max(valid)
    return mid - FALLBACK_STOP_ATR * atr, f"{FALLBACK_STOP_ATR:g} 个 ATR（附近没有合适的结构位）"


def targets_for(mid: float, risk: float, over: list[dict]) -> tuple[float, str, float, str]:
    """Target 1: the first level above within 2R, else 2R; target 2: the next level within 4R, else 3R."""
    if over and over[0]["price"] < mid + 2 * risk:
        t1, t1_basis = over[0]["price"], over[0]["label"]
    else:
        t1, t1_basis = mid + 2 * risk, "2R"
    nxt = [p for p in over if p["price"] > t1 * 1.005]
    if nxt and nxt[0]["price"] < mid + 4 * risk:
        t2, t2_basis = nxt[0]["price"], nxt[0]["label"]
    else:
        t2, t2_basis = max(mid + 3 * risk, t1), "3R"
    return t1, t1_basis, t2, t2_basis


def entry(name: str, buy_point: float, f: dict, above: list[dict], below: list[dict], note: str) -> dict:
    atr = f["atr14"]
    hi = buy_point + min(ZONE_ATR * atr, MAX_ZONE_PCT * buy_point)
    mid = (buy_point + hi) / 2
    stop, basis = stop_for(mid, buy_point, below, atr)
    risk = mid - stop
    t1, t1_basis, t2, t2_basis = targets_for(mid, risk, [p for p in above if p["price"] > hi])
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
    """The plan a recorded buy starts from: the entry whose zone holds the fill, else stop and
    targets computed the same way from the fill itself (a buy outside any zone still gets a plan)."""
    e = next((x for x in pm.get("entries") or [] if x["zone_lo"] <= fill <= x["zone_hi"]), None)
    if e:
        name, stop, basis, t1, b1, t2, b2 = (e["name"], e["stop"], e["stop_basis"], e["target1"], e["target1_basis"],
                                             e["target2"], e["target2_basis"])
    else:
        atr = pm.get("atr")
        if not atr:
            return None
        pts = (pm.get("resistances") or []) + (pm.get("supports") or [])
        above = sorted((p for p in pts if p["price"] > fill), key=lambda p: p["price"])
        below = sorted((p for p in pts if p["price"] < fill), key=lambda p: -p["price"])
        stop, basis = stop_for(fill, fill, below, atr, ref="成交价")
        t1, b1, t2, b2 = targets_for(fill, fill - stop, above)
        name = "按成交价"
    return {"from": name, "stop": round(stop, 2), "stop_basis": basis, "target1": round(t1, 2), "target1_basis": b1,
            "target2": round(t2, 2), "target2_basis": b2, "time_stop_days": TIME_STOP.get(name, 15),
            "opened_on": opened_on.isoformat(), "map_asof": pm.get("asof"), "risk_pct": round(stop / fill - 1, 4),
            "in_zone": e is not None, "history": []}


def manage(plan: dict, highs: list[float], closes: list[float], atr: float | None, asof: date) -> tuple[float, float | None, str, list[str]]:
    """Today's system stop and target for an open trade, from its plan and the bars since the buy.

    `highs`: prices traded after the fill per session (the buy day counts with its close);
    `closes` start on the buy day. Stop: the plan's stop, raised (never lowered) to the chandelier stop over the closes since the
    buy (highest close of the last 22 sessions − 3 ATR) once that is higher and still below the
    last close. Target: target 1, then target 2 once target 1 traded, then none (the trailing stop
    manages the rest). Changes are appended to plan['history']; returns (stop, target, stop basis, messages)."""
    stop, basis = plan.get("stop_now", plan["stop"]), plan.get("stop_now_basis", plan["stop_basis"])
    msgs: list[str] = []
    if atr and closes:
        chand = round(max(closes[-CHANDELIER_DAYS:]) - CHANDELIER_ATR * atr, 2)
        if stop + 0.005 < chand < closes[-1]:
            msgs.append(f"止损由 {stop:.2f} 上移到 {chand:.2f}（吊灯止损）")
            stop, basis = chand, f"吊灯止损（{CHANDELIER_DAYS} 日最高收盘 − {CHANDELIER_ATR:g}×ATR）"
        elif closes[-1] <= chand and closes[-1] > stop and not plan.get("chandelier_broken"):
            plan["chandelier_broken"] = asof.isoformat()
            msgs.append(f"收盘 {closes[-1]:.2f} 已低于吊灯止损 {chand:.2f}，趋势转弱，止损仍为 {stop:.2f}")
    top = max(highs) if highs else None
    for k in ("target1", "target2"):
        if top is not None and top >= plan[k] and not plan.get(f"{k}_hit"):
            plan[f"{k}_hit"] = asof.isoformat()
            msgs.append(f"到达目标 {k[-1]} {plan[k]:.2f}" + ("，可减仓一部分，止盈改看目标 2" if k == "target1"
                                                         else "，余下仓位只用移动止损管理"))
    target = None if plan.get("target2_hit") else plan["target2"] if plan.get("target1_hit") else plan["target1"]
    plan["stop_now"], plan["stop_now_basis"] = stop, basis
    for m in msgs:
        plan.setdefault("history", []).append({"date": asof.isoformat(), "text": m})
    return stop, target, basis, msgs


# ── stored per data date ──

async def get_map(ticker: str, refresh: bool = False, before: date | None = None) -> dict | None:
    """The price map for the latest data date (or the last one before `before`: what was known
    when a buy was made); computed and stored on first use."""
    from scout import db, research_data, structure

    feat = await db.fetch_one("SELECT date, payload FROM scout.features_daily WHERE ticker = :t AND date < :b "
                              "ORDER BY date DESC LIMIT 1", {"t": ticker, "b": before or date(9999, 1, 1)})
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
    opt = await research_data.latest(ticker, "options", feat["date"])
    est = await research_data.latest(ticker, "estimates", feat["date"])
    pm = price_map(feat["payload"] or {}, structure.compute(bars), (opt or {}).get("payload"), (est or {}).get("payload"))
    if not pm:
        return None
    pm.update(asof=str(feat["date"]), computed_at=datetime.now(timezone.utc).isoformat(timespec="minutes"))
    await db.execute(
        "INSERT INTO scout.price_levels (ticker, asof, payload) VALUES (:t, :d, CAST(:p AS JSONB)) "
        "ON CONFLICT (ticker, asof) DO UPDATE SET payload = EXCLUDED.payload, created_at = now()",
        {"t": ticker, "d": feat["date"], "p": db.jsonb(pm)})
    return pm
