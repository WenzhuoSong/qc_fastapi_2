"""The user's own execution record, shown where a decision is made (phase 3 of the market read work, user's request
2026-11-25: "交易心理" includes one's own; a stop signal went unexecuted for days).

From the journal, against the user's own plan: each stop signal (executed on plan, late, or still outstanding) with
what the wait cost — the planned exit is the open of the session after the signal, compared with the actual sell
price or, while outstanding, the last close, times the shares; each sell made without a signal (early) and how the
stock has moved since; each buy that left the satellite over its cap (account size and cap as set now, so an
approximation). Facts about a handful of trades, not statistics: the text never infers a habit from them.

Lines go into the to-do list's sell items, the buy checks (and so the candidate cards and check_trade_idea), the
early-sell check, and one line of the weekly push. Buys before the cap existed (CAP_SINCE) are not counted.
"""
from __future__ import annotations

from datetime import date

from scout import db, levels, portfolio, position_risk

CAP_SINCE = date(2026, 10, 31)      # the satellite cap is the user's rule from this day; earlier buys are not held to it


async def _bars(tickers: list[str], since: date) -> dict[str, list[dict]]:
    rows = await db.fetch_all("SELECT ticker, date, open, close FROM scout.bars_daily WHERE ticker = ANY(:t) AND date >= :d ORDER BY date",
                              {"t": tickers, "d": since})
    out: dict[str, list[dict]] = {}
    for r in rows:
        out.setdefault(r["ticker"], []).append(r)
    return out


def _remaining(t: dict, sells: list[dict], on: date) -> float:
    return t["shares"] - sum(s["shares"] for s in sells if s["sold_on"] < on)


async def record(today: date) -> dict:
    trades = [t for t in await portfolio.trades() if not levels.is_core(t["ticker"])]
    if not trades:
        return {"stops": [], "early": [], "over_cap": []}
    sells = {t["id"]: await portfolio.sells(t["id"]) for t in trades}
    bars = await _bars(sorted({t["ticker"] for t in trades}), min(t["opened_on"] for t in trades))
    stops, early = [], []
    for t in trades:
        ts, b = sells[t["id"]], bars.get(t["ticker"], [])
        hit = (t.get("plan") or {}).get("stop_hit")
        if hit:
            d = date.fromisoformat(hit)
            nxt = next((x for x in b if x["date"] > d), None)
            after = [s for s in ts if s["sold_on"] > d]
            sessions = position_risk.adherence(t, ts, today)
            if after:
                s = after[0]
                waited = sessions_between(d, s["sold_on"])
                cost = (nxt["open"] - s["price"]) * s["shares"] if nxt else None
                stops.append({"ticker": t["ticker"], "signal": d, "state": "late" if waited > position_risk.ADHERENCE_GRACE else "on_plan",
                              "sessions": waited, "cost": cost})
            elif not t.get("closed_on"):
                last = b[-1] if b else None
                shares = _remaining(t, ts, today)
                cost = (nxt["open"] - last["close"]) * shares if nxt and last else None
                stops.append({"ticker": t["ticker"], "signal": d, "state": "outstanding",
                              "sessions": (sessions.get("signal") or {}).get("sessions") or sessions_between(d, today), "cost": cost})
        adh = position_risk.adherence(t, ts, today)
        for s, a in zip(ts, adh["sells"]):
            if a["kind"] == "early" and b:
                early.append({"ticker": t["ticker"], "sold_on": s["sold_on"], "price": s["price"], "since": b[-1]["close"] / s["price"] - 1})
    return {"stops": stops, "early": early, "over_cap": await over_cap_buys(trades, sells, bars)}


def sessions_between(a: date, b: date) -> int:
    from scout.intraday import sessions_since

    return sessions_since(a, b)


async def over_cap_buys(trades: list[dict], sells: dict[int, list[dict]], bars: dict[str, list[dict]]) -> list[dict]:
    """Buys after which the satellite (other open satellite trades at that day's close + this buy) exceeded the cap."""
    st = await portfolio.get_settings()
    if not st.get("account_size") or not st.get("satellite_max_pct"):
        return []
    cap = st["account_size"] * st["satellite_max_pct"]
    out = []
    for t in trades:
        d = t["opened_on"]
        if d < CAP_SINCE:
            continue
        used = t["shares"] * t["entry_price"]
        for o in trades:
            if o["id"] == t["id"] or o["opened_on"] > d or (o.get("closed_on") and o["closed_on"] <= d):
                continue
            px = next((x["close"] for x in reversed(bars.get(o["ticker"], [])) if x["date"] <= d), o["entry_price"])
            used += _remaining(o, sells[o["id"]], d) * px
        if used > cap:
            last = (bars.get(t["ticker"]) or [{}])[-1].get("close")
            out.append({"ticker": t["ticker"], "date": d, "pct": used / st["account_size"],
                        "since": last / t["entry_price"] - 1 if last else None})
    return out


# ── lines ──

def _usd(x: float) -> str:
    return f"{'+' if x > 0 else '−'}${abs(x):,.0f}"


def stop_line(rec: dict) -> str | None:
    s = rec["stops"]
    if not s:
        return None
    on = sum(x["state"] == "on_plan" for x in s)
    late = [x for x in s if x["state"] == "late"]
    out = [x for x in s if x["state"] == "outstanding"]
    parts = [f"止损信号 {len(s)} 次：按时执行 {on} 次"]
    if late:
        parts.append(f"拖延后执行 {len(late)} 次（" + "、".join(f"{x['ticker']} 晚 {x['sessions']} 个交易日" for x in late) + "）")
    if out:
        parts.append(f"还没执行 {len(out)} 次（" + "、".join(f"{x['ticker']} 已过 {x['sessions']} 个交易日" for x in out) + "）")
    costs = [x["cost"] for x in late + out if x["cost"] is not None]
    if costs:
        total = sum(costs)
        parts.append(f"和按计划在信号后开盘卖出相比，等待{'多亏' if total > 0 else '少亏'}约 ${abs(total):,.0f}")
    return "你的记录：" + "，".join(parts)


def early_line(rec: dict) -> str | None:
    e = rec["early"]
    if not e:
        return None
    return ("你的记录：没有卖出信号时提前卖出 " + str(len(e)) + " 次，卖出后到现在："
            + "、".join(f"{x['ticker']}（{x['sold_on']:%m-%d}）{x['since'] * 100:+.1f}%" for x in e))


def over_cap_line(rec: dict) -> str | None:
    o = rec["over_cap"]
    if not o:
        return None
    return ("你的记录：买入后卫星仓超过上限 " + str(len(o)) + " 次："
            + "、".join(f"{x['ticker']}（{x['date']:%m-%d}，买入后占账户 {x['pct'] * 100:.0f}%"
                       + (f"，至今 {x['since'] * 100:+.1f}%" if x["since"] is not None else "") + "）" for x in o))


async def lines_for(kind: str, today: date) -> list[str]:
    """'sell_due' (a stop to act on), 'buy' (a buy being checked), 'early_sell' (a sell without a signal), 'week'."""
    rec = await record(today)
    pick = {"sell_due": (stop_line,), "buy": (stop_line, over_cap_line), "early_sell": (early_line,),
            "week": (stop_line, early_line, over_cap_line)}[kind]
    return [x for f in pick if (x := f(rec))]
