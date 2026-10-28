"""The user's side: account settings, position sizing, trade journal, concentration.

Scout never trades. The journal is what the user says they did (ticker, date,
fill, shares), so the scoreboard of the rules and the user's own results can be
compared. Stops and targets are the system's: set from the price map at the buy
and advanced after each close (`manage_trade`). Sizing is fixed-fractional: risk
a set share of the account between entry and stop.
"""
from __future__ import annotations

import logging
import math
from collections import Counter
from datetime import date

from scout import db

logger = logging.getLogger("scout.portfolio")

DEFAULTS = {"account_size": None, "risk_pct": 0.01, "max_position_pct": 0.20}
SECTOR_CROWDED = 3       # this many open ideas/trades in one sector is one bet, not several
INDUSTRY_CROWDED = 2


# ── Settings ──

async def get_settings() -> dict:
    rows = await db.fetch_all("SELECT key, value FROM scout.user_settings WHERE key = ANY(:k)", {"k": list(DEFAULTS)})
    return {**DEFAULTS, **{r["key"]: r["value"] for r in rows}}


async def save_settings(values: dict) -> None:
    rows = [{"k": k, "v": db.jsonb(v)} for k, v in values.items() if k in DEFAULTS]
    await db.execute_many(
        "INSERT INTO scout.user_settings (key, value, updated_at) VALUES (:k, CAST(:v AS JSONB), now()) "
        "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()", rows)


# ── Sizing ──

def size(entry: float, stop: float, settings: dict) -> dict | None:
    """Shares so that a stop-out loses risk_pct of the account, capped by max_position_pct."""
    risk_per_share = entry - stop
    account = settings.get("account_size")
    if risk_per_share <= 0 or entry <= 0:
        return None
    if not account:
        return {"configured": False, "shares_per_1k": math.floor(1000 / risk_per_share), "risk_per_share": risk_per_share}
    budget = account * settings["risk_pct"]
    shares = math.floor(budget / risk_per_share)
    cap = math.floor(account * settings["max_position_pct"] / entry)
    capped = shares > cap
    shares = min(shares, cap)
    return {"configured": True, "shares": shares, "notional": shares * entry, "pct_of_account": shares * entry / account,
            "risk_usd": shares * risk_per_share, "risk_pct": settings["risk_pct"], "capped": capped,
            "risk_per_share": risk_per_share}


def size_text(sz: dict | None) -> str | None:
    if not sz:
        return None
    if not sz["configured"]:
        return f"每承担 $1,000 风险约 {sz['shares_per_1k']} 股（每股风险 {sz['risk_per_share']:.2f}；在「我的交易」设置账户后按比例计算）"
    text = (f"按单笔风险 {sz['risk_pct'] * 100:.1f}%：约 {sz['shares']} 股，${sz['notional']:,.0f}"
            f"（占账户 {sz['pct_of_account'] * 100:.0f}%），止损亏损约 ${sz['risk_usd']:,.0f}")
    return text + "；已按单只仓位上限截断" if sz["capped"] else text


# ── Journal ──

async def add_trade(t: dict) -> int:
    row = await db.execute_returning(
        "INSERT INTO scout.trades (ticker, opportunity_id, opened_on, entry_price, shares, stop, target, note, plan) "
        "VALUES (:ticker, :opp, :opened, :entry, :shares, :stop, :target, :note, CAST(:plan AS JSONB)) RETURNING id",
        {"ticker": t["ticker"].upper().strip(), "opp": t.get("opportunity_id") or None, "opened": t["opened_on"],
         "entry": t["entry_price"], "shares": t["shares"], "stop": t.get("stop"), "target": t.get("target"),
         "note": t.get("note") or None, "plan": db.jsonb(t["plan"]) if t.get("plan") else None})
    return row["id"]


async def manage_trade(t: dict) -> list[str]:
    """Set an open trade's stop and target from its plan (created from the price map if missing),
    as the system manages them; returns what changed today."""
    from scout import levels

    plan = t.get("plan")
    if not plan or "target1" not in plan:     # no plan yet (a sell note alone does not make one)
        pm = await levels.get_map(t["ticker"], before=t["opened_on"]) or {}
        new = levels.locked_plan(pm, t["entry_price"], t["opened_on"])
        if not new:
            return []
        plan = {**new, "history": (plan or {}).get("history", [])}
    bars = await db.fetch_all("SELECT date, high, close FROM scout.bars_daily WHERE ticker = :t AND date >= :d ORDER BY date",
                              {"t": t["ticker"], "d": t["opened_on"]})
    feat = await db.fetch_one("SELECT payload FROM scout.features_daily WHERE ticker = :t ORDER BY date DESC LIMIT 1",
                              {"t": t["ticker"]})
    asof = bars[-1]["date"] if bars else t["opened_on"]
    # the buy day's high may have come before the fill, its close did not
    reached = [b["close"] if b["date"] == t["opened_on"] else b["high"] for b in bars]
    stop, target, _, msgs = levels.manage(plan, reached, [b["close"] for b in bars],
                                          ((feat or {}).get("payload") or {}).get("atr14"), asof)
    remaining = t["shares"] - sum(r["shares"] for r in await sells(t["id"]))
    for st in levels.exit_plan(plan, t["entry_price"], t["shares"], remaining, stop):
        if st["state"] == "due" and st["sell_now"] and plan.get("hold_limit_hit") == asof.isoformat():
            msgs.append(f"建议卖出余下 {st['sell_now']:g} 股，卖出后记录（/sell 或「我的交易」）")
    await db.execute("UPDATE scout.trades SET stop = :s, target = :g, plan = CAST(:p AS JSONB), updated_at = now() WHERE id = :id",
                     {"s": stop, "g": target, "p": db.jsonb(plan), "id": t["id"]})
    return msgs


async def manage_open_trades() -> list[dict]:
    """After the close: every open trade's system stop / target; [{ticker, messages}] for the push."""
    out = []
    for t in await db.fetch_all("SELECT * FROM scout.trades WHERE closed_on IS NULL ORDER BY id"):
        try:
            msgs = await manage_trade(t)
        except Exception:
            logger.exception("managing trade %s failed", t["id"])
            continue
        if msgs:
            out.append({"ticker": t["ticker"], "messages": msgs})
    return out


async def sells(trade_id: int) -> list[dict]:
    return await db.fetch_all("SELECT * FROM scout.trade_sells WHERE trade_id = :id ORDER BY sold_on, id", {"id": trade_id})


async def _note(trade_id: int, text: str, on: date) -> None:
    await db.execute("UPDATE scout.trades SET plan = jsonb_set(coalesce(plan, '{}'::jsonb), '{history}', "
                     "coalesce(plan->'history', '[]'::jsonb) || CAST(:h AS JSONB)) WHERE id = :id",
                     {"h": db.jsonb([{"date": on.isoformat(), "text": text}]), "id": trade_id})


async def _settle(trade_id: int) -> dict:
    """Close the trade when nothing remains (exit = average sell), reopen it otherwise; then manage it."""
    t = await db.fetch_one("SELECT * FROM scout.trades WHERE id = :id", {"id": trade_id})
    rows = await sells(trade_id)
    n = sum(r["shares"] for r in rows)
    if rows and n >= t["shares"] - 1e-9:
        avg = sum(r["shares"] * r["price"] for r in rows) / n
        await db.execute("UPDATE scout.trades SET exit_price = :p, closed_on = :d, updated_at = now() WHERE id = :id",
                         {"p": avg, "d": max(r["sold_on"] for r in rows), "id": trade_id})
    else:
        await db.execute("UPDATE scout.trades SET exit_price = NULL, closed_on = NULL, updated_at = now() WHERE id = :id",
                         {"id": trade_id})
        await manage_trade(await db.fetch_one("SELECT * FROM scout.trades WHERE id = :id", {"id": trade_id}))
    return {"remaining": t["shares"] - n, "closed": bool(rows) and n >= t["shares"] - 1e-9}


async def add_sell(trade_id: int, sold_on: date, price: float, shares: float | None, note: str = "") -> dict:
    """Record a sell (all remaining shares when `shares` is None). Raises ValueError on bad input."""
    t = await db.fetch_one("SELECT * FROM scout.trades WHERE id = :id", {"id": trade_id})
    if not t or t["closed_on"]:
        raise ValueError("这笔持仓不存在或已平仓")
    remaining = t["shares"] - sum(r["shares"] for r in await sells(trade_id))
    shares = remaining if shares is None else shares
    if price <= 0 or shares <= 0 or shares > remaining + 1e-9:
        raise ValueError(f"卖出股数须在 0 到剩余 {remaining:g} 股之间，价格须为正数")
    if sold_on < t["opened_on"]:
        raise ValueError("卖出日期早于买入日期")
    await db.execute("INSERT INTO scout.trade_sells (trade_id, sold_on, price, shares, note) VALUES (:t, :d, :p, :n, :note)",
                     {"t": trade_id, "d": sold_on, "p": price, "n": shares, "note": note or None})
    left = remaining - shares
    await _note(trade_id, f"卖出 {shares:g} 股 @ {price:.2f}（{price / t['entry_price'] - 1:+.1%}），剩余 {left:g} 股", sold_on)
    return {**await _settle(trade_id), "ticker": t["ticker"], "shares": shares, "price": price}


async def delete_sell(sell_id: int) -> None:
    row = await db.execute_returning("DELETE FROM scout.trade_sells WHERE id = :id RETURNING trade_id, sold_on, shares, price", {"id": sell_id})
    if row:
        await _note(row["trade_id"], f"撤销卖出记录 {row['shares']:g} 股 @ {row['price']:.2f}", row["sold_on"])
        await _settle(row["trade_id"])


async def delete_trade(trade_id: int) -> None:
    await db.execute("DELETE FROM scout.trades WHERE id = :id", {"id": trade_id})


async def trades(open_only: bool | None = None) -> list[dict]:
    where = "" if open_only is None else ("WHERE t.closed_on IS NULL" if open_only else "WHERE t.closed_on IS NOT NULL")
    return await db.fetch_all(
        f"""SELECT t.*, i.name, i.sector, i.industry, b.close AS last_close, b.date AS last_date,
                   coalesce(sl.sold, 0) AS sold_shares, coalesce(sl.value, 0) AS sold_value
            FROM scout.trades t
            LEFT JOIN scout.instruments i USING (ticker)
            LEFT JOIN LATERAL (SELECT close, date FROM scout.bars_daily WHERE ticker = t.ticker ORDER BY date DESC LIMIT 1) b ON true
            LEFT JOIN LATERAL (SELECT sum(shares) AS sold, sum(shares * price) AS value FROM scout.trade_sells
                               WHERE trade_id = t.id) sl ON true
            {where} ORDER BY coalesce(t.closed_on, t.opened_on) DESC, t.id DESC""")


def sold(t: dict) -> tuple[float, float]:
    """(shares sold, proceeds); a trade closed before partial sells existed counts as one sell at its exit."""
    n, v = t.get("sold_shares") or 0.0, t.get("sold_value") or 0.0
    if t.get("closed_on") and not n and t.get("exit_price"):
        return t["shares"], t["shares"] * t["exit_price"]
    return n, v


def metrics(t: dict) -> dict:
    """P&L of one trade: realized on the shares sold plus, while open, the rest at the last close."""
    n_sold, proceeds = sold(t)
    remaining = max(0.0, t["shares"] - n_sold)
    price = t.get("exit_price") if t.get("closed_on") else t.get("last_close")
    out: dict = {"price": price, "remaining": remaining, "sold": n_sold}
    realized = proceeds - n_sold * t["entry_price"]
    if price is None and remaining:
        return out
    pnl = realized + ((price - t["entry_price"]) * remaining if remaining else 0.0)
    out.update(pnl=pnl, realized=realized, pnl_pct=pnl / (t["entry_price"] * t["shares"]))
    # R is measured against the initial stop; the stop the system has raised since only sets the open risk
    first = (t.get("plan") or {}).get("stop") or t.get("stop")
    if first and t["entry_price"] > first:
        out["r"] = pnl / ((t["entry_price"] - first) * t["shares"])
    if t.get("stop") and price and not t.get("closed_on"):
        out["open_risk"] = max(0.0, (price - t["stop"]) * remaining)
        out["to_stop"] = t["stop"] / price - 1
    return out


def journal_summary(closed: list[dict]) -> dict:
    ms = [metrics(t) for t in closed]
    pnl = [m["pnl"] for m in ms if "pnl" in m]
    rs = [m["r"] for m in ms if "r" in m]
    if not pnl:
        return {"n": 0}
    return {"n": len(pnl), "win_rate": sum(p > 0 for p in pnl) / len(pnl), "total_pnl": sum(pnl),
            "avg_r": sum(rs) / len(rs) if rs else None}


# ── Concentration ──

def concentration(items: list[dict]) -> list[str]:
    """items: {ticker, sector, industry, source}; warnings when one sector or industry dominates."""
    out = []
    for field, limit, label in (("sector", SECTOR_CROWDED, "板块"), ("industry", INDUSTRY_CROWDED, "细分行业")):
        counts = Counter(i.get(field) for i in items if i.get(field))
        for name, n in counts.most_common():
            if n >= limit:
                names = "、".join(sorted({i["ticker"] for i in items if i.get(field) == name}))
                out.append(f"{label}「{name}」同时有 {n} 个（{names}），走势高度相关，实际上接近一笔仓位")
    return out
