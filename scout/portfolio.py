"""The user's side: account settings, position sizing, trade journal, concentration.

Scout never trades. The journal is what the user says they did, so the
scoreboard of the rules and the user's own results can be compared. Sizing
is fixed-fractional: risk a set share of the account between entry and stop.
"""
from __future__ import annotations

import math
from collections import Counter
from datetime import date

from scout import db

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
        "INSERT INTO scout.trades (ticker, opportunity_id, opened_on, entry_price, shares, stop, target, note) "
        "VALUES (:ticker, :opp, :opened, :entry, :shares, :stop, :target, :note) RETURNING id",
        {"ticker": t["ticker"].upper().strip(), "opp": t.get("opportunity_id") or None, "opened": t["opened_on"],
         "entry": t["entry_price"], "shares": t["shares"], "stop": t.get("stop"), "target": t.get("target"),
         "note": t.get("note") or None})
    return row["id"]


async def close_trade(trade_id: int, exit_price: float, closed_on: date) -> None:
    await db.execute("UPDATE scout.trades SET exit_price = :p, closed_on = :d, updated_at = now() WHERE id = :id",
                     {"p": exit_price, "d": closed_on, "id": trade_id})


async def update_stop(trade_id: int, stop: float) -> None:
    await db.execute("UPDATE scout.trades SET stop = :s, updated_at = now() WHERE id = :id", {"s": stop, "id": trade_id})


async def delete_trade(trade_id: int) -> None:
    await db.execute("DELETE FROM scout.trades WHERE id = :id", {"id": trade_id})


async def trades(open_only: bool | None = None) -> list[dict]:
    where = "" if open_only is None else ("WHERE t.closed_on IS NULL" if open_only else "WHERE t.closed_on IS NOT NULL")
    return await db.fetch_all(
        f"""SELECT t.*, i.name, i.sector, i.industry, b.close AS last_close, b.date AS last_date
            FROM scout.trades t
            LEFT JOIN scout.instruments i USING (ticker)
            LEFT JOIN LATERAL (SELECT close, date FROM scout.bars_daily WHERE ticker = t.ticker ORDER BY date DESC LIMIT 1) b ON true
            {where} ORDER BY coalesce(t.closed_on, t.opened_on) DESC, t.id DESC""")


def metrics(t: dict) -> dict:
    """P&L of one trade at its exit (closed) or the last close (open)."""
    price = t.get("exit_price") if t.get("closed_on") else t.get("last_close")
    out: dict = {"price": price}
    if price is None:
        return out
    pnl = (price - t["entry_price"]) * t["shares"]
    out.update(pnl=pnl, pnl_pct=price / t["entry_price"] - 1)
    risk = t["entry_price"] - t["stop"] if t.get("stop") else None
    if risk and risk > 0:
        out["r"] = (price - t["entry_price"]) / risk
        out["open_risk"] = max(0.0, (price - t["stop"]) * t["shares"]) if not t.get("closed_on") else None
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
