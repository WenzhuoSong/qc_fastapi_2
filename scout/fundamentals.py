"""Fundamentals and expectations from Finnhub's free tier, cached a day.

Valuation, growth, profitability and balance sheet (company metrics), the
analyst rating mix and how it moved over three months, the last four
quarters of EPS against the consensus, and open-market insider trades over
90 days. Values are stored in Scout units (fractions, not percents) under
the keys defined in scout/knowledge/features.yaml.
"""
from __future__ import annotations

import asyncio
from datetime import date, timedelta

from scout import db
from scout.data import finnhub

INSIDER_DAYS = 90
MIN_EPS_FOR_SURPRISE = 0.05     # surprise % on a near-zero estimate is meaningless

# Finnhub metric → (Scout key, scale): percents become fractions.
METRICS = {
    "marketCapitalization": ("market_cap", 1e6),
    "peTTM": ("pe_ttm", 1), "psTTM": ("ps_ttm", 1), "pbQuarterly": ("pb_ratio", 1),
    "revenueGrowthTTMYoy": ("rev_growth_ttm", 0.01), "revenueGrowthQuarterlyYoy": ("rev_growth_q", 0.01),
    "epsGrowthTTMYoy": ("eps_growth_ttm", 0.01),
    "grossMarginTTM": ("gross_margin", 0.01), "operatingMarginTTM": ("op_margin", 0.01),
    "netProfitMarginTTM": ("net_margin", 0.01), "roeTTM": ("roe_ttm", 0.01),
    "currentRatioQuarterly": ("current_ratio", 1), "totalDebt/totalEquityQuarterly": ("debt_equity", 1),
}


def parse_metrics(m: dict) -> dict:
    out = {}
    for src, (key, scale) in METRICS.items():
        v = m.get(src)
        if isinstance(v, (int, float)):
            out[key] = v * scale
    return out


def parse_recommendations(items: list[dict]) -> dict:
    """Share of buy ratings now and the change against three months earlier."""
    def share(r: dict) -> tuple[float | None, int]:
        n = sum(r.get(k, 0) or 0 for k in ("strongBuy", "buy", "hold", "sell", "strongSell"))
        return ((r.get("strongBuy", 0) + r.get("buy", 0)) / n if n else None), n

    if not items:
        return {}
    items = sorted(items, key=lambda r: r.get("period", ""), reverse=True)
    now, n = share(items[0])
    out = {"analyst_count": n} if n else {}
    if now is not None:
        out["analyst_buy_share"] = now
        if len(items) >= 4:
            before, _ = share(items[3])
            if before is not None:
                out["analyst_buy_change"] = now - before
    return out


def parse_earnings(items: list[dict]) -> dict:
    rows = [r for r in sorted(items, key=lambda r: r.get("period", ""), reverse=True)
            if r.get("actual") is not None and r.get("estimate") is not None][:4]
    if not rows:
        return {}
    out = {"eps_beat_count": sum(r["actual"] > r["estimate"] for r in rows), "eps_quarters": len(rows)}
    last = rows[0]
    if abs(last["estimate"]) >= MIN_EPS_FOR_SURPRISE:
        out["eps_surprise_last"] = (last["actual"] - last["estimate"]) / abs(last["estimate"])
    out["eps_last_period"] = last.get("period")
    return out


def parse_insiders(items: list[dict], since: date) -> dict:
    """Open-market buys (P) and sells (S) only; grants and option exercises are not signals."""
    buys = sells = 0.0
    buyers, sellers = set(), set()
    for r in items:
        if (r.get("transactionDate") or "") < since.isoformat() or r.get("transactionCode") not in ("P", "S"):
            continue
        value = abs((r.get("change") or 0) * (r.get("transactionPrice") or 0))
        if r["transactionCode"] == "P":
            buys += value
            buyers.add(r.get("name"))
        else:
            sells += value
            sellers.add(r.get("name"))
    if not (buyers or sellers):
        return {"insider_net_value_90d": 0.0, "insider_buyers_90d": 0, "insider_sellers_90d": 0}
    return {"insider_net_value_90d": buys - sells, "insider_buyers_90d": len(buyers), "insider_sellers_90d": len(sellers)}


async def fetch(ticker: str, today: date | None = None) -> dict:
    today = today or date.today()
    m, rec, earn, ins = await asyncio.gather(
        finnhub.metrics(ticker), finnhub.recommendations(ticker), finnhub.earnings_history(ticker),
        finnhub.insider_transactions(ticker, today - timedelta(days=INSIDER_DAYS)))
    since = today - timedelta(days=INSIDER_DAYS)
    return {**parse_metrics(m), **parse_recommendations(rec), **parse_earnings(earn), **parse_insiders(ins, since)}


async def get(ticker: str, max_age_days: int = 1) -> dict:
    """Cached fundamentals; refreshed when older than `max_age_days`."""
    row = await db.fetch_one("SELECT asof, payload FROM scout.fundamentals WHERE ticker = :t", {"t": ticker})
    if row and (date.today() - row["asof"]).days < max_age_days:
        return row["payload"]
    data = await fetch(ticker)
    if data:
        await db.execute(
            "INSERT INTO scout.fundamentals (ticker, asof, payload, updated_at) VALUES (:t, :d, CAST(:p AS JSONB), now()) "
            "ON CONFLICT (ticker) DO UPDATE SET asof = EXCLUDED.asof, payload = EXCLUDED.payload, updated_at = now()",
            {"t": ticker, "d": date.today(), "p": db.jsonb(data)})
    return data or (row or {}).get("payload") or {}
