"""Prediction-market odds, read-only, from the public market-data endpoints (no account, no key, nothing sent):
Polymarket's Gamma API and Kalshi's trade API v2. Every function returns normalised events
{source, id, title, url, end_date, volume, volume_24h, outcomes: [{id, label, prob, volume, volume_24h, day_change,
week_change}], rules} or [] / None on failure — a market feed must never fail a Scout job.

`prob` is the mid of the best bid and ask when both exist, else the last price (Polymarket's outcome price): the price
of a contract paying $1 if the outcome happens, which is a probability only roughly (fees, risk appetite, thin books).
"""
from __future__ import annotations

import json
import logging
from datetime import date, datetime

import httpx

logger = logging.getLogger("scout.data.prediction")

GAMMA = "https://gamma-api.polymarket.com"
KALSHI = "https://api.elections.kalshi.com/trade-api/v2"
TIMEOUT = 20


async def _get(url: str, params: dict | None = None) -> dict | list | None:
    try:
        async with httpx.AsyncClient(timeout=TIMEOUT, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"}) as client:
            resp = await client.get(url, params=params)
        if resp.status_code != 200:
            logger.warning("prediction feed %s: HTTP %s", url, resp.status_code)
            return None
        return resp.json()
    except Exception as exc:
        logger.warning("prediction feed %s failed: %s", url, exc)
        return None


def _f(x) -> float | None:
    try:
        return None if x in (None, "") else float(x)
    except (TypeError, ValueError):
        return None


def _day(s: str | None) -> date | None:
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).date() if s else None
    except ValueError:
        return None


def mid(bid: float | None, ask: float | None, last: float | None) -> float | None:
    """Mid of a real two-sided quote; otherwise the last price."""
    if bid is not None and ask is not None and ask >= bid and (bid > 0 or ask > 0):
        return (bid + ask) / 2
    return last


# ── Polymarket ──

def polymarket_event(e: dict) -> dict | None:
    if not e or e.get("closed"):
        return None
    outcomes = []
    for m in e.get("markets") or []:
        if m.get("closed") or not _f(m.get("volume")):        # placeholders ("Person V") have never traded
            continue
        try:
            last = _f(json.loads(m.get("outcomePrices") or "[]")[0])
        except (ValueError, IndexError, TypeError):
            last = None
        outcomes.append({"id": str(m.get("id")), "label": (m.get("groupItemTitle") or m.get("question") or "").strip()[:120],
                         "prob": mid(_f(m.get("bestBid")), _f(m.get("bestAsk")), last), "volume": _f(m.get("volume")),
                         "volume_24h": _f(m.get("volume24hr")), "day_change": _f(m.get("oneDayPriceChange")),
                         "week_change": _f(m.get("oneWeekPriceChange")), "question": m.get("question")})
    if not outcomes:
        return None
    desc = ((e.get("markets") or [{}])[0].get("description") or e.get("description") or "")[:800]
    return {"source": "polymarket", "id": str(e["id"]), "title": e.get("title") or "", "url": f"https://polymarket.com/event/{e.get('slug')}",
            "end_date": _day(e.get("endDate")), "volume": _f(e.get("volume")), "volume_24h": _f(e.get("volume24hr")),
            "outcomes": outcomes, "rules": desc}


async def polymarket_search(query: str, limit: int = 8) -> list[dict]:
    d = await _get(f"{GAMMA}/public-search", {"q": query, "limit_per_type": limit, "events_status": "active"})
    return [x for x in (polymarket_event(e) for e in ((d or {}).get("events") or [])) if x]


async def polymarket_get(event_id: str) -> dict | None:
    return polymarket_event(await _get(f"{GAMMA}/events/{event_id}"))


async def polymarket_top(limit: int = 50) -> list[dict]:
    d = await _get(f"{GAMMA}/events", {"active": "true", "closed": "false", "order": "volume24hr", "ascending": "false", "limit": limit})
    return [x for x in (polymarket_event(e) for e in (d or [])) if x]


# ── Kalshi ──

def kalshi_event(e: dict, markets: list[dict]) -> dict | None:
    outcomes = []
    for m in markets:
        if m.get("status") not in ("active", "open", None):
            continue
        outcomes.append({"id": m.get("ticker"), "label": (m.get("yes_sub_title") or m.get("subtitle") or m.get("title") or "")[:120],
                         "prob": mid(_f(m.get("yes_bid_dollars")), _f(m.get("yes_ask_dollars")), _f(m.get("last_price_dollars"))),
                         "volume": _f(m.get("volume_fp")), "volume_24h": _f(m.get("volume_24h_fp")), "day_change": None, "week_change": None,
                         "question": m.get("title")})
    if not outcomes:
        return None
    close = min((d for d in (_day(m.get("close_time")) for m in markets) if d), default=None)
    return {"source": "kalshi", "id": e["event_ticker"], "title": e.get("title") or "", "url": f"https://kalshi.com/markets/{e['event_ticker'].lower()}",
            "end_date": close, "volume": sum(o["volume"] or 0 for o in outcomes), "volume_24h": sum(o["volume_24h"] or 0 for o in outcomes),
            "outcomes": outcomes, "rules": (markets[0].get("rules_primary") or "")[:800] if markets else ""}


async def kalshi_get(event_ticker: str) -> dict | None:
    e = await _get(f"{KALSHI}/events/{event_ticker}")
    m = await _get(f"{KALSHI}/markets", {"event_ticker": event_ticker, "limit": 100})
    if not e or not m:
        return None
    return kalshi_event(e.get("event") or {"event_ticker": event_ticker}, m.get("markets") or [])


async def kalshi_series(series: str) -> list[str]:
    """Open event tickers of a series (e.g. KXFEDDECISION → KXFEDDECISION-26DEC, …)."""
    d = await _get(f"{KALSHI}/events", {"series_ticker": series, "status": "open", "limit": 20})
    return [e["event_ticker"] for e in (d or {}).get("events") or []]
