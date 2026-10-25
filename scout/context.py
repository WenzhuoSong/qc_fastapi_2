"""Context enrichment for candidates: news and earnings events.

News comes from the shared news library written by `cron.pre_fetch_news`, with
an on-demand Finnhub fetch for candidates outside its ticker list.
"""
from __future__ import annotations

import asyncio
import logging
import time
from datetime import date, datetime, timedelta, timezone

from scout import db
from scout.config import get_scout_settings

logger = logging.getLogger("scout.context")

NEWS_HOURS = 72
MAX_HEADLINES = 6


async def load_news(ticker: str, hours: int = NEWS_HOURS) -> list[dict]:
    since = int(time.time()) - hours * 3600
    try:
        rows = await db.fetch_all(
            """
            SELECT headline, source, url, llm_summary, sentiment, datetime_utc, credibility
            FROM public.ticker_news_library
            WHERE ticker = :t AND datetime_utc >= :since
            ORDER BY credibility DESC NULLS LAST, datetime_utc DESC
            LIMIT :n
            """,
            {"t": ticker, "since": since, "n": MAX_HEADLINES},
        )
    except Exception as exc:  # table absent in a fresh database
        logger.debug("news library unavailable: %s", exc)
        rows = []
    if rows or not get_scout_settings().finnhub_api_key:
        return [_news_item(r) for r in rows]
    from services.finnhub_client import fetch_ticker_news, get_source_credibility

    items = await asyncio.to_thread(fetch_ticker_news, ticker, max(1, hours // 24), 20)
    items = [i for i in items if i.get("datetime", 0) >= since and i.get("headline")]
    items.sort(key=lambda i: (get_source_credibility(i.get("source", "")), i["datetime"]), reverse=True)
    return [
        {
            "headline": i["headline"][:240],
            "source": i.get("source"),
            "url": i.get("url"),
            "summary": (i.get("summary") or "")[:300] or None,
            "sentiment": None,
            "published": datetime.fromtimestamp(i["datetime"], timezone.utc).isoformat(),
        }
        for i in items[:MAX_HEADLINES]
    ]


def _news_item(r: dict) -> dict:
    ts = r.get("datetime_utc")
    return {
        "headline": (r.get("headline") or "")[:240],
        "source": r.get("source"),
        "url": r.get("url"),
        "summary": r.get("llm_summary"),
        "sentiment": r.get("sentiment"),
        "published": datetime.fromtimestamp(ts, timezone.utc).isoformat() if ts else None,
    }


async def refresh_earnings(days_back: int = 10, days_ahead: int = 21) -> int:
    if not get_scout_settings().finnhub_api_key:
        return 0
    from services.finnhub_client import fetch_earnings_range

    today = date.today()
    items = await asyncio.to_thread(
        fetch_earnings_range,
        (today - timedelta(days=days_back)).isoformat(),
        (today + timedelta(days=days_ahead)).isoformat(),
    )
    rows = []
    for it in items:
        symbol, day = it.get("symbol"), it.get("date")
        if not symbol or not day:
            continue
        rows.append({
            "t": symbol.upper().replace(".", "-"),
            "d": date.fromisoformat(day),
            "p": db.jsonb({
                "hour": it.get("hour"),
                "eps_estimate": it.get("epsEstimate"),
                "eps_actual": it.get("epsActual"),
                "revenue_estimate": it.get("revenueEstimate"),
                "revenue_actual": it.get("revenueActual"),
            }),
        })
    if rows:
        await db.execute_many(
            """
            INSERT INTO scout.market_events (ticker, event_type, event_date, source, payload, updated_at)
            VALUES (:t, 'earnings', :d, 'finnhub', CAST(:p AS JSONB), now())
            ON CONFLICT (ticker, event_type, event_date) DO UPDATE SET payload = EXCLUDED.payload, updated_at = now()
            """,
            rows,
        )
    return len(rows)


async def earnings_map(asof: date, days_back: int = 10, days_ahead: int = 21) -> dict[str, list[dict]]:
    rows = await db.fetch_all(
        """
        SELECT ticker, event_date, payload FROM scout.market_events
        WHERE event_type = 'earnings' AND event_date BETWEEN :a AND :b
        ORDER BY event_date
        """,
        {"a": asof - timedelta(days=days_back), "b": asof + timedelta(days=days_ahead)},
    )
    out: dict[str, list[dict]] = {}
    for r in rows:
        out.setdefault(r["ticker"], []).append({"date": r["event_date"], **(r["payload"] or {})})
    return out


def earnings_summary(events: list[dict], asof: date) -> dict:
    """Most recent past report and next upcoming report relative to `asof`."""
    past = [e for e in events if e["date"] <= asof]
    future = [e for e in events if e["date"] > asof]
    out: dict = {}
    if past:
        last = past[-1]
        out["last"] = {
            "date": last["date"].isoformat(),
            "days_ago": (asof - last["date"]).days,
            "eps_actual": last.get("eps_actual"),
            "eps_estimate": last.get("eps_estimate"),
            "eps_surprise_pct": _surprise(last.get("eps_actual"), last.get("eps_estimate")),
            "revenue_surprise_pct": _surprise(last.get("revenue_actual"), last.get("revenue_estimate")),
        }
    if future:
        nxt = future[0]
        out["next"] = {"date": nxt["date"].isoformat(), "days_until": (nxt["date"] - asof).days, "hour": nxt.get("hour")}
    return out


def _surprise(actual, estimate) -> float | None:
    try:
        if actual is None or estimate in (None, 0):
            return None
        return round((float(actual) - float(estimate)) / abs(float(estimate)), 4)
    except (TypeError, ValueError):
        return None
