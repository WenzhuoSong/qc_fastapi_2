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


_NEWS_COLUMNS = "headline, source, url, llm_summary, sentiment, datetime_utc, credibility"


async def load_news(ticker: str, hours: int = NEWS_HOURS, limit: int = MAX_HEADLINES) -> list[dict]:
    """Recent news from the shared library; spans beyond 48h also read the
    archive (the library is pruned to 48h, the archive keeps history)."""
    since = int(time.time()) - hours * 3600
    source = "public.ticker_news_library"
    if hours > 48:
        source = (
            f"(SELECT {_NEWS_COLUMNS} , ticker FROM public.ticker_news_library "
            f"UNION SELECT {_NEWS_COLUMNS} , ticker FROM public.ticker_news_archive) n"
        )
    try:
        rows = await db.fetch_all(
            f"""
            SELECT {_NEWS_COLUMNS}
            FROM {source}
            WHERE ticker = :t AND datetime_utc >= :since
            ORDER BY {"datetime_utc DESC" if hours > 72 else "credibility DESC NULLS LAST, datetime_utc DESC"}
            LIMIT :n
            """,
            {"t": ticker, "since": since, "n": limit},
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


EARNINGS_WINDOW_DAYS = 2          # ~200–600 rows per day at the peak of earnings season
FINNHUB_EARNINGS_CAP = 1500


async def refresh_earnings(days_back: int = 10, days_ahead: int = 21) -> int:
    if not get_scout_settings().finnhub_api_key:
        return 0
    from services.finnhub_client import fetch_earnings_range

    # Finnhub caps one response at 1500 rows and drops the earliest dates, so
    # a month in one call silently loses the nearest weeks of earnings season.
    today = date.today()
    items: list[dict] = []
    complete: list[tuple[date, date]] = []    # windows fetched in full: stale dates in them can go
    start, end = today - timedelta(days=days_back), today + timedelta(days=days_ahead)
    while start <= end:
        stop = min(start + timedelta(days=EARNINGS_WINDOW_DAYS - 1), end)
        chunk = await asyncio.to_thread(fetch_earnings_range, start.isoformat(), stop.isoformat())
        if len(chunk) >= FINNHUB_EARNINGS_CAP:
            logger.warning("earnings %s..%s hit the %d-row cap; some dates may be missing", start, stop, FINNHUB_EARNINGS_CAP)
        elif chunk:                           # empty may be an API failure: keep what we have
            complete.append((start, stop))
        items += chunk
        start = stop + timedelta(days=1)
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
    rows = list({(r["t"], r["d"]): r for r in rows}.values())    # one row per (ticker, date) for ON CONFLICT
    if rows:
        await db.execute_many(
            """
            INSERT INTO scout.market_events (ticker, event_type, event_date, source, payload, updated_at)
            VALUES (:t, 'earnings', :d, 'finnhub', CAST(:p AS JSONB), now())
            ON CONFLICT (ticker, event_type, event_date) DO UPDATE SET payload = EXCLUDED.payload, updated_at = now()
            """,
            rows,
        )
        await _drop_moved_earnings(rows, complete)
    return len(rows)


async def _drop_moved_earnings(rows: list[dict], windows: list[tuple[date, date]]) -> int:
    """A rescheduled report leaves its old date behind; remove dates Finnhub no
    longer lists inside windows it returned in full."""
    fresh = {(r["t"], r["d"]) for r in rows}
    stale = []
    for a, b in windows:
        existing = await db.fetch_all(
            "SELECT id, ticker, event_date FROM scout.market_events "
            "WHERE event_type = 'earnings' AND source = 'finnhub' AND event_date BETWEEN :a AND :b", {"a": a, "b": b})
        stale += [{"id": r["id"]} for r in existing if (r["ticker"], r["event_date"]) not in fresh]
    if stale:
        await db.execute_many("DELETE FROM scout.market_events WHERE id = :id", stale)
        logger.info("removed %d moved earnings dates", len(stale))
    return len(stale)


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


# ── Economic calendar ──
# Finnhub's economic calendar is not in the free tier (403). Forex Factory's
# weekly JSON is free and keyless: title, country, ET timestamp, impact,
# forecast, previous (no actuals).
ECON_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
ECON_ZH = {
    "CPI m/m": "CPI 月率", "Core CPI m/m": "核心 CPI 月率", "CPI y/y": "CPI 年率", "PPI m/m": "PPI 月率",
    "Core PPI m/m": "核心 PPI 月率", "Non-Farm Employment Change": "非农就业人数", "Unemployment Rate": "失业率",
    "Average Hourly Earnings m/m": "平均时薪月率", "Unemployment Claims": "初请失业金人数",
    "Federal Funds Rate": "联邦基金利率决议", "FOMC Statement": "FOMC 声明", "FOMC Press Conference": "FOMC 新闻发布会",
    "FOMC Meeting Minutes": "FOMC 会议纪要", "Advance GDP q/q": "GDP 初值（季环比）", "Prelim GDP q/q": "GDP 修正值",
    "Final GDP q/q": "GDP 终值", "Core PCE Price Index m/m": "核心 PCE 物价指数月率", "Retail Sales m/m": "零售销售月率",
    "Core Retail Sales m/m": "核心零售销售月率", "ISM Manufacturing PMI": "ISM 制造业 PMI", "ISM Services PMI": "ISM 服务业 PMI",
    "CB Consumer Confidence": "谘商会消费者信心", "Prelim UoM Consumer Sentiment": "密歇根消费者信心初值",
    "JOLTS Job Openings": "JOLTS 职位空缺", "ADP Non-Farm Employment Change": "ADP 就业人数",
    "Fed Chair Powell Speaks": "美联储主席讲话", "Empire State Manufacturing Index": "纽约联储制造业指数",
    "Philly Fed Manufacturing Index": "费城联储制造业指数", "Durable Goods Orders m/m": "耐用品订单月率",
}


async def economic_calendar(country: str = "USD") -> list[dict]:
    """This week's events for one country, Medium/High impact; [] on failure."""
    import httpx

    try:
        async with httpx.AsyncClient(timeout=20, headers={"User-Agent": "Mozilla/5.0"}) as client:
            resp = await client.get(ECON_URL)
            resp.raise_for_status()
            items = resp.json()
    except Exception as exc:
        logger.warning("economic calendar unavailable: %s", exc)
        return []
    return parse_economic_calendar(items, country)


def parse_economic_calendar(items: list[dict], country: str = "USD") -> list[dict]:
    from zoneinfo import ZoneInfo

    et, bj = ZoneInfo("America/New_York"), ZoneInfo("Asia/Shanghai")
    out = []
    for it in items:
        if it.get("country") != country or it.get("impact") not in ("High", "Medium"):
            continue
        try:
            ts = datetime.fromisoformat(it["date"])
        except (KeyError, ValueError):
            continue
        title = it.get("title") or ""
        out.append({
            "date": ts.astimezone(et).date().isoformat(), "time_et": ts.astimezone(et).strftime("%H:%M"),
            "time_bj": ts.astimezone(bj).strftime("%m-%d %H:%M"), "title": title, "title_zh": ECON_ZH.get(title, title),
            "impact": "高" if it["impact"] == "High" else "中",
            "forecast": it.get("forecast") or None, "previous": it.get("previous") or None,
        })
    return sorted(out, key=lambda e: (e["date"], e["time_et"]))


# ── Event-driven moves ──
# A technical rule's replay says nothing about a stock that moves on a deal,
# a drug decision or a share sale; flag them from the headlines.
EVENT_PATTERNS: dict[str, tuple[str, ...]] = {
    "并购/出售": ("acquire", "acquisition", "merger", "buyout", "takeover", "to be bought", "selling itself",
                "explore a sale", "exploring a sale", "explores sale", "strategic alternatives", "deal to buy",
                "agreed to buy", "go-private", "take-private", "tender offer", "收购", "并购"),
    "药监/临床": ("fda approv", "fda decision", "pdufa", "complete response letter", "phase 3 data", "fda panel"),
    "增发/融资": ("public offering", "secondary offering", "share offering", "stock offering", "convertible notes",
                "at-the-market offering", "增发"),
    "监管/诉讼": ("sec investigation", "doj ", "antitrust", "subpoena", "class action", "indicted", "probe into"),
}


def event_flags(news: list[dict]) -> list[dict]:
    """One flag per event type, with the headline that triggered it."""
    out = []
    for kind, words in EVENT_PATTERNS.items():
        for n in news:
            text = f"{n.get('headline') or ''} {n.get('summary') or ''}".lower()
            if any(w in text for w in words):
                out.append({"type": kind, "headline": (n.get("headline") or "")[:160], "published": n.get("published")})
                break
    return out
