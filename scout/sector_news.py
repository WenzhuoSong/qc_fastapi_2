"""Sector news: attention (news volume vs normal), tone, and the headlines.

Sources: the shared news library (48h) and archive (history), written by
`cron.pre_fetch_news` for the sector ETFs, each sector's most liquid stocks,
the Scout watchlist and open ideas. Headlines are de-duplicated by URL.
"""
from __future__ import annotations

import time
from collections import defaultdict
from datetime import datetime, timezone

from scout import db, universe

RECENT_DAYS = 3          # tone and headlines
ATTENTION_DAYS = 7       # attention = last 7 days vs the 7 before (one weekend in each)
BASELINE_DAYS = 14
TOP_HEADLINES = 5
MARKET_WIDE_SECTORS = 3  # a URL tagged to this many sectors is a market wrap, not sector news

# Excluded: items the summarizer marked not relevant, and RSS items attached to
# sector ETFs (their keyword lists — "utilities", "consumer" — match personal-
# finance pieces); RSS matched to a company name is kept.
_NEWS_SQL = """
SELECT ticker, url, headline, source, source_api, relevance, llm_summary, sentiment, credibility, datetime_utc
FROM (
    SELECT ticker, url, headline, source, source_api, relevance, llm_summary, sentiment, credibility, datetime_utc
    FROM public.ticker_news_library WHERE datetime_utc >= :since
    UNION
    SELECT ticker, url, headline, source, source_api, relevance, llm_summary, sentiment, credibility, datetime_utc
    FROM public.ticker_news_archive WHERE datetime_utc >= :since
) n
WHERE coalesce(relevance, '') <> 'not_relevant'
  AND NOT (source_api = 'rss' AND ticker = ANY(:etfs))
"""


async def _sector_of() -> dict[str, str]:
    """ticker → sector ETF, for stocks (by GICS sector) and the sector ETFs themselves."""
    rows = await db.fetch_all("SELECT ticker, sector FROM scout.instruments WHERE sector IS NOT NULL")
    out = {r["ticker"]: universe.SECTOR_ETFS[r["sector"]] for r in rows if r["sector"] in universe.SECTOR_ETFS}
    out.update({etf: etf for etf in universe.SECTOR_ETFS.values()})
    return out


async def by_sector(now: float | None = None) -> dict[str, dict]:
    now = now or time.time()
    since = int(now) - BASELINE_DAYS * 86400
    recent_cut = int(now) - RECENT_DAYS * 86400
    try:
        rows = await db.fetch_all(_NEWS_SQL, {"since": since, "etfs": list(universe.SECTOR_ETFS.values())})
    except Exception:          # news tables absent (fresh database)
        return {}
    sector_of = await _sector_of()
    buckets: dict[str, dict[str, dict]] = defaultdict(dict)     # etf → url → item
    for r in rows:
        etf = sector_of.get(r["ticker"])
        if not etf or not r["url"]:
            continue
        item = buckets[etf].get(r["url"])
        if item is None:
            buckets[etf][r["url"]] = {**r, "tickers": {r["ticker"]}}
        else:
            item["tickers"].add(r["ticker"])
    url_sectors: dict[str, int] = defaultdict(int)
    for items in buckets.values():
        for url in items:
            url_sectors[url] += 1
    attention_cut = int(now) - ATTENTION_DAYS * 86400
    out = {}
    for etf, items in buckets.items():
        own = {u: i for u, i in items.items() if url_sectors[u] < MARKET_WIDE_SECTORS}
        recent = [i for i in own.values() if (i["datetime_utc"] or 0) >= recent_cut]
        week = [i for i in own.values() if (i["datetime_utc"] or 0) >= attention_cut]
        older = [i for i in own.values() if (i["datetime_utc"] or 0) < attention_cut]
        # Attention compares like with like: only tickers already covered in the
        # prior week count, so widening news coverage does not read as a spike.
        covered = {t for i in older for t in i["tickers"]}
        week_cov = [i for i in week if i["tickers"] & covered]
        base_per_day = len(older) / (BASELINE_DAYS - ATTENTION_DAYS)
        per_day = len(week_cov) / ATTENTION_DAYS
        tone = {"positive": 0, "negative": 0, "neutral": 0}
        for i in recent:
            tone[i["sentiment"] if i["sentiment"] in tone else "neutral"] += 1
        rank = {"direct": 2, "indirect": 1}
        top = sorted(recent, key=lambda i: (rank.get(i["relevance"], 0), i["credibility"] or 0, i["datetime_utc"] or 0),
                     reverse=True)
        out[etf] = {
            "news_3d": len(recent),
            "news_7d": len(week),
            "news_per_day_baseline": round(base_per_day, 2),
            # None = baseline too thin to compare (e.g. coverage just widened).
            "news_attention": round(per_day / base_per_day, 2) if len(older) >= 5 and week_cov else None,
            "tone": tone,
            "tone_net": round((tone["positive"] - tone["negative"]) / len(recent), 3) if recent else None,
            "tickers_with_news": len({t for i in recent for t in i["tickers"]}),
            "headlines": [
                {"headline": i["headline"][:200], "source": i["source"], "url": i["url"],
                 "tickers": sorted(i["tickers"])[:4], "sentiment": i["sentiment"],
                 "summary": (i["llm_summary"] or "")[:200] or None,
                 "published": datetime.fromtimestamp(i["datetime_utc"], timezone.utc).strftime("%Y-%m-%d %H:%M")}
                for i in top[:TOP_HEADLINES]
            ],
        }
    return out


async def market_wraps(now: float | None = None, limit: int = 6) -> list[dict]:
    """Articles tagged to several sectors at once (market wraps, rotation pieces)."""
    now = now or time.time()
    try:
        rows = await db.fetch_all(_NEWS_SQL, {"since": int(now) - RECENT_DAYS * 86400,
                                              "etfs": list(universe.SECTOR_ETFS.values())})
    except Exception:
        return []
    sector_of = await _sector_of()
    seen: dict[str, dict] = {}
    for r in rows:
        etf = sector_of.get(r["ticker"])
        if etf and r["url"]:
            item = seen.setdefault(r["url"], {**r, "sectors": set()})
            item["sectors"].add(etf)
    wraps = [i for i in seen.values() if len(i["sectors"]) >= MARKET_WIDE_SECTORS]
    wraps.sort(key=lambda i: (i["credibility"] or 0, i["datetime_utc"] or 0), reverse=True)
    return [{"headline": i["headline"][:200], "source": i["source"], "url": i["url"],
             "published": datetime.fromtimestamp(i["datetime_utc"], timezone.utc).strftime("%Y-%m-%d %H:%M")}
            for i in wraps[:limit]]


async def macro_headlines(limit: int = 10) -> list[dict]:
    """Market-wide headlines from the macro news cache (Reuters / CNBC / ...)."""
    try:
        row = await db.fetch_one("SELECT macro_news, updated_at FROM public.macro_news_cache ORDER BY updated_at DESC LIMIT 1")
    except Exception:
        return []
    items = (row or {}).get("macro_news") or []
    items = sorted(items, key=lambda i: i.get("datetime") or 0, reverse=True)[:limit]
    return [{"headline": (i.get("headline") or "")[:200], "source": i.get("source"), "url": i.get("url"),
             "published": datetime.fromtimestamp(i["datetime"], timezone.utc).strftime("%Y-%m-%d %H:%M")
             if i.get("datetime") else None} for i in items]
