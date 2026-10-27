"""
cron/pre_fetch_news.py -- multi-source news pre-fetch shared with Scout.

Runs every 30 minutes (Railway cron `*/30 * * * *`); `plan_run` decides what a
slot does, by US Eastern time:
    full   weekdays 05:30 and 16:30 ET: the whole Scout scan universe (~900 stocks)
    core   weekdays 04:00-20:00 ET: ETFs, holdings, open ideas, watchlist, sector leaders (~100)
    core   other times: every second hour only
    skip   the remaining slots

Phases per run:
    A  Finnhub macro news -> MacroNewsCache; Finnhub per-ticker news -> TickerNewsLibrary
       (one call per ticker; earnings-soon flags read the scout.market_events table)
    C  RSS feeds -> keyword-matched to ETFs -> TickerNewsLibrary
    D  LLM summary per new article (gpt-4o-mini), inside A
    F  archive and drop library rows older than 48h

Alpha Vantage NEWS_SENTIMENT became a premium endpoint and was removed
(2026-10-27). The news pipeline does not import Scout; it reads Scout tables
with plain SQL.

Usage:
    python -m cron.pre_fetch_news [--tier full|core]
"""
from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Any

from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert

from constants import resolve_universe
from db.models import MacroNewsCache, TickerNewsArchive, TickerNewsLibrary
from db.session import AsyncSessionLocal
from services.finnhub_client import (
    fetch_macro_news,
    fetch_ticker_news,
    get_source_credibility,
    scan_hard_risks,
)
from services.news_summarizer import sanitize, summarize_headlines_batch
from services.rss_fetcher import fetch_rss_news
from tools.notify_tools import tool_send_telegram

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("qc_fastapi_2.cron.pre_fetch_news")

TIMEOUT_S = {"core": 1500, "full": 3300}   # a full run must end before the slot after next
TTL_SECONDS         = 48 * 3600 # 48h
# One Finnhub call per ticker; >= 1.1 s per ticker stays under the free tier's 60 calls/min.
FINNHUB_TICKER_PACING_S = 1.1
ET = ZoneInfo("America/New_York")
FULL_SLOTS = {(5, 30), (16, 30)}            # ET: before the pre-market brief, after the close


def plan_run(now: datetime | None = None) -> str | None:
    """'full', 'core' or None (skip) for the 30-minute slot containing `now`."""
    et = (now or datetime.now(ET)).astimezone(ET)
    slot = (et.hour, 30 if et.minute >= 30 else 0)
    weekday = et.weekday() < 5
    if weekday and slot in FULL_SLOTS:
        return "full"
    if weekday and 4 <= et.hour < 20:
        return "core"
    return "core" if slot[1] == 0 and et.hour % 2 == 0 else None

# ═══════════════════════════════════════════════════════════════
# Macro section
# ═══════════════════════════════════════════════════════════════

async def _refresh_macro_cache(universe: list[str]) -> dict[str, Any]:
    """
    Pull macro news, upsert to MacroNewsCache (single row, id=1).
    The economic calendar is not in Finnhub's free tier (Scout reads a free
    weekly feed instead), and nothing reads the old LLM structuring any more.
    """
    macro_news   = await asyncio.to_thread(fetch_macro_news, 20)
    econ_events: list[dict] = []
    structured = None

    prose = _build_macro_prose(macro_news, econ_events)

    async with AsyncSessionLocal() as db:
        existing = (await db.execute(select(MacroNewsCache).where(MacroNewsCache.id == 1))).scalar_one_or_none()
        if existing:
            existing.as_of              = datetime.utcnow()
            existing.macro_news         = macro_news
            existing.economic_calendar  = econ_events
            existing.prose_summary      = prose
            existing.raw_payload        = macro_news
            existing.structured_payload  = structured
        else:
            db.add(MacroNewsCache(
                id                 = 1,
                as_of              = datetime.utcnow(),
                macro_news         = macro_news,
                economic_calendar  = econ_events,
                prose_summary      = prose,
                raw_payload        = macro_news,
                structured_payload = structured,
            ))
        await db.commit()

    logger.info(
        f"macro cache refreshed | news={len(macro_news)}"
    )
    return {"n_news": len(macro_news), "n_events": len(econ_events)}


def _build_macro_prose(macro_news: list[dict], econ_events: list[dict]) -> str:
    """Assemble macro news + calendar into prose ready for prompt insertion."""
    lines: list[str] = []

    if macro_news:
        lines.append("## Macro News")
        for it in macro_news[:8]:
            headline = sanitize(it.get("headline", ""))
            source   = it.get("source", "") or "unknown"
            if headline:
                lines.append(f"- [{source}] {headline}")
    else:
        lines.append("## Macro News\n(no data)")

    lines.append("")
    if econ_events:
        lines.append("## This Week's Economic Calendar")
        for e in econ_events[:8]:
            event  = sanitize(e.get("event", ""))
            impact = e.get("impact", "")
            time_str = e.get("time", "") or e.get("date", "")
            if event:
                lines.append(f"- [{impact}] {time_str} {event}")
    else:
        lines.append("## This Week's Economic Calendar\n(no high-impact events)")

    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════
# Ticker section
# ═══════════════════════════════════════════════════════════════

async def _process_ticker(ticker: str, earnings_soon: set[str]) -> int:
    """
    Process single ticker: fetch -> dedupe -> summarize -> hard_risks -> insert.
    Returns number of newly written entries.
    """
    news_items = await asyncio.to_thread(fetch_ticker_news, ticker, 2, 10)
    if not news_items:
        return 0

    # Clean headline + url
    cleaned: list[dict] = []
    for it in news_items:
        url = (it.get("url") or "").strip()
        if not url:
            continue
        it["headline"] = sanitize(it.get("headline", ""))
        cleaned.append(it)

    if not cleaned:
        return 0

    # Check existing url for dedup
    urls = [it["url"] for it in cleaned]
    async with AsyncSessionLocal() as db:
        stmt = (
            select(TickerNewsLibrary.url)
            .where(TickerNewsLibrary.ticker == ticker)
            .where(TickerNewsLibrary.url.in_(urls))
        )
        existing_urls = {row for (row,) in (await db.execute(stmt)).all()}

    new_items = [it for it in cleaned if it["url"] not in existing_urls]
    if not new_items:
        return 0

    # LLM batch summarize -- has fallback on failure, no exception thrown
    summaries = await summarize_headlines_batch(ticker, new_items)

    # earnings + hard risks (based on all news_items, not just newly added)
    hard_risks   = scan_hard_risks(ticker, news_items, ticker in earnings_soon)

    # Batch insert
    rows: list[TickerNewsLibrary] = []
    for it, summary_data in zip(new_items, summaries):
        source = it.get("source", "")
        rows.append(TickerNewsLibrary(
            ticker        = ticker,
            url           = it["url"],
            headline      = it.get("headline", ""),
            source        = source,
            source_api    = "finnhub",
            summary       = it.get("summary", ""),
            llm_summary   = summary_data.get("summary", ""),
            sentiment     = summary_data.get("sentiment", "neutral"),
            relevance     = summary_data.get("relevance", "direct"),
            is_hard_event = bool(summary_data.get("is_hard_event", False)),
            hard_risks    = hard_risks or None,
            category      = it.get("category", ""),
            related       = it.get("related", []),
            datetime_utc  = int(it.get("datetime", 0) or 0),
            credibility   = get_source_credibility(source),
        ))

    async with AsyncSessionLocal() as db:
        db.add_all(rows)
        try:
            await db.commit()
        except Exception as e:
            # UniqueConstraint concurrent conflict -- skip this batch
            await db.rollback()
            logger.warning(f"{ticker}: insert conflict, skipped ({e})")
            return 0

    return len(rows)


# ═══════════════════════════════════════════════════════════════
# Phase C: RSS feeds
# ═══════════════════════════════════════════════════════════════

# Simple ETF -> keyword mapping for associating RSS macro news to tickers
_TICKER_KEYWORDS: dict[str, list[str]] = {
    "XLK":  ["tech", "technology", "semiconductor", "chip", "ai ", "artificial intelligence", "software", "apple", "microsoft", "nvidia"],
    "XLF":  ["bank", "financial", "fed ", "federal reserve", "interest rate", "credit", "jpmorgan", "goldman"],
    "XLV":  ["health", "pharma", "biotech", "drug", "fda", "medical", "hospital"],
    "XLE":  ["oil", "energy", "crude", "opec", "natural gas", "petroleum", "drilling"],
    "XLI":  ["industrial", "manufacturing", "defense", "aerospace", "infrastructure", "construction"],
    "XLP":  ["consumer staple", "grocery", "food", "beverage", "household", "procter", "walmart", "costco"],
    "XLY":  ["consumer discretion", "retail", "amazon", "tesla", "auto", "luxury", "spending"],
    "XLU":  ["utility", "utilities", "electric", "power grid", "renewable"],
    "XLB":  ["materials", "mining", "steel", "chemical", "commodity", "copper", "lithium"],
    "XLRE": ["real estate", "reit", "housing", "mortgage", "property", "home sale"],
    "XLC":  ["communication", "media", "telecom", "streaming", "google", "meta ", "facebook", "netflix"],
    "SPY":  ["s&p 500", "s&p500", "sp500", "market rally", "stock market", "wall street", "equities"],
    "QQQ":  ["nasdaq", "tech stock", "growth stock", "magnificent seven", "mag 7"],
    "IWM":  ["small cap", "russell 2000", "small-cap"],
    "GLD":  ["gold", "precious metal", "safe haven", "bullion"],
    "TLT":  ["treasury", "bond", "yield", "10-year", "10 year", "long bond", "fixed income"],
    "HYG":  ["high yield", "junk bond", "credit spread", "corporate bond", "leveraged loan"],
}


def _match_tickers_from_headline(headline: str, summary: str, universe_set: set[str]) -> set[str]:
    """Match RSS articles to tickers in universe based on keyword matching."""
    text = (headline + " " + summary).lower()
    matched = set()
    for ticker, keywords in _TICKER_KEYWORDS.items():
        if ticker not in universe_set:
            continue
        for kw in keywords:
            if kw in text:
                matched.add(ticker)
                break
    return matched


async def _fetch_rss(universe: list[str]) -> int:
    """
    Phase C: Fetch news from RSS feeds, keyword-match tickers, write to TickerNewsLibrary.
    Returns number of newly written entries.
    """
    try:
        rss_items = await asyncio.to_thread(fetch_rss_news, 10)
    except Exception as e:
        logger.error(f"RSS fetch failed: {e}")
        return 0

    if not rss_items:
        return 0

    universe_set = set(universe)
    ticker_buckets: dict[str, list[dict]] = {}

    for item in rss_items:
        url = (item.get("url") or "").strip()
        if not url:
            continue
        item["headline"] = sanitize(item.get("headline", ""))
        if not item["headline"]:
            continue

        matched = _match_tickers_from_headline(
            item["headline"], item.get("summary", ""), universe_set
        )
        if not matched:
            continue

        for t in matched:
            ticker_buckets.setdefault(t, []).append(item)

    total_new = 0
    for ticker, items in ticker_buckets.items():
        n = await _insert_external_news(ticker, items, "rss")
        total_new += n

    logger.info(f"Phase C (RSS): {total_new} new articles across {len(ticker_buckets)} tickers")
    return total_new


# ═══════════════════════════════════════════════════════════════
# Shared: insert external news (AV / RSS)
# ═══════════════════════════════════════════════════════════════

async def _insert_external_news(
    ticker: str, items: list[dict], source_api: str
) -> int:
    """
    Write Alpha Vantage or RSS articles to TickerNewsLibrary.
    Dedupe by url, LLM summarize (only for entries without pre-set sentiment), batch insert.
    Returns number of newly written entries.
    """
    # url dedup
    urls = [it["url"] for it in items]
    async with AsyncSessionLocal() as db:
        stmt = (
            select(TickerNewsLibrary.url)
            .where(TickerNewsLibrary.ticker == ticker)
            .where(TickerNewsLibrary.url.in_(urls))
        )
        existing_urls = {row for (row,) in (await db.execute(stmt)).all()}

    new_items = [it for it in items if it["url"] not in existing_urls]
    if not new_items:
        return 0

    # Only LLM summarize entries without pre-set sentiment
    needs_llm = [it for it in new_items if not it.get("sentiment")]
    if needs_llm:
        summaries = await summarize_headlines_batch(ticker, needs_llm)
        summary_map = {it["url"]: s for it, s in zip(needs_llm, summaries)}
    else:
        summary_map = {}

    rows: list[TickerNewsLibrary] = []
    for it in new_items:
        source = it.get("source", "")
        llm_data = summary_map.get(it["url"], {})
        rows.append(TickerNewsLibrary(
            ticker        = ticker,
            url           = it["url"],
            headline      = it.get("headline", ""),
            source        = source,
            source_api    = source_api,
            summary       = it.get("summary", ""),
            llm_summary   = llm_data.get("summary", ""),
            sentiment     = it.get("sentiment") or llm_data.get("sentiment", "neutral"),
            relevance     = llm_data.get("relevance", "indirect"),
            is_hard_event = bool(llm_data.get("is_hard_event", False)),
            hard_risks    = None,
            category      = it.get("category", ""),
            related       = it.get("related", []),
            datetime_utc  = int(it.get("datetime", 0) or 0),
            credibility   = it.get("credibility") or get_source_credibility(source),
        ))

    async with AsyncSessionLocal() as db:
        db.add_all(rows)
        try:
            await db.commit()
        except Exception as e:
            await db.rollback()
            logger.warning(f"{ticker} ({source_api}): insert conflict, skipped ({e})")
            return 0

    return len(rows)


# ═══════════════════════════════════════════════════════════════
# Cleanup
# ═══════════════════════════════════════════════════════════════

async def _cleanup_old_news() -> int:
    cutoff = int(time.time()) - TTL_SECONDS
    async with AsyncSessionLocal() as db:
        expired = (
            await db.execute(
                select(TickerNewsLibrary).where(TickerNewsLibrary.datetime_utc < cutoff)
            )
        ).scalars().all()
        if expired:
            archive_rows = [
                {
                    "source_library_id": row.id,
                    "ticker": row.ticker,
                    "url": row.url,
                    "headline": row.headline,
                    "source": row.source,
                    "source_api": row.source_api,
                    "summary": row.summary,
                    "llm_summary": row.llm_summary,
                    "sentiment": row.sentiment,
                    "relevance": row.relevance,
                    "is_hard_event": row.is_hard_event,
                    "hard_risks": row.hard_risks,
                    "category": row.category,
                    "related": row.related,
                    "datetime_utc": row.datetime_utc,
                    "credibility": row.credibility,
                    "source_created_at": row.created_at,
                }
                for row in expired
            ]
            await db.execute(
                insert(TickerNewsArchive)
                .values(archive_rows)
                .on_conflict_do_nothing(
                    index_elements=[TickerNewsArchive.ticker, TickerNewsArchive.url]
                )
            )
        stmt = delete(TickerNewsLibrary).where(TickerNewsLibrary.datetime_utc < cutoff)
        res = await db.execute(stmt)
        await db.commit()
        return res.rowcount or 0


# ═══════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════

async def _earnings_soon(days: int = 7) -> set[str]:
    """Tickers reporting within `days`, read off Scout's earnings calendar table (no API call)."""
    from sqlalchemy import text

    try:
        async with AsyncSessionLocal() as db:
            rows = await db.execute(text(
                "SELECT DISTINCT ticker FROM scout.market_events WHERE event_type = 'earnings' "
                "AND event_date BETWEEN current_date AND current_date + :d"), {"d": days})
            return {r[0] for r in rows}
    except Exception:
        return set()


async def run_pre_fetch(tier: str = "core") -> dict:
    """Main entry. Returns statistics."""
    from db.session import init_db
    await init_db()

    universe = await resolve_universe(full=tier == "full")
    earnings_soon = await _earnings_soon()
    logger.info(f"=== pre_fetch_news START | tier={tier} | universe={len(universe)} tickers ===")

    # ── Phase A: Finnhub (macro + per-ticker) ──
    macro_stats = await _refresh_macro_cache(universe)

    finnhub_new = 0
    for ticker in universe:
        started = time.monotonic()
        try:
            n = await _process_ticker(ticker, earnings_soon)
            if n > 0:
                logger.info(f"  [finnhub] {ticker}: {n} new articles")
            finnhub_new += n
        except Exception as e:
            logger.error(f"  [finnhub] {ticker}: FAILED — {e}")
        # Summarizing new articles already takes time; only wait out the rest of the pace.
        await asyncio.sleep(max(0.0, FINNHUB_TICKER_PACING_S - (time.monotonic() - started)))

    # ── Phase C: RSS feeds (keyword-matched to tickers) ──
    rss_new = 0
    try:
        rss_new = await _fetch_rss(universe)
    except Exception as e:
        logger.error(f"Phase C (RSS) FAILED — {e}")

    # ── Phase F: Cleanup 48h ──
    deleted = await _cleanup_old_news()

    total_new = finnhub_new + rss_new
    logger.info(
        f"=== pre_fetch_news DONE | tier={tier} | macro_news={macro_stats['n_news']} "
        f"| finnhub={finnhub_new} | rss={rss_new} | total_new={total_new} | cleaned={deleted} ==="
    )

    return {
        "tier":             tier,
        "universe_size":    len(universe),
        "macro_news":       macro_stats["n_news"],
        "finnhub_articles": finnhub_new,
        "rss_articles":     rss_new,
        "total_new":        total_new,
        "cleaned":          deleted,
    }


async def main(tier: str | None = None) -> None:
    from services.cron_audit import audit_cron_run

    tier = tier or plan_run()
    if tier is None:
        logger.info("pre_fetch_news: nothing scheduled for this slot")
        return
    try:
        async with audit_cron_run("pre_fetch_news") as audit:
            result = await asyncio.wait_for(run_pre_fetch(tier), timeout=TIMEOUT_S[tier])
            audit.add_rows(result.get("total_new"))
            audit.set_summary(**result)
            logger.info(f"pre_fetch_news result: {result}")
    except asyncio.TimeoutError:
        logger.error(f"pre_fetch_news TIMEOUT after {TIMEOUT_S[tier]}s (tier={tier})")
        raise
    except Exception as e:
        logger.exception("pre_fetch_news FAILED")
        try:
            await tool_send_telegram(
                {"text": f"News pre-fetch error: {e}", "parse_mode": ""}
            )
        except Exception:
            pass
        raise


if __name__ == "__main__":
    import sys

    forced = sys.argv[sys.argv.index("--tier") + 1] if "--tier" in sys.argv else None
    asyncio.run(main(forced))
