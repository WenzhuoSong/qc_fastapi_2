"""
cron/pre_fetch_news.py -- Cron 1: Independent multi-source news pre-fetch.

Responsibilities: Fully decoupled from the main pipeline, runs every 2h:
    Phase A: Finnhub -> fetch_macro_news + fetch_economic_calendar -> upsert MacroNewsCache
                       + per-ticker news -> TickerNewsLibrary
    Phase B: Alpha Vantage -> bulk ticker news -> TickerNewsLibrary (source_api=alphavantage)
    Phase C: RSS feeds -> fetch 5 feeds in parallel -> keyword-match ticker -> TickerNewsLibrary (source_api=rss)
    Phase D: LLM batch summarize (gpt-4o-mini, existing, executed within Phase A)
    Phase E: cross-source dedup (url dedup, already done per-Phase by url)
    Phase F: cleanup 48h old news

Two crons fail independently: news down -> main pipeline uses previous-round cache;
                   main pipeline down -> news continues refreshing.
Each Phase fails independently: Phase B/C down does not affect Phase A (Finnhub), and vice versa.

Usage:
    python -m cron.pre_fetch_news

Railway recommended cron (ET):
    09:50, 11:50, 13:50  (once before/during/after trading session)
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime
from typing import Any

from openai import AsyncOpenAI
from sqlalchemy import delete, select

from constants import DEFAULT_ETF_UNIVERSE, resolve_universe
from db.models import MacroNewsCache, TickerNewsLibrary
from db.session import AsyncSessionLocal
from services.alphavantage_client import fetch_ticker_news_av
from services.finnhub_client import (
    fetch_economic_calendar,
    fetch_earnings_flag,
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

PRE_FETCH_TIMEOUT_S = 600       # 10 minutes overall timeout
TTL_SECONDS         = 48 * 3600 # 48h

# ═══════════════════════════════════════════════════════════════
# News Structuring (Phase D')
# ═══════════════════════════════════════════════════════════════

NEWS_STRUCTURING_PROMPT = """
You are a financial news structuring engine. I will give you a set of raw news articles,
and you need to extract structured signals useful for ETF portfolio management.

## Input news
{raw_news_text}

## ETF Universe
{etf_universe}

## Output requirements (strict JSON, no prefix)

{{
  "processed_at": "ISO timestamp",
  "macro_signals": [
    {{
      "event": "event name (≤20 chars)",
      "category": "fed_policy"|"inflation"|"employment"|"geopolitical"|"earnings"|"other",
      "direction": "positive"|"negative"|"neutral"|"mixed",
      "impact_horizon": "immediate"|"short_term"|"medium_term",
      "confidence": "high"|"medium"|"low",
      "affected_sectors": ["XLK", "XLF"],
      "summary": "one-sentence explanation (≤40 chars)"
    }}
  ],
  "ticker_signals": {{
    "<TICKER>": {{
      "sentiment": "positive"|"negative"|"neutral",
      "news_count": <int>,
      "key_events": ["event 1 (≤20 chars)", "event 2"],
      "data_quality": "fresh"|"stale"|"no_news"
    }}
  }},
  "noise_filtered": <int>,  // duplicate/low-quality news filtered out
  "data_gaps": ["XLE has no valid news in past 48h", "..."]
}}

macro_signals: max 5 items, only keep impact >= medium.
ticker_signals: only include tickers with news; do not include tickers with no news.
If multiple news items describe the same event, merge into one; do not duplicate.
"""


async def structure_news_with_llm(
    raw_news: list[dict],
    etf_universe: list[str],
) -> dict:
    """
    Call gpt-4o-mini to structurize raw news.
    raw_news format: Finnhub/AlphaVantage/RSS unified format
      {headline, summary, source, datetime, url, ...}
    Cost: ~$0.002-0.005 per run (far lower than gpt-4o)
    """
    from config import get_settings
    settings = get_settings()

    if not raw_news:
        return {
            "macro_signals": [],
            "ticker_signals": {},
            "noise_filtered": 0,
            "data_gaps": ["no news data"],
        }

    # Concatenate raw news (control tokens, truncate key fields)
    news_parts = []
    for n in raw_news[:40]:  # max 40 items to prevent token overflow
        source = n.get("source", "?")
        dt = n.get("datetime", 0)
        if dt:
            from datetime import datetime as dt_cls
            try:
                dt_str = dt_cls.utcfromtimestamp(dt).strftime("%Y-%m-%d %H:%M")
            except (ValueError, OSError):
                dt_str = str(dt)
        else:
            dt_str = "?"
        headline = n.get("headline", "")
        summary = n.get("summary", "")[:300]
        news_parts.append(
            f"[{source}] {dt_str}\nTitle: {headline}\nSummary: {summary}"
        )
    news_text = "\n\n".join(news_parts)

    prompt = NEWS_STRUCTURING_PROMPT.format(
        raw_news_text=news_text,
        etf_universe=", ".join(etf_universe),
    )

    try:
        client = AsyncOpenAI(api_key=settings.openai_api_key)
        response = await client.chat.completions.create(
            model=settings.openai_model,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=1500,
            temperature=0.1,  # low temperature to ensure structure stability
            response_format={"type": "json_object"},
        )

        structured = json.loads(response.choices[0].message.content)
        structured["_model"] = settings.openai_model
        structured["_raw_news_count"] = len(raw_news)
        logger.info(
            f"News structuring complete: {len(raw_news)} raw -> "
            f"{len(structured.get('macro_signals', []))} macro_signals"
        )
        return structured

    except Exception as e:
        logger.error(f"News structuring failed: {e}, using empty structure")
        return {
            "macro_signals": [],
            "ticker_signals": {},
            "noise_filtered": 0,
            "data_gaps": [f"structuring failed: {str(e)}"],
            "_fallback": True,
        }


# ═══════════════════════════════════════════════════════════════
# Macro section
# ═══════════════════════════════════════════════════════════════

async def _refresh_macro_cache(universe: list[str]) -> dict[str, Any]:
    """
    Pull macro news + calendar, upsert to MacroNewsCache (single row, id=1).
    Phase D': call gpt-4o-mini for structurization, store raw_payload + structured_payload.
    """
    # Move sync httpx to to_thread to avoid blocking event loop
    macro_news   = await asyncio.to_thread(fetch_macro_news, 20)
    econ_events  = await asyncio.to_thread(fetch_economic_calendar, 3)

    # Phase D': News structurization
    structured = await structure_news_with_llm(macro_news, universe)

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
        f"macro cache refreshed | news={len(macro_news)} | calendar={len(econ_events)} | "
        f"structured={not structured.get('_fallback')}"
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

async def _process_ticker(ticker: str) -> int:
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
    has_earnings = await asyncio.to_thread(fetch_earnings_flag, ticker, 7)
    hard_risks   = scan_hard_risks(ticker, news_items, has_earnings)

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
# Phase B: Alpha Vantage
# ═══════════════════════════════════════════════════════════════

async def _fetch_alphavantage(universe: list[str]) -> int:
    """
    Phase B: Bulk fetch news from Alpha Vantage.
    AV supports comma-separated multi-ticker in one request (free 25 req/day).
    Returns number of newly written entries.
    """
    try:
        av_items = await asyncio.to_thread(fetch_ticker_news_av, universe, 50)
    except Exception as e:
        logger.error(f"Alpha Vantage fetch failed: {e}")
        return 0

    if not av_items:
        return 0

    # Bucket by ticker (AV returns related ticker list in 'related')
    universe_set = set(universe)
    ticker_buckets: dict[str, list[dict]] = {}
    for item in av_items:
        url = (item.get("url") or "").strip()
        if not url:
            continue
        item["headline"] = sanitize(item.get("headline", ""))
        if not item["headline"]:
            continue

        # Match our universe tickers from ticker_sentiments or related
        related = item.get("related") or []
        av_sentiments = item.get("ticker_sentiments") or {}
        matched_tickers = set()
        for t in related:
            if t in universe_set:
                matched_tickers.add(t)
        for t in av_sentiments:
            if t in universe_set:
                matched_tickers.add(t)

        if not matched_tickers:
            continue

        for t in matched_tickers:
            ticker_buckets.setdefault(t, []).append(item)

    total_new = 0
    for ticker, items in ticker_buckets.items():
        n = await _insert_external_news(ticker, items, "alphavantage")
        total_new += n

    logger.info(f"Phase B (Alpha Vantage): {total_new} new articles across {len(ticker_buckets)} tickers")
    return total_new


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
        stmt = delete(TickerNewsLibrary).where(TickerNewsLibrary.datetime_utc < cutoff)
        res = await db.execute(stmt)
        await db.commit()
        return res.rowcount or 0


# ═══════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════

async def run_pre_fetch() -> dict:
    """Main entry. Returns statistics."""
    from db.session import init_db
    await init_db()

    universe = await resolve_universe()
    logger.info(f"=== pre_fetch_news START | universe={len(universe)} tickers ===")

    # ── Phase A: Finnhub (macro + per-ticker) ──
    macro_stats = await _refresh_macro_cache(universe)

    finnhub_new = 0
    for ticker in universe:
        try:
            n = await _process_ticker(ticker)
            if n > 0:
                logger.info(f"  [finnhub] {ticker}: {n} new articles")
            finnhub_new += n
        except Exception as e:
            logger.error(f"  [finnhub] {ticker}: FAILED — {e}")
        await asyncio.sleep(0.5)  # rate limit finnhub

    # ── Phase B: Alpha Vantage (bulk ticker news) ──
    av_new = 0
    try:
        av_new = await _fetch_alphavantage(universe)
    except Exception as e:
        logger.error(f"Phase B (Alpha Vantage) FAILED — {e}")

    # ── Phase C: RSS feeds (keyword-matched to tickers) ──
    rss_new = 0
    try:
        rss_new = await _fetch_rss(universe)
    except Exception as e:
        logger.error(f"Phase C (RSS) FAILED — {e}")

    # ── Phase F: Cleanup 48h ──
    deleted = await _cleanup_old_news()

    total_new = finnhub_new + av_new + rss_new
    logger.info(
        f"=== pre_fetch_news DONE | macro_news={macro_stats['n_news']} "
        f"| finnhub={finnhub_new} | alphavantage={av_new} | rss={rss_new} "
        f"| total_new={total_new} | cleaned={deleted} ==="
    )

    return {
        "universe_size":    len(universe),
        "macro_news":       macro_stats["n_news"],
        "macro_events":     macro_stats["n_events"],
        "finnhub_articles": finnhub_new,
        "av_articles":      av_new,
        "rss_articles":     rss_new,
        "total_new":        total_new,
        "cleaned":          deleted,
    }


async def main() -> None:
    try:
        result = await asyncio.wait_for(run_pre_fetch(), timeout=PRE_FETCH_TIMEOUT_S)
        logger.info(f"pre_fetch_news result: {result}")
    except asyncio.TimeoutError:
        logger.error(f"pre_fetch_news TIMEOUT after {PRE_FETCH_TIMEOUT_S}s")
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
    asyncio.run(main())
