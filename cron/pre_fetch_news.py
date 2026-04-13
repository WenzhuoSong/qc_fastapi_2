"""
cron/pre_fetch_news.py —— Cron 1: 独立的多源新闻预抓取任务。

职责：与主 pipeline 完全解耦，每 2h 跑一次：
    Phase A: Finnhub → fetch_macro_news + fetch_economic_calendar → upsert MacroNewsCache
                       + per-ticker news → TickerNewsLibrary
    Phase B: Alpha Vantage → 批量拉取 ticker news → TickerNewsLibrary (source_api=alphavantage)
    Phase C: RSS feeds → 并行抓 5 个 feed → 关键词匹配 ticker → TickerNewsLibrary (source_api=rss)
    Phase D: LLM batch summarize (gpt-4o-mini, 已有，Phase A 内执行)
    Phase E: cross-source dedup (url 去重，已在各 Phase 内按 url 去重)
    Phase F: cleanup 48h old news

两条 cron 独立失败：新闻挂了，主 pipeline 用上一轮缓存继续跑；
                   主 pipeline 挂了，新闻照常刷新。
各 Phase 独立失败：Phase B/C 挂了不影响 Phase A（Finnhub），反之亦然。

使用：
    python -m cron.pre_fetch_news

Railway 推荐 cron (ET):
    09:50, 11:50, 13:50  (交易时段前/中/后各一次)
"""
from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime
from typing import Any

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

PRE_FETCH_TIMEOUT_S = 600       # 10 分钟整体超时
TTL_SECONDS         = 48 * 3600 # 48h


# ═══════════════════════════════════════════════════════════════
# Macro section
# ═══════════════════════════════════════════════════════════════

async def _refresh_macro_cache() -> dict[str, Any]:
    """拉 macro news + calendar，upsert 到 MacroNewsCache (单行，id=1)。"""
    # 同步 httpx 放到 to_thread 里避免阻塞事件循环
    macro_news   = await asyncio.to_thread(fetch_macro_news, 20)
    econ_events  = await asyncio.to_thread(fetch_economic_calendar, 3)

    prose = _build_macro_prose(macro_news, econ_events)

    async with AsyncSessionLocal() as db:
        existing = (await db.execute(select(MacroNewsCache).where(MacroNewsCache.id == 1))).scalar_one_or_none()
        if existing:
            existing.as_of             = datetime.utcnow()
            existing.macro_news        = macro_news
            existing.economic_calendar = econ_events
            existing.prose_summary     = prose
        else:
            db.add(MacroNewsCache(
                id                = 1,
                as_of             = datetime.utcnow(),
                macro_news        = macro_news,
                economic_calendar = econ_events,
                prose_summary     = prose,
            ))
        await db.commit()

    logger.info(
        f"macro cache refreshed | news={len(macro_news)} | calendar={len(econ_events)}"
    )
    return {"n_news": len(macro_news), "n_events": len(econ_events)}


def _build_macro_prose(macro_news: list[dict], econ_events: list[dict]) -> str:
    """把 macro news + calendar 拼成一段可直接塞进 prompt 的散文。"""
    lines: list[str] = []

    if macro_news:
        lines.append("## 宏观新闻")
        for it in macro_news[:8]:
            headline = sanitize(it.get("headline", ""))
            source   = it.get("source", "") or "unknown"
            if headline:
                lines.append(f"- [{source}] {headline}")
    else:
        lines.append("## 宏观新闻\n(无数据)")

    lines.append("")
    if econ_events:
        lines.append("## 本周经济日程")
        for e in econ_events[:8]:
            event  = sanitize(e.get("event", ""))
            impact = e.get("impact", "")
            time_str = e.get("time", "") or e.get("date", "")
            if event:
                lines.append(f"- [{impact}] {time_str} {event}")
    else:
        lines.append("## 本周经济日程\n(无高影响事件)")

    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════
# Ticker section
# ═══════════════════════════════════════════════════════════════

async def _process_ticker(ticker: str) -> int:
    """
    处理单个 ticker：fetch → dedupe → summarize → hard_risks → insert。
    返回新写入的条数。
    """
    news_items = await asyncio.to_thread(fetch_ticker_news, ticker, 2, 10)
    if not news_items:
        return 0

    # 清洗 headline + url
    cleaned: list[dict] = []
    for it in news_items:
        url = (it.get("url") or "").strip()
        if not url:
            continue
        it["headline"] = sanitize(it.get("headline", ""))
        cleaned.append(it)

    if not cleaned:
        return 0

    # 查已有 url 去重
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

    # LLM 批量摘要 —— 失败有回落，不抛异常
    summaries = await summarize_headlines_batch(ticker, new_items)

    # earnings + hard risks (基于全部 news_items, 不只是新加的)
    has_earnings = await asyncio.to_thread(fetch_earnings_flag, ticker, 7)
    hard_risks   = scan_hard_risks(ticker, news_items, has_earnings)

    # 批量插入
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
            # UniqueConstraint 并发冲突 —— 跳过这一批
            await db.rollback()
            logger.warning(f"{ticker}: insert conflict, skipped ({e})")
            return 0

    return len(rows)


# ═══════════════════════════════════════════════════════════════
# Phase B: Alpha Vantage
# ═══════════════════════════════════════════════════════════════

async def _fetch_alphavantage(universe: list[str]) -> int:
    """
    Phase B: 批量从 Alpha Vantage 拉取新闻。
    AV 支持逗号分隔多 ticker 一次请求（免费 25 req/day）。
    返回新写入条数。
    """
    try:
        av_items = await asyncio.to_thread(fetch_ticker_news_av, universe, 50)
    except Exception as e:
        logger.error(f"Alpha Vantage fetch failed: {e}")
        return 0

    if not av_items:
        return 0

    # 按 ticker 分桶（AV 返回的 related 包含关联 ticker 列表）
    universe_set = set(universe)
    ticker_buckets: dict[str, list[dict]] = {}
    for item in av_items:
        url = (item.get("url") or "").strip()
        if not url:
            continue
        item["headline"] = sanitize(item.get("headline", ""))
        if not item["headline"]:
            continue

        # 从 ticker_sentiments 或 related 中匹配我们 universe 的 ticker
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

# 简单的 ETF → 关键词映射，用于将 RSS 宏观新闻关联到 ticker
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
    """基于关键词匹配将 RSS 文章关联到 universe 中的 ticker。"""
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
    Phase C: 从 RSS feeds 抓取新闻，关键词匹配 ticker 后写入 TickerNewsLibrary。
    返回新写入条数。
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
    将 Alpha Vantage 或 RSS 的文章写入 TickerNewsLibrary。
    按 url 去重，LLM 摘要（仅对没有预置 sentiment 的条目），批量插入。
    返回新写入条数。
    """
    # url 去重
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

    # 只对没有预置 sentiment 的条目做 LLM 摘要
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
    """主入口。返回统计信息。"""
    from db.session import init_db
    await init_db()

    universe = await resolve_universe()
    logger.info(f"=== pre_fetch_news START | universe={len(universe)} tickers ===")

    # ── Phase A: Finnhub (macro + per-ticker) ──
    macro_stats = await _refresh_macro_cache()

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
                {"text": f"新闻预抓取异常: {e}", "parse_mode": ""}
            )
        except Exception:
            pass
        raise


if __name__ == "__main__":
    asyncio.run(main())
