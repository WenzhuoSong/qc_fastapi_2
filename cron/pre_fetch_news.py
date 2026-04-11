"""
cron/pre_fetch_news.py —— Cron 1: 独立的新闻预抓取任务。

职责：与主 pipeline 完全解耦，每 2h 跑一次：
    1. Finnhub → fetch_macro_news + fetch_economic_calendar → upsert MacroNewsCache
    2. for ticker in UNIVERSE (ETF 维度):
         - fetch_ticker_news + fetch_earnings_flag
         - 按 url 去重 (查 TickerNewsLibrary)
         - LLM 批量摘要 (gpt-4o-mini, 失败回落)
         - scan_hard_risks
         - 批量插入 TickerNewsLibrary
    3. 收尾清理：DELETE FROM ticker_news_library WHERE datetime_utc < now-48h

两条 cron 独立失败：新闻挂了，主 pipeline 用上一轮缓存继续跑；
                   主 pipeline 挂了，新闻照常刷新。

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

from constants import resolve_universe
from db.models import MacroNewsCache, TickerNewsLibrary
from db.session import AsyncSessionLocal
from services.finnhub_client import (
    fetch_economic_calendar,
    fetch_earnings_flag,
    fetch_macro_news,
    fetch_ticker_news,
    get_source_credibility,
    scan_hard_risks,
)
from services.news_summarizer import sanitize, summarize_headlines_batch
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

    macro_stats = await _refresh_macro_cache()

    total_new = 0
    for ticker in universe:
        try:
            n = await _process_ticker(ticker)
            if n > 0:
                logger.info(f"  {ticker}: {n} new articles")
            total_new += n
        except Exception as e:
            logger.error(f"  {ticker}: FAILED — {e}")
        await asyncio.sleep(0.5)  # rate limit finnhub

    deleted = await _cleanup_old_news()
    logger.info(
        f"=== pre_fetch_news DONE | macro_news={macro_stats['n_news']} "
        f"| new_ticker_articles={total_new} | cleaned={deleted} ==="
    )

    return {
        "universe_size": len(universe),
        "macro_news":    macro_stats["n_news"],
        "macro_events":  macro_stats["n_events"],
        "new_articles":  total_new,
        "cleaned":       deleted,
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
