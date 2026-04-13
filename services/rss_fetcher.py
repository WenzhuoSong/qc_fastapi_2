"""
RSS 新闻采集器 —— 从财经媒体 RSS feeds 抓取宏观/行业头条。

免费、无 API key、无 rate limit，适合补充 Finnhub 和 Alpha Vantage 的盲区。
使用 feedparser 库解析，所有网络调用同步执行，上游用 asyncio.to_thread 包装。

失败时返回空结果，保证上游 cron 不会崩溃。
"""
from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import List, Tuple

import feedparser

from services.finnhub_client import get_source_credibility

logger = logging.getLogger("qc_fastapi_2.rss_fetcher")

# ═══════════════════════════════════════════════════════════════
# RSS Feed 列表
# ═══════════════════════════════════════════════════════════════

RSS_FEEDS: List[Tuple[str, str]] = [
    ("https://feeds.content.dowjones.io/public/rss/mw_topstories", "MarketWatch"),
    ("https://feeds.content.dowjones.io/public/rss/mw_marketpulse", "MarketWatch"),
    ("https://www.cnbc.com/id/100003114/device/rss/rss.html", "CNBC"),
    ("https://finance.yahoo.com/news/rssindex", "Yahoo Finance"),
    ("https://feeds.reuters.com/reuters/businessNews", "Reuters"),
]


def fetch_rss_news(max_per_feed: int = 10) -> List[dict]:
    """
    从所有 RSS feeds 抓取最新文章。
    返回 normalize 后的 list[dict]，格式与 finnhub/alphavantage 一致。
    """
    all_articles: List[dict] = []

    for feed_url, source_name in RSS_FEEDS:
        try:
            articles = _fetch_single_feed(feed_url, source_name, max_per_feed)
            all_articles.extend(articles)
            logger.info(f"RSS {source_name}: {len(articles)} articles")
        except Exception as e:
            logger.warning(f"RSS {source_name} failed: {e}")

    logger.info(f"RSS total: {len(all_articles)} articles from {len(RSS_FEEDS)} feeds")
    return all_articles


def _fetch_single_feed(feed_url: str, source_name: str, max_items: int) -> List[dict]:
    """解析单个 RSS feed，返回 normalize 后的文章列表。"""
    feed = feedparser.parse(feed_url)

    if feed.bozo and not feed.entries:
        logger.warning(f"RSS parse error for {source_name}: {feed.bozo_exception}")
        return []

    articles: List[dict] = []
    for entry in feed.entries[:max_items]:
        article = _normalize_entry(entry, source_name)
        if article:
            articles.append(article)

    return articles


def _normalize_entry(entry: dict, source_name: str) -> dict | None:
    """将 RSS entry 转换为与 finnhub 一致的通用格式。"""
    title = (entry.get("title") or "").strip()
    link = (entry.get("link") or "").strip()

    if not title or not link:
        return None

    # 提取摘要（RSS 通常在 summary 或 description 里）
    summary = (entry.get("summary") or entry.get("description") or "").strip()
    # 去掉 HTML 标签（简单处理）
    summary = _strip_html(summary)
    # 截断过长的摘要
    if len(summary) > 500:
        summary = summary[:500] + "..."

    dt_unix = _parse_rss_time(entry)

    return {
        "headline":    title,
        "summary":     summary,
        "source":      source_name,
        "source_api":  "rss",
        "category":    _extract_categories(entry),
        "related":     [],   # RSS 没有 ticker 关联，由 LLM 摘要时判断
        "datetime":    dt_unix,
        "url":         link,
        "credibility": get_source_credibility(source_name),
    }


def _parse_rss_time(entry: dict) -> int:
    """从 RSS entry 解析发布时间 → Unix 秒。"""
    # feedparser 解析后的 struct_time
    published = entry.get("published_parsed") or entry.get("updated_parsed")
    if published:
        try:
            return int(time.mktime(published))
        except (ValueError, OverflowError, TypeError):
            pass

    # 回落：尝试解析原始字符串
    date_str = entry.get("published") or entry.get("updated") or ""
    if date_str:
        for fmt in ("%a, %d %b %Y %H:%M:%S %Z", "%Y-%m-%dT%H:%M:%S%z"):
            try:
                dt = datetime.strptime(date_str, fmt)
                return int(dt.timestamp())
            except (ValueError, TypeError):
                continue

    return int(time.time())


def _extract_categories(entry: dict) -> str:
    """从 RSS entry 提取分类标签。"""
    tags = entry.get("tags") or []
    if not tags:
        return ""
    names = []
    for tag in tags:
        if isinstance(tag, dict):
            names.append(tag.get("term", "") or tag.get("label", ""))
        elif isinstance(tag, str):
            names.append(tag)
    return ", ".join(n for n in names if n)[:100]


def _strip_html(text: str) -> str:
    """简单去除 HTML 标签。"""
    import re
    clean = re.sub(r"<[^>]+>", " ", text)
    clean = re.sub(r"\s+", " ", clean)
    return clean.strip()
