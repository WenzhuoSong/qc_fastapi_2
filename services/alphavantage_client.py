"""
Alpha Vantage News Sentiment API 客户端。

免费层限制：25 requests/day，所以只在 pre_fetch_news 中调一次批量拉取。
返回格式与 finnhub_client 对齐（normalize 后一致），方便 pre_fetch_news 统一处理。

所有函数失败时返回空结果，保证上游 cron 不会崩溃。
网络调用是同步 httpx，上游用 asyncio.to_thread 包装。
"""
from __future__ import annotations

import logging
import time
from typing import Dict, List

import httpx

from config import get_settings
from services.finnhub_client import get_source_credibility

logger = logging.getLogger("qc_fastapi_2.alphavantage")
settings = get_settings()

_BASE = "https://www.alphavantage.co/query"
_TIMEOUT = 15

# Alpha Vantage sentiment label → 我们的 sentiment 映射
_SENTIMENT_MAP: Dict[str, str] = {
    "Bullish":           "positive",
    "Somewhat-Bullish":  "positive",
    "Neutral":           "neutral",
    "Somewhat-Bearish":  "negative",
    "Bearish":           "negative",
}


def fetch_ticker_news_av(tickers: List[str], limit: int = 50) -> List[dict]:
    """
    批量拉取多个 ticker 的新闻（Alpha Vantage 支持逗号分隔）。
    返回 normalize 后的 list[dict]，格式与 finnhub 一致。

    Alpha Vantage 免费层 25 req/day，所以一次调用传所有 tickers。
    """
    api_key = settings.alphavantage_api_key
    if not api_key:
        logger.warning("fetch_ticker_news_av: ALPHAVANTAGE_API_KEY not set, returning empty")
        return []

    # Alpha Vantage 最多接受 ~50 个 ticker，我们的 universe 17 个，够用
    tickers_str = ",".join(tickers)

    try:
        resp = httpx.get(
            _BASE,
            params={
                "function": "NEWS_SENTIMENT",
                "tickers":  tickers_str,
                "limit":    str(limit),
                "apikey":   api_key,
            },
            timeout=_TIMEOUT,
        )
        data = resp.json()

        # Alpha Vantage 错误响应检查
        if "Error Message" in data or "Note" in data:
            msg = data.get("Error Message") or data.get("Note", "")
            logger.warning(f"Alpha Vantage API warning: {msg}")
            return []

        feed = data.get("feed") or []
        return [_normalize(item) for item in feed]

    except Exception as e:
        logger.error(f"fetch_ticker_news_av error: {e}")
        return []


def _normalize(item: dict) -> dict:
    """将 Alpha Vantage 文章转换为与 finnhub 一致的格式。"""
    # 提取 ticker_sentiment 里的 ticker 列表
    ticker_sentiments = item.get("ticker_sentiment") or []
    tickers = [ts.get("ticker", "") for ts in ticker_sentiments if ts.get("ticker")]

    # 取整体 sentiment
    overall = item.get("overall_sentiment_label", "Neutral")
    sentiment = _SENTIMENT_MAP.get(overall, "neutral")

    # 时间：Alpha Vantage 格式 "20260413T120000"
    time_str = item.get("time_published", "")
    dt_unix = _parse_av_time(time_str)

    source = item.get("source", "") or ""

    return {
        "headline":    item.get("title", "") or "",
        "summary":     item.get("summary", "") or "",
        "source":      source,
        "source_api":  "alphavantage",
        "category":    ", ".join(item.get("topics", []) or []) if isinstance(item.get("topics"), list)
                       else _extract_topics(item),
        "related":     tickers,
        "datetime":    dt_unix,
        "url":         item.get("url", "") or "",
        "sentiment":   sentiment,
        "credibility": get_source_credibility(source),
        "ticker_sentiments": {
            ts.get("ticker", ""): {
                "score": float(ts.get("ticker_sentiment_score", 0)),
                "label": ts.get("ticker_sentiment_label", "Neutral"),
            }
            for ts in ticker_sentiments
            if ts.get("ticker")
        },
    }


def _extract_topics(item: dict) -> str:
    """从 topics 数组里提取 topic 名称。"""
    topics = item.get("topics") or []
    if not topics:
        return ""
    names = []
    for t in topics:
        if isinstance(t, dict):
            names.append(t.get("topic", ""))
        elif isinstance(t, str):
            names.append(t)
    return ", ".join(n for n in names if n)


def _parse_av_time(time_str: str) -> int:
    """解析 Alpha Vantage 时间格式 '20260413T120000' → Unix 秒。"""
    if not time_str:
        return int(time.time())
    try:
        # 格式：YYYYMMDDTHHMMSS
        from datetime import datetime
        dt = datetime.strptime(time_str, "%Y%m%dT%H%M%S")
        return int(dt.timestamp())
    except (ValueError, TypeError):
        return int(time.time())
