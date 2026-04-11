"""
Finnhub 客户端 —— 新闻 / 经济日历 / 财报日历 / 硬风险扫描。

所有函数失败时返回空结果（[] / False / {}），保证上游 cron 不会因单一 API 异常崩溃。
所有网络调用都是同步 httpx，上游用 asyncio.to_thread 包装即可。

Ported from qc_fastapi/app/pipeline/data_fetcher.py + pre_fetch_pipeline.py 的 credibility 表。
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List

import httpx

from config import get_settings

logger = logging.getLogger("qc_fastapi_2.finnhub")
settings = get_settings()

_BASE = "https://finnhub.io/api/v1"
_TIMEOUT = 10
_MAX_RETRIES = 3


def _token() -> str:
    return settings.finnhub_api_key or ""


def _get(url: str, params: dict) -> httpx.Response:
    """GET with exponential backoff retry."""
    for attempt in range(_MAX_RETRIES):
        try:
            return httpx.get(url, params=params, timeout=_TIMEOUT)
        except Exception:
            if attempt < _MAX_RETRIES - 1:
                time.sleep(2 ** attempt)
            else:
                raise


# ═══════════════════════════════════════════════════════════════
# Source credibility (0 − 100)
# ═══════════════════════════════════════════════════════════════

SOURCE_CREDIBILITY: Dict[str, int] = {
    # Tier 1 —— 顶级财经 (100)
    "Bloomberg":            100,
    "Reuters":              100,
    "Wall Street Journal":  100,
    "Financial Times":      100,
    "WSJ":                  100,
    # Tier 2 —— 主流商业媒体 (85)
    "CNBC":                  85,
    "MarketWatch":           85,
    "Barron's":              85,
    "The Economist":         85,
    # Tier 3 —— 财经博客 / 分析 (55 − 60)
    "Seeking Alpha":         60,
    "Benzinga":              60,
    "Motley Fool":           60,
    "Zacks":                 55,
    # Tier 4 —— 新闻通稿 (40)
    "PR Newswire":           40,
    "Business Wire":         40,
    "GlobeNewswire":         40,
    # 默认
    "_DEFAULT":              30,
}


def get_source_credibility(source: str) -> int:
    if not source:
        return SOURCE_CREDIBILITY["_DEFAULT"]
    if source in SOURCE_CREDIBILITY:
        return SOURCE_CREDIBILITY[source]
    source_lower = source.lower()
    for key, score in SOURCE_CREDIBILITY.items():
        if key != "_DEFAULT" and key.lower() in source_lower:
            return score
    return SOURCE_CREDIBILITY["_DEFAULT"]


# ═══════════════════════════════════════════════════════════════
# Macro
# ═══════════════════════════════════════════════════════════════

def fetch_macro_news(limit: int = 20) -> List[dict]:
    """拉取最近 ~24h 的宏观新闻。返回 list[dict]。失败返回空列表。"""
    if not _token():
        logger.warning("fetch_macro_news: FINNHUB_API_KEY not set, returning empty")
        return []
    try:
        resp = _get(
            f"{_BASE}/news",
            params={"category": "general", "token": _token()},
        )
        items = resp.json()[:limit]
        return [_normalize_article(it) for it in items]
    except Exception as e:
        logger.error(f"fetch_macro_news error: {e}")
        return []


def fetch_economic_calendar(days_ahead: int = 3) -> List[dict]:
    """拉取未来 N 天的高影响经济事件 (CPI / NFP / FOMC ...)。"""
    if not _token():
        return []
    today = datetime.utcnow().date()
    end = today + timedelta(days=days_ahead)
    try:
        resp = _get(
            f"{_BASE}/calendar/economic",
            params={"from": str(today), "to": str(end), "token": _token()},
        )
        events = resp.json().get("economicCalendar", []) or []
        high = [e for e in events if (e.get("impact") or "").lower() == "high"]
        if not high:
            high = events[:5]
        return high[:10]
    except Exception as e:
        logger.error(f"fetch_economic_calendar error: {e}")
        return []


# ═══════════════════════════════════════════════════════════════
# Ticker
# ═══════════════════════════════════════════════════════════════

def fetch_ticker_news(ticker: str, days_back: int = 2, limit: int = 10) -> List[dict]:
    """拉取单个 ticker 最近 N 天的公司新闻。"""
    if not _token():
        return []
    today = datetime.utcnow().date()
    start = today - timedelta(days=days_back)
    try:
        resp = _get(
            f"{_BASE}/company-news",
            params={
                "symbol": ticker,
                "from":   str(start),
                "to":     str(today),
                "token":  _token(),
            },
        )
        items = resp.json()[:limit]
        return [_normalize_article(it) for it in items]
    except Exception as e:
        logger.error(f"fetch_ticker_news({ticker}) error: {e}")
        return []


def fetch_earnings_flag(ticker: str, days_ahead: int = 7) -> bool:
    """判断 ticker 未来 N 天内是否有财报。ETF 通常不会命中。"""
    if not _token():
        return False
    today = datetime.utcnow().date()
    end = today + timedelta(days=days_ahead)
    try:
        resp = _get(
            f"{_BASE}/calendar/earnings",
            params={
                "from":   str(today),
                "to":     str(end),
                "symbol": ticker,
                "token":  _token(),
            },
        )
        items = resp.json().get("earningsCalendar", []) or []
        return len(items) > 0
    except Exception:
        return False


# ═══════════════════════════════════════════════════════════════
# Hard risk scanning —— 关键字匹配 (factual, not predictive)
# ═══════════════════════════════════════════════════════════════

_HARD_RISK_PATTERNS: Dict[str, List[str]] = {
    "earnings_soon":      ["earnings", "quarterly results", "revenue report", "eps"],
    "fda_pending":        ["fda approval", "fda decision", "fda review", "drug approval"],
    "trading_halted":     ["trading halt", "halted trading", "suspended trading"],
    "acquisition_target": [
        "acquisition", "merger", "takeover", "buyout",
        "takeover bid", "merger agreement",
    ],
    "major_lawsuit":      ["lawsuit", "class action", "sec investigation", "fraud"],
}


def scan_hard_risks(
    ticker: str, news: List[dict], has_earnings: bool
) -> Dict[str, str]:
    """
    扫描一个 ticker 的近期新闻，返回 {risk_type: reason}。

    这些是事实事件检测（非预测），用于 hard_risk_filter 禁止新建仓。
    Phase 1 (ETF 维度) 基本空转 —— ETF 不会命中 earnings / FDA / lawsuit。
    """
    risks: Dict[str, str] = {}

    if has_earnings:
        risks["earnings_soon"] = "Earnings within 7 days"

    combined_text = " ".join(
        ((a.get("headline") or "") + " " + (a.get("summary") or "")).lower()
        for a in news
    )

    for risk_type, keywords in _HARD_RISK_PATTERNS.items():
        if risk_type == "earnings_soon":
            continue
        for kw in keywords:
            if kw in combined_text:
                risks[risk_type] = f"keyword '{kw}' detected"
                break

    return risks


# ═══════════════════════════════════════════════════════════════
# Internal helpers
# ═══════════════════════════════════════════════════════════════

def _normalize_article(it: dict) -> dict:
    return {
        "headline": it.get("headline", "") or "",
        "summary":  it.get("summary", "")  or "",
        "source":   it.get("source", "")   or "",
        "category": it.get("category", "") or "",
        "related":  it.get("related", [])  or [],
        "datetime": int(it.get("datetime", 0) or 0),
        "url":      it.get("url", "")      or "",
    }
