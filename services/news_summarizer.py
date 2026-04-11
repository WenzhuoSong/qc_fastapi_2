"""
LLM 新闻批量摘要 —— 调 gpt-4o-mini 的 Structured Outputs。

单个 ticker 一次 LLM 调用，最多同时处理 BATCH_SIZE 条 headline。
失败或无 key 时回落到 headline 截断作为 summary，保证 cron 不会因 LLM 挂掉而终止。

Ported from qc_fastapi/pre_fetch_pipeline.py:185−259。
"""
from __future__ import annotations

import logging
import re
from typing import List, Literal

from openai import AsyncOpenAI
from pydantic import BaseModel, Field

from config import get_settings

logger = logging.getLogger("qc_fastapi_2.news_summarizer")
settings = get_settings()

BATCH_SIZE = 10

_client: AsyncOpenAI | None = None


def _get_client() -> AsyncOpenAI:
    global _client
    if _client is None:
        _client = AsyncOpenAI(api_key=settings.openai_api_key)
    return _client


# ═══════════════════════════════════════════════════════════════
# Pydantic schemas for Structured Outputs
# ═══════════════════════════════════════════════════════════════

class NewsAnalysis(BaseModel):
    index: int = Field(description="1-based index matching the input headline")
    summary: str = Field(description="Impact on this ticker in one sentence, max 30 words")
    sentiment: Literal["positive", "negative", "neutral"]
    relevance: Literal["direct", "indirect", "not_relevant"]
    is_hard_event: bool


class BatchAnalysisResponse(BaseModel):
    results: List[NewsAnalysis]


# ═══════════════════════════════════════════════════════════════
# System prompt
# ═══════════════════════════════════════════════════════════════

_SUMMARIZE_SYSTEM = (
    "You are a quantitative financial news analyst. Be concise and accurate.\n\n"
    "RELEVANCE RULE:\n"
    "  direct       → headline is directly about this ticker / ETF itself\n"
    "  indirect     → industry trend that affects this ticker and peers\n"
    "  not_relevant → headline has nothing to do with this ticker\n\n"
    "SENTIMENT RULE:\n"
    "  sentiment is the IMPACT on this ticker, NOT the tone of the headline.\n"
    "  Example: 'Oil prices spike on supply cut' → sentiment=positive for XLE.\n\n"
    "HARD EVENT RULE:\n"
    "is_hard_event = true ONLY for NEGATIVE, BINARY-OUTCOME events with UNHEDGEABLE RISK:\n"
    "  - Earnings miss / revenue shortfall / guidance cut\n"
    "  - FDA rejection / clinical trial failure\n"
    "  - Trading halt / suspension\n"
    "  - Being acquired (target of takeover, NOT the acquirer)\n"
    "  - SEC investigation / fraud allegation / class-action lawsuit\n"
    "  - Bankruptcy filing / debt default\n"
    "  - Regulatory ban / sanctions\n\n"
    "is_hard_event = false for ALL of these:\n"
    "  - Positive deals, partnerships, investments, contracts\n"
    "  - Analyst upgrades / downgrades\n"
    "  - General market commentary or sector trends\n"
    "  - Price movements or trading volume\n"
    "  - Product launches or expansion plans\n\n"
    "When in doubt, set is_hard_event = false."
)


# ═══════════════════════════════════════════════════════════════
# Text sanitization
# ═══════════════════════════════════════════════════════════════

_CTRL_CHARS = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")


def sanitize(text: str) -> str:
    """移除控制字符和多余空白。"""
    if not text:
        return ""
    text = _CTRL_CHARS.sub("", text)
    text = text.replace("\r\n", " ").replace("\n", " ").replace("\r", " ")
    return text.strip()


# ═══════════════════════════════════════════════════════════════
# Main entry
# ═══════════════════════════════════════════════════════════════

async def summarize_headlines_batch(
    ticker: str, news_items: List[dict]
) -> List[dict]:
    """
    为一个 ticker 的一组 headline 做批量 LLM 摘要。

    输入：news_items 是 finnhub_client.fetch_ticker_news 的返回格式。
    输出：list[{summary, sentiment, relevance, is_hard_event}]，与 news_items 一一对应。
    """
    if not news_items:
        return []

    # 无 key / test key —— 回落
    if not settings.openai_api_key or settings.openai_api_key.startswith("sk-test"):
        return [_fallback(it) for it in news_items]

    # 只处理前 BATCH_SIZE 条
    batch = news_items[:BATCH_SIZE]
    headlines_block = "\n".join(
        f"{i + 1}. {sanitize(it.get('headline', ''))}"
        for i, it in enumerate(batch)
    )

    client = _get_client()

    try:
        response = await client.beta.chat.completions.parse(
            model=settings.openai_model,  # mini
            messages=[
                {"role": "system", "content": _SUMMARIZE_SYSTEM},
                {"role": "user", "content": f"Ticker: {ticker}\nHeadlines:\n{headlines_block}"},
            ],
            temperature=0.0,
            max_tokens=1000,
            response_format=BatchAnalysisResponse,
        )

        parsed_results = response.choices[0].message.parsed.results

        results = []
        for i, it in enumerate(batch):
            matched = next((p for p in parsed_results if p.index == i + 1), None)
            if matched:
                results.append({
                    "summary":       matched.summary,
                    "sentiment":     matched.sentiment,
                    "relevance":     matched.relevance,
                    "is_hard_event": matched.is_hard_event,
                })
            else:
                results.append(_fallback(it))
        return results

    except Exception as e:
        logger.error(f"summarize_headlines_batch({ticker}) LLM error: {e}")
        return [_fallback(it) for it in batch]


def _fallback(item: dict) -> dict:
    return {
        "summary":       (item.get("headline") or "")[:80],
        "sentiment":     "neutral",
        "relevance":     "direct",
        "is_hard_event": False,
    }
