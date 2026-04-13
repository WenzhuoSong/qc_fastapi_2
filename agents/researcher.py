# agents/researcher.py
"""
Stage 3: RESEARCHER —— 信息合成层（V2.1 重构）

V2.1 的职责变化：
    V2 Phase 1: 又分析又决策，直接输出 adjusted_weights
    V2.1:       **只分析不决策**，输出结构化 research_report 供 Bull/Bear 消费

输入：brief（prose + macro + per_ticker_news）+ quant_baseline（scoring + base_weights）
输出：research_report（ticker_signals, macro_outlook, cross_signal_insights）

核心创新：ticker_signals 把每个 ticker 的量化因子 + 新闻情绪 + 综合信号打在一起。
Bull/Bear 不需要各自从头解析原始数据，直接基于这份报告辩论。

LLM: settings.openai_model_heavy (gpt-4o)，单次调用，3 次重试。
容错：3 次重试均失败 → 生成仅含 quant 数据的降级报告（无 news 合成）。
"""
from __future__ import annotations

import json
import logging
import time

from openai import AsyncOpenAI

from config import get_settings

logger = logging.getLogger("qc_fastapi_2.researcher")
settings = get_settings()

_client: AsyncOpenAI | None = None


def _get_client() -> AsyncOpenAI:
    global _client
    if _client is None:
        _client = AsyncOpenAI(api_key=settings.openai_api_key)
    return _client


SYSTEM_PROMPT = """你是量化交易系统的首席市场分析师（Stage 3 RESEARCHER）。

【你的位置】
    上游 Stage 2 是 Python 量化基准层，已算好 base_weights 和 scoring_breakdown。
    下游 Stage 4a/4b 是 Bull/Bear 辩论层，它们将基于你的报告构建多空论点。

【你的任务 —— 只分析，不决策】
    综合量化因子 + 新闻 + 宏观 + 日程，为每个 ticker 产出结构化的综合信号评估。
    你不做仓位调整，不输出 weights，只输出一份客观的市场分析报告。

【输出规则】
1. market_regime: 判断当前市场制度和置信度
2. macro_outlook: 宏观环境总结 + 未来关键事件
3. ticker_signals: 每个有意义的 ticker 的量化+新闻综合信号
4. cross_signal_insights: 跨 ticker 的模式观察（共振/矛盾/轮动）

【combined_signal 取值规则】
    strong_positive: quant_score top 30% AND news_sentiment = positive
    positive:        quant_score top 50% OR news_sentiment = positive
    neutral:         信号矛盾或无明确方向
    negative:        quant_score bottom 50% OR news_sentiment = negative
    strong_negative: quant_score bottom 30% AND news_sentiment = negative

【key_events 规则（至关重要！）】
  · 必须产出 3-5 条短语，每条 ≤ 60 字。
  · 必须使用下游 transmission 匹配器能识别的关键字：
      oil surge / hormuz / middle east / opec / war / russia / ukraine / taiwan /
      rate hike / fed hawkish / cpi / pce / fomc / yields surge /
      rate cut / dovish pivot / liquidity / credit stress / vix spike /
      bank crisis / recession / pmi contraction / jobless claims /
      demand destruction / earnings recession
  · 没有宏观事件时返回 ["normal market conditions"]，不要编造。

【必须输出纯 JSON】
{
  "market_regime": {
    "regime": "bull_trend|bull_weak|neutral|bear_weak|bear_trend|high_vol",
    "confidence": <float 0.0-1.0>,
    "evidence": "<一句话解释判断依据>"
  },
  "macro_outlook": {
    "summary": "<≤200 字宏观概要>",
    "key_events": ["event phrase 1", "event phrase 2", ...],
    "impact_bias": "positive|neutral|negative"
  },
  "ticker_signals": [
    {
      "ticker": "<TICKER>",
      "quant_score": <float>,
      "quant_rank": <int>,
      "quant_factors": "<关键因子一行>",
      "news_sentiment": "positive|neutral|negative",
      "news_count": <int>,
      "news_digest": "<≤50 字新闻要点>",
      "combined_signal": "strong_positive|positive|neutral|negative|strong_negative",
      "flag": "<风险或机会标记，无则 null>"
    }
  ],
  "cross_signal_insights": [
    "<跨 ticker 观察 1>",
    "<跨 ticker 观察 2>"
  ]
}

仅输出 JSON。任何额外文本都会被视为错误。"""


# ═══════════════════════════════════════════════════════════════
# 主入口
# ═══════════════════════════════════════════════════════════════


async def run_researcher_async(
    pipeline_context: dict,
    brief: dict,
    quant_baseline: dict,
) -> dict:
    """Stage 3: 信息合成。消费 baseline + brief，产出 research_report。"""
    user_payload = _build_user_message(brief, quant_baseline)

    client = _get_client()
    model  = settings.openai_model_heavy

    last_error: str | None = None
    for attempt in range(3):
        t0 = time.time()
        try:
            messages = [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user",   "content": user_payload},
            ]
            if attempt > 0 and last_error:
                messages[1]["content"] = (
                    f"[RETRY {attempt}] 上次输出错误: {last_error}\n\n" + user_payload
                )

            resp = await client.chat.completions.create(
                model=model,
                messages=messages,
                temperature=0.0,
                max_tokens=3000,
                response_format={"type": "json_object"},
            )
            raw = resp.choices[0].message.content or ""
            elapsed = round(time.time() - t0, 2)
            logger.info(
                f"[RESEARCHER] done in {elapsed}s | "
                f"input_tokens={resp.usage.prompt_tokens} "
                f"output_tokens={resp.usage.completion_tokens}"
            )

            parsed = json.loads(raw)
            return _validate_and_normalize(parsed, quant_baseline)

        except Exception as e:
            last_error = str(e)
            logger.warning(f"[RESEARCHER] attempt {attempt} failed: {e}")

    # 所有重试失败 → 降级报告（仅含 quant 数据，无 news 合成）
    logger.error(
        f"[RESEARCHER] all retries failed, generating degraded report. last_error={last_error}"
    )
    return _degraded_report(quant_baseline, last_error)


# ═══════════════════════════════════════════════════════════════
# User message builder
# ═══════════════════════════════════════════════════════════════


def _build_user_message(brief: dict, quant_baseline: dict) -> str:
    prose    = brief.get("prose_summary") or "(无)"
    macro    = brief.get("macro_news_section") or "(无)"
    calendar = brief.get("calendar_section") or "(无)"
    key_facts = brief.get("key_facts") or {}

    base_weights = quant_baseline.get("base_weights") or {}
    scoring      = quant_baseline.get("scoring_breakdown") or []
    ranking      = quant_baseline.get("ranking_summary") or {}

    # 裁剪 scoring breakdown 到前 15 条
    top_scored = scoring[:15]

    # 格式化 per_ticker_news 为紧凑文本
    per_ticker_news = brief.get("per_ticker_news") or {}
    news_block = _format_per_ticker_news(per_ticker_news)

    return (
        "## 市场技术面\n"
        f"{prose}\n\n"
        "## 定量指标\n"
        f"{json.dumps(key_facts, ensure_ascii=False, indent=2)}\n\n"
        "## 宏观新闻\n"
        f"{macro}\n\n"
        "## 本周日程\n"
        f"{calendar}\n\n"
        "## 个股新闻（按 ticker）\n"
        f"{news_block}\n\n"
        "## Python Stage 2 基准仓位 (base_weights)\n"
        f"{json.dumps(base_weights, ensure_ascii=False, indent=2)}\n\n"
        "## 基准打分明细 (top 15)\n"
        f"{json.dumps(top_scored, ensure_ascii=False, indent=2)}\n\n"
        "## 基准排名\n"
        f"{json.dumps(ranking, ensure_ascii=False, indent=2)}\n\n"
        "## 你的任务\n"
        "综合以上材料，输出 market_regime + macro_outlook + ticker_signals +\n"
        "cross_signal_insights。只分析不决策，仅返回纯 JSON。"
    )


def _format_per_ticker_news(per_ticker_news: dict) -> str:
    """把 per_ticker_news 格式化为紧凑文本块。"""
    if not per_ticker_news:
        return "(无个股新闻)"

    lines = []
    for ticker, news_list in sorted(per_ticker_news.items()):
        if not news_list:
            continue
        lines.append(f"### {ticker} ({len(news_list)} 条)")
        for n in news_list[:3]:  # 每个 ticker 最多 3 条
            source = n.get("source", "")
            source_api = n.get("source_api", "")
            headline = n.get("headline", "")[:80]
            sentiment = n.get("sentiment", "neutral")
            tag = f"[{source}|{source_api}|{sentiment}]" if source else f"[{sentiment}]"
            summary = n.get("llm_summary") or ""
            if summary:
                lines.append(f"  {tag} {headline}")
                lines.append(f"    → {summary[:100]}")
            else:
                lines.append(f"  {tag} {headline}")

    return "\n".join(lines) if lines else "(无个股新闻)"


# ═══════════════════════════════════════════════════════════════
# Validation + normalization
# ═══════════════════════════════════════════════════════════════

_VALID_REGIMES = {"bull_trend", "bull_weak", "neutral", "bear_weak", "bear_trend", "high_vol"}
_VALID_SIGNALS = {"strong_positive", "positive", "neutral", "negative", "strong_negative"}


def _validate_and_normalize(out: dict, quant_baseline: dict) -> dict:
    """验证并规范化 LLM 输出。"""
    # market_regime
    mr = out.get("market_regime") or {}
    regime = str(mr.get("regime", "neutral")).strip()
    if regime not in _VALID_REGIMES:
        regime = "neutral"
    try:
        confidence = max(0.0, min(1.0, float(mr.get("confidence", 0.5))))
    except (TypeError, ValueError):
        confidence = 0.5

    # macro_outlook
    mo = out.get("macro_outlook") or {}
    key_events = mo.get("key_events") or []
    if not isinstance(key_events, list):
        key_events = []
    key_events = [str(e).strip() for e in key_events if str(e).strip()][:5]
    if not key_events:
        key_events = ["normal market conditions"]
    impact_bias = str(mo.get("impact_bias", "neutral")).strip()
    if impact_bias not in ("positive", "neutral", "negative"):
        impact_bias = "neutral"

    # ticker_signals
    raw_signals = out.get("ticker_signals") or []
    if not isinstance(raw_signals, list):
        raw_signals = []
    ticker_signals = []
    for sig in raw_signals:
        if not isinstance(sig, dict) or not sig.get("ticker"):
            continue
        combined = str(sig.get("combined_signal", "neutral")).strip()
        if combined not in _VALID_SIGNALS:
            combined = "neutral"
        ticker_signals.append({
            "ticker":          str(sig["ticker"]).upper().strip(),
            "quant_score":     _safe_float(sig.get("quant_score"), 0.0),
            "quant_rank":      int(sig.get("quant_rank", 0) or 0),
            "quant_factors":   str(sig.get("quant_factors", ""))[:120],
            "news_sentiment":  str(sig.get("news_sentiment", "neutral")).strip(),
            "news_count":      int(sig.get("news_count", 0) or 0),
            "news_digest":     str(sig.get("news_digest", ""))[:100],
            "combined_signal": combined,
            "flag":            sig.get("flag") or None,
        })

    # cross_signal_insights
    insights = out.get("cross_signal_insights") or []
    if not isinstance(insights, list):
        insights = []
    insights = [str(i).strip() for i in insights if str(i).strip()][:5]

    return {
        "market_regime": {
            "regime":     regime,
            "confidence": confidence,
            "evidence":   str(mr.get("evidence", ""))[:200],
        },
        "macro_outlook": {
            "summary":     str(mo.get("summary", ""))[:300],
            "key_events":  key_events,
            "impact_bias": impact_bias,
        },
        "ticker_signals":        ticker_signals,
        "cross_signal_insights": insights,
        "used_degraded_fallback": False,
    }


def _safe_float(val, default: float) -> float:
    try:
        return float(val) if val is not None else default
    except (TypeError, ValueError):
        return default


def _degraded_report(quant_baseline: dict, error: str | None) -> dict:
    """LLM 全部重试失败时的降级报告：只有 quant 数据，无 news 合成。"""
    scoring = quant_baseline.get("scoring_breakdown") or []
    ticker_signals = []
    for i, item in enumerate(scoring):
        if not isinstance(item, dict):
            continue
        ticker = str(item.get("ticker", "")).upper()
        if not ticker:
            continue
        ticker_signals.append({
            "ticker":          ticker,
            "quant_score":     _safe_float(item.get("score"), 0.0),
            "quant_rank":      i + 1,
            "quant_factors":   str(item.get("factors", ""))[:120],
            "news_sentiment":  "neutral",
            "news_count":      0,
            "news_digest":     "",
            "combined_signal": "neutral",
            "flag":            None,
        })

    return {
        "market_regime": {
            "regime":     "neutral",
            "confidence": 0.3,
            "evidence":   f"LLM 降级：无法合成新闻信号 (error={error})",
        },
        "macro_outlook": {
            "summary":     "LLM 降级，无宏观分析",
            "key_events":  ["normal market conditions"],
            "impact_bias": "neutral",
        },
        "ticker_signals":        ticker_signals,
        "cross_signal_insights": [],
        "used_degraded_fallback": True,
    }
