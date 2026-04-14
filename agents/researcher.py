# agents/researcher.py
"""
Stage 3: RESEARCHER — information synthesis layer (V2.1 refactor)

V2.1 role change:
    V2 Phase 1: analyzed and decided; output adjusted_weights directly
    V2.1:       **analyze only, no decision** — structured research_report for Bull/Bear

Inputs: brief (prose + macro + per_ticker_news) + quant_baseline (scoring + base_weights)
Output: research_report (ticker_signals, macro_outlook, cross_signal_insights)

Core idea: ticker_signals bundles quant factors + news sentiment + combined signal per ticker.
Bull/Bear debate from this report instead of re-parsing raw inputs.

LLM: settings.openai_model_heavy (gpt-4o), single call, 3 retries.
Fallback: after 3 failures → degraded report with quant-only data (no news synthesis).
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


SYSTEM_PROMPT = """You are the chief market analyst (Stage 3 RESEARCHER) for a quantitative trading system.

【Your place in the pipeline】
    Upstream Stage 2 is the Python quant baseline (base_weights, scoring_breakdown).
    Downstream Stage 4a/4b are Bull/Bear; they build long/short arguments from your report.

【Task — analyze only, no decision】
    Combine quant factors + news + macro + calendar into structured signal assessments per ticker.
    Do not output position weights; output an objective market analysis report only.

【Output rules】
1. market_regime: current regime and confidence
2. macro_outlook: macro summary + upcoming key events
3. ticker_signals: quant + news combined signal per meaningful ticker
4. cross_signal_insights: cross-ticker patterns (alignment / conflict / rotation)

【combined_signal】
    strong_positive: quant_score top 30% AND news_sentiment = positive
    positive:        quant_score top 50% OR news_sentiment = positive
    neutral:         conflicting or no clear direction
    negative:        quant_score bottom 50% OR news_sentiment = negative
    strong_negative: quant_score bottom 30% AND news_sentiment = negative

【key_events (critical)】
  · Produce 3-5 event objects (not plain strings).
  · Each event MUST contain a keyword the downstream transmission matcher recognizes:
      oil surge / hormuz / middle east / opec / war / russia / ukraine / taiwan /
      rate hike / fed hawkish / cpi / pce / fomc / yields surge /
      rate cut / dovish pivot / liquidity / credit stress / vix spike /
      bank crisis / recession / pmi contraction / jobless claims /
      demand destruction / earnings recession
  · Additionally, provide context the keyword alone cannot capture:
      - "freshness": "breaking" (< 24h), "developing" (1-3 days), "ongoing" (> 3 days)
      - "magnitude": "high" / "medium" / "low" — how much market impact
      - "description": ≤ 80 chars — what specifically happened (not just the keyword)
  · If no macro events, return [{"keyword": "normal market conditions", "freshness": "ongoing", "magnitude": "low", "description": "no significant macro events"}]
  · Do NOT invent events not supported by the input data.

【Output: JSON only】
{
  "market_regime": {
    "regime": "bull_trend|bull_weak|neutral|bear_weak|bear_trend|high_vol",
    "confidence": <float 0.0-1.0>,
    "evidence": "<one sentence on the basis for the regime>"
  },
  "macro_outlook": {
    "summary": "<≤200 chars macro overview>",
    "key_events": [
      {
        "keyword": "<transmission-matchable keyword phrase>",
        "freshness": "breaking|developing|ongoing",
        "magnitude": "high|medium|low",
        "description": "<≤80 chars what specifically happened>"
      }
    ],
    "impact_bias": "positive|neutral|negative"
  },
  "ticker_signals": [
    {
      "ticker": "<TICKER>",
      "quant_score": <float>,
      "quant_rank": <int>,
      "quant_factors": "<key factors one line>",
      "news_sentiment": "positive|neutral|negative",
      "news_count": <int>,
      "news_digest": "<≤50 chars news gist>",
      "combined_signal": "strong_positive|positive|neutral|negative|strong_negative",
      "flag": "<risk or opportunity tag, or null>"
    }
  ],
  "cross_signal_insights": [
    "<cross-ticker observation 1>",
    "<cross-ticker observation 2>"
  ]
}

JSON only. Any extra text is an error."""


# ═══════════════════════════════════════════════════════════════
# Main entry
# ═══════════════════════════════════════════════════════════════


async def run_researcher_async(
    pipeline_context: dict,
    brief: dict,
    quant_baseline: dict,
) -> dict:
    """Stage 3: synthesize information from baseline + brief → research_report."""
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
                    f"[RETRY {attempt}] Previous output error: {last_error}\n\n" + user_payload
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

    # All retries failed → degraded report (quant only, no news synthesis)
    logger.error(
        f"[RESEARCHER] all retries failed, generating degraded report. last_error={last_error}"
    )
    return _degraded_report(quant_baseline, last_error)


# ═══════════════════════════════════════════════════════════════
# User message builder
# ═══════════════════════════════════════════════════════════════


def _build_user_message(brief: dict, quant_baseline: dict) -> str:
    prose    = brief.get("prose_summary") or "(none)"
    macro    = brief.get("macro_news_section") or "(none)"
    calendar = brief.get("calendar_section") or "(none)"
    key_facts = brief.get("key_facts") or {}

    base_weights    = quant_baseline.get("base_weights") or {}
    current_weights = brief.get("current_weights") or {}
    scoring         = quant_baseline.get("scoring_breakdown") or []
    ranking         = quant_baseline.get("ranking_summary") or {}

    # Per-ticker news: all articles, no cap
    per_ticker_news = brief.get("per_ticker_news") or {}
    news_block = _format_per_ticker_news(per_ticker_news)

    return (
        "## Market technicals\n"
        f"{prose}\n\n"
        "## Quantitative facts\n"
        f"{json.dumps(key_facts, ensure_ascii=False, indent=2)}\n\n"
        "## Macro news\n"
        f"{macro}\n\n"
        "## Calendar this week\n"
        f"{calendar}\n\n"
        "## Per-ticker news\n"
        f"{news_block}\n\n"
        "## Current portfolio weights (actual holdings)\n"
        f"{json.dumps(current_weights, ensure_ascii=False, indent=2)}\n\n"
        "## Python Stage 2 baseline weights (base_weights)\n"
        f"{json.dumps(base_weights, ensure_ascii=False, indent=2)}\n\n"
        "## Scoring breakdown (all tickers)\n"
        f"{json.dumps(scoring, ensure_ascii=False, indent=2)}\n\n"
        "## Ranking summary\n"
        f"{json.dumps(ranking, ensure_ascii=False, indent=2)}\n\n"
        "## Your task\n"
        "From the above, output market_regime + macro_outlook + ticker_signals +\n"
        "cross_signal_insights. Analyze only — no trading decision. JSON only."
    )


def _format_per_ticker_news(per_ticker_news: dict) -> str:
    """Format per_ticker_news into a compact text block. All articles included."""
    if not per_ticker_news:
        return "(no per-ticker news)"

    lines = []
    for ticker, news_list in sorted(per_ticker_news.items()):
        if not news_list:
            continue
        lines.append(f"### {ticker} ({len(news_list)} items)")
        for n in news_list:
            source = n.get("source", "")
            source_api = n.get("source_api", "")
            headline = n.get("headline", "")[:100]
            sentiment = n.get("sentiment", "neutral")
            tag = f"[{source}|{source_api}|{sentiment}]" if source else f"[{sentiment}]"
            summary = n.get("llm_summary") or ""
            if summary:
                lines.append(f"  {tag} {headline}")
                lines.append(f"    → {summary[:150]}")
            else:
                lines.append(f"  {tag} {headline}")

    return "\n".join(lines) if lines else "(no per-ticker news)"


# ═══════════════════════════════════════════════════════════════
# Validation + normalization
# ═══════════════════════════════════════════════════════════════

_VALID_REGIMES = {"bull_trend", "bull_weak", "neutral", "bear_weak", "bear_trend", "high_vol"}
_VALID_SIGNALS = {"strong_positive", "positive", "neutral", "negative", "strong_negative"}


def _validate_and_normalize(out: dict, quant_baseline: dict) -> dict:
    """Validate and normalize LLM output."""
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
    raw_events = mo.get("key_events") or []
    if not isinstance(raw_events, list):
        raw_events = []

    # Normalize key_events: accept both new dict format and legacy string format
    key_events_rich: list[dict] = []
    key_events_keywords: list[str] = []
    for e in raw_events[:5]:
        if isinstance(e, dict) and e.get("keyword"):
            kw = str(e["keyword"]).strip()
            key_events_rich.append({
                "keyword":     kw,
                "freshness":   str(e.get("freshness", "ongoing")).strip(),
                "magnitude":   str(e.get("magnitude", "medium")).strip(),
                "description": str(e.get("description", ""))[:100],
            })
            key_events_keywords.append(kw)
        elif isinstance(e, str) and e.strip():
            # Legacy string format fallback
            key_events_rich.append({
                "keyword":     e.strip(),
                "freshness":   "ongoing",
                "magnitude":   "medium",
                "description": e.strip(),
            })
            key_events_keywords.append(e.strip())

    if not key_events_rich:
        key_events_rich = [{
            "keyword": "normal market conditions",
            "freshness": "ongoing",
            "magnitude": "low",
            "description": "no significant macro events",
        }]
        key_events_keywords = ["normal market conditions"]

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
            "summary":      str(mo.get("summary", ""))[:300],
            "key_events":   key_events_rich,       # Rich format with freshness/magnitude
            "key_keywords": key_events_keywords,    # Flat list for transmission matcher
            "impact_bias":  impact_bias,
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
    """When all LLM retries fail: quant-only data, no news synthesis."""
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
            "evidence":   f"LLM degraded: could not synthesize news signals (error={error})",
        },
        "macro_outlook": {
            "summary":      "LLM degraded — no macro analysis",
            "key_events":   [{"keyword": "normal market conditions", "freshness": "ongoing", "magnitude": "low", "description": "no significant macro events"}],
            "key_keywords": ["normal market conditions"],
            "impact_bias":  "neutral",
        },
        "ticker_signals":        ticker_signals,
        "cross_signal_insights": [],
        "used_degraded_fallback": True,
    }
