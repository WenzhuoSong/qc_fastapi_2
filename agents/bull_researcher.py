# agents/bull_researcher.py
"""
Stage 4a (draft): Bull Researcher — long-side argumentation only (no weights).

Role: From RESEARCHER's research_report, build the strongest bullish case.
Does not allocate capital; Stage 5 PM sets adjusted_weights.

Outputs: stance, confidence, core_arguments, target_tickers, risk_acknowledgments.
Parallel with Bear draft; Stage 4c cross-exam and Stage 5 PM follow.

LLM: settings.openai_model_heavy (gpt-4o)
"""
from __future__ import annotations

import json
import logging
import time

from openai import AsyncOpenAI

from config import get_settings

logger = logging.getLogger("qc_fastapi_2.bull_researcher")
settings = get_settings()

_client: AsyncOpenAI | None = None


def _get_client() -> AsyncOpenAI:
    global _client
    if _client is None:
        _client = AsyncOpenAI(api_key=settings.openai_api_key)
    return _client


BULL_OUTPUT_SCHEMA = """
{
  "overall_stance": "bullish" | "cautious_bullish" | "neutral",
  "overall_confidence": "high" | "medium" | "low",
  "thesis_summary": "one-sentence core thesis (≤50 chars)",
  "ticker_views": {
    "<TICKER>": {
      "direction": "overweight" | "hold" | "underweight",
      "magnitude": "strong" | "moderate" | "slight",
      "confidence": "high" | "medium" | "low",
      "primary_reason": "single most important reason (≤30 chars)",
      "key_risk": "single biggest counter-argument risk (≤30 chars)"
    }
  },
  "top_3_conviction": ["TICKER1", "TICKER2", "TICKER3"],
  "macro_tailwinds": ["bullish factor 1", "bullish factor 2"],
  "conflicting_signals": [
    {
      "ticker": "TICKER",
      "signal_a": "technically strong",
      "signal_b": "fundamentally weakening",
      "resolution": "bullish short-term, uncertain medium-term"
    }
  ]
}

ticker_views: include only tickers with a clear view; omit uncertain tickers (do not pad with "hold").
magnitude: strong=±5-10%, moderate=±3-5%, slight=±1-3%
"""

SYSTEM_PROMPT = f"""You are the Bull Analyst for a quantitative trading system.

【Critical】You do NOT control capital. Do NOT output portfolio weights, dollar amounts, or
suggested_weights. The Portfolio Manager (Stage 5) assigns weights. You only argue the long side.

【Your stance】
    Build the strongest bullish case for the current portfolio strategy. Argue the long side forcefully in logic, not in position sizes.

【Inputs】
    1. research_report — market_regime, macro_outlook, ticker_signals, cross_signal_insights
    2. base_weights — context only (which names matter); do not turn this into a weight proposal

【You must】
    1. Each core argument MUST cite specific fields from research_report (e.g., ticker_signals[].combined_signal, market_regime.regime, macro_outlook.impact_bias, cross_signal_insights[])
    2. Provide overall_stance (bullish/cautious_bullish/neutral)
    3. Provide overall_confidence (high/medium/low)
    4. Write a concise thesis_summary (≤50 chars)
    5. For each ticker with clear view: provide ticker_views entry with direction, magnitude, confidence, primary_reason, key_risk
    6. List top_3_conviction tickers you are most confident about
    7. List macro_tailwinds (bullish factors)
    8. Note any conflicting_signals where indicators disagree

【Confidence calibration — CRITICAL】
    overall_confidence = high/medium/low reflects how strongly the DATA supports the bullish view.
    In bear_weak / high_vol with mostly negative signals, overall_confidence MUST be low.

【Constraints】
    · Only include tickers with clear views; omit uncertain ones
    · No weights, no deltas as portfolio fractions

【Output: JSON only — follow this schema exactly】
{BULL_OUTPUT_SCHEMA}

JSON only."""


async def run_bull_researcher_async(
    research_report: dict,
    base_weights: dict,
) -> dict:
    """Stage 4a draft: long-side arguments only (no weights)."""
    user_payload = _build_user_message(research_report, base_weights)

    client = _get_client()
    model  = settings.openai_model_heavy

    last_error: str | None = None
    for attempt in range(2):
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
                temperature=0.2,
                max_tokens=1200,
                response_format={"type": "json_object"},
            )
            raw = resp.choices[0].message.content or ""
            elapsed = round(time.time() - t0, 2)
            logger.info(
                f"[BULL] done in {elapsed}s | "
                f"input_tokens={resp.usage.prompt_tokens} "
                f"output_tokens={resp.usage.completion_tokens}"
            )

            parsed = json.loads(raw)
            return _normalize(parsed)

        except Exception as e:
            last_error = str(e)
            logger.warning(f"[BULL] attempt {attempt} failed: {e}")

    logger.error(f"[BULL] all retries failed. last_error={last_error}")
    return _degraded_output(last_error)


def _build_user_message(research_report: dict, base_weights: dict) -> str:
    return (
        "## Research Report\n"
        f"{json.dumps(research_report, ensure_ascii=False, indent=2)}\n\n"
        "## Base weights (context only — do not output weights)\n"
        f"{json.dumps(base_weights, ensure_ascii=False, indent=2)}\n\n"
        "## Your task\n"
        "Draft the strongest bullish case: overall_stance, overall_confidence, thesis_summary, "
        "ticker_views (per-ticker direction + magnitude + confidence + primary_reason + key_risk), "
        "top_3_conviction, macro_tailwinds, conflicting_signals. "
        "No portfolio weights. JSON only."
    )


def _normalize(out: dict) -> dict:
    overall_stance = str(out.get("overall_stance", "neutral")).strip()
    if overall_stance not in ("bullish", "cautious_bullish", "neutral"):
        overall_stance = "neutral"

    overall_confidence = str(out.get("overall_confidence", "medium")).strip()
    if overall_confidence not in ("high", "medium", "low"):
        overall_confidence = "medium"

    thesis_summary = str(out.get("thesis_summary", ""))[:100]

    ticker_views = out.get("ticker_views") or {}
    if not isinstance(ticker_views, dict):
        ticker_views = {}
    cleaned_views = {}
    for ticker, view in ticker_views.items():
        if not isinstance(view, dict):
            continue
        direction = str(view.get("direction", "hold")).strip().lower()
        if direction not in ("overweight", "hold", "underweight"):
            direction = "hold"
        magnitude = str(view.get("magnitude", "moderate")).strip().lower()
        if magnitude not in ("strong", "moderate", "slight"):
            magnitude = "moderate"
        conf = str(view.get("confidence", "medium")).strip().lower()
        if conf not in ("high", "medium", "low"):
            conf = "medium"
        cleaned_views[str(ticker).upper().strip()] = {
            "direction": direction,
            "magnitude": magnitude,
            "confidence": conf,
            "primary_reason": str(view.get("primary_reason", ""))[:60],
            "key_risk": str(view.get("key_risk", ""))[:60],
        }

    top_3 = out.get("top_3_conviction") or []
    if not isinstance(top_3, list):
        top_3 = []
    top_3 = [str(t).upper().strip() for t in top_3 if str(t).strip()][:3]

    macro_tailwinds = out.get("macro_tailwinds") or []
    if not isinstance(macro_tailwinds, list):
        macro_tailwinds = []
    macro_tailwinds = [str(w).strip() for w in macro_tailwinds if str(w).strip()][:5]

    conflicting_signals = out.get("conflicting_signals") or []
    if not isinstance(conflicting_signals, list):
        conflicting_signals = []
    cleaned_signals = []
    for sig in conflicting_signals:
        if isinstance(sig, dict) and sig.get("ticker"):
            cleaned_signals.append({
                "ticker": str(sig["ticker"]).upper().strip(),
                "signal_a": str(sig.get("signal_a", ""))[:80],
                "signal_b": str(sig.get("signal_b", ""))[:80],
                "resolution": str(sig.get("resolution", ""))[:120],
            })
    conflicting_signals = cleaned_signals[:3]

    return {
        "overall_stance":       overall_stance,
        "overall_confidence":   overall_confidence,
        "thesis_summary":       thesis_summary,
        "ticker_views":         cleaned_views,
        "top_3_conviction":      top_3,
        "macro_tailwinds":      macro_tailwinds,
        "conflicting_signals":  conflicting_signals,
        "failed":              False,
    }


def _degraded_output(error: str | None) -> dict:
    return {
        "overall_stance":       "neutral",
        "overall_confidence":   "low",
        "thesis_summary":      f"Bull draft degraded (error={error})",
        "ticker_views":        {},
        "top_3_conviction":    [],
        "macro_tailwinds":     [],
        "conflicting_signals": [],
        "failed":             True,
    }
