# agents/bear_researcher.py
"""
Stage 4b: Bear Researcher — short-side argumentation agent

Role: From RESEARCHER's research_report, build the strongest risk warning from the **short** side.
Runs in parallel with Stage 4a Bull Researcher via asyncio.gather.

Inputs: research_report + base_weights
Output: bear_output (thesis, arguments, ticker_views, suggested_weights, confidence)

Constraints:
  - Argue only reduce or defensive
  - Must cite concrete flags, risk, news_sentiment=negative from research_report
  - Do not ignore Bull-side positive signals; explain why they may be unreliable

LLM: settings.openai_model_heavy (gpt-4o)
"""
from __future__ import annotations

import json
import logging
import time

from openai import AsyncOpenAI

from config import get_settings

logger = logging.getLogger("qc_fastapi_2.bear_researcher")
settings = get_settings()

_client: AsyncOpenAI | None = None


def _get_client() -> AsyncOpenAI:
    global _client
    if _client is None:
        _client = AsyncOpenAI(api_key=settings.openai_api_key)
    return _client


SYSTEM_PROMPT = """You are the Bear Analyst for a quantitative trading system.

【Your stance】
    Your job is to build the strongest risk warning for the current market. You must argue the short side forcefully.

【Inputs】
    You receive:
    1. research_report — market_regime, macro_outlook, ticker_signals, cross_signal_insights
    2. base_weights — Stage 2 Python quantitative baseline weights

【You must】
    1. Find all risk signals and negative indicators (cite concrete ticker_signals values)
    2. Emphasize drawdown risk, overheating, macro threats
    3. Assign confidence to each bearish argument
    4. Say which sectors/tickers to trim or avoid and why
    5. If a ticker has a positive combined_signal, explain why it may be unreliable

【Confidence calibration — CRITICAL】
    confidence reflects how strongly the DATA supports your bearish view, NOT how forceful your argument is.
    · 0.9–1.0: overwhelming data — most tickers negative/strong_negative, macro negative, multiple flags
    · 0.7–0.9: solid data — majority negative signals, clear macro headwinds
    · 0.5–0.7: mixed data — some risk signals but also positive factors
    · 0.3–0.5: weak data — few negatives, mostly neutral or positive
    · 0.0–0.3: data strongly contradicts the bear case
    Be honest. If you are a bear arguing in a bull_trend regime with mostly positive signals, your confidence MUST be low (0.3–0.5).

【Constraints】
    · Recommend only reduce or defensive
    · suggested_weights: all values ≥ 0, sum = 1.0, must include CASH
    · Adjust from base_weights; single position ≤ 0.20
    · Prefer higher CASH (larger cash buffer)

【Output: JSON only】
{
  "stance": "reduce|defensive",
  "confidence": <float 0.0-1.0>,
  "arguments": [
    "<bearish argument 1 with data>",
    "<bearish argument 2>"
  ],
  "ticker_views": [
    {
      "ticker": "<TICKER>",
      "action": "underweight|trim|avoid",
      "delta": <float>,
      "reason": "<≤40 chars>"
    }
  ],
  "suggested_weights": {"<TICKER>": <float>, "CASH": <float>},
  "bullish_rebuttals": [
    "<rebuttal to a positive signal>"
  ]
}

JSON only."""


async def run_bear_researcher_async(
    research_report: dict,
    base_weights: dict,
) -> dict:
    """Stage 4b: short-side arguments. Parallel; does not wait for Bull."""
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
                max_tokens=1500,
                response_format={"type": "json_object"},
            )
            raw = resp.choices[0].message.content or ""
            elapsed = round(time.time() - t0, 2)
            logger.info(
                f"[BEAR] done in {elapsed}s | "
                f"input_tokens={resp.usage.prompt_tokens} "
                f"output_tokens={resp.usage.completion_tokens}"
            )

            parsed = json.loads(raw)
            return _normalize(parsed, base_weights)

        except Exception as e:
            last_error = str(e)
            logger.warning(f"[BEAR] attempt {attempt} failed: {e}")

    logger.error(f"[BEAR] all retries failed. last_error={last_error}")
    return _degraded_output(base_weights, last_error)


def _build_user_message(research_report: dict, base_weights: dict) -> str:
    return (
        "## Research Report\n"
        f"{json.dumps(research_report, ensure_ascii=False, indent=2)}\n\n"
        "## Base Weights (Stage 2 baseline)\n"
        f"{json.dumps(base_weights, ensure_ascii=False, indent=2)}\n\n"
        "## Your task\n"
        "From the short side, build the strongest risk case from the materials above. "
        "Output stance + confidence + arguments + ticker_views + suggested_weights. "
        "Return JSON only."
    )


def _normalize(out: dict, base_weights: dict) -> dict:
    stance = str(out.get("stance", "reduce")).strip()
    if stance not in ("reduce", "defensive"):
        stance = "reduce"

    try:
        confidence = max(0.0, min(1.0, float(out.get("confidence", 0.5))))
    except (TypeError, ValueError):
        confidence = 0.5

    arguments = out.get("arguments") or []
    if not isinstance(arguments, list):
        arguments = []
    arguments = [str(a).strip() for a in arguments if str(a).strip()][:5]

    ticker_views = out.get("ticker_views") or []
    if not isinstance(ticker_views, list):
        ticker_views = []
    cleaned_views = []
    for v in ticker_views:
        if not isinstance(v, dict) or not v.get("ticker"):
            continue
        action = str(v.get("action", "underweight")).strip()
        if action not in ("underweight", "trim", "avoid"):
            action = "underweight"
        cleaned_views.append({
            "ticker": str(v["ticker"]).upper().strip(),
            "action": action,
            "delta":  _safe_float(v.get("delta"), 0.0),
            "reason": str(v.get("reason", ""))[:80],
        })

    suggested = out.get("suggested_weights") or {}
    if not isinstance(suggested, dict) or not suggested:
        # Degraded: bump CASH to 0.30
        suggested = dict(base_weights)
        suggested["CASH"] = 0.30

    rebuttals = out.get("bullish_rebuttals") or []
    if not isinstance(rebuttals, list):
        rebuttals = []
    rebuttals = [str(r).strip() for r in rebuttals if str(r).strip()][:3]

    return {
        "stance":              stance,
        "confidence":          confidence,
        "arguments":           arguments,
        "ticker_views":        cleaned_views,
        "suggested_weights":   suggested,
        "bullish_rebuttals":   rebuttals,
        "failed":              False,
    }


def _degraded_output(base_weights: dict, error: str | None) -> dict:
    defensive_weights = dict(base_weights)
    defensive_weights["CASH"] = 0.30
    return {
        "stance":              "defensive",
        "confidence":          0.3,
        "arguments":           [f"Bear LLM degraded (error={error})"],
        "ticker_views":        [],
        "suggested_weights":   defensive_weights,
        "bullish_rebuttals":   [],
        "failed":              True,
    }


def _safe_float(val, default: float) -> float:
    try:
        return float(val) if val is not None else default
    except (TypeError, ValueError):
        return default
