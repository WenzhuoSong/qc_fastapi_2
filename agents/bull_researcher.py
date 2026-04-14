# agents/bull_researcher.py
"""
Stage 4a: Bull Researcher — long-side argumentation agent

Role: From RESEARCHER's research_report, build the strongest bullish case from the **long** side.
Runs in parallel with Stage 4b Bear Researcher via asyncio.gather.

Inputs: research_report + base_weights
Output: bull_output (thesis, arguments, ticker_views, suggested_weights, confidence)

Constraints:
  - Argue only maintain or increase
  - Must cite concrete ticker_signals and combined_signal from research_report
  - Do not ignore Bear-side risk flags; explain why risks are manageable

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


SYSTEM_PROMPT = """You are the Bull Analyst for a quantitative trading system.

【Your stance】
    Your job is to build the strongest bullish case for the current portfolio strategy. You must argue the long side forcefully.

【Inputs】
    You receive:
    1. research_report — market_regime, macro_outlook, ticker_signals, cross_signal_insights
    2. base_weights — Stage 2 Python quantitative baseline weights

【You must】
    1. Find quantitative evidence supporting the long view (cite concrete ticker_signals values)
    2. Emphasize growth, competitive advantage, supportive macro
    3. Assign confidence to each bullish argument
    4. Say which sectors/tickers to add to and why
    5. If a ticker has a risk flag, explain why the risk is manageable

【Confidence calibration — CRITICAL】
    confidence reflects how strongly the DATA supports your bullish view, NOT how forceful your argument is.
    · 0.9–1.0: overwhelming data — most tickers strong_positive, macro positive, no flags
    · 0.7–0.9: solid data — majority positive signals, manageable risks
    · 0.5–0.7: mixed data — some positive signals but significant headwinds
    · 0.3–0.5: weak data — few positives, mostly neutral or negative
    · 0.0–0.3: data strongly contradicts the bull case
    Be honest. If you are a bull arguing in a bear_weak regime with mostly negative signals, your confidence MUST be low (0.3–0.5).

【Constraints】
    · Recommend only maintain or increase
    · suggested_weights: all values ≥ 0, sum = 1.0, must include CASH
    · Adjust from base_weights; single position ≤ 0.20
    · You may reduce CASH (less cash, more equity)

【Output: JSON only】
{
  "stance": "maintain|increase",
  "confidence": <float 0.0-1.0>,
  "arguments": [
    "<bullish argument 1 with data>",
    "<bullish argument 2>"
  ],
  "ticker_views": [
    {
      "ticker": "<TICKER>",
      "action": "overweight|hold",
      "delta": <float>,
      "reason": "<≤40 chars>"
    }
  ],
  "suggested_weights": {"<TICKER>": <float>, "CASH": <float>},
  "risk_acknowledgments": [
    "<risk 1, why it is controllable>"
  ]
}

JSON only."""


async def run_bull_researcher_async(
    research_report: dict,
    base_weights: dict,
) -> dict:
    """Stage 4a: long-side arguments. Parallel; does not wait for Bear."""
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
                f"[BULL] done in {elapsed}s | "
                f"input_tokens={resp.usage.prompt_tokens} "
                f"output_tokens={resp.usage.completion_tokens}"
            )

            parsed = json.loads(raw)
            return _normalize(parsed, base_weights)

        except Exception as e:
            last_error = str(e)
            logger.warning(f"[BULL] attempt {attempt} failed: {e}")

    logger.error(f"[BULL] all retries failed. last_error={last_error}")
    return _degraded_output(base_weights, last_error)


def _build_user_message(research_report: dict, base_weights: dict) -> str:
    return (
        "## Research Report\n"
        f"{json.dumps(research_report, ensure_ascii=False, indent=2)}\n\n"
        "## Base Weights (Stage 2 baseline)\n"
        f"{json.dumps(base_weights, ensure_ascii=False, indent=2)}\n\n"
        "## Your task\n"
        "From the long side, build the strongest bullish case from the materials above. "
        "Output stance + confidence + arguments + ticker_views + suggested_weights. "
        "Return JSON only."
    )


def _normalize(out: dict, base_weights: dict) -> dict:
    stance = str(out.get("stance", "maintain")).strip()
    if stance not in ("maintain", "increase"):
        stance = "maintain"

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
        action = str(v.get("action", "hold")).strip()
        if action not in ("overweight", "hold"):
            action = "hold"
        cleaned_views.append({
            "ticker": str(v["ticker"]).upper().strip(),
            "action": action,
            "delta":  _safe_float(v.get("delta"), 0.0),
            "reason": str(v.get("reason", ""))[:80],
        })

    suggested = out.get("suggested_weights") or {}
    if not isinstance(suggested, dict) or not suggested:
        suggested = dict(base_weights)

    risk_acks = out.get("risk_acknowledgments") or []
    if not isinstance(risk_acks, list):
        risk_acks = []
    risk_acks = [str(r).strip() for r in risk_acks if str(r).strip()][:3]

    return {
        "stance":                stance,
        "confidence":            confidence,
        "arguments":             arguments,
        "ticker_views":          cleaned_views,
        "suggested_weights":     suggested,
        "risk_acknowledgments":  risk_acks,
        "failed":                False,
    }


def _degraded_output(base_weights: dict, error: str | None) -> dict:
    return {
        "stance":                "maintain",
        "confidence":            0.3,
        "arguments":             [f"Bull LLM degraded (error={error})"],
        "ticker_views":          [],
        "suggested_weights":     dict(base_weights),
        "risk_acknowledgments":  [],
        "failed":                True,
    }


def _safe_float(val, default: float) -> float:
    try:
        return float(val) if val is not None else default
    except (TypeError, ValueError):
        return default
