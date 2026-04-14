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


SYSTEM_PROMPT = """You are the Bull Analyst for a quantitative trading system.

【Critical】You do NOT control capital. Do NOT output portfolio weights, dollar amounts, or
suggested_weights. The Portfolio Manager (Stage 5) assigns weights. You only argue the long side.

【Your stance】
    Build the strongest bullish case for the current portfolio strategy. Argue the long side forcefully in logic, not in position sizes.

【Inputs】
    1. research_report — market_regime, macro_outlook, ticker_signals, cross_signal_insights
    2. base_weights — context only (which names matter); do not turn this into a weight proposal

【You must】
    1. Cite concrete ticker_signals / combined_signal from research_report
    2. List core_arguments (numbered logic, data-backed)
    3. List target_tickers you favor for add/hold with bias and reason (no percentages)
    4. risk_acknowledgments: material risks and why they may be manageable

【Confidence calibration — CRITICAL】
    confidence = how strongly the DATA supports the bullish view, not rhetorical force.
    · 0.9–1.0: overwhelming supportive data
    · 0.7–0.9: solid supportive data
    · 0.5–0.7: mixed
    · 0.3–0.5: weak support
    · 0.0–0.3: data contradicts the bull case
    In bear_weak / high_vol with mostly negative signals, confidence MUST be low.

【Constraints】
    · stance: maintain or increase (intent only)
    · No weights, no deltas as portfolio fractions

【Output: JSON only】
{
  "stance": "maintain|increase",
  "confidence": <float 0.0-1.0>,
  "core_arguments": [
    "<argument with data reference>",
    "..."
  ],
  "target_tickers": [
    {
      "ticker": "<TICKER>",
      "bias": "overweight|hold",
      "reason": "<why this name, ≤200 chars>"
    }
  ],
  "risk_acknowledgments": [
    "<risk and why it may be manageable>"
  ]
}

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
        "Draft the strongest bullish case: stance, confidence, core_arguments, "
        "target_tickers (names + bias + reason), risk_acknowledgments. "
        "No portfolio weights. JSON only."
    )


def _normalize(out: dict) -> dict:
    stance = str(out.get("stance", "maintain")).strip()
    if stance not in ("maintain", "increase"):
        stance = "maintain"

    try:
        confidence = max(0.0, min(1.0, float(out.get("confidence", 0.5))))
    except (TypeError, ValueError):
        confidence = 0.5

    core = out.get("core_arguments") or out.get("arguments") or []
    if not isinstance(core, list):
        core = []
    core = [str(a).strip() for a in core if str(a).strip()][:8]

    targets = out.get("target_tickers") or []
    if not isinstance(targets, list):
        targets = []
    cleaned_targets = []
    for v in targets:
        if not isinstance(v, dict) or not v.get("ticker"):
            continue
        bias = str(v.get("bias", "hold")).strip().lower()
        if bias not in ("overweight", "hold"):
            bias = "hold"
        cleaned_targets.append({
            "ticker": str(v["ticker"]).upper().strip(),
            "bias":   bias,
            "reason": str(v.get("reason", ""))[:220],
        })

    risk_acks = out.get("risk_acknowledgments") or []
    if not isinstance(risk_acks, list):
        risk_acks = []
    risk_acks = [str(r).strip() for r in risk_acks if str(r).strip()][:5]

    return {
        "stance":               stance,
        "confidence":           confidence,
        "core_arguments":       core,
        "target_tickers":       cleaned_targets,
        "risk_acknowledgments": risk_acks,
        "failed":               False,
    }


def _degraded_output(error: str | None) -> dict:
    return {
        "stance":               "maintain",
        "confidence":           0.3,
        "core_arguments":       [f"Bull draft degraded (error={error})"],
        "target_tickers":       [],
        "risk_acknowledgments": [],
        "failed":               True,
    }
