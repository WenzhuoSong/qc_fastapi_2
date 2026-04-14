# agents/bear_researcher.py
"""
Stage 4b (draft): Bear Researcher — short-side argumentation only (no weights).

Role: From RESEARCHER's research_report, build the strongest risk case.
Does not allocate capital; Stage 5 PM sets adjusted_weights.

Outputs: stance, confidence, core_arguments, target_tickers, opportunity_acknowledgments.
Parallel with Bull draft; Stage 4c cross-exam and Stage 5 PM follow.

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

【Critical】You do NOT control capital. Do NOT output portfolio weights or suggested_weights.
The Portfolio Manager (Stage 5) assigns weights. You only argue risks and the short/defensive view.

【Your stance】
    Build the strongest risk warning for the current market. Argue with data, not fabricated position sizes.

【Inputs】
    1. research_report — market_regime, macro_outlook, ticker_signals, cross_signal_insights
    2. base_weights — context only; not a template for your allocation

【You must】
    1. Each core argument MUST cite specific fields from research_report (e.g., ticker_signals[].combined_signal, market_regime.regime, macro_outlook.impact_bias, cross_signal_insights[])
    2. core_arguments: data-backed risk points
    3. target_tickers: names to trim, avoid, or underweight with reason (no percentages)
    4. opportunity_acknowledgments: acknowledge potential upside risks or opportunities that could limit downside
    5. Do not "guess" the Bull's text — a later cross-exam stage will address the Bull draft

【Confidence calibration — CRITICAL】
    confidence = how strongly the DATA supports the bearish view.
    · 0.9–1.0: overwhelming risk evidence
    · 0.7–0.9: solid risk evidence
    · 0.5–0.7: mixed
    · 0.3–0.5: weak risk evidence
    · 0.0–0.3: data contradicts the bear case
    In bull_trend with mostly positive signals, confidence MUST be low.

【Constraints】
    · stance: reduce or defensive (intent only)
    · No weights

【Output: JSON only】
{
  "stance": "reduce|defensive",
  "confidence": <float 0.0-1.0>,
  "core_arguments": [
    "<risk argument with data>",
    "..."
  ],
  "target_tickers": [
    {
      "ticker": "<TICKER>",
      "bias": "underweight|trim|avoid",
      "reason": "<why, ≤200 chars>"
    }
  ],
  "opportunity_acknowledgments": [
    "<potential upside or opportunity that may limit downside>"
  ]
}

JSON only."""


async def run_bear_researcher_async(
    research_report: dict,
    base_weights: dict,
) -> dict:
    """Stage 4b draft: short-side arguments only (no weights)."""
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
                f"[BEAR] done in {elapsed}s | "
                f"input_tokens={resp.usage.prompt_tokens} "
                f"output_tokens={resp.usage.completion_tokens}"
            )

            parsed = json.loads(raw)
            return _normalize(parsed)

        except Exception as e:
            last_error = str(e)
            logger.warning(f"[BEAR] attempt {attempt} failed: {e}")

    logger.error(f"[BEAR] all retries failed. last_error={last_error}")
    return _degraded_output(last_error)


def _build_user_message(research_report: dict, base_weights: dict) -> str:
    return (
        "## Research Report\n"
        f"{json.dumps(research_report, ensure_ascii=False, indent=2)}\n\n"
        "## Base weights (context only — do not output weights)\n"
        f"{json.dumps(base_weights, ensure_ascii=False, indent=2)}\n\n"
        "## Your task\n"
        "Draft the strongest risk case: stance, confidence, core_arguments, "
        "target_tickers (names + bias + reason), opportunity_acknowledgments. "
        "No portfolio weights. JSON only."
    )


def _normalize(out: dict) -> dict:
    stance = str(out.get("stance", "reduce")).strip()
    if stance not in ("reduce", "defensive"):
        stance = "reduce"

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
        bias = str(v.get("bias", "underweight")).strip().lower()
        if bias not in ("underweight", "trim", "avoid"):
            bias = "underweight"
        cleaned_targets.append({
            "ticker": str(v["ticker"]).upper().strip(),
            "bias":   bias,
            "reason": str(v.get("reason", ""))[:220],
        })

    opp_acks = out.get("opportunity_acknowledgments") or []
    if not isinstance(opp_acks, list):
        opp_acks = []
    opp_acks = [str(r).strip() for r in opp_acks if str(r).strip()][:5]

    return {
        "stance":                    stance,
        "confidence":                confidence,
        "core_arguments":            core,
        "target_tickers":            cleaned_targets,
        "opportunity_acknowledgments": opp_acks,
        "failed":                    False,
    }


def _degraded_output(error: str | None) -> dict:
    return {
        "stance":                    "defensive",
        "confidence":                0.3,
        "core_arguments":            [f"Bear draft degraded (error={error})"],
        "target_tickers":            [],
        "opportunity_acknowledgments": [],
        "failed":                    True,
    }
