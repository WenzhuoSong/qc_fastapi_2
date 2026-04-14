# agents/synthesizer.py
"""
Stage 5: Portfolio Manager (PM / Judge) — sole owner of adjusted_weights.

Bull and Bear only argue (draft + cross-exam). This stage:
    - Reads research_report + base_weights + full debate record (including rebuttals)
    - Produces final adjusted_weights (sum = 1.0) and decision_rationale

Output remains compatible with legacy researcher_out; Risk MGR unchanged.

LLM: settings.openai_model_heavy (gpt-4o)
"""
from __future__ import annotations

import json
import logging
import time

from openai import AsyncOpenAI

from config import get_settings

logger = logging.getLogger("qc_fastapi_2.synthesizer")
settings = get_settings()

_client: AsyncOpenAI | None = None


def _get_client() -> AsyncOpenAI:
    global _client
    if _client is None:
        _client = AsyncOpenAI(api_key=settings.openai_api_key)
    return _client


SYSTEM_PROMPT = """You are the Portfolio Manager (PM) / Judge for a quantitative trading system.

Your goal is to maximize risk-adjusted returns (Sharpe) and protect capital. You alone assign
portfolio weights — Bull and Bear only provide arguments; they did not output weights.

You receive:
  · Stage 3 research_report (authoritative data)
  · Stage 2 base_weights (quantitative baseline)
  · Bull draft + Bear draft + cross-examination rebuttals (who attacked whom)

【How to judge】
    · Anchor evidence in research_report (ticker_signals, macro_outlook). Do not invent facts.
    · Evaluate logical rigor and data backing — not rhetorical volume.
    · Do NOT mechanically average Bull vs Bear confidence. If Bear's logic is irrefutable given the data,
      lean heavily into cash and defense. If Bull's logic prevails, allow momentum — within constraints.
    · Cross-exam rebuttals exist to expose weak claims; use them when deciding uncertainty.

【Constraints (hard)】
    · adjusted_weights must sum to 1.0, all non-negative, include CASH
    · Single name ≤ max_single_position; respect min_cash_pct
    · Typical deviation from base_weights ±5%; up to ±10% only with explicit justification in weight_adjustments.reason
    · If Bull/Bear confidences are both middling and close, set uncertainty_flag=true and prefer conservative weights

【regime (exactly one of 6)】
    bull_trend / bull_weak / neutral / bear_weak / bear_trend / high_vol

【recommended_stance (exactly one of 5)】
    buy / overweight / maintain / underweight / sell

【key_events】
    Prefer research_report.macro_outlook.key_keywords (or key_events) for transmission matcher keywords

【Output: JSON only】
{
  "market_judgment": {
    "regime": "bull_trend|bull_weak|neutral|bear_weak|bear_trend|high_vol",
    "adjusted_confidence": <float 0.0-1.0>,
    "uncertainty_flag": <bool>
  },
  "recommended_stance": "buy|overweight|maintain|underweight|sell",
  "adjusted_weights": {"<TICKER>": <float>, "CASH": <float>},
  "weight_adjustments": [
    {
      "ticker": "<TICKER>",
      "base": <float>,
      "adjusted": <float>,
      "delta": <float>,
      "reason": "<≤40 chars English>"
    }
  ],
  "decision_rationale": "<≤400 chars: whose argument won, tied to data; name major tickers if relevant>",
  "reasoning": "<≤200 chars short summary (may mirror decision_rationale)",
  "consensus_points": ["...", "..."],
  "divergence_points": ["...", "..."],
  "key_events": ["<event phrase 1>", "..."],
  "debate_resolution": "<one sentence arbitration>"
}

JSON only."""

# ═══════════════════════════════════════════════════════════════
# 5-level stance → legacy 4-level mapping lives in strategies (defensive_adjust, etc.)
# ═══════════════════════════════════════════════════════════════

_VALID_REGIMES = {"bull_trend", "bull_weak", "neutral", "bear_weak", "bear_trend", "high_vol"}
_VALID_STANCES_5 = {"buy", "overweight", "maintain", "underweight", "sell"}


# ═══════════════════════════════════════════════════════════════
# Main entry
# ═══════════════════════════════════════════════════════════════


async def run_synthesizer_async(
    research_report: dict,
    bull_output: dict,
    bear_output: dict,
    base_weights: dict,
    brief: dict,
    risk_params: dict,
) -> dict:
    """
    Stage 5: arbitrate Bull/Bear → adjusted_weights.
    Output compatible with legacy researcher_out; downstream Risk MGR unchanged.
    """
    max_single_position = float(risk_params.get("max_single_position", 0.20))

    allowed_tickers = _collect_allowed_tickers(brief, base_weights)

    user_payload = _build_user_message(
        research_report, bull_output, bear_output, base_weights, risk_params
    )

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
                max_tokens=2400,
                response_format={"type": "json_object"},
            )
            raw = resp.choices[0].message.content or ""
            elapsed = round(time.time() - t0, 2)
            logger.info(
                f"[SYNTHESIZER] done in {elapsed}s | "
                f"input_tokens={resp.usage.prompt_tokens} "
                f"output_tokens={resp.usage.completion_tokens}"
            )

            parsed = json.loads(raw)
            _validate(parsed)
            return _normalize(
                parsed,
                base_weights=base_weights,
                allowed_tickers=allowed_tickers,
                max_single_position=max_single_position,
                bull_output=bull_output,
                bear_output=bear_output,
                research_report=research_report,
            )

        except Exception as e:
            last_error = str(e)
            logger.warning(f"[SYNTHESIZER] attempt {attempt} failed: {e}")

    # All retries failed → fall back to base_weights
    logger.error(
        f"[SYNTHESIZER] all retries failed, degrading to base_weights. last_error={last_error}"
    )
    return _degraded_output(
        base_weights, bull_output, bear_output, research_report, last_error
    )


# ═══════════════════════════════════════════════════════════════
# User message builder
# ═══════════════════════════════════════════════════════════════


def _build_user_message(
    research_report: dict,
    bull_output: dict,
    bear_output: dict,
    base_weights: dict,
    risk_params: dict,
) -> str:
    max_pos = float(risk_params.get("max_single_position", 0.20))
    min_cash = float(risk_params.get("min_cash_pct", 0.05))

    # Trim research_report (omit full ticker_signals if needed)
    regime = research_report.get("market_regime", {})
    macro = research_report.get("macro_outlook", {})
    insights = research_report.get("cross_signal_insights", [])

    return (
        "## Research report summary\n"
        f"market_regime: {json.dumps(regime, ensure_ascii=False)}\n"
        f"macro_outlook: {json.dumps(macro, ensure_ascii=False)}\n"
        f"cross_signal_insights: {json.dumps(insights, ensure_ascii=False)}\n\n"
        "## Bull — draft thesis + cross-exam vs Bear\n"
        f"{json.dumps(bull_output, ensure_ascii=False, indent=2)}\n\n"
        "## Bear — draft thesis + cross-exam vs Bull\n"
        f"{json.dumps(bear_output, ensure_ascii=False, indent=2)}\n\n"
        "## Base weights (Stage 2 baseline — you allocate)\n"
        f"{json.dumps(base_weights, ensure_ascii=False, indent=2)}\n\n"
        "## Constraints\n"
        f"max_single_position = {max_pos}\n"
        f"min_cash_pct = {min_cash}\n\n"
        "## Your task (PM)\n"
        "Output final adjusted_weights + decision_rationale + market_judgment + recommended_stance + "
        "weight_adjustments + consensus/divergence + key_events + debate_resolution. JSON only."
    )


def _collect_allowed_tickers(brief: dict, base_weights: dict) -> set[str]:
    """adjusted_weights may only use tickers from brief.holdings or base_weights (plus CASH)."""
    tickers: set[str] = {"CASH"}
    for h in brief.get("holdings") or []:
        t = (h.get("ticker") or "").upper().strip()
        if t:
            tickers.add(t)
    for t in base_weights.keys():
        t = str(t).upper().strip()
        if t:
            tickers.add(t)
    return tickers


# ═══════════════════════════════════════════════════════════════
# Validation + normalization
# ═══════════════════════════════════════════════════════════════


def _validate(out: dict) -> None:
    required = [
        "market_judgment",
        "recommended_stance",
        "adjusted_weights",
    ]
    missing = [k for k in required if k not in out]
    if missing:
        raise ValueError(f"missing fields: {missing}")
    if not (out.get("reasoning") or out.get("decision_rationale")):
        raise ValueError("need reasoning or decision_rationale")

    mj = out.get("market_judgment") or {}
    if "regime" not in mj:
        raise ValueError("market_judgment.regime missing")

    weights = out.get("adjusted_weights")
    if not isinstance(weights, dict) or not weights:
        raise ValueError("adjusted_weights must be a non-empty dict")


def _normalize(
    out: dict,
    *,
    base_weights: dict,
    allowed_tickers: set[str],
    max_single_position: float,
    bull_output: dict,
    bear_output: dict,
    research_report: dict,
) -> dict:
    mj = out.get("market_judgment") or {}
    regime = str(mj.get("regime", "")).strip()
    if regime not in _VALID_REGIMES:
        regime = "neutral"

    stance = str(out.get("recommended_stance", "")).strip()
    if stance not in _VALID_STANCES_5:
        stance = "maintain"

    try:
        conf = float(mj.get("adjusted_confidence", 0.5) or 0.5)
    except (TypeError, ValueError):
        conf = 0.5
    conf = max(0.0, min(1.0, conf))

    uncertainty = bool(mj.get("uncertainty_flag", False))
    # Auto: if Bull/Bear confidence gap < 0.15, force uncertainty
    bull_conf = float(bull_output.get("confidence", 0.5) or 0.5)
    bear_conf = float(bear_output.get("confidence", 0.5) or 0.5)
    if abs(bull_conf - bear_conf) < 0.15:
        uncertainty = True

    # key_events: LLM output (string list) or inherit from research_report.key_keywords
    key_events = out.get("key_events") or []
    if not isinstance(key_events, list) or not key_events:
        # Prefer key_keywords (flat string list for transmission matcher)
        key_events = (research_report.get("macro_outlook") or {}).get("key_keywords") or []
    # If still empty, try rich key_events and extract keywords
    if not key_events:
        rich = (research_report.get("macro_outlook") or {}).get("key_events") or []
        key_events = [
            e["keyword"] if isinstance(e, dict) else str(e)
            for e in rich if e
        ]
    key_events = [str(e).strip() for e in key_events if str(e).strip()][:5]
    if not key_events:
        key_events = ["normal market conditions"]

    raw_weights = out.get("adjusted_weights") or {}
    adjusted = _sanitize_weights(
        raw_weights,
        allowed_tickers=allowed_tickers,
        max_single_position=max_single_position,
        fallback=base_weights,
    )

    actual_adjustments = _compute_adjustments(base_weights, adjusted)

    # Prefer LLM-provided reasons per ticker
    llm_adjustments = out.get("weight_adjustments") or []
    if isinstance(llm_adjustments, list) and llm_adjustments:
        reason_by_ticker = {}
        for item in llm_adjustments:
            if isinstance(item, dict) and item.get("ticker"):
                reason_by_ticker[str(item["ticker"]).upper()] = str(item.get("reason", ""))[:80]
        for item in actual_adjustments:
            item["reason"] = reason_by_ticker.get(item["ticker"], "")

    debate_summary = _build_debate_summary(bull_output, bear_output, out)

    decision_rationale = str(out.get("decision_rationale") or out.get("reasoning") or "")[:800]
    reasoning_line = str(out.get("reasoning") or decision_rationale)[:500]

    return {
        # researcher_out fields (Risk MGR consumes these)
        "market_judgment": {
            "regime":              regime,
            "adjusted_confidence": conf,
            "uncertainty_flag":    uncertainty,
        },
        "recommended_stance":  stance,
        "adjusted_weights":    adjusted,
        "weight_adjustments":  actual_adjustments,
        "reasoning":           reasoning_line,
        "decision_rationale":  decision_rationale,
        "consensus_points":    list(out.get("consensus_points") or [])[:5],
        "divergence_points":   list(out.get("divergence_points") or [])[:5],
        "key_events":          key_events,
        "used_degraded_fallback": False,
        # Extra (Communicator; not consumed by Risk MGR)
        "debate_summary":      debate_summary,
    }


def _build_debate_summary(bull_output: dict, bear_output: dict, synth_raw: dict) -> dict:
    """Build debate_summary for Communicator."""
    bull_args = bull_output.get("core_arguments") or bull_output.get("arguments") or []
    bear_args = bear_output.get("core_arguments") or bear_output.get("arguments") or []
    rb = bull_output.get("rebuttal_vs_bear") or {}
    rbull = bear_output.get("rebuttal_vs_bull") or {}
    return {
        "bull_confidence":      float(bull_output.get("confidence", 0.5) or 0.5),
        "bear_confidence":      float(bear_output.get("confidence", 0.5) or 0.5),
        "bull_stance":          bull_output.get("stance", "maintain"),
        "bear_stance":          bear_output.get("stance", "reduce"),
        "bull_arguments":       list(bull_args)[:3],
        "bear_arguments":       list(bear_args)[:3],
        "bull_rebuttal_vs_bear": str(rb.get("rebuttal_statement", ""))[:280],
        "bear_rebuttal_vs_bull": str(rbull.get("rebuttal_statement", ""))[:280],
        "resolution":           str(synth_raw.get("debate_resolution", ""))[:200],
        "decision_rationale":   str(synth_raw.get("decision_rationale", ""))[:400],
        "bull_failed":          bool(bull_output.get("failed", False)),
        "bear_failed":          bool(bear_output.get("failed", False)),
    }


def _sanitize_weights(
    raw: dict,
    *,
    allowed_tickers: set[str],
    max_single_position: float,
    fallback: dict,
) -> dict:
    """
    Sanitize LLM adjusted_weights:
      1. Keep only allowed_tickers
      2. Negative / non-numeric → 0
      3. Clip single name to max_single_position
      4. Ensure CASH present
      5. Renormalize sum to 1.0
    """
    cleaned: dict[str, float] = {}
    for k, v in raw.items():
        ticker = str(k).upper().strip()
        if ticker not in allowed_tickers:
            logger.warning(f"[SYNTHESIZER] dropped unknown ticker '{ticker}'")
            continue
        try:
            w = float(v)
        except (TypeError, ValueError):
            continue
        if w < 0:
            w = 0.0
        cleaned[ticker] = w

    if not cleaned:
        logger.warning("[SYNTHESIZER] weights empty after cleaning — fallback to base_weights")
        return {k: round(float(v), 4) for k, v in fallback.items()}

    for t in list(cleaned.keys()):
        if t == "CASH":
            continue
        if cleaned[t] > max_single_position:
            cleaned[t] = max_single_position

    cleaned.setdefault("CASH", 0.0)

    total = sum(cleaned.values())
    if total <= 0:
        return {k: round(float(v), 4) for k, v in fallback.items()}

    scaled = {t: w / total for t, w in cleaned.items()}

    out = {t: round(w, 4) for t, w in scaled.items() if t != "CASH"}
    out["CASH"] = round(max(1.0 - sum(out.values()), 0.0), 4)
    return out


def _compute_adjustments(
    base: dict[str, float],
    adjusted: dict[str, float],
    threshold: float = 0.01,
) -> list[dict]:
    """Base vs adjusted deltas; drop noise below threshold."""
    out: list[dict] = []
    all_tickers = set(base.keys()) | set(adjusted.keys())
    for ticker in sorted(all_tickers):
        b = float(base.get(ticker, 0.0) or 0.0)
        a = float(adjusted.get(ticker, 0.0) or 0.0)
        delta = a - b
        if abs(delta) < threshold:
            continue
        out.append({
            "ticker":   ticker,
            "base":     round(b, 4),
            "adjusted": round(a, 4),
            "delta":    round(delta, 4),
            "reason":   "",
        })
    return out


def _degraded_output(
    base_weights: dict,
    bull_output: dict,
    bear_output: dict,
    research_report: dict,
    error: str | None,
) -> dict:
    """Safe fallback when all LLM retries fail."""
    macro = research_report.get("macro_outlook") or {}
    key_events = macro.get("key_keywords") or []
    if not key_events:
        # Extract from rich key_events
        rich = macro.get("key_events") or []
        key_events = [
            e["keyword"] if isinstance(e, dict) else str(e)
            for e in rich if e
        ]
    if not key_events:
        key_events = ["normal market conditions"]

    degraded_reason = f"PM degraded: using Stage 2 baseline weights (error={error})"
    synth_stub = {"debate_resolution": degraded_reason, "decision_rationale": degraded_reason}
    return {
        "market_judgment": {
            "regime":              "neutral",
            "adjusted_confidence": 0.3,
            "uncertainty_flag":    True,
        },
        "recommended_stance":   "maintain",
        "adjusted_weights":     {k: round(float(v), 4) for k, v in base_weights.items()},
        "weight_adjustments":   [],
        "reasoning":            degraded_reason,
        "decision_rationale":   degraded_reason,
        "consensus_points":     [],
        "divergence_points":    [],
        "key_events":           key_events,
        "used_degraded_fallback": True,
        "debate_summary":       _build_debate_summary(bull_output, bear_output, synth_stub),
    }
