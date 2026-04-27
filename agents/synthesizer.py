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


CONFIDENCE_ARBITRATION_RULES = """
## 基于 Researcher Confidence 的权重调整上限

从 research_report.ticker_signals_dict 读取每个 ticker 的 confidence，
并严格遵守以下调整幅度上限（相对 base_weight）：

| Researcher Confidence | 最大权重调整幅度 |
|-----------------------|----------------|
| high                  | ±5%            |
| medium                | ±3%            |
| low                   | ±1%            |
| ticker 不在报告中     | ±1%（保守）    |

即使辩论中 Bull/Bear 双方均强烈建议大幅调整，
如果 Researcher 对该 ticker 的 confidence = low，
你也只能做 ±1% 的微调。

**在 step3_debate_arbitration 的每条记录里，
必须填写 researcher_confidence 字段和实际使用的 max_delta。**
"""

SYNTHESIZER_COT_SCHEMA = """
## Output format (strict order, complete JSON)

{
  "reasoning_chain": {

    "step1_regime_acknowledgment": {
      "regime": "copy from regime_result verbatim",
      "constraints_accepted": true | false,
      "override_reason": null | "only in extreme cases: explain why constraints are not followed"
    },

    "step2_quant_baseline_assessment": {
      "baseline_quality": "reliable" | "questionable",
      "questionable_reason": null | "explain why the quant baseline is not trusted",
      "top3_by_score": ["TICKER1", "TICKER2", "TICKER3"],
      "bottom3_by_score": ["TICKER1", "TICKER2", "TICKER3"]
    },

    "step3_debate_arbitration": [
      {
        "ticker": "TICKER",
        "bull_stance": "overweight/hold/underweight",
        "bear_stance": "overweight/hold/underweight",
        "my_decision": "overweight/hold/underweight",
        "decision_basis": "bull_wins" | "bear_wins" | "compromise" | "quant_override",
        "rationale": "one-sentence rationale (≤30 chars)"
      }
    ],

    "step4_risk_sanity_check": {
      "total_equity_pct": <float>,
      "largest_single_position": {"ticker": "X", "weight": <float>},
      "hedge_allocation_pct": <float>,
      "cash_pct": <float>,
      "regime_constraints_satisfied": true | false
    },

    "step5_final_judgment": {
      "market_view": "one-sentence market view (≤30 chars)",
      "key_conviction": "single highest-confidence conviction",
      "biggest_uncertainty": "largest source of uncertainty"
    }
  },

  "adjusted_weights": {
    "<TICKER>": <float>,
    "CASH": <float>
  },

  "decision_rationale": "human-readable decision summary (≤100 chars)",

  "market_judgment": "bullish" | "cautious_bullish" | "neutral" | "cautious_bearish" | "bearish"
}

Rules:
1. reasoning_chain must appear before adjusted_weights; order cannot be changed
2. step3_debate_arbitration includes only tickers where Bull/Bear disagree
3. step4 values must be consistent with actual adjusted_weights, no contradictions
4. sum of all adjusted_weights values must = 1.0 (±0.001 float tolerance allowed)
5. If step1.constraints_accepted = false, override_reason must be filled in,
   and adjusted_weights must still satisfy defensive requirements (CASH >= 0.15)
"""

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

""" + CONFIDENCE_ARBITRATION_RULES + """

【Disagreement Map — REQUIRED for step3_debate_arbitration】
A Structured Disagreement Map has been appended to your input below.

For each ticker in the map:
  1. Read researcher_confidence to determine the max delta from base_weights
  2. bull_wins → move toward overweight within allowed delta
  3. bear_wins → move toward underweight within allowed delta
  4. compromise → split the difference within both constraints

Do NOT re-read raw bull/bear JSON for arbitration decisions.
Use the Structured Disagreement Map table directly.

【Output: JSON only — must include reasoning_chain, order is fixed】

 reasoning_chain must appear before adjusted_weights; order cannot be changed.
 reasoning_chain contains 5 steps: regime acknowledgment → quant baseline assessment →
 debate arbitration → risk sanity check → final judgment.
 step3_debate_arbitration includes only tickers where Bull/Bear disagree.
 step4 values must be consistent with actual adjusted_weights, no contradictions allowed.

""" + SYNTHESIZER_COT_SCHEMA + """

JSON only."""

# ═══════════════════════════════════════════════════════════════
# 5-level stance → legacy 4-level mapping lives in strategies (defensive_adjust, etc.)
# ═══════════════════════════════════════════════════════════════

_VALID_REGIMES = {"bull_trend", "bull_weak", "neutral", "bear_weak", "bear_trend", "high_vol"}
_VALID_STANCES_5 = {"buy", "overweight", "maintain", "underweight", "sell"}


# ═══════════════════════════════════════════════════════════════
# Synthesizer Chain-of-Thought Schema (Task 5)
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
    regime_result: dict | None = None,
    debate_summary: dict | None = None,   # NEW: structured disagreement_map for PM injection
) -> dict:
    """
    Stage 5: arbitrate Bull/Bear → adjusted_weights.
    Output compatible with legacy researcher_out; downstream Risk MGR unchanged.
    """
    # Defensive: ensure all dict inputs are actually dicts before any .get() call
    if not isinstance(bull_output, dict):
        bull_output = {}
    if not isinstance(bear_output, dict):
        bear_output = {}
    if not isinstance(research_report, dict):
        research_report = {}
    if not isinstance(brief, dict):
        brief = {}
    if not isinstance(risk_params, dict):
        risk_params = {}
    if not isinstance(regime_result, dict):
        regime_result = None
    if debate_summary is not None and not isinstance(debate_summary, dict):
        debate_summary = None

    max_single_position = float(risk_params.get("max_single_position", 0.20))

    allowed_tickers = _collect_allowed_tickers(brief, base_weights)

    user_payload = _build_user_message(
        research_report, bull_output, bear_output, base_weights, risk_params, regime_result,
        debate_summary=debate_summary,
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
    regime_result: dict | None = None,
    debate_summary: dict | None = None,   # NEW: structured disagreement_map
) -> str:
    max_pos = float(risk_params.get("max_single_position", 0.20))
    min_cash = float(risk_params.get("min_cash_pct", 0.05))

    regime = research_report.get("market_regime", {})
    macro = research_report.get("macro_outlook", {})
    insights = research_report.get("cross_signal_insights", [])

    # Regime constraint block
    regime_block = ""
    if regime_result:
        regime_block = (
            "## SYSTEM REGIME CONSTRAINT (hard classification, do not override)\n"
            f"Regime: {regime_result.get('regime', 'unknown')}\n"
            f"Confidence: {regime_result.get('confidence', 'unknown')}\n"
            f"Constraint: {regime_result.get('constraints', {}).get('llm_instruction', '')}\n\n"
        )

    user_payload = (
        f"{regime_block}"
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

    # ── Inject Structured Disagreement Map ──────────────────────
    if debate_summary:
        disagreement_map = debate_summary.get("disagreement_map") or []
        if disagreement_map:
            rows = []
            for item in disagreement_map:
                ticker = item.get("ticker", "?")
                bull_v = item.get("bull", "?")
                bear_v = item.get("bear", "?")
                researcher_conf = item.get("researcher_confidence", "medium")
                max_delta = item.get("max_allowed_delta", 0.03)
                rows.append(
                    f"{ticker}: Bull={bull_v} | Bear={bear_v} | "
                    f"researcher_confidence={researcher_conf} | max_delta={max_delta:.2%}"
                )
            user_payload += (
                "\n## Structured Disagreement Map "
                "(REQUIRED for step3_debate_arbitration)\n"
                "Use this table for systematic ticker-level arbitration.\n"
                "High researcher_confidence → ±5% max delta | "
                "Medium → ±3% | Low → ±1%\n"
                + "\n".join(rows)
            )

    return user_payload


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
        "reasoning_chain",
        "adjusted_weights",
        "decision_rationale",
    ]
    missing = [k for k in required if k not in out]
    if missing:
        raise ValueError(f"missing fields: {missing}")

    # Validate reasoning_chain structure (Task 5)
    rc = out.get("reasoning_chain") or {}
    required_steps = [
        "step1_regime_acknowledgment",
        "step2_quant_baseline_assessment",
        "step3_debate_arbitration",
        "step4_risk_sanity_check",
        "step5_final_judgment",
    ]
    for step in required_steps:
        if step not in rc:
            raise ValueError(f"reasoning_chain missing step: {step}")

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
    # Defensive: guard against non-dict inputs leaking through (e.g., error strings)
    if not isinstance(bull_output, dict):
        bull_output = {}
    if not isinstance(bear_output, dict):
        bear_output = {}

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
    # Map overall_confidence (high=0.9, medium=0.5, low=0.3) to float for gap calculation
    _conf_map = {"high": 0.9, "medium": 0.5, "low": 0.3}
    bull_conf_str = bull_output.get("overall_confidence", "medium")
    bear_conf_str = bear_output.get("overall_confidence", "medium")
    bull_conf = _conf_map.get(bull_conf_str, 0.5)
    bear_conf = _conf_map.get(bear_conf_str, 0.5)
    if abs(bull_conf - bear_conf) < 0.15:
        uncertainty = True
    # Defensive programming: ensure uncertainty is a boolean
    uncertainty = bool(uncertainty)

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

    debate_summary = _build_debate_summary(
        bull_output, bear_output, out,
        researcher_signals=research_report.get("ticker_signals_dict"),
    )

    decision_rationale = str(out.get("decision_rationale") or out.get("reasoning") or "")[:800]
    reasoning_line = str(out.get("reasoning") or decision_rationale)[:500]

    # Preserve reasoning_chain from LLM output (Task 5)
    reasoning_chain = out.get("reasoning_chain") or {}

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
        # Task 5: Chain-of-Thought reasoning chain
        "reasoning_chain":     reasoning_chain,
    }


def _build_debate_summary(
    bull_output: dict,
    bear_output: dict,
    synth_raw: dict,
    researcher_signals: dict | None = None,
) -> dict:
    """Build debate_summary for Communicator from structured outputs.

    Args:
        bull_output: Bull researcher output
        bear_output: Bear researcher output
        synth_raw: Raw synthesizer output
        researcher_signals: research_report["ticker_signals_dict"] with confidence info (Task 7)
    """
    # Defensive: ensure inputs are dicts (guard against string error messages leaking through)
    if not isinstance(bull_output, dict):
        bull_output = {}
    if not isinstance(bear_output, dict):
        bear_output = {}
    if not isinstance(synth_raw, dict):
        synth_raw = {}
    if researcher_signals is None:
        researcher_signals = {}

    bull_views = bull_output.get("ticker_views") or {}
    bear_views = bear_output.get("ticker_views") or {}

    # Handle legacy cross_exam output format
    bull_rb = bull_output.get("rebuttal_vs_bear") if isinstance(bull_output.get("rebuttal_vs_bear"), dict) else {}
    bear_rb = bear_output.get("rebuttal_vs_bull") if isinstance(bear_output.get("rebuttal_vs_bull"), dict) else {}

    # Handle new cross_exam output format
    if not bull_rb and "rebuttals" in bull_output:
        bull_rb = bull_output
    if not bear_rb and "rebuttals" in bear_output:
        bear_rb = bear_output

    # Confidence mapping (high=0.9, medium=0.5, low=0.3)
    conf_map = {"high": 0.9, "medium": 0.5, "low": 0.3}
    bull_conf_str = bull_output.get("overall_confidence", "medium")
    bear_conf_str = bear_output.get("overall_confidence", "medium")
    bull_conf = conf_map.get(bull_conf_str, 0.5)
    bear_conf = conf_map.get(bear_conf_str, 0.5)

    # Task 7: delta map from researcher confidence
    CONFIDENCE_DELTA_MAP = {
        "high": 0.05,
        "medium": 0.03,
        "low": 0.01,
    }

    disagreements = []
    agreements = []
    all_tickers = set(bull_views.keys()) | set(bear_views.keys())

    for ticker in all_tickers:
        bull_view = bull_views.get(ticker)
        bear_view = bear_views.get(ticker)

        if not bull_view or not bear_view:
            continue

        bull_dir = bull_view.get("direction", "hold")
        bear_dir = bear_view.get("direction", "hold")

        is_conflict = (
            (bull_dir == "overweight" and bear_dir == "underweight") or
            (bull_dir == "underweight" and bear_dir == "overweight")
        )

        # Task 7: extract researcher confidence for this ticker
        researcher_confidence = "medium"
        max_allowed_delta = 0.03
        if researcher_signals:
            sig = researcher_signals.get(ticker.upper(), {}) if isinstance(researcher_signals, dict) else {}
            researcher_confidence = str(sig.get("confidence", "medium")).strip()
            max_allowed_delta = CONFIDENCE_DELTA_MAP.get(researcher_confidence, 0.03)

        if is_conflict:
            # Find corresponding rebuttals
            bull_rebuttal_list = bull_rb.get("rebuttals", []) if isinstance(bull_rb, dict) else []
            bear_rebuttal_list = bear_rb.get("rebuttals", []) if isinstance(bear_rb, dict) else []

            bull_rb_item = next(
                (r for r in bull_rebuttal_list if isinstance(r, dict) and r.get("ticker", "").upper() == ticker.upper()),
                None,
            )
            bear_rb_item = next(
                (r for r in bear_rebuttal_list if isinstance(r, dict) and r.get("ticker", "").upper() == ticker.upper()),
                None,
            )

            disagreements.append({
                "ticker": ticker,
                "bull": f"{bull_dir}({bull_view.get('magnitude')}) - {bull_view.get('primary_reason')}",
                "bear": f"{bear_dir}({bear_view.get('magnitude')}) - {bear_view.get('primary_reason')}",
                "bull_rebuttal": bull_rb_item.get("rebuttal") if bull_rb_item else None,
                "bear_rebuttal": bear_rb_item.get("rebuttal") if bear_rb_item else None,
                "researcher_confidence": researcher_confidence,  # Task 7
                "max_allowed_delta": max_allowed_delta,         # Task 7
            })
        else:
            agreements.append(ticker)

    return {
        "bull_confidence":       bull_conf,
        "bear_confidence":       bear_conf,
        "bull_stance":           str(bull_output.get("overall_stance", "neutral")),
        "bear_stance":           str(bear_output.get("overall_stance", "neutral")),
        "thesis_summary":        str(bull_output.get("thesis_summary", ""))[:200],
        "disagreement_map":      disagreements,
        "agreed_tickers":        agreements,
        "bull_top_3":            bull_output.get("top_3_conviction", []),
        "bear_top_3":            bear_output.get("top_3_conviction", []),
        "bull_macro_tailwinds":   bull_output.get("macro_tailwinds", []),
        "bear_macro_headwinds":   bear_output.get("macro_headwinds", []),
        "resolution":            str(synth_raw.get("debate_resolution", ""))[:200],
        "decision_rationale":     str(synth_raw.get("decision_rationale", ""))[:400],
        "bull_failed":           bool(bull_output.get("failed", False)),
        "bear_failed":           bool(bear_output.get("failed", False)),
        "researcher_signals":    researcher_signals or {},  # Task 7: pass through for downstream
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
        "debate_summary":       _build_debate_summary(
            bull_output, bear_output, synth_stub,
            researcher_signals=research_report.get("ticker_signals_dict"),
        ),
        # Task 5: Chain-of-Thought (degraded fallback)
        "reasoning_chain": {
            "_degraded": True,
            "_error": str(error) if error else "unknown",
            "step1_regime_acknowledgment": {"regime": "neutral", "constraints_accepted": True, "override_reason": None},
            "step2_quant_baseline_assessment": {"baseline_quality": "questionable", "questionable_reason": degraded_reason, "top3_by_score": [], "bottom3_by_score": []},
            "step3_debate_arbitration": [],
            "step4_risk_sanity_check": {"total_equity_pct": 1.0, "largest_single_position": {"ticker": "N/A", "weight": 0.0}, "hedge_allocation_pct": 0.0, "cash_pct": 0.0, "regime_constraints_satisfied": False},
            "step5_final_judgment": {"market_view": degraded_reason, "key_conviction": "none", "biggest_uncertainty": "system degraded"},
        },
    }
