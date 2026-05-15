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
## Weight adjustment limits based on Researcher confidence

Read each ticker's confidence from research_report.ticker_signals_dict and
strictly follow these maximum adjustment limits relative to base_weight:

| Researcher Confidence | Max weight adjustment |
|-----------------------|----------------|
| high                  | ±5%            |
| medium                | ±3%            |
| low                   | ±1%            |
| ticker absent from report | ±1% conservative |

Even if both Bull and Bear argue strongly for a larger adjustment, if the
Researcher confidence for that ticker is low, you may only make a +/-1% minor adjustment.

Every item in step3_debate_arbitration must include researcher_confidence and
the actual max_delta used.
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

  "market_judgment": {
    "regime": "bull_trend" | "bull_weak" | "neutral" | "bear_weak" | "bear_trend" | "high_vol",
    "adjusted_confidence": <float between 0.0 and 1.0>,
    "uncertainty_flag": true | false
  },

  "scorecard_compliance": {
    "scorecard_alignment": "aligned" | "partially_aligned" | "conflict",
    "action_permission_used": "copy investment_permission from market_scorecard",
    "data_quality_adjustment": "how data quality affected sizing",
    "why_this_trade_is_reasonable": "one sentence, <= 120 chars",
    "known_limitations": ["limitation or warning"]
  },

  "style_compliance": {
    "analysis_style_used": "copy decision_style.analysis_style",
    "trade_style_used": "copy decision_style.trade_style",
    "style_limits_respected": true | false,
    "news_bias_used": "summarize macro/ticker news action bias used",
    "sizing_adjustment": "how style_limits affected sizing and turnover",
    "blocked_or_clipped_actions": ["action blocked or clipped by style limits"],
    "known_limitations": ["style/news limitation or warning"]
  }
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
  · Optional Strategy Playground comparison bundle (traditional strategies; advisory only)

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
    · Strategy Playground may guide strategy selection/blending, but never overrides hard risk constraints.
    · Market Scorecard permissions are hard PM constraints. If market_scorecard says small_overweight_only,
      limited data, hold_or_trim, reduce_risk_only, or cash_only, your adjusted_weights must reflect that.
    · You must include scorecard_compliance and it must honestly explain how the final weights obey the scorecard.
    · Decision Style is a hard PM interpretation/execution contract. It does not create weights, but its
      style_limits must cap how aggressively you move from base_weights. Include style_compliance.
    · Structured News Evidence action_bias is advisory evidence. News may confirm, block, or reduce a trade,
      but news cannot directly create target weights.

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
    playground_bundle: dict | None = None,
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
    if playground_bundle is not None and not isinstance(playground_bundle, dict):
        playground_bundle = None

    max_single_position = float(risk_params.get("max_single_position", 0.20))

    allowed_tickers = _collect_allowed_tickers(brief, base_weights)
    market_scorecard = brief.get("market_scorecard") or {}
    news_evidence = brief.get("news_evidence") or (brief.get("evidence_bundle") or {}).get("news_evidence") or {}
    decision_style = brief.get("decision_style") or (brief.get("evidence_bundle") or {}).get("decision_style") or {}

    user_payload = _build_user_message(
        research_report, bull_output, bear_output, base_weights, risk_params, regime_result,
        debate_summary=debate_summary,
        playground_bundle=playground_bundle,
        market_scorecard=market_scorecard,
        evidence_bundle=brief.get("evidence_bundle") or {},
        news_evidence=news_evidence,
        decision_style=decision_style,
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
            result = _normalize(
                parsed,
                base_weights=base_weights,
                allowed_tickers=allowed_tickers,
                max_single_position=max_single_position,
                bull_output=bull_output,
                bear_output=bear_output,
                research_report=research_report,
                market_scorecard=market_scorecard,
                decision_style=decision_style,
            )
            result["_token_usage"] = {
                "prompt_tokens": resp.usage.prompt_tokens,
                "completion_tokens": resp.usage.completion_tokens,
            }
            return result

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
    playground_bundle: dict | None = None,
    market_scorecard: dict | None = None,
    evidence_bundle: dict | None = None,
    news_evidence: dict | None = None,
    decision_style: dict | None = None,
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

    scorecard = market_scorecard or {}
    evidence = evidence_bundle or {}
    news = news_evidence or evidence.get("news_evidence") or {}
    style = decision_style or evidence.get("decision_style") or {}
    scorecard_block = ""
    if scorecard:
        scorecard_block = (
            "## Market Scorecard (hard PM permission contract)\n"
            f"{json.dumps(scorecard, ensure_ascii=False, indent=2)}\n\n"
            "You must obey investment_permission, max_adjustment_from_base, "
            "max_equity_weight, min_cash_weight, allow_new_positions, and "
            "max_turnover_per_cycle. If evidence is limited or missing, use smaller "
            "tilts and explain the data-quality adjustment in scorecard_compliance.\n\n"
        )
    evidence_block = ""
    if evidence:
        evidence_block = (
            "## Evidence Bundle Summary\n"
            f"{json.dumps(_compact_evidence_bundle(evidence), ensure_ascii=False, indent=2)}\n\n"
        )
    news_block = ""
    if news:
        news_block = (
            "## Structured News Evidence\n"
            f"{json.dumps(_compact_news_evidence(news), ensure_ascii=False, indent=2)}\n\n"
            "Use action_bias deterministically: block_new_buy blocks new buys for that ticker, "
            "reduce_or_wait argues against adding risk, confirm_existing_signal can only confirm "
            "an existing quant signal, and allow_overweight still must obey scorecard/style limits.\n\n"
        )
    style_block = ""
    if style:
        style_block = (
            "## Decision Style Contract\n"
            f"{json.dumps(style, ensure_ascii=False, indent=2)}\n\n"
            "You must obey style_limits. max_adjustment_multiplier tightens scorecard max deltas; "
            "max_turnover_per_cycle and max_new_buys_per_cycle limit execution aggressiveness; "
            "min_cash_floor_addition is additive to scorecard min_cash_weight. "
            "Explain this in style_compliance.\n\n"
        )

    user_payload = (
        f"{regime_block}"
        f"{scorecard_block}"
        f"{evidence_block}"
        f"{news_block}"
        f"{style_block}"
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
        f"{_build_playground_section(playground_bundle)}"
        "## Constraints\n"
        f"max_single_position = {max_pos}\n"
        f"min_cash_pct = {min_cash}\n\n"
        "## Your task (PM)\n"
        "Output final adjusted_weights + decision_rationale + market_judgment + recommended_stance + "
        "weight_adjustments + consensus/divergence + key_events + debate_resolution. "
        "Include scorecard_compliance with scorecard_alignment, action_permission_used, "
        "data_quality_adjustment, why_this_trade_is_reasonable, and known_limitations. "
        "Include style_compliance with analysis_style_used, trade_style_used, "
        "style_limits_respected, news_bias_used, sizing_adjustment, "
        "blocked_or_clipped_actions, and known_limitations. "
        "If Strategy Playground is present, also include playground_strategy_assessment with "
        "selected_strategy, blend_weights, discounted_strategies, and reasoning; respect "
        "memory_feedback discounts as advisory only. JSON only."
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


def _compact_evidence_bundle(bundle: dict) -> dict:
    return {
        "generated_at": bundle.get("generated_at"),
        "max_age_seconds": bundle.get("max_age_seconds"),
        "market": bundle.get("market") or {},
        "rotation": bundle.get("rotation") or {},
        "strategies": {
            "playground_available": (bundle.get("strategies") or {}).get("playground_available"),
            "snapshot_count": (bundle.get("strategies") or {}).get("snapshot_count"),
            "forward_return_samples": (bundle.get("strategies") or {}).get("forward_return_samples"),
            "data_quality": (bundle.get("strategies") or {}).get("data_quality"),
            "consensus_top5": (bundle.get("strategies") or {}).get("consensus_top5") or [],
            "turnover_warnings": (bundle.get("strategies") or {}).get("turnover_warnings") or [],
            "evidence_summary": (bundle.get("strategies") or {}).get("evidence_summary") or {},
            "strategy_use_summary": (bundle.get("strategies") or {}).get("strategy_use_summary") or {},
            "strategy_confidence": _compact_strategy_confidence(
                (bundle.get("strategies") or {}).get("strategy_confidence") or {}
            ),
        },
        "data_quality": bundle.get("data_quality") or {},
    }


def _compact_strategy_confidence(confidence: dict) -> dict:
    compact: dict[str, dict] = {}
    for name, row in (confidence or {}).items():
        if not isinstance(row, dict):
            continue
        compact[name] = {
            "suggested_use": row.get("suggested_use"),
            "confidence_score": row.get("confidence_score"),
            "historical_reliability": row.get("historical_reliability"),
            "historical_samples": row.get("historical_samples"),
            "live_samples": row.get("live_samples"),
            "regime_fit": row.get("regime_fit"),
            "consensus_conflict": row.get("consensus_conflict"),
            "reason_codes": row.get("reason_codes") or [],
        }
    return compact


def _compact_news_evidence(news_evidence: dict) -> dict:
    ticker_scores = {}
    for ticker, item in (news_evidence.get("ticker_news_scores") or {}).items():
        if not isinstance(item, dict):
            continue
        ticker_scores[ticker] = {
            "bias": item.get("bias"),
            "confidence": item.get("confidence"),
            "effective_credibility": item.get("effective_credibility"),
            "market_impact": item.get("market_impact"),
            "action_bias": item.get("action_bias"),
            "supporting_items": (item.get("supporting_items") or [])[:3],
            "conflicting_items": (item.get("conflicting_items") or [])[:2],
        }
    return {
        "macro_news_score": news_evidence.get("macro_news_score") or {},
        "ticker_news_scores": ticker_scores,
        "hard_risk_events": news_evidence.get("hard_risk_events") or {},
        "data_gaps": news_evidence.get("data_gaps") or [],
    }


def _build_playground_section(playground_bundle: dict | None) -> str:
    if not playground_bundle:
        return ""

    compact = {
        "regime_label": playground_bundle.get("regime_label"),
        "regime_confidence": playground_bundle.get("regime_confidence"),
        "historical_snapshot_count": playground_bundle.get("historical_snapshot_count"),
        "strategy_confidence": playground_bundle.get("strategy_confidence"),
        "strategies": [
            {
                "strategy_name": item.get("strategy_name"),
                "regime_fit": item.get("regime_fit"),
                "data_ready": item.get("data_ready"),
                "missing_fields": (item.get("data_readiness") or {}).get("missing_fields"),
                "feature_contract_verdict": (item.get("feature_contract") or {}).get("verdict"),
                "can_influence_allocation": (item.get("feature_contract") or {}).get("can_influence_allocation"),
                "weights": item.get("weights"),
                "selected_tickers": item.get("selected_tickers"),
                "expected_turnover_pct": item.get("expected_turnover_pct"),
                "estimated_cost_pct": item.get("estimated_cost_pct"),
                "memory_feedback": item.get("memory_feedback"),
            }
            for item in (playground_bundle.get("strategies") or [])
        ],
        "largest_divergences": (playground_bundle.get("divergence_map") or [])[:5],
        "consensus_weights": playground_bundle.get("consensus_weights"),
    }
    return (
        "## Strategy Playground comparison bundle (advisory only)\n"
        "This compares traditional strategy outputs. You may choose a single strategy, "
        "blend, consensus, or reject all if current evidence is weak. Do not treat this "
        "as execution authority; final weights must still obey all constraints and stay "
        "near Stage 2 base_weights unless evidence is strong.\n"
        f"{json.dumps(compact, ensure_ascii=False, indent=2)}\n\n"
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
        "reasoning_chain",
        "adjusted_weights",
        "decision_rationale",
        "market_judgment",
        "scorecard_compliance",
        "style_compliance",
    ]
    missing = [k for k in required if k not in out]
    if missing:
        raise ValueError(f"missing fields: {missing}")

    # Validate reasoning_chain structure (Task 5)
    rc = out.get("reasoning_chain")
    # Guard: reasoning_chain must be a dict, not a string or other type
    if not isinstance(rc, dict):
        raise ValueError(f"reasoning_chain must be dict, got {type(rc).__name__}")

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

    market_judgment = out.get("market_judgment")
    if not isinstance(market_judgment, dict):
        raise ValueError(
            f"market_judgment must be dict, got {type(market_judgment).__name__}"
        )
    for key in ("regime", "adjusted_confidence", "uncertainty_flag"):
        if key not in market_judgment:
            raise ValueError(f"market_judgment missing field: {key}")

    scorecard_compliance = out.get("scorecard_compliance")
    if not isinstance(scorecard_compliance, dict):
        raise ValueError(
            f"scorecard_compliance must be dict, got {type(scorecard_compliance).__name__}"
        )
    for key in (
        "scorecard_alignment",
        "action_permission_used",
        "data_quality_adjustment",
        "why_this_trade_is_reasonable",
        "known_limitations",
    ):
        if key not in scorecard_compliance:
            raise ValueError(f"scorecard_compliance missing field: {key}")

    style_compliance = out.get("style_compliance")
    if not isinstance(style_compliance, dict):
        raise ValueError(
            f"style_compliance must be dict, got {type(style_compliance).__name__}"
        )
    for key in (
        "analysis_style_used",
        "trade_style_used",
        "style_limits_respected",
        "news_bias_used",
        "sizing_adjustment",
        "blocked_or_clipped_actions",
        "known_limitations",
    ):
        if key not in style_compliance:
            raise ValueError(f"style_compliance missing field: {key}")


def _normalize(
    out: dict,
    *,
    base_weights: dict,
    allowed_tickers: set[str],
    max_single_position: float,
    bull_output: dict,
    bear_output: dict,
    research_report: dict,
    market_scorecard: dict | None = None,
    decision_style: dict | None = None,
) -> dict:
    # Defensive: guard against non-dict inputs leaking through (e.g., error strings)
    if not isinstance(bull_output, dict):
        bull_output = {}
    if not isinstance(bear_output, dict):
        bear_output = {}

    mj = out.get("market_judgment") or {}
    if not isinstance(mj, dict):
        mj = {}
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
    scorecard_compliance = _normalize_scorecard_compliance(
        out.get("scorecard_compliance") or {},
        base_weights=base_weights,
        adjusted_weights=adjusted,
        market_scorecard=market_scorecard or {},
    )
    style_compliance = _normalize_style_compliance(
        out.get("style_compliance") or {},
        base_weights=base_weights,
        adjusted_weights=adjusted,
        decision_style=decision_style or {},
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
    if not isinstance(reasoning_chain, dict):
        reasoning_chain = {}

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
        "consensus_points":    list(out.get("consensus_points"))[:5] if isinstance(out.get("consensus_points"), list) else [],
        "divergence_points":   list(out.get("divergence_points"))[:5] if isinstance(out.get("divergence_points"), list) else [],
        "key_events":          key_events,
        "used_degraded_fallback": False,
        # Extra (Communicator; not consumed by Risk MGR)
        "debate_summary":      debate_summary,
        "playground_strategy_assessment": (
            out.get("playground_strategy_assessment")
            if isinstance(out.get("playground_strategy_assessment"), dict)
            else {}
        ),
        "scorecard_compliance": scorecard_compliance,
        "style_compliance": style_compliance,
        # Task 5: Chain-of-Thought reasoning chain
        "reasoning_chain":     reasoning_chain,
    }


def _normalize_style_compliance(
    raw: dict,
    *,
    base_weights: dict,
    adjusted_weights: dict,
    decision_style: dict,
) -> dict:
    if not isinstance(raw, dict):
        raw = {}
    python_check = _check_style_weight_compliance(
        base_weights=base_weights,
        adjusted_weights=adjusted_weights,
        decision_style=decision_style,
    )
    return {
        "analysis_style_used": str(
            raw.get("analysis_style_used")
            or decision_style.get("analysis_style")
            or "unknown"
        )[:80],
        "trade_style_used": str(
            raw.get("trade_style_used")
            or decision_style.get("trade_style")
            or "unknown"
        )[:80],
        "style_limits_respected": bool(raw.get("style_limits_respected", python_check["compliant"])),
        "news_bias_used": str(raw.get("news_bias_used") or "")[:300],
        "sizing_adjustment": str(raw.get("sizing_adjustment") or "")[:300],
        "blocked_or_clipped_actions": (
            raw.get("blocked_or_clipped_actions")[:8]
            if isinstance(raw.get("blocked_or_clipped_actions"), list)
            else []
        ),
        "known_limitations": (
            raw.get("known_limitations")[:8]
            if isinstance(raw.get("known_limitations"), list)
            else []
        ),
        "python_validation": python_check,
        "style_non_compliant": not python_check["compliant"],
    }


def _check_style_weight_compliance(
    *,
    base_weights: dict,
    adjusted_weights: dict,
    decision_style: dict,
) -> dict:
    violations: list[str] = []
    if not decision_style:
        return {"compliant": True, "violations": [], "checked": False}

    limits = decision_style.get("style_limits") or {}
    max_multiplier = float(limits.get("max_adjustment_multiplier", 1.0) or 1.0)
    max_new_buys = int(float(limits.get("max_new_buys_per_cycle", 999) or 999))
    allow_new = bool(limits.get("allow_new_positions", True))
    trade_style = str(decision_style.get("trade_style") or "")

    max_delta = 0.05 * max_multiplier
    new_buys = 0
    for ticker in set(base_weights) | set(adjusted_weights):
        if ticker == "CASH":
            continue
        base = float(base_weights.get(ticker, 0.0) or 0.0)
        adjusted = float(adjusted_weights.get(ticker, 0.0) or 0.0)
        delta = adjusted - base
        if abs(delta) > max_delta + 1e-6:
            violations.append(
                f"{ticker} delta {delta:.2%} exceeds style max {max_delta:.2%}"
            )
        if base <= 0.01 and adjusted > 0.01:
            new_buys += 1
            if not allow_new:
                violations.append(f"{ticker} new position blocked by style")

    if new_buys > max_new_buys:
        violations.append(f"new buys {new_buys} exceeds style max {max_new_buys}")
    if trade_style == "cash_only":
        equity = sum(float(v or 0.0) for t, v in adjusted_weights.items() if t != "CASH")
        if equity > 1e-6:
            violations.append("cash_only trade style forbids non-cash exposure")

    return {
        "compliant": not violations,
        "violations": violations,
        "checked": True,
        "limits": {
            "trade_style": trade_style,
            "max_adjustment_multiplier": max_multiplier,
            "max_delta_assumption": max_delta,
            "max_new_buys_per_cycle": max_new_buys,
            "allow_new_positions": allow_new,
        },
    }


def _normalize_scorecard_compliance(
    raw: dict,
    *,
    base_weights: dict,
    adjusted_weights: dict,
    market_scorecard: dict,
) -> dict:
    if not isinstance(raw, dict):
        raw = {}
    python_check = _check_scorecard_weight_compliance(
        base_weights=base_weights,
        adjusted_weights=adjusted_weights,
        market_scorecard=market_scorecard,
    )
    return {
        "scorecard_alignment": str(raw.get("scorecard_alignment") or "partially_aligned")[:50],
        "action_permission_used": str(
            raw.get("action_permission_used")
            or market_scorecard.get("investment_permission")
            or "unknown"
        )[:80],
        "data_quality_adjustment": str(raw.get("data_quality_adjustment") or "")[:300],
        "why_this_trade_is_reasonable": str(raw.get("why_this_trade_is_reasonable") or "")[:300],
        "known_limitations": (
            raw.get("known_limitations")[:8]
            if isinstance(raw.get("known_limitations"), list)
            else []
        ),
        "python_validation": python_check,
        "scorecard_non_compliant": not python_check["compliant"],
    }


def _check_scorecard_weight_compliance(
    *,
    base_weights: dict,
    adjusted_weights: dict,
    market_scorecard: dict,
) -> dict:
    violations: list[str] = []
    if not market_scorecard:
        return {"compliant": True, "violations": [], "checked": False}

    max_delta = float(market_scorecard.get("max_adjustment_from_base", 1.0) or 1.0)
    max_equity = float(market_scorecard.get("max_equity_weight", 1.0) or 1.0)
    min_cash = float(market_scorecard.get("min_cash_weight", 0.0) or 0.0)
    allow_new = bool(market_scorecard.get("allow_new_positions", True))
    permission = str(market_scorecard.get("investment_permission") or "")

    for ticker in set(base_weights) | set(adjusted_weights):
        if ticker == "CASH":
            continue
        base = float(base_weights.get(ticker, 0.0) or 0.0)
        adjusted = float(adjusted_weights.get(ticker, 0.0) or 0.0)
        delta = adjusted - base
        if abs(delta) > max_delta + 1e-6:
            violations.append(
                f"{ticker} delta {delta:.2%} exceeds scorecard max {max_delta:.2%}"
            )
        if not allow_new and base <= 0.01 and adjusted > 0.01:
            violations.append(f"{ticker} new position not allowed by scorecard")

    equity = sum(float(v or 0.0) for t, v in adjusted_weights.items() if t != "CASH")
    cash = float(adjusted_weights.get("CASH", 0.0) or 0.0)
    if equity > max_equity + 1e-6:
        violations.append(f"equity {equity:.2%} exceeds scorecard max {max_equity:.2%}")
    if cash < min_cash - 1e-6:
        violations.append(f"cash {cash:.2%} below scorecard floor {min_cash:.2%}")
    if permission == "cash_only" and equity > 1e-6:
        violations.append("cash_only permission forbids non-cash exposure")

    return {
        "compliant": not violations,
        "violations": violations,
        "checked": True,
        "limits": {
            "investment_permission": permission,
            "max_adjustment_from_base": max_delta,
            "max_equity_weight": max_equity,
            "min_cash_weight": min_cash,
            "allow_new_positions": allow_new,
        },
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
        "scorecard_compliance": {
            "scorecard_alignment": "partially_aligned",
            "action_permission_used": "degraded_fallback",
            "data_quality_adjustment": "PM LLM failed; using Stage 2 baseline weights",
            "why_this_trade_is_reasonable": "baseline fallback avoids unsupported LLM changes",
            "known_limitations": [degraded_reason],
            "python_validation": {"compliant": True, "violations": [], "checked": False},
            "scorecard_non_compliant": False,
        },
        "style_compliance": {
            "analysis_style_used": "degraded_fallback",
            "trade_style_used": "hold_unless_strong",
            "style_limits_respected": True,
            "news_bias_used": "PM LLM failed; no discretionary news interpretation applied",
            "sizing_adjustment": "using Stage 2 baseline weights",
            "blocked_or_clipped_actions": [],
            "known_limitations": [degraded_reason],
            "python_validation": {"compliant": True, "violations": [], "checked": False},
            "style_non_compliant": False,
        },
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
