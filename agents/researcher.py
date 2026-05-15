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

# ═══════════════════════════════════════════════════════════════
# Decision Learning helpers (Phase C)
# ═══════════════════════════════════════════════════════════════


async def _retrieve_similar_cases_for_researcher(
    quant_baseline: dict,
    brief: dict,
) -> list[dict]:
    """Retrieve similar historical cases for the current regime + market conditions."""
    try:
        from services.similar_case_retrieval import get_similar_cases_for_researcher

        regime = (quant_baseline.get("regime_result") or {}).get("regime", "")
        if not regime:
            return []

        key_facts = brief.get("key_facts") or {}
        market_conditions = {
            "vix": None,
            "drawdown_pct": key_facts.get("drawdown_pct"),
            "breadth_pct": key_facts.get("breadth_pct"),
        }

        # Fetch VIX from system_config (same source as circuit_breaker uses)
        try:
            from db.session import AsyncSessionLocal
            from db.queries import get_system_config

            async with AsyncSessionLocal() as db:
                vix_cfg = await get_system_config(db, "last_vix")
            if vix_cfg:
                market_conditions["vix"] = (
                    float((vix_cfg.value or {}).get("value", 0) or 0) or None
                )
        except Exception:
            pass

        return await get_similar_cases_for_researcher(
            regime, market_conditions, max_cases=5
        )
    except Exception as e:
        logger.warning(
            f"[RESEARCHER] similar_case_retrieval failed (non-fatal): {e}"
        )
        return []


async def _read_calibration_bias() -> dict | None:
    """Read researcher_confidence_bias from system_config."""
    try:
        from db.session import AsyncSessionLocal
        from db.queries import get_system_config

        async with AsyncSessionLocal() as db:
            cfg = await get_system_config(db, "researcher_confidence_bias")
        if not cfg or not cfg.value:
            return None
        val = cfg.value or {}
        if val.get("sample_size", 0) < 10:
            return None
        return val
    except Exception as e:
        logger.warning(
            f"[RESEARCHER] calibration bias read failed (non-fatal): {e}"
        )
        return None


def _build_calibration_section(bias: dict | None) -> str:
    """Build calibration guidance section for the RESEARCHER prompt."""
    if not bias:
        return ""

    multipliers = bias.get("bias_multipliers", {})
    accuracy = bias.get("per_level_accuracy", {})
    sample = bias.get("sample_size", 0)
    recs = bias.get("recommendations", [])
    h = multipliers.get("high", 1.0)
    m = multipliers.get("medium", 1.0)

    if all(abs(x - 1.0) < 0.05 for x in [h, m, multipliers.get("low", 1.0)]):
        return ""  # well-calibrated, no guidance needed

    lines = [
        "\n\n## HISTORICAL CONFIDENCE CALIBRATION "
        f"(based on {sample} past decisions with known outcomes)",
    ]

    if h < 0.85:
        lines.append(
            f"- HIGH confidence: historically OVERCONFIDENT "
            f"(actual accuracy {accuracy.get('high', 0):.0%}, expected 70%+). "
            f"Consider reporting medium instead of high when in doubt."
        )
    elif h > 1.15:
        lines.append(
            f"- HIGH confidence: historically UNDERCONFIDENT "
            f"(actual accuracy {accuracy.get('high', 0):.0%})."
        )

    if m < 0.85:
        lines.append(
            f"- MEDIUM confidence: historically OVERCONFIDENT "
            f"(actual accuracy {accuracy.get('medium', 0):.0%}, expected 55%)."
        )

    if recs:
        lines.append("Calibration summary: " + " | ".join(recs[:2]))

    lines.append(
        "Downstream PM uses your confidence to set max weight adjustments: "
        "high=±5%, medium=±3%, low=±1%. Calibrate honestly."
    )

    return "\n".join(lines)


def _build_similar_cases_section(cases: list[dict] | None) -> str:
    """Build similar historical cases section for the RESEARCHER prompt."""
    if not cases:
        return ""
    from services.similar_case_retrieval import format_cases_for_prompt

    cases_text = format_cases_for_prompt(cases)
    return (
        "\n\n## SIMILAR HISTORICAL CASES (same regime, similar market conditions)\n"
        "These past decisions were made in similar regimes and market conditions.\n"
        "Use them to calibrate your confidence and identify recurring patterns.\n"
        "Do not anchor on them — if current signals clearly differ, trust the current data.\n\n"
        f"{cases_text}\n\n"
        "**Interpretation**: DQS=decision quality score (0–100%). "
        "High-DQS cases with correct direction are the most relevant reference points.\n"
    )


def _get_client() -> AsyncOpenAI:
    global _client
    if _client is None:
        _client = AsyncOpenAI(api_key=settings.openai_api_key)
    return _client


RESEARCHER_OUTPUT_SCHEMA = """
You must output strict JSON in the following schema. Do not include any prefix, markdown, or prose outside JSON.

{
  "market_regime": {
    "assessment": "trending_bull" | "trending_bear" | "high_vol" |
                  "mean_reverting" | "defensive",
    "confidence": "high" | "medium" | "low",
    "alignment_with_quant": "agree" | "disagree" | "partial",
    "disagreement_reason": null | "specific reason if you disagree with the system regime"
  },

  "macro_outlook": {
    "summary": "one-sentence macro summary, <= 25 words",
    "confidence": "high" | "medium" | "low",
    "key_drivers": [
      {
        "driver": "driver name",
        "direction": "positive" | "negative" | "neutral",
        "time_horizon": "immediate" | "short_term" | "medium_term",
        "confidence": "high" | "medium" | "low"
      }
    ],
    "data_quality": "fresh" | "stale" | "missing",
    "data_gaps": ["data gap description, if any"]
  },

  "ticker_signals": {
    "<TICKER>": {
      "overall_signal": "bullish" | "bearish" | "neutral",
      "confidence": "high" | "medium" | "low",
      "signal_sources": {
        "quant_score": "high" | "medium" | "low" | null,
        "news_sentiment": "positive" | "negative" | "neutral" | null,
        "macro_alignment": "tailwind" | "headwind" | "neutral" | null
      },
      "confidence_drivers": {
        "supporting_count": <int>,
        "conflicting_signals": ["specific conflicting signals, if any"]
      },
      "note": null | "short note, only when needed"
    }
  },

  "cross_signal_insights": [
    {
      "insight": "cross-asset or cross-sector insight, <= 25 words",
      "confidence": "high" | "medium" | "low",
      "affected_tickers": ["TICKER1", "TICKER2"],
      "actionable": true | false
    }
  ],

  "overall_confidence": "high" | "medium" | "low",
  "low_confidence_reasons": ["reason if overall_confidence is low"]
}

Confidence rules:
- high: multiple independent signals agree, data is fresh (<4h), and there are no major contradictions.
- medium: some signals agree, data is mildly stale (4-12h), or there are minor conflicts.
- low: signals conflict, data is stale (>12h), or important information is missing.

Only include tickers where you have a substantive view based on news, macro, or quant anomalies.
For other tickers, omit the entry rather than manufacturing a neutral signal.
"""

RESEARCHER_CONFIDENCE_INSTRUCTION = """
## Important confidence instructions

Your confidence score is used directly by the downstream PM agent:
- ticker_signals with confidence=high allow PM weight adjustments up to +/-5%.
- ticker_signals with confidence=medium allow PM weight adjustments up to +/-3%.
- ticker_signals with confidence=low allow only minor adjustments up to +/-1%.

Do not inflate confidence to appear useful. It is better to report low confidence
with honest data_gaps than to pretend certainty when information is insufficient.
"""

SYSTEM_PROMPT = """You are the chief market analyst (Stage 3 RESEARCHER) for a quantitative trading system.

Your place in the pipeline:
    Upstream Stage 2 is the Python quant baseline (base_weights, scoring_breakdown).
    Downstream Stage 4a/4b are Bull/Bear; they build long/short arguments from your report.

Task - analyze only, no decision:
    Combine quant factors + news + macro + calendar into structured signal assessments per ticker.
    Do not output position weights; output an objective market analysis report only.

Output rules:
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

""" + RESEARCHER_OUTPUT_SCHEMA + """

""" + RESEARCHER_CONFIDENCE_INSTRUCTION + """

JSON only. Any extra text is an error."""


# ═══════════════════════════════════════════════════════════════
# Main entry
# ═══════════════════════════════════════════════════════════════


async def run_researcher_async(
    pipeline_context: dict,
    brief: dict,
    quant_baseline: dict,
    regime_result: dict | None = None,
) -> dict:
    """Stage 3: synthesize information from baseline + brief → research_report."""
    # Phase C pre-steps: retrieve similar historical cases + read calibration bias
    similar_cases = await _retrieve_similar_cases_for_researcher(quant_baseline, brief)
    calibration_bias = await _read_calibration_bias()

    user_payload = _build_user_message(
        brief, quant_baseline, regime_result, similar_cases, calibration_bias
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
            result = _validate_and_normalize(parsed, quant_baseline)
            result["_token_usage"] = {
                "prompt_tokens": resp.usage.prompt_tokens,
                "completion_tokens": resp.usage.completion_tokens,
            }
            return result

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


def _build_user_message(
    brief: dict,
    quant_baseline: dict,
    regime_result: dict | None = None,
    similar_cases: list[dict] | None = None,
    calibration_bias: dict | None = None,
) -> str:
    prose     = brief.get("prose_summary") or "(none)"
    macro     = brief.get("macro_news_section") or "(none)"
    calendar  = brief.get("calendar_section") or "(none)"
    key_facts = brief.get("key_facts") or {}
    sector_rotation = brief.get("sector_rotation") or {}
    sector_rotation_section = brief.get("sector_rotation_section") or "(none)"
    feature_provenance = brief.get("feature_provenance") or {}
    evidence_bundle = brief.get("evidence_bundle") or {}
    market_scorecard = brief.get("market_scorecard") or {}
    news_evidence = brief.get("news_evidence") or evidence_bundle.get("news_evidence") or {}
    decision_style = brief.get("decision_style") or evidence_bundle.get("decision_style") or {}

    base_weights    = quant_baseline.get("base_weights") or {}
    current_weights = brief.get("current_weights") or {}
    scoring         = quant_baseline.get("scoring_breakdown") or []
    ranking         = quant_baseline.get("ranking_summary") or {}

    per_ticker_news = brief.get("per_ticker_news") or {}
    news_block = _format_per_ticker_news(per_ticker_news)

    # Memory context block (from context_assembler, injected during market_brief)
    memory_context = brief.get("memory_context") or {}
    memory_section = ""
    if memory_context.get("has_memory"):
        memory_section = (
            "\n\n## HISTORICAL MEMORY CONTEXT (for reference only — do not be dominated by it)\n\n"
            f"**Recent Regime Trend**: {memory_context.get('regime_trend', 'none')}\n\n"
            f"{memory_context.get('memory_prose', '')}\n\n"
            "**Note**: Historical memory is for reference only. "
            "If current market signals clearly differ from historical patterns, "
            "prioritize the current data.\n"
        )

    # Regime constraint block
    regime_block = ""
    if regime_result:
        regime_block = (
            "## SYSTEM REGIME CONSTRAINT (hard classification, do not override)\n"
            f"Regime: {regime_result.get('regime', 'unknown')}\n"
            f"Confidence: {regime_result.get('confidence', 'unknown')}\n"
            f"Constraint: {regime_result.get('constraints', {}).get('llm_instruction', '')}\n\n"
        )

    return (
        f"{regime_block}"
        "## Market condition scorecard (deterministic Python permission layer)\n"
        f"{json.dumps(market_scorecard, ensure_ascii=False, indent=2)}\n\n"
        "Use this scorecard as the system's auditable market-state contract. "
        "Your analysis may disagree with the interpretation, but you must explicitly "
        "explain any disagreement and cannot ignore data-quality or permission warnings.\n\n"
        "## Evidence bundle summary\n"
        f"{json.dumps(_compact_evidence_bundle(evidence_bundle), ensure_ascii=False, indent=2)}\n\n"
        "Use strategy_use_summary and strategy_confidence as structured strategy evidence. "
        "primary/advisory strategies may support confidence; watch_only/ignore strategies "
        "must not be treated as action signals. Reason codes explain why a strategy is "
        "discounted or usable.\n\n"
        "## Structured news evidence (deterministic action-bias layer)\n"
        f"{json.dumps(_compact_news_evidence(news_evidence), ensure_ascii=False, indent=2)}\n\n"
        "## Decision style (deterministic analysis/execution style)\n"
        f"{json.dumps(decision_style, ensure_ascii=False, indent=2)}\n\n"
        "Use decision_style to calibrate how strongly you interpret evidence. "
        "Do not convert it into weights; only reflect it in confidence, data gaps, "
        "and cross-signal insights.\n\n"
        "## Market technicals\n"
        f"{prose}\n\n"
        "## Quantitative facts\n"
        f"{json.dumps(key_facts, ensure_ascii=False, indent=2)}\n\n"
        "## Sector and factor rotation\n"
        f"{sector_rotation_section}\n\n"
        "Structured rotation signal:\n"
        f"{json.dumps(sector_rotation, ensure_ascii=False, indent=2)}\n\n"
        "## Data provenance and freshness\n"
        f"{json.dumps(feature_provenance, ensure_ascii=False, indent=2)}\n\n"
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
        "cross_signal_insights. Analyze only — no trading decision. Include scorecard "
        "alignment or disagreement and decision_style effects inside the relevant "
        "market_regime disagreement_reason, macro_outlook data_gaps, and "
        "cross_signal_insights fields. JSON only."
        + _build_calibration_section(calibration_bias)
        + _build_similar_cases_section(similar_cases)
        + memory_section
        + _build_earnings_section(memory_context)
        + _build_macro_section(memory_context)
        + _build_scenario_section(brief)
    )


def _compact_evidence_bundle(bundle: dict) -> dict:
    if not bundle:
        return {}
    return {
        "generated_at": bundle.get("generated_at"),
        "max_age_seconds": bundle.get("max_age_seconds"),
        "market": bundle.get("market") or {},
        "rotation": bundle.get("rotation") or {},
        "news": {
            "data_quality": (bundle.get("news") or {}).get("data_quality"),
            "warnings": (bundle.get("news") or {}).get("warnings") or [],
            "hard_risk_tickers": (bundle.get("news") or {}).get("hard_risk_tickers") or [],
            "per_ticker_news_count": (bundle.get("news") or {}).get("per_ticker_news_count"),
        },
        "strategies": {
            "playground_available": (bundle.get("strategies") or {}).get("playground_available"),
            "snapshot_count": (bundle.get("strategies") or {}).get("snapshot_count"),
            "forward_return_samples": (bundle.get("strategies") or {}).get("forward_return_samples"),
            "data_quality": (bundle.get("strategies") or {}).get("data_quality"),
            "consensus_top5": (bundle.get("strategies") or {}).get("consensus_top5") or [],
            "turnover_warnings": (bundle.get("strategies") or {}).get("turnover_warnings") or [],
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
    if not news_evidence:
        return {}
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

# New regime values (Task 7)
_VALID_REGIMES_NEW = {
    "trending_bull", "trending_bear", "high_vol",
    "mean_reverting", "defensive",
}
# Legacy regime mapping for downstream compatibility
_REGIME_MAP = {
    "trending_bull": "bull_trend",
    "trending_bear": "bear_trend",
    "high_vol": "high_vol",
    "mean_reverting": "neutral",
    "defensive": "neutral",
}
_VALID_SIGNALS = {"strong_positive", "positive", "neutral", "negative", "strong_negative"}
_VALID_SIGNALS_NEW = {"bullish", "bearish", "neutral"}


def _validate_and_normalize(out: dict, quant_baseline: dict) -> dict:
    """Validate and normalize LLM output with new confidence schema (Task 7)."""
    # market_regime
    mr = out.get("market_regime") or {}
    assessment = str(mr.get("assessment", "neutral")).strip()
    if assessment not in _VALID_REGIMES_NEW:
        assessment = "mean_reverting"

    # confidence is now a string: "high" | "medium" | "low"
    conf_str = str(mr.get("confidence", "medium")).strip()
    if conf_str not in ("high", "medium", "low"):
        conf_str = "medium"
    conf_map = {"high": 0.9, "medium": 0.5, "low": 0.3}
    confidence = conf_map.get(conf_str, 0.5)

    alignment = str(mr.get("alignment_with_quant", "agree")).strip()
    if alignment not in ("agree", "disagree", "partial"):
        alignment = "agree"
    disagreement_reason = mr.get("disagreement_reason")

    # macro_outlook
    mo = out.get("macro_outlook") or {}
    macro_summary = str(mo.get("summary", ""))[:200]

    # key_drivers (new format) — convert to key_events for downstream
    raw_drivers = mo.get("key_drivers") or []
    key_events_rich: list[dict] = []
    key_events_keywords: list[str] = []
    if isinstance(raw_drivers, list):
        for d in raw_drivers[:5]:
            if isinstance(d, dict):
                driver_name = str(d.get("driver", "")).strip()
                direction = str(d.get("direction", "neutral")).strip()
                horizon = str(d.get("time_horizon", "short_term")).strip()
                d_conf = str(d.get("confidence", "medium")).strip()
                if driver_name:
                    key_events_rich.append({
                        "keyword": driver_name,
                        "freshness": horizon,
                        "magnitude": direction,
                        "description": f"{driver_name} ({direction})",
                    })
                    key_events_keywords.append(driver_name)
    if not key_events_rich:
        key_events_rich = [{
            "keyword": "normal market conditions",
            "freshness": "ongoing",
            "magnitude": "low",
            "description": "no significant macro events",
        }]
        key_events_keywords = ["normal market conditions"]

    data_quality = str(mo.get("data_quality", "fresh")).strip()
    if data_quality not in ("fresh", "stale", "missing"):
        data_quality = "fresh"
    data_gaps = mo.get("data_gaps") or []
    if not isinstance(data_gaps, list):
        data_gaps = []

    # ticker_signals — new dict format (keyed by ticker)
    raw_signals = out.get("ticker_signals") or {}
    ticker_signals_dict: dict[str, dict] = {}  # new format with confidence
    ticker_signals_list: list[dict] = []      # legacy list format for downstream

    if isinstance(raw_signals, dict):
        for ticker, sig in raw_signals.items():
            if not isinstance(sig, dict):
                continue
            t = str(ticker).upper().strip()
            if not t:
                continue

            overall = str(sig.get("overall_signal", "neutral")).strip()
            if overall not in _VALID_SIGNALS_NEW:
                overall = "neutral"
            sig_conf = str(sig.get("confidence", "medium")).strip()
            if sig_conf not in ("high", "medium", "low"):
                sig_conf = "medium"

            signal_sources = sig.get("signal_sources") or {}
            quant_src = str(signal_sources.get("quant_score") or "medium").strip()
            news_src = str(signal_sources.get("news_sentiment") or "neutral").strip()
            macro_src = str(signal_sources.get("macro_alignment") or "neutral").strip()

            conf_drivers = sig.get("confidence_drivers") or {}
            supporting = int(conf_drivers.get("supporting_count") or 0)
            conflicts = conf_drivers.get("conflicting_signals") or []
            if not isinstance(conflicts, list):
                conflicts = []

            note = sig.get("note")

            # Build new-format entry
            ticker_signals_dict[t] = {
                "overall_signal": overall,
                "confidence": sig_conf,
                "signal_sources": {
                    "quant_score": quant_src,
                    "news_sentiment": news_src,
                    "macro_alignment": macro_src,
                },
                "confidence_drivers": {
                    "supporting_count": supporting,
                    "conflicting_signals": conflicts,
                },
                "note": note,
            }

            # Legacy list format for downstream Bull/Bear/Synthesizer
            # Map overall_signal to combined_signal
            combined_map = {"bullish": "positive", "bearish": "negative", "neutral": "neutral"}
            combined = combined_map.get(overall, "neutral")
            ticker_signals_list.append({
                "ticker": t,
                "combined_signal": combined,
                "confidence": sig_conf,
                "news_sentiment": news_src if news_src != "null" else "neutral",
                "note": note,
            })
    elif isinstance(raw_signals, list):
        # Legacy list format (fallback for degraded upstream)
        for sig in raw_signals:
            if not isinstance(sig, dict) or not sig.get("ticker"):
                continue
            combined = str(sig.get("combined_signal", "neutral")).strip()
            if combined not in _VALID_SIGNALS:
                combined = "neutral"
            t = str(sig["ticker"]).upper().strip()
            ticker_signals_list.append({
                "ticker": t,
                "combined_signal": combined,
                "confidence": "medium",
                "news_sentiment": str(sig.get("news_sentiment", "neutral")).strip(),
                "note": sig.get("flag") or sig.get("note"),
            })
            ticker_signals_dict[t] = {
                "overall_signal": combined,
                "confidence": "medium",
                "signal_sources": {
                    "quant_score": "medium",
                    "news_sentiment": str(sig.get("news_sentiment", "neutral")).strip(),
                    "macro_alignment": "neutral",
                },
                "confidence_drivers": {"supporting_count": 1, "conflicting_signals": []},
                "note": sig.get("note"),
            }

    # cross_signal_insights — new format: list of dicts with insight/confidence/affected_tickers/actionable
    raw_insights = out.get("cross_signal_insights") or []
    insights_list: list[dict] = []
    insights_str_list: list[str] = []  # legacy string list for downstream

    if isinstance(raw_insights, list):
        for item in raw_insights:
            if isinstance(item, dict):
                insight_text = str(item.get("insight", "")).strip()
                ins_conf = str(item.get("confidence", "medium")).strip()
                if ins_conf not in ("high", "medium", "low"):
                    ins_conf = "medium"
                affected = item.get("affected_tickers") or []
                if not isinstance(affected, list):
                    affected = []
                actionable = bool(item.get("actionable", False))
                insights_list.append({
                    "insight": insight_text,
                    "confidence": ins_conf,
                    "affected_tickers": affected,
                    "actionable": actionable,
                })
                insights_str_list.append(insight_text)
            elif isinstance(item, str) and item.strip():
                insights_str_list.append(item.strip())

    # overall_confidence + low_confidence_reasons
    overall_conf = str(out.get("overall_confidence", "medium")).strip()
    if overall_conf not in ("high", "medium", "low"):
        overall_conf = "medium"
    low_reasons = out.get("low_confidence_reasons") or []
    if not isinstance(low_reasons, list):
        low_reasons = []

    # Return: new format fields + legacy-compatible fields for downstream
    return {
        # New format fields (Task 7)
        "market_regime": {
            "assessment": assessment,
            "confidence": conf_str,
            "alignment_with_quant": alignment,
            "disagreement_reason": disagreement_reason,
            # Legacy compat
            "regime": _REGIME_MAP.get(assessment, "neutral"),
            "evidence": str(mr.get("disagreement_reason") or "")[:200],
        },
        "macro_outlook": {
            "summary": macro_summary,
            "confidence": conf_str,
            "key_drivers": raw_drivers if isinstance(raw_drivers, list) else [],
            "data_quality": data_quality,
            "data_gaps": data_gaps,
            # Legacy compat
            "key_events": key_events_rich,
            "key_keywords": key_events_keywords,
            "impact_bias": "neutral",
        },
        "ticker_signals_dict": ticker_signals_dict,  # New dict format with confidence
        "ticker_signals": ticker_signals_list,       # Legacy list for downstream
        "cross_signal_insights_list": insights_list,  # New structured format
        "cross_signal_insights": insights_str_list,   # Legacy string list for downstream
        "overall_confidence": overall_conf,
        "low_confidence_reasons": low_reasons,
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
    ticker_signals_list: list[dict] = []
    ticker_signals_dict: dict[str, dict] = {}

    for i, item in enumerate(scoring):
        if not isinstance(item, dict):
            continue
        ticker = str(item.get("ticker", "")).upper()
        if not ticker or ticker == "CASH":
            continue
        ticker_signals_list.append({
            "ticker":          ticker,
            "combined_signal": "neutral",
            "confidence":      "low",
            "news_sentiment":  "neutral",
            "note":            None,
        })
        ticker_signals_dict[ticker] = {
            "overall_signal": "neutral",
            "confidence":      "low",
            "signal_sources": {
                "quant_score":     "medium",
                "news_sentiment":  "neutral",
                "macro_alignment": "neutral",
            },
            "confidence_drivers": {
                "supporting_count": 0,
                "conflicting_signals": ["no news synthesis — LLM degraded"],
            },
            "note": None,
        }

    degraded_reason = f"LLM degraded: could not synthesize news signals (error={error})"
    return {
        "market_regime": {
            "assessment": "mean_reverting",
            "confidence": "low",
            "alignment_with_quant": "agree",
            "disagreement_reason": degraded_reason,
            "regime": "neutral",
            "evidence": degraded_reason,
        },
        "macro_outlook": {
            "summary":      "LLM degraded — no macro analysis",
            "confidence":   "low",
            "key_drivers":   [],
            "data_quality":  "missing",
            "data_gaps":     [degraded_reason],
            "key_events":   [{"keyword": "normal market conditions", "freshness": "ongoing", "magnitude": "low", "description": "no significant macro events"}],
            "key_keywords": ["normal market conditions"],
            "impact_bias":  "neutral",
        },
        "ticker_signals_dict": ticker_signals_dict,
        "ticker_signals":        ticker_signals_list,
        "cross_signal_insights_list": [],
        "cross_signal_insights": [],
        "overall_confidence": "low",
        "low_confidence_reasons": [degraded_reason],
        "used_degraded_fallback": True,
    }


# ── P1-3: Earnings + Macro Context Helpers ──────────────────────────────────────


def _build_earnings_section(memory_context: dict) -> str:
    """Build earnings context section for the RESEARCHER prompt."""
    earnings_ctx = memory_context.get("earnings_context") or {}
    if not earnings_ctx.get("has_earnings"):
        return ""

    return (
        "\n\n## UPCOMING EARNINGS (next 7 days — factor into risk assessment)\n\n"
        f"{earnings_ctx.get('earnings_prose', '')}\n\n"
        "**Note**: Held positions with upcoming earnings may carry elevated risk. "
        "Factor this into your ticker confidence assessments.\n"
    )


def _build_macro_section(memory_context: dict) -> str:
    """Build macro events context section for the RESEARCHER prompt."""
    macro_ctx = memory_context.get("macro_events_context") or {}
    if not macro_ctx.get("has_data"):
        return ""

    parts = []
    if macro_ctx.get("key_dates"):
        parts.append(f"Key dates: {', '.join(macro_ctx['key_dates'])}")

    events = macro_ctx.get("events", [])
    if events:
        event_lines = [f"  - [{e.get('impact', '?')}] {e.get('date', '')} {e.get('event', '')}" for e in events[:5]]
        parts.append(f"Upcoming events:\n" + "\n".join(event_lines))

    if not parts:
        return ""

    return (
        "\n\n## IMPORTANT MACRO EVENTS (next 5 days)\n\n"
        + "\n".join(parts) + "\n\n"
        "**Note**: These events may affect market volatility and sector rotations. "
        "Factor into your macro outlook and risk assessment.\n"
    )


def _build_scenario_section(brief: dict) -> str:
    """Build scenario stress-test results section for the RESEARCHER prompt (P2-2)."""
    scenario_result = brief.get("scenario_result")
    if not scenario_result or not scenario_result.get("results"):
        return ""

    from services.scenario_analyst import build_scenario_context
    return build_scenario_context(scenario_result)
