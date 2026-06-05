# agents/communicator.py
"""
COMMUNICATOR — Telegram copy layer (LLM + mandatory fallback)

Role:
    Turn researcher + strategy + risk structured output into a natural-language card for Telegram.
    Hard 5s timeout; any failure falls back to Python f-string templates.

This layer is **not on the correctness path**: LLM copy affects readability only;
the fallback template must always send a card, even if stiff.
"""
import asyncio
import json
import logging
from typing import Any

from openai import AsyncOpenAI

from config import get_settings

logger   = logging.getLogger("qc_fastapi_2.communicator")
settings = get_settings()
_openai  = AsyncOpenAI(api_key=settings.openai_api_key)

LLM_TIMEOUT_SECONDS = 5.0


COMMUNICATOR_SYSTEM_PROMPT = """You are the Telegram copy editor for a quantitative trading system.
You receive structured data after decisions are made. Turn it into a **concise, professional, human-readable Telegram card**.

Hard format:
- Use HTML (<b>bold</b>), not Markdown
- Total length ≤ 800 characters
- Include 5 blocks: long/short debate summary / market view / rebalance / risk result / command hints (only include command hints if approved is true)
- Debate summary: Bull/Bear confidence and resolution (1–2 lines)
- Include market scorecard condition, permission, data quality, and any clipping/blocking details when present
- Include feature source summary when present, especially live_state/research/fallback/stale details
- Include strategy-use evidence and strategy-use clipping/blocking details when present
- Include evidence cap observe diagnostics when present; make clear they are diagnostic-only
- Include news bias/confidence, analysis style, trade style, and style clipping/blocking details when present
- Do not invent numbers — only repeat fields you are given
- No graphical characters beyond emoji
- CRITICAL: For SEMI_AUTO mode (when auth_mode == "SEMI_AUTO") AND approved is true, you MUST include clickable command options at the end: <b>/confirm</b>  <b>/skip</b>  <b>/pause</b>
- If auth_mode is FULL_AUTO or MANUAL, or approved is false, omit the command options

Output: return the card text only — no explanation, no JSON, no markdown fences."""


def remove_command_hints(text: str) -> str:
    """移除 Telegram 命令提示（/confirm, /skip, /pause），包括可能的 HTML 标签。"""
    import re
    # 移除包含命令的行或短语
    text = re.sub(r'\s*<b>\s*/confirm\s*</b>', '', text, flags=re.IGNORECASE)
    text = re.sub(r'\s*<b>\s*/skip\s*</b>', '', text, flags=re.IGNORECASE)
    text = re.sub(r'\s*<b>\s*/pause\s*</b>', '', text, flags=re.IGNORECASE)
    # 也移除不带标签的纯文本命令
    text = re.sub(r'\s*/confirm\b', '', text, flags=re.IGNORECASE)
    text = re.sub(r'\s*/skip\b', '', text, flags=re.IGNORECASE)
    text = re.sub(r'\s*/pause\b', '', text, flags=re.IGNORECASE)
    # 移除可能残留的空行
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


async def run_communicator_async(
    pipeline_context: dict,
    researcher_out:   dict,
    risk_out:         dict,
) -> dict:
    """
    Build Telegram copy.
    Returns: {"text": str, "used_fallback": bool}

    target_weights / rebalance_actions / overlays_applied all come from
    risk_out (Stage 4 Risk Manager final execution plan).
    """
    payload = _build_payload(pipeline_context, researcher_out, risk_out)
    if not payload.get("approved", False):
        return {"text": _fallback_template(payload), "used_fallback": True}

    try:
        text = await asyncio.wait_for(_llm_format(payload), timeout=LLM_TIMEOUT_SECONDS)
        if text and text.strip():
            # 移除未批准或非 SEMI_AUTO 模式下的命令提示
            auth_mode = payload.get("auth_mode", "SEMI_AUTO")
            approved = payload.get("approved", False)

            # 如果未批准或非 SEMI_AUTO 模式，移除任何命令提示
            if not approved or auth_mode != "SEMI_AUTO":
                text = remove_command_hints(text)
            return {"text": text.strip(), "used_fallback": False}
        logger.warning("COMMUNICATOR: empty LLM response, falling back")
    except asyncio.TimeoutError:
        logger.warning(f"COMMUNICATOR: LLM timeout after {LLM_TIMEOUT_SECONDS}s, falling back")
    except Exception as e:
        logger.warning(f"COMMUNICATOR: LLM failed ({e}), falling back")

    return {"text": _fallback_template(payload), "used_fallback": True}


# ─────────────────────────────── LLM path ───────────────────────────────


async def _llm_format(payload: dict) -> str:
    resp = await _openai.chat.completions.create(
        model=settings.openai_model,   # gpt-4o-mini
        messages=[
            {"role": "system", "content": COMMUNICATOR_SYSTEM_PROMPT},
            {"role": "user",   "content": json.dumps(payload, ensure_ascii=False)},
        ],
        temperature=0.3,
        max_tokens=400,
    )
    return (resp.choices[0].message.content or "").strip()


# ─────────────────────────────── Payload builder ───────────────────────────────


def _build_payload(
    pipeline_context: dict,
    researcher_out:   dict,
    risk_out:         dict,
) -> dict:
    mj = researcher_out.get("market_judgment", {}) or {}
    debate = researcher_out.get("debate_summary") or {}
    scorecard = pipeline_context.get("market_scorecard") or {}
    news_evidence = pipeline_context.get("news_evidence") or {}
    decision_style = pipeline_context.get("decision_style") or {}
    evidence_bundle = pipeline_context.get("evidence_bundle") or {}
    bundle_data_quality = evidence_bundle.get("data_quality") or {}
    source_timestamps = evidence_bundle.get("source_timestamps") or {}
    feature_provenance = pipeline_context.get("feature_provenance") or {}
    knowledge = evidence_bundle.get("knowledge") or {}
    strategies = evidence_bundle.get("strategies") or {}
    enforcement = risk_out.get("scorecard_enforcement") or {}
    style_enforcement = risk_out.get("style_enforcement") or {}
    position_governance = risk_out.get("position_governance") or {}
    decision_ledger = risk_out.get("decision_ledger") or {}
    strategy_use_enforcement = (
        researcher_out.get("strategy_use_enforcement")
        or pipeline_context.get("strategy_use_enforcement")
        or {}
    )
    compliance = enforcement.get("post_clip_compliance") or {}
    style_compliance = style_enforcement.get("post_clip_compliance") or {}
    macro_news = news_evidence.get("macro_news_score") or {}
    synth_style = researcher_out.get("style_compliance") or {}
    proposal_shaping = researcher_out.get("proposal_shaping") or {}
    return {
        "approved":         bool(risk_out.get("approved", False)),
        "regime":           mj.get("regime", "neutral"),
        "confidence":       round(float(mj.get("adjusted_confidence", 0) or 0), 2),
        "stance":           researcher_out.get("recommended_stance", "maintain"),
        "reasoning":        (researcher_out.get("reasoning") or "")[:200],
        "decision_rationale": (researcher_out.get("decision_rationale") or "")[:400],
        "target_weights":   risk_out.get("target_weights", {}),
        "rebalance_actions":risk_out.get("rebalance_actions", []),
        "estimated_cost":   risk_out.get("estimated_cost_pct", 0),
        "overlays_applied": risk_out.get("overlays_applied", []),
        "rejection_reasons":risk_out.get("rejection_reasons", []),
        "market_scorecard": {
            "market_condition": scorecard.get("market_condition"),
            "investment_permission": scorecard.get("investment_permission"),
            "confidence": scorecard.get("confidence"),
            "data_quality": scorecard.get("data_quality"),
            "dominant_constraint": scorecard.get("dominant_constraint"),
            "require_human_confirmation": scorecard.get("require_human_confirmation"),
            "confirmation_classes": list(scorecard.get("confirmation_classes") or []),
            "reasons": (scorecard.get("reasons") or [])[:3],
            "warnings": (scorecard.get("warnings") or [])[:3],
        },
        "data_quality_detail": {
            "overall": bundle_data_quality.get("overall"),
            "warnings": (bundle_data_quality.get("warnings") or [])[:3],
            "source_timestamps": source_timestamps,
            "feature_source_counts": feature_provenance.get("source_counts") or {},
            "feature_authority_counts": feature_provenance.get("authority_counts") or {},
            "stale_fields": feature_provenance.get("stale_fields") or {},
            "has_stale_fields": feature_provenance.get("has_stale_fields"),
            "qc_snapshots": strategies.get("snapshot_count"),
            "qc_forward_samples": strategies.get("forward_return_samples"),
            "historical_snapshots": strategies.get("historical_snapshot_count"),
            "historical_forward_samples": strategies.get("historical_forward_return_samples"),
            "strategy_data_quality": strategies.get("data_quality"),
            "news_data_quality": macro_news.get("data_quality"),
            "evidence_summary": strategies.get("evidence_summary") or {},
        },
        "scorecard_enforcement": {
            "applied": enforcement.get("applied"),
            "violations": enforcement.get("violations") or [],
            "pre_clip": enforcement.get("target_weights_pre_scorecard_clip") or {},
            "post_clip": enforcement.get("target_weights_post_scorecard_clip") or {},
            "post_clip_compliant": compliance.get("compliant"),
        },
        "news_evidence": {
            "overall_bias": macro_news.get("overall_bias"),
            "confidence": macro_news.get("confidence"),
            "market_impact": macro_news.get("market_impact"),
            "data_quality": macro_news.get("data_quality"),
            "hard_risk_events": news_evidence.get("hard_risk_events") or {},
            "data_gaps": (news_evidence.get("data_gaps") or [])[:3],
        },
        "decision_style": {
            "analysis_style": decision_style.get("analysis_style"),
            "trade_style": decision_style.get("trade_style"),
            "style_reason": decision_style.get("style_reason"),
            "dominant_style_constraint": decision_style.get("dominant_style_constraint"),
            "weighted_conviction": decision_style.get("weighted_conviction"),
            "style_limits": decision_style.get("style_limits") or {},
        },
        "style_compliance": {
            "analysis_style_used": synth_style.get("analysis_style_used"),
            "trade_style_used": synth_style.get("trade_style_used"),
            "news_bias_used": synth_style.get("news_bias_used"),
            "sizing_adjustment": synth_style.get("sizing_adjustment"),
            "blocked_or_clipped_actions": synth_style.get("blocked_or_clipped_actions") or [],
            "style_non_compliant": synth_style.get("style_non_compliant"),
        },
        "proposal_shaping": {
            "applied": proposal_shaping.get("applied"),
            "clip_log": proposal_shaping.get("clip_log") or [],
            "constraints": proposal_shaping.get("constraints") or {},
        },
        "style_enforcement": {
            "applied": style_enforcement.get("applied"),
            "violations": style_enforcement.get("violations") or [],
            "pre_clip": style_enforcement.get("target_weights_pre_style_clip") or {},
            "post_clip": style_enforcement.get("target_weights_post_style_clip") or {},
            "post_clip_compliant": style_compliance.get("compliant"),
            "one_way_tightening_ok": style_enforcement.get("one_way_tightening_ok"),
        },
        "strategy_use_enforcement": {
            "applied": strategy_use_enforcement.get("applied"),
            "violations": strategy_use_enforcement.get("violations") or strategy_use_enforcement.get("clip_log") or [],
            "strategy_use_summary": strategy_use_enforcement.get("strategy_use_summary") or {},
            "evidence_summary": strategy_use_enforcement.get("evidence_summary") or {},
            "pre_clip": strategy_use_enforcement.get("target_weights_pre_strategy_use_clip") or {},
            "post_clip": strategy_use_enforcement.get("target_weights_post_strategy_use_clip") or {},
        },
        "evidence_cap_observe": _compact_evidence_cap_observe(
            cap_diagnostics=(
                risk_out.get("evidence_cap_diagnostics")
                or strategies.get("evidence_cap_diagnostics")
                or {}
            ),
            vote_summary=strategies.get("evidence_vote_summary") or {},
            strategy_results=strategies.get("strategy_results") or [],
        ),
        "execution_gateway": strategies.get("execution_gateway") or {},
        "strategy_certification": _compact_strategy_certification(
            strategies.get("strategy_certification") or {}
        ),
        "knowledge_resolution": _compact_knowledge_resolution(
            knowledge.get("resolution") or {},
            strategies.get("strategy_confidence_calibration") or knowledge.get("strategy_confidence_calibration") or {},
        ),
        "position_governance": {
            "mode": position_governance.get("mode"),
            "position_decisions": (position_governance.get("position_decisions") or [])[:8],
            "blocked_actions": (position_governance.get("blocked_actions") or [])[:8],
            "forced_trims": (position_governance.get("forced_trims") or [])[:8],
            "replacements": (position_governance.get("replacements") or [])[:8],
            "advisory_overrides": (position_governance.get("advisory_overrides") or [])[:8],
            "manual_action_hints": (position_governance.get("manual_action_hints") or [])[:8],
            "trade_summary": position_governance.get("trade_summary") or {},
            "portfolio_summary": position_governance.get("portfolio_summary") or {},
        },
        "decision_ledger": _compact_decision_ledger(decision_ledger),
        "portfolio_construction_evaluation": _compact_portfolio_construction_evaluation(
            risk_out.get("portfolio_construction_evaluation") or {}
        ),
        "portfolio_construction_readiness": _compact_portfolio_construction_readiness(
            risk_out.get("portfolio_construction_readiness") or {}
        ),
        "portfolio_construction_promotion_gate": _compact_portfolio_construction_promotion_gate(
            risk_out.get("portfolio_construction_promotion_gate") or {}
        ),
        "hedge_intent_outcome": _compact_hedge_intent_outcome(
            risk_out.get("hedge_intent_outcome") or {}
        ),
        "final_validation": _compact_final_validation(risk_out.get("final_validation") or {}),
        "auth_mode":        pipeline_context.get("auth_mode", "SEMI_AUTO"),
        "timeout_minutes":  settings.semi_auto_timeout_minutes,
        "debate_summary":   debate,
    }


# ─────────────────────────────── Fallback template ───────────────────────────────


def _fallback_template(p: dict) -> str:
    approved = p["approved"]
    regime   = p["regime"]
    stance   = p["stance"]
    actions  = p["rebalance_actions"] or []
    cost     = float(p["estimated_cost"] or 0)
    overlays = p["overlays_applied"] or []
    debate   = p.get("debate_summary") or {}
    scorecard = p.get("market_scorecard") or {}
    data_quality_detail = p.get("data_quality_detail") or {}
    enforcement = p.get("scorecard_enforcement") or {}
    news = p.get("news_evidence") or {}
    style = p.get("decision_style") or {}
    proposal_shaping = p.get("proposal_shaping") or {}
    style_enforcement = p.get("style_enforcement") or {}
    strategy_use_enforcement = p.get("strategy_use_enforcement") or {}
    evidence_cap_observe = p.get("evidence_cap_observe") or {}
    execution_gateway = p.get("execution_gateway") or {}
    strategy_certification = p.get("strategy_certification") or {}
    knowledge_resolution = p.get("knowledge_resolution") or {}
    position_governance = p.get("position_governance") or {}
    decision_ledger = p.get("decision_ledger") or {}
    pc_eval = p.get("portfolio_construction_evaluation") or {}
    pc_readiness = p.get("portfolio_construction_readiness") or {}
    pc_gate = p.get("portfolio_construction_promotion_gate") or {}
    hedge_outcome = p.get("hedge_intent_outcome") or {}
    final_validation = p.get("final_validation") or {}

    up, down = "\u25b2", "\u25bc"

    debate_line = ""
    if debate:
        bull_conf = debate.get("bull_confidence", 0)
        bear_conf = debate.get("bear_confidence", 0)
        bull_st   = debate.get("bull_stance", "?")
        bear_st   = debate.get("bear_stance", "?")
        resolution = debate.get("resolution", "")[:80]
        debate_line = (
            f"<b>Long / short debate</b>\n"
            f"  {up} Bull: {bull_st} ({bull_conf:.0%}) "
            f"vs {down} Bear: {bear_st} ({bear_conf:.0%})\n"
        )
        if resolution:
            debate_line += f"  → {resolution}\n"
        debate_line += "\n"

    scorecard_line = _format_scorecard_line(scorecard)
    data_quality_line = _format_data_quality_detail_line(data_quality_detail)
    feature_source_line = _format_feature_source_summary_line(data_quality_detail)
    enforcement_line = _format_enforcement_line(enforcement)
    news_line = _format_news_line(news)
    style_line = _format_style_line(style)
    proposal_shaping_line = _format_proposal_shaping_line(proposal_shaping)
    style_enforcement_line = _format_style_enforcement_line(style_enforcement)
    strategy_use_line = _format_strategy_use_enforcement_line(strategy_use_enforcement)
    evidence_cap_line = _format_evidence_cap_observe_line(evidence_cap_observe)
    execution_gateway_line = _format_execution_gateway_line(execution_gateway)
    strategy_certification_line = _format_strategy_certification_line(strategy_certification)
    knowledge_line = _format_knowledge_resolution_line(knowledge_resolution)
    pc_eval_line = _format_portfolio_construction_evaluation_line(pc_eval)
    pc_readiness_line = _format_portfolio_construction_readiness_line(pc_readiness)
    pc_gate_line = _format_portfolio_construction_promotion_gate_line(pc_gate)
    hedge_outcome_line = _format_hedge_intent_outcome_line(hedge_outcome)
    final_validation_line = _format_final_validation_line(final_validation)
    decision_ledger_line = _format_decision_ledger_line(decision_ledger)
    position_governance_line = _format_position_governance_line(position_governance)

    if not approved:
        reasons_text = (
            "\n".join(f"  - {_display_rejection_reason(r)}" for r in p["rejection_reasons"])
            or "  - No reason provided"
        )
        return (
            f"🚫 <b>Rebalance rejected by risk</b>\n"
            f"――――――――――――――――\n"
            f"{debate_line}"
            f"🌡️ Regime: {regime}\n"
            f"📊 Stance: {stance}\n\n"
            f"{news_line}"
            f"{style_line}"
            f"{proposal_shaping_line}"
            f"{scorecard_line}"
            f"{data_quality_line}"
            f"{feature_source_line}"
            f"{enforcement_line}"
            f"{style_enforcement_line}"
            f"{strategy_use_line}"
            f"{evidence_cap_line}"
            f"{execution_gateway_line}"
            f"{strategy_certification_line}"
            f"{knowledge_line}"
            f"{pc_eval_line}"
            f"{pc_readiness_line}"
            f"{pc_gate_line}"
            f"{hedge_outcome_line}"
            f"{final_validation_line}"
            f"{decision_ledger_line}"
            f"{position_governance_line}"
            f"<b>Failed checks:</b>\n{reasons_text}\n\n"
            f"No execution this round — wait for the next analysis."
        )

    if actions:
        actions_str = "\n".join(
            f"  {up if a.get('action') == 'buy' else down} "
            f"{a.get('ticker')} "
            f"{'+' if a.get('action') == 'buy' else ''}"
            f"{float(a.get('weight_delta', 0)):.1%}"
            for a in actions
        )
    else:
        actions_str = "  No rebalance (hold current positions)"

    overlay_line = f"🔧 Overlays: {', '.join(overlays)}\n" if overlays else ""

    # 只有在 SEMI_AUTO 模式下且已批准时才显示操作命令
    auth_mode = p.get("auth_mode", "SEMI_AUTO")
    command_buttons = ""
    if auth_mode == "SEMI_AUTO" and approved:
        command_buttons = f"\n<b>/confirm</b>  <b>/skip</b>  <b>/pause</b>"

    return (
        f"📋 <b>Rebalance proposal</b>\n"
        f"――――――――――――――――\n"
        f"{debate_line}"
        f"🌡️ Regime: {regime}\n"
        f"📊 Stance: {stance}\n\n"
        f"{news_line}"
        f"{style_line}"
        f"{proposal_shaping_line}"
        f"{scorecard_line}"
        f"{data_quality_line}"
        f"{feature_source_line}"
        f"{enforcement_line}"
        f"{style_enforcement_line}"
        f"{strategy_use_line}"
        f"{evidence_cap_line}"
        f"{execution_gateway_line}"
        f"{strategy_certification_line}"
        f"{knowledge_line}"
        f"{pc_eval_line}"
        f"{pc_readiness_line}"
        f"{pc_gate_line}"
        f"{hedge_outcome_line}"
        f"{final_validation_line}"
        f"{decision_ledger_line}"
        f"{position_governance_line}"
        f"<b>Suggested actions</b>\n"
        f"{actions_str}\n\n"
        f"💰 Est. cost: {cost:.2%}\n"
        f"🛡️ Risk: ✅ APPROVED\n"
        f"{overlay_line}"
        f"\n⏱️ No reply in {p['timeout_minutes']} min → auto-execute when market is normal"
        f"{command_buttons}"
    )


def _format_scorecard_line(scorecard: dict) -> str:
    if not scorecard or not scorecard.get("market_condition"):
        return ""
    condition = scorecard.get("market_condition")
    permission = scorecard.get("investment_permission")
    data_quality = scorecard.get("data_quality")
    dominant = scorecard.get("dominant_constraint")
    tightening_classes = _scorecard_tightening_classes(scorecard)
    tightened = f" | tightened={','.join(tightening_classes)}" if tightening_classes else ""
    return (
        f"<b>Market scorecard</b>\n"
        f"  {condition} | permission={permission} | data={data_quality}"
        f"{tightened}\n"
        f"  dominant: {dominant}\n\n"
    )


def _format_data_quality_detail_line(detail: dict) -> str:
    if not detail:
        return ""
    evidence = detail.get("evidence_summary") or {}
    qc_snapshots = detail.get("qc_snapshots")
    qc_forward = detail.get("qc_forward_samples")
    hist = detail.get("historical_snapshots")
    hist_forward = detail.get("historical_forward_samples")
    strategy_quality = detail.get("strategy_data_quality")
    news_quality = detail.get("news_data_quality")
    overall = detail.get("overall")
    source_counts = detail.get("feature_source_counts") or {}
    source_timestamps = detail.get("source_timestamps") or {}
    bits = []
    if source_counts.get("qc_heartbeat") is not None:
        bits.append(f"QC heartbeat fields={int(source_counts.get('qc_heartbeat') or 0)}")
    if source_counts.get("qc_daily_snapshot") is not None:
        bits.append(f"Daily snapshot fields={int(source_counts.get('qc_daily_snapshot') or 0)}")
    if source_counts.get("yfinance") is not None:
        bits.append(f"yfinance fields={int(source_counts.get('yfinance') or 0)}")
    if qc_snapshots is not None:
        bits.append(f"QC live snapshots={int(qc_snapshots or 0)}/{int(qc_forward or 0)} forward")
    if evidence.get("execution_intel_status"):
        bits.append(f"QC execution intel={evidence.get('execution_intel_status')}")
    if hist is not None:
        bits.append(f"yfinance history={int(hist or 0)}/{int(hist_forward or 0)} forward")
    if evidence.get("historical_evidence"):
        bits.append(f"yfinance evidence={evidence.get('historical_evidence')}")
    if news_quality:
        news_cache = source_timestamps.get("macro_news_cache")
        suffix = f" @{news_cache}" if news_cache else ""
        bits.append(f"News cache={news_quality}{suffix}")
    if strategy_quality:
        bits.append(f"strategy_data={strategy_quality}")
    if overall:
        bits.append(f"overall={overall}")
    if not bits:
        return ""
    return (
        f"<b>Data quality detail</b>\n"
        f"  {' | '.join(bits)}\n\n"
    )


def _format_feature_source_summary_line(detail: dict) -> str:
    if not detail:
        return ""
    source_counts = detail.get("feature_source_counts") or {}
    authority_counts = detail.get("feature_authority_counts") or {}
    stale_fields = detail.get("stale_fields") or {}

    live_fields = int(authority_counts.get("live_state") or 0)
    intraday_fields = int(authority_counts.get("intraday") or 0)
    research_fields = int(authority_counts.get("daily_research") or 0)
    fallback_fields = (
        int(authority_counts.get("qc_eod_audit") or 0)
        + int(authority_counts.get("legacy_debug") or 0)
        + int(authority_counts.get("unknown") or 0)
    )
    stale_count = sum(len(fields or []) for fields in stale_fields.values())

    if not any([source_counts, authority_counts, stale_count]):
        return ""

    live_source = "QC heartbeat" if source_counts.get("qc_heartbeat") is not None or live_fields or intraday_fields else "missing"
    research_source = "yfinance" if source_counts.get("yfinance") is not None or research_fields else "missing"
    if fallback_fields:
        fallback = f"{fallback_fields} fields"
    else:
        fallback = "none"

    bits = [
        f"live_state={live_source}",
        f"research={research_source}",
        f"fallback={fallback}",
    ]
    if intraday_fields:
        bits.append(f"intraday={intraday_fields} fields")
    if stale_count:
        tickers = ",".join(sorted(stale_fields)[:4])
        bits.append(f"stale={stale_count} fields ({tickers})")

    return (
        f"<b>Feature source summary</b>\n"
        f"  Data: {' | '.join(bits)}\n\n"
    )


def _format_enforcement_line(enforcement: dict) -> str:
    if not enforcement:
        return ""
    violations = enforcement.get("violations") or []
    if not violations:
        return ""
    shown = "; ".join(str(v) for v in violations[:3])
    extra = f" (+{len(violations) - 3} more)" if len(violations) > 3 else ""
    return (
        f"<b>Risk clipping</b>\n"
        f"  {shown}{extra}\n\n"
    )


def _format_news_line(news: dict) -> str:
    if not news or not news.get("overall_bias"):
        return ""
    hard = news.get("hard_risk_events") or {}
    hard_text = f" | hard_risk={','.join(sorted(hard.keys())[:3])}" if hard else ""
    return (
        f"<b>News evidence</b>\n"
        f"  bias={news.get('overall_bias')} | confidence={news.get('confidence')} "
        f"| impact={news.get('market_impact')} | data={news.get('data_quality')}"
        f"{hard_text}\n\n"
    )


def _format_style_line(style: dict) -> str:
    if not style or not style.get("analysis_style"):
        return ""
    reason = str(style.get("style_reason") or style.get("dominant_style_constraint") or "")[:90]
    conviction = style.get("weighted_conviction")
    conviction_text = f" | conviction={float(conviction):.2f}" if isinstance(conviction, (int, float)) else ""
    return (
        f"<b>Decision style</b>\n"
        f"  analysis={style.get('analysis_style')} | trade={style.get('trade_style')}"
        f"{conviction_text}\n"
        f"  reason: {reason}\n\n"
    )


def _format_proposal_shaping_line(shaping: dict) -> str:
    if not shaping or not shaping.get("applied"):
        return ""
    clips = shaping.get("clip_log") or []
    constraints = shaping.get("constraints") or {}
    shown = "; ".join(str(v) for v in clips[:3])
    extra = f" (+{len(clips) - 3} more)" if len(clips) > 3 else ""
    cap_bits = []
    if constraints.get("max_single_delta") is not None:
        cap_bits.append(f"single_delta<={float(constraints.get('max_single_delta')):.1%}")
    if constraints.get("max_turnover") is not None:
        cap_bits.append(f"turnover<={float(constraints.get('max_turnover')):.1%}")
    if constraints.get("human_required"):
        classes = constraints.get("confirmation_classes") or []
        class_text = ",".join(str(item) for item in classes if str(item)) or "scorecard"
        cap_bits.append(f"scorecard_tightened={class_text}")
    if constraints.get("scorecard_data_quality"):
        cap_bits.append(f"data={constraints.get('scorecard_data_quality')}")
    cap_text = " | " + " | ".join(cap_bits) if cap_bits else ""
    return (
        f"<b>Proposal shaping</b>\n"
        f"  pre-risk clipped{cap_text}\n"
        f"  {shown}{extra}\n\n"
    )


def _format_style_enforcement_line(enforcement: dict) -> str:
    if not enforcement:
        return ""
    violations = enforcement.get("violations") or []
    if not violations:
        return ""
    shown = "; ".join(str(v) for v in violations[:3])
    extra = f" (+{len(violations) - 3} more)" if len(violations) > 3 else ""
    return (
        f"<b>Style clipping</b>\n"
        f"  {shown}{extra}\n\n"
    )


def _format_strategy_use_enforcement_line(enforcement: dict) -> str:
    if not enforcement:
        return ""
    evidence = enforcement.get("evidence_summary") or {}
    summary = enforcement.get("strategy_use_summary") or {}
    violations = enforcement.get("violations") or []
    best = evidence.get("best_strategy") or summary.get("best_actionable") or {}
    evidence_bits = []
    if evidence.get("historical_evidence"):
        evidence_bits.append(f"historical={evidence.get('historical_evidence')}")
    if evidence.get("execution_intel_status"):
        evidence_bits.append(f"execution={evidence.get('execution_intel_status')}")
    if evidence.get("execution_permission"):
        evidence_bits.append(f"permission={_display_final_permission(evidence.get('execution_permission'))}")
    if best:
        evidence_bits.append(
            f"best={best.get('strategy_name')}({best.get('suggested_use')})"
        )

    lines = []
    if evidence_bits:
        lines.append("  " + " | ".join(str(bit) for bit in evidence_bits))
    if violations:
        shown = "; ".join(str(v) for v in violations[:3])
        extra = f" (+{len(violations) - 3} more)" if len(violations) > 3 else ""
        lines.append(f"  clipped: {shown}{extra}")
    if not lines:
        return ""
    return (
        f"<b>Strategy-use clipping</b>\n"
        + "\n".join(lines)
        + "\n\n"
    )


def _compact_evidence_cap_observe(
    *,
    cap_diagnostics: dict,
    vote_summary: dict,
    strategy_results: list,
) -> dict:
    if not cap_diagnostics:
        return {}
    rows = []
    for ticker, raw in cap_diagnostics.items():
        if not isinstance(raw, dict):
            continue
        clean_ticker = str(raw.get("ticker") or ticker or "").upper().strip()
        if not clean_ticker:
            continue
        votes = vote_summary.get(clean_ticker) if isinstance(vote_summary, dict) else {}
        if not isinstance(votes, dict):
            votes = {}
        static_cap = _float_or_zero(raw.get("static_cap"))
        adjusted_cap = _float_or_zero(raw.get("evidence_adjusted_cap"))
        cap_reduction = max(static_cap - adjusted_cap, 0.0)
        row = {
            "ticker": clean_ticker,
            "static_cap": static_cap,
            "evidence_adjusted_cap": adjusted_cap,
            "cap_reduction": cap_reduction,
            "current_or_target_weight": _float_or_zero(raw.get("current_or_target_weight")),
            "would_clip": bool(raw.get("would_clip")),
            "coverage_ratio": _float_or_zero(raw.get("coverage_ratio")),
            "voted_count": int(raw.get("voted_count", votes.get("voted_count")) or 0),
            "abstain_count": int(raw.get("abstain_count", votes.get("abstain_count")) or 0),
            "mapping_error_count": int(raw.get("mapping_error_count", votes.get("mapping_error_count")) or 0),
            "conviction_status": raw.get("conviction_status"),
            "history_days": raw.get("history_days"),
            "main_abstain_reason": _first_abstain_reason(votes.get("abstain_reasons") or []),
        }
        rows.append(row)
    rows.sort(
        key=lambda item: (
            not bool(item.get("would_clip")),
            -float(item.get("cap_reduction") or 0.0),
            str(item.get("ticker") or ""),
        )
    )
    meaningful_rows = [
        row for row in rows
        if row.get("would_clip")
        or float(row.get("cap_reduction") or 0.0) >= 0.005
        or int(row.get("mapping_error_count") or 0) > 0
    ]
    mapping_errors = _evidence_mapping_error_display_rows(strategy_results)
    return {
        "available": True,
        "execution_effect": "diagnostic_only",
        "ticker_count": len(rows),
        "degraded_ticker_count": sum(1 for row in rows if float(row.get("cap_reduction") or 0.0) > 0.0),
        "would_clip_count": sum(1 for row in rows if row.get("would_clip")),
        "mapping_error_count": len(mapping_errors),
        "rows": meaningful_rows[:3],
        "mapping_error_rows": mapping_errors[:3],
    }


def _format_evidence_cap_observe_line(observe: dict) -> str:
    if not observe or not observe.get("available"):
        return ""
    rows = observe.get("rows") or []
    mapping_errors = observe.get("mapping_error_rows") or []
    if not rows and not mapping_errors:
        return ""
    lines = [
        "<b>Evidence cap observe</b>",
        (
            f"  diagnostic_only | degraded={int(observe.get('degraded_ticker_count') or 0)} "
            f"| would_clip={int(observe.get('would_clip_count') or 0)} "
            f"| mapping_error={int(observe.get('mapping_error_count') or 0)}"
        ),
    ]
    for row in rows[:3]:
        reason = row.get("main_abstain_reason") or row.get("conviction_status") or "n/a"
        lines.append(
            f"  {row.get('ticker')} cap {_format_pct(row.get('static_cap'))}->{_format_pct(row.get('evidence_adjusted_cap'))} "
            f"| voted={int(row.get('voted_count') or 0)} "
            f"| abstain={int(row.get('abstain_count') or 0)} "
            f"| reason={reason}"
        )
    if mapping_errors:
        shown = "; ".join(
            f"{row.get('ticker')}:{row.get('strategy')}:{row.get('reason_code')}"
            for row in mapping_errors[:3]
        )
        lines.append(f"  mapping_error: {shown}")
    return "\n".join(lines) + "\n\n"


def _evidence_mapping_error_display_rows(strategy_results: list) -> list[dict]:
    rows = []
    seen = set()
    for strategy in strategy_results or []:
        if not isinstance(strategy, dict):
            continue
        strategy_name = strategy.get("strategy_name")
        for card in strategy.get("evidence_cards") or []:
            if not isinstance(card, dict) or str(card.get("vote_status") or "") != "mapping_error":
                continue
            vote_diag = card.get("vote_diagnostics") if isinstance(card.get("vote_diagnostics"), dict) else {}
            key = str(
                vote_diag.get("dedupe_key")
                or f"{card.get('strategy') or strategy_name}:{card.get('ticker')}:{vote_diag.get('reason_code')}"
            )
            if key in seen:
                continue
            seen.add(key)
            rows.append({
                "ticker": card.get("ticker"),
                "strategy": card.get("strategy") or strategy_name,
                "reason_code": vote_diag.get("reason_code"),
                "dedupe_key": key,
            })
    rows.sort(key=lambda row: (str(row.get("ticker") or ""), str(row.get("strategy") or "")))
    return rows


def _first_abstain_reason(rows: list) -> str | None:
    for row in rows:
        if not isinstance(row, dict):
            continue
        reason = str(row.get("reason") or "").strip()
        fields = [str(field) for field in row.get("fields") or [] if str(field)]
        if reason and fields:
            return f"{reason}:{','.join(fields)}"
        if reason:
            return reason
    return None


def _format_pct(value) -> str:
    return f"{_float_or_zero(value):.1%}"


def _float_or_zero(value) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _format_execution_gateway_line(gateway: dict) -> str:
    if not gateway or not gateway.get("final_permission"):
        return ""
    strategy = gateway.get("strategy_layer") or {}
    execution = gateway.get("execution_intel_layer") or {}
    final_permission = _display_final_permission(gateway.get("final_permission"))
    response_class = gateway.get("response_class")
    response_text = f" | class={response_class}" if response_class else ""
    return (
        f"<b>Execution gateway</b>\n"
        f"  final={final_permission} | source={gateway.get('source')} "
        f"| reason={gateway.get('primary_reason')}{response_text}\n"
        f"  strategy={strategy.get('verdict', 'unknown')}:{strategy.get('reason', 'unknown')} "
        f"| execution={execution.get('verdict', 'unknown')}:{execution.get('reason', 'unknown')}\n\n"
    )


def _scorecard_tightening_classes(scorecard: dict) -> list[str]:
    if not scorecard or not bool(scorecard.get("require_human_confirmation")):
        return []
    raw = scorecard.get("confirmation_classes") or []
    classes = [str(item) for item in raw if str(item)]
    return classes or ["scorecard"]


def _display_final_permission(value) -> str:
    raw = str(value or "").strip()
    if raw == "human_required":
        return "tightened"
    return raw


def _display_reason_code(value) -> str:
    raw = str(value or "").strip()
    if raw == "scorecard_human_required":
        return "scorecard_tightened"
    if raw == "human_required":
        return "review_flag"
    if "human_required" in raw:
        return raw.replace("human_required", "scorecard_tightened")
    return raw


def _display_reason_codes(values) -> list[str]:
    return [_display_reason_code(value) for value in (values or [])]


def _display_rejection_reason(value) -> str:
    raw = str(value or "")
    if raw == "Market scorecard requires human confirmation":
        return "Market scorecard tightened the proposal"
    if "human_required" in raw:
        return raw.replace("human_required", "scorecard_tightened")
    return raw


def _compact_knowledge_resolution(resolution: dict, calibration: dict) -> dict:
    if not resolution and not calibration:
        return {}
    conflicts = resolution.get("conflicts") or []
    constraints = resolution.get("hard_constraints") or []
    missing = resolution.get("missing_knowledge") or []
    return {
        "conflicts": conflicts[:5],
        "hard_constraints": constraints[:5],
        "missing_knowledge": missing[:5],
        "calibration": calibration or {},
    }


def _compact_strategy_certification(certification: dict) -> dict:
    if not certification:
        return {}
    items = certification.get("items") or {}
    audit = certification.get("audit") or {}
    compact_items = []
    for name, row in items.items():
        if not isinstance(row, dict):
            continue
        compact_items.append({
            "strategy_name": name,
            "status": row.get("status"),
            "approved_use": row.get("approved_use"),
            "promotion_blockers": (row.get("promotion_blockers") or [])[:3],
            "demotion_reasons": (row.get("demotion_reasons") or [])[:3],
        })
    return {
        "summary": certification.get("summary") or {},
        "audit": _compact_strategy_certification_audit(audit),
        "items": compact_items[:5],
    }


def _compact_strategy_certification_audit(audit: dict) -> dict:
    if not audit:
        return {}
    rows = []
    for row in audit.get("rows") or []:
        if not isinstance(row, dict):
            continue
        rows.append({
            "strategy_name": row.get("strategy_name"),
            "promotion_eligible": bool(row.get("promotion_eligible")),
            "risk_flags": (row.get("risk_flags") or [])[:3],
        })
    return {
        "summary": audit.get("summary") or {},
        "execution_authority": audit.get("execution_authority"),
        "rows": rows[:5],
    }


def _format_strategy_certification_line(certification: dict) -> str:
    if not certification:
        return ""
    items = certification.get("items") or []
    if not items:
        return ""
    parts = [
        f"{item.get('strategy_name')}={item.get('status')}"
        + (
            f" block:{','.join(item.get('promotion_blockers') or [])}"
            if item.get("promotion_blockers") else ""
        )
        + (
            f" demote:{','.join(item.get('demotion_reasons') or [])}"
            if item.get("demotion_reasons") else ""
        )
        for item in items[:3]
    ]
    summary = certification.get("summary") or {}
    counts = (summary.get("counts") or {})
    count_text = ", ".join(
        f"{key}={value}" for key, value in counts.items() if value
    )
    count_line = f"  counts: {count_text}\n" if count_text else ""
    audit_summary = ((certification.get("audit") or {}).get("summary") or {})
    review_line = (
        "  audit: operator_review_required\n"
        if audit_summary.get("requires_operator_review") else ""
    )
    return (
        "<b>Strategy certification</b>\n"
        f"{count_line}"
        f"{review_line}"
        f"  " + "; ".join(parts) + "\n\n"
    )


def _format_knowledge_resolution_line(resolution: dict) -> str:
    if not resolution:
        return ""
    conflicts = resolution.get("conflicts") or []
    constraints = resolution.get("hard_constraints") or []
    missing = resolution.get("missing_knowledge") or []
    calibration = resolution.get("calibration") or {}
    summary = calibration.get("summary") or {}

    lines = []
    if conflicts:
        shown = "; ".join(
            f"{item.get('id')}:{item.get('strategy') or item.get('ticker') or item.get('regime')}"
            for item in conflicts[:3]
        )
        lines.append(f"  conflicts: {shown}")
    if constraints:
        shown = "; ".join(
            f"{item.get('id')}:{item.get('ticker') or item.get('action')}"
            for item in constraints[:3]
        )
        lines.append(f"  constraints: {shown}")
    if summary.get("total"):
        lines.append(
            "  confidence calibration: "
            f"accepted={int(summary.get('accepted') or 0)}, "
            f"rejected={int(summary.get('rejected') or 0)}"
        )
    if missing:
        shown = "; ".join(
            f"{item.get('severity')}:{item.get('kind')}:{item.get('id')}"
            for item in missing[:3]
        )
        lines.append(f"  missing: {shown}")

    if not lines:
        return ""
    return "<b>Knowledge resolution</b>\n" + "\n".join(lines) + "\n\n"


def _compact_decision_ledger(ledger: dict) -> dict:
    if not ledger:
        return {}
    tickers = ledger.get("tickers") or {}
    rows = []
    for ticker, row in tickers.items():
        if not isinstance(row, dict):
            continue
        lifecycle = row.get("trade_lifecycle") or {}
        governance = (row.get("evidence_used") or {}).get("position_governance") or {}
        explanation = row.get("explanation") or {}
        advisory = row.get("llm_advisory") or {}
        policy = row.get("execution_policy") or {}
        hedge_path = row.get("hedge_path") or {}
        raw_reason_codes = list(row.get("reason_codes") or [])
        display_reason_codes = row.get("display_reason_codes") or _display_reason_codes(raw_reason_codes)
        rows.append({
            "ticker": row.get("ticker") or ticker,
            "proposed_action": row.get("proposed_action"),
            "final_action": row.get("final_action"),
            "execution_status": row.get("execution_status"),
            "cmd_id": row.get("cmd_id"),
            "qc_status": row.get("qc_status"),
            "qc_rejection_reason": row.get("qc_rejection_reason"),
            "qc_timestamp": row.get("qc_timestamp"),
            "risk_result": row.get("risk_result"),
            "reason_codes": display_reason_codes[:4],
            "internal_reason_codes": raw_reason_codes[:4],
            "ticker_role": policy.get("ticker_role"),
            "single_cap": policy.get("single_cap"),
            "group_cap": policy.get("group_cap"),
            "policy_version": policy.get("policy_version"),
            "policy_cap_applied": policy.get("policy_cap_applied"),
            "policy_cap_original": policy.get("policy_cap_original"),
            "policy_group_scaled": policy.get("policy_group_scaled"),
            "cash_raised_by_policy_cap": policy.get("cash_raised_by_policy_cap"),
            "entered_via_hedge_path": hedge_path.get("entered_via_hedge_path"),
            "hedge_trigger_reasons": hedge_path.get("hedge_trigger_reasons") or [],
            "governance_decision": governance.get("decision"),
            "risk_rank": governance.get("risk_rank"),
            "position_state": explanation.get("position_state"),
            "final_explanation": explanation.get("final_explanation"),
            "llm_effect": explanation.get("llm_effect"),
            "construction_effect": explanation.get("construction_effect"),
            "final_target": lifecycle.get("final_target"),
            "diagnostic_llm_target": lifecycle.get("diagnostic_llm_target"),
            "target_builder_target": lifecycle.get("target_builder_target"),
            "validated_advisory_delta": lifecycle.get("validated_advisory_delta"),
            "advisory_validator_result": advisory.get("validator_result"),
            "changed_by": lifecycle.get("changed_by") or [],
            "source_effects": _compact_source_effects(row.get("display_source_effects") or row.get("source_effects") or {}),
            "sort_score": _decision_ledger_sort_score(row),
        })
    rows.sort(key=lambda item: (-int(item.get("sort_score") or 0), str(item.get("ticker") or "")))
    summary = ledger.get("portfolio_summary") or {}
    return {
        "phase": ledger.get("phase"),
        "portfolio_summary": {
            "risk_approved": summary.get("risk_approved"),
            "execution_status": summary.get("execution_status"),
            "cmd_id": summary.get("cmd_id"),
            "qc_status": summary.get("qc_status"),
            "qc_rejection_reason": summary.get("qc_rejection_reason"),
            "qc_timestamp": summary.get("qc_timestamp"),
            "governance_available": summary.get("governance_available"),
            "target_construction_mode": summary.get("target_construction_mode"),
            "raw_llm_adjusted_weights_consumed": summary.get("raw_llm_adjusted_weights_consumed"),
            "policy_version": summary.get("policy_version"),
            "cash_raised_by_policy_cap": summary.get("cash_raised_by_policy_cap"),
            "final_policy_version": summary.get("final_policy_version"),
            "final_policy_cap_triggered": summary.get("final_policy_cap_triggered"),
            "final_policy_cap_events": summary.get("final_policy_cap_events") or [],
            "minimum_weight_floor_events": summary.get("minimum_weight_floor_events") or [],
            "final_policy_cash_raised": summary.get("final_policy_cash_raised"),
            "final_policy_cash_raised_by_minimum_weight_floor": summary.get(
                "final_policy_cash_raised_by_minimum_weight_floor"
            ),
            "active_basket_policy": summary.get("active_basket_policy") or {},
            "hedge_intent": summary.get("hedge_intent"),
            "ticker_count": summary.get("ticker_count"),
        },
        "top_decisions": rows[:5],
        "warnings": (ledger.get("warnings") or [])[:3],
    }


def _decision_ledger_sort_score(row: dict) -> int:
    score = 0
    proposed = str(row.get("proposed_action") or "")
    final = str(row.get("final_action") or "")
    reasons = {str(item) for item in row.get("reason_codes") or []}
    governance = (row.get("evidence_used") or {}).get("position_governance") or {}
    explanation = row.get("explanation") or {}
    governance_decision = str(governance.get("decision") or "")
    state = str(explanation.get("position_state") or "")
    if final in {"none", "unknown"} and proposed not in {"hold", "none", ""}:
        score += 80
    if "hard_risk" in reasons or state == "hard_risk":
        score += 70
    if governance_decision in {"trim", "trim_review"} or final == "trim":
        score += 50
    if governance_decision == "hold_review" or state.endswith("_review"):
        score += 35
    if final == "add":
        score += 25
    if reasons:
        score += min(20, len(reasons) * 4)
    risk_rank = governance.get("risk_rank")
    if isinstance(risk_rank, (int, float)):
        score += max(0, 10 - int(risk_rank))
    return score


def _format_decision_ledger_line(ledger: dict) -> str:
    if not ledger:
        return ""
    rows = ledger.get("top_decisions") or []
    summary = ledger.get("portfolio_summary") or {}
    warnings = ledger.get("warnings") or []
    if (
        not rows
        and not warnings
        and not (summary.get("final_policy_cap_events") or [])
        and not (summary.get("minimum_weight_floor_events") or [])
        and not summary.get("active_basket_policy")
        and not summary.get("hedge_intent")
    ):
        return ""
    lines = ["<b>Decision ledger</b>"]
    status_bits = []
    if summary.get("risk_approved") is not None:
        status_bits.append(f"risk_approved={bool(summary.get('risk_approved'))}")
    if summary.get("execution_status"):
        status_bits.append(f"execution={summary.get('execution_status')}")
    if summary.get("qc_status"):
        status_bits.append(f"qc={summary.get('qc_status')}")
    if summary.get("governance_available") is not None:
        status_bits.append(f"governance={bool(summary.get('governance_available'))}")
    if summary.get("target_construction_mode"):
        status_bits.append(f"target={summary.get('target_construction_mode')}")
    if summary.get("policy_version"):
        status_bits.append(f"policy={summary.get('policy_version')}")
    if summary.get("final_policy_cap_triggered"):
        status_bits.append("final_cap=true")
    if summary.get("minimum_weight_floor_events"):
        status_bits.append("min_floor=true")
    if summary.get("raw_llm_adjusted_weights_consumed") is not None:
        status_bits.append(f"raw_llm={bool(summary.get('raw_llm_adjusted_weights_consumed'))}")
    if status_bits:
        lines.append("  " + " | ".join(status_bits))
    final_cap_events = summary.get("final_policy_cap_events") or []
    if final_cap_events:
        lines.append("  " + _format_final_policy_cap_warning(final_cap_events))
    floor_events = summary.get("minimum_weight_floor_events") or []
    if floor_events:
        lines.append("  " + _format_minimum_weight_floor_warning(floor_events))
    basket_line = _format_active_basket_policy_summary(summary.get("active_basket_policy") or {})
    if basket_line:
        lines.append("  " + basket_line)
    hedge_line = _format_hedge_intent_summary(summary.get("hedge_intent") or {})
    if hedge_line:
        lines.append(hedge_line)
    for row in rows[:5]:
        ticker = row.get("ticker")
        proposed = row.get("proposed_action") or "hold"
        final = row.get("final_action") or "unknown"
        reasons = ",".join(_display_reason_codes(row.get("display_reason_codes") or row.get("reason_codes") or [])[:3])
        changed_by = ",".join(str(item) for item in (row.get("changed_by") or [])[:2])
        sources = ",".join(str(item) for item in (row.get("source_effects") or [])[:4])
        suffix_parts = []
        target_bits = _format_lifecycle_targets(row)
        if target_bits:
            suffix_parts.append(target_bits)
        if reasons:
            suffix_parts.append(reasons)
        if row.get("advisory_validator_result"):
            suffix_parts.append(f"advisory={row.get('advisory_validator_result')}")
        if row.get("policy_cap_applied"):
            suffix_parts.append(f"policy_cap={row.get('policy_cap_original')}->{row.get('final_target')}")
        if row.get("entered_via_hedge_path"):
            suffix_parts.append("hedge_path=true")
        if row.get("qc_rejection_reason"):
            suffix_parts.append(f"qc_reject={row.get('qc_rejection_reason')}")
        if changed_by:
            suffix_parts.append(f"changed_by={changed_by}")
        if sources:
            suffix_parts.append(f"sources={sources}")
        if row.get("final_explanation"):
            suffix_parts.append(str(row.get("final_explanation")))
        suffix = " | " + " | ".join(suffix_parts) if suffix_parts else ""
        lines.append(f"  {ticker}: {proposed} -> {final}{suffix}")
    if warnings:
        lines.append("  warnings: " + "; ".join(str(item) for item in warnings[:3]))
    return "\n".join(lines) + "\n\n"


def _format_hedge_intent_summary(hedge_intent: dict) -> str:
    if not isinstance(hedge_intent, dict) or not hedge_intent:
        return ""
    triggered = bool(hedge_intent.get("triggered"))
    severity = _float_or_zero(hedge_intent.get("severity"))
    add_hedge = bool(hedge_intent.get("add_hedge_etf"))
    reason = str(hedge_intent.get("why_not_add_hedge") or "").strip()
    selected = str(hedge_intent.get("selected_hedge") or hedge_intent.get("hedge_instrument") or "").strip()
    trim_targets = [
        str(item or "").upper().strip()
        for item in (hedge_intent.get("trim_targets") or [])
        if str(item or "").strip()
    ][:4]
    cash_raise = _float_or_zero(hedge_intent.get("cash_raise_pct"))
    parts = [
        f"triggered={triggered}",
        f"severity={severity:.2f}",
        f"add_hedge={add_hedge}",
    ]
    if selected:
        parts.append(f"selected={selected}")
    if reason:
        parts.append(f"reason={reason}")
    action_bits = []
    if trim_targets:
        action_bits.append("trim " + ",".join(trim_targets))
    if cash_raise > 0:
        action_bits.append(f"raise_cash {cash_raise:.0%}")
    action = " | action: " + " + ".join(action_bits) if action_bits else ""
    return "  Hedge intent: " + " | ".join(parts) + action


def _format_final_policy_cap_warning(events: list[dict]) -> str:
    bits = []
    for event in events[:4]:
        if not isinstance(event, dict):
            continue
        ticker = event.get("ticker") or event.get("group_role")
        original = event.get("original", event.get("original_total"))
        capped = event.get("capped_to", event.get("cap"))
        if ticker and original is not None and capped is not None:
            try:
                bits.append(f"{ticker} ({float(original):.2%} -> {float(capped):.2%})")
            except (TypeError, ValueError):
                bits.append(str(ticker))
    shown = ", ".join(bits) if bits else "unknown"
    return (
        "WARNING: post-governance policy cap triggered for "
        f"{shown}. Upstream governance/position_manager introduced out-of-policy weights."
    )


def _format_minimum_weight_floor_warning(events: list[dict]) -> str:
    bits = []
    for event in events[:5]:
        if not isinstance(event, dict):
            continue
        ticker = str(event.get("ticker") or "").upper().strip()
        original = event.get("original")
        if not ticker or original is None:
            continue
        try:
            bits.append(f"{ticker} {float(original):.2%}->0")
        except (TypeError, ValueError):
            bits.append(ticker)
    shown = ", ".join(bits) if bits else "unknown"
    return f"Minimum position floor cleared: {shown}"


def _format_active_basket_policy_summary(policy: dict) -> str:
    if not isinstance(policy, dict) or not policy:
        return ""
    active_count = policy.get("active_count")
    target_min = policy.get("target_active_count_min")
    target_max = policy.get("target_active_count_max")
    try:
        count_part = f"{int(active_count)}/{int(target_min)}-{int(target_max)}"
    except (TypeError, ValueError):
        count_part = str(active_count or "unknown")

    roles = policy.get("roles") or {}
    role_bits = []
    for role in ("core", "sector", "thematic", "satellite", "hedge"):
        row = roles.get(role) or {}
        if not row:
            continue
        role_policy = row.get("policy") or {}
        try:
            role_bits.append(f"{role}={int(row.get('active_count') or 0)}/{int(role_policy.get('max_positions'))}")
        except (TypeError, ValueError):
            role_bits.append(f"{role}={row.get('active_count')}")

    suffix_bits = []
    subscale = _format_basket_position_list(policy.get("subscale_positions") or [], max_items=4)
    if subscale:
        suffix_bits.append(f"subscale: {subscale}")
    floor = _format_basket_position_list(policy.get("floor_cleared_positions") or [], max_items=4)
    if floor:
        suffix_bits.append(f"floor: {floor}")
    warnings = [str(item) for item in (policy.get("warnings") or [])[:2] if str(item).strip()]
    if warnings:
        suffix_bits.append("warnings: " + ";".join(warnings))

    parts = [f"Active basket: {count_part} diagnostic"]
    if role_bits:
        parts.append(" ".join(role_bits))
    if suffix_bits:
        parts.append(" | ".join(suffix_bits))
    return " | ".join(parts)


def _format_basket_position_list(rows: list[dict], *, max_items: int) -> str:
    bits = []
    for row in rows[:max_items]:
        if not isinstance(row, dict):
            continue
        ticker = str(row.get("ticker") or "").upper().strip()
        if not ticker:
            continue
        weight = row.get("weight")
        try:
            bits.append(f"{ticker} {float(weight):.2%}")
        except (TypeError, ValueError):
            bits.append(ticker)
    return ", ".join(bits)


def _compact_portfolio_construction_evaluation(evaluation: dict) -> dict:
    if not evaluation:
        return {}
    metrics = evaluation.get("metrics") or {}
    return {
        "status": evaluation.get("status"),
        "promotion_ready": bool(evaluation.get("promotion_ready")),
        "execution_authority": evaluation.get("execution_authority"),
        "blockers": list(evaluation.get("blockers") or []),
        "warnings": list(evaluation.get("warnings") or []),
        "mean_abs_weight_deviation": metrics.get("mean_abs_weight_deviation"),
        "turnover_delta": metrics.get("turnover_delta"),
        "shadow_policy_allowed": metrics.get("shadow_policy_allowed"),
        "candidate_policy_allowed": metrics.get("candidate_policy_allowed"),
        "basket_policy_ok": metrics.get("basket_policy_ok"),
        "turnover_ok": metrics.get("turnover_ok"),
        "subscale_count": metrics.get("subscale_count"),
        "shadow_high_risk_tickers_added": metrics.get("shadow_high_risk_tickers_added") or [],
    }


def _compact_portfolio_construction_readiness(readiness: dict) -> dict:
    if not readiness:
        return {}
    return {
        "status": readiness.get("status"),
        "promotion_ready": bool(readiness.get("promotion_ready")),
        "ready": bool(readiness.get("ready", readiness.get("promotion_ready"))),
        "cycles": readiness.get("cycles"),
        "pass_rate": readiness.get("pass_rate"),
        "basket_policy_ok_rate": readiness.get("basket_policy_ok_rate"),
        "policy_ok_rate": readiness.get("policy_ok_rate"),
        "turnover_ok_rate": readiness.get("turnover_ok_rate"),
        "subscale_position_rate": readiness.get("subscale_position_rate"),
        "blockers": list(readiness.get("blockers") or []),
        "blocker_counts": readiness.get("blocker_counts") or {},
        "warning_counts": readiness.get("warning_counts") or {},
        "execution_authority": readiness.get("execution_authority"),
    }


def _compact_portfolio_construction_promotion_gate(gate: dict) -> dict:
    if not gate:
        return {}
    return {
        "status": gate.get("status"),
        "eligible": bool(gate.get("eligible")),
        "portfolio_construction_mode": gate.get("portfolio_construction_mode"),
        "enabled": bool(gate.get("enabled")),
        "approval_mode": gate.get("approval_mode"),
        "blockers": list(gate.get("blockers") or []),
        "would_promote_to": gate.get("would_promote_to"),
        "execution_authority": gate.get("execution_authority"),
    }


def _compact_hedge_intent_outcome(outcome: dict) -> dict:
    if not outcome:
        return {}
    return {
        "report_version": outcome.get("report_version"),
        "date": outcome.get("date"),
        "triggered": bool(outcome.get("triggered")),
        "severity": outcome.get("severity"),
        "add_hedge_etf": bool(outcome.get("add_hedge_etf")),
        "selected_instrument": outcome.get("selected_instrument"),
        "candidate_hedge_instrument": outcome.get("candidate_hedge_instrument"),
        "why_not_add_hedge": outcome.get("why_not_add_hedge"),
        "outcome_status": outcome.get("outcome_status"),
        "spy_return_5d": outcome.get("spy_return_5d"),
        "hedge_instrument_return_5d": outcome.get("hedge_instrument_return_5d"),
        "hedge_would_have_helped": outcome.get("hedge_would_have_helped"),
        "threshold_assessment": outcome.get("threshold_assessment"),
        "execution_authority": outcome.get("execution_authority"),
    }


def _compact_final_validation(validation: dict) -> dict:
    if not validation:
        return {}
    drift = validation.get("drift") or {}
    policy = validation.get("policy_evaluation") or {}
    return {
        "mode": validation.get("mode"),
        "approved": bool(validation.get("approved")),
        "execution_effect": validation.get("execution_effect"),
        "policy_allowed": bool(policy.get("allowed")),
        "severe_block": bool(validation.get("severe_block")),
        "max_abs_drift": drift.get("max_abs_drift"),
        "material_drift_threshold": drift.get("material_drift_threshold"),
        "material_drift": bool(drift.get("material_drift")),
        "mutation_types": list(validation.get("mutation_types") or []),
        "blocking_violations": list(validation.get("blocking_violations") or []),
        "conditional_mutation_violations": list(validation.get("conditional_mutation_violations") or []),
        "severe_violations": list(validation.get("severe_violations") or []),
    }


def _format_portfolio_construction_evaluation_line(evaluation: dict) -> str:
    if not evaluation:
        return ""
    blockers = evaluation.get("blockers") or []
    warnings = evaluation.get("warnings") or []
    status = evaluation.get("status") or "unknown"
    ready = bool(evaluation.get("promotion_ready"))
    parts = [
        f"status={status}",
        f"ready={ready}",
        f"mean_dev={float(evaluation.get('mean_abs_weight_deviation') or 0.0):.2%}",
        f"turnover_delta={float(evaluation.get('turnover_delta') or 0.0):+.2%}",
        f"policy_ok={bool(evaluation.get('candidate_policy_allowed', evaluation.get('shadow_policy_allowed')))}",
        f"basket_ok={bool(evaluation.get('basket_policy_ok'))}",
    ]
    if evaluation.get("subscale_count") is not None:
        parts.append(f"subscale={int(evaluation.get('subscale_count') or 0)}")
    if blockers:
        parts.append("blockers=" + ",".join(str(item) for item in blockers[:4]))
    if warnings:
        parts.append("warnings=" + ",".join(str(item) for item in warnings[:3]))
    return "<b>Portfolio construction evaluation</b>\n  " + " | ".join(parts) + "\n\n"


def _format_portfolio_construction_readiness_line(readiness: dict) -> str:
    if not readiness:
        return ""
    blockers = readiness.get("blockers") or []
    blocker_counts = readiness.get("blocker_counts") or {}
    blocker_bits = ",".join(str(item) for item in blockers[:4])
    if not blocker_bits:
        blocker_bits = ",".join(f"{key}:{value}" for key, value in sorted(blocker_counts.items())[:4])
    parts = [
        f"status={readiness.get('status') or 'unknown'}",
        f"ready={bool(readiness.get('ready', readiness.get('promotion_ready')))}",
        f"cycles={int(readiness.get('cycles') or 0)}",
        f"pass_rate={float(readiness.get('pass_rate') or 0.0):.0%}",
        f"basket_ok={float(readiness.get('basket_policy_ok_rate') or 0.0):.0%}",
        f"policy_ok={float(readiness.get('policy_ok_rate') or 0.0):.0%}",
        f"turnover_ok={float(readiness.get('turnover_ok_rate') or 0.0):.0%}",
    ]
    if blocker_bits:
        parts.append(f"blockers={blocker_bits}")
    return "<b>Portfolio construction rolling readiness</b>\n  " + " | ".join(parts) + "\n\n"


def _format_portfolio_construction_promotion_gate_line(gate: dict) -> str:
    if not gate:
        return ""
    blockers = gate.get("blockers") or []
    parts = [
        f"status={gate.get('status') or 'unknown'}",
        f"mode={gate.get('portfolio_construction_mode') or 'shadow'}",
        f"enabled={bool(gate.get('enabled'))}",
        f"eligible={bool(gate.get('eligible'))}",
        f"approval={gate.get('approval_mode') or 'auto'}",
        f"authority={gate.get('execution_authority') or 'none'}",
    ]
    if gate.get("would_promote_to"):
        parts.append(f"would_promote_to={gate.get('would_promote_to')}")
    if blockers:
        parts.append("blockers=" + ",".join(str(item) for item in blockers[:4]))
    return "<b>Portfolio construction promotion gate</b>\n  " + " | ".join(parts) + "\n\n"


def _format_hedge_intent_outcome_line(outcome: dict) -> str:
    if not outcome:
        return ""
    parts = [
        f"status={outcome.get('outcome_status') or 'unknown'}",
        f"triggered={bool(outcome.get('triggered'))}",
        f"add_hedge={bool(outcome.get('add_hedge_etf'))}",
    ]
    if outcome.get("candidate_hedge_instrument"):
        parts.append(f"candidate={outcome.get('candidate_hedge_instrument')}")
    if outcome.get("selected_instrument"):
        parts.append(f"selected={outcome.get('selected_instrument')}")
    if outcome.get("why_not_add_hedge"):
        parts.append(f"reason={outcome.get('why_not_add_hedge')}")
    if outcome.get("threshold_assessment"):
        parts.append(f"assessment={outcome.get('threshold_assessment')}")
    return "<b>Hedge intent outcome log</b>\n  " + " | ".join(parts) + "\n\n"


def _format_final_validation_line(validation: dict) -> str:
    if not validation:
        return ""
    blockers = (
        validation.get("blocking_violations")
        or validation.get("severe_violations")
        or validation.get("conditional_mutation_violations")
        or []
    )
    parts = [
        f"mode={validation.get('mode') or 'observe'}",
        f"approved={bool(validation.get('approved'))}",
        f"policy_ok={bool(validation.get('policy_allowed'))}",
        f"max_drift={float(validation.get('max_abs_drift') or 0.0):.2%}",
        f"threshold={float(validation.get('material_drift_threshold') or 0.0):.2%}",
    ]
    mutation_types = validation.get("mutation_types") or []
    if mutation_types:
        parts.append("mutations=" + ",".join(str(item) for item in mutation_types[:4]))
    if blockers:
        parts.append("blockers=" + ",".join(str(item) for item in blockers[:4]))
    return "<b>Final risk validation</b>\n  " + " | ".join(parts) + "\n\n"


def _format_lifecycle_targets(row: dict) -> str:
    parts = []
    if row.get("final_target") is not None:
        parts.append(f"final={float(row.get('final_target') or 0.0):.1%}")
    if row.get("target_builder_target") is not None:
        parts.append(f"tb={float(row.get('target_builder_target') or 0.0):.1%}")
    if row.get("validated_advisory_delta") not in (None, 0, 0.0):
        parts.append(f"adv={float(row.get('validated_advisory_delta') or 0.0):+.1%}")
    return ",".join(parts)


def _compact_source_effects(source_effects: dict) -> list[str]:
    priority = ("news", "scorecard", "risk", "knowledge", "qc", "yfinance", "strategy")
    return [
        source
        for source in priority
        if source_effects.get(source)
    ]


def _format_position_governance_line(governance: dict) -> str:
    if not governance:
        return ""
    diagnostic_only = governance.get("mode") == "diagnostic_only"
    decisions = governance.get("position_decisions") or []
    interesting = [
        row for row in decisions
        if row.get("decision") in {"trim", "trim_review", "hold_review", "add"}
        or row.get("reason_codes")
    ][:5]
    portfolio_summary = governance.get("portfolio_summary") or {}
    if (
        not interesting
        and not governance.get("blocked_actions")
        and not governance.get("forced_trims")
        and not portfolio_summary
    ):
        return ""
    lines = ["<b>Position governance</b>"]
    if diagnostic_only:
        lines.append("  mode=diagnostic_only (no target changes)")
    elif governance.get("mode") == "full_auto_governance_only":
        lines.append("  mode=full_auto_governance_only (risk-reducing trims only)")
    concentration = _format_governance_concentration(portfolio_summary)
    if concentration:
        lines.append("  risk concentration: " + concentration)
    top_risk = _format_governance_top_risk(portfolio_summary)
    if top_risk:
        lines.append("  top risk: " + top_risk)
    basket_reviews = _format_governance_basket_reviews(portfolio_summary)
    if basket_reviews:
        lines.append("  basket review: " + basket_reviews)
    thesis = _format_governance_thesis_status(portfolio_summary)
    if thesis:
        lines.append("  thesis: " + thesis)
    explanations = _format_position_explanations(portfolio_summary)
    if explanations:
        lines.extend(explanations)
    for row in interesting:
        reasons = ",".join(_display_reason_codes(row.get("reason_codes") or [])[:3])
        lines.append(
            f"  {row.get('ticker')}: {row.get('decision')} | "
            f"support={row.get('strategy_support')} | "
            f"target {float(row.get('target_before') or 0):.1%}->{float(row.get('target_after') or 0):.1%}"
            + (f" | {reasons}" if reasons else "")
        )
    if governance.get("blocked_actions"):
        lines.append("  blocked: " + "; ".join(str(x) for x in governance["blocked_actions"][:3]))
    if governance.get("forced_trims") and not diagnostic_only:
        lines.append("  trims: " + "; ".join(str(x) for x in governance["forced_trims"][:3]))
    if governance.get("replacements") and not diagnostic_only:
        repl = [
            f"{item.get('ticker')} +{float(item.get('added_weight') or 0):.1%} "
            f"({item.get('support')}, score={float(item.get('score') or 0):.2f})"
            for item in governance["replacements"][:3]
        ]
        lines.append("  replacements: " + "; ".join(repl))
    if governance.get("advisory_overrides"):
        overrides = [
            f"{item.get('ticker')} {item.get('llm_advisory')}->{item.get('validator_result')}"
            for item in governance["advisory_overrides"][:3]
        ]
        lines.append("  llm advisory: " + "; ".join(overrides))
    manual_hints = [] if governance.get("mode") == "full_auto_governance_only" else governance.get("manual_action_hints") or portfolio_summary.get("manual_action_hints") or []
    if manual_hints:
        hints = [
            _format_manual_action_hint(item)
            for item in manual_hints[:3]
        ]
        lines.append("  manual trim review: " + "; ".join(hints))
    quality = (portfolio_summary.get("advisory_quality") or {}).get("current_run") or {}
    if quality.get("total"):
        lines.append(
            "  advisory quality: "
            f"accepted={int(quality.get('accepted') or 0)}, "
            f"rejected={int(quality.get('rejected') or 0)}, "
            f"converted={int(quality.get('converted') or 0)}"
        )
    return "\n".join(lines) + "\n\n"


def _format_manual_action_hint(item: dict) -> str:
    ticker = item.get("ticker")
    current = float(item.get("current_weight") or 0.0)
    target = float(item.get("suggested_target") or 0.0)
    reasons = set(item.get("reason_codes") or [])
    labels: list[str] = []
    if "advisory_basket_loss_review" in reasons:
        labels.append("advisory=weak-positive")
        labels.append("basket loss review")
    elif "hard_risk" in reasons:
        labels.append("hard-risk")
    elif "basket_review" in reasons:
        labels.append("basket review")
    elif "unrealized_loss_review" in reasons:
        labels.append("loss review")
    elif "winner_risk_budget_review" in reasons:
        labels.append("winner risk review")
    reason_text = f" ({', '.join(labels)})" if labels else ""
    return f"{ticker} {current:.1%}->{target:.1%}{reason_text}"


def _format_governance_concentration(portfolio_summary: dict) -> str:
    groups = portfolio_summary.get("group_exposures") or {}
    ordered = sorted(
        groups.items(),
        key=lambda item: abs(float((item[1] or {}).get("headroom") or 0.0)),
    )
    parts = []
    for group, row in ordered[:3]:
        exposure = float((row or {}).get("exposure") or 0.0)
        limit = (row or {}).get("limit")
        headroom = (row or {}).get("headroom")
        if limit is None or headroom is None or exposure <= 0:
            continue
        parts.append(
            f"{group} {exposure:.1%} [limit {float(limit):.1%}, headroom {float(headroom):+.1%}]"
        )
    return "; ".join(parts)


def _format_governance_top_risk(portfolio_summary: dict) -> str:
    rows = portfolio_summary.get("top_risk_contributors") or []
    parts = []
    for row in rows[:3]:
        ticker = row.get("ticker")
        contribution = float(row.get("risk_contribution") or 0.0)
        status = row.get("risk_budget_status") or "normal"
        if ticker:
            parts.append(f"{ticker} {contribution:.2%} ({status})")
    return "; ".join(parts)


def _format_governance_basket_reviews(portfolio_summary: dict) -> str:
    rows = portfolio_summary.get("basket_reviews") or []
    parts = []
    for row in rows[:3]:
        group = row.get("group")
        tickers = ",".join((row.get("tickers") or [])[:4])
        if group and tickers:
            parts.append(f"{group} [{tickers}]")
    return "; ".join(parts)


def _format_governance_thesis_status(portfolio_summary: dict) -> str:
    summary = portfolio_summary.get("thesis_status_summary") or {}
    rows = summary.get("problem_tickers") or []
    parts = []
    for row in rows[:4]:
        ticker = row.get("ticker")
        status = row.get("status")
        if ticker and status:
            parts.append(f"{ticker}={status}")
    return "; ".join(parts)


def _format_position_explanations(portfolio_summary: dict) -> list[str]:
    rows = portfolio_summary.get("position_explanations") or []
    parts: list[str] = []
    for row in rows[:3]:
        ticker = row.get("ticker")
        state = row.get("position_state")
        thesis = (row.get("thesis_status") or {}).get("status")
        trigger = row.get("next_trigger")
        why_not_add = (row.get("why_not_add") or [""])[0]
        if ticker and state:
            detail = f" | no add: {why_not_add}" if why_not_add else ""
            thesis_text = f" | thesis={thesis}" if thesis and thesis != "unknown" else ""
            trigger_text = f" | next: {trigger}" if trigger else ""
            parts.append(f"  explain {ticker}: {state}{thesis_text}{detail}{trigger_text}")
    return parts


def append_command_hints(text: str) -> str:
    cleaned = remove_command_hints(text)
    if cleaned:
        return f"{cleaned}\n\n<b>/confirm</b>  <b>/skip</b>  <b>/pause</b>"
    return "<b>/confirm</b>  <b>/skip</b>  <b>/pause</b>"
