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
- Include strategy-use evidence and strategy-use clipping/blocking details when present
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
            "reasons": (scorecard.get("reasons") or [])[:3],
            "warnings": (scorecard.get("warnings") or [])[:3],
        },
        "data_quality_detail": {
            "qc_snapshots": strategies.get("snapshot_count"),
            "qc_forward_samples": strategies.get("forward_return_samples"),
            "historical_snapshots": strategies.get("historical_snapshot_count"),
            "historical_forward_samples": strategies.get("historical_forward_return_samples"),
            "strategy_data_quality": strategies.get("data_quality"),
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
    strategy_certification = p.get("strategy_certification") or {}
    knowledge_resolution = p.get("knowledge_resolution") or {}
    position_governance = p.get("position_governance") or {}
    decision_ledger = p.get("decision_ledger") or {}

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
    enforcement_line = _format_enforcement_line(enforcement)
    news_line = _format_news_line(news)
    style_line = _format_style_line(style)
    proposal_shaping_line = _format_proposal_shaping_line(proposal_shaping)
    style_enforcement_line = _format_style_enforcement_line(style_enforcement)
    strategy_use_line = _format_strategy_use_enforcement_line(strategy_use_enforcement)
    strategy_certification_line = _format_strategy_certification_line(strategy_certification)
    knowledge_line = _format_knowledge_resolution_line(knowledge_resolution)
    decision_ledger_line = _format_decision_ledger_line(decision_ledger)
    position_governance_line = _format_position_governance_line(position_governance)

    if not approved:
        reasons_text = "\n".join(f"  - {r}" for r in p["rejection_reasons"]) or "  - No reason provided"
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
            f"{enforcement_line}"
            f"{style_enforcement_line}"
            f"{strategy_use_line}"
            f"{strategy_certification_line}"
            f"{knowledge_line}"
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
        f"{enforcement_line}"
        f"{style_enforcement_line}"
        f"{strategy_use_line}"
        f"{strategy_certification_line}"
        f"{knowledge_line}"
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
    human = " | human confirm" if scorecard.get("require_human_confirmation") else ""
    return (
        f"<b>Market scorecard</b>\n"
        f"  {condition} | permission={permission} | data={data_quality}"
        f"{human}\n"
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
    bits = []
    if qc_snapshots is not None:
        bits.append(f"QC live={int(qc_snapshots or 0)} snapshots/{int(qc_forward or 0)} forward")
    if hist is not None:
        bits.append(f"yfinance={int(hist or 0)} history/{int(hist_forward or 0)} forward")
    if evidence.get("live_fit"):
        bits.append(f"live_fit={evidence.get('live_fit')}")
    if evidence.get("historical_evidence"):
        bits.append(f"historical={evidence.get('historical_evidence')}")
    if strategy_quality:
        bits.append(f"strategy_data={strategy_quality}")
    if not bits:
        return ""
    return (
        f"<b>Data quality detail</b>\n"
        f"  {' | '.join(bits)}\n\n"
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
        cap_bits.append("human_required")
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
    if evidence.get("live_fit"):
        evidence_bits.append(f"live={evidence.get('live_fit')}")
    if evidence.get("execution_permission"):
        evidence_bits.append(f"permission={evidence.get('execution_permission')}")
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
        "items": compact_items[:5],
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
    return (
        "<b>Strategy certification</b>\n"
        f"{count_line}"
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
        rows.append({
            "ticker": row.get("ticker") or ticker,
            "proposed_action": row.get("proposed_action"),
            "final_action": row.get("final_action"),
            "execution_status": row.get("execution_status"),
            "risk_result": row.get("risk_result"),
            "reason_codes": (row.get("reason_codes") or [])[:4],
            "governance_decision": governance.get("decision"),
            "risk_rank": governance.get("risk_rank"),
            "position_state": explanation.get("position_state"),
            "final_target": lifecycle.get("final_target"),
            "changed_by": lifecycle.get("changed_by") or [],
            "sort_score": _decision_ledger_sort_score(row),
        })
    rows.sort(key=lambda item: (-int(item.get("sort_score") or 0), str(item.get("ticker") or "")))
    summary = ledger.get("portfolio_summary") or {}
    return {
        "phase": ledger.get("phase"),
        "portfolio_summary": {
            "risk_approved": summary.get("risk_approved"),
            "execution_status": summary.get("execution_status"),
            "governance_available": summary.get("governance_available"),
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
    if not rows and not warnings:
        return ""
    lines = ["<b>Decision ledger</b>"]
    status_bits = []
    if summary.get("risk_approved") is not None:
        status_bits.append(f"risk_approved={bool(summary.get('risk_approved'))}")
    if summary.get("execution_status"):
        status_bits.append(f"execution={summary.get('execution_status')}")
    if summary.get("governance_available") is not None:
        status_bits.append(f"governance={bool(summary.get('governance_available'))}")
    if status_bits:
        lines.append("  " + " | ".join(status_bits))
    for row in rows[:5]:
        ticker = row.get("ticker")
        proposed = row.get("proposed_action") or "hold"
        final = row.get("final_action") or "unknown"
        reasons = ",".join(str(item) for item in (row.get("reason_codes") or [])[:3])
        changed_by = ",".join(str(item) for item in (row.get("changed_by") or [])[:2])
        suffix_parts = []
        if reasons:
            suffix_parts.append(reasons)
        if changed_by:
            suffix_parts.append(f"changed_by={changed_by}")
        suffix = " | " + " | ".join(suffix_parts) if suffix_parts else ""
        lines.append(f"  {ticker}: {proposed} -> {final}{suffix}")
    if warnings:
        lines.append("  warnings: " + "; ".join(str(item) for item in warnings[:3]))
    return "\n".join(lines) + "\n\n"


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
        reasons = ",".join((row.get("reason_codes") or [])[:3])
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
    manual_hints = governance.get("manual_action_hints") or portfolio_summary.get("manual_action_hints") or []
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
