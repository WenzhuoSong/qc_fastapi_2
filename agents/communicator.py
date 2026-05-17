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
    enforcement = risk_out.get("scorecard_enforcement") or {}
    style_enforcement = risk_out.get("style_enforcement") or {}
    position_governance = risk_out.get("position_governance") or {}
    strategy_use_enforcement = (
        researcher_out.get("strategy_use_enforcement")
        or pipeline_context.get("strategy_use_enforcement")
        or {}
    )
    compliance = enforcement.get("post_clip_compliance") or {}
    style_compliance = style_enforcement.get("post_clip_compliance") or {}
    macro_news = news_evidence.get("macro_news_score") or {}
    synth_style = researcher_out.get("style_compliance") or {}
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
        "position_governance": {
            "mode": position_governance.get("mode"),
            "position_decisions": (position_governance.get("position_decisions") or [])[:8],
            "blocked_actions": (position_governance.get("blocked_actions") or [])[:8],
            "forced_trims": (position_governance.get("forced_trims") or [])[:8],
            "replacements": (position_governance.get("replacements") or [])[:8],
            "advisory_overrides": (position_governance.get("advisory_overrides") or [])[:8],
            "trade_summary": position_governance.get("trade_summary") or {},
            "portfolio_summary": position_governance.get("portfolio_summary") or {},
        },
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
    enforcement = p.get("scorecard_enforcement") or {}
    news = p.get("news_evidence") or {}
    style = p.get("decision_style") or {}
    style_enforcement = p.get("style_enforcement") or {}
    strategy_use_enforcement = p.get("strategy_use_enforcement") or {}
    position_governance = p.get("position_governance") or {}

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
    enforcement_line = _format_enforcement_line(enforcement)
    news_line = _format_news_line(news)
    style_line = _format_style_line(style)
    style_enforcement_line = _format_style_enforcement_line(style_enforcement)
    strategy_use_line = _format_strategy_use_enforcement_line(strategy_use_enforcement)
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
            f"{scorecard_line}"
            f"{enforcement_line}"
            f"{style_enforcement_line}"
            f"{strategy_use_line}"
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
        f"{scorecard_line}"
        f"{enforcement_line}"
        f"{style_enforcement_line}"
        f"{strategy_use_line}"
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
    quality = (portfolio_summary.get("advisory_quality") or {}).get("current_run") or {}
    if quality.get("total"):
        lines.append(
            "  advisory quality: "
            f"accepted={int(quality.get('accepted') or 0)}, "
            f"rejected={int(quality.get('rejected') or 0)}, "
            f"converted={int(quality.get('converted') or 0)}"
        )
    return "\n".join(lines) + "\n\n"


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


def _format_position_explanations(portfolio_summary: dict) -> list[str]:
    rows = portfolio_summary.get("position_explanations") or []
    parts: list[str] = []
    for row in rows[:3]:
        ticker = row.get("ticker")
        state = row.get("position_state")
        trigger = row.get("next_trigger")
        why_not_add = (row.get("why_not_add") or [""])[0]
        if ticker and state:
            detail = f" | no add: {why_not_add}" if why_not_add else ""
            trigger_text = f" | next: {trigger}" if trigger else ""
            parts.append(f"  explain {ticker}: {state}{detail}{trigger_text}")
    return parts


def append_command_hints(text: str) -> str:
    cleaned = remove_command_hints(text)
    if cleaned:
        return f"{cleaned}\n\n<b>/confirm</b>  <b>/skip</b>  <b>/pause</b>"
    return "<b>/confirm</b>  <b>/skip</b>  <b>/pause</b>"
