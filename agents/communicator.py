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

    if not approved:
        reasons_text = "\n".join(f"  - {r}" for r in p["rejection_reasons"]) or "  - No reason provided"
        return (
            f"🚫 <b>Rebalance rejected by risk</b>\n"
            f"――――――――――――――――\n"
            f"{debate_line}"
            f"🌡️ Regime: {regime}\n"
            f"📊 Stance: {stance}\n\n"
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
        f"<b>Suggested actions</b>\n"
        f"{actions_str}\n\n"
        f"💰 Est. cost: {cost:.2%}\n"
        f"🛡️ Risk: ✅ APPROVED\n"
        f"{overlay_line}"
        f"\n⏱️ No reply in {p['timeout_minutes']} min → auto-execute when market is normal"
        f"{command_buttons}"
    )


def append_command_hints(text: str) -> str:
    cleaned = remove_command_hints(text)
    if cleaned:
        return f"{cleaned}\n\n<b>/confirm</b>  <b>/skip</b>  <b>/pause</b>"
    return "<b>/confirm</b>  <b>/skip</b>  <b>/pause</b>"
