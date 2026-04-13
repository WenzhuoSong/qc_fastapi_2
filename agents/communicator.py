# agents/communicator.py
"""
COMMUNICATOR —— Telegram 文案生成层（LLM + 强制 fallback）

职责：
    把 researcher + strategy + risk 的结构化结果包装成自然语言卡片，
    发给 Telegram。严格 5s 超时，任何异常都降级到 Python f-string 模板。

本层 **不在正确性路径上**：LLM 文案只影响可读性，
fallback 模板即便生硬也必须能把卡片发出去。
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


COMMUNICATOR_SYSTEM_PROMPT = """你是量化交易系统的 Telegram 文案编辑。
你拿到的是已经做完决策的结构化数据，你的任务是把它翻译成**简洁、专业、人类可读的 Telegram 卡片**。

硬性格式要求：
- 使用 HTML（<b>粗体</b>），不要 Markdown
- 总长度不超过 800 字符
- 必须包含 5 个区块：多空辩论摘要 / 市场判断 / 调仓动作 / 风控结果 / 命令提示
- 多空辩论摘要：展示 Bull/Bear 置信度和仲裁结果（1-2 行即可）
- 不要编造数字，只能复述给你的字段
- 不要使用表情符号之外的图形字符

输出要求：直接返回纯文本卡片内容，不要任何解释、不要 JSON、不要 markdown 围栏。"""


async def run_communicator_async(
    pipeline_context: dict,
    researcher_out:   dict,
    risk_out:         dict,
) -> dict:
    """
    生成 Telegram 文案。
    返回: {"text": str, "used_fallback": bool}

    新架构下 target_weights / rebalance_actions / overlays_applied 全部来自
    risk_out（Stage 4 Risk Manager 产出的最终执行方案）。
    """
    payload = _build_payload(pipeline_context, researcher_out, risk_out)

    try:
        text = await asyncio.wait_for(_llm_format(payload), timeout=LLM_TIMEOUT_SECONDS)
        if text and text.strip():
            return {"text": text.strip(), "used_fallback": False}
        logger.warning("COMMUNICATOR: empty LLM response, falling back")
    except asyncio.TimeoutError:
        logger.warning(f"COMMUNICATOR: LLM timeout after {LLM_TIMEOUT_SECONDS}s, falling back")
    except Exception as e:
        logger.warning(f"COMMUNICATOR: LLM failed ({e}), falling back")

    return {"text": _fallback_template(payload), "used_fallback": True}


# ─────────────────────────────── LLM 路径 ───────────────────────────────


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


# ─────────────────────────────── 数据打包 ───────────────────────────────


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
        "target_weights":   risk_out.get("target_weights", {}),
        "rebalance_actions":risk_out.get("rebalance_actions", []),
        "estimated_cost":   risk_out.get("estimated_cost_pct", 0),
        "overlays_applied": risk_out.get("overlays_applied", []),
        "rejection_reasons":risk_out.get("rejection_reasons", []),
        "auth_mode":        pipeline_context.get("auth_mode", "SEMI_AUTO"),
        "timeout_minutes":  settings.semi_auto_timeout_minutes,
        # V2.1 Bull/Bear debate summary
        "debate_summary":   debate,
    }


# ─────────────────────────────── Fallback 模板 ───────────────────────────────


def _fallback_template(p: dict) -> str:
    approved = p["approved"]
    regime   = p["regime"]
    stance   = p["stance"]
    actions  = p["rebalance_actions"] or []
    cost     = float(p["estimated_cost"] or 0)
    overlays = p["overlays_applied"] or []
    debate   = p.get("debate_summary") or {}

    up, down = "\u25b2", "\u25bc"

    # Bull/Bear 辩论摘要行
    debate_line = ""
    if debate:
        bull_conf = debate.get("bull_confidence", 0)
        bear_conf = debate.get("bear_confidence", 0)
        bull_st   = debate.get("bull_stance", "?")
        bear_st   = debate.get("bear_stance", "?")
        resolution = debate.get("resolution", "")[:80]
        debate_line = (
            f"<b>多空辩论</b>\n"
            f"  {up} Bull: {bull_st} ({bull_conf:.0%}) "
            f"vs {down} Bear: {bear_st} ({bear_conf:.0%})\n"
        )
        if resolution:
            debate_line += f"  → {resolution}\n"
        debate_line += "\n"

    if not approved:
        reasons_text = "\n".join(f"  - {r}" for r in p["rejection_reasons"]) or "  - 未提供原因"
        return (
            f"🚫 <b>调仓被风控拒绝</b>\n"
            f"――――――――――――――――\n"
            f"{debate_line}"
            f"🌡️ 市场制度: {regime}\n"
            f"📊 建议立场: {stance}\n\n"
            f"<b>风控失败项:</b>\n{reasons_text}\n\n"
            f"本轮不执行，等待下一次分析。"
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
        actions_str = "  无调仓操作（保持当前持仓）"

    overlay_line = f"🔧 已应用: {', '.join(overlays)}\n" if overlays else ""

    return (
        f"📋 <b>调仓建议</b>\n"
        f"――――――――――――――――\n"
        f"{debate_line}"
        f"🌡️ 市场制度: {regime}\n"
        f"📊 建议立场: {stance}\n\n"
        f"<b>建议操作</b>\n"
        f"{actions_str}\n\n"
        f"💰 预估成本: {cost:.2%}\n"
        f"🛡️ 风控: ✅ APPROVED\n"
        f"{overlay_line}"
        f"\n⏱️ {p['timeout_minutes']} 分钟后无回复 → 市场正常时自动执行\n"
        f"\n<b>/confirm</b>  <b>/skip</b>  <b>/pause</b>"
    )
