# services/telegram_commands.py
"""
Telegram 命令处理。由 web 服务的 telegram_webhook 调用。
"""
import logging

from db.session         import AsyncSessionLocal
from db.queries         import get_system_config, upsert_system_config, get_latest_portfolio
from tools.db_tools     import tool_verify_approval_token
from tools.qc_tools     import tool_send_weight_command
from services.proposal  import load_pending_proposal, mark_proposal_done
from config             import get_settings

logger   = logging.getLogger("qc_fastapi_2.tg_cmd")
settings = get_settings()


async def handle_telegram_command(text: str, from_chat_id: str) -> str:
    if from_chat_id != settings.tg_chat_id:
        logger.warning(f"Unauthorized Telegram access from chat_id={from_chat_id}")
        return ""

    cmd = text.strip().lower().split()[0]

    if cmd == "/confirm":
        return await _cmd_confirm()
    if cmd == "/skip":
        return await _cmd_skip()
    if cmd == "/pause":
        return await _cmd_pause()
    if cmd == "/status":
        return await _cmd_status()
    return "未识别的指令。可用：/confirm /skip /pause /status"


async def _cmd_confirm() -> str:
    pending = await load_pending_proposal()
    if not pending or pending.get("status") != "pending":
        return "当前没有待确认建议。"

    weights = pending.get("weights", {})
    token   = pending.get("token", "")

    verify = await tool_verify_approval_token({"token": token})
    if not verify.get("valid"):
        return f"❌ Token {verify.get('reason')}，请等待下一次分析。"

    result = await tool_send_weight_command({"weights": weights})
    if result.get("success"):
        await mark_proposal_done(pending.get("analysis_id"), "executed_user_confirmed")
        return "✅ 已确认执行！"
    return f"❌ 执行失败：{result.get('error')}"


async def _cmd_skip() -> str:
    pending = await load_pending_proposal()
    if not pending or pending.get("status") != "pending":
        return "当前没有待确认建议。"
    await mark_proposal_done(pending.get("analysis_id"), "skipped_by_user")
    return "⏭️ 已跳过，本周期不操作。"


async def _cmd_pause() -> str:
    async with AsyncSessionLocal() as db:
        await upsert_system_config(db, "authorization_mode", {"value": "MANUAL"}, "user")
    return "⏸️ 已切换到 MANUAL 模式。将不再自动分析。\n/confirm resume 可恢复。"


async def _cmd_status() -> str:
    async with AsyncSessionLocal() as db:
        auth_cfg    = await get_system_config(db, "authorization_mode")
        circuit_cfg = await get_system_config(db, "circuit_state")
        latest      = await get_latest_portfolio(db)

    mode    = (auth_cfg.value    if auth_cfg    else {}).get("value", "SEMI_AUTO")
    circuit = (circuit_cfg.value if circuit_cfg else {}).get("value", "CLOSED")
    val     = float(latest.total_value or 0)          if latest else 0
    dd      = float(latest.current_drawdown_pct or 0) if latest else 0
    return (
        f"📊 系统状态\n"
        f"  授权模式: {mode}\n"
        f"  熔断状态: {circuit}\n"
        f"  净值: ${val:,.0f}\n"
        f"  回撤: -{dd:.2%}"
    )
