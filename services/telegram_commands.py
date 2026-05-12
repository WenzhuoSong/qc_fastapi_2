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
    if cmd == "/reset_circuit":
        return await _cmd_reset_circuit()
    if cmd == "/approve_strategy":
        return await _cmd_approve_strategy()
    if cmd == "/skip_strategy":
        return await _cmd_skip_strategy()
    return "未识别的指令。可用：/confirm /skip /pause /status /reset_circuit /approve_strategy /skip_strategy"


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


async def _cmd_reset_circuit() -> str:
    """Human command to reset circuit breaker to CLOSED after issue resolution."""
    from datetime import datetime
    async with AsyncSessionLocal() as db:
        circuit_cfg = await get_system_config(db, "circuit_state")
    current = (circuit_cfg.value if circuit_cfg else {}).get("value", "CLOSED")

    if current == "CLOSED":
        return "🟢 Circuit breaker is already CLOSED (normal)."

    # Reset to CLOSED
    from services.circuit_breaker import CircuitState
    async with AsyncSessionLocal() as db:
        await upsert_system_config(
            db,
            "circuit_state",
            {
                "value": CircuitState.CLOSED.value,
                "reason": "human_reset",
                "primary_trigger": "manual_reset",
                "updated_at": datetime.utcnow().isoformat(),
            },
            "user",
        )
    await tool_send_telegram({
        "text": f"🟢 Circuit breaker manually reset to CLOSED by human."
    })
    logger.warning("[circuit_breaker] Circuit manually reset to CLOSED by human command")
    return "🟢 Circuit breaker reset to CLOSED. Pipeline will resume normal operation."


async def _cmd_approve_strategy() -> str:
    """
    Human approves a strategy_revision_v1 recommendation.
    Applies parameter changes to system_config and updates active_strategy.
    """
    from datetime import datetime
    import json

    async with AsyncSessionLocal() as db:
        revision_cfg = await get_system_config(db, "strategy_revision_v1")

    if not revision_cfg:
        return "ℹ️ 没有待审批的策略调整建议。"

    revision = revision_cfg.value or {}
    status = revision.get("status", "")

    if status not in ("pending_approval", ""):
        return (
            f"ℹ️ 策略建议状态为 {status}，无法审批。"
            f"请等待新的季度审查。"
        )

    strategy_rev = revision.get("strategy_revision_v1", {})
    changes_recommended = strategy_rev.get("changes_recommended", False)

    if not changes_recommended:
        # No changes recommended but human wants to override — apply anyway
        param_changes = strategy_rev.get("parameter_changes", {})
        if not param_changes:
            return "⚠️ 策略未推荐任何更改，无需审批。"

    param_changes = strategy_rev.get("parameter_changes", {})
    regime_overrides = strategy_rev.get("regime_overrides", {})
    new_version = strategy_rev.get("version", "2.0")

    async with AsyncSessionLocal() as db:
        # Build new strategy params: merge current params with changes
        active_cfg = await get_system_config(db, "active_strategy")
        current_strategy = (active_cfg.value if active_cfg else {}).get("value", "momentum_lite_v1")

        # Read current strategy params
        params_key = f"strategy_{current_strategy}_params"
        params_cfg = await get_system_config(db, params_key)
        current_params = (params_cfg.value if params_cfg else {}) or {}

        # Apply parameter changes
        updated_params = dict(current_params)
        for param_name, change in param_changes.items():
            if isinstance(change, dict) and "new" in change:
                updated_params[param_name] = change["new"]
            elif isinstance(change, dict) and "adjustment" in change:
                # regime override — store separately
                updated_params[f"override_{param_name}"] = change["adjustment"]

        # Save updated params as new version
        new_params_key = f"strategy_{current_strategy}_v{new_version}_params"
        await upsert_system_config(db, new_params_key, updated_params, "human_approval")

        # Update active strategy
        await upsert_system_config(
            db,
            "active_strategy",
            {"value": current_strategy},
            "human_approval",
        )
        await upsert_system_config(db, "active_strategy_params", updated_params, "human_approval")

        # Record in approved history
        history_cfg = await get_system_config(db, "strategy_approved_history")
        history = (history_cfg.value if history_cfg else {}) or {}
        approved_list = history.get("approved", [])
        approved_list.append({
            "version": new_version,
            "approved_at": datetime.utcnow().isoformat(),
            "approved_by": "human_telegram",
            "change_summary": strategy_rev.get("change_summary", ""),
            "parameter_changes": list(param_changes.keys()),
        })
        history["approved"] = approved_list
        await upsert_system_config(db, "strategy_approved_history", history, "human_approval")

        # Mark revision as approved
        revision["status"] = "approved"
        revision["approved_at"] = datetime.utcnow().isoformat()
        revision["approved_by"] = "human_telegram"
        await upsert_system_config(db, "strategy_revision_v1", revision, "human_approval")

    # Notify via Telegram
    await tool_send_telegram({
        "text": (
            f"✅ 策略调整已审批并应用！\n"
            f"Version: {new_version}\n"
            f"Changes: {strategy_rev.get('change_summary', 'N/A')}\n"
            f"Parameters updated: {list(param_changes.keys())}\n"
            f"New params stored in {new_params_key}\n"
            f"Pipeline will use updated params from next run."
        )
    })
    logger.warning(
        f"[STRATEGY_APPROVAL] Approved v{new_version}: "
        f"{param_changes.keys()} — applied by human"
    )
    return (
        f"✅ 策略调整已审批并应用！\n"
        f"Version: {new_version}\n"
        f"Changes: {strategy_rev.get('change_summary', 'N/A')}\n"
        f"Updated params: {list(param_changes.keys())}"
    )


async def _cmd_skip_strategy() -> str:
    """Human rejects or skips a strategy_revision_v1 recommendation."""
    from datetime import datetime

    async with AsyncSessionLocal() as db:
        revision_cfg = await get_system_config(db, "strategy_revision_v1")

    if not revision_cfg:
        return "ℹ️ 没有待审批的策略调整建议。"

    revision = revision_cfg.value or {}
    status = revision.get("status", "")

    if status not in ("pending_approval", ""):
        return f"ℹ️ 策略建议状态为 {status}，无法跳过。"

    revision["status"] = "rejected"
    revision["rejected_at"] = datetime.utcnow().isoformat()
    revision["rejected_by"] = "human_telegram"

    async with AsyncSessionLocal() as db:
        await upsert_system_config(db, "strategy_revision_v1", revision, "human_telegram")

    await tool_send_telegram({
        "text": "⏭️ 策略调整建议已跳过。当前提名策略保持不变。"
    })
    logger.info("[STRATEGY_APPROVAL] Rejected by human via /skip_strategy")
    return "⏭️ 策略调整建议已跳过。当前提名策略保持不变。"
