# services/telegram_commands.py
"""
Telegram 命令处理。由 web 服务的 telegram_webhook 调用。
"""
import logging
from datetime import datetime, timedelta

from db.session         import AsyncSessionLocal
from db.queries         import get_system_config, upsert_system_config, get_latest_portfolio
from tools.db_tools     import tool_verify_approval_token
from tools.qc_tools     import tool_send_weight_command
from tools.notify_tools import tool_send_telegram
from services.proposal  import load_pending_proposal, mark_proposal_done
from services.pc_promotion_config import default_pc_promotion_config, format_pc_promotion_config
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
    if cmd == "/config":
        return await _cmd_config(text)
    if cmd == "/pc_promotion":
        return await _cmd_pc_promotion(text)
    return "Unknown command. Available: /confirm /skip /pause /status /reset_circuit /approve_strategy /skip_strategy /config /pc_promotion"


async def _cmd_confirm() -> str:
    pending = await load_pending_proposal()
    if not pending or pending.get("status") != "pending":
        return await _cmd_confirm_circuit_override()

    weights = pending.get("weights", {})
    token   = pending.get("token", "")

    verify = await tool_verify_approval_token({"token": token})
    if not verify.get("valid"):
        return f"❌ Token {verify.get('reason')}. Please wait for the next analysis."

    analysis_id = pending.get("analysis_id")
    command_id = f"analysis_{analysis_id}" if analysis_id else None
    result = await tool_send_weight_command({"weights": weights, "command_id": command_id})
    if result.get("success"):
        await mark_proposal_done(pending.get("analysis_id"), "executed_user_confirmed")
        return "✅ Execution confirmed."
    return f"❌ Execution failed: {result.get('error')}"


async def _cmd_confirm_circuit_override() -> str:
    async with AsyncSessionLocal() as db:
        circuit_cfg = await get_system_config(db, "circuit_state")
    circuit = _circuit_state_from_cfg(circuit_cfg)
    if circuit not in {"ALERT", "DEFENSIVE"}:
        return "No pending proposal."

    now = datetime.utcnow()
    expires_at = now + timedelta(minutes=30)
    async with AsyncSessionLocal() as db:
        await upsert_system_config(
            db,
            "circuit_override",
            {
                "value": "ONE_SHOT",
                "circuit_state": circuit,
                "uses_remaining": 1,
                "created_at": now.isoformat(),
                "expires_at": expires_at.isoformat(),
                "reason": "human_confirmed_circuit_override",
            },
            "user",
        )
    return (
        f"✅ Circuit override armed for the next FULL_AUTO run "
        f"while Circuit={circuit}. Risk manager will still run in DEFENSIVE mode. "
        f"Use /reset_circuit only after the condition is resolved."
    )


async def _cmd_skip() -> str:
    pending = await load_pending_proposal()
    if not pending or pending.get("status") != "pending":
        return "No pending proposal."
    await mark_proposal_done(pending.get("analysis_id"), "skipped_by_user")
    return "⏭️ Skipped. No action this cycle."


async def _cmd_pause() -> str:
    async with AsyncSessionLocal() as db:
        await upsert_system_config(db, "authorization_mode", {"value": "MANUAL"}, "user")
    return "⏸️ Switched to MANUAL mode. Automatic analysis is paused.\nUse /confirm resume to resume."


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
        f"📊 System status\n"
        f"  Authorization mode: {mode}\n"
        f"  Circuit state: {circuit}\n"
        f"  Portfolio value: ${val:,.0f}\n"
        f"  Drawdown: -{dd:.2%}"
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


def _circuit_state_from_cfg(circuit_cfg) -> str:
    return str((circuit_cfg.value if circuit_cfg else {}).get("value", "CLOSED"))


async def _cmd_config(text: str) -> str:
    """
    Read/update whitelisted runtime config.

    Usage:
      /config
      /config position_manager_config max_new_buys_per_cycle 2
      /config pm max_turnover_per_cycle 0.25
    """
    allowed_keys = {
        "max_new_buys_per_cycle",
        "max_positions",
        "max_single_trade_pct",
        "max_turnover_per_cycle",
        "max_daily_trades",
        "min_hold_days",
    }
    parts = text.strip().split()

    async with AsyncSessionLocal() as db:
        cfg = await get_system_config(db, "position_manager_config")
        current = (cfg.value if cfg else {}) or {}

    if len(parts) == 1:
        rows = [f"{k}: {current.get(k)}" for k in sorted(allowed_keys)]
        return "⚙️ position_manager_config\n" + "\n".join(rows)

    if len(parts) != 4 or parts[1] not in ("pm", "position_manager_config"):
        return (
            "Usage: /config pm <key> <value>\n"
            "Allowed keys: " + ", ".join(sorted(allowed_keys))
        )

    key = parts[2]
    if key not in allowed_keys:
        return f"❌ Cannot modify {key}。Allowed keys: " + ", ".join(sorted(allowed_keys))

    try:
        value = _parse_config_value(parts[3])
    except ValueError as e:
        return f"❌ Invalid parameter: {e}"

    updated = dict(current)
    updated[key] = value
    async with AsyncSessionLocal() as db:
        await upsert_system_config(db, "position_manager_config", updated, "telegram_config")

    return f"✅ Updated position_manager_config.{key} = {value}"


def _parse_config_value(raw: str) -> int | float:
    if "." in raw:
        value = float(raw)
    else:
        value = int(raw)
    if value < 0:
        raise ValueError("value must be non-negative")
    return value


async def _cmd_pc_promotion(text: str) -> str:
    """
    Configure Portfolio Construction promotion gate.

    Usage:
      /pc_promotion
      /pc_promotion status
      /pc_promotion on
      /pc_promotion off
      /pc_promotion auto
      /pc_promotion manual
    """
    parts = text.strip().lower().split()
    async with AsyncSessionLocal() as db:
        cfg = await get_system_config(db, "portfolio_construction_promotion_config")
        current = default_pc_promotion_config((cfg.value if cfg else {}) or {})

    if len(parts) == 1 or parts[1] == "status":
        return format_pc_promotion_config(current)

    if len(parts) != 2 or parts[1] not in {"on", "off", "auto", "manual"}:
        return "Usage: /pc_promotion status|on|off|auto|manual"

    updated = dict(current)
    if parts[1] == "on":
        updated["enabled"] = True
    elif parts[1] == "off":
        updated["enabled"] = False
    elif parts[1] == "auto":
        updated["enabled"] = True
        updated["require_manual_approval"] = False
    elif parts[1] == "manual":
        updated["enabled"] = True
        updated["require_manual_approval"] = True

    updated["updated_at"] = datetime.utcnow().isoformat()
    updated["updated_by"] = "telegram"
    async with AsyncSessionLocal() as db:
        await upsert_system_config(db, "portfolio_construction_promotion_config", updated, "telegram_config")

    return "✅ Updated Portfolio Construction promotion gate\n" + format_pc_promotion_config(updated)


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
        return "ℹ️ No pending strategy revision."

    revision = revision_cfg.value or {}
    status = revision.get("status", "")

    if status not in ("pending_approval", ""):
        return (
            f"ℹ️ Strategy revision status is {status}. It cannot be approved."
            f"Please wait for the next quarterly review."
        )

    strategy_rev = revision.get("strategy_revision_v1", {})
    changes_recommended = strategy_rev.get("changes_recommended", False)

    if not changes_recommended:
        # No changes recommended but human wants to override — apply anyway
        param_changes = strategy_rev.get("parameter_changes", {})
        if not param_changes:
            return "⚠️ No strategy changes were recommended; approval is not needed."

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
            f"✅ Strategy revision approved and applied!\n"
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
        f"✅ Strategy revision approved and applied!\n"
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
        return "ℹ️ No pending strategy revision."

    revision = revision_cfg.value or {}
    status = revision.get("status", "")

    if status not in ("pending_approval", ""):
        return f"ℹ️ Strategy revision status is {status}. It cannot be skipped."

    revision["status"] = "rejected"
    revision["rejected_at"] = datetime.utcnow().isoformat()
    revision["rejected_by"] = "human_telegram"

    async with AsyncSessionLocal() as db:
        await upsert_system_config(db, "strategy_revision_v1", revision, "human_telegram")

    await tool_send_telegram({
        "text": "⏭️ Strategy revision skipped. The current nominated strategy remains unchanged."
    })
    logger.info("[STRATEGY_APPROVAL] Rejected by human via /skip_strategy")
    return "⏭️ Strategy revision skipped. The current nominated strategy remains unchanged."
