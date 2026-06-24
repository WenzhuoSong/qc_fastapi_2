# services/telegram_commands.py
"""
Telegram 命令处理。由 web 服务的 telegram_webhook 调用。
"""
import logging
from datetime import datetime, timedelta

from db.session         import AsyncSessionLocal
from db.queries         import get_system_config, upsert_system_config, get_latest_portfolio
from tools.db_tools     import tool_verify_approval_token
from tools.qc_tools     import tool_send_cancel_orders_command
from tools.notify_tools import tool_send_telegram
from services.qc_command_sender import send_setweights_command
from services.broker_order_filter import apply_broker_order_filter
from services.proposal  import load_pending_proposal, mark_proposal_done, validate_proposal_still_relevant
from services.pc_promotion_config import default_pc_promotion_config, format_pc_promotion_config
from services.execution_log_store import (
    create_or_update_submitted_log,
    force_reconcile_command,
    record_cancel_orders_requested,
    record_active_execution_wait,
    record_preflight_block,
    record_recent_same_target_dedupe,
)
from services.execution_lifecycle import evaluate_active_execution_gate, load_active_execution_command
from services.execution_policy import policy_snapshot
from services.execution_preflight import (
    check_recent_same_target_dedupe,
    format_command_preflight_blockers,
    preflight_execution_command,
    preflight_execution_weights,
)
from services.account_state_guard import default_account_state_guard_config, load_latest_account_state_guard
from services.policy_alignment import (
    default_manual_confirm_policy_alignment_config,
    policy_alignment_from_account_guard,
)
from services.operator_halt import (
    CONFIG_KEY as OPERATOR_HALT_CONFIG_KEY,
    build_operator_halt_state,
    normalize_operator_halt_state,
)
from services.newbase_monitoring import (
    is_active_newbase_observer,
    is_newbase_observer_strategy,
)
from config             import get_settings

logger   = logging.getLogger("qc_fastapi_2.tg_cmd")
settings = get_settings()

_HIGH_RISK_COMMANDS_DISABLED_IN_NEWBASE = {
    "/confirm",
    "/cancel_orders",
    "/approve_strategy",
    "/skip_strategy",
    "/pc_promotion",
    "/force_reconcile",
}


async def handle_telegram_command(text: str, from_chat_id: str) -> str:
    if from_chat_id != settings.tg_chat_id:
        logger.warning(f"Unauthorized Telegram access from chat_id={from_chat_id}")
        return ""

    cmd = text.strip().lower().split()[0]

    if cmd in _HIGH_RISK_COMMANDS_DISABLED_IN_NEWBASE and await is_active_newbase_observer():
        return _newbase_observer_disabled_command_message(cmd)

    if cmd == "/confirm":
        return await _cmd_confirm()
    if cmd == "/skip":
        return await _cmd_skip()
    if cmd == "/pause":
        return await _cmd_pause()
    if cmd == "/halt":
        return await _cmd_halt(text)
    if cmd == "/resume":
        return await _cmd_resume(text)
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
    if cmd == "/force_reconcile":
        return await _cmd_force_reconcile(text)
    if cmd == "/cancel_orders":
        return await _cmd_cancel_orders(text)
    return "Unknown command. Available: /confirm /skip /pause /halt /resume /status /reset_circuit /approve_strategy /skip_strategy /config /pc_promotion /force_reconcile /cancel_orders"


def _newbase_observer_disabled_command_message(cmd: str) -> str:
    return (
        f"{cmd} is disabled while active_strategy=newbase.\n"
        "FastAPI/Railway is observer-only for newBase: it may record, audit, "
        "monitor, and report, but it must not send SetWeights, CancelOrders, "
        "or apply strategy revisions."
    )


async def _cmd_confirm() -> str:
    pending = await load_pending_proposal()
    if not pending or pending.get("status") != "pending":
        return await _cmd_confirm_circuit_override()

    weights = pending.get("weights", {})
    proposed_weights = dict(weights or {})
    token   = pending.get("token", "")
    final_validation = pending.get("final_validation") or {}
    if not final_validation or not final_validation.get("approved"):
        return "❌ Final risk validation missing or failed. Please wait for the next analysis."

    async with AsyncSessionLocal() as db:
        latest_row = await get_latest_portfolio(db)
    valid, reason = await validate_proposal_still_relevant(pending, latest_row)
    if not valid:
        await mark_proposal_done(pending.get("analysis_id"), f"skipped_invalidation_{reason}")
        return (
            "❌ Proposal invalidated before confirmation. "
            f"reason={reason}. Please wait for the next analysis."
        )

    analysis_id = pending.get("analysis_id")
    command_id = f"analysis_{analysis_id}" if analysis_id else None
    weight_preflight = preflight_execution_weights(weights)
    if not weight_preflight.get("allowed"):
        return f"❌ Execution preflight blocked: {weight_preflight.get('policy_evaluation', {}).get('violations')}"
    current_weights = final_validation.get("current_weights") or {}

    policy = policy_snapshot()
    _account_guard, policy_alignment = await _load_manual_confirm_policy_alignment(
        expected_policy_version=str(policy.get("version") or "")
    )
    policy_sync = None
    if not policy_alignment.get("aligned"):
        age = policy_alignment.get("age_seconds")
        max_age = policy_alignment.get("max_age_seconds")
        if age is None or not policy_alignment.get("age_ok"):
            age_text = "unknown" if age is None else f"{float(age):.0f}s"
            return (
                "❌ No recent account state policy alignment. "
                f"latest_age={age_text}, max_age={float(max_age or 0):.0f}s. "
                "Wait for the next pipeline/account-state refresh before confirming."
            )
        return (
            "❌ Policy version is not aligned. "
            f"expected={policy_alignment.get('expected_policy_version')} "
            f"actual={policy_alignment.get('actual_policy_version')}. "
            "Deploy/sync the QC compiled policy before confirming."
        )

    async with AsyncSessionLocal() as db:
        lifecycle_cfg = await get_system_config(db, "execution_lifecycle_config")
        execution_command_cfg = await get_system_config(db, "execution_command_config")
    broker_order_filter = await apply_broker_order_filter(
        target_weights=weights,
        current_weights=current_weights,
        config=(execution_command_cfg.value if execution_command_cfg else {}) or {},
    )
    if broker_order_filter.get("adjusted"):
        weights = broker_order_filter.get("target_weights") or weights
        post_broker_preflight = preflight_execution_weights(weights)
        if not post_broker_preflight.get("allowed"):
            return (
                "❌ Broker order filter suppressed micro orders, but the filtered target no longer "
                "satisfies execution policy. No command sent to QC."
            )
    if broker_order_filter.get("no_executable_delta"):
        await mark_proposal_done(pending.get("analysis_id"), "skipped_broker_order_filter")
        return (
            "⏭ Command not sent: broker order filter left no executable delta.\n"
            f"Suppressed micro orders: {len(broker_order_filter.get('suppressed_orders') or [])}. "
            f"Rounded buy orders: {len(broker_order_filter.get('rounded_orders') or [])}."
        )

    active_execution = await load_active_execution_command()
    active_execution_gate = evaluate_active_execution_gate(
        target_weights=weights,
        active_execution=active_execution,
        config=(lifecycle_cfg.value if lifecycle_cfg else {}) or {},
    )
    if not active_execution_gate.get("allowed"):
        await record_active_execution_wait(
            command_id=command_id or "unknown",
            analysis_id=analysis_id,
            target_weights=weights,
            active_execution_gate=active_execution_gate,
            policy_version=policy.get("version"),
        )
        active_label = active_execution_gate.get("active_command_id") or "unknown"
        return (
            "⏳ Command not sent: active execution is still pending reconciliation.\n"
            f"active_command={active_label}\n"
            f"status={active_execution_gate.get('status')} "
            f"classification={active_execution_gate.get('classification')}\n"
            "Wait for QC heartbeat reconciliation before confirming again."
        )

    command_preflight = await preflight_execution_command(
        command_id=command_id or "",
        analysis_id=analysis_id,
        target_weights=weights,
        current_weights=current_weights,
        policy_version=policy.get("version"),
        policy_sync_result=policy_sync,
        policy_alignment_result=policy_alignment,
        config=(execution_command_cfg.value if execution_command_cfg else {}) or {},
    )
    command_preflight["broker_order_filter"] = broker_order_filter
    if not command_preflight.get("allowed"):
        if "command_id_idempotent" not in (command_preflight.get("blockers") or []):
            await record_preflight_block(
                command_id=command_id or "unknown",
                analysis_id=analysis_id,
                target_weights=weights,
                preflight_result=command_preflight,
                policy_version=policy.get("version"),
                policy_sync_result=policy_sync,
            )
        return "❌ Command preflight blocked:\n" + format_command_preflight_blockers(command_preflight)

    command_cfg = command_preflight.get("config") or {}
    same_target_dedupe = await check_recent_same_target_dedupe(
        proposed_target=weights,
        command_id=command_id or "",
        policy_version=policy.get("version"),
        command_type="SetWeights",
        lookback_minutes=int(command_cfg.get("recent_same_target_dedupe_minutes") or 5),
        tolerance=float(command_cfg.get("recent_same_target_dedupe_tolerance") or 0.005),
    )
    if not same_target_dedupe.get("should_send", True):
        await record_recent_same_target_dedupe(
            command_id=command_id or "unknown",
            analysis_id=analysis_id,
            target_weights=weights,
            dedupe_result=same_target_dedupe,
            policy_version=policy.get("version"),
            preflight_result=command_preflight,
        )
        await mark_proposal_done(pending.get("analysis_id"), "deduped")
        reference_id = same_target_dedupe.get("reference_command_id") or "unknown"
        fp = str(same_target_dedupe.get("target_fingerprint") or "")[:12] or "n/a"
        return (
            "⏭ Command deduped. No command sent to QC.\n"
            f"Recent reconciled command={reference_id}\n"
            f"target_fingerprint={fp}"
        )

    verify = await tool_verify_approval_token({"token": token})
    if not verify.get("valid"):
        return f"❌ Token {verify.get('reason')}. Please wait for the next analysis."

    target_fingerprint = same_target_dedupe.get("target_fingerprint")
    result = await send_setweights_command(
        weights=weights,
        command_id=command_id,
        analysis_id=analysis_id,
        policy_version=policy.get("version"),
        target_fingerprint=target_fingerprint,
    )
    if result.get("success"):
        await create_or_update_submitted_log(
            command_id=command_id or result.get("command_id"),
            target_weights=weights,
            proposed_weights=proposed_weights,
            analysis_id=analysis_id,
            policy_version=policy.get("version"),
            preflight_result=command_preflight,
            policy_sync_result=policy_sync,
            qc_response=result.get("response"),
        )
        await mark_proposal_done(pending.get("analysis_id"), "executed_user_confirmed")
        return "✅ Execution confirmed."
    return f"❌ Execution failed: {result.get('error')}"


async def _load_manual_confirm_policy_alignment(*, expected_policy_version: str) -> tuple[dict, dict]:
    async with AsyncSessionLocal() as db:
        account_guard_cfg = await get_system_config(db, "account_state_guard_config")
        alignment_cfg = await get_system_config(db, "manual_confirm_policy_alignment_config")

    guard_config = default_account_state_guard_config((account_guard_cfg.value if account_guard_cfg else {}) or {})
    guard_config["expected_policy_version"] = expected_policy_version
    manual_config = default_manual_confirm_policy_alignment_config(
        (alignment_cfg.value if alignment_cfg else {}) or {}
    )
    if not manual_config.get("enabled", True):
        return {}, {
            "source": "manual_confirm_policy_alignment_config",
            "aligned": True,
            "bypass": True,
            "max_age_seconds": manual_config.get("max_age_seconds"),
        }

    account_guard = await load_latest_account_state_guard(config=guard_config)
    policy_alignment = policy_alignment_from_account_guard(
        account_guard,
        expected_policy_version=expected_policy_version,
        max_age_seconds=float(manual_config.get("max_age_seconds") or 300),
    )
    return account_guard, policy_alignment


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


async def _cmd_halt(text: str) -> str:
    reason = _command_reason(text) or "operator_halt"
    state = build_operator_halt_state(
        halted=True,
        reason=reason,
        updated_by="telegram",
    )
    async with AsyncSessionLocal() as db:
        await upsert_system_config(db, OPERATOR_HALT_CONFIG_KEY, state, "telegram")
    return (
        "🛑 Operator halt enabled.\n"
        "New SEMI_AUTO and FULL_AUTO pipeline runs will stop before analysis/execution.\n"
        f"Reason: {reason}\n"
        "Use /resume <reason> to clear only the operator halt latch."
    )


async def _cmd_resume(text: str) -> str:
    reason = _command_reason(text) or "operator_resume"
    state = build_operator_halt_state(
        halted=False,
        reason=reason,
        updated_by="telegram",
    )
    async with AsyncSessionLocal() as db:
        await upsert_system_config(db, OPERATOR_HALT_CONFIG_KEY, state, "telegram")
    return (
        "✅ Operator halt cleared.\n"
        "Circuit and reconciliation halt, if active, are not cleared by /resume.\n"
        f"Reason: {reason}"
    )


async def _cmd_status() -> str:
    async with AsyncSessionLocal() as db:
        auth_cfg    = await get_system_config(db, "authorization_mode")
        active_cfg  = await get_system_config(db, "active_strategy")
        circuit_cfg = await get_system_config(db, "circuit_state")
        halt_cfg    = await get_system_config(db, OPERATOR_HALT_CONFIG_KEY)
        latest      = await get_latest_portfolio(db)

    mode    = (auth_cfg.value    if auth_cfg    else {}).get("value", "SEMI_AUTO")
    active_strategy = (active_cfg.value if active_cfg else {}).get("value", "unknown")
    control_mode = (
        "newbase_observer_only"
        if is_newbase_observer_strategy(active_strategy)
        else "legacy_execution_pipeline"
    )
    circuit = (circuit_cfg.value if circuit_cfg else {}).get("value", "CLOSED")
    halt_state = normalize_operator_halt_state(halt_cfg.value if halt_cfg else None)
    val     = float(latest.total_value or 0)          if latest else 0
    dd      = float(latest.current_drawdown_pct or 0) if latest else 0
    return (
        f"📊 System status\n"
        f"  Authorization mode: {mode}\n"
        f"  Active strategy: {active_strategy}\n"
        f"  FastAPI control mode: {control_mode}\n"
        f"  Circuit state: {circuit}\n"
        f"  Operator halt: {'HALTED' if halt_state.get('halted') else 'running'}\n"
        f"  Operator halt reason: {halt_state.get('reason') or 'none'}\n"
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


async def _cmd_force_reconcile(text: str) -> str:
    parts = text.strip().split()
    if len(parts) != 2:
        return "Usage: /force_reconcile <command_id>"
    command_id = parts[1].strip()
    result = await force_reconcile_command(
        command_id=command_id,
        operator="telegram",
        reason="operator_force_reconcile",
    )
    if not result.get("success"):
        return f"❌ Force reconcile failed: {result.get('error')}"
    return (
        f"✅ Force reconciled `{result.get('command_id')}`\n"
        f"Status: {result.get('status')}\n"
        f"Max drift: {float(result.get('max_abs_diff') or 0.0):.4%}\n"
        f"Diff rows: {result.get('diff_count')}\n"
        "Next cycle will use QC actual holdings as baseline."
    )


async def _cmd_cancel_orders(text: str) -> str:
    parts = text.strip().split()
    target_command_id = parts[1].strip() if len(parts) >= 2 else ""
    active = None
    if not target_command_id:
        active = await load_active_execution_command()
        target_command_id = str((active or {}).get("command_id") or "").strip()
    if not target_command_id:
        return "ℹ️ No active execution command found. Use /cancel_orders <command_id> if you know the command."

    cancel_command_id = (
        f"cancel_orders_{target_command_id}_{int(datetime.utcnow().timestamp())}"
    )
    result = await tool_send_cancel_orders_command({
        "command_id": cancel_command_id,
        "target_command_id": target_command_id,
        "reason": "operator_cancel_orders",
    })
    await record_cancel_orders_requested(
        active_command_id=target_command_id,
        cancel_command_id=cancel_command_id,
        operator="telegram",
        qc_result=result,
    )
    if result.get("success"):
        return (
            f"🧯 CancelOrders sent for `{target_command_id}`\n"
            f"Control command: `{cancel_command_id}`\n"
            "Wait for QC heartbeat reconciliation before sending ordinary rebalance."
        )
    return f"❌ CancelOrders failed for `{target_command_id}`: {result.get('error')}"


def _circuit_state_from_cfg(circuit_cfg) -> str:
    return str((circuit_cfg.value if circuit_cfg else {}).get("value", "CLOSED"))


def _command_reason(text: str) -> str:
    parts = text.strip().split(maxsplit=1)
    if len(parts) < 2:
        return ""
    return parts[1].strip()[:240]


async def _cmd_config(text: str) -> str:
    """
    Read/update whitelisted runtime config.

    Usage:
      /config
      /config position_manager_config max_new_buys_per_cycle 4
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

    if len(parts) == 1:
        async with AsyncSessionLocal() as db:
            cfg = await get_system_config(db, "position_manager_config")
        current = (cfg.value if cfg else {}) or {}
        rows = [f"{k}: {current.get(k)}" for k in sorted(allowed_keys)]
        return "⚙️ position_manager_config\n" + "\n".join(rows)

    async with AsyncSessionLocal() as db:
        cfg = await get_system_config(db, "position_manager_config")
        current = (cfg.value if cfg else {}) or {}

    if len(parts) != 4 or parts[1] not in ("pm", "position_manager_config"):
        return (
            "Usage: /config pm <key> <value>\n"
            "Allowed PM keys: " + ", ".join(sorted(allowed_keys))
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
      /pc_promotion gated
    """
    parts = text.strip().lower().split()
    async with AsyncSessionLocal() as db:
        cfg = await get_system_config(db, "portfolio_construction_promotion_config")
        current = default_pc_promotion_config((cfg.value if cfg else {}) or {})

    if len(parts) == 1 or parts[1] == "status":
        return format_pc_promotion_config(current)

    if len(parts) != 2 or parts[1] not in {"on", "off", "auto", "manual", "gated"}:
        return "Usage: /pc_promotion status|on|off|auto|manual|gated"

    updated = dict(current)
    if parts[1] == "on":
        updated["enabled"] = True
        updated["portfolio_construction_mode"] = "candidate"
    elif parts[1] == "off":
        updated["enabled"] = False
        updated["portfolio_construction_mode"] = "shadow"
    elif parts[1] == "auto":
        updated["enabled"] = True
        updated["portfolio_construction_mode"] = "candidate"
        updated["require_manual_approval"] = False
    elif parts[1] == "manual":
        updated["enabled"] = True
        updated["portfolio_construction_mode"] = "candidate"
        updated["require_manual_approval"] = True
    elif parts[1] == "gated":
        updated["enabled"] = True
        updated["portfolio_construction_mode"] = "gated"
        updated["require_manual_approval"] = True
        updated["allow_full_auto_gated"] = False

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
