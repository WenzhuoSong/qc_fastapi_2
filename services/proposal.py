# services/proposal.py
"""
SEMI_AUTO 待确认建议的持久化 + 超时处理。
在 cron 架构下，超时处理由独立的 pending_check cron 定期轮询。

P2-1: Proposal Invalidation — 在执行前检查 proposal 是否仍然有效。
"""
import logging
from datetime import datetime

from sqlalchemy import update

from db.session import AsyncSessionLocal
from db.queries import (
    get_system_config,
    upsert_system_config,
    get_latest_portfolio,
)
from db.models import AgentAnalysis
from tools.notify_tools import tool_send_telegram
from config import get_settings

logger = logging.getLogger("qc_fastapi_2.proposal")
settings = get_settings()


async def save_pending_proposal(proposal: dict) -> None:
    async with AsyncSessionLocal() as db:
        await upsert_system_config(db, "pending_proposal", proposal, "pipeline")


async def load_pending_proposal() -> dict | None:
    async with AsyncSessionLocal() as db:
        config = await get_system_config(db, "pending_proposal")
        return config.value if config else None


async def mark_proposal_done(analysis_id: int | None, status: str) -> None:
    async with AsyncSessionLocal() as db:
        config = await get_system_config(db, "pending_proposal")
        if config:
            proposal = dict(config.value)
            proposal["status"] = status
            await upsert_system_config(db, "pending_proposal", proposal, "pipeline")
        if analysis_id:
            await db.execute(
                update(AgentAnalysis)
                .where(AgentAnalysis.id == analysis_id)
                .values(execution_status=status)
            )
            await db.commit()


async def check_and_handle_timeout() -> dict:
    """
    由 pending_check cron 每分钟调用。
    若 pending_proposal 已过期，自动跳过并告警。

    Timed-out proposals must not auto-execute from this side path. Any command
    sent to QC must pass through the hardened execution path with lifecycle,
    active-command, preflight, dedupe, and fingerprint controls.
    """
    pending = await load_pending_proposal()
    if not pending or pending.get("status") != "pending":
        return {"action": "none"}

    expires_at_str = pending.get("expires_at")
    if not expires_at_str:
        return {"action": "none"}

    if datetime.utcnow().isoformat() < expires_at_str:
        return {"action": "still_pending"}

    analysis_id = pending.get("analysis_id")

    async with AsyncSessionLocal() as db:
        risk_params_cfg = await get_system_config(db, "risk_params")
        last_vix_cfg    = await get_system_config(db, "last_vix")
        latest_row      = await get_latest_portfolio(db)

    risk_params = risk_params_cfg.value if risk_params_cfg else {}
    vix          = float((last_vix_cfg.value if last_vix_cfg else {}).get("value", 0) or 0)
    est_cost     = float(pending.get("estimated_cost_pct", 0))
    max_cost_pct = float(risk_params.get("max_trade_cost_pct", 0.005))

    if vix > 30:
        await mark_proposal_done(analysis_id, "skipped_timeout_vix")
        await tool_send_telegram({"text": f"⚠️ Proposal timed out; VIX={vix:.1f}>30, auto-skipped"})
        return {"action": "skipped_vix"}

    if est_cost > max_cost_pct:
        await mark_proposal_done(analysis_id, "skipped_timeout_cost")
        await tool_send_telegram(
            {"text": f"⚠️ Proposal timed out; estimated cost {est_cost:.2%}>{max_cost_pct:.2%}, auto-skipped"}
        )
        return {"action": "skipped_cost"}

    # P2-1: Proposal validation before execution
    valid, reason = await validate_proposal_still_relevant(pending, latest_row)
    if not valid:
        await mark_proposal_done(analysis_id, f"skipped_invalidation_{reason}")
        await tool_send_telegram(
            {"text": f"⚠️ Proposal invalidated ({reason}); original plan will not execute"}
        )
        return {"action": f"skipped_invalidation_{reason}"}

    await mark_proposal_done(analysis_id, "skipped_timeout_auto_exec_disabled")
    await tool_send_telegram(
        {
            "text": (
                "⏱️ Proposal timed out; auto-execution is disabled. "
                "No command sent to QC. Use /confirm before expiry or wait for the next analysis."
            )
        }
    )
    return {"action": "skipped_auto_exec_disabled"}


async def validate_proposal_still_relevant(
    pending: dict,
    latest_portfolio,
) -> tuple[bool, str]:
    """
    P2-1: 检查 proposal 是否仍然有效。

    检查项：
    1. VIX 是否突破阈值（> proposal_invalidation_vix_threshold 立即作废）
    2. 组合价值是否已大幅变动（相对 proposal 产出时变化 > drift_threshold）
    3. 是否超过日内最大可接受回撤

    返回 (valid, reason) — valid=True 表示可以执行，reason 说明原因。
    """
    # 1. VIX check
    vix_threshold = settings.proposal_invalidation_vix_threshold
    async with AsyncSessionLocal() as db:
        last_vix_cfg = await get_system_config(db, "last_vix")
    vix = float((last_vix_cfg.value if last_vix_cfg else {}).get("value", 0) or 0)
    if vix > vix_threshold:
        return False, f"vix_spike_{vix:.1f}"

    # 2. Portfolio drift check
    if latest_portfolio and pending.get("proposal_value"):
        try:
            current_value = float(latest_portfolio.total_value or 0)
            proposal_value = float(pending["proposal_value"])
            if proposal_value > 0:
                drift = abs(current_value - proposal_value) / proposal_value
                drift_threshold = settings.proposal_invalidation_portfolio_drift_threshold
                if drift > drift_threshold:
                    return False, f"portfolio_drift_{drift:.2%}"
        except (TypeError, ValueError):
            pass

    # 3. Circuit state check (Phase 3: also blocks DEFENSIVE)
    async with AsyncSessionLocal() as db:
        circuit_cfg = await get_system_config(db, "circuit_state")
    circuit = (circuit_cfg.value if circuit_cfg else {}).get("value", "CLOSED")
    if circuit in ("ALERT", "DEFENSIVE"):
        return False, f"circuit_{circuit.lower()}"

    return True, "valid"
