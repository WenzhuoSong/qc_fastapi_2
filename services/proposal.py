# services/proposal.py
"""
SEMI_AUTO 待确认建议的持久化 + 超时处理。
在 cron 架构下，超时处理由独立的 pending_check cron 定期轮询。
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
from tools.db_tools import tool_verify_approval_token
from tools.qc_tools import tool_send_weight_command
from tools.notify_tools import tool_send_telegram

logger = logging.getLogger("qc_fastapi_2.proposal")


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
    若 pending_proposal 已过期，按策略自动执行或跳过。
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
        await tool_send_telegram({"text": f"⚠️ 建议超时，VIX={vix:.1f}>30，自动跳过"})
        return {"action": "skipped_vix"}

    if est_cost > max_cost_pct:
        await mark_proposal_done(analysis_id, "skipped_timeout_cost")
        await tool_send_telegram(
            {"text": f"⚠️ 建议超时，成本{est_cost:.2%}>{max_cost_pct:.2%}，自动跳过"}
        )
        return {"action": "skipped_cost"}

    weights = pending.get("weights", {})
    token   = pending.get("token", "")

    verify = await tool_verify_approval_token({"token": token})
    if not verify.get("valid"):
        await mark_proposal_done(analysis_id, f"aborted_token_{verify.get('reason')}")
        await tool_send_telegram(
            {"text": f"⚠️ 超时自动执行失败：token {verify.get('reason')}"}
        )
        return {"action": "token_invalid"}

    result = await tool_send_weight_command({"weights": weights})
    if result.get("success"):
        await mark_proposal_done(analysis_id, "executed_timeout_auto")
        await tool_send_telegram({"text": "⏱️ 超时自动执行成功"})
        return {"action": "executed"}

    await mark_proposal_done(analysis_id, "failed_timeout_auto")
    await tool_send_telegram(
        {"text": f"❌ 超时自动执行失败: {result.get('error')}"}
    )
    return {"action": "failed"}
