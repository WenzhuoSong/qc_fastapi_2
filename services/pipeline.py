# services/pipeline.py
"""
完整 agent pipeline 的异步编排。
由 cron 脚本调用（每个 cron 进程自带 asyncio.run）。
"""
import logging
from datetime import datetime, timedelta

from agents.planner      import run_planner_async
from agents.researcher   import run_researcher_async
from agents.allocator    import run_allocator_async
from agents.risk_manager import run_risk_manager_async
from agents.executor     import run_executor_async
from db.session          import AsyncSessionLocal
from db.queries          import get_system_config, upsert_system_config
from db.models           import AgentAnalysis, ExecutionLog
from tools.notify_tools  import tool_send_telegram
from tools.db_tools      import tool_write_approval_token
from services.proposal   import save_pending_proposal
from config              import get_settings

logger   = logging.getLogger("qc_fastapi_2.pipeline")
settings = get_settings()

# --------------- Pipeline TTL Lock ---------------
PIPELINE_LOCK_KEY = "pipeline_lock"
PIPELINE_TTL_MINUTES = 55  # 略小于 1 小时 cron 间隔


async def _acquire_pipeline_lock() -> bool:
    """尝试获取 pipeline 锁。返回 True 表示获取成功。"""
    async with AsyncSessionLocal() as db:
        lock_cfg = await get_system_config(db, PIPELINE_LOCK_KEY)
        if lock_cfg:
            lock = lock_cfg.value
            expires_at = lock.get("expires_at", "1970-01-01T00:00:00")
            if datetime.utcnow() < datetime.fromisoformat(expires_at):
                return False  # 锁未过期，上一个 pipeline 仍在运行
        expires = (datetime.utcnow() + timedelta(minutes=PIPELINE_TTL_MINUTES)).isoformat()
        await upsert_system_config(db, PIPELINE_LOCK_KEY, {
            "locked": True,
            "started_at": datetime.utcnow().isoformat(),
            "expires_at": expires,
        }, "pipeline")
    return True


async def _release_pipeline_lock() -> None:
    """释放 pipeline 锁。"""
    async with AsyncSessionLocal() as db:
        await upsert_system_config(db, PIPELINE_LOCK_KEY, {
            "locked": False,
            "released_at": datetime.utcnow().isoformat(),
            "expires_at": "1970-01-01T00:00:00",
        }, "pipeline")


async def run_full_pipeline(trigger: str = "scheduled_hourly") -> dict:
    """运行完整 6-agent pipeline。"""
    logger.info(f"=== Pipeline START | trigger={trigger} ===")

    # TTL 锁：防止并发 pipeline
    if not await _acquire_pipeline_lock():
        logger.warning("Pipeline lock held by another instance — skipped")
        return {"status": "skipped_concurrent"}

    try:
        return await _run_pipeline_inner(trigger)
    finally:
        await _release_pipeline_lock()


async def _run_pipeline_inner(trigger: str) -> dict:
    # 检查 trading_paused
    async with AsyncSessionLocal() as db:
        cfg = await get_system_config(db, "trading_paused")
        paused = cfg.value.get("paused", False) if cfg else False
    if paused:
        logger.info("trading_paused=True — pipeline skipped")
        return {"status": "skipped_paused"}

    # 1. PLANNER
    plan = await run_planner_async(trigger_type=trigger)
    logger.info(f"PLANNER done | mode={plan['mode']} | auth={plan['auth_mode']}")

    auth_mode = plan["auth_mode"]
    if auth_mode == "MANUAL":
        logger.info("MANUAL mode — pipeline skipped")
        return {"status": "skipped_manual"}

    # 2. RESEARCHER
    researcher_out = await run_researcher_async(plan)
    logger.info(
        f"RESEARCHER done | regime={researcher_out.get('market_judgment', {}).get('regime')}"
        f" | stance={researcher_out.get('recommended_stance')}"
    )

    # 3. ALLOCATOR
    allocator_out = await run_allocator_async(plan, researcher_out)
    logger.info(f"ALLOCATOR done | recommended={allocator_out.get('recommended_plan')}")

    # 4. RISK MGR
    risk_out = await run_risk_manager_async(plan, allocator_out)
    approved = risk_out.get("approved", False)
    logger.info(f"RISK MGR done | approved={approved}")

    # 5. 确定性生成 approval_token（不依赖 LLM 调用工具）
    if approved:
        token_result = await tool_write_approval_token({})
        risk_out["approval_token"] = token_result["approval_token"]
        logger.info(f"Approval token generated deterministically")

    # 6. 保存 analysis
    analysis_id = await _save_analysis(trigger, plan, researcher_out, allocator_out, risk_out)

    # 7. 分支执行
    if not approved:
        logger.info("Risk rejected — skipping execution")
        return {"status": "rejected_by_risk", "analysis_id": analysis_id}

    if auth_mode == "SEMI_AUTO":
        await _send_semi_auto_proposal(
            plan, researcher_out, allocator_out, risk_out, analysis_id
        )
        return {"status": "semi_auto_pending", "analysis_id": analysis_id}

    if auth_mode == "FULL_AUTO":
        result = await run_executor_async(plan, allocator_out, risk_out, analysis_id)
        await _save_execution(analysis_id, result)
        logger.info(f"FULL_AUTO execution: {result['execution_status']}")
        return {"status": result["execution_status"], "analysis_id": analysis_id}

    return {"status": "unknown_auth_mode", "analysis_id": analysis_id}


async def _send_semi_auto_proposal(
    plan:           dict,
    researcher_out: dict,
    allocator_out:  dict,
    risk_out:       dict,
    analysis_id:    int,
) -> None:
    plan_key   = f"plan_{allocator_out.get('recommended_plan', 'a').lower()}"
    chosen     = allocator_out.get(plan_key, {})
    weights    = chosen.get("target_weights", {})
    actions    = chosen.get("rebalance_actions", [])
    cost       = chosen.get("estimated_cost_pct", 0)
    regime     = researcher_out.get("market_judgment", {}).get("regime", "N/A")
    token      = risk_out.get("approval_token", "")
    expires_at = datetime.utcnow() + timedelta(minutes=settings.semi_auto_timeout_minutes)

    await save_pending_proposal({
        "analysis_id":        analysis_id,
        "plan":               plan_key,
        "weights":            weights,
        "token":              token,
        "expires_at":         expires_at.isoformat(),
        "status":             "pending",
        "estimated_cost_pct": cost,
    })

    up, down = "\u25b2", "\u25bc"
    actions_str = "\n".join(
        f"  {up if a.get('action') == 'buy' else down} "
        f"{a.get('ticker')} {'+' if a.get('action') == 'buy' else ''}"
        f"{a.get('weight_delta', 0):.1%}"
        for a in actions
    ) or "  无调仓操作"

    msg = (
        f"📋 <b>调仓建议</b> #{plan.get('plan_id', '')}\n"
        f"――――――――――――――――\n"
        f"🌡️ 市场制度：{regime}\n\n"
        f"🎯 建议操作（方案 {allocator_out.get('recommended_plan', 'A')}）\n"
        f"{actions_str}\n\n"
        f"预估成本：{cost:.2%}\n"
        f"🛡️ 风控：✅ APPROVED\n\n"
        f"⏱️ {settings.semi_auto_timeout_minutes} 分钟后无回复 → 市场正常时自动执行\n"
        f"\n<b>/confirm</b>  <b>/skip</b>  <b>/pause</b>"
    )
    await tool_send_telegram({"text": msg})


async def _save_analysis(
    trigger:    str,
    plan:       dict,
    researcher: dict,
    allocator:  dict,
    risk:       dict,
) -> int:
    async with AsyncSessionLocal() as db:
        row = AgentAnalysis(
            analyzed_at       = datetime.utcnow(),
            trigger_type      = trigger,
            planner_output    = plan,
            researcher_output = researcher,
            allocator_output  = allocator,
            risk_output       = risk,
            risk_approved     = risk.get("approved", False),
            execution_status  = "pending",
        )
        db.add(row)
        await db.commit()
        await db.refresh(row)
        return row.id


async def _save_execution(analysis_id: int, result: dict) -> None:
    async with AsyncSessionLocal() as db:
        db.add(ExecutionLog(
            analysis_id     = analysis_id,
            command_type    = "weight_adjustment",
            command_payload = result.get("weights_sent", {}),
            status          = result.get("execution_status", "unknown"),
        ))
        await db.commit()
