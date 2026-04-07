# scheduler/jobs.py
import asyncio
import logging
from datetime import datetime, timedelta

from agents.planner   import run_planner
from agents.researcher import run_researcher
from agents.allocator  import run_allocator
from agents.risk_manager import run_risk_manager
from agents.executor   import run_executor
from agents.reporter   import run_reporter
from db.session        import AsyncSessionLocal
from db.queries        import get_system_config, upsert_system_config
from db.models         import AgentAnalysis, ExecutionLog
from tools.notify_tools import tool_send_telegram
from config            import get_settings

logger   = logging.getLogger("qc_fastapi_2.jobs")
settings = get_settings()


# ────────────────────────────────────────
# 核心：小时级分析流水线
# ────────────────────────────────────────
def job_hourly_analysis():
    logger.info("=== Hourly Analysis Pipeline START ===")
    try:
        _run_pipeline(trigger="scheduled_hourly")
    except Exception as e:
        logger.error(f"Hourly analysis FAILED: {e}")
        tool_send_telegram({"text": f"🚨 小时分析异常: {e}"})


def _run_pipeline(trigger: str):
    # 1. PLANNER
    plan = run_planner(trigger_type=trigger)
    logger.info(f"PLANNER done | mode={plan['mode']} | auth={plan['auth_mode']}")

    auth_mode = plan["auth_mode"]
    if auth_mode == "MANUAL":
        logger.info("MANUAL mode — pipeline skipped")
        return

    # 2. RESEARCHER
    researcher_out = run_researcher(plan)
    logger.info(
        f"RESEARCHER done | regime={researcher_out.get('market_judgment', {}).get('regime')}"
        f" | stance={researcher_out.get('recommended_stance')}"
    )

    # 3. ALLOCATOR
    allocator_out = run_allocator(plan, researcher_out)
    logger.info(
        f"ALLOCATOR done | recommended={allocator_out.get('recommended_plan')}"
    )

    # 4. RISK MGR
    risk_out = run_risk_manager(plan, allocator_out)
    approved = risk_out.get("approved", False)
    logger.info(f"RISK MGR done | approved={approved}")

    # 5. 写入 agent_analysis
    analysis_id = _save_analysis(
        trigger, plan, researcher_out, allocator_out, risk_out
    )

    # 6. SEMI_AUTO / FULL_AUTO 分支
    if not approved:
        logger.info("Risk rejected — skipping execution")
        return

    if auth_mode == "SEMI_AUTO":
        _handle_semi_auto(plan, allocator_out, risk_out, analysis_id)
    elif auth_mode == "FULL_AUTO":
        result = run_executor(plan, allocator_out, risk_out, analysis_id)
        _save_execution(analysis_id, result)
        logger.info(f"FULL_AUTO execution: {result['execution_status']}")


# ────────────────────────────────────────
# SEMI_AUTO 确认协议
# ────────────────────────────────────────
def _handle_semi_auto(
    plan:          dict,
    allocator_out: dict,
    risk_out:      dict,
    analysis_id:   int,
):
    """
    SEMI_AUTO 流程：
    1. 推送建议卡片到 Telegram
    2. 写入 pending_proposal 到 system_config
    3. 启动判断循环等待用户回复（由 Telegram Bot Handler 处理）
    4. 超时后根据市场状况判断是否自动执行
    """
    plan_key  = f"plan_{allocator_out.get('recommended_plan', 'a').lower()}"
    chosen    = allocator_out.get(plan_key, {})
    weights   = chosen.get("target_weights", {})
    actions   = chosen.get("rebalance_actions", [])
    cost      = chosen.get("estimated_cost_pct", 0)
    regime    = allocator_out.get("_researcher_regime", "N/A")  # passed through
    token     = risk_out.get("approval_token", "")
    expires_at = datetime.utcnow() + timedelta(minutes=settings.semi_auto_timeout_minutes)

    # 保存待处理建议
    asyncio.get_event_loop().run_until_complete(
        _save_pending_proposal({
            "analysis_id":  analysis_id,
            "plan":         plan_key,
            "weights":      weights,
            "token":        token,
            "expires_at":   expires_at.isoformat(),
            "status":       "pending",
        })
    )

    # 生成调仓卡片
    up_arrow = "\u25b2"
    down_arrow = "\u25bc"
    actions_str = "\n".join(
        f"  {up_arrow if a.get('action')=='buy' else down_arrow} "
        f"{a.get('ticker')} {'+' if a.get('action')=='buy' else ''}"
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
    tool_send_telegram({"text": msg})

    # 起一个后台线程等待超时后处理
    import threading
    t = threading.Timer(
        settings.semi_auto_timeout_minutes * 60,
        _timeout_handler,
        args=[analysis_id],
    )
    t.daemon = True
    t.start()


def _timeout_handler(analysis_id: int):
    """
    超时后的处理逻辑：
    - VIX > 30 或预估成本 > 0.3% → 跳过
    - 否则 → 自动执行方案 A
    """
    pending = asyncio.get_event_loop().run_until_complete(_load_pending_proposal())

    if not pending or pending.get("status") != "pending":
        return  # 已经被用户处理

    # 读取当前市场状态
    config = asyncio.get_event_loop().run_until_complete(_load_config())
    latest = asyncio.get_event_loop().run_until_complete(_load_latest_portfolio())

    vix        = float(config.get("last_vix", {}).get("value", 0) or 0)
    est_cost   = float(pending.get("estimated_cost_pct", 0))
    drawdown   = float(latest.get("current_drawdown_pct", 0)) if latest else 0

    # 保守条件：超时跳过
    if vix > 30:
        _mark_proposal_done(analysis_id, "skipped_timeout_vix")
        tool_send_telegram({"text": f"⚠️ 建议超时，VIX={vix:.1f}>30，自动跳过"})
        return

    if est_cost > 0.003:
        _mark_proposal_done(analysis_id, "skipped_timeout_cost")
        tool_send_telegram({"text": f"⚠️ 建议超时，成本{est_cost:.2%}>0.3%，自动跳过"})
        return

    # 正常市况：自动执行
    weights = pending.get("weights", {})
    token   = pending.get("token", "")

    from tools.qc_tools import tool_send_weight_command
    from tools.db_tools import tool_verify_approval_token

    verify = tool_verify_approval_token({"token": token})
    if not verify.get("valid"):
        tool_send_telegram({"text": f"⚠️ 超时自动执行失败：token {verify.get('reason')}"})
        return

    result = tool_send_weight_command({"weights": weights})
    if result.get("success"):
        _mark_proposal_done(analysis_id, "executed_timeout_auto")
        tool_send_telegram({"text": "⏱️ 超时自动执行成功"})
    else:
        tool_send_telegram({"text": f"❌ 超时自动执行失败: {result.get('error')}"})


# ────────────────────────────────────────
# Telegram Bot Handler（/confirm 、/skip 、/pause）
# ────────────────────────────────────────
def handle_telegram_command(text: str, from_chat_id: str) -> str:
    """
    由 FastAPI 的 Telegram Webhook 端点调用。
    返回回复给用户的消息。
    """
    # 安全：只接受来自配置的 chat_id
    if from_chat_id != settings.tg_chat_id:
        return ""

    cmd = text.strip().lower().split()[0]

    if cmd == "/confirm":
        return _cmd_confirm()
    elif cmd == "/skip":
        return _cmd_skip()
    elif cmd == "/pause":
        return _cmd_pause()
    elif cmd == "/status":
        return _cmd_status()
    else:
        return "未识别的指令。可用：/confirm /skip /pause /status"


def _cmd_confirm() -> str:
    pending = asyncio.get_event_loop().run_until_complete(_load_pending_proposal())
    if not pending or pending.get("status") != "pending":
        return "当前没有待确认建议。"

    weights = pending.get("weights", {})
    token   = pending.get("token", "")
    from tools.db_tools import tool_verify_approval_token
    from tools.qc_tools import tool_send_weight_command

    verify = tool_verify_approval_token({"token": token})
    if not verify.get("valid"):
        return f"❌ Token {verify.get('reason')}，请等待下一次分析。"

    result = tool_send_weight_command({"weights": weights})
    if result.get("success"):
        _mark_proposal_done(pending.get("analysis_id"), "executed_user_confirmed")
        return "✅ 已确认执行！"
    return f"❌ 执行失败：{result.get('error')}"


def _cmd_skip() -> str:
    pending = asyncio.get_event_loop().run_until_complete(_load_pending_proposal())
    if not pending or pending.get("status") != "pending":
        return "当前没有待确认建议。"
    _mark_proposal_done(pending.get("analysis_id"), "skipped_by_user")
    return "⏭️ 已跳过，本周期不操作。"


def _cmd_pause() -> str:
    asyncio.get_event_loop().run_until_complete(
        _save_system_config("authorization_mode", {"value": "MANUAL"}, "user")
    )
    return "⏸️ 已切换到 MANUAL 模式。将不再自动分析。\n/confirm resume 可恢复。"


def _cmd_status() -> str:
    config  = asyncio.get_event_loop().run_until_complete(_load_config())
    latest  = asyncio.get_event_loop().run_until_complete(_load_latest_portfolio())
    mode    = config.get("authorization_mode", {}).get("value", "SEMI_AUTO")
    circuit = config.get("circuit_state",      {}).get("value", "CLOSED")
    val     = float(latest.get("total_value", 0)) if latest else 0
    dd      = float(latest.get("current_drawdown_pct", 0)) if latest else 0
    return (
        f"📊 系统状态\n"
        f"  授权模式: {mode}\n"
        f"  熔断状态: {circuit}\n"
        f"  净值: ${val:,.0f}\n"
        f"  回撤: -{dd:.2%}"
    )


# ────────────────────────────────────────
# 其他定时任务
# ────────────────────────────────────────
def job_post_market_report():
    logger.info("=== Post Market Report START ===")
    try:
        result = run_reporter()
        logger.info(f"Reporter done | reported={result.get('reported')}")
    except Exception as e:
        logger.error(f"Reporter FAILED: {e}")
        tool_send_telegram({"text": f"🚨 日报生成异常: {e}"})


def job_morning_health_check():
    logger.info("=== Morning Health Check ===")
    config = asyncio.get_event_loop().run_until_complete(_load_config())
    mode   = config.get("authorization_mode", {}).get("value", "SEMI_AUTO")
    circuit = config.get("circuit_state",     {}).get("value", "CLOSED")
    tool_send_telegram({
        "text": (
            f"🧩 系统健康摘要 | {datetime.utcnow().strftime('%Y-%m-%d')}\n"
            f"  授权模式: {mode}\n"
            f"  熔断状态: {circuit}\n"
            f"  市场即将开盘 🚀"
        )
    })


# ────────────────────────────────────────
# 辅助异步函数
# ────────────────────────────────────────
async def _save_pending_proposal(proposal: dict):
    async with AsyncSessionLocal() as db:
        await upsert_system_config(db, "pending_proposal", proposal, "scheduler")


async def _load_pending_proposal() -> dict | None:
    async with AsyncSessionLocal() as db:
        config = await get_system_config(db, "pending_proposal")
        return config.value if config else None


async def _load_config() -> dict:
    async with AsyncSessionLocal() as db:
        risk_params = await get_system_config(db, "risk_params")
        circuit = await get_system_config(db, "circuit_state")
        auth_mode = await get_system_config(db, "authorization_mode")
        last_vix = await get_system_config(db, "last_vix")

        return {
            "risk_params": risk_params.value if risk_params else {},
            "circuit_state": circuit.value if circuit else {"value": "CLOSED"},
            "authorization_mode": auth_mode.value if auth_mode else {"value": "SEMI_AUTO"},
            "last_vix": last_vix.value if last_vix else {"value": 0},
        }


async def _load_latest_portfolio() -> dict | None:
    from db.queries import get_latest_portfolio
    async with AsyncSessionLocal() as db:
        row = await get_latest_portfolio(db)
        if not row:
            return None
        return {
            "total_value":          float(row.total_value or 0),
            "current_drawdown_pct": float(row.current_drawdown_pct or 0),
        }


async def _save_system_config(key: str, value: dict, by: str):
    async with AsyncSessionLocal() as db:
        await upsert_system_config(db, key, value, by)


def _save_analysis(
    trigger: str, plan: dict,
    researcher: dict, allocator: dict, risk: dict
) -> int:
    async def _():
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
            return row.id
    return asyncio.get_event_loop().run_until_complete(_())


def _save_execution(analysis_id: int, result: dict):
    async def _():
        async with AsyncSessionLocal() as db:
            db.add(ExecutionLog(
                analysis_id     = analysis_id,
                command_type    = "weight_adjustment",
                command_payload = result.get("weights_sent", {}),
                status          = result.get("execution_status", "unknown"),
            ))
            await db.commit()
    asyncio.get_event_loop().run_until_complete(_())


def _mark_proposal_done(analysis_id: int | None, status: str):
    async def _():
        async with AsyncSessionLocal() as db:
            config = await get_system_config(db, "pending_proposal")
            if config:
                proposal = config.value
                proposal["status"] = status
                await upsert_system_config(db, "pending_proposal", proposal, "scheduler")
            if analysis_id:
                from sqlalchemy import update
                await db.execute(
                    update(AgentAnalysis)
                    .where(AgentAnalysis.id == analysis_id)
                    .values(execution_status=status)
                )
                await db.commit()
    asyncio.get_event_loop().run_until_complete(_())
