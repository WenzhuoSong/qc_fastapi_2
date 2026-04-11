# services/pipeline.py
"""
完整 agent pipeline 的异步编排（update.txt 对齐版）。

流水线 stage（Python-LLM-Python 三段接力）：
    0. guard_and_config      (Python)   —— 锁 / 暂停检查 / 读配置 / 构 context
    1. market_brief          (Python)   —— 读快照+新闻缓存 / 算定量指标 / 拼散文
    2. quant_baseline        (Python)   —— 纯数学打分 → base_weights
    3. RESEARCHER            (LLM)      —— base_weights + brief → adjusted_weights + reasoning
    4. RISK MGR              (Python)   —— overlays + 6 项检查 → final target_weights + token
    5. _save_analysis        (Python)   —— 写 agent_analysis 表
    6. COMMUNICATOR          (LLM+fb)   —— Telegram 文案（可降级）
    7. 分支: rejected / SEMI_AUTO pending / FULL_AUTO 直接执行

核心数据流（接力棒传的是 weights）：
    base_weights       (Stage 2 Python) →
    adjusted_weights   (Stage 3 LLM)    →
    target_weights     (Stage 4 Python) →
    execute            (Stage 8 Python)

新闻数据由独立的 cron/pre_fetch_news.py 每 2h 单独刷新，主 pipeline
只从 DB 读缓存。两条 cron 独立失败：新闻挂 → pipeline 用旧缓存继续跑；
pipeline 挂 → 新闻照常刷新。
"""
import logging
from datetime import datetime, timedelta

from agents.researcher    import run_researcher_async
from agents.risk_manager  import run_risk_manager_async
from agents.communicator  import run_communicator_async
from agents.executor      import run_executor_async
from services.market_brief    import build_market_brief
from services.quant_baseline  import run_quant_baseline_async
from db.session          import AsyncSessionLocal
from db.queries          import get_system_config, upsert_system_config
from db.models           import AgentAnalysis, ExecutionLog
from tools.notify_tools  import tool_send_telegram
from services.proposal   import save_pending_proposal
from config              import get_settings

logger   = logging.getLogger("qc_fastapi_2.pipeline")
settings = get_settings()

# --------------- Pipeline TTL Lock ---------------
PIPELINE_LOCK_KEY    = "pipeline_lock"
PIPELINE_TTL_MINUTES = 55  # 略小于 1 小时 cron 间隔


async def _acquire_pipeline_lock() -> bool:
    async with AsyncSessionLocal() as db:
        lock_cfg = await get_system_config(db, PIPELINE_LOCK_KEY)
        if lock_cfg:
            lock = lock_cfg.value
            expires_at = lock.get("expires_at", "1970-01-01T00:00:00")
            if datetime.utcnow() < datetime.fromisoformat(expires_at):
                return False
        expires = (datetime.utcnow() + timedelta(minutes=PIPELINE_TTL_MINUTES)).isoformat()
        await upsert_system_config(db, PIPELINE_LOCK_KEY, {
            "locked":     True,
            "started_at": datetime.utcnow().isoformat(),
            "expires_at": expires,
        }, "pipeline")
    return True


async def _release_pipeline_lock() -> None:
    async with AsyncSessionLocal() as db:
        await upsert_system_config(db, PIPELINE_LOCK_KEY, {
            "locked":      False,
            "released_at": datetime.utcnow().isoformat(),
            "expires_at":  "1970-01-01T00:00:00",
        }, "pipeline")


# ─────────────────────────────── Stage 0: guard_and_config ───────────────────────────────


async def _guard_and_config(trigger: str) -> dict | None:
    """
    读系统配置，构建 pipeline_context。
    返回 None 表示需要 skip（暂停 / MANUAL 模式）。
    """
    async with AsyncSessionLocal() as db:
        paused_cfg    = await get_system_config(db, "trading_paused")
        risk_cfg      = await get_system_config(db, "risk_params")
        auth_cfg      = await get_system_config(db, "authorization_mode")
        circuit_cfg   = await get_system_config(db, "circuit_state")
        active_cfg    = await get_system_config(db, "active_strategy")

    paused = bool((paused_cfg.value if paused_cfg else {}).get("paused", False))
    if paused:
        logger.info("trading_paused=True — pipeline skipped")
        return None

    auth_mode = (auth_cfg.value if auth_cfg else {"value": "SEMI_AUTO"}).get("value", "SEMI_AUTO")
    if auth_mode == "MANUAL":
        logger.info("MANUAL mode — pipeline skipped")
        return None

    risk_params = (risk_cfg.value if risk_cfg else {}) or {}
    circuit     = (circuit_cfg.value if circuit_cfg else {"value": "CLOSED"}).get("value", "CLOSED")
    active_name = (active_cfg.value if active_cfg else {"value": "momentum_lite_v1"}).get(
        "value", "momentum_lite_v1"
    )

    params_key = f"strategy_{active_name}_params"
    async with AsyncSessionLocal() as db:
        params_cfg = await get_system_config(db, params_key)
    strategy_params = (params_cfg.value if params_cfg else {}) or {}

    override_mode = "DEFENSIVE" if circuit in ("ALERT", "DEFENSIVE") else None

    return {
        "trigger":         trigger,
        "plan_id":         f"P-{datetime.utcnow().strftime('%Y%m%d-%H%M')}",
        "auth_mode":       auth_mode,
        "circuit_state":   circuit,
        "override_mode":   override_mode,
        "risk_params":     risk_params,
        "active_strategy": active_name,
        "strategy_params": strategy_params,
    }


# ─────────────────────────────── 主入口 ───────────────────────────────


async def run_full_pipeline(trigger: str = "scheduled_hourly") -> dict:
    """运行完整 agent pipeline。"""
    logger.info(f"=== Pipeline START | trigger={trigger} ===")

    if not await _acquire_pipeline_lock():
        logger.warning("Pipeline lock held by another instance — skipped")
        return {"status": "skipped_concurrent"}

    try:
        return await _run_pipeline_inner(trigger)
    finally:
        await _release_pipeline_lock()


async def _run_pipeline_inner(trigger: str) -> dict:
    # Stage 0: guard + config
    pipeline_context = await _guard_and_config(trigger)
    if pipeline_context is None:
        return {"status": "skipped_gated"}

    logger.info(
        f"Stage 0 done | auth={pipeline_context['auth_mode']} "
        f"| override={pipeline_context['override_mode']} "
        f"| strategy={pipeline_context['active_strategy']}"
    )

    # Stage 1: market_brief (Python)
    brief = await build_market_brief(pipeline_context)
    if not brief.get("holdings"):
        logger.warning("Stage 1 market_brief: no holdings in latest snapshot — skipping pipeline")
        return {"status": "skipped_no_snapshot"}
    logger.info(
        f"Stage 1 market_brief done | "
        f"n_holdings={len(brief.get('holdings', []))} "
        f"| hard_risks={len(brief.get('hard_risks_map', {}))}"
    )

    # Stage 2: quant_baseline (Python)
    quant_baseline = await run_quant_baseline_async(pipeline_context, brief)
    logger.info(
        f"Stage 2 quant_baseline done | "
        f"n_selected={len(quant_baseline.get('selected_tickers', []))} "
        f"| top5={quant_baseline.get('ranking_summary', {}).get('top_5', [])}"
    )

    # Stage 3: RESEARCHER (LLM)
    researcher_out = await run_researcher_async(pipeline_context, brief, quant_baseline)
    logger.info(
        f"Stage 3 RESEARCHER done | "
        f"regime={researcher_out.get('market_judgment', {}).get('regime')} "
        f"| stance={researcher_out.get('recommended_stance')} "
        f"| n_adjustments={len(researcher_out.get('weight_adjustments', []))} "
        f"| key_events={len(researcher_out.get('key_events', []))} "
        f"| degraded={researcher_out.get('used_degraded_fallback', False)}"
    )

    # Stage 4: RISK MGR (Python) —— overlays + 6 checks
    risk_out = await run_risk_manager_async(
        pipeline_context, brief, quant_baseline, researcher_out
    )
    approved = bool(risk_out.get("approved", False))
    logger.info(
        f"Stage 4 RISK MGR done | approved={approved} "
        f"| n_actions={len(risk_out.get('rebalance_actions', []))} "
        f"| cost={risk_out.get('estimated_cost_pct', 0):.4%} "
        f"| overlays={risk_out.get('overlays_applied', [])}"
    )

    # Stage 5: 写 analysis
    analysis_id = await _save_analysis(
        trigger, pipeline_context, quant_baseline, researcher_out, risk_out
    )

    # Stage 6: COMMUNICATOR —— LLM + fallback
    comm_out = await run_communicator_async(
        pipeline_context, researcher_out, risk_out
    )
    logger.info(
        f"Stage 6 COMMUNICATOR done | used_fallback={comm_out.get('used_fallback', False)}"
    )

    # Stage 7: 分支执行
    auth_mode = pipeline_context["auth_mode"]

    if not approved:
        await tool_send_telegram({"text": comm_out["text"]})
        logger.info("Risk rejected — notified and stopping")
        return {"status": "rejected_by_risk", "analysis_id": analysis_id}

    if auth_mode == "SEMI_AUTO":
        await _send_semi_auto_proposal(
            pipeline_context, risk_out, comm_out, analysis_id
        )
        return {"status": "semi_auto_pending", "analysis_id": analysis_id}

    if auth_mode == "FULL_AUTO":
        result = await run_executor_async(
            pipeline_context, risk_out, analysis_id
        )
        await _save_execution(analysis_id, result)
        logger.info(f"FULL_AUTO execution: {result.get('execution_status')}")
        return {"status": result.get("execution_status", "unknown"), "analysis_id": analysis_id}

    return {"status": "unknown_auth_mode", "analysis_id": analysis_id}


# ─────────────────────────────── SEMI_AUTO 提案 ───────────────────────────────


async def _send_semi_auto_proposal(
    pipeline_context: dict,
    risk_out:         dict,
    comm_out:         dict,
    analysis_id:      int,
) -> None:
    weights = risk_out.get("target_weights", {}) or {}
    cost    = float(risk_out.get("estimated_cost_pct", 0) or 0)
    token   = risk_out.get("approval_token", "")
    expires = datetime.utcnow() + timedelta(minutes=settings.semi_auto_timeout_minutes)

    await save_pending_proposal({
        "analysis_id":        analysis_id,
        "weights":            weights,
        "token":              token,
        "expires_at":         expires.isoformat(),
        "status":             "pending",
        "estimated_cost_pct": cost,
    })

    await tool_send_telegram({"text": comm_out["text"]})


# ─────────────────────────────── 存档 ───────────────────────────────


async def _save_analysis(
    trigger:          str,
    pipeline_context: dict,
    quant_baseline:   dict,
    researcher_out:   dict,
    risk_out:         dict,
) -> int:
    """
    AgentAnalysis 表列名沿用旧命名（方案 A）：
      planner_output   ← pipeline_context   （Stage 0）
      allocator_output ← quant_baseline     （Stage 2 纯数学基准）
      researcher_output← researcher_out     （Stage 3 LLM 调整草案）
      risk_output      ← risk_out           （Stage 4 最终执行方案）
    """
    async with AsyncSessionLocal() as db:
        row = AgentAnalysis(
            analyzed_at       = datetime.utcnow(),
            trigger_type      = trigger,
            planner_output    = pipeline_context,
            researcher_output = researcher_out,
            allocator_output  = quant_baseline,
            risk_output       = risk_out,
            risk_approved     = bool(risk_out.get("approved", False)),
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
