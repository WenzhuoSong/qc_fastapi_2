# services/pipeline.py
"""
完整 agent pipeline 的异步编排（V2.1 Bull/Bear 辩论版）。

流水线 stage（10-stage Python-LLM-Python 三段接力）：
    0. guard_and_config      (Python)   —— 锁 / 暂停检查 / 读配置 / 构 context
    1. market_brief          (Python)   —— 读快照+新闻缓存 / 算定量指标 / 拼散文
    2. quant_baseline        (Python)   —— 纯数学打分 → base_weights
    3. RESEARCHER            (LLM)      —— base_weights + brief → research_report（只分析不决策）
   4a. BULL RESEARCHER       (LLM)      —— draft thesis（无权重）
   4b. BEAR RESEARCHER       (LLM)      —— draft thesis（无权重，与 4a 并行）
   4c. CROSS_EXAM           (LLM)      —— 交换论点，短反驳（与对侧并行）
    5. PM / SYNTHESIZER      (LLM)      —— 唯一 adjusted_weights + decision_rationale
    6. RISK MGR              (Python)   —— overlays + 6 项检查 → final target_weights + token
    7. _save_analysis        (Python)   —— 写 agent_analysis 表
    8. COMMUNICATOR          (LLM+fb)   —— Telegram 文案（可降级）
    9. 分支: rejected / SEMI_AUTO pending / FULL_AUTO 直接执行

核心数据流（接力棒传的是 weights）：
    base_weights       (Stage 2 Python)       →
    research_report    (Stage 3 LLM 信息合成)  →
    bull/bear_output   (Stage 4a/4b LLM 辩论) →
    adjusted_weights   (Stage 5 LLM 仲裁)     →
    target_weights     (Stage 6 Python)       →
    execute            (Stage 9 Python)

新闻数据由独立的 cron/pre_fetch_news.py 每 2h 单独刷新，主 pipeline
只从 DB 读缓存。两条 cron 独立失败：新闻挂 → pipeline 用旧缓存继续跑；
pipeline 挂 → 新闻照常刷新。
"""
import asyncio
import logging
import time
from datetime import datetime, timedelta

from agents.researcher       import run_researcher_async
from agents.bull_researcher  import run_bull_researcher_async
from agents.bear_researcher  import run_bear_researcher_async
from agents.cross_exam       import run_bull_cross_exam_async, run_bear_cross_exam_async
from agents.synthesizer      import run_synthesizer_async
from agents.risk_manager     import run_risk_manager_async
from agents.communicator     import run_communicator_async
from agents.executor         import run_executor_async
from services.market_brief    import build_market_brief
from services.quant_baseline  import run_quant_baseline_async
from db.session          import AsyncSessionLocal
from db.queries          import get_system_config, upsert_system_config
from db.models           import AgentAnalysis, AgentStepLog, ExecutionLog
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


# ─────────────────────────────── Step Log Helper ───────────────────────────────


async def _save_step_log(
    analysis_id: int,
    stage: str,
    agent_name: str,
    input_data: dict | None,
    output_data: dict | None,
    duration_ms: int = 0,
    model: str | None = None,
    failed: bool = False,
) -> None:
    """写一条 agent_step_log 记录。静默失败，不影响 pipeline。"""
    try:
        async with AsyncSessionLocal() as db:
            db.add(AgentStepLog(
                analysis_id = analysis_id,
                stage       = stage,
                agent_name  = agent_name,
                input_data  = input_data,
                output_data = output_data,
                duration_ms = duration_ms,
                model       = model,
                failed      = failed,
            ))
            await db.commit()
    except Exception as e:
        logger.warning(f"Failed to save step log for {stage}: {e}")


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
    model_heavy = settings.openai_model_heavy

    # Stage 0: guard + config
    pipeline_context = await _guard_and_config(trigger)
    if pipeline_context is None:
        return {"status": "skipped_gated"}

    logger.info(
        f"Stage 0 done | auth={pipeline_context['auth_mode']} "
        f"| override={pipeline_context['override_mode']} "
        f"| strategy={pipeline_context['active_strategy']}"
    )

    # 提前创建 analysis 行，拿到 analysis_id 供 step log 使用
    analysis_id = await _create_analysis_placeholder(trigger, pipeline_context)

    # Stage 1: market_brief (Python)
    t0 = time.time()
    brief = await build_market_brief(pipeline_context)
    dur_brief = int((time.time() - t0) * 1000)
    if not brief.get("holdings"):
        logger.warning("Stage 1 market_brief: no holdings in latest snapshot — skipping pipeline")
        return {"status": "skipped_no_snapshot"}
    logger.info(
        f"Stage 1 market_brief done | "
        f"n_holdings={len(brief.get('holdings', []))} "
        f"| hard_risks={len(brief.get('hard_risks_map', {}))}"
    )
    await _save_step_log(
        analysis_id, "1_brief", "market_brief",
        input_data={"n_holdings": len(brief.get("holdings", []))},
        output_data=brief,
        duration_ms=dur_brief,
    )

    # Stage 2: quant_baseline (Python)
    t0 = time.time()
    quant_baseline = await run_quant_baseline_async(pipeline_context, brief)
    dur_quant = int((time.time() - t0) * 1000)
    logger.info(
        f"Stage 2 quant_baseline done | "
        f"n_selected={len(quant_baseline.get('selected_tickers', []))} "
        f"| top5={quant_baseline.get('ranking_summary', {}).get('top_5', [])}"
    )
    await _save_step_log(
        analysis_id, "2_quant", "quant_baseline",
        input_data={"strategy": pipeline_context.get("active_strategy")},
        output_data=quant_baseline,
        duration_ms=dur_quant,
    )

    # Stage 3: RESEARCHER (LLM) —— 信息合成（只分析不决策）
    t0 = time.time()
    research_report = await run_researcher_async(pipeline_context, brief, quant_baseline)
    dur_researcher = int((time.time() - t0) * 1000)
    logger.info(
        f"Stage 3 RESEARCHER done | "
        f"regime={research_report.get('market_regime', {}).get('regime')} "
        f"| impact_bias={research_report.get('macro_outlook', {}).get('impact_bias')} "
        f"| n_ticker_signals={len(research_report.get('ticker_signals', []))} "
        f"| degraded={research_report.get('used_degraded_fallback', False)}"
    )
    await _save_step_log(
        analysis_id, "3_researcher", "researcher",
        input_data={"base_weights": quant_baseline.get("base_weights")},
        output_data=research_report,
        duration_ms=dur_researcher,
        model=model_heavy,
        failed=research_report.get("used_degraded_fallback", False),
    )

    # Stage 4a/4b: BULL + BEAR drafts (parallel, no weights)
    base_weights = quant_baseline.get("base_weights", {})
    t0 = time.time()
    bull_draft, bear_draft = await asyncio.gather(
        run_bull_researcher_async(research_report, base_weights),
        run_bear_researcher_async(research_report, base_weights),
    )
    dur_draft = int((time.time() - t0) * 1000)
    logger.info(
        f"Stage 4a BULL draft | stance={bull_draft.get('stance')} "
        f"| confidence={bull_draft.get('confidence')} "
        f"| failed={bull_draft.get('failed', False)}"
    )
    logger.info(
        f"Stage 4b BEAR draft | stance={bear_draft.get('stance')} "
        f"| confidence={bear_draft.get('confidence')} "
        f"| failed={bear_draft.get('failed', False)}"
    )
    await _save_step_log(
        analysis_id, "4a_bull", "bull_researcher",
        input_data={"base_weights": base_weights},
        output_data=bull_draft,
        duration_ms=dur_draft,
        model=model_heavy,
        failed=bull_draft.get("failed", False),
    )
    await _save_step_log(
        analysis_id, "4b_bear", "bear_researcher",
        input_data={"base_weights": base_weights},
        output_data=bear_draft,
        duration_ms=dur_draft,
        model=model_heavy,
        failed=bear_draft.get("failed", False),
    )

    # Stage 4c: cross-examination (Bull sees Bear draft, Bear sees Bull draft; parallel)
    t_ce = time.time()
    rebuttal_vs_bear, rebuttal_vs_bull = await asyncio.gather(
        run_bull_cross_exam_async(bear_draft, research_report),
        run_bear_cross_exam_async(bull_draft, research_report),
    )
    dur_ce = int((time.time() - t_ce) * 1000)
    bull_output = {**bull_draft, "rebuttal_vs_bear": rebuttal_vs_bear}
    bear_output = {**bear_draft, "rebuttal_vs_bull": rebuttal_vs_bull}
    logger.info(
        f"Stage 4c CROSS_EXAM | bull_vs_bear_failed={rebuttal_vs_bear.get('failed')} "
        f"| bear_vs_bull_failed={rebuttal_vs_bull.get('failed')}"
    )
    await _save_step_log(
        analysis_id, "4c_cross_exam", "cross_exam",
        input_data={
            "bull_draft_failed": bull_draft.get("failed", False),
            "bear_draft_failed": bear_draft.get("failed", False),
        },
        output_data={
            "rebuttal_vs_bear": rebuttal_vs_bear,
            "rebuttal_vs_bull": rebuttal_vs_bull,
        },
        duration_ms=dur_ce,
        model=model_heavy,
        failed=bool(rebuttal_vs_bear.get("failed") and rebuttal_vs_bull.get("failed")),
    )

    # Stage 5: PM / SYNTHESIZER (LLM) —— final adjusted_weights + decision_rationale
    risk_params = pipeline_context.get("risk_params", {})
    t0 = time.time()
    synthesizer_out = await run_synthesizer_async(
        research_report, bull_output, bear_output,
        base_weights, brief, risk_params,
    )
    dur_synth = int((time.time() - t0) * 1000)
    logger.info(
        f"Stage 5 PM done | "
        f"regime={synthesizer_out.get('market_judgment', {}).get('regime')} "
        f"| stance={synthesizer_out.get('recommended_stance')} "
        f"| n_adjustments={len(synthesizer_out.get('weight_adjustments', []))} "
        f"| key_events={len(synthesizer_out.get('key_events', []))} "
        f"| degraded={synthesizer_out.get('used_degraded_fallback', False)}"
    )
    await _save_step_log(
        analysis_id, "5_synthesizer", "synthesizer",
        input_data={
            "bull_stance": bull_output.get("stance"),
            "bull_confidence": bull_output.get("confidence"),
            "bear_stance": bear_output.get("stance"),
            "bear_confidence": bear_output.get("confidence"),
        },
        output_data=synthesizer_out,
        duration_ms=dur_synth,
        model=model_heavy,
        failed=synthesizer_out.get("used_degraded_fallback", False),
    )

    # Stage 6: RISK MGR (Python) —— overlays + 6 checks
    # synthesizer_out 接口兼容旧 researcher_out，Risk MGR 无需改动
    t0 = time.time()
    risk_out = await run_risk_manager_async(
        pipeline_context, brief, quant_baseline, synthesizer_out
    )
    dur_risk = int((time.time() - t0) * 1000)
    approved = bool(risk_out.get("approved", False))
    logger.info(
        f"Stage 6 RISK MGR done | approved={approved} "
        f"| n_actions={len(risk_out.get('rebalance_actions', []))} "
        f"| cost={risk_out.get('estimated_cost_pct', 0):.4%} "
        f"| overlays={risk_out.get('overlays_applied', [])}"
    )
    await _save_step_log(
        analysis_id, "6_risk_mgr", "risk_manager",
        input_data={"adjusted_weights": synthesizer_out.get("adjusted_weights")},
        output_data=risk_out,
        duration_ms=dur_risk,
    )

    # Stage 7: 更新 analysis 行（填充完整数据）
    await _finalize_analysis(
        analysis_id, quant_baseline, synthesizer_out, risk_out
    )

    # Stage 8: COMMUNICATOR —— LLM + fallback
    t0 = time.time()
    comm_out = await run_communicator_async(
        pipeline_context, synthesizer_out, risk_out
    )
    dur_comm = int((time.time() - t0) * 1000)
    logger.info(
        f"Stage 8 COMMUNICATOR done | used_fallback={comm_out.get('used_fallback', False)}"
    )
    await _save_step_log(
        analysis_id, "8_communicator", "communicator",
        input_data={"approved": approved, "stance": synthesizer_out.get("recommended_stance")},
        output_data=comm_out,
        duration_ms=dur_comm,
        model=settings.openai_model if not comm_out.get("used_fallback") else None,
        failed=comm_out.get("used_fallback", False),
    )

    # Stage 9: 分支执行
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


async def _create_analysis_placeholder(trigger: str, pipeline_context: dict) -> int:
    """提前创建 analysis 行（仅含 trigger + context），拿到 ID 供 step log 使用。"""
    async with AsyncSessionLocal() as db:
        row = AgentAnalysis(
            analyzed_at       = datetime.utcnow(),
            trigger_type      = trigger,
            planner_output    = pipeline_context,
            execution_status  = "running",
        )
        db.add(row)
        await db.commit()
        await db.refresh(row)
        return row.id


async def _finalize_analysis(
    analysis_id:    int,
    quant_baseline: dict,
    synthesizer_out: dict,
    risk_out:       dict,
) -> None:
    """Pipeline 结束时回填 analysis 行的完整数据。"""
    from sqlalchemy import update
    async with AsyncSessionLocal() as db:
        await db.execute(
            update(AgentAnalysis)
            .where(AgentAnalysis.id == analysis_id)
            .values(
                researcher_output = synthesizer_out,
                allocator_output  = quant_baseline,
                risk_output       = risk_out,
                risk_approved     = bool(risk_out.get("approved", False)),
                execution_status  = "pending",
            )
        )
        await db.commit()


async def _save_execution(analysis_id: int, result: dict) -> None:
    async with AsyncSessionLocal() as db:
        db.add(ExecutionLog(
            analysis_id     = analysis_id,
            command_type    = "weight_adjustment",
            command_payload = result.get("weights_sent", {}),
            status          = result.get("execution_status", "unknown"),
        ))
        await db.commit()
