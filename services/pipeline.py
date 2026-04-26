# services/pipeline.py
"""
Full agent pipeline async orchestration (V2.1 Bull/Bear debate version).

Pipeline stages (10-stage Python-LLM-Python relay):
    0. guard_and_config      (Python)   -- lock / pause check / read config / build context
    1. market_brief          (Python)   -- read snapshot + news cache / compute quant metrics / build prose
    2. quant_baseline        (Python)   -- pure math scoring -> base_weights
    3. RESEARCHER            (LLM)      -- base_weights + brief -> research_report (analysis only, no decisions)
   4a. BULL RESEARCHER       (LLM)      -- draft thesis (no weights)
   4b. BEAR RESEARCHER       (LLM)      -- draft thesis (no weights, parallel with 4a)
   4c. CROSS_EXAM           (LLM)      -- swap arguments, short rebuttals (parallel with opposite side)
    5. PM / SYNTHESIZER      (LLM)      -- sole adjusted_weights + decision_rationale
    6. RISK MGR              (Python)   -- overlays + 6 checks -> final target_weights + token
    7. _save_analysis        (Python)   -- write agent_analysis table
    8. COMMUNICATOR          (LLM+fb)   -- Telegram copy (degradable)
    9. Branch: rejected / SEMI_AUTO pending / FULL_AUTO execute

Core data flow (the baton being passed is weights):
    base_weights       (Stage 2 Python)       ->
    research_report    (Stage 3 LLM synthesis)  ->
    bull/bear_output   (Stage 4a/4b LLM debate) ->
    adjusted_weights   (Stage 5 LLM arbitration) ->
    target_weights     (Stage 6 Python)       ->
    execute            (Stage 9 Python)

News data is refreshed independently every 2h by cron/pre_fetch_news.py; the main pipeline
only reads from DB cache. Both crons fail independently: news down -> pipeline uses stale cache;
pipeline down -> news continues refreshing.
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
from agents.communicator     import run_communicator_async, append_command_hints, remove_command_hints
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
PIPELINE_TTL_MINUTES = 55  # slightly less than 1 hour cron interval


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


# ─────────────────────────────── PM Hard Clip ───────────────────────────────


def enforce_pm_constraints(
    base_weights: dict[str, float],
    adjusted_weights: dict[str, float],
    max_delta: float = 0.05,
    hard_max_delta: float = 0.10,
) -> tuple[dict[str, float], list[str]]:
    """
    Python-level hard clip of SYNTHESIZER's adjusted_weights output.

    Rules:
    1. Each ticker's deviation from base_weights must not exceed max_delta (default 5%)
    2. CASH handled separately: only allowed to increase, not decrease (conservative principle)
    3. New tickers (not in base): weight capped at hard_max_delta (10%)
    4. Renormalize after clip to ensure sum = 1.0
    5. Return clipped weights + clip log list

    Returns:
        clipped_weights: post-clip normalized weight dict
        clip_log: clipped entry list, format ["SPY: 0.32→0.27 (base=0.22, delta capped)"]
    """
    clipped: dict[str, float] = {}
    clip_log: list[str] = []

    all_tickers = set(base_weights.keys()) | set(adjusted_weights.keys())

    for ticker in all_tickers:
        base = base_weights.get(ticker, 0.0)
        adj = adjusted_weights.get(ticker, 0.0)

        if ticker == "CASH":
            if adj < base:
                clipped[ticker] = base
                clip_log.append(f"CASH: {adj:.3f}→{base:.3f} (cash floor enforced)")
            else:
                clipped[ticker] = adj
            continue

        if base == 0.0:
            if adj > hard_max_delta:
                clipped[ticker] = hard_max_delta
                clip_log.append(
                    f"{ticker}: {adj:.3f}→{hard_max_delta:.3f} (new position hard cap)"
                )
            else:
                clipped[ticker] = adj
        else:
            lower = max(0.0, base - max_delta)
            upper = base + max_delta
            capped = round(min(max(adj, lower), upper), 6)
            if abs(capped - adj) > 0.001:
                clip_log.append(
                    f"{ticker}: {adj:.3f}→{capped:.3f} (base={base:.3f}, delta={adj-base:+.3f} capped)"
                )
            clipped[ticker] = capped

    total = sum(clipped.values())
    if total <= 0:
        raise ValueError("enforce_pm_constraints: post-clip total weight is 0, data anomaly")

    normalized = {k: round(v / total, 6) for k, v in clipped.items()}

    diff = 1.0 - sum(normalized.values())
    if abs(diff) > 1e-9:
        largest = max(normalized, key=lambda k: normalized[k] if k != "CASH" else -1)
        normalized[largest] = round(normalized[largest] + diff, 6)

    return normalized, clip_log


def enforce_pm_constraints_v2(
    base_weights: dict[str, float],
    adjusted_weights: dict[str, float],
    researcher_signals: dict | None = None,
    default_max_delta: float = 0.03,
    hard_max_delta: float = 0.10,
) -> tuple[dict[str, float], list[str]]:
    """
    Python-level hard clip of SYNTHESIZER's adjusted_weights output (Task 7).

    Per-ticker max_delta derived from researcher_signals[ticker]["confidence"]:
        high:   0.05
        medium: 0.03
        low:    0.01
    Falls back to default_max_delta if ticker not in researcher_signals.

    Returns:
        clipped_weights: post-clip normalized weight dict
        clip_log: clipped entry list
    """
    clipped: dict[str, float] = {}
    clip_log: list[str] = []

    all_tickers = set(base_weights.keys()) | set(adjusted_weights.keys())

    CONFIDENCE_DELTA_MAP = {
        "high": 0.05,
        "medium": 0.03,
        "low": 0.01,
    }

    for ticker in all_tickers:
        base = base_weights.get(ticker, 0.0)
        adj = adjusted_weights.get(ticker, 0.0)

        if ticker == "CASH":
            if adj < base:
                clipped[ticker] = base
                clip_log.append(f"CASH: {adj:.3f}→{base:.3f} (cash floor enforced)")
            else:
                clipped[ticker] = adj
            continue

        # Dynamic max_delta from researcher confidence (Task 7)
        ticker_confidence = "medium"
        if researcher_signals and isinstance(researcher_signals, dict):
            sig = researcher_signals.get(ticker.upper(), {}) or {}
            ticker_confidence = str(sig.get("confidence", "medium")).strip()
        max_delta = CONFIDENCE_DELTA_MAP.get(ticker_confidence, default_max_delta)

        if base == 0.0:
            if adj > hard_max_delta:
                clipped[ticker] = hard_max_delta
                clip_log.append(
                    f"{ticker}: {adj:.3f}→{hard_max_delta:.3f} (new pos hard cap)"
                )
            else:
                clipped[ticker] = adj
        else:
            lower = max(0.0, base - max_delta)
            upper = base + max_delta
            capped = round(min(max(adj, lower), upper), 6)
            if abs(capped - adj) > 0.001:
                clip_log.append(
                    f"{ticker}: {adj:.3f}→{capped:.3f} "
                    f"(base={base:.3f}, confidence={ticker_confidence}, "
                    f"max_delta={max_delta:.2%})"
                )
            clipped[ticker] = capped

    total = sum(clipped.values())
    if total <= 0:
        raise ValueError("enforce_pm_constraints_v2: post-clip total weight is 0")

    normalized = {k: round(v / total, 6) for k, v in clipped.items()}

    diff = 1.0 - sum(normalized.values())
    if abs(diff) > 1e-9:
        largest = max(normalized, key=lambda k: normalized[k] if k != "CASH" else -1)
        normalized[largest] = round(normalized[largest] + diff, 6)

    return normalized, clip_log


# ─────────────────────────────── Regime Constraint Validation ───────────────────────────────


HEDGE_TICKERS = {"GLD", "TLT", "BND", "IEF"}


def apply_regime_constraints(
    target_weights: dict[str, float],
    regime_result: dict | None,
    base_weights: dict[str, float],
) -> tuple[dict[str, float], list[str]]:
    """
    Final validation and clipping of target_weights using regime constraints.

    Rules:
    1. Check allow_new_positions -- new tickers not in base with > 0 -> cap or reject
    2. Total equity weight must not exceed max_equity_weight
    3. CASH must not be below min_cash_weight

    Returns:
        (clipped_weights, violations_log)
    """
    if not regime_result:
        return target_weights, []

    constraints = regime_result.get("constraints", {})
    violations: list[str] = []
    working = dict(target_weights)

    # 1. New position check
    if not constraints.get("allow_new_positions", True):
        for ticker, w in list(working.items()):
            if ticker == "CASH" or ticker in HEDGE_TICKERS:
                continue
            if base_weights.get(ticker, 0.0) == 0.0 and w > 0:
                hard_cap = constraints.get("max_single_position", 0.15)
                if w > hard_cap:
                    violations.append(
                        f"new_pos {ticker}: {w:.3f}→{hard_cap:.3f} (new pos blocked in {regime_result.get('regime')})"
                    )
                    working[ticker] = hard_cap

    # 2. Equity weight cap
    max_equity = constraints.get("max_equity_weight", 1.0)
    cash_and_hedges = {k: v for k, v in working.items() if k == "CASH" or k in HEDGE_TICKERS}
    equity_weight = 1.0 - sum(cash_and_hedges.values())
    if equity_weight > max_equity + 1e-6:
        scale = max_equity / equity_weight
        for k in working:
            if k != "CASH" and k not in HEDGE_TICKERS:
                working[k] = round(working[k] * scale, 6)
        working["CASH"] = round(working.get("CASH", 0.0) + equity_weight - max_equity, 6)
        violations.append(
            f"equity_weight {equity_weight:.2%}→{max_equity:.2%} (regime cap)"
        )

    # 3. CASH floor
    min_cash = constraints.get("min_cash_weight", 0.05)
    if working.get("CASH", 0.0) < min_cash - 1e-6:
        shortfall = min_cash - working.get("CASH", 0.0)
        equity_tickers = [k for k in working if k != "CASH" and k not in HEDGE_TICKERS]
        if equity_tickers:
            largest = max(equity_tickers, key=lambda k: working[k])
            working[largest] = round(working[largest] - shortfall, 6)
        working["CASH"] = round(min_cash, 6)
        violations.append(
            f"CASH {working.get('CASH', 0.0):.2%}→{min_cash:.2%} (regime floor)"
        )

    # Normalize
    total = sum(working.values())
    if total > 0:
        working = {k: round(v / total, 6) for k, v in working.items()}

    return working, violations


# ─────────────────────────────── Stage 4d: Disagreement Map ───────────────────────────────


def _build_disagreement_map_for_pm(
    bull_output: dict,
    bear_output: dict,
    researcher_signals: dict,
) -> list[dict]:
    """
    Extract per-ticker Bull/Bear disagreements with researcher confidence constraints,
    for injection into PM's user_message before Stage 5 synthesizer call.

    Identical logic to synthesizer._build_debate_summary()["disagreement_map"]
    but called here so output can be passed as structured input to the synthesizer.
    """
    bull_views = bull_output.get("ticker_views") or {}
    bear_views = bear_output.get("ticker_views") or {}

    CONFIDENCE_DELTA_MAP = {
        "high": 0.05,
        "medium": 0.03,
        "low": 0.01,
    }

    disagreements: list[dict] = []
    all_tickers = set(bull_views.keys()) | set(bear_views.keys())

    for ticker in all_tickers:
        bull_view = bull_views.get(ticker)
        bear_view = bear_views.get(ticker)
        if not bull_view or not bear_view:
            continue

        bull_dir = bull_view.get("direction", "hold")
        bear_dir = bear_view.get("direction", "hold")

        is_conflict = (
            (bull_dir == "overweight" and bear_dir == "underweight")
            or (bull_dir == "underweight" and bear_dir == "overweight")
        )
        if not is_conflict:
            continue

        researcher_confidence = "medium"
        max_allowed_delta = 0.03
        if researcher_signals:
            sig = researcher_signals.get(ticker.upper(), {}) or {}
            researcher_confidence = str(sig.get("confidence", "medium")).strip()
            max_allowed_delta = CONFIDENCE_DELTA_MAP.get(researcher_confidence, 0.03)

        disagreements.append({
            "ticker": ticker,
            "bull": f"{bull_dir}({bull_view.get('magnitude')}) - {bull_view.get('primary_reason')}",
            "bear": f"{bear_dir}({bear_view.get('magnitude')}) - {bear_view.get('primary_reason')}",
            "researcher_confidence": researcher_confidence,
            "max_allowed_delta": max_allowed_delta,
        })

    return disagreements


# ─────────────────────────────── Stage 0: guard_and_config ───────────────────────────────


async def _guard_and_config(trigger: str) -> dict | None:
    """
    Read system config, build pipeline_context.
    Returns None to signal skip (paused / MANUAL mode).
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
    """Write one agent_step_log record. Silent failure, does not affect pipeline."""
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


# ─────────────────────────────── Main Entry ───────────────────────────────


async def run_full_pipeline(trigger: str = "scheduled_hourly") -> dict:
    """Run full agent pipeline."""
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

    # Pre-create analysis row to get analysis_id for step log
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

    # ── Regime hard classification step log ───────────────────────────────────
    regime_result = quant_baseline.get("regime_result")
    if regime_result:
        await _save_step_log(
            analysis_id, "2b_regime", "regime_classification",
            input_data=regime_result.get("signals", {}),
            output_data={
                "regime":      regime_result.get("regime"),
                "confidence":  regime_result.get("confidence"),
                "constraints": regime_result.get("constraints"),
                "reasoning":   regime_result.get("reasoning"),
            },
            duration_ms=0,
        )

    # Stage 3: RESEARCHER (LLM) -- info synthesis (analysis only, no decisions)
    t0 = time.time()
    research_report = await run_researcher_async(
        pipeline_context, brief, quant_baseline, regime_result
    )
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

    # Task 7: Extract researcher_signals for downstream confidence-based clipping
    researcher_signals: dict = research_report.get("ticker_signals_dict") or {}

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

    # Stage 4d: Build structured disagreement map for PM input injection
    disagreement_map = _build_disagreement_map_for_pm(
        bull_output=bull_output,
        bear_output=bear_output,
        researcher_signals=researcher_signals,
    )
    debate_summary_for_pm = {"disagreement_map": disagreement_map}
    if disagreement_map:
        logger.info(
            f"[Stage 4d] disagreement_map built: "
            f"{len(disagreement_map)} tickers with Bull/Bear conflict"
        )

    # Stage 5: PM / SYNTHESIZER (LLM) —— final adjusted_weights + decision_rationale
    risk_params = pipeline_context.get("risk_params", {})
    t0 = time.time()
    synthesizer_out = await run_synthesizer_async(
        research_report, bull_output, bear_output,
        base_weights, brief, risk_params, regime_result,
        debate_summary=debate_summary_for_pm,
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
            "regime": regime_result.get("regime") if regime_result else None,
        },
        output_data=synthesizer_out,
        duration_ms=dur_synth,
        model=model_heavy,
        failed=synthesizer_out.get("used_degraded_fallback", False),
    )

    # ── Stage 5 Task 5: CoT reasoning_chain extraction + consistency validation ──
    reasoning_chain = synthesizer_out.get("reasoning_chain") or {}
    if reasoning_chain:
        # Step 4 consistency validation + auto-correction: cash_pct vs actual CASH weight
        step4 = reasoning_chain.get("step4_risk_sanity_check") or {}
        stated_cash = step4.get("cash_pct")
        actual_cash = synthesizer_out.get("adjusted_weights", {}).get("CASH", 0)
        if stated_cash is not None and actual_cash is not None:
            cash_diff = abs(stated_cash - actual_cash)
            if cash_diff > 0.03:
                logger.warning(
                    f"[Stage5] Synthesizer CoT inconsistency: "
                    f"step4.cash_pct={stated_cash:.2%} vs actual CASH={actual_cash:.2%} — auto-correcting"
                )
                step4["cash_pct"] = round(actual_cash, 4)
                reasoning_chain["step4_risk_sanity_check"] = step4
                synthesizer_out["reasoning_chain"] = reasoning_chain
            else:
                logger.info(f"[Stage5] CoT cash consistency check passed: step4={stated_cash:.2%} actual={actual_cash:.2%}")

        # Also validate total_equity_pct
        actual_equity = round(1.0 - actual_cash, 4)
        stated_equity = step4.get("total_equity_pct")
        if stated_equity is not None:
            equity_diff = abs(stated_equity - actual_equity)
            if equity_diff > 0.01:
                logger.warning(
                    f"[Stage5] total_equity_pct {stated_equity:.2%} ≠ {actual_equity:.2%} — auto-correcting"
                )
                step4["total_equity_pct"] = actual_equity
                reasoning_chain["step4_risk_sanity_check"] = step4
                synthesizer_out["reasoning_chain"] = reasoning_chain

        # Log reasoning_chain to agent_step_log for audit
        await _save_step_log(
            analysis_id, "5a_synthesizer_cot", "synthesizer_cot",
            input_data={"regime": regime_result.get("regime") if regime_result else None},
            output_data=reasoning_chain,
            duration_ms=0,
            model=model_heavy,
            failed=False,
        )
    else:
        logger.warning("[Stage5] No reasoning_chain in synthesizer output (Task 5)")

    # ── Stage 5→6: PM Hard Clip (Task 7: confidence-aware) ─────────────────
    adjusted_weights_raw = synthesizer_out.get("adjusted_weights") or {}
    if not adjusted_weights_raw:
        logger.info("[Stage5→6] degraded fallback, skipping PM hard clip")
    else:
        adjusted_weights_clipped, clip_log = enforce_pm_constraints_v2(
            base_weights=base_weights,
            adjusted_weights=adjusted_weights_raw,
            researcher_signals=researcher_signals,
            default_max_delta=0.03,
            hard_max_delta=0.10,
        )
        synthesizer_out["adjusted_weights"] = adjusted_weights_clipped

        if clip_log:
            logger.warning(
                f"[Stage5→6] PM weights hard-clipped {len(clip_log)} items:\n" + "\n".join(clip_log)
            )
            await _save_step_log(
                analysis_id, "5b_pm_constraint", "pm_constraint_enforcement",
                input_data={"adjusted_weights_raw": adjusted_weights_raw},
                output_data={
                    "adjusted_weights_clipped": adjusted_weights_clipped,
                    "clip_log": clip_log,
                },
                duration_ms=0,
            )
        else:
            logger.info("[Stage5→6] PM weights within constraints, no clip needed")

    # Stage 6: RISK MGR (Python) —— overlays + 6 checks
    # synthesizer_out interface compatible with old researcher_out, Risk MGR unchanged
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

    # ── Stage 6→7: Regime Hard Constraint Validation ───────────────────────────────
    if regime_result and risk_out.get("approved"):
        target_from_risk = risk_out.get("target_weights") or {}
        clipped, regime_violations = apply_regime_constraints(
            target_from_risk, regime_result, base_weights
        )
        risk_out["target_weights"] = clipped
        if regime_violations:
            logger.warning(
                f"[Stage6→7] Regime constraint clipped {len(regime_violations)} items:\n"
                + "\n".join(regime_violations)
            )
            await _save_step_log(
                analysis_id, "6b_regime_constraint", "regime_enforcement",
                input_data={"regime": regime_result.get("regime"), "target_weights_raw": target_from_risk},
                output_data={"target_weights_clipped": clipped, "violations": regime_violations},
                duration_ms=0,
            )
        else:
            logger.info("[Stage6→7] Regime weights within constraints, no clip needed")

    # Stage 7: Update analysis row (fill complete data)
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

    # Stage 9: Branch execution
    auth_mode = pipeline_context["auth_mode"]

    if not approved:
        # Never expose confirm/skip/pause when there is no pending proposal.
        await tool_send_telegram({"text": remove_command_hints(comm_out["text"])})
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


# ─────────────────────────────── SEMI_AUTO Proposal ───────────────────────────────


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

    # Command hints are bound to pending state, so append only after proposal is saved.
    await tool_send_telegram({"text": append_command_hints(comm_out["text"])})


# ─────────────────────────────── Archival ───────────────────────────────


async def _create_analysis_placeholder(trigger: str, pipeline_context: dict) -> int:
    """Pre-create analysis row (with trigger + context only), get ID for step log use."""
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
    """Backfill complete data to analysis row when pipeline ends."""
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
