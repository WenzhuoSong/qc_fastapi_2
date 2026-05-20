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
    5. PM / SYNTHESIZER      (LLM)      -- thesis/advisory + diagnostic adjusted_weights
   5e. TARGET BUILDER        (Python)   -- deterministic target_weights from base + advisory
    6. RISK MGR              (Python)   -- validation checks -> approval token
    6.5 POSITION MANAGER     (Python)   -- quantity/frequency constraints -> adjusted target_weights
    7. _save_analysis        (Python)   -- write agent_analysis table
    8. COMMUNICATOR          (LLM+fb)   -- Telegram copy (degradable)
    9. Branch: rejected / SEMI_AUTO pending / FULL_AUTO execute

Core data flow:
    base_weights       (Stage 2 Python)       ->
    research_report    (Stage 3 LLM synthesis)  ->
    bull/bear_output   (Stage 4a/4b LLM debate) ->
    advisory_proposals (Stage 5 LLM semantics) ->
    target_weights     (Stage 5e Python target_builder) ->
    risk approval      (Stage 6 Python)       ->
    execute            (Stage 9 Python)

News data is refreshed independently every 2h by cron/pre_fetch_news.py; the main pipeline
only reads from DB cache. Both crons fail independently: news down -> pipeline uses stale cache;
pipeline down -> news continues refreshing.
"""
import asyncio
import hashlib
import json
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
from services.position_manager import apply_position_constraints
from services.playground      import run_playground
from services.evidence_bundle import build_evidence_bundle
from services.market_scorecard import build_market_scorecard
from services.news_evidence   import build_news_evidence
from services.decision_style  import resolve_decision_style
from services.decision_ledger import (
    apply_execution_audit_to_decision_ledger,
    build_decision_ledger,
)
from services.target_builder import build_target_weights, compare_target_weights
from services.decision_live_validation import (
    format_decision_live_validation_report,
    validate_decision_live_artifacts,
)
from services.strategy_use_constraints import apply_strategy_use_constraints
from services.proposal_shaper import shape_proposal_before_risk
from services.position_governance import apply_position_governance
from services.empirical_profile_store import (
    build_empirical_profiles_from_feature_store,
    collect_empirical_profile_tickers,
)
from services.execution_audit import build_execution_audit_payload, count_today_actual_execution_actions
from strategies              import compute_rebalance_actions, estimate_cost_pct
from tracking.monitor_client import PipelineRunTracker
from db.session          import AsyncSessionLocal
from db.queries          import get_system_config, upsert_system_config
from db.models           import AgentAnalysis, AgentStepLog, ExecutionLog
from tools.notify_tools  import tool_send_telegram
from services.proposal   import save_pending_proposal, validate_proposal_still_relevant
from config              import get_settings

logger   = logging.getLogger("qc_fastapi_2.pipeline")
settings = get_settings()

# --------------- Pipeline TTL Lock ---------------
PIPELINE_LOCK_KEY    = "pipeline_lock"
PIPELINE_TTL_MINUTES = 55  # slightly less than 1 hour cron interval
REJECTED_NOTIFICATION_STATE_KEY = "last_rejected_pipeline_notification"
REJECTED_NOTIFICATION_COOLDOWN_MINUTES = 360


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
    Returns None to signal skip (paused / MANUAL mode / circuit ALERT).
    """
    # Read config (circuit state read separately below for trigger evaluation)
    async with AsyncSessionLocal() as db:
        paused_cfg      = await get_system_config(db, "trading_paused")
        risk_cfg        = await get_system_config(db, "risk_params")
        auth_cfg        = await get_system_config(db, "authorization_mode")
        circuit_cfg     = await get_system_config(db, "circuit_state")
        active_cfg      = await get_system_config(db, "active_strategy")
        alerts_cfg      = await get_system_config(db, "pending_critical_alerts")
        pm_cfg          = await get_system_config(db, "position_manager_config")
        pg_cfg          = await get_system_config(db, "position_governance_config")

    paused = bool((paused_cfg.value if paused_cfg else {}).get("paused", False))
    if paused:
        logger.info("trading_paused=True — pipeline skipped")
        return None

    auth_mode = (auth_cfg.value if auth_cfg else {"value": "SEMI_AUTO"}).get("value", "SEMI_AUTO")
    if auth_mode == "MANUAL":
        logger.info("MANUAL mode — pipeline skipped")
        return None

    risk_params = (risk_cfg.value if risk_cfg else {}) or {}

    # ── Phase 3: Circuit state — circuit is already evaluated at pipeline entry ─
    # circuit_cfg.value may be stale; we use it as fallback only
    circuit = (circuit_cfg.value if circuit_cfg else {"value": "CLOSED"}).get("value", "CLOSED")

    # Phase 3: In FULL_AUTO with circuit ALERT/DEFENSIVE, alert instead of running
    if auth_mode == "FULL_AUTO" and circuit in ("ALERT", "DEFENSIVE"):
        from tools.notify_tools import tool_send_telegram
        emoji = "🟡" if circuit == "ALERT" else "🔴"
        await tool_send_telegram({
            "text": (
                f"{emoji} FULL_AUTO: Circuit={circuit} is open. "
                f"Pipeline paused for {circuit}. "
                f"Reply /confirm to override or /reset_circuit once resolved."
            )
        })
        logger.warning(f"[pipeline] FULL_AUTO blocked by circuit={circuit}")
        return None

    active_name = (active_cfg.value if active_cfg else {"value": "momentum_lite_v1"}).get(
        "value", "momentum_lite_v1"
    )
    pending_alerts = (alerts_cfg.value if alerts_cfg else {}).get("alerts", []) or []
    position_manager_config = (pm_cfg.value if pm_cfg else {}) or {}
    position_governance_config = (pg_cfg.value if pg_cfg else {}) or {}

    params_key = f"strategy_{active_name}_params"
    async with AsyncSessionLocal() as db:
        params_cfg = await get_system_config(db, params_key)
    strategy_params = (params_cfg.value if params_cfg else {}) or {}

    override_mode = "DEFENSIVE" if circuit in ("ALERT", "DEFENSIVE") else None

    return {
        "trigger":           trigger,
        "plan_id":           f"P-{datetime.utcnow().strftime('%Y%m%d-%H%M')}",
        "auth_mode":         auth_mode,
        "circuit_state":     circuit,
        "override_mode":     override_mode,
        "risk_params":       risk_params,
        "active_strategy":   active_name,
        "strategy_params":   strategy_params,
        "pending_alerts":    pending_alerts,
        "position_manager_config": position_manager_config,
        "position_governance_config": position_governance_config,
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
        token_usage = None
        if isinstance(output_data, dict) and isinstance(output_data.get("_token_usage"), dict):
            token_usage = output_data.get("_token_usage")
        async with AsyncSessionLocal() as db:
            db.add(AgentStepLog(
                analysis_id = analysis_id,
                stage       = stage,
                agent_name  = agent_name,
                input_data  = input_data,
                output_data = output_data,
                duration_ms = duration_ms,
                model       = model,
                token_usage = token_usage,
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

    # Local monitor telemetry facade. Durable per-stage data is written to AgentStepLog.
    tracker = PipelineRunTracker()
    regime_result_for_tracker: dict | None = None
    synthesizer_out_for_tracker: dict | None = None
    risk_out_for_tracker: dict | None = None
    pipeline_status = "unknown"

    # ── Phase 3: Circuit Breaker — evaluate triggers before pipeline runs ───────
    from services.circuit_breaker import evaluate_and_apply
    try:
        transition = await evaluate_and_apply()
        if transition:
            logger.info(
                f"[circuit_breaker] pre-pipeline transition: "
                f"{transition.from_state.value} → {transition.to_state.value} | {transition.reason}"
            )
            tracker.log_circuit_transition(transition)
            tracker.log_trigger_results(transition.all_trigger_results)
    except Exception as e:
        logger.warning(f"[circuit_breaker] trigger evaluation failed: {e}")

    # Stage 0: guard + config
    pipeline_context = await _guard_and_config(trigger)
    if pipeline_context is None:
        tracker.end_run("skipped_gated")
        return {"status": "skipped_gated"}

    if not tracker.is_disabled:
        regime_result_for_tracker = pipeline_context.get("regime_result")
        tracker.start_run(pipeline_context, regime_result_for_tracker)

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
        pipeline_status = "skipped_no_snapshot"
        tracker.end_run(pipeline_status)
        return {"status": pipeline_status}
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
    tracker.log_stage_metrics("1_brief", dur_brief, n_holdings=len(brief.get("holdings", [])))

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
    tracker.log_stage_metrics("2_quant", dur_quant,
        n_signals=len(quant_baseline.get("signal_scores", [])),
        n_selected=len(quant_baseline.get("selected_tickers", [])))

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

    # Stage 2c: Strategy Playground comparison bundle (advisory only)
    playground_bundle = None
    try:
        playground_brief = {**brief, "risk_params": pipeline_context.get("risk_params", {})}
        playground_bundle_obj = await run_playground(playground_brief)
        playground_bundle = playground_bundle_obj.to_dict()
        await _save_step_log(
            analysis_id, "2c_playground", "strategy_playground",
            input_data={"strategies": [s["strategy_name"] for s in playground_bundle.get("strategies", [])]},
            output_data=playground_bundle,
            duration_ms=0,
        )
        logger.info(
            f"Stage 2c Playground done | strategies={len(playground_bundle.get('strategies', []))} "
            f"| divergences={len(playground_bundle.get('divergence_map', []))}"
        )
    except Exception as e:
        logger.warning(f"Stage 2c Playground skipped: {e}")

    # Stage 2d: Evidence bundle + market scorecard contracts (no execution effect yet)
    empirical_profiles = {}
    try:
        empirical_tickers = collect_empirical_profile_tickers(
            brief=brief,
            quant_baseline=quant_baseline,
            playground_bundle=playground_bundle,
        )
        async with AsyncSessionLocal() as db:
            empirical_profiles = await build_empirical_profiles_from_feature_store(
                db,
                tickers=empirical_tickers,
                lookback_days=420,
                source="yfinance",
            )
        brief["empirical_profiles"] = empirical_profiles
        logger.info(
            "Stage 2d empirical profiles loaded | tickers=%s profiles=%s",
            len(empirical_tickers),
            len(empirical_profiles),
        )
    except Exception as e:
        logger.warning(f"Stage 2d empirical profile loading failed; continuing without profiles: {e}")
        brief["empirical_profiles"] = {}

    try:
        news_evidence = build_news_evidence(brief)
    except Exception as e:
        logger.warning(f"Stage 2d news evidence scoring failed; using fallback: {e}")
        news_evidence = {
            "macro_news_score": {
                "overall_bias": "neutral",
                "confidence": "low",
                "dominant_themes": [],
                "market_impact": "low",
                "time_horizon": "medium_term",
                "data_quality": "missing",
                "warnings": [f"news evidence scoring failed: {e}"],
            },
            "ticker_news_scores": {},
            "hard_risk_events": {},
            "ignored_items": [],
            "data_gaps": [f"news evidence scoring failed: {e}"],
        }
    evidence_bundle = build_evidence_bundle(
        brief=brief,
        quant_baseline=quant_baseline,
        playground_bundle=playground_bundle,
        news_evidence=news_evidence,
        empirical_profiles=empirical_profiles,
    )
    market_scorecard = build_market_scorecard(evidence_bundle)
    try:
        decision_style = resolve_decision_style(
            market_scorecard=market_scorecard,
            news_evidence=news_evidence,
            strategy_evidence=evidence_bundle.get("strategies") or {},
            config=pipeline_context.get("decision_style_config") or {},
        )
    except Exception as e:
        logger.warning(f"Stage 2e decision style resolver failed; using fallback: {e}")
        decision_style = {
            "analysis_style": "balanced",
            "trade_style": "hold_unless_strong",
            "style_reason": f"Decision style resolver failed: {e}",
            "style_limits": {
                "max_adjustment_multiplier": 0.5,
                "max_turnover_per_cycle": 0.10,
                "max_single_trade_pct": 0.04,
                "max_new_buys_per_cycle": 0,
                "min_cash_floor_addition": 0.05,
                "rebalance_threshold_boost": 0.02,
                "allow_new_positions": False,
                "prefer_hedges": False,
                "sell_priority": False,
            },
            "dominant_style_constraint": "decision_style_resolver_failed",
            "triggered_style_rules": ["decision_style_resolver_failed"],
            "reasons": [f"Decision style resolver failed: {e}"],
            "warnings": [f"Decision style resolver failed: {e}"],
        }
    evidence_bundle["decision_style"] = decision_style
    brief["evidence_bundle"] = evidence_bundle
    brief["market_scorecard"] = market_scorecard
    brief["news_evidence"] = news_evidence
    brief["decision_style"] = decision_style
    pipeline_context["evidence_bundle"] = evidence_bundle
    pipeline_context["feature_provenance"] = brief.get("feature_provenance") or {}
    pipeline_context["market_scorecard"] = market_scorecard
    pipeline_context["news_evidence"] = news_evidence
    pipeline_context["decision_style"] = decision_style
    logger.info(
        "Stage 2d Evidence Scorecard done | "
        f"condition={market_scorecard.get('market_condition')} "
        f"| permission={market_scorecard.get('investment_permission')} "
        f"| data_quality={market_scorecard.get('data_quality')} "
        f"| dominant={market_scorecard.get('dominant_constraint')} "
        f"| style={decision_style.get('analysis_style')}/{decision_style.get('trade_style')}"
    )
    await _save_step_log(
        analysis_id, "2d_evidence_scorecard", "evidence_scorecard",
        input_data={
            "playground_available": bool(playground_bundle),
            "regime": (quant_baseline.get("regime_result") or {}).get("regime"),
        },
        output_data={
            "evidence_bundle": evidence_bundle,
            "market_scorecard": market_scorecard,
            "news_evidence": news_evidence,
        },
        duration_ms=0,
    )
    await _save_step_log(
        analysis_id, "2e_decision_style", "decision_style",
        input_data={
            "scorecard_permission": market_scorecard.get("investment_permission"),
            "scorecard_condition": market_scorecard.get("market_condition"),
            "news_macro_bias": (news_evidence.get("macro_news_score") or {}).get("overall_bias"),
            "strategy_data_quality": (evidence_bundle.get("strategies") or {}).get("data_quality"),
        },
        output_data=decision_style,
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
    tracker.log_stage_metrics("3_researcher", dur_researcher,
        tokens=research_report.get("_token_usage"),
        n_ticker_signals=len(research_report.get("ticker_signals", [])),
        degraded=research_report.get("used_degraded_fallback", False))

    # Phase 3: Record LLM failure for circuit breaker monitoring
    if research_report.get("used_degraded_fallback", False):
        from services.circuit_breaker import record_stage_failure
        await record_stage_failure("researcher")
        tracker.log_retry_event("researcher", 1, "degraded_fallback")

    # Task 7: Extract researcher_signals for downstream confidence-based clipping
    researcher_signals: dict = research_report.get("ticker_signals_dict") or {}

    # Stage 4a/4b: BULL + BEAR drafts (parallel, no weights)
    base_weights = quant_baseline.get("base_weights", {})
    t0 = time.time()
    bull_draft, bear_draft = await asyncio.gather(
        run_bull_researcher_async(
            research_report,
            base_weights,
            news_evidence=news_evidence,
            decision_style=decision_style,
        ),
        run_bear_researcher_async(
            research_report,
            base_weights,
            news_evidence=news_evidence,
            decision_style=decision_style,
        ),
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
    tracker.log_stage_metrics("4_drafts", dur_draft,
        tokens_bull=bull_draft.get("_token_usage"),
        tokens_bear=bear_draft.get("_token_usage"),
        bull_failed=bull_draft.get("failed", False),
        bear_failed=bear_draft.get("failed", False))

    # Phase 3: Record LLM failures for circuit breaker monitoring
    if bull_draft.get("failed", False) or bear_draft.get("failed", False):
        from services.circuit_breaker import record_stage_failure
        if bull_draft.get("failed"):
            await record_stage_failure("bull_researcher")
            tracker.log_retry_event("bull_researcher", 1, "degraded_fallback")
        if bear_draft.get("failed"):
            await record_stage_failure("bear_researcher")
            tracker.log_retry_event("bear_researcher", 1, "degraded_fallback")

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
    tracker.log_stage_metrics("4c_cross_exam", dur_ce,
        tokens_bull=rebuttal_vs_bear.get("_token_usage"),
        tokens_bear=rebuttal_vs_bull.get("_token_usage"))

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

    # Stage 5: PM / SYNTHESIZER (LLM) —— advisory + diagnostic adjusted_weights
    risk_params = pipeline_context.get("risk_params", {})
    t0 = time.time()
    synthesizer_out = await run_synthesizer_async(
        research_report, bull_output, bear_output,
        base_weights, brief, risk_params, regime_result,
        debate_summary=debate_summary_for_pm,
        playground_bundle=playground_bundle,
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
            "playground_consensus_weights": (playground_bundle or {}).get("consensus_weights"),
        },
        output_data=synthesizer_out,
        duration_ms=dur_synth,
        model=model_heavy,
        failed=synthesizer_out.get("used_degraded_fallback", False),
    )
    tracker.log_stage_metrics("5_synthesizer", dur_synth,
        tokens=synthesizer_out.get("_token_usage"),
        n_adjustments=len(synthesizer_out.get("weight_adjustments", [])),
        degraded=synthesizer_out.get("used_degraded_fallback", False))
    synthesizer_out_for_tracker = synthesizer_out

    # Phase 3: Record LLM failure for circuit breaker monitoring
    if synthesizer_out.get("used_degraded_fallback", False):
        from services.circuit_breaker import record_stage_failure
        await record_stage_failure("synthesizer")
        tracker.log_retry_event("synthesizer", 1, "degraded_fallback")

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

    # ── Stage 5 diagnostic guardrails: keep legacy adjusted_weights bounded for reporting.
    # Execution target construction below uses advisory proposals and deterministic state.
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

        strategy_weights_clipped, strategy_clip_log = apply_strategy_use_constraints(
            base_weights=base_weights,
            adjusted_weights=synthesizer_out.get("adjusted_weights") or {},
            strategy_evidence=(evidence_bundle.get("strategies") or {}),
        )
        synthesizer_out["adjusted_weights"] = strategy_weights_clipped
        strategy_use_enforcement = {
            "applied": bool(strategy_clip_log),
            "violations": strategy_clip_log,
            "clip_log": strategy_clip_log,
            "strategy_use_summary": (evidence_bundle.get("strategies") or {}).get("strategy_use_summary") or {},
            "evidence_summary": (evidence_bundle.get("strategies") or {}).get("evidence_summary") or {},
            "target_weights_pre_strategy_use_clip": adjusted_weights_clipped,
            "target_weights_post_strategy_use_clip": strategy_weights_clipped,
        }
        synthesizer_out["strategy_use_enforcement"] = strategy_use_enforcement
        pipeline_context["strategy_use_enforcement"] = strategy_use_enforcement
        if strategy_clip_log:
            logger.warning(
                "[Stage5→6] Strategy-use constraints clipped "
                f"{len(strategy_clip_log)} items:\n" + "\n".join(strategy_clip_log)
            )
            await _save_step_log(
                analysis_id, "5c_strategy_use_constraint", "strategy_use_constraint",
                input_data={
                    "adjusted_weights_after_pm_clip": adjusted_weights_clipped,
                    "strategy_use_summary": strategy_use_enforcement["strategy_use_summary"],
                },
                output_data={
                    "adjusted_weights_clipped": strategy_weights_clipped,
                    "clip_log": strategy_clip_log,
                },
                duration_ms=0,
            )
        else:
            logger.info("[Stage5→6] Strategy-use constraints passed, no clip needed")

        proposal_shape = shape_proposal_before_risk(
            adjusted_weights=synthesizer_out.get("adjusted_weights") or {},
            current_weights=brief.get("current_weights") or {},
            holdings_meta=brief.get("holdings") or [],
            market_scorecard=market_scorecard,
            decision_style=decision_style,
        )
        synthesizer_out["proposal_shaping"] = proposal_shape
        if proposal_shape.get("applied"):
            pre_shape = synthesizer_out.get("adjusted_weights") or {}
            synthesizer_out["adjusted_weights"] = proposal_shape.get("adjusted_weights") or pre_shape
            logger.warning(
                "[Stage5→6] Proposal shaper clipped %s items:\n%s",
                len(proposal_shape.get("clip_log") or []),
                "\n".join(proposal_shape.get("clip_log") or []),
            )
            await _save_step_log(
                analysis_id, "5d_proposal_shaper", "proposal_shaper",
                input_data={
                    "adjusted_weights_before_shaping": pre_shape,
                    "current_weights": brief.get("current_weights") or {},
                    "market_scorecard": market_scorecard,
                    "decision_style": decision_style,
                },
                output_data=proposal_shape,
                duration_ms=0,
            )
        else:
            logger.info("[Stage5→6] Proposal shaper passed, no pre-risk clip needed")

    target_builder_enabled = bool((pipeline_context.get("risk_params") or {}).get("target_builder_enabled", True))
    if target_builder_enabled:
        try:
            pre_risk_governance = apply_position_governance(
                target_weights=base_weights,
                current_weights=brief.get("current_weights") or {},
                holdings_meta=brief.get("holdings") or [],
                strategy_evidence=evidence_bundle.get("strategies") or {},
                market_scorecard=market_scorecard,
                news_evidence=news_evidence,
                llm_advisory_proposals=synthesizer_out.get("position_advisory_proposals") or [],
                config=pipeline_context.get("position_governance_config") or {},
            )
            target_builder_gated = build_target_weights(
                base_weights=base_weights,
                current_weights=brief.get("current_weights") or {},
                market_scorecard=market_scorecard,
                decision_style=decision_style,
                position_governance={
                    "mode": "pre_risk_target_builder_gated",
                    "position_decisions": pre_risk_governance.position_decisions,
                    "advisory_overrides": pre_risk_governance.advisory_overrides,
                    "portfolio_summary": pre_risk_governance.portfolio_summary,
                },
                validated_advisory=pre_risk_governance.advisory_overrides,
                constraints={
                    "max_turnover": (market_scorecard or {}).get("max_turnover_per_cycle"),
                    "max_single_delta": (market_scorecard or {}).get("max_adjustment_from_base"),
                },
            ).to_dict()
            pipeline_context["target_builder_enabled"] = True
            pipeline_context["target_builder_gated"] = target_builder_gated
            await _save_step_log(
                analysis_id, "5e_target_builder_gated_input", "target_builder",
                input_data={
                    "mode": "gated_pre_risk",
                    "base_weights": base_weights,
                    "current_weights": brief.get("current_weights") or {},
                    "market_scorecard": market_scorecard,
                    "decision_style": decision_style,
                },
                output_data={
                    "target_builder": target_builder_gated,
                    "pre_risk_governance": {
                        "position_decisions": pre_risk_governance.position_decisions,
                        "advisory_overrides": pre_risk_governance.advisory_overrides,
                        "trade_summary": pre_risk_governance.trade_summary,
                    },
                },
                duration_ms=0,
            )
            logger.info("[Stage5→6] Target Builder gated input prepared")
        except Exception as e:
            pipeline_context["target_builder_enabled"] = True
            pipeline_context["target_builder_gated_error"] = str(e)
            logger.warning("[Stage5→6] Target Builder gated input failed; RiskMgr will use deterministic base fallback: %s", e)
            await _save_step_log(
                analysis_id, "5e_target_builder_gated_input", "target_builder",
                input_data={"mode": "gated_pre_risk", "base_weights": base_weights},
                output_data={"error": str(e), "fallback": "deterministic_base_weights"},
                duration_ms=0,
                failed=True,
            )

    # Stage 6: RISK MGR (Python) —— validate deterministic target + checks
    t0 = time.time()
    risk_out = await run_risk_manager_async(
        pipeline_context, brief, quant_baseline, synthesizer_out
    )
    dur_risk = int((time.time() - t0) * 1000)
    approved = bool(risk_out.get("approved", False))

    # Phase 3: Record rejection for circuit breaker monitoring
    if not approved:
        from services.circuit_breaker import record_rejection_event
        await record_rejection_event(analysis_id)

    # P1-1: Clear pending critical alerts after RISK MGR consumes them
    if pipeline_context.get("pending_alerts"):
        async with AsyncSessionLocal() as db:
            await upsert_system_config(db, "pending_critical_alerts", {"alerts": []}, "pipeline")
        logger.info("[pipeline] Cleared pending_critical_alerts after RISK MGR consumption")
    logger.info(
        f"Stage 6 RISK MGR done | approved={approved} "
        f"| n_actions={len(risk_out.get('rebalance_actions', []))} "
        f"| cost={risk_out.get('estimated_cost_pct', 0):.4%} "
        f"| overlays={risk_out.get('overlays_applied', [])}"
    )
    await _save_step_log(
        analysis_id, "6_risk_mgr", "risk_manager",
        input_data={
            "target_builder_input": pipeline_context.get("target_builder_gated"),
            "diagnostic_llm_adjusted_weights": synthesizer_out.get("adjusted_weights"),
        },
        output_data=risk_out,
        duration_ms=dur_risk,
    )
    tracker.log_stage_metrics("6_risk_mgr", dur_risk,
        risk_approved=bool(risk_out.get("approved")),
        n_actions=len(risk_out.get("rebalance_actions", [])),
        overlay_count=len(risk_out.get("overlays_applied") or []))

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

    # ── Stage 6.5a: Position Governance lifecycle controls ───────────────────
    auth_mode = pipeline_context.get("auth_mode")
    full_auto_governance_only = bool(
        auth_mode == "FULL_AUTO"
        and not risk_out.get("approved")
        and (brief.get("current_weights") or {})
    )
    run_governance_execution = bool(
        (risk_out.get("approved") and risk_out.get("target_weights"))
        or full_auto_governance_only
    )
    run_governance_diagnostic = bool(
        (not risk_out.get("approved"))
        and not full_auto_governance_only
        and (brief.get("current_weights") or {})
    )
    if run_governance_execution or run_governance_diagnostic:
        governance_mode = "full_auto_governance_only" if full_auto_governance_only else "execution" if run_governance_execution else "diagnostic_only"
        target_before_governance = (
            (risk_out.get("target_weights") or {})
            if run_governance_execution and not full_auto_governance_only
            else (brief.get("current_weights") or {})
        )
        governance_config = dict(pipeline_context.get("position_governance_config") or {})
        if auth_mode == "FULL_AUTO":
            governance_config["advisory_basket_loss_auto_trim_enabled"] = 1.0
        if full_auto_governance_only:
            governance_config["replacement_enabled"] = 0.0
            governance_config["llm_advisory_enabled"] = 0.0
        governance_out = apply_position_governance(
            target_weights=target_before_governance,
            current_weights=brief.get("current_weights") or {},
            holdings_meta=brief.get("holdings") or [],
            strategy_evidence=evidence_bundle.get("strategies") or {},
            market_scorecard=market_scorecard,
            news_evidence=news_evidence,
            llm_advisory_proposals=synthesizer_out.get("position_advisory_proposals") or [],
            config=governance_config,
        )
        risk_out["position_governance"] = {
            "mode": governance_mode,
            "position_decisions": governance_out.position_decisions,
            "blocked_actions": governance_out.blocked_actions,
            "forced_trims": governance_out.forced_trims if run_governance_execution else [],
            "replacements": governance_out.replacements if run_governance_execution else [],
            "advisory_overrides": governance_out.advisory_overrides,
            "manual_action_hints": governance_out.manual_action_hints,
            "trade_summary": governance_out.trade_summary,
            "portfolio_summary": governance_out.portfolio_summary,
            "config": governance_out.config,
        }

        if run_governance_execution:
            risk_out["target_weights"] = governance_out.adjusted_weights
            rebalance_threshold = float((pipeline_context.get("risk_params") or {}).get("rebalance_threshold", 0.02))
            if full_auto_governance_only:
                rebalance_threshold = min(rebalance_threshold, 0.005)
            rebalance_actions = compute_rebalance_actions(
                governance_out.adjusted_weights,
                brief.get("current_weights") or {},
                rebalance_threshold,
            )
            risk_out["rebalance_actions"] = rebalance_actions
            risk_out["estimated_cost_pct"] = estimate_cost_pct(rebalance_actions)
            risk_out["n_holdings"] = governance_out.trade_summary.get("position_count", risk_out.get("n_holdings"))
            if full_auto_governance_only and governance_out.forced_trims and rebalance_actions:
                from tools.db_tools import tool_write_approval_token
                token_result = await tool_write_approval_token({})
                risk_out["approved"] = True
                approved = True
                risk_out["approval_token"] = token_result["approval_token"]
                risk_out["token_expires_at"] = token_result["expires_at"]
                risk_out["approval_source"] = "full_auto_position_governance_risk_reduction"
                risk_out["original_risk_approved"] = False
                risk_out.setdefault("overlays_applied", []).append("full_auto_position_governance_risk_reduction")
                risk_out.setdefault("governance_execution_notes", []).append(
                    "FULL_AUTO executed deterministic risk-reducing position governance trims only"
                )

        if governance_out.blocked_actions or governance_out.forced_trims:
            logger.warning(
                "[Stage6.5a] Position Governance %s | blocked=%s trims=%s",
                governance_mode,
                governance_out.blocked_actions,
                governance_out.forced_trims if run_governance_execution else [],
            )
        else:
            logger.info("[Stage6.5a] Position Governance %s, no adjustment needed", governance_mode)

        await _save_step_log(
            analysis_id, "6ba_position_governance", "position_governance",
            input_data={
                "mode": governance_mode,
                "target_weights_raw": target_before_governance,
                "current_weights": brief.get("current_weights") or {},
                "strategy_use_summary": (evidence_bundle.get("strategies") or {}).get("strategy_use_summary"),
                "market_scorecard": market_scorecard,
            },
            output_data={
                "mode": governance_mode,
                "adjusted_weights": governance_out.adjusted_weights,
                "position_decisions": governance_out.position_decisions,
                "blocked_actions": governance_out.blocked_actions,
                "forced_trims": governance_out.forced_trims if run_governance_execution else [],
                "replacements": governance_out.replacements if run_governance_execution else [],
                "advisory_overrides": governance_out.advisory_overrides,
                "manual_action_hints": governance_out.manual_action_hints,
                "trade_summary": governance_out.trade_summary,
                "portfolio_summary": governance_out.portfolio_summary,
            },
            duration_ms=0,
        )

    # ── Stage 6.5b: Position Manager quantity/frequency controls ───────────────
    if risk_out.get("approved") and risk_out.get("target_weights"):
        target_before_pm = risk_out.get("target_weights") or {}
        actual_daily_trades = await count_today_actual_execution_actions()
        pm_config = _merge_position_style_config(
            pipeline_context.get("position_manager_config") or {},
            decision_style,
        )
        pm_out = apply_position_constraints(
            target_weights=target_before_pm,
            current_holdings=brief.get("current_weights") or {},
            config=pm_config,
            holdings_meta=brief.get("holdings") or [],
            actual_daily_trades=actual_daily_trades,
        )
        risk_out["target_weights"] = pm_out.adjusted_weights
        rebalance_threshold = float((pipeline_context.get("risk_params") or {}).get("rebalance_threshold", 0.02))
        rebalance_actions = compute_rebalance_actions(
            pm_out.adjusted_weights,
            brief.get("current_weights") or {},
            rebalance_threshold,
        )
        risk_out["rebalance_actions"] = rebalance_actions
        risk_out["estimated_cost_pct"] = estimate_cost_pct(rebalance_actions)
        risk_out["n_holdings"] = pm_out.trade_summary.get("position_count", risk_out.get("n_holdings"))
        risk_out["position_manager"] = {
            "violations": pm_out.violations,
            "trade_summary": pm_out.trade_summary,
            "constraints": pm_out.constraints,
        }

        if pm_out.violations:
            logger.warning(
                f"[Stage6.5] Position Manager adjusted weights | violations={pm_out.violations}"
            )
        else:
            logger.info("[Stage6.5] Position Manager passed, no adjustment needed")

        await _save_step_log(
            analysis_id, "6c_position_manager", "position_manager",
            input_data={
                "target_weights_raw": target_before_pm,
                "current_weights": brief.get("current_weights") or {},
                "config": pm_config,
                "actual_daily_trades": actual_daily_trades,
            },
            output_data={
                "adjusted_weights": pm_out.adjusted_weights,
                "violations": pm_out.violations,
                "trade_summary": pm_out.trade_summary,
            },
            duration_ms=0,
        )

    if risk_out.get("position_governance"):
        try:
            target_builder_out = build_target_weights(
                base_weights=base_weights,
                current_weights=brief.get("current_weights") or {},
                market_scorecard=market_scorecard,
                decision_style=decision_style,
                position_governance=risk_out.get("position_governance") or {},
                validated_advisory=(risk_out.get("position_governance") or {}).get("advisory_overrides") or [],
                constraints={
                    "max_turnover": (market_scorecard or {}).get("max_turnover_per_cycle"),
                    "max_single_delta": (market_scorecard or {}).get("max_adjustment_from_base"),
                },
            ).to_dict()
            target_builder_diff = compare_target_weights(
                live_target_weights=risk_out.get("target_weights") or {},
                shadow_target_weights=target_builder_out.get("target_weights") or {},
            )
            risk_out["target_builder_shadow"] = {
                **target_builder_out,
                "live_target_diff": target_builder_diff,
            }
            if target_builder_diff.get("requires_review"):
                logger.warning(
                    "[Stage6bb] Target Builder shadow diff requires review | max_diff=%s turnover_diff=%s",
                    target_builder_diff.get("max_abs_diff"),
                    target_builder_diff.get("aggregate_turnover_diff"),
                )
            else:
                logger.info("[Stage6bb] Target Builder shadow completed, no material diff")
            await _save_step_log(
                analysis_id, "6bb_target_builder_shadow", "target_builder",
                input_data={
                    "mode": "shadow",
                    "base_weights": base_weights,
                    "current_weights": brief.get("current_weights") or {},
                    "market_scorecard": market_scorecard,
                    "decision_style": decision_style,
                    "position_governance_available": True,
                },
                output_data=risk_out["target_builder_shadow"],
                duration_ms=0,
            )
        except Exception as e:
            logger.warning("[pipeline] target builder shadow failed: %s", e)
            risk_out["target_builder_shadow_error"] = str(e)
            await _save_step_log(
                analysis_id, "6bb_target_builder_shadow", "target_builder",
                input_data={
                    "mode": "shadow",
                    "base_weights": base_weights,
                    "current_weights": brief.get("current_weights") or {},
                },
                output_data={"error": str(e)},
                duration_ms=0,
                failed=True,
            )

    try:
        decision_ledger = build_decision_ledger(
            evidence_bundle=evidence_bundle,
            market_scorecard=market_scorecard,
            strategy_output={
                "base_weights": base_weights,
                "strategy_target_weights": (playground_bundle or {}).get("consensus_weights") or {},
                "strategies": evidence_bundle.get("strategies") or {},
            },
            synthesizer_output=synthesizer_out,
            risk_output=risk_out,
            position_governance=risk_out.get("position_governance"),
            execution_audit=None,
            current_holdings={
                "current_weights": brief.get("current_weights") or {},
                "holdings": brief.get("holdings") or [],
            },
        )
        risk_out["decision_ledger"] = decision_ledger
        await _save_step_log(
            analysis_id, "6d_decision_ledger", "decision_ledger",
            input_data={
                "risk_approved": bool(risk_out.get("approved")),
                "has_position_governance": bool(risk_out.get("position_governance")),
                "current_weight_count": len(brief.get("current_weights") or {}),
            },
            output_data=decision_ledger,
            duration_ms=0,
        )
    except Exception as e:
        logger.warning("[pipeline] decision ledger build failed: %s", e)
        risk_out["decision_ledger_error"] = str(e)
        await _save_step_log(
            analysis_id, "6d_decision_ledger", "decision_ledger",
            input_data={
                "risk_approved": bool(risk_out.get("approved")),
                "has_position_governance": bool(risk_out.get("position_governance")),
            },
            output_data={"error": str(e)},
            duration_ms=0,
            failed=True,
        )

    risk_out_for_tracker = risk_out

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
    tracker.log_stage_metrics("8_communicator", dur_comm,
        used_fallback=comm_out.get("used_fallback", False))

    validation_out = validate_decision_live_artifacts(
        stage_outputs={
            "2d_evidence_scorecard": {
                "evidence_bundle": evidence_bundle,
                "market_scorecard": market_scorecard,
                "news_evidence": news_evidence,
            },
            "5_synthesizer": synthesizer_out,
            "5d_proposal_shaper": synthesizer_out.get("proposal_shaping") or {},
            "6_risk_mgr": risk_out,
            "6ba_position_governance": risk_out.get("position_governance") or {},
            "6d_decision_ledger": risk_out.get("decision_ledger") or {},
            "8_communicator": comm_out,
        }
    )
    risk_out["live_validation"] = validation_out
    await _save_step_log(
        analysis_id, "8b_live_validation", "decision_live_validation",
        input_data={"analysis_id": analysis_id, "communicator_used_fallback": comm_out.get("used_fallback", False)},
        output_data=validation_out,
        duration_ms=0,
        failed=validation_out.get("overall") == "fail",
    )
    await _update_analysis_risk_output(analysis_id, risk_out)
    if validation_out.get("overall") in {"warn", "fail"}:
        comm_out["text"] = _append_live_validation_report(comm_out.get("text") or "", validation_out)

    # Phase 3: Record LLM failure for circuit breaker monitoring (communicator uses LLM + fallback)
    if comm_out.get("used_fallback", False):
        from services.circuit_breaker import record_stage_failure
        await record_stage_failure("communicator")
        tracker.log_retry_event("communicator", 1, "degraded_fallback")

    # Stage 9: Branch execution
    auth_mode = pipeline_context["auth_mode"]

    if not approved:
        should_notify, suppress_reason = await _should_notify_rejected_pipeline(
            risk_out=risk_out,
            synthesizer_out=synthesizer_out,
            pipeline_context=pipeline_context,
        )
        if should_notify:
            await tool_send_telegram({"text": remove_command_hints(comm_out["text"])})
            logger.info("Risk rejected — notified and stopping")
        else:
            logger.info("Risk rejected — duplicate notification suppressed: %s", suppress_reason)
        pipeline_status = "rejected_by_risk"
    elif auth_mode == "SEMI_AUTO":
        await _send_semi_auto_proposal(pipeline_context, risk_out, comm_out, analysis_id)
        pipeline_status = "semi_auto_pending"
    elif auth_mode == "FULL_AUTO":
        # P2-1: Validate proposal still relevant before execution
        from db.queries import get_latest_portfolio
        async with AsyncSessionLocal() as db:
            latest_portfolio = await get_latest_portfolio(db)
        pending_for_val = {
            "analysis_id": analysis_id,
            "weights": risk_out.get("target_weights", {}),
            "token": risk_out.get("approval_token", ""),
        }
        valid, reason = await validate_proposal_still_relevant(pending_for_val, latest_portfolio)
        if not valid:
            logger.warning(f"[pipeline] FULL_AUTO blocked by proposal invalidation: {reason}")
            await tool_send_telegram(
                {"text": f"⚠️ FULL_AUTO skipped ({reason}); market state changed"}
            )
            pipeline_status = f"skipped_invalidation_{reason}"
        else:
            # Phase 3: Re-check circuit state — could have escalated during pipeline run
            circuit = pipeline_context.get("circuit_state", "CLOSED")
            if circuit in ("ALERT", "DEFENSIVE"):
                # Circuit opened mid-pipeline — store as pending, alert human
                await _send_semi_auto_proposal(pipeline_context, risk_out, comm_out, analysis_id)
                pipeline_status = f"full_auto_circuit_{circuit.lower()}"
                emoji = "🟡" if circuit == "ALERT" else "🔴"
                await tool_send_telegram({
                    "text": (
                        f"{emoji} FULL_AUTO: Circuit={circuit} opened during pipeline run. "
                        f"Proposal stored as pending. "
                        f"Reply /confirm to execute or /reset_circuit once resolved."
                    )
                })
                logger.warning(f"[pipeline] FULL_AUTO: circuit opened mid-pipeline, stored pending")
            else:
                result = await run_executor_async(pipeline_context, risk_out, analysis_id)
                await _save_execution(analysis_id, result)
                logger.info(f"FULL_AUTO execution: {result.get('execution_status')}")
                pipeline_status = result.get("execution_status", "unknown")
    else:
        pipeline_status = "unknown_auth_mode"

    if synthesizer_out_for_tracker and risk_out_for_tracker:
        tracker.log_final_decision(synthesizer_out_for_tracker, risk_out_for_tracker)
    tracker.end_run(pipeline_status)
    return {"status": pipeline_status, "analysis_id": analysis_id}


# ─────────────────────────────── SEMI_AUTO Proposal ───────────────────────────────


async def _should_notify_rejected_pipeline(
    *,
    risk_out: dict,
    synthesizer_out: dict,
    pipeline_context: dict,
    cooldown_minutes: int = REJECTED_NOTIFICATION_COOLDOWN_MINUTES,
) -> tuple[bool, str | None]:
    fingerprint = _rejected_pipeline_fingerprint(risk_out, synthesizer_out, pipeline_context)
    now = datetime.utcnow()
    async with AsyncSessionLocal() as db:
        cfg = await get_system_config(db, REJECTED_NOTIFICATION_STATE_KEY)
        previous = (cfg.value if cfg else {}) or {}
        previous_fingerprint = previous.get("fingerprint")
        previous_at_raw = previous.get("notified_at")
        previous_at = None
        if previous_at_raw:
            try:
                previous_at = datetime.fromisoformat(previous_at_raw)
            except (TypeError, ValueError):
                previous_at = None

        if previous_fingerprint == fingerprint and previous_at is not None:
            age = now - previous_at
            if age < timedelta(minutes=cooldown_minutes):
                return False, f"same rejected proposal fingerprint within {cooldown_minutes}m"

        await upsert_system_config(db, REJECTED_NOTIFICATION_STATE_KEY, {
            "fingerprint": fingerprint,
            "notified_at": now.isoformat(),
            "cooldown_minutes": cooldown_minutes,
        }, "pipeline")
    return True, None


def _rejected_pipeline_fingerprint(
    risk_out: dict,
    synthesizer_out: dict,
    pipeline_context: dict,
) -> str:
    scorecard = pipeline_context.get("market_scorecard") or {}
    strategy_use = (
        synthesizer_out.get("strategy_use_enforcement")
        or pipeline_context.get("strategy_use_enforcement")
        or {}
    )
    payload = {
        "rejection_reasons": sorted(str(item) for item in (risk_out.get("rejection_reasons") or [])[:8]),
        "rebalance_actions": [
            {
                "ticker": str(action.get("ticker")),
                "action": str(action.get("action")),
                "weight_delta": round(float(action.get("weight_delta") or 0.0), 4),
            }
            for action in (risk_out.get("rebalance_actions") or [])[:10]
            if isinstance(action, dict)
        ],
        "scorecard": {
            "condition": scorecard.get("market_condition"),
            "permission": scorecard.get("investment_permission"),
            "dominant": scorecard.get("dominant_constraint"),
            "human": bool(scorecard.get("require_human_confirmation")),
        },
        "style_violations": sorted(
            str(item)
            for item in ((risk_out.get("style_enforcement") or {}).get("violations") or [])[:8]
        ),
        "strategy_use_violations": sorted(
            str(item)
            for item in (strategy_use.get("violations") or strategy_use.get("clip_log") or [])[:8]
        ),
    }
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


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

    # Store current portfolio value for P2-1 proposal invalidation
    proposal_value = None
    if risk_out.get("target_weights"):
        from db.queries import get_latest_portfolio
        async with AsyncSessionLocal() as db:
            latest = await get_latest_portfolio(db)
        if latest and latest.total_value:
            proposal_value = float(latest.total_value)

    await save_pending_proposal({
        "analysis_id":        analysis_id,
        "weights":            weights,
        "token":              token,
        "expires_at":         expires.isoformat(),
        "status":             "pending",
        "estimated_cost_pct": cost,
        "proposal_value":     proposal_value,
    })

    await _save_execution(
        analysis_id,
        {
            "execution_status": "proposed",
            "execution_audit": build_execution_audit_payload(
                action_status="proposed",
                proposed_weights=weights,
                rebalance_actions=risk_out.get("rebalance_actions") or [],
                estimated_cost_pct=cost,
                reason="semi_auto_pending_confirmation",
            ),
        },
    )

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


def _merge_position_style_config(base_config: dict, decision_style: dict | None) -> dict:
    """Convert decision style limits into Position Manager caps without loosening config."""
    merged = dict(base_config or {})
    limits = (decision_style or {}).get("style_limits") or {}
    min_keys = {
        "max_new_buys_per_cycle": int,
        "max_single_trade_pct": float,
        "max_turnover_per_cycle": float,
    }
    for key, caster in min_keys.items():
        if key not in limits:
            continue
        try:
            style_value = caster(limits[key])
        except (TypeError, ValueError):
            continue
        if key not in merged:
            merged[key] = style_value
            continue
        try:
            merged[key] = min(caster(merged[key]), style_value)
        except (TypeError, ValueError):
            merged[key] = style_value
    if limits.get("allow_new_positions") is False:
        merged["max_new_buys_per_cycle"] = 0
    return merged


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


async def _update_analysis_risk_output(analysis_id: int, risk_out: dict) -> None:
    """Persist post-communicator audit fields without changing execution status."""
    from sqlalchemy import update
    try:
        async with AsyncSessionLocal() as db:
            await db.execute(
                update(AgentAnalysis)
                .where(AgentAnalysis.id == analysis_id)
                .values(risk_output=risk_out)
            )
            await db.commit()
    except Exception as e:
        logger.warning("[pipeline] failed to persist updated risk_output: %s", e)


def _append_live_validation_report(text: str, validation_out: dict) -> str:
    report = format_decision_live_validation_report(validation_out)
    if not report:
        return text
    return f"{text.rstrip()}\n\n{report}".strip()


async def _save_execution(analysis_id: int, result: dict) -> None:
    audit_payload = result.get("execution_audit") or build_execution_audit_payload(
        action_status=result.get("execution_status", "failed"),
        sent_weights=result.get("weights_sent") or {},
        command_id=result.get("command_id"),
        reason=result.get("error"),
    )
    async with AsyncSessionLocal() as db:
        db.add(ExecutionLog(
            analysis_id     = analysis_id,
            command_type    = "weight_adjustment",
            command_payload = audit_payload,
            qc_response     = result.get("qc_response"),
            status          = result.get("execution_status", "unknown"),
        ))
        analysis = await db.get(AgentAnalysis, analysis_id)
        if analysis:
            risk_output = dict(analysis.risk_output or {})
            if risk_output.get("decision_ledger"):
                risk_output["decision_ledger"] = apply_execution_audit_to_decision_ledger(
                    risk_output.get("decision_ledger") or {},
                    audit_payload,
                )
                analysis.risk_output = risk_output
        await db.commit()
