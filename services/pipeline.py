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

News data is refreshed independently every 2h, 24/7 by cron/pre_fetch_news.py.
Trading-analysis entrypoints require a fresh news cache before entering this pipeline.
Both crons still fail independently: pipeline down -> news continues refreshing.
"""
import asyncio
import hashlib
import json
import logging
import time
from datetime import datetime, timedelta
from typing import Any

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
from services.portfolio_construction import (
    PortfolioConstructionModel,
    build_construction_alpha_decision_context,
    build_construction_signal_strengths,
)
from services.portfolio_construction_gate import construction_input_for_target_builder
from services.pc_promotion_config import default_pc_promotion_config
from services.decision_ledger import (
    apply_execution_audit_to_decision_ledger,
    build_decision_ledger,
)
from services.decision_degradation import build_decision_degradation_report
from services.target_builder import build_target_weights, compare_target_weights
from services.decision_live_validation import (
    format_decision_live_validation_report,
    validate_decision_live_artifacts,
)
from services.strategy_use_constraints import apply_strategy_use_constraints
from services.proposal_shaper import shape_proposal_before_risk
from services.position_governance import apply_position_governance
from services.execution_policy import policy_snapshot
from services.final_execution_policy_cap import apply_final_execution_policy_cap
from services.execution_throttle import apply_execution_throttle
from services.deferred_execution_ledger import record_deferred_execution_plan
from services.final_risk_validation import validate_final_execution_target
from services.final_risk_validation_config import (
    default_final_risk_validation_config,
    resolve_final_risk_validation_mode,
)
from services.full_auto_safety import full_auto_safety_precondition_violations
from services.account_state_guard import (
    account_state_guard_pipeline_effect,
    default_account_state_guard_config,
    load_latest_account_state_guard,
)
from services.auto_pause import (
    apply_auto_pause_if_needed,
    default_auto_pause_config,
    load_auto_pause_verdict,
)
from services.execution_lifecycle import (
    default_execution_lifecycle_config,
    is_reduce_only_vs_actual,
)
from services.reconciliation_guard import (
    default_reconciliation_guard_config,
    format_reconciliation_guard_alert,
    load_reconciliation_guard,
)
from services.policy_sync_recovery import (
    default_policy_sync_recovery_config,
    run_policy_sync_recovery,
)
from services.transaction_cost_gate import (
    default_transaction_cost_gate_config,
    evaluate_transaction_cost_gate,
)
from services.weight_ops import normalize_cash_first
from services.target_envelope import TargetEnvelope
from services.target_envelope import default_target_envelope_config
from services.evidence_cap_config import default_evidence_cap_config
from services.alpha_decision_policy import default_alpha_decision_policy_config
from services.strategy_certification import default_strategy_execution_evidence_config
from services.strategy_execution_evidence_sources import (
    disabled_paper_live_outcome_metrics,
    load_paper_live_outcome_metrics,
)
from services.strategy_execution_evidence_notifications import (
    notify_strategy_execution_evidence_certification,
)
from services.portfolio_risk_diagnostic import load_portfolio_var_cvar_diagnostic
from services.alpha_validation_persistence import persist_alpha_validation_run
from services.validation_observation_loop import persist_observations_for_analysis
from services.diagnostic_artifacts import (
    append_diagnostic_artifacts,
    build_pipeline_diagnostic_artifacts,
)
from services.operator_halt import normalize_operator_halt_state
from services.mutation_ownership import (
    REGIME_CONSTRAINT_MUTATION_TYPE,
    legacy_mutation_classification_summary,
)
from services.mutation_ledger import MutationLedger
from services.empirical_profile_store import (
    build_empirical_profiles_from_feature_store,
    collect_empirical_profile_tickers,
)
from services.execution_audit import build_execution_audit_payload, count_today_actual_execution_actions
from services.json_safety import json_safe as _json_safe
from strategies              import compute_rebalance_actions, estimate_cost_pct
from tracking.monitor_client import PipelineRunTracker
from db.session          import AsyncSessionLocal
from db.queries          import get_system_config, upsert_system_config
from db.models           import AgentAnalysis, AgentStepLog
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
    4. Fill remaining weight with CASH after clip; do not amplify risk weights
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

    normalized, _ = normalize_cash_first(clipped)

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

    normalized, _ = normalize_cash_first(clipped)

    return normalized, clip_log


# ─────────────────────────────── Regime Constraint Validation ───────────────────────────────


HEDGE_TICKERS = {
    "GLD", "TLT", "BND", "IEF",
    "TQQQ", "SQQQ", "SOXL", "SOXS", "SPXL", "SPXS", "UVXY", "VIXY",
    "SH", "PSQ", "RWM", "DOG", "MYY", "SBB", "SEF", "REK", "EUM", "EFZ", "YXI",
    "SJB", "TBF", "TBX",
}


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
                    released = max(float(w) - float(hard_cap), 0.0)
                    violations.append(
                        f"new_pos {ticker}: {w:.3f}→{hard_cap:.3f} (new pos blocked in {regime_result.get('regime')})"
                    )
                    working[ticker] = hard_cap
                    working["CASH"] = round(working.get("CASH", 0.0) + released, 6)

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

    working, _ = normalize_cash_first(working)

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
        operator_halt_cfg = await get_system_config(db, "operator_halt_state")
        circuit_cfg     = await get_system_config(db, "circuit_state")
        active_cfg      = await get_system_config(db, "active_strategy")
        alerts_cfg      = await get_system_config(db, "pending_critical_alerts")
        pm_cfg          = await get_system_config(db, "position_manager_config")
        pg_cfg          = await get_system_config(db, "position_governance_config")
        pc_promo_cfg    = await get_system_config(db, "portfolio_construction_promotion_config")
        final_validation_cfg = await get_system_config(db, "final_risk_validation_config")
        execution_command_cfg = await get_system_config(db, "execution_command_config")
        account_guard_cfg = await get_system_config(db, "account_state_guard_config")
        reconciliation_guard_cfg = await get_system_config(db, "reconciliation_guard_config")
        auto_pause_cfg    = await get_system_config(db, "auto_pause_config")
        execution_lifecycle_cfg = await get_system_config(db, "execution_lifecycle_config")
        policy_sync_recovery_cfg = await get_system_config(db, "policy_sync_recovery_config")
        transaction_cost_cfg = await get_system_config(db, "transaction_cost_gate_config")
        evidence_cap_cfg = await get_system_config(db, "evidence_cap_config")
        alpha_decision_policy_cfg = await get_system_config(db, "alpha_decision_policy_config")
        strategy_execution_evidence_cfg = await get_system_config(db, "strategy_execution_evidence_config")
        target_envelope_cfg = await get_system_config(db, "target_envelope_config")
        override_cfg    = await get_system_config(db, "circuit_override")
        alert_cfg       = await get_system_config(db, "circuit_pause_alert")

    paused = bool((paused_cfg.value if paused_cfg else {}).get("paused", False))
    if paused:
        logger.info("trading_paused=True — pipeline skipped")
        return None

    operator_halt_state = normalize_operator_halt_state(
        operator_halt_cfg.value if operator_halt_cfg else None
    )
    if operator_halt_state.get("halted"):
        logger.warning(
            "operator_halt_state halted — pipeline skipped | reason=%s fail_safe=%s",
            operator_halt_state.get("reason"),
            operator_halt_state.get("fail_safe"),
        )
        return None

    auth_mode = (auth_cfg.value if auth_cfg else {"value": "SEMI_AUTO"}).get("value", "SEMI_AUTO")
    if auth_mode == "MANUAL":
        logger.info("MANUAL mode — pipeline skipped")
        return None

    risk_params = (risk_cfg.value if risk_cfg else {}) or {}

    # ── Phase 3: Circuit state — circuit is already evaluated at pipeline entry ─
    # circuit_cfg.value may be stale; we use it as fallback only
    circuit = (circuit_cfg.value if circuit_cfg else {"value": "CLOSED"}).get("value", "CLOSED")
    circuit_override_consumed = False

    # Phase 3: In FULL_AUTO with circuit ALERT/DEFENSIVE, alert instead of running
    if auth_mode == "FULL_AUTO" and circuit in ("ALERT", "DEFENSIVE"):
        circuit_override_consumed = await _consume_circuit_override(circuit, override_cfg)
        if circuit_override_consumed:
            logger.warning(f"[pipeline] FULL_AUTO circuit override consumed for circuit={circuit}")
        else:
            await _send_circuit_pause_alert_if_due(circuit, alert_cfg)
            logger.warning(f"[pipeline] FULL_AUTO blocked by circuit={circuit}")
            return None

    active_name = (active_cfg.value if active_cfg else {"value": "momentum_lite_v1"}).get(
        "value", "momentum_lite_v1"
    )
    pending_alerts = (alerts_cfg.value if alerts_cfg else {}).get("alerts", []) or []
    position_manager_config = (pm_cfg.value if pm_cfg else {}) or {}
    position_governance_config = (pg_cfg.value if pg_cfg else {}) or {}
    portfolio_construction_promotion_config = default_pc_promotion_config(
        (pc_promo_cfg.value if pc_promo_cfg else {}) or {}
    )
    final_risk_validation_config = default_final_risk_validation_config(
        (final_validation_cfg.value if final_validation_cfg else {}) or {}
    )
    execution_command_config = (execution_command_cfg.value if execution_command_cfg else {}) or {}
    account_state_guard_config = default_account_state_guard_config(
        (account_guard_cfg.value if account_guard_cfg else {}) or {}
    )
    account_state_guard_config["configured_mode"] = account_state_guard_config.get("mode")
    if auth_mode == "SEMI_AUTO":
        configured_mode = str(account_state_guard_config.get("configured_mode") or "observe")
        account_state_guard_config["mode"] = "blocking"
        account_state_guard_config["semi_auto_effective_mode"] = "blocking"
        account_state_guard_config["mode_forced_reason"] = (
            "semi_auto_requires_fresh_account_truth"
            if configured_mode != "blocking"
            else None
        )
    account_state_guard_config["expected_policy_version"] = str(policy_snapshot().get("version") or "")
    auto_pause_config = default_auto_pause_config(
        (auto_pause_cfg.value if auto_pause_cfg else {}) or {}
    )
    execution_lifecycle_config = default_execution_lifecycle_config(
        (execution_lifecycle_cfg.value if execution_lifecycle_cfg else {}) or {}
    )
    reconciliation_guard_config = default_reconciliation_guard_config(
        (reconciliation_guard_cfg.value if reconciliation_guard_cfg else {}) or {}
    )
    policy_sync_recovery_config = default_policy_sync_recovery_config(
        (policy_sync_recovery_cfg.value if policy_sync_recovery_cfg else {}) or {}
    )
    transaction_cost_gate_config = default_transaction_cost_gate_config(
        (transaction_cost_cfg.value if transaction_cost_cfg else {}) or {}
    )
    evidence_cap_config = default_evidence_cap_config(
        (evidence_cap_cfg.value if evidence_cap_cfg else {}) or {}
    )
    alpha_decision_policy_config = default_alpha_decision_policy_config(
        (alpha_decision_policy_cfg.value if alpha_decision_policy_cfg else {}) or {}
    )
    strategy_execution_evidence_config = default_strategy_execution_evidence_config(
        (strategy_execution_evidence_cfg.value if strategy_execution_evidence_cfg else {}) or {}
    )
    target_envelope_config = default_target_envelope_config(
        (target_envelope_cfg.value if target_envelope_cfg else {}) or {}
    )

    full_auto_safety_violations = full_auto_safety_precondition_violations(
        auth_mode=auth_mode,
        account_state_guard_config=account_state_guard_config,
        final_risk_validation_config=final_risk_validation_config,
        auto_pause_config=auto_pause_config,
        execution_lifecycle_config=execution_lifecycle_config,
        reconciliation_guard_config=reconciliation_guard_config,
    )
    if full_auto_safety_violations:
        message = (
            "⛔ FULL_AUTO configuration rejected before pipeline execution\n"
            "FULL_AUTO requires code-enforced safety layers, not observe-only diagnostics.\n"
            + "\n".join(f"- {item}" for item in full_auto_safety_violations)
            + "\nDowngrade to SEMI_AUTO or fix configuration."
        )
        logger.error("[pipeline] FULL_AUTO safety preconditions failed: %s", full_auto_safety_violations)
        await tool_send_telegram({"text": message})
        return None

    params_key = f"strategy_{active_name}_params"
    async with AsyncSessionLocal() as db:
        params_cfg = await get_system_config(db, params_key)
    strategy_params = (params_cfg.value if params_cfg else {}) or {}

    override_mode = "DEFENSIVE" if circuit in ("ALERT", "DEFENSIVE") else None

    return {
        "trigger":           trigger,
        "plan_id":           f"P-{datetime.utcnow().strftime('%Y%m%d-%H%M')}",
        "auth_mode":         auth_mode,
        "operator_halt_state": operator_halt_state,
        "circuit_state":     circuit,
        "circuit_override_consumed": circuit_override_consumed,
        "override_mode":     override_mode,
        "risk_params":       risk_params,
        "active_strategy":   active_name,
        "strategy_params":   strategy_params,
        "pending_alerts":    pending_alerts,
        "position_manager_config": position_manager_config,
        "position_governance_config": position_governance_config,
        "portfolio_construction_promotion_config": portfolio_construction_promotion_config,
        "final_risk_validation_config": final_risk_validation_config,
        "execution_command_config": execution_command_config,
        "account_state_guard_config": account_state_guard_config,
        "auto_pause_config": auto_pause_config,
        "execution_lifecycle_config": execution_lifecycle_config,
        "reconciliation_guard_config": reconciliation_guard_config,
        "policy_sync_recovery_config": policy_sync_recovery_config,
        "transaction_cost_gate_config": transaction_cost_gate_config,
        "evidence_cap_config": evidence_cap_config,
        "alpha_decision_policy_config": alpha_decision_policy_config,
        "strategy_execution_evidence_config": strategy_execution_evidence_config,
        "target_envelope_config": target_envelope_config,
    }


async def _consume_circuit_override(circuit: str, override_cfg) -> bool:
    value = (override_cfg.value if override_cfg else {}) or {}
    if value.get("value") != "ONE_SHOT":
        return False
    if str(value.get("circuit_state") or "") != circuit:
        return False
    if int(value.get("uses_remaining") or 0) <= 0:
        return False
    expires_at = _parse_iso_datetime(value.get("expires_at"))
    if expires_at and expires_at < datetime.utcnow():
        return False

    updated = dict(value)
    updated["uses_remaining"] = 0
    updated["consumed_at"] = datetime.utcnow().isoformat()
    async with AsyncSessionLocal() as db:
        await upsert_system_config(db, "circuit_override", updated, "pipeline")
    return True


async def _send_circuit_pause_alert_if_due(circuit: str, alert_cfg) -> None:
    value = (alert_cfg.value if alert_cfg else {}) or {}
    now = datetime.utcnow()
    last_sent = _parse_iso_datetime(value.get("last_sent_at"))
    last_circuit = str(value.get("circuit_state") or "")
    if last_sent and last_circuit == circuit and now - last_sent < timedelta(minutes=30):
        return

    emoji = "🟡" if circuit == "ALERT" else "🔴"
    await tool_send_telegram({
        "text": (
            f"{emoji} FULL_AUTO: Circuit={circuit} is open. "
            f"Pipeline paused for {circuit}. "
            f"Reply /confirm to override the next run or /reset_circuit once resolved."
        )
    })
    async with AsyncSessionLocal() as db:
        await upsert_system_config(
            db,
            "circuit_pause_alert",
            {
                "circuit_state": circuit,
                "last_sent_at": now.isoformat(),
                "min_interval_minutes": 30,
            },
            "pipeline",
        )


def _parse_iso_datetime(value: object) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError:
        return None


def _news_evidence_audit_summary(news_evidence: dict | None) -> dict:
    """Small step-log summary showing how news evidence entered analysis."""
    evidence = news_evidence or {}
    macro = evidence.get("macro_news_score") or {}
    ticker_scores = evidence.get("ticker_news_scores") or {}
    hard_risks = evidence.get("hard_risk_events") or {}
    return {
        "macro_bias": macro.get("overall_bias"),
        "macro_confidence": macro.get("confidence"),
        "macro_impact": macro.get("market_impact"),
        "macro_data_quality": macro.get("data_quality"),
        "ticker_news_score_count": len(ticker_scores),
        "hard_risk_tickers": sorted(str(ticker) for ticker in hard_risks.keys())[:20],
        "data_gaps": list(evidence.get("data_gaps") or [])[:8],
        "ignored_item_count": len(evidence.get("ignored_items") or []),
    }


def _news_context_audit_summary(brief: dict | None) -> dict:
    """Small step-log summary for raw news context without duplicating article text."""
    payload = brief or {}
    context = payload.get("news_context") or {}
    per_ticker_news = payload.get("per_ticker_news") or {}
    ticker_signals = context.get("ticker_signals") or {}
    return {
        "macro_signal_count": len(context.get("macro_signals") or []),
        "ticker_signal_count": len(ticker_signals),
        "per_ticker_news_ticker_count": len(per_ticker_news),
        "per_ticker_news_item_count": sum(len(items or []) for items in per_ticker_news.values()),
        "hard_risk_ticker_count": len(payload.get("hard_risks_map") or {}),
        "stale_warning": context.get("_stale_warning"),
        "fallback": bool(context.get("_fallback")),
        "data_gaps": list(context.get("data_gaps") or [])[:8],
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
        safe_input_data = _json_safe(input_data or {})
        safe_output_data = _json_safe(output_data or {})
        token_usage = None
        if isinstance(safe_output_data, dict) and isinstance(safe_output_data.get("_token_usage"), dict):
            token_usage = safe_output_data.get("_token_usage")
        async with AsyncSessionLocal() as db:
            db.add(AgentStepLog(
                analysis_id = analysis_id,
                stage       = stage,
                agent_name  = agent_name,
                input_data  = safe_input_data,
                output_data = safe_output_data,
                duration_ms = duration_ms,
                model       = model,
                token_usage = token_usage,
                failed      = failed,
            ))
            await db.commit()
    except Exception as e:
        logger.warning(f"Failed to save step log for {stage}: {e}")


# ─────────────────────────────── Main Entry ───────────────────────────────


async def run_full_pipeline(trigger: str = "scheduled_hourly", *, require_trading_gate: bool = True) -> dict:
    """Run full agent pipeline."""
    logger.info(f"=== Pipeline START | trigger={trigger} ===")
    trading_analysis_gate: dict[str, Any] | None = None

    if require_trading_gate:
        try:
            from services.trading_analysis_gate import evaluate_trading_analysis_gate

            gate = await evaluate_trading_analysis_gate(require_market_open=True)
            trading_analysis_gate = gate
        except Exception as gate_error:
            logger.exception("[trading_analysis_gate] unavailable; skipping pipeline")
            return {
                "status": "skipped_trading_analysis_gate",
                "trading_analysis_gate": {
                    "allowed": False,
                    "reason": "trading_analysis_gate_unavailable",
                    "error": str(gate_error),
                },
            }
        if not gate.get("allowed"):
            logger.warning("[trading_analysis_gate] skipped pipeline | reason=%s", gate.get("reason"))
            return {
                "status": "skipped_trading_analysis_gate",
                "trading_analysis_gate": gate,
            }

    if not await _acquire_pipeline_lock():
        logger.warning("Pipeline lock held by another instance — skipped")
        return {"status": "skipped_concurrent"}

    try:
        return await _run_pipeline_inner(trigger, trading_analysis_gate=trading_analysis_gate)
    finally:
        await _release_pipeline_lock()


async def _run_pipeline_inner(trigger: str, *, trading_analysis_gate: dict[str, Any] | None = None) -> dict:
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
    if trading_analysis_gate:
        pipeline_context["trading_analysis_gate"] = trading_analysis_gate
        if trading_analysis_gate.get("news_degraded_mode"):
            pipeline_context["news_degraded_mode"] = {
                "enabled": True,
                "mode": trading_analysis_gate.get("degraded_mode") or "news_stale_reduce_only",
                "reason": trading_analysis_gate.get("reason"),
                "news_cache": trading_analysis_gate.get("news_cache") or {},
                "risk_increase_allowed": bool(trading_analysis_gate.get("risk_increase_allowed")),
                "reduce_only_allowed": bool(trading_analysis_gate.get("reduce_only_allowed", True)),
            }

    try:
        account_state_guard = await load_latest_account_state_guard(
            config=pipeline_context.get("account_state_guard_config") or {}
        )
    except Exception as account_guard_error:
        account_state_guard = {
            "enabled": True,
            "mode": "observe",
            "status": "unavailable",
            "allowed": True,
            "would_block": True,
            "execution_effect": "diagnostic_only",
            "blockers": ["account_state_guard_unavailable"],
            "warnings": [str(account_guard_error)],
            "checks": {},
            "snapshot": None,
        }
        logger.warning("[account_state_guard] unavailable: %s", account_guard_error)
    account_guard_effect = account_state_guard_pipeline_effect(account_state_guard)
    account_state_guard.update(account_guard_effect)
    pipeline_context["account_state_guard"] = account_state_guard
    if account_state_guard.get("would_block"):
        logger.warning(
            "[account_state_guard] %s would_block | blockers=%s",
            account_state_guard.get("pipeline_enforcement"),
            account_state_guard.get("blockers") or [],
        )

    try:
        policy_sync_recovery = await run_policy_sync_recovery(
            account_guard_result=account_state_guard,
            config=pipeline_context.get("policy_sync_recovery_config") or {},
        )
    except Exception as policy_sync_recovery_error:
        policy_sync_recovery = {
            "enabled": True,
            "status": "unavailable",
            "action": "none",
            "reason": "policy_sync_recovery_unavailable",
            "trading_blocked": False,
            "warnings": [str(policy_sync_recovery_error)],
        }
        logger.warning("[policy_sync_recovery] unavailable: %s", policy_sync_recovery_error)
    pipeline_context["policy_sync_recovery"] = policy_sync_recovery
    if policy_sync_recovery.get("status") in {"recoverable", "unrecoverable"}:
        logger.warning(
            "[policy_sync_recovery] status=%s action=%s reason=%s",
            policy_sync_recovery.get("status"),
            policy_sync_recovery.get("action"),
            policy_sync_recovery.get("reason"),
        )

    try:
        auto_pause = await load_auto_pause_verdict(
            config=pipeline_context.get("auto_pause_config") or {},
            account_state_guard=account_state_guard,
            policy_sync_recovery=policy_sync_recovery,
        )
    except Exception as auto_pause_error:
        auto_pause = {
            "enabled": True,
            "mode": "observe",
            "status": "unavailable",
            "would_pause": False,
            "should_pause": False,
            "execution_effect": "diagnostic_only",
            "primary_trigger": None,
            "reason": None,
            "triggers": [],
            "warnings": [str(auto_pause_error)],
        }
        logger.warning("[auto_pause] unavailable: %s", auto_pause_error)
    pipeline_context["auto_pause"] = auto_pause
    if auto_pause.get("would_pause"):
        logger.warning(
            "[auto_pause] %s | primary=%s reason=%s",
            auto_pause.get("status"),
            auto_pause.get("primary_trigger"),
            auto_pause.get("reason"),
        )
    if auto_pause.get("should_pause"):
        await apply_auto_pause_if_needed(auto_pause)
        if pipeline_context.get("auth_mode") == "FULL_AUTO":
            tracker.end_run("skipped_auto_paused")
            return {"status": "skipped_auto_paused", "auto_pause": auto_pause}

    if (
        account_guard_effect.get("should_block_pipeline")
        and policy_sync_recovery.get("status") == "recoverable"
    ):
        tracker.end_run("skipped_policy_sync_recovery")
        return {
            "status": "skipped_policy_sync_recovery",
            "account_state_guard": account_state_guard,
            "policy_sync_recovery": policy_sync_recovery,
            "auto_pause": auto_pause,
        }

    if account_guard_effect.get("should_block_pipeline"):
        tracker.end_run("skipped_account_state_guard")
        return {
            "status": "skipped_account_state_guard",
            "account_state_guard": account_state_guard,
            "policy_sync_recovery": policy_sync_recovery,
            "auto_pause": auto_pause,
        }

    try:
        reconciliation_guard = await load_reconciliation_guard(
            config=pipeline_context.get("reconciliation_guard_config") or {},
            account_state_guard=account_state_guard,
        )
    except Exception as reconciliation_guard_error:
        reconciliation_guard = {
            "enabled": True,
            "mode": "blocking",
            "status": "unavailable",
            "reason": "reconciliation_guard_unavailable",
            "execution_effect": "blocking",
            "should_block_current_run": True,
            "should_set_reconciliation_halt": False,
            "warnings": [str(reconciliation_guard_error)],
        }
        logger.warning("[reconciliation_guard] unavailable: %s", reconciliation_guard_error)
    pipeline_context["reconciliation_guard"] = reconciliation_guard
    if reconciliation_guard.get("should_block_current_run"):
        logger.warning(
            "[reconciliation_guard] blocked current run | status=%s reason=%s",
            reconciliation_guard.get("status"),
            reconciliation_guard.get("reason"),
        )
        await tool_send_telegram({"text": format_reconciliation_guard_alert(reconciliation_guard)})
        tracker.end_run("skipped_reconciliation_guard")
        return {
            "status": "skipped_reconciliation_guard",
            "account_state_guard": account_state_guard,
            "reconciliation_guard": reconciliation_guard,
            "policy_sync_recovery": policy_sync_recovery,
            "auto_pause": auto_pause,
        }

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
    pipeline_context["brief"] = brief
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
        playground_bundle_obj = await run_playground(
            playground_brief,
            evidence_cap_config=pipeline_context.get("evidence_cap_config") or {},
        )
        playground_bundle = playground_bundle_obj.to_dict()
        strategy_evidence_cfg = pipeline_context.get("strategy_execution_evidence_config") or {}
        if (
            bool(strategy_evidence_cfg.get("enabled", True))
            and not bool(strategy_evidence_cfg.get("force_advisory_only", False))
            and bool(strategy_evidence_cfg.get("paper_live_outcome_evidence_enabled", True))
        ):
            try:
                async with AsyncSessionLocal() as db:
                    playground_bundle["paper_live_outcome_metrics"] = await load_paper_live_outcome_metrics(
                        db,
                        signal_source=str(
                            strategy_evidence_cfg.get("paper_live_signal_source")
                            or "fastapi_live_freeze"
                        ),
                        horizon_days=int(
                            strategy_evidence_cfg.get("paper_live_outcome_horizon_days")
                            or 1
                        ),
                        actions=strategy_evidence_cfg.get("paper_live_actions") or ["increase"],
                    )
            except Exception as e:
                logger.warning(
                    "Stage 2c paper-live execution evidence failed; continuing fail-closed: %s",
                    e,
                )
                playground_bundle["paper_live_outcome_metrics"] = disabled_paper_live_outcome_metrics(
                    f"load_failed:{e}"
                )
        else:
            playground_bundle["paper_live_outcome_metrics"] = disabled_paper_live_outcome_metrics(
                "disabled_by_strategy_execution_evidence_config"
            )
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
        strategy_execution_evidence_config=pipeline_context.get("strategy_execution_evidence_config") or {},
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
    pipeline_context["evidence_cap_diagnostics"] = (
        (evidence_bundle.get("strategies") or {}).get("evidence_cap_diagnostics") or {}
    )
    pipeline_context["feature_provenance"] = brief.get("feature_provenance") or {}
    pipeline_context["market_scorecard"] = market_scorecard
    pipeline_context["news_evidence"] = news_evidence
    pipeline_context["decision_style"] = decision_style
    news_evidence_summary = _news_evidence_audit_summary(news_evidence)
    news_context_summary = _news_context_audit_summary(brief)
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
            "news_context_summary": news_context_summary,
        },
        output_data={
            "evidence_bundle": evidence_bundle,
            "market_scorecard": market_scorecard,
            "news_evidence": news_evidence,
            "news_evidence_summary": news_evidence_summary,
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
        input_data={
            "base_weights": quant_baseline.get("base_weights"),
            "news_context_summary": news_context_summary,
            "news_evidence_summary": news_evidence_summary,
        },
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
        input_data={
            "base_weights": base_weights,
            "news_evidence_summary": news_evidence_summary,
            "decision_style_summary": {
                "analysis_style": decision_style.get("analysis_style"),
                "trade_style": decision_style.get("trade_style"),
                "dominant_style_constraint": decision_style.get("dominant_style_constraint"),
            },
        },
        output_data=bull_draft,
        duration_ms=dur_draft,
        model=model_heavy,
        failed=bull_draft.get("failed", False),
    )
    await _save_step_log(
        analysis_id, "4b_bear", "bear_researcher",
        input_data={
            "base_weights": base_weights,
            "news_evidence_summary": news_evidence_summary,
            "decision_style_summary": {
                "analysis_style": decision_style.get("analysis_style"),
                "trade_style": decision_style.get("trade_style"),
                "dominant_style_constraint": decision_style.get("dominant_style_constraint"),
            },
        },
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
            "news_evidence_summary": news_evidence_summary,
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
            "news_context_summary": news_context_summary,
            "news_evidence_summary": news_evidence_summary,
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
            try:
                pc_config = default_pc_promotion_config(
                    pipeline_context.get("portfolio_construction_promotion_config") or {}
                )
                pc_mode = str(pc_config.get("portfolio_construction_mode") or "shadow")
                pc_stage = (
                    "5e_portfolio_construction_candidate"
                    if pc_mode in {"candidate", "gated"}
                    else "5e_portfolio_construction_shadow"
                )
                signal_strengths = build_construction_signal_strengths(evidence_bundle)
                alpha_decision_context = build_construction_alpha_decision_context(
                    evidence_bundle,
                    policy_config=pipeline_context.get("alpha_decision_policy_config") or {},
                )
                portfolio_construction_payload = PortfolioConstructionModel().construct(
                    base_weights=base_weights,
                    current_weights=brief.get("current_weights") or {},
                    signal_strengths=signal_strengths,
                    alpha_decision_context=alpha_decision_context,
                    basket_reviews=(pre_risk_governance.portfolio_summary or {}).get("basket_reviews") or [],
                    scorecard_permission=(market_scorecard or {}).get("investment_permission"),
                    turnover_budget=_effective_portfolio_turnover_budget(market_scorecard, decision_style),
                ).to_dict()
                portfolio_construction_payload["portfolio_construction_mode"] = pc_mode
                portfolio_construction_payload.setdefault("diagnostics", {})
                portfolio_construction_payload["diagnostics"].update({
                    "runtime_mode": pc_mode,
                    "execution_effect": "diagnostic_only",
                    "target_builder_consumed": False,
                })
                pipeline_context["portfolio_construction_shadow"] = portfolio_construction_payload
                if pc_mode in {"candidate", "gated"}:
                    pipeline_context["portfolio_construction_candidate"] = portfolio_construction_payload
                pipeline_context["portfolio_construction_mode"] = pc_mode
                await _save_step_log(
                    analysis_id, pc_stage, "portfolio_construction",
                    input_data={
                        "mode": pc_mode,
                        "base_weights": base_weights,
                        "current_weights": brief.get("current_weights") or {},
                        "signal_strengths": signal_strengths,
                        "alpha_decision_context": alpha_decision_context,
                        "basket_reviews": (pre_risk_governance.portfolio_summary or {}).get("basket_reviews") or [],
                        "scorecard_permission": (market_scorecard or {}).get("investment_permission"),
                        "turnover_budget": _effective_portfolio_turnover_budget(market_scorecard, decision_style),
                        "market_scorecard": market_scorecard,
                        "decision_style": decision_style,
                        "execution_effect": "none",
                    },
                    output_data=portfolio_construction_payload,
                    duration_ms=0,
                )
            except Exception as pc_error:
                pipeline_context["portfolio_construction_shadow_error"] = str(pc_error)
                logger.warning("[Stage5→6] Portfolio Construction shadow failed; continuing target_builder: %s", pc_error)
                pc_mode = str(
                    (pipeline_context.get("portfolio_construction_promotion_config") or {}).get(
                        "portfolio_construction_mode",
                        "shadow",
                    )
                )
                pc_stage = (
                    "5e_portfolio_construction_candidate"
                    if pc_mode in {"candidate", "gated"}
                    else "5e_portfolio_construction_shadow"
                )
                await _save_step_log(
                    analysis_id, pc_stage, "portfolio_construction",
                    input_data={
                        "mode": pc_mode,
                        "base_weights": base_weights,
                        "current_weights": brief.get("current_weights") or {},
                    },
                    output_data={"error": str(pc_error), "execution_effect": "none"},
                    duration_ms=0,
                    failed=True,
                )
            hedge_intent = _build_hedge_intent_plan(
                brief=brief,
                evidence_bundle=evidence_bundle,
                market_scorecard=market_scorecard,
            )
            hedge_intent_outcome = _build_hedge_intent_outcome_record(
                brief=brief,
                evidence_bundle=evidence_bundle,
                hedge_intent=hedge_intent,
            )
            pipeline_context["hedge_intent"] = hedge_intent
            pipeline_context["hedge_intent_outcome"] = hedge_intent_outcome
            await _save_step_log(
                analysis_id, "5e_hedge_intent", "hedge_intent",
                input_data={
                    "current_weights": brief.get("current_weights") or {},
                    "market": (evidence_bundle.get("market") or {}),
                    "market_scorecard": market_scorecard,
                },
                output_data={
                    "hedge_intent": hedge_intent,
                    "hedge_intent_outcome": hedge_intent_outcome,
                },
                duration_ms=0,
            )
            construction_input = await _target_builder_construction_input(
                pipeline_context=pipeline_context,
                portfolio_construction_payload=(
                    pipeline_context.get("portfolio_construction_candidate")
                    or pipeline_context.get("portfolio_construction_shadow")
                    or {}
                ),
            )
            pipeline_context["target_builder_construction_input"] = construction_input
            target_builder_gated = build_target_weights(
                base_weights=base_weights,
                recall_tickers=quant_baseline.get("selected_tickers") or [],
                construction_weights=construction_input.get("construction_weights"),
                construction_source=construction_input.get("construction_source"),
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
                    "hedge_intent": hedge_intent,
                    "portfolio_construction_gate": construction_input,
                    "evidence_cap_diagnostics": pipeline_context.get("evidence_cap_diagnostics") or {},
                    "evidence_cap_config": pipeline_context.get("evidence_cap_config") or {},
                },
                mode="target_builder_gated",
            ).to_dict()
            pipeline_context["target_builder_enabled"] = True
            pipeline_context["target_builder_gated"] = target_builder_gated
            await _save_step_log(
                analysis_id, "5e_target_builder_gated_input", "target_builder",
                input_data={
                    "mode": "gated_pre_risk",
                    "base_weights": base_weights,
                    "current_weights": brief.get("current_weights") or {},
                    "portfolio_construction_input": construction_input,
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
    risk_out["decision_degradation"] = build_decision_degradation_report(
        pipeline_context=pipeline_context,
        brief=brief,
        base_weights=base_weights,
        news_evidence=news_evidence,
        research_report=research_report,
        bull_output=bull_output,
        bear_output=bear_output,
        rebuttal_vs_bear=rebuttal_vs_bear,
        rebuttal_vs_bull=rebuttal_vs_bull,
        synthesizer_out=synthesizer_out,
    )
    risk_out["legacy_mutation_classification"] = legacy_mutation_classification_summary()
    if pipeline_context.get("account_state_guard"):
        risk_out["account_state_guard"] = pipeline_context.get("account_state_guard")
    if pipeline_context.get("reconciliation_guard"):
        risk_out["reconciliation_guard"] = pipeline_context.get("reconciliation_guard")
    if pipeline_context.get("auto_pause"):
        risk_out["auto_pause"] = pipeline_context.get("auto_pause")
    if pipeline_context.get("evidence_cap_diagnostics"):
        risk_out["evidence_cap_diagnostics"] = pipeline_context.get("evidence_cap_diagnostics")
    if pipeline_context.get("hedge_intent"):
        risk_out["hedge_intent"] = pipeline_context.get("hedge_intent")
    if pipeline_context.get("hedge_intent_outcome"):
        risk_out["hedge_intent_outcome"] = pipeline_context.get("hedge_intent_outcome")
    if pipeline_context.get("portfolio_construction_shadow"):
        risk_out["portfolio_construction_shadow"] = pipeline_context.get("portfolio_construction_shadow")
    if pipeline_context.get("portfolio_construction_candidate"):
        risk_out["portfolio_construction_candidate"] = pipeline_context.get("portfolio_construction_candidate")
    if pipeline_context.get("portfolio_construction_shadow_error"):
        risk_out["portfolio_construction_shadow_error"] = pipeline_context.get("portfolio_construction_shadow_error")
    if risk_out.get("target_weights") and pipeline_context.get("portfolio_construction_shadow"):
        try:
            from services.portfolio_construction_evaluator import (
                criteria_from_pc_promotion_config,
                evaluate_portfolio_construction_shadow,
            )

            pc_config = pipeline_context.get("portfolio_construction_promotion_config") or {}
            pc_payload = (
                pipeline_context.get("portfolio_construction_candidate")
                or pipeline_context.get("portfolio_construction_shadow")
                or {}
            )
            pc_eval = evaluate_portfolio_construction_shadow(
                shadow_weights=(pc_payload or {}).get("target_weights") or {},
                actual_weights=risk_out.get("target_weights") or {},
                current_weights=brief.get("current_weights") or {},
                candidate_weights=(pc_payload or {}).get("candidate_weights") or {},
                basket_evaluation=(pc_payload or {}).get("basket_evaluation") or {},
                objective_terms=(pc_payload or {}).get("objective_terms") or {},
                hard_risk_tickers=_hard_risk_tickers_from_governance(risk_out.get("position_governance") or {}),
                regime_context=_portfolio_construction_regime_context(
                    market_scorecard=market_scorecard,
                    research_report=research_report,
                    quant_baseline=quant_baseline,
                ),
                criteria=criteria_from_pc_promotion_config(pc_config),
            ).to_dict()
            risk_out["portfolio_construction_evaluation"] = pc_eval
            try:
                from services.portfolio_construction_evaluator import (
                    build_portfolio_construction_rollout_gate,
                    load_gated_semi_auto_confirmed_cycles,
                    load_portfolio_construction_readiness,
                    readiness_limits_from_pc_promotion_config,
                )

                readiness_limits = readiness_limits_from_pc_promotion_config(pc_config)
                risk_out["portfolio_construction_readiness"] = await load_portfolio_construction_readiness(
                    limit=readiness_limits["limit"],
                    min_cycles=readiness_limits["min_cycles"],
                    min_pass_rate=readiness_limits["min_pass_rate"],
                    min_basket_policy_ok_rate=readiness_limits["min_basket_policy_ok_rate"],
                    min_policy_ok_rate=readiness_limits["min_policy_ok_rate"],
                    min_turnover_ok_rate=readiness_limits["min_turnover_ok_rate"],
                    max_mean_weight_deviation=readiness_limits["max_mean_weight_deviation"],
                    max_subscale_position_rate=readiness_limits["max_subscale_position_rate"],
                    require_no_unclassified_mutations=readiness_limits["require_no_unclassified_mutations"],
                    require_regime_coverage=readiness_limits["require_regime_coverage"],
                    min_non_bull_regime_cycles=readiness_limits["min_non_bull_regime_cycles"],
                    min_regime_confidence_for_coverage=readiness_limits[
                        "min_regime_confidence_for_coverage"
                    ],
                )
                confirmed_cycles = await load_gated_semi_auto_confirmed_cycles(
                    limit=max(
                        int(pc_config.get("min_gated_semi_auto_confirmed_cycles") or 5),
                        5,
                    )
                )
                risk_out["portfolio_construction_rollout"] = confirmed_cycles
                risk_out["portfolio_construction_promotion_gate"] = build_portfolio_construction_rollout_gate(
                    risk_out["portfolio_construction_readiness"],
                    pc_config,
                    auth_mode=pipeline_context.get("auth_mode", "SEMI_AUTO"),
                    semi_auto_confirmed_cycles=int(confirmed_cycles.get("count") or 0),
                )
            except Exception as readiness_error:
                risk_out["portfolio_construction_readiness"] = {
                    "status": "unavailable",
                    "error": str(readiness_error),
                    "execution_authority": "none",
                }
                risk_out["portfolio_construction_promotion_gate"] = {
                    "status": "unavailable",
                    "eligible": False,
                    "blockers": ["readiness_unavailable"],
                    "execution_authority": "none",
                }
            await _save_step_log(
                analysis_id, "6c_pc_eval", "pc_evaluator",
                input_data={
                    "shadow_weights": (pc_payload or {}).get("target_weights") or {},
                    "actual_weights": risk_out.get("target_weights") or {},
                    "current_weights": brief.get("current_weights") or {},
                    "portfolio_construction_mode": pc_config.get("portfolio_construction_mode"),
                },
                output_data=pc_eval,
                duration_ms=0,
            )
        except Exception as pc_eval_error:
            risk_out["portfolio_construction_evaluation_error"] = str(pc_eval_error)
            logger.warning("[Stage6] Portfolio Construction evaluation failed: %s", pc_eval_error)
    dur_risk = int((time.time() - t0) * 1000)
    approved = bool(risk_out.get("approved", False))
    if approved and risk_out.get("target_weights"):
        risk_out["risk_approved_target_weights"] = dict(risk_out.get("target_weights") or {})
        _create_target_envelope_if_needed(
            pipeline_context=pipeline_context,
            risk_out=risk_out,
            current_weights=brief.get("current_weights") or {},
            risk_approved_target=risk_out["risk_approved_target_weights"],
        )

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
            risk_out.setdefault("post_risk_mutation_types", []).append(REGIME_CONSTRAINT_MUTATION_TYPE)
            regime_ledger = MutationLedger()
            for ticker in sorted((set(target_from_risk) | set(clipped)) - {"CASH"}):
                before = float((target_from_risk or {}).get(ticker, 0.0) or 0.0)
                after = float((clipped or {}).get(ticker, 0.0) or 0.0)
                if after < before - 1e-9:
                    regime_ledger.record(
                        mutation_type=REGIME_CONSTRAINT_MUTATION_TYPE,
                        ticker=ticker,
                        before=before,
                        after=after,
                        reason="regime hard constraint tightened target weight",
                    )
            if regime_ledger.mutations:
                risk_out.setdefault("post_risk_mutation_ledgers", []).append(regime_ledger.to_dict())
            _sync_target_envelope_stage(
                pipeline_context=pipeline_context,
                risk_out=risk_out,
                new_weights=clipped,
                stage="regime_constraint",
                fallback_mutation_type=REGIME_CONSTRAINT_MUTATION_TYPE,
                reason="regime hard constraint output imported into TargetEnvelope",
                mutation_ledger=regime_ledger.to_dict(),
            )
            risk_out["regime_constraint"] = {
                "owner": "post_risk_tighten_only",
                "mutation_type": REGIME_CONSTRAINT_MUTATION_TYPE,
                "target_weight_mutation": "tighten_only",
                "violations": regime_violations,
                "target_weights_before": target_from_risk,
                "target_weights_after": clipped,
                "mutation_ledger": regime_ledger.to_dict(),
            }
            logger.warning(
                f"[Stage6→7] Regime constraint clipped {len(regime_violations)} items:\n"
                + "\n".join(regime_violations)
            )
            await _save_step_log(
                analysis_id, "6b_regime_constraint", "regime_enforcement",
                input_data={"regime": regime_result.get("regime"), "target_weights_raw": target_from_risk},
                output_data={
                    "target_weights_clipped": clipped,
                    "violations": regime_violations,
                    "mutation_type": REGIME_CONSTRAINT_MUTATION_TYPE,
                    "owner": "post_risk_tighten_only",
                    "target_weight_mutation": "tighten_only",
                    "mutation_ledger": regime_ledger.to_dict(),
                },
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
            # FULL_AUTO keeps LLM advisory as trim-only influence. The LLM can
            # still surface lifecycle concerns, but it cannot increase a live
            # target weight without deterministic hedge/strategy construction.
            governance_config["llm_advisory_max_add_pct"] = 0.0
            governance_config["llm_advisory_full_auto_policy"] = "trim_only_no_add"
        if full_auto_governance_only:
            governance_config["replacement_enabled"] = 0.0
            governance_config["llm_advisory_enabled"] = 0.0
        if run_governance_execution:
            _create_target_envelope_if_needed(
                pipeline_context=pipeline_context,
                risk_out=risk_out,
                current_weights=brief.get("current_weights") or {},
                risk_approved_target=target_before_governance,
            )
        governance_out = apply_position_governance(
            target_weights=target_before_governance,
            current_weights=brief.get("current_weights") or {},
            holdings_meta=brief.get("holdings") or [],
            strategy_evidence=evidence_bundle.get("strategies") or {},
            market_scorecard=market_scorecard,
            news_evidence=news_evidence,
            llm_advisory_proposals=synthesizer_out.get("position_advisory_proposals") or [],
            hedge_intent=pipeline_context.get("hedge_intent"),
            config=governance_config,
        )
        governance_feature_summary = risk_out.get("feature_source_summary") or {}
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
            "feature_source_summary": governance_feature_summary,
            "data_source_policy": risk_out.get("data_source_policy") or {},
        }

        if run_governance_execution:
            governance_mutation_type = (
                "emergency_reduce_only"
                if full_auto_governance_only
                else "loss_trim"
            )
            governance_ledger = _apply_position_governance_to_target_envelope(
                pipeline_context=pipeline_context,
                risk_out=risk_out,
                target_before_governance=target_before_governance,
                adjusted_weights=governance_out.adjusted_weights,
                mutation_type=governance_mutation_type,
            )
            if governance_ledger.get("total_mutations", 0) > 0:
                risk_out.setdefault("post_risk_mutation_ledgers", []).append(
                    governance_ledger
                )
                risk_out.setdefault("post_risk_mutation_types", []).append(
                    (governance_ledger.get("mutation_types") or [governance_mutation_type])[0]
                )
            rebalance_threshold = float((pipeline_context.get("risk_params") or {}).get("rebalance_threshold", 0.02))
            if full_auto_governance_only:
                rebalance_threshold = min(rebalance_threshold, 0.005)
            rebalance_actions = compute_rebalance_actions(
                risk_out.get("target_weights") or {},
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
                risk_out.setdefault("post_risk_mutation_types", []).append("emergency_reduce_only")
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
                "feature_source_summary": governance_feature_summary,
                "data_source_policy": risk_out.get("data_source_policy") or {},
                "mutation_ledger": (
                    governance_ledger
                    if run_governance_execution
                    else MutationLedger().to_dict()
                ),
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
        min_hold_exempt_tickers = _position_manager_min_hold_exempt_tickers(
            position_governance=risk_out.get("position_governance") or {},
            hard_risks_map=brief.get("hard_risks_map") or {},
            critical_alerts=brief.get("critical_alerts") or [],
        )
        turnover_exempt_tickers = _position_manager_turnover_exempt_tickers(
            position_governance=risk_out.get("position_governance") or {},
            hard_risks_map=brief.get("hard_risks_map") or {},
            critical_alerts=brief.get("critical_alerts") or [],
        )
        if min_hold_exempt_tickers:
            configured_exempt = {
                str(ticker or "").upper().strip()
                for ticker in pm_config.get("min_hold_exempt_tickers") or []
                if str(ticker or "").strip()
            }
            pm_config["min_hold_exempt_tickers"] = sorted(
                configured_exempt | set(min_hold_exempt_tickers)
            )
        if turnover_exempt_tickers:
            configured_turnover_exempt = {
                str(ticker or "").upper().strip()
                for ticker in pm_config.get("turnover_exempt_tickers") or []
                if str(ticker or "").strip()
            }
            pm_config["turnover_exempt_tickers"] = sorted(
                configured_turnover_exempt | set(turnover_exempt_tickers)
            )
        pm_out = apply_position_constraints(
            target_weights=target_before_pm,
            current_holdings=brief.get("current_weights") or {},
            config=pm_config,
            holdings_meta=brief.get("holdings") or [],
            actual_daily_trades=actual_daily_trades,
        )
        pm_envelope_ledger = _apply_position_manager_to_target_envelope(
            pipeline_context=pipeline_context,
            risk_out=risk_out,
            target_before_pm=target_before_pm,
            adjusted_weights=pm_out.adjusted_weights,
            mutation_ledger=pm_out.mutation_ledger,
        )
        rebalance_threshold = float((pipeline_context.get("risk_params") or {}).get("rebalance_threshold", 0.02))
        rebalance_actions = compute_rebalance_actions(
            risk_out.get("target_weights") or {},
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
            "mutation_types": pm_out.mutation_types,
            "mutation_details": pm_out.mutation_details,
            "mutation_ledger": pm_envelope_ledger,
            "diagnostic_legacy_mutation_ledger": pm_out.mutation_ledger,
        }
        risk_out.setdefault("post_risk_mutation_types", []).extend(pm_out.mutation_types)
        risk_out.setdefault("post_risk_mutation_details", []).extend(pm_out.mutation_details)
        risk_out.setdefault("post_risk_mutation_ledgers", []).append(pm_envelope_ledger)

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
                "mutation_types": pm_out.mutation_types,
                "mutation_details": pm_out.mutation_details,
                "mutation_ledger": pm_envelope_ledger,
                "diagnostic_legacy_mutation_ledger": pm_out.mutation_ledger,
            },
            duration_ms=0,
        )

    await _apply_final_execution_policy_cap(
        analysis_id=analysis_id,
        risk_out=risk_out,
        current_weights=brief.get("current_weights") or {},
        rebalance_threshold=float((pipeline_context.get("risk_params") or {}).get("rebalance_threshold", 0.02)),
        pipeline_context=pipeline_context,
    )
    await _apply_execution_throttle(
        analysis_id=analysis_id,
        risk_out=risk_out,
        current_weights=brief.get("current_weights") or {},
        rebalance_threshold=float((pipeline_context.get("risk_params") or {}).get("rebalance_threshold", 0.02)),
        pipeline_context=pipeline_context,
    )
    await _apply_transaction_cost_gate_observe(
        analysis_id=analysis_id,
        risk_out=risk_out,
        current_weights=brief.get("current_weights") or {},
        strategy_evidence=evidence_bundle.get("strategies") or {},
        pipeline_context=pipeline_context,
    )
    await _apply_final_risk_validation(
        analysis_id=analysis_id,
        risk_out=risk_out,
        current_weights=brief.get("current_weights") or {},
        hard_risks_map=brief.get("hard_risks_map") or {},
        critical_alerts=brief.get("critical_alerts") or [],
        pipeline_context=pipeline_context,
    )
    await _apply_portfolio_risk_diagnostic(
        analysis_id=analysis_id,
        risk_out=risk_out,
        current_weights=brief.get("current_weights") or {},
    )
    approved = bool(risk_out.get("approved", False))

    if risk_out.get("position_governance"):
        try:
            target_builder_out = build_target_weights(
                base_weights=base_weights,
                recall_tickers=quant_baseline.get("selected_tickers") or [],
                current_weights=brief.get("current_weights") or {},
                market_scorecard=market_scorecard,
                decision_style=decision_style,
                position_governance=risk_out.get("position_governance") or {},
                validated_advisory=(risk_out.get("position_governance") or {}).get("advisory_overrides") or [],
                constraints={
                    "max_turnover": (market_scorecard or {}).get("max_turnover_per_cycle"),
                    "max_single_delta": (market_scorecard or {}).get("max_adjustment_from_base"),
                    "hedge_intent": pipeline_context.get("hedge_intent"),
                    "evidence_cap_diagnostics": pipeline_context.get("evidence_cap_diagnostics") or {},
                    "evidence_cap_config": pipeline_context.get("evidence_cap_config") or {},
                },
                mode="target_builder_shadow",
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
                    "mode": "target_builder_shadow",
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
                    "mode": "target_builder_shadow",
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

    await _persist_alpha_validation_snapshot(
        analysis_id=analysis_id,
        trigger_type=trigger,
        risk_out=risk_out,
        evidence_bundle=evidence_bundle,
    )
    await _persist_validation_observations_snapshot(
        analysis_id=analysis_id,
        trigger_type=trigger,
        risk_out=risk_out,
    )

    try:
        diagnostic_artifacts = build_pipeline_diagnostic_artifacts(
            analysis_id=analysis_id,
            as_of_time=datetime.utcnow(),
            pipeline_context=pipeline_context,
            brief=brief,
            market_scorecard=market_scorecard,
            synthesizer_out=synthesizer_out,
            risk_out=risk_out,
            base_weights=base_weights,
            bull_output=bull_output,
            bear_output=bear_output,
        )
        risk_out = append_diagnostic_artifacts(risk_out, diagnostic_artifacts)
        risk_out["decision_feature_snapshot_id"] = next(
            (
                item.artifact_id
                for item in diagnostic_artifacts
                if item.schema_version == "decision_feature_snapshot_v1"
            ),
            None,
        )
        await _save_step_log(
            analysis_id,
            "6e_diagnostic_artifacts",
            "diagnostic_artifacts",
            input_data={
                "artifact_count": len(diagnostic_artifacts),
                "schemas": sorted({item.schema_version for item in diagnostic_artifacts}),
            },
            output_data={
                "artifact_count": len(diagnostic_artifacts),
                "decision_feature_snapshot_id": risk_out.get("decision_feature_snapshot_id"),
                "execution_authority": "none",
            },
            duration_ms=0,
        )
        try:
            async with AsyncSessionLocal() as db:
                notify_result = await notify_strategy_execution_evidence_certification(
                    db=db,
                    analysis_id=analysis_id,
                    diagnostic_artifacts=risk_out.get("diagnostic_artifacts") or [],
                    notifier=tool_send_telegram,
                )
            if notify_result.get("sent"):
                logger.info(
                    "[pipeline] strategy execution evidence certification notified: %s",
                    notify_result,
                )
        except Exception as notify_exc:
            logger.warning(
                "[pipeline] strategy execution evidence certification notification failed: %s",
                notify_exc,
            )
    except Exception as e:
        logger.warning("[pipeline] diagnostic artifact build failed: %s", e)
        risk_out["diagnostic_artifacts_error"] = str(e)

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
    news_degraded_gate = _evaluate_news_degraded_execution_gate(
        pipeline_context=pipeline_context,
        risk_out=risk_out,
        current_weights=(pipeline_context.get("brief") or {}).get("current_weights") or {},
    )
    risk_out["news_degraded_execution_gate"] = news_degraded_gate
    if not news_degraded_gate.get("allowed", True):
        await _save_step_log(
            analysis_id,
            "8c_news_degraded_execution_gate",
            "news_degraded_execution_gate",
            input_data={
                "analysis_id": analysis_id,
                "approved": bool(risk_out.get("approved")),
                "auth_mode": auth_mode,
            },
            output_data=news_degraded_gate,
            duration_ms=0,
            failed=True,
        )
        await _update_analysis_risk_output(analysis_id, risk_out)
        await _save_execution(
            analysis_id,
            {
                "execution_status": "skipped_news_stale_risk_increase",
                "execution_audit": build_execution_audit_payload(
                    action_status="skipped_news_stale_risk_increase",
                    proposed_weights=risk_out.get("target_weights") or {},
                    rebalance_actions=risk_out.get("rebalance_actions") or [],
                    estimated_cost_pct=float(risk_out.get("estimated_cost_pct") or 0.0),
                    reason=news_degraded_gate.get("reason") or "news_stale_degraded_mode_blocks_risk_increase",
                ),
            },
        )
        message = (
            remove_command_hints(comm_out["text"]).rstrip()
            + "\n\n⚠️ <b>News stale degraded mode</b>\n"
            + "  Risk-increasing proposal blocked. Reduce-only actions remain allowed after deterministic checks."
        )
        await tool_send_telegram({"text": message})
        pipeline_status = "skipped_news_stale_risk_increase"

    elif not approved:
        should_notify, suppress_reason = await _should_notify_rejected_pipeline(
            risk_out=risk_out,
            synthesizer_out=synthesizer_out,
            pipeline_context=pipeline_context,
        )
        if should_notify:
            notify_result = await tool_send_telegram({"text": remove_command_hints(comm_out["text"])})
            if bool(notify_result.get("sent")):
                await _mark_rejected_pipeline_notified(
                    risk_out=risk_out,
                    synthesizer_out=synthesizer_out,
                    pipeline_context=pipeline_context,
                )
                logger.info("Risk rejected — notified and stopping")
            else:
                logger.warning(
                    "Risk rejected — Telegram notification failed and cooldown was not recorded: %s",
                    notify_result.get("error"),
                )
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
            if circuit in ("ALERT", "DEFENSIVE") and not pipeline_context.get("circuit_override_consumed"):
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


def _evaluate_news_degraded_execution_gate(
    *,
    pipeline_context: dict,
    risk_out: dict,
    current_weights: dict,
) -> dict[str, Any]:
    degraded = pipeline_context.get("news_degraded_mode") or {}
    if not degraded.get("enabled"):
        return {
            "allowed": True,
            "status": "not_applicable",
            "reason": "news_cache_ok_or_gate_not_required",
            "execution_effect": "none",
        }
    if not risk_out.get("approved"):
        return {
            "allowed": True,
            "status": "risk_rejected_no_execution",
            "reason": degraded.get("reason") or "news_stale_degraded_mode",
            "execution_effect": "none",
            "degraded_mode": degraded.get("mode") or "news_stale_reduce_only",
        }

    target_weights = risk_out.get("target_weights") or {}
    reduce_only = is_reduce_only_vs_actual(target_weights, current_weights or {})
    if reduce_only:
        return {
            "allowed": True,
            "status": "reduce_only_allowed",
            "reason": degraded.get("reason") or "news_stale_degraded_mode",
            "execution_effect": "allow_reduce_only",
            "degraded_mode": degraded.get("mode") or "news_stale_reduce_only",
            "reduce_only": True,
        }
    return {
        "allowed": False,
        "status": "blocked_risk_increase",
        "reason": "news_stale_degraded_mode_blocks_risk_increase",
        "execution_effect": "blocking",
        "degraded_mode": degraded.get("mode") or "news_stale_reduce_only",
        "reduce_only": False,
        "news_cache": degraded.get("news_cache") or {},
    }


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

    return True, None


async def _mark_rejected_pipeline_notified(
    *,
    risk_out: dict,
    synthesizer_out: dict,
    pipeline_context: dict,
    cooldown_minutes: int = REJECTED_NOTIFICATION_COOLDOWN_MINUTES,
) -> None:
    fingerprint = _rejected_pipeline_fingerprint(risk_out, synthesizer_out, pipeline_context)
    now = datetime.utcnow()
    async with AsyncSessionLocal() as db:
        await upsert_system_config(db, REJECTED_NOTIFICATION_STATE_KEY, {
            "fingerprint": fingerprint,
            "notified_at": now.isoformat(),
            "cooldown_minutes": cooldown_minutes,
        }, "pipeline")


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


def _target_envelope_enabled(pipeline_context: dict) -> bool:
    config = default_target_envelope_config(
        pipeline_context.get("target_envelope_config") or {}
    )
    return bool(config.get("enabled"))


def _target_envelope_authoritative(pipeline_context: dict) -> bool:
    config = default_target_envelope_config(
        pipeline_context.get("target_envelope_config") or {}
    )
    return bool(config.get("enabled")) and str(config.get("mode") or "active") in {"active", "strict"}


def _target_envelope_obj(pipeline_context: dict) -> TargetEnvelope | None:
    envelope = pipeline_context.get("_target_envelope_obj")
    return envelope if isinstance(envelope, TargetEnvelope) else None


def _create_target_envelope_if_needed(
    *,
    pipeline_context: dict,
    risk_out: dict,
    current_weights: dict,
    risk_approved_target: dict,
) -> TargetEnvelope | None:
    if not _target_envelope_enabled(pipeline_context):
        return None
    existing = _target_envelope_obj(pipeline_context)
    if existing is not None:
        return existing
    if not risk_approved_target:
        return None
    envelope = TargetEnvelope(
        current_weights=current_weights or {},
        risk_approved_target=risk_approved_target or {},
    )
    pipeline_context["_target_envelope_obj"] = envelope
    _attach_target_envelope(
        pipeline_context=pipeline_context,
        risk_out=risk_out,
        envelope=envelope,
    )
    return envelope


def _attach_target_envelope(
    *,
    pipeline_context: dict,
    risk_out: dict,
    envelope: TargetEnvelope,
) -> None:
    payload = envelope.to_dict()
    payload["config"] = default_target_envelope_config(
        pipeline_context.get("target_envelope_config") or {}
    )
    if risk_out.get("target_envelope_errors"):
        payload["bridge_errors"] = list(risk_out.get("target_envelope_errors") or [])
    risk_out["target_envelope"] = payload
    if _target_envelope_authoritative(pipeline_context):
        risk_out["target_weights"] = envelope.final_target


def _sync_target_envelope_stage(
    *,
    pipeline_context: dict,
    risk_out: dict,
    new_weights: dict,
    stage: str,
    fallback_mutation_type: str,
    reason: str,
    mutation_ledger: dict | None = None,
) -> None:
    if not _target_envelope_enabled(pipeline_context):
        return
    envelope = _target_envelope_obj(pipeline_context)
    if envelope is None:
        envelope = _create_target_envelope_if_needed(
            pipeline_context=pipeline_context,
            risk_out=risk_out,
            current_weights=(pipeline_context.get("brief") or {}).get("current_weights") or {},
            risk_approved_target=(
                risk_out.get("risk_approved_target_weights")
                or risk_out.get("target_weights")
                or {}
            ),
        )
    if envelope is None:
        return
    try:
        if mutation_ledger:
            envelope.apply_stage_ledger(
                new_weights=new_weights or {},
                mutation_ledger=mutation_ledger,
                fallback_mutation_type=fallback_mutation_type,
                reason=reason,
                stage=stage,
            )
        else:
            envelope.apply_stage_target(
                new_weights=new_weights or {},
                mutation_type=fallback_mutation_type,
                reason=reason,
                stage=stage,
            )
    except Exception as exc:
        risk_out.setdefault("target_envelope_errors", []).append(
            f"{stage}: {type(exc).__name__}: {exc}"
        )
    finally:
        _attach_target_envelope(
            pipeline_context=pipeline_context,
            risk_out=risk_out,
            envelope=envelope,
        )


def _apply_position_governance_to_target_envelope(
    *,
    pipeline_context: dict,
    risk_out: dict,
    target_before_governance: dict,
    adjusted_weights: dict,
    mutation_type: str,
) -> dict:
    """Write position-governance executable changes directly into envelope."""
    if not _target_envelope_enabled(pipeline_context):
        legacy_ledger = _position_governance_mutation_ledger(
            before=target_before_governance,
            after=adjusted_weights,
            mutation_type=mutation_type,
        )
        risk_out["target_weights"] = adjusted_weights
        return legacy_ledger.to_dict()

    envelope = _create_target_envelope_if_needed(
        pipeline_context=pipeline_context,
        risk_out=risk_out,
        current_weights=(pipeline_context.get("brief") or {}).get("current_weights") or {},
        risk_approved_target=target_before_governance,
    )
    if envelope is None:
        risk_out["target_weights"] = adjusted_weights
        return MutationLedger().to_dict()

    start_index = len(envelope.ledger.mutations)
    try:
        envelope.apply_stage_target(
            new_weights=adjusted_weights or {},
            mutation_type=mutation_type,
            reason="position governance executable mutation",
            stage="position_governance",
        )
    except Exception as exc:
        risk_out.setdefault("target_envelope_errors", []).append(
            f"position_governance: {type(exc).__name__}: {exc}"
        )
    finally:
        _attach_target_envelope(
            pipeline_context=pipeline_context,
            risk_out=risk_out,
            envelope=envelope,
        )

    stage_ledger = MutationLedger()
    stage_ledger.extend(envelope.ledger.mutations[start_index:])
    return stage_ledger.to_dict()


def _apply_position_manager_to_target_envelope(
    *,
    pipeline_context: dict,
    risk_out: dict,
    target_before_pm: dict,
    adjusted_weights: dict,
    mutation_ledger: dict,
) -> dict:
    """Write position-manager executable changes directly into envelope."""
    if not _target_envelope_enabled(pipeline_context):
        risk_out["target_weights"] = adjusted_weights
        return mutation_ledger or MutationLedger().to_dict()

    envelope = _create_target_envelope_if_needed(
        pipeline_context=pipeline_context,
        risk_out=risk_out,
        current_weights=(pipeline_context.get("brief") or {}).get("current_weights") or {},
        risk_approved_target=target_before_pm,
    )
    if envelope is None:
        risk_out["target_weights"] = adjusted_weights
        return MutationLedger().to_dict()

    start_index = len(envelope.ledger.mutations)
    try:
        envelope.apply_stage_mutation_ledger(
            mutation_ledger=mutation_ledger or {},
            stage="position_manager",
            reason="position manager executable mutation",
        )
        _record_unaccounted_stage_drift(
            pipeline_context=pipeline_context,
            risk_out=risk_out,
            stage="position_manager",
            expected=adjusted_weights or {},
            actual=envelope.final_target,
        )
    except Exception as exc:
        risk_out.setdefault("target_envelope_errors", []).append(
            f"position_manager: {type(exc).__name__}: {exc}"
        )
    finally:
        _attach_target_envelope(
            pipeline_context=pipeline_context,
            risk_out=risk_out,
            envelope=envelope,
        )

    stage_ledger = MutationLedger()
    stage_ledger.extend(envelope.ledger.mutations[start_index:])
    return stage_ledger.to_dict()


def _apply_structured_ledger_stage_to_target_envelope(
    *,
    pipeline_context: dict,
    risk_out: dict,
    stage: str,
    expected_weights: dict,
    mutation_ledger: dict,
    reason: str,
) -> dict:
    """Apply a post-risk stage through its structured mutation ledger only."""
    if not _target_envelope_enabled(pipeline_context):
        risk_out["target_weights"] = expected_weights
        return mutation_ledger or MutationLedger().to_dict()

    envelope = _target_envelope_obj(pipeline_context)
    if envelope is None:
        envelope = _create_target_envelope_if_needed(
            pipeline_context=pipeline_context,
            risk_out=risk_out,
            current_weights=(pipeline_context.get("brief") or {}).get("current_weights") or {},
            risk_approved_target=(
                risk_out.get("risk_approved_target_weights")
                or risk_out.get("target_weights")
                or {}
            ),
        )
    if envelope is None:
        risk_out["target_weights"] = expected_weights
        return MutationLedger().to_dict()

    start_index = len(envelope.ledger.mutations)
    try:
        envelope.apply_stage_mutation_ledger(
            mutation_ledger=mutation_ledger or {},
            stage=stage,
            reason=reason,
        )
        _record_unaccounted_stage_drift(
            pipeline_context=pipeline_context,
            risk_out=risk_out,
            stage=stage,
            expected=expected_weights or {},
            actual=envelope.final_target,
        )
    except Exception as exc:
        risk_out.setdefault("target_envelope_errors", []).append(
            f"{stage}: {type(exc).__name__}: {exc}"
        )
    finally:
        _attach_target_envelope(
            pipeline_context=pipeline_context,
            risk_out=risk_out,
            envelope=envelope,
        )

    stage_ledger = MutationLedger()
    stage_ledger.extend(envelope.ledger.mutations[start_index:])
    return stage_ledger.to_dict()


def _record_unaccounted_stage_drift(
    *,
    pipeline_context: dict,
    risk_out: dict,
    stage: str,
    expected: dict,
    actual: dict,
    tolerance: float = 1e-6,
) -> None:
    if not _target_envelope_authoritative(pipeline_context):
        return
    expected_clean = _clean_weight_map_for_contract(expected)
    actual_clean = _clean_weight_map_for_contract(actual)
    for ticker in sorted((set(expected_clean) | set(actual_clean)) - {"CASH"}):
        expected_w = expected_clean.get(ticker, 0.0)
        actual_w = actual_clean.get(ticker, 0.0)
        if abs(expected_w - actual_w) > tolerance:
            risk_out.setdefault("target_envelope_errors", []).append(
                f"{stage}: unaccounted non-CASH drift for {ticker}: "
                f"stage_output={expected_w:.6f}, envelope={actual_w:.6f}"
            )


def _clean_weight_map_for_contract(weights: dict | None) -> dict[str, float]:
    out: dict[str, float] = {}
    for raw_ticker, raw_weight in (weights or {}).items():
        ticker = str(raw_ticker or "").upper().strip()
        if not ticker:
            continue
        try:
            out[ticker] = max(float(raw_weight or 0.0), 0.0)
        except (TypeError, ValueError):
            out[ticker] = 0.0
    return out


async def _apply_final_execution_policy_cap(
    *,
    analysis_id: int,
    risk_out: dict,
    current_weights: dict,
    rebalance_threshold: float,
    pipeline_context: dict,
) -> None:
    """Final execution-policy clamp after governance and position manager edits."""
    if not risk_out.get("approved") or not risk_out.get("target_weights"):
        return

    pre_cap = dict(risk_out.get("target_weights") or {})
    final_cap = apply_final_execution_policy_cap(
        target_weights=pre_cap,
        current_weights=current_weights,
        rebalance_threshold=rebalance_threshold,
    )
    capped = final_cap["target_weights"]
    cap_events = final_cap["cap_events"]
    risk_out["final_policy_version"] = final_cap["policy_version"]
    risk_out["final_policy_cap_events"] = cap_events
    risk_out["minimum_weight_floor_events"] = final_cap.get("floor_events") or []
    risk_out["active_basket_policy"] = final_cap.get("active_basket_policy") or {}
    risk_out["final_policy_cash_raised"] = final_cap["cash_raised"]
    risk_out["final_policy_cash_raised_by_policy_cap"] = final_cap.get("cash_raised_by_policy_cap", 0.0)
    risk_out["final_policy_cash_raised_by_minimum_weight_floor"] = final_cap.get(
        "cash_raised_by_minimum_weight_floor",
        0.0,
    )
    risk_out["final_policy_cap_triggered"] = final_cap["triggered"]
    risk_out["final_policy_evaluation"] = final_cap.get("policy_evaluation") or {}
    if final_cap.get("mutation_types"):
        risk_out.setdefault("post_risk_mutation_types", []).extend(final_cap.get("mutation_types") or [])
    final_cap_envelope_ledger = _apply_structured_ledger_stage_to_target_envelope(
        pipeline_context=pipeline_context,
        risk_out=risk_out,
        stage="final_policy_cap",
        expected_weights=capped,
        mutation_ledger=final_cap.get("mutation_ledger") or {},
        reason="final execution policy cap executable mutation",
    )
    if final_cap_envelope_ledger.get("total_mutations", 0) > 0:
        risk_out.setdefault("post_risk_mutation_ledgers", []).append(final_cap_envelope_ledger)

    if not final_cap["triggered"]:
        await _save_step_log(
            analysis_id,
            "6cb_final_policy_cap",
            "execution_policy",
            input_data={"target_weights_raw": pre_cap},
            output_data={
                "target_weights": capped,
                "policy_version": risk_out["final_policy_version"],
                "cap_events": [],
                "floor_events": [],
                "active_basket_policy": final_cap.get("active_basket_policy") or {},
                "cash_raised": 0.0,
                "triggered": False,
                "mutation_ledger": final_cap_envelope_ledger,
                "diagnostic_legacy_mutation_ledger": final_cap.get("mutation_ledger") or {},
                "policy_evaluation": final_cap.get("policy_evaluation") or {},
            },
            duration_ms=0,
        )
        return

    logger.warning(
        "[FINAL_CAP] Post-governance weights required final execution repair: caps=%s floors=%s. "
        "This indicates a policy gap upstream.",
        cap_events,
        final_cap.get("floor_events") or [],
    )
    risk_out["target_weights"] = capped
    risk_out["rebalance_actions"] = final_cap["rebalance_actions"]
    risk_out["estimated_cost_pct"] = final_cap["estimated_cost_pct"]
    risk_out["n_holdings"] = final_cap["n_holdings"]
    risk_out.setdefault("overlays_applied", []).append("final_execution_policy_cap")

    await _save_step_log(
        analysis_id,
        "6cb_final_policy_cap",
        "execution_policy",
        input_data={"target_weights_raw": pre_cap},
        output_data={
            "target_weights": capped,
            "policy_version": risk_out["final_policy_version"],
            "cap_events": cap_events,
            "floor_events": final_cap.get("floor_events") or [],
            "active_basket_policy": final_cap.get("active_basket_policy") or {},
            "cash_raised": final_cap["cash_raised"],
            "cash_raised_by_policy_cap": final_cap.get("cash_raised_by_policy_cap", 0.0),
            "cash_raised_by_minimum_weight_floor": final_cap.get(
                "cash_raised_by_minimum_weight_floor",
                0.0,
            ),
            "triggered": True,
            "mutation_types": final_cap.get("mutation_types") or [],
            "mutation_ledger": final_cap_envelope_ledger,
            "diagnostic_legacy_mutation_ledger": final_cap.get("mutation_ledger") or {},
            "policy_evaluation": final_cap.get("policy_evaluation") or {},
            "rebalance_actions": final_cap["rebalance_actions"],
            "estimated_cost_pct": risk_out["estimated_cost_pct"],
        },
        duration_ms=0,
    )


async def _apply_transaction_cost_gate_observe(
    *,
    analysis_id: int,
    risk_out: dict,
    current_weights: dict,
    strategy_evidence: dict,
    pipeline_context: dict,
) -> None:
    """Attach observe-only transaction cost diagnostics to final target."""
    if not risk_out.get("target_weights"):
        return

    rebalance_actions = risk_out.get("rebalance_actions") or compute_rebalance_actions(
        risk_out.get("target_weights") or {},
        current_weights or {},
        float((pipeline_context.get("risk_params") or {}).get("rebalance_threshold", 0.02)),
    )
    verdict = evaluate_transaction_cost_gate(
        target_weights=risk_out.get("target_weights") or {},
        current_weights=current_weights or {},
        rebalance_actions=rebalance_actions,
        strategy_evidence=strategy_evidence or {},
        config=pipeline_context.get("transaction_cost_gate_config") or {},
    )
    risk_out["transaction_cost_gate"] = verdict
    if verdict.get("warnings"):
        logger.warning("[COST_GATE] Observe warnings: %s", verdict.get("warnings"))
    else:
        logger.info("[COST_GATE] Observe passed with no cost warnings")

    await _save_step_log(
        analysis_id,
        "6cc_transaction_cost_gate",
        "transaction_cost_gate",
        input_data={
            "target_weights": risk_out.get("target_weights") or {},
            "current_weights": current_weights or {},
            "rebalance_actions": rebalance_actions,
            "config": pipeline_context.get("transaction_cost_gate_config") or {},
        },
        output_data=verdict,
        duration_ms=0,
    )


async def _apply_execution_throttle(
    *,
    analysis_id: int,
    risk_out: dict,
    current_weights: dict,
    rebalance_threshold: float,
    pipeline_context: dict,
) -> None:
    """Stage final target to per-command execution-delta limits."""
    if not risk_out.get("approved") or not risk_out.get("target_weights"):
        return

    desired = dict(risk_out.get("target_weights") or {})
    throttle = apply_execution_throttle(
        target_weights=desired,
        current_weights=current_weights or {},
        config=pipeline_context.get("execution_command_config") or {},
    )
    risk_out["execution_throttle"] = throttle
    try:
        ledger = await record_deferred_execution_plan(
            analysis_id=analysis_id,
            command_id=f"analysis_{analysis_id}",
            throttle=throttle,
        )
        risk_out["deferred_execution_ledger"] = ledger
        throttle["deferred_execution_ledger"] = ledger
    except Exception as exc:
        ledger = {
            "contract_version": "v1",
            "execution_effect": "diagnostic_only",
            "status": "failed",
            "reason": f"{type(exc).__name__}: {exc}",
        }
        risk_out["deferred_execution_ledger"] = ledger
        throttle["deferred_execution_ledger"] = ledger
        logger.warning("[DEFERRED_EXECUTION_LEDGER] failed to persist deferred plan: %s", exc)
    if throttle.get("applied"):
        staged = throttle.get("staged_target_weights") or {}
        throttle_envelope_ledger = _apply_structured_ledger_stage_to_target_envelope(
            pipeline_context=pipeline_context,
            risk_out=risk_out,
            stage="execution_throttle",
            expected_weights=staged,
            mutation_ledger=throttle.get("mutation_ledger") or {},
            reason="execution throttle executable mutation",
        )
        risk_out["execution_desired_target_weights"] = throttle.get("desired_target_weights") or desired
        risk_out["execution_deferred_delta"] = throttle.get("deferred_delta") or {}
        risk_out["rebalance_actions"] = compute_rebalance_actions(
            risk_out.get("target_weights") or {},
            current_weights or {},
            rebalance_threshold,
        )
        risk_out["estimated_cost_pct"] = estimate_cost_pct(risk_out["rebalance_actions"])
        risk_out["n_holdings"] = sum(
            1 for ticker, weight in (risk_out.get("target_weights") or {}).items()
            if ticker != "CASH" and float(weight or 0.0) > 0.01
        )
        risk_out.setdefault("overlays_applied", []).append("execution_throttle")
        risk_out.setdefault("post_risk_mutation_types", []).extend(throttle.get("mutation_types") or [])
        if throttle_envelope_ledger.get("total_mutations", 0) > 0:
            risk_out.setdefault("post_risk_mutation_ledgers", []).append(throttle_envelope_ledger)
        throttle["envelope_mutation_ledger"] = throttle_envelope_ledger
        throttle["diagnostic_legacy_mutation_ledger"] = throttle.get("mutation_ledger") or {}
        throttle["mutation_ledger"] = throttle_envelope_ledger
        logger.warning(
            "[EXECUTION_THROTTLE] staged target | buy_delta %s -> %s | deferred=%s",
            (throttle.get("metrics_before") or {}).get("buy_delta"),
            (throttle.get("metrics_after") or {}).get("buy_delta"),
            throttle.get("deferred_buy_delta"),
        )
    else:
        logger.info("[EXECUTION_THROTTLE] no staging needed: %s", throttle.get("reason"))

    await _save_step_log(
        analysis_id,
        "6ccb_execution_throttle",
        "execution_throttle",
        input_data={
            "target_weights_desired": desired,
            "current_weights": current_weights or {},
            "config": pipeline_context.get("execution_command_config") or {},
        },
        output_data=throttle,
        duration_ms=0,
    )


async def _apply_final_risk_validation(
    *,
    analysis_id: int,
    risk_out: dict,
    current_weights: dict,
    hard_risks_map: dict,
    critical_alerts: list[dict],
    pipeline_context: dict,
) -> None:
    """Validate final target after all post-risk mutations."""
    if not risk_out.get("approved") or not risk_out.get("target_weights"):
        return

    risk_params = pipeline_context.get("risk_params") or {}
    market_scorecard = pipeline_context.get("market_scorecard") or {}
    final_validation_config = default_final_risk_validation_config(
        pipeline_context.get("final_risk_validation_config") or {}
    )
    forced_trim_tickers = _tickers_from_forced_trim_strings(
        (risk_out.get("position_governance") or {}).get("forced_trims") or []
    )
    critical_alert_tickers = [
        str((row or {}).get("ticker") or "").upper().strip()
        for row in critical_alerts or []
        if str((row or {}).get("ticker") or "").strip()
    ]
    scorecard_restricted_tickers = _scorecard_restricted_tickers(
        risk_out.get("position_governance") or {}
    )
    envelope_payload = risk_out.get("target_envelope") or {}
    envelope_authoritative = bool(
        envelope_payload and _target_envelope_authoritative(pipeline_context)
    )
    if envelope_authoritative:
        risk_approved_for_validation = (
            envelope_payload.get("risk_approved_target") or {}
        )
        final_target_for_validation = envelope_payload.get("final_target") or {}
        risk_out["target_weights"] = dict(final_target_for_validation)
        envelope_ledger = envelope_payload.get("ledger") or {}
        post_risk_mutation_ledgers = [envelope_ledger] if envelope_ledger else []
        post_risk_mutation_types = envelope_ledger.get("mutation_types") or []
        post_risk_mutation_details = []
    else:
        risk_approved_for_validation = (
            risk_out.get("risk_approved_target_weights")
            or risk_out.get("risk_manager_input_target_weights")
            or {}
        )
        final_target_for_validation = risk_out.get("target_weights") or {}
        post_risk_mutation_ledgers = risk_out.get("post_risk_mutation_ledgers") or []
        post_risk_mutation_types = risk_out.get("post_risk_mutation_types") or []
        post_risk_mutation_details = risk_out.get("post_risk_mutation_details") or []
    policy_context = {
        "post_risk_mutation_types": post_risk_mutation_types,
        "post_risk_mutation_details": post_risk_mutation_details,
        "post_risk_mutation_ledgers": post_risk_mutation_ledgers,
        "target_envelope": envelope_payload,
        "target_envelope_config": default_target_envelope_config(
            pipeline_context.get("target_envelope_config") or {}
        ),
        "target_envelope_errors": risk_out.get("target_envelope_errors") or [],
        "material_drift_threshold": final_validation_config.get("material_drift_threshold"),
        "require_human_confirmation_for_conditional_material_drift": bool(
            final_validation_config.get("require_human_confirmation_for_conditional_material_drift", True)
        ),
        "human_confirmed": bool(risk_out.get("human_confirmed_final_validation")),
        "hard_risk_tickers": [
            str(ticker or "").upper().strip()
            for ticker, risk in (hard_risks_map or {}).items()
            if ticker and risk
        ],
        "critical_alert_tickers": critical_alert_tickers,
        "forced_trim_tickers": forced_trim_tickers,
        "scorecard_restricted_tickers": scorecard_restricted_tickers,
        "execution_policy_context": {
            "min_cash_pct": risk_params.get("min_cash_pct"),
            "max_single_position": risk_params.get("max_single_position"),
            "min_cash_weight": market_scorecard.get("min_cash_weight"),
            "max_equity_weight": market_scorecard.get("max_equity_weight"),
            "max_turnover_per_cycle": market_scorecard.get("max_turnover_per_cycle"),
            "max_single_delta": market_scorecard.get("max_adjustment_from_base"),
        },
    }
    validation = validate_final_execution_target(
        risk_approved_target=risk_approved_for_validation,
        final_target=final_target_for_validation,
        current_weights=current_weights or {},
        risk_context={
            "target_construction_mode": risk_out.get("target_construction_mode"),
            "approval_source": risk_out.get("approval_source"),
            "target_envelope_authoritative": envelope_authoritative,
        },
        policy_context=policy_context,
        mode=resolve_final_risk_validation_mode(
            final_validation_config,
            auth_mode=str(pipeline_context.get("auth_mode") or ""),
        ),
    )
    validation["configured_mode"] = final_validation_config.get("mode", "observe")
    validation["effective_mode"] = validation.get("mode")
    validation["auth_mode"] = str(pipeline_context.get("auth_mode") or "")
    risk_out["final_validation"] = validation
    if not validation.get("approved"):
        risk_out["approved"] = False
        risk_out.pop("approval_token", None)
        risk_out.pop("token_expires_at", None)
        details = (
            validation.get("severe_violations")
            or validation.get("blocking_violations")
            or validation.get("conditional_mutation_violations")
            or validation.get("unknown_mutation_types")
            or []
        )
        risk_out.setdefault("rejection_reasons", []).append(
            "Final validation blocked execution target: "
            + ", ".join(str(row) for row in details)
        )
        risk_out.setdefault("overlays_applied", []).append("final_validation_hard_block")

    await _save_step_log(
        analysis_id,
        "6cc_final_risk_validation",
        "final_risk_validation",
        input_data={
            "risk_approved_target": risk_approved_for_validation,
            "final_target": final_target_for_validation,
            "current_weights": current_weights or {},
            "policy_context": policy_context,
            "final_validation_config": final_validation_config,
        },
        output_data=validation,
        duration_ms=0,
        failed=not bool(validation.get("approved")),
    )


async def _apply_portfolio_risk_diagnostic(
    *,
    analysis_id: int,
    risk_out: dict,
    current_weights: dict,
) -> None:
    """Attach VaR/CVaR diagnostics after final target validation."""
    target_weights = risk_out.get("target_weights") or current_weights or {}
    if not target_weights:
        return

    try:
        async with AsyncSessionLocal() as db:
            diagnostic = await load_portfolio_var_cvar_diagnostic(
                db,
                target_weights=target_weights,
                current_weights=current_weights or {},
            )
        risk_out["portfolio_risk_diagnostic"] = diagnostic
        await _save_step_log(
            analysis_id,
            "6cd_portfolio_var_cvar",
            "portfolio_risk_diagnostic",
            input_data={
                "target_weights": target_weights,
                "current_weights": current_weights or {},
                "execution_effect": "diagnostic_only",
            },
            output_data=diagnostic,
            duration_ms=0,
        )
    except Exception as exc:
        logger.warning("[Stage6] Portfolio VaR/CVaR diagnostic failed: %s", exc)
        risk_out["portfolio_risk_diagnostic"] = {
            "status": "unavailable",
            "mode": "diagnostic_only",
            "execution_authority": "none",
            "error": str(exc),
        }
        await _save_step_log(
            analysis_id,
            "6cd_portfolio_var_cvar",
            "portfolio_risk_diagnostic",
            input_data={
                "target_weights": target_weights,
                "current_weights": current_weights or {},
                "execution_effect": "diagnostic_only",
            },
            output_data=risk_out["portfolio_risk_diagnostic"],
            duration_ms=0,
            failed=True,
        )


async def _persist_alpha_validation_snapshot(
    *,
    analysis_id: int,
    trigger_type: str,
    risk_out: dict,
    evidence_bundle: dict,
) -> None:
    """Persist alpha/cost/risk diagnostics for trend analysis."""
    try:
        async with AsyncSessionLocal() as db:
            record = await persist_alpha_validation_run(
                db,
                analysis_id=analysis_id,
                analyzed_at=datetime.utcnow(),
                trigger_type=trigger_type,
                risk_out=risk_out,
                evidence_bundle=evidence_bundle,
                execution_status="pre_execution_diagnostic",
            )
        risk_out["alpha_validation_run"] = record
        await _save_step_log(
            analysis_id,
            "6e_alpha_validation_persistence",
            "alpha_validation_persistence",
            input_data={
                "analysis_id": analysis_id,
                "trigger_type": trigger_type,
                "execution_effect": "diagnostic_only",
            },
            output_data=record,
            duration_ms=0,
        )
    except Exception as exc:
        logger.warning("[Stage6] Alpha validation persistence failed: %s", exc)
        risk_out["alpha_validation_run"] = {
            "status": "unavailable",
            "execution_authority": "none",
            "error": str(exc),
        }
        await _save_step_log(
            analysis_id,
            "6e_alpha_validation_persistence",
            "alpha_validation_persistence",
            input_data={
                "analysis_id": analysis_id,
                "trigger_type": trigger_type,
                "execution_effect": "diagnostic_only",
            },
            output_data=risk_out["alpha_validation_run"],
            duration_ms=0,
            failed=True,
        )


async def _persist_validation_observations_snapshot(
    *,
    analysis_id: int,
    trigger_type: str,
    risk_out: dict,
) -> None:
    """Persist durable observe-only validation observations for calibration."""
    try:
        analysis_stub = {
            "id": analysis_id,
            "analyzed_at": datetime.utcnow(),
            "trigger_type": trigger_type,
            "risk_output": risk_out,
            "execution_status": "pre_execution_diagnostic",
        }
        async with AsyncSessionLocal() as db:
            written = await persist_observations_for_analysis(db, analysis_stub)
        record = {
            "status": "ok",
            "observations_written": written,
            "contract_version": "validation_observation_loop_v1",
            "execution_authority": "none",
            "target_weight_mutation": "none",
        }
        risk_out["validation_observation_loop"] = record
        await _save_step_log(
            analysis_id,
            "6f_validation_observation_loop",
            "validation_observation_loop",
            input_data={
                "analysis_id": analysis_id,
                "trigger_type": trigger_type,
                "execution_effect": "diagnostic_only",
            },
            output_data=record,
            duration_ms=0,
        )
    except Exception as exc:
        logger.warning("[Stage6] Validation observation persistence failed: %s", exc)
        risk_out["validation_observation_loop"] = {
            "status": "unavailable",
            "execution_authority": "none",
            "target_weight_mutation": "none",
            "error": str(exc),
        }
        await _save_step_log(
            analysis_id,
            "6f_validation_observation_loop",
            "validation_observation_loop",
            input_data={
                "analysis_id": analysis_id,
                "trigger_type": trigger_type,
                "execution_effect": "diagnostic_only",
            },
            output_data=risk_out["validation_observation_loop"],
            duration_ms=0,
            failed=True,
        )


def _build_hedge_intent_plan(
    *,
    brief: dict,
    evidence_bundle: dict,
    market_scorecard: dict | None,
) -> dict:
    from services.hedge_intent import evaluate_hedge_intent

    current_weights = brief.get("current_weights") or {}
    market = evidence_bundle.get("market") or {}
    key_facts = brief.get("key_facts") or {}
    net_long = 0.0
    for weight in current_weights.values():
        try:
            value = float(weight or 0.0)
        except (TypeError, ValueError):
            value = 0.0
        if value > 0.0:
            net_long += value
    plan = evaluate_hedge_intent(
        vix_level=market.get("vix", 20.0),
        portfolio_drawdown_pct=market.get("drawdown_pct", key_facts.get("drawdown_pct", 0.0)),
        net_long_exposure=net_long,
        market_regime_raw=market.get("regime") or (market_scorecard or {}).get("market_condition") or "normal",
        current_holdings=current_weights,
        scorecard_requires_human=bool((market_scorecard or {}).get("require_human_confirmation")),
        market_breadth_pct=market.get("breadth_pct", key_facts.get("breadth_pct", 0.5)),
    )
    return plan.to_dict()


def _build_hedge_intent_outcome_record(
    *,
    brief: dict,
    evidence_bundle: dict,
    hedge_intent: dict,
) -> dict:
    from services.hedge_intent_outcome_log import build_hedge_intent_outcome_record

    market = dict(evidence_bundle.get("market") or {})
    key_facts = brief.get("key_facts") or {}
    if "drawdown_pct" not in market and key_facts.get("drawdown_pct") is not None:
        market["drawdown_pct"] = key_facts.get("drawdown_pct")
    if "breadth_pct" not in market and key_facts.get("breadth_pct") is not None:
        market["breadth_pct"] = key_facts.get("breadth_pct")
    return build_hedge_intent_outcome_record(
        hedge_intent=hedge_intent,
        market_context=market,
        current_weights=brief.get("current_weights") or {},
    )


def _hard_risk_tickers_from_governance(position_governance: dict | None) -> list[str]:
    tickers: list[str] = []
    for row in (position_governance or {}).get("position_decisions") or []:
        if not isinstance(row, dict):
            continue
        state = str(row.get("position_state") or "").lower()
        permission = str(row.get("action_permission") or "").lower()
        reasons = " ".join(str(item).lower() for item in row.get("why_hold") or [])
        if "hard_risk" in state or "hard-risk" in reasons or permission == "trim_or_exit":
            ticker = str(row.get("ticker") or "").upper().strip()
            if ticker:
                tickers.append(ticker)
    return tickers


def _portfolio_construction_regime_context(
    *,
    market_scorecard: dict | None,
    research_report: dict | None,
    quant_baseline: dict | None,
) -> dict[str, Any]:
    """Freeze the point-in-time regime label used for PC promotion readiness."""
    scorecard = market_scorecard if isinstance(market_scorecard, dict) else {}
    research = research_report if isinstance(research_report, dict) else {}
    quant = quant_baseline if isinstance(quant_baseline, dict) else {}
    research_regime = research.get("market_regime")
    if not isinstance(research_regime, dict):
        research_regime = {}
    quant_regime = quant.get("regime_result")
    if not isinstance(quant_regime, dict):
        quant_regime = {}

    regime = (
        scorecard.get("regime")
        or research_regime.get("regime")
        or research_regime.get("label")
        or quant_regime.get("regime")
        or scorecard.get("market_condition")
        or "unknown"
    )
    confidence = (
        scorecard.get("confidence")
        or research_regime.get("confidence")
        or quant_regime.get("confidence")
    )
    return {
        "schema_version": "pc_regime_context_v1",
        "source": "pipeline_point_in_time",
        "regime": str(regime or "unknown"),
        "confidence": confidence,
        "scorecard_market_condition": scorecard.get("market_condition"),
        "scorecard_permission": scorecard.get("investment_permission"),
        "researcher_regime": research_regime.get("regime"),
        "quant_regime": quant_regime.get("regime"),
        "point_in_time": True,
    }


def _tickers_from_forced_trim_strings(rows: list[str]) -> list[str]:
    tickers: list[str] = []
    for row in rows or []:
        ticker = str(row or "").strip().split(" ", 1)[0].upper()
        if ticker and ticker not in tickers:
            tickers.append(ticker)
    return tickers


def _scorecard_restricted_tickers(position_governance: dict | None) -> list[str]:
    tickers: list[str] = []
    for row in (position_governance or {}).get("position_decisions") or []:
        if not isinstance(row, dict):
            continue
        reasons = {str(item or "") for item in row.get("reason_codes") or []}
        permission = str(row.get("action_permission") or "")
        scorecard_reasons = {reason for reason in reasons if reason.startswith("scorecard_")}
        scorecard_only_human_required = scorecard_reasons == {"scorecard_human_required"}
        if (
            permission in {"hold_or_trim", "reduce_risk_only", "defensive_only", "cash_only", "trim_or_exit"}
            and not scorecard_only_human_required
        ):
            ticker = str(row.get("ticker") or "").upper().strip()
            if ticker and ticker not in tickers:
                tickers.append(ticker)
            continue
        if any(reason.startswith("scorecard_") and reason != "scorecard_human_required" for reason in reasons):
            ticker = str(row.get("ticker") or "").upper().strip()
            if ticker and ticker not in tickers:
                tickers.append(ticker)
    return tickers


def _position_governance_mutation_ledger(
    *,
    before: dict | None,
    after: dict | None,
    mutation_type: str,
) -> MutationLedger:
    ledger = MutationLedger()
    before_weights = _clean_weight_map(before)
    after_weights = _clean_weight_map(after)
    for ticker in sorted((set(before_weights) | set(after_weights)) - {"CASH"}):
        before_w = float(before_weights.get(ticker, 0.0) or 0.0)
        after_w = float(after_weights.get(ticker, 0.0) or 0.0)
        if after_w < before_w - 1e-9:
            ledger.record(
                mutation_type=mutation_type,
                ticker=ticker,
                before=before_w,
                after=after_w,
                reason="position_governance reduced post-risk target weight",
            )
    return ledger


def _clean_weight_map(weights: dict | None) -> dict[str, float]:
    out: dict[str, float] = {}
    for raw_ticker, raw_weight in (weights or {}).items():
        ticker = str(raw_ticker or "").upper().strip()
        if not ticker:
            continue
        try:
            out[ticker] = max(float(raw_weight or 0.0), 0.0)
        except (TypeError, ValueError):
            out[ticker] = 0.0
    return out


def _position_manager_min_hold_exempt_tickers(
    *,
    position_governance: dict | None,
    hard_risks_map: dict | None,
    critical_alerts: list[dict] | None,
) -> list[str]:
    tickers: set[str] = set()
    tickers.update(_hard_risk_tickers_from_governance(position_governance))
    tickers.update(_tickers_from_forced_trim_strings((position_governance or {}).get("forced_trims") or []))
    tickers.update(
        str(ticker or "").upper().strip()
        for ticker, risk in (hard_risks_map or {}).items()
        if ticker and risk
    )
    tickers.update(
        str((row or {}).get("ticker") or "").upper().strip()
        for row in critical_alerts or []
        if str((row or {}).get("ticker") or "").strip()
    )
    return sorted(ticker for ticker in tickers if ticker)


def _position_manager_turnover_exempt_tickers(
    *,
    position_governance: dict | None,
    hard_risks_map: dict | None,
    critical_alerts: list[dict] | None,
) -> list[str]:
    tickers: set[str] = set()
    tickers.update(_hard_risk_tickers_from_governance(position_governance))
    tickers.update(_tickers_from_forced_trim_strings((position_governance or {}).get("forced_trims") or []))
    tickers.update(_scorecard_restricted_tickers(position_governance))
    tickers.update(
        str(ticker or "").upper().strip()
        for ticker, risk in (hard_risks_map or {}).items()
        if ticker and risk
    )
    tickers.update(
        str((row or {}).get("ticker") or "").upper().strip()
        for row in critical_alerts or []
        if str((row or {}).get("ticker") or "").strip()
    )
    return sorted(ticker for ticker in tickers if ticker)


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
        "final_validation":   risk_out.get("final_validation") or {},
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


def _effective_portfolio_turnover_budget(
    market_scorecard: dict | None,
    decision_style: dict | None,
) -> float | None:
    values: list[float] = []
    for value in (
        (market_scorecard or {}).get("max_turnover_per_cycle"),
        ((decision_style or {}).get("style_limits") or {}).get("max_turnover_per_cycle"),
    ):
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            continue
        if parsed >= 0:
            values.append(parsed)
    return min(values) if values else None


async def _target_builder_construction_input(
    *,
    pipeline_context: dict,
    portfolio_construction_payload: dict | None,
) -> dict:
    pc_config = default_pc_promotion_config(
        pipeline_context.get("portfolio_construction_promotion_config") or {}
    )
    mode = str(pc_config.get("portfolio_construction_mode") or "shadow")
    payload = portfolio_construction_payload or {}
    gate: dict[str, Any] = {
        "status": "not_requested",
        "eligible": False,
        "portfolio_construction_mode": mode,
        "execution_authority": "none",
    }

    if mode == "gated":
        try:
            from services.portfolio_construction_evaluator import (
                build_portfolio_construction_rollout_gate,
                load_gated_semi_auto_confirmed_cycles,
                load_portfolio_construction_readiness,
                readiness_limits_from_pc_promotion_config,
            )

            readiness_limits = readiness_limits_from_pc_promotion_config(pc_config)
            readiness = await load_portfolio_construction_readiness(
                limit=readiness_limits["limit"],
                min_cycles=readiness_limits["min_cycles"],
                min_pass_rate=readiness_limits["min_pass_rate"],
                min_basket_policy_ok_rate=readiness_limits["min_basket_policy_ok_rate"],
                min_policy_ok_rate=readiness_limits["min_policy_ok_rate"],
                min_turnover_ok_rate=readiness_limits["min_turnover_ok_rate"],
                max_mean_weight_deviation=readiness_limits["max_mean_weight_deviation"],
                max_subscale_position_rate=readiness_limits["max_subscale_position_rate"],
                require_no_unclassified_mutations=readiness_limits["require_no_unclassified_mutations"],
                require_regime_coverage=readiness_limits["require_regime_coverage"],
                min_non_bull_regime_cycles=readiness_limits["min_non_bull_regime_cycles"],
                min_regime_confidence_for_coverage=readiness_limits[
                    "min_regime_confidence_for_coverage"
                ],
            )
            confirmed_cycles = await load_gated_semi_auto_confirmed_cycles(
                limit=max(
                    int(pc_config.get("min_gated_semi_auto_confirmed_cycles") or 5),
                    5,
                )
            )
            gate = build_portfolio_construction_rollout_gate(
                readiness,
                pc_config,
                auth_mode=pipeline_context.get("auth_mode", "SEMI_AUTO"),
                semi_auto_confirmed_cycles=int(confirmed_cycles.get("count") or 0),
            )
            pipeline_context["portfolio_construction_pre_target_readiness"] = readiness
            pipeline_context["portfolio_construction_pre_target_gate"] = gate
            pipeline_context["portfolio_construction_rollout"] = confirmed_cycles
        except Exception as exc:
            gate = {
                "status": "unavailable",
                "eligible": False,
                "portfolio_construction_mode": mode,
                "blockers": ["readiness_unavailable"],
                "error": str(exc),
                "execution_authority": "none",
            }
            pipeline_context["portfolio_construction_pre_target_gate"] = gate

    return construction_input_for_target_builder(
        portfolio_construction_payload=payload,
        promotion_gate=gate,
        config=pc_config,
    )


async def _finalize_analysis(
    analysis_id:    int,
    quant_baseline: dict,
    synthesizer_out: dict,
    risk_out:       dict,
) -> None:
    """Backfill complete data to analysis row when pipeline ends."""
    from sqlalchemy import update
    safe_synthesizer_out = _json_safe(synthesizer_out)
    safe_quant_baseline = _json_safe(quant_baseline)
    safe_risk_out = _json_safe(risk_out)
    async with AsyncSessionLocal() as db:
        await db.execute(
            update(AgentAnalysis)
            .where(AgentAnalysis.id == analysis_id)
            .values(
                researcher_output = safe_synthesizer_out,
                allocator_output  = safe_quant_baseline,
                risk_output       = safe_risk_out,
                risk_approved     = bool(safe_risk_out.get("approved", False)),
                execution_status  = "pending",
            )
        )
        await db.commit()


async def _update_analysis_risk_output(analysis_id: int, risk_out: dict) -> None:
    """Persist post-communicator audit fields without changing execution status."""
    from sqlalchemy import update
    try:
        safe_risk_out = _json_safe(risk_out)
        async with AsyncSessionLocal() as db:
            await db.execute(
                update(AgentAnalysis)
                .where(AgentAnalysis.id == analysis_id)
                .values(risk_output=safe_risk_out)
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
    from services.execution_log_store import update_execution_result

    audit_payload = result.get("execution_audit") or build_execution_audit_payload(
        action_status=result.get("execution_status", "failed"),
        sent_weights=result.get("weights_sent") or {},
        command_id=result.get("command_id"),
        reason=result.get("error"),
    )
    command_id = audit_payload.get("command_id") or result.get("command_id") or f"analysis_{analysis_id}"
    if result.get("policy_version") is not None:
        audit_payload["policy_version"] = result.get("policy_version")
    if result.get("preflight") is not None:
        audit_payload["command_preflight"] = result.get("preflight")
    if result.get("policy_sync") is not None:
        audit_payload["policy_sync"] = result.get("policy_sync")
    audit_payload = _json_safe(audit_payload)
    qc_response = _json_safe(result.get("qc_response")) if result.get("qc_response") is not None else None
    await update_execution_result(
        command_id=command_id,
        analysis_id=analysis_id,
        audit_payload=audit_payload,
        qc_response=qc_response,
        status=result.get("execution_status", "unknown"),
    )
    async with AsyncSessionLocal() as db:
        analysis = await db.get(AgentAnalysis, analysis_id)
        if analysis:
            risk_output = dict(analysis.risk_output or {})
            if risk_output.get("decision_ledger"):
                risk_output["decision_ledger"] = apply_execution_audit_to_decision_ledger(
                    risk_output.get("decision_ledger") or {},
                    audit_payload,
                )
                analysis.risk_output = _json_safe(risk_output)
        await db.commit()
