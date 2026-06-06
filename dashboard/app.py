"""Read-only operator dashboard.

Run as a separate Railway service:

    uvicorn dashboard.app:app --host 0.0.0.0 --port $PORT
"""
from __future__ import annotations

import os
import json
import secrets
from datetime import datetime
from html import escape
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.responses import HTMLResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from sqlalchemy import desc, func, select, text

for _key in (
    "OPENAI_API_KEY",
    "WEBHOOK_SECRET",
    "QC_USER_ID",
    "QC_API_TOKEN",
    "QC_PROJECT_ID",
    "TG_BOT_TOKEN",
    "TG_CHAT_ID",
):
    os.environ.setdefault(_key, "dashboard-unused")

from db.models import (
    AccountStateSnapshot,
    AgentAnalysis,
    AgentStepLog,
    AlphaValidationRun,
    CommandLifecycleEvent,
    DeferredExecutionLedger,
    CronRunLog,
    ExecutionLog,
    HoldingsFactor,
    PerformanceAttribution,
    PortfolioTimeseries,
    QCSnapshot,
    SystemConfig,
)
from db.session import AsyncSessionLocal
from services.operational_health import build_operational_health_snapshot
from services.playground import _recent_snapshot_row_limit
from services.evidence_cap_calibration import load_evidence_cap_calibration_report
from services.alpha_attribution_report import load_monthly_alpha_attribution_report
from services.hedge_intent_outcome_log import summarize_hedge_threshold_assessments
from services.validation_observation_loop import load_validation_observation_summary
from services.strategy_breadth_calibration import build_strategy_breadth_calibration_report
from services.command_lifecycle import build_reconciliation_lag_report
from services.weight_source_contract import (
    classify_weight_column,
    dashboard_weight_source_labels,
    weight_source_contract_summary,
)
from services.target_path_visibility import build_target_path_visibility
from services.weekend_review_operator_view import load_latest_weekend_review_operator_pack

DATA_QUALITY_AUDIT_NAME = "qc_yfinance_feature_parity"


app = FastAPI(
    title="QC Operator Dashboard",
    description="Read-only trading-agent observability dashboard.",
    version="0.1.0",
)
security = HTTPBasic()


def require_dashboard_auth(credentials: HTTPBasicCredentials = Depends(security)) -> str:
    user = os.getenv("DASHBOARD_USER")
    password = os.getenv("DASHBOARD_PASSWORD")
    if not user or not password:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Dashboard auth is not configured. Set DASHBOARD_USER and DASHBOARD_PASSWORD.",
        )
    ok = secrets.compare_digest(credentials.username, user) and secrets.compare_digest(credentials.password, password)
    if not ok:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid dashboard credentials",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/summary")
async def api_summary(_: str = Depends(require_dashboard_auth)) -> dict[str, Any]:
    return await build_dashboard_summary()


@app.get("/api/account-truth")
async def api_account_truth(_: str = Depends(require_dashboard_auth)) -> dict[str, Any]:
    latest_analysis = await _latest_analysis()
    execution_control = await _execution_control_status(latest_analysis)
    account_holdings = await _account_holdings_dashboard(latest_analysis)
    return _account_truth_view(account_holdings, execution_control)


@app.get("/", response_class=HTMLResponse)
async def index(_: str = Depends(require_dashboard_auth)) -> str:
    summary = await build_dashboard_summary()
    return render_dashboard(summary)


async def build_dashboard_summary() -> dict[str, Any]:
    ops = await build_operational_health_snapshot()
    latest_analysis = await _latest_analysis()
    pc_readiness = await _portfolio_construction_readiness()
    config = await _dashboard_config()
    pc_objective = _portfolio_construction_objective_status(
        latest_analysis,
        pc_readiness,
        config.get("portfolio_construction_promotion_config") or {},
    )
    strategy_evidence = _strategy_evidence_dashboard_status(
        latest_analysis.get("strategy_evidence") or {}
    )
    evidence_cap_calibration = await _evidence_cap_calibration_dashboard(
        config.get("evidence_cap_config") or {}
    )
    live_signal_conviction = await _live_signal_conviction_dashboard()
    performance_attribution = await _performance_attribution_dashboard()
    portfolio_risk_diagnostic = latest_analysis.get("portfolio_risk_diagnostic") or {}
    alpha_validation_trend = await _alpha_validation_trend_dashboard()
    strategy_regime_gap_analysis = await _strategy_regime_gap_analysis_dashboard()
    strategy_promotion_recommendations = await _strategy_promotion_recommendations_dashboard()
    alpha_decision_profiles = await _alpha_decision_profiles_dashboard()
    alpha_readiness_report = _alpha_readiness_report_dashboard(
        strategy_evidence=strategy_evidence,
        alpha_decision_profiles=alpha_decision_profiles,
    )
    alpha_decision_policy = _alpha_decision_policy_dashboard_status(
        config.get("alpha_decision_policy_config") or {},
        alpha_decision_profiles,
        evidence_cap_calibration,
    )
    alpha_decision_review_surface = _alpha_decision_review_surface_status(
        policy=alpha_decision_policy,
        profiles=alpha_decision_profiles,
        recommendations=strategy_promotion_recommendations,
        portfolio_construction=pc_objective,
        performance_attribution=performance_attribution,
        strategy_evidence=strategy_evidence,
    )
    hedge_calibration = await _hedge_intent_outcome_dashboard()
    validation_observation_loop = await _validation_observation_loop_dashboard()
    validation_overview = _validation_overview_dashboard(
        strategy_evidence=strategy_evidence,
        performance_attribution=performance_attribution,
        portfolio_construction=pc_objective,
        latest_analysis=latest_analysis,
        hedge_calibration=hedge_calibration,
        validation_observation_loop=validation_observation_loop,
        portfolio_risk_diagnostic=portfolio_risk_diagnostic,
    )
    cron_runs = await _latest_cron_runs()
    data_quality_audit = await _data_quality_audit_trend()
    execution = await _latest_execution()
    execution_control = await _execution_control_status(latest_analysis)
    account_holdings = await _account_holdings_dashboard(latest_analysis)
    account_truth = _account_truth_view(account_holdings, execution_control)
    account_holdings["truth"] = account_truth
    weekend_review_operator = await _weekend_review_operator_dashboard()
    replay = await _replay_diagnostics()
    return {
        "generated_at": datetime.utcnow().isoformat(),
        "ops": ops,
        "latest_analysis": latest_analysis,
        "account_holdings": account_holdings,
        "account_truth": account_truth,
        "portfolio_construction_objective": pc_objective,
        "portfolio_construction_readiness": pc_readiness,
        "strategy_evidence": strategy_evidence,
        "evidence_cap_calibration": evidence_cap_calibration,
        "live_signal_conviction": live_signal_conviction,
        "performance_attribution": performance_attribution,
        "portfolio_risk_diagnostic": portfolio_risk_diagnostic,
        "alpha_validation_trend": alpha_validation_trend,
        "strategy_regime_gap_analysis": strategy_regime_gap_analysis,
        "strategy_promotion_recommendations": strategy_promotion_recommendations,
        "alpha_decision_profiles": alpha_decision_profiles,
        "alpha_readiness_report": alpha_readiness_report,
        "alpha_decision_policy": alpha_decision_policy,
        "alpha_decision_review_surface": alpha_decision_review_surface,
        "hedge_calibration": hedge_calibration,
        "validation_observation_loop": validation_observation_loop,
        "validation_overview": validation_overview,
        "cron_runs": cron_runs,
        "data_quality_audit": data_quality_audit,
        "execution": execution,
        "execution_control": execution_control,
        "weekend_review_operator": weekend_review_operator,
        "replay": replay,
        "weight_source_contract": _weight_source_contract_dashboard(),
        "config": config,
    }


def _weight_source_contract_dashboard() -> dict[str, Any]:
    contract = weight_source_contract_summary()
    return {
        "available": True,
        **contract,
        "labels": dashboard_weight_source_labels(),
        "execution_authority": "display_contract_only",
    }


async def _weekend_review_operator_dashboard() -> dict[str, Any]:
    try:
        pack = await load_latest_weekend_review_operator_pack(include_full_report=False)
    except Exception as exc:  # pragma: no cover - dashboard should degrade instead of failing.
        return {
            "available": False,
            "reason": f"weekend_review_unavailable:{type(exc).__name__}",
            "execution_authority": "none",
            "target_weight_mutation": "none",
        }
    if not pack:
        return {
            "available": False,
            "reason": "no_weekend_review_found",
            "execution_authority": "none",
            "target_weight_mutation": "none",
        }
    return {
        "available": True,
        **pack,
    }


async def _hedge_intent_outcome_dashboard(limit: int = 30) -> dict[str, Any]:
    limit = max(min(int(limit or 30), 100), 1)
    try:
        async with AsyncSessionLocal() as db:
            rows = (
                await db.execute(
                    select(AgentAnalysis.id, AgentAnalysis.analyzed_at, AgentAnalysis.risk_output)
                    .order_by(desc(AgentAnalysis.analyzed_at), desc(AgentAnalysis.id))
                    .limit(max(limit * 3, limit))
                )
            ).all()
    except Exception as exc:
        return {
            "available": False,
            "reason": f"{type(exc).__name__}: {exc}",
            "summary": {},
            "recent_rows": [],
            "severity_distribution": {},
            "no_hedge_followed_by_drawdown_count": 0,
            "hedge_buy_followed_by_rebound_loss_count": 0,
            "execution_authority": "none",
            "target_weight_mutation": "none",
        }

    outcome_rows: list[dict[str, Any]] = []
    for analysis_id, analyzed_at, risk_output in rows:
        risk = risk_output if isinstance(risk_output, dict) else {}
        outcome = risk.get("hedge_intent_outcome") if isinstance(risk, dict) else None
        if not isinstance(outcome, dict) or not outcome:
            continue
        compact = _compact_hedge_intent_outcome(outcome)
        compact["analysis_id"] = analysis_id
        compact["analyzed_at"] = _iso(analyzed_at)
        outcome_rows.append(compact)
        if len(outcome_rows) >= limit:
            break

    summary = summarize_hedge_threshold_assessments(outcome_rows, limit=limit)
    return {
        "available": bool(outcome_rows),
        "reason": "" if outcome_rows else "no hedge intent outcome rows found",
        "summary": summary,
        "recent_rows": outcome_rows,
        "severity_distribution": _hedge_severity_distribution(outcome_rows),
        "no_hedge_followed_by_drawdown_count": _count_no_hedge_followed_by_drawdown(outcome_rows),
        "hedge_buy_followed_by_rebound_loss_count": _count_hedge_buy_followed_by_rebound_loss(outcome_rows),
        "execution_authority": summary.get("execution_authority", "none"),
        "target_weight_mutation": summary.get("target_weight_mutation", "none"),
    }


async def _validation_observation_loop_dashboard(limit: int = 50) -> dict[str, Any]:
    try:
        async with AsyncSessionLocal() as db:
            return await load_validation_observation_summary(db, limit=limit)
    except Exception as exc:
        return {
            "available": False,
            "reason": f"{type(exc).__name__}: {exc}",
            "contract_version": "validation_observation_loop_v1",
            "execution_authority": "none",
            "target_weight_mutation": "none",
            "observation_counts": {},
            "hedge_threshold_summary": {},
            "recent_observations": [],
        }


def _hedge_severity_distribution(rows: list[dict[str, Any]]) -> dict[str, int]:
    buckets = {"lt_0_40": 0, "0_40_to_0_70": 0, "gte_0_70": 0, "missing": 0}
    for row in rows:
        severity = _json_safe_number(row.get("severity"))
        if severity is None:
            buckets["missing"] += 1
        elif severity < 0.40:
            buckets["lt_0_40"] += 1
        elif severity < 0.70:
            buckets["0_40_to_0_70"] += 1
        else:
            buckets["gte_0_70"] += 1
    return buckets


def _count_no_hedge_followed_by_drawdown(rows: list[dict[str, Any]]) -> int:
    return sum(
        1
        for row in rows
        if bool(row.get("triggered"))
        and not bool(row.get("add_hedge_etf"))
        and (_json_safe_number(row.get("spy_return_5d")) or 0.0) <= -0.03
    )


def _count_hedge_buy_followed_by_rebound_loss(rows: list[dict[str, Any]]) -> int:
    return sum(
        1
        for row in rows
        if bool(row.get("add_hedge_etf"))
        and (_json_safe_number(row.get("spy_return_5d")) or 0.0) >= 0.02
        and (_json_safe_number(row.get("hedge_instrument_return_5d")) or 0.0) < 0.0
    )


def _validation_overview_dashboard(
    *,
    strategy_evidence: dict[str, Any],
    performance_attribution: dict[str, Any],
    portfolio_construction: dict[str, Any],
    latest_analysis: dict[str, Any],
    hedge_calibration: dict[str, Any],
    validation_observation_loop: dict[str, Any],
    portfolio_risk_diagnostic: dict[str, Any],
) -> dict[str, Any]:
    monthly = performance_attribution.get("monthly_alpha_report") or {}
    breadth = strategy_evidence.get("strategy_breadth_calibration") or {}
    active_basket = portfolio_construction.get("active_basket_policy") or {}
    basket_calibration = portfolio_construction.get("active_basket_calibration") or {}
    readiness = portfolio_construction.get("readiness") or {}
    hedge_outcome = latest_analysis.get("hedge_intent_outcome") or {}
    hedge_summary = hedge_calibration.get("summary") or {}
    observation_summary = validation_observation_loop.get("hedge_threshold_summary") or {}
    risk_summary = (portfolio_risk_diagnostic or {}).get("summary") or {}

    return {
        "available": True,
        "report_version": "alpha_basket_hedge_validation_overview_v1",
        "execution_authority": "none",
        "target_weight_mutation": "none",
        "alpha_evidence_panel": {
            "sample_status": monthly.get("sample_status"),
            "sample_count": monthly.get("sample_count"),
            "alpha_t_stat": monthly.get("alpha_t_stat"),
            "beta_vs_spy": monthly.get("beta_vs_spy"),
            "r_squared": monthly.get("r_squared"),
            "meets_harvey_t3_threshold": monthly.get("meets_harvey_t3_threshold"),
            "honest_interpretation": monthly.get("honest_interpretation"),
            "estimated_independent_clusters": breadth.get("estimated_independent_clusters"),
            "duplication_ratio": breadth.get("duplication_ratio"),
            "high_correlation_pair_count": len(breadth.get("high_correlation_pairs") or []),
            "diversifying_pair_count": len(breadth.get("diversifying_pairs") or []),
        },
        "active_basket_panel": {
            "active_count": active_basket.get("active_count"),
            "target_active_count_min": active_basket.get("target_active_count_min"),
            "target_active_count_max": active_basket.get("target_active_count_max"),
            "within_target_active_count": active_basket.get("within_target_active_count"),
            "subscale_count": active_basket.get("subscale_count"),
            "floor_cleared_count": active_basket.get("floor_cleared_count"),
            "suggested_range": basket_calibration.get("suggested_range"),
            "suggestion": basket_calibration.get("suggestion"),
            "suggestion_reason": basket_calibration.get("suggestion_reason") or [],
            "readiness_status": readiness.get("status"),
            "readiness_ready": readiness.get("ready"),
            "readiness_cycles": readiness.get("cycles"),
            "readiness_blockers": readiness.get("blockers") or [],
        },
        "hedge_calibration_panel": {
            "triggered": hedge_outcome.get("triggered"),
            "severity": hedge_outcome.get("severity"),
            "add_hedge_etf": hedge_outcome.get("add_hedge_etf"),
            "selected_instrument": hedge_outcome.get("selected_instrument"),
            "candidate_hedge_instrument": hedge_outcome.get("candidate_hedge_instrument"),
            "why_not_add_hedge": hedge_outcome.get("why_not_add_hedge"),
            "outcome_status": hedge_outcome.get("outcome_status"),
            "threshold_assessment": hedge_outcome.get("threshold_assessment"),
            "hedge_would_have_helped": hedge_outcome.get("hedge_would_have_helped"),
            "recent_sampled": hedge_summary.get("sampled"),
            "pending_count": hedge_summary.get("pending_count"),
            "assessment_counts": hedge_summary.get("assessment_counts") or {},
            "durable_sampled": observation_summary.get("sampled"),
            "durable_pending_count": observation_summary.get("pending_count"),
            "durable_assessment_counts": observation_summary.get("assessment_counts") or {},
            "severity_distribution": hedge_calibration.get("severity_distribution") or {},
            "no_hedge_followed_by_drawdown_count": hedge_calibration.get("no_hedge_followed_by_drawdown_count"),
            "hedge_buy_followed_by_rebound_loss_count": hedge_calibration.get("hedge_buy_followed_by_rebound_loss_count"),
        },
        "validation_observation_panel": {
            "sampled": validation_observation_loop.get("sampled"),
            "observation_counts": validation_observation_loop.get("observation_counts") or {},
            "recent_count": len(validation_observation_loop.get("recent_observations") or []),
            "execution_authority": validation_observation_loop.get("execution_authority"),
            "target_weight_mutation": validation_observation_loop.get("target_weight_mutation"),
        },
        "stress_diagnostic_panel": {
            "max_current_historical_scenario_loss": risk_summary.get("max_current_historical_scenario_loss"),
            "max_target_historical_scenario_loss": risk_summary.get("max_target_historical_scenario_loss"),
            "max_current_beta_shock_loss": risk_summary.get("max_current_beta_shock_loss"),
            "max_target_beta_shock_loss": risk_summary.get("max_target_beta_shock_loss"),
            "max_current_scenario_loss": risk_summary.get("max_current_scenario_loss"),
            "max_target_scenario_loss": risk_summary.get("max_target_scenario_loss"),
        },
        "recent_hedge_outcome_rows": hedge_calibration.get("recent_rows") or [],
        "diagnostics_first_contract": {
            "alpha_authority": monthly.get("execution_authority"),
            "breadth_authority": breadth.get("execution_authority"),
            "basket_authority": basket_calibration.get("execution_authority"),
            "hedge_authority": hedge_outcome.get("execution_authority"),
            "portfolio_risk_authority": (portfolio_risk_diagnostic or {}).get("execution_authority"),
            "no_execution_blocker_introduced": True,
        },
    }


async def _account_holdings_dashboard(latest_analysis: dict[str, Any]) -> dict[str, Any]:
    async with AsyncSessionLocal() as db:
        latest_account = (
            await db.execute(
                select(AccountStateSnapshot)
                .order_by(desc(AccountStateSnapshot.recorded_at), desc(AccountStateSnapshot.id))
                .limit(1)
            )
        ).scalar_one_or_none()
        portfolio_rows = (
            await db.execute(
                select(PortfolioTimeseries)
                .order_by(desc(PortfolioTimeseries.recorded_at), desc(PortfolioTimeseries.id))
                .limit(30)
            )
        ).scalars().all()
        factor_snapshot_id = (
            await db.execute(
                select(HoldingsFactor.snapshot_id)
                .order_by(desc(HoldingsFactor.recorded_at), desc(HoldingsFactor.id))
                .limit(1)
            )
        ).scalar_one_or_none()
        factor_rows = []
        if factor_snapshot_id is not None:
            factor_rows = (
                await db.execute(
                    select(HoldingsFactor)
                    .where(HoldingsFactor.snapshot_id == factor_snapshot_id)
                    .order_by(HoldingsFactor.ticker)
                )
            ).scalars().all()
        recent_command_rows = (
            await db.execute(
                select(ExecutionLog)
                .where(ExecutionLog.command_type == "weight_adjustment")
                .order_by(desc(ExecutionLog.executed_at), desc(ExecutionLog.id))
                .limit(10)
            )
        ).scalars().all()

    portfolio_series = [_compact_portfolio_timeseries_row(row) for row in reversed(portfolio_rows)]
    latest_portfolio = portfolio_series[-1] if portfolio_series else {}
    latest_command_target = _latest_command_target_weights(recent_command_rows)
    holdings = _account_holdings_rows(
        latest_account=latest_account,
        factor_rows=factor_rows,
        latest_analysis=latest_analysis,
        latest_command_target=latest_command_target,
    )
    account = _account_overview_from_rows(
        latest_account=latest_account,
        latest_portfolio=latest_portfolio,
        portfolio_series=portfolio_series,
        holdings=holdings,
    )
    return {
        "available": bool(latest_account or portfolio_series or holdings),
        "account": account,
        "nav_series": portfolio_series,
        "pnl_series": portfolio_series,
        "holdings": holdings,
        "signals": _account_key_signals(holdings),
        "contract": {
            "truth_source": "latest account_state_snapshots holdings_weights",
            "target_source": "account_state_snapshots target_weights, then latest execution_log command target",
            "return_source": "latest holdings_factors daily_return_pct",
            "holding_days_source": "QC in-memory holding_days counter; resets when QC algorithm restarts",
            "contribution_formula": "contribution_pct = weight_current * daily_return_pct * 100",
            "sorting": "client-side by table data-sort-key",
        },
    }


def _account_truth_view(
    account_holdings: dict[str, Any],
    execution_control: dict[str, Any],
) -> dict[str, Any]:
    """Single read-only account truth contract for operator API and dashboard UI."""
    account = account_holdings.get("account") or {}
    holdings = account_holdings.get("holdings") or []
    active = execution_control.get("active_execution") or {}
    snapshot = execution_control.get("latest_account_snapshot") or {}
    recent_commands = execution_control.get("recent_commands") or []
    latest_command = recent_commands[0] if recent_commands else {}
    drift_rows = _account_truth_drift_rows(holdings)
    max_abs_drift = max((abs(float(row.get("weight_drift") or 0.0)) for row in drift_rows), default=0.0)
    reconciliation_status = _account_truth_reconciliation_status(
        account=account,
        active=active,
        drift_rows=drift_rows,
        execution_control=execution_control,
    )
    execution_state = (
        active.get("status")
        or latest_command.get("display_status")
        or latest_command.get("execution_state")
        or snapshot.get("active_execution_status")
        or "unknown"
    )
    last_command_id = (
        account.get("last_command_id")
        or snapshot.get("last_command_id")
        or latest_command.get("command_id")
    )
    return {
        "schema_version": "account_truth_view_v1",
        "available": bool(account_holdings.get("available") or execution_control.get("available")),
        "truth_source": "qc_account_state_snapshot",
        "snapshot_id": snapshot.get("id"),
        "source_packet_type": account.get("source_packet_type") or snapshot.get("source_packet_type"),
        "snapshot_recorded_at": snapshot.get("recorded_at"),
        "snapshot_age_min": account.get("snapshot_age_min"),
        "nav": account.get("nav"),
        "cash_pct": account.get("cash_pct"),
        "buying_power": account.get("buying_power") or snapshot.get("buying_power"),
        "open_orders": account.get("open_orders"),
        "holding_count": account.get("holding_count"),
        "last_command_id": last_command_id,
        "active_command_id": active.get("active_command_id") or snapshot.get("active_command_id"),
        "active_execution_status": snapshot.get("active_execution_status"),
        "execution_state": execution_state,
        "latest_command": latest_command,
        "reconciliation_status": reconciliation_status,
        "max_abs_drift_pct": round(max_abs_drift, 4),
        "drift_rows": drift_rows[:12],
        "operator_questions": {
            "qc_actual_holdings": "Holdings table is sourced from latest AccountStateSnapshot holdings_weights plus QC holding detail rows when present.",
            "fastapi_expected_reconciled": (
                "reconciliation_status answers whether FastAPI target and QC actual holdings are aligned within dashboard drift tolerance."
            ),
        },
    }


def _account_truth_drift_rows(holdings: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for row in holdings or []:
        ticker = str(row.get("ticker") or "").upper().strip()
        if not ticker or ticker == "CASH":
            continue
        drift = _json_safe_number(row.get("weight_drift"))
        if drift is None:
            continue
        rows.append(
            {
                "ticker": ticker,
                "weight_current": row.get("weight_current"),
                "weight_target": row.get("weight_target"),
                "weight_drift": round(float(drift), 4),
                "target_source": row.get("target_source"),
            }
        )
    rows.sort(key=lambda item: (-abs(float(item.get("weight_drift") or 0.0)), str(item.get("ticker") or "")))
    return rows


def _account_truth_reconciliation_status(
    *,
    account: dict[str, Any],
    active: dict[str, Any],
    drift_rows: list[dict[str, Any]],
    execution_control: dict[str, Any],
) -> str:
    if not account:
        return "no_account_snapshot"
    if active.get("active"):
        return "active_execution"
    lag = execution_control.get("reconciliation_lag") or {}
    if int(lag.get("overdue_count") or 0) > 0:
        return "pending_reconciliation"
    if not any(row.get("weight_target") is not None for row in drift_rows):
        return "no_target"
    max_abs_drift = max((abs(float(row.get("weight_drift") or 0.0)) for row in drift_rows), default=0.0)
    if max_abs_drift > 0.5:
        return "drift_present"
    return "reconciled"


def _compact_portfolio_timeseries_row(row: PortfolioTimeseries) -> dict[str, Any]:
    return {
        "recorded_at": _iso(row.recorded_at),
        "date_label": row.recorded_at.strftime("%m-%d") if row.recorded_at else "",
        "total_value": _json_safe_number(row.total_value),
        "cash_pct": _ratio_decimal(row.cash_pct),
        "daily_pnl_pct": _ratio_decimal(row.daily_pnl_pct),
        "current_drawdown_pct": _ratio_decimal(row.current_drawdown_pct),
        "regime_label": row.regime_label,
        "vix": _json_safe_number(row.vix),
    }


def _account_holdings_rows(
    *,
    latest_account: AccountStateSnapshot | None,
    factor_rows: list[HoldingsFactor],
    latest_analysis: dict[str, Any],
    latest_command_target: dict[str, float],
) -> list[dict[str, Any]]:
    factors = {str(row.ticker or "").upper(): row for row in factor_rows if row.ticker}
    holding_details = _account_holding_detail_by_ticker(latest_account)
    factor_targets_available = any(
        abs(_ratio_decimal(getattr(row, "weight_target", None)) or 0.0) > 0.00001
        for row in factor_rows
    )
    account_holdings = _weight_map_from_snapshot(getattr(latest_account, "holdings_weights", None))
    account_targets = _weight_map_from_snapshot(getattr(latest_account, "target_weights", None))
    target_source = "account_snapshot" if account_targets else ""
    if not account_targets and latest_command_target:
        account_targets = dict(latest_command_target)
        target_source = "latest_command"
    action_map = _governance_action_by_ticker(latest_analysis)
    tickers = set(factors) | set(account_holdings) | set(account_targets)
    cash_current = _ratio_decimal(getattr(latest_account, "cash_pct", None))
    if cash_current is not None:
        tickers.add("CASH")
        account_holdings.setdefault("CASH", cash_current)
    if account_targets and "CASH" not in account_targets:
        target_cash = 1.0 - sum(value for ticker, value in account_targets.items() if ticker != "CASH")
        if target_cash >= 0:
            account_targets["CASH"] = target_cash

    rows: list[dict[str, Any]] = []
    for ticker in sorted(tickers):
        factor = factors.get(ticker)
        detail = holding_details.get(ticker) or {}
        role = "cash" if ticker == "CASH" else (getattr(factor, "universe_role", None) or "unknown")
        current = account_holdings.get(ticker)
        if current is None and factor is not None:
            current = _ratio_decimal(getattr(factor, "weight_current", None))
        target = account_targets.get(ticker)
        if target is None and not account_targets and factor_targets_available and factor is not None:
            target = _ratio_decimal(getattr(factor, "weight_target", None))
            if target is not None and target > 0:
                target_source = target_source or "holdings_factor"
        current = float(current or 0.0)
        target_value = float(target) if target is not None else None
        if ticker != "CASH" and abs(current) < 0.00001 and abs(target_value or 0.0) < 0.00001:
            continue
        daily_return = _ratio_decimal(getattr(factor, "daily_return_pct", None)) if factor is not None else 0.0
        unrealized = _ratio_decimal(getattr(factor, "unrealized_pnl_pct", None)) if factor is not None else 0.0
        contribution = current * float(daily_return or 0.0) * 100.0
        drift = (current - target_value) if target_value is not None else None
        rows.append({
            "ticker": ticker,
            "role": role,
            "weight_current": round(current * 100.0, 4),
            "weight_target": round(target_value * 100.0, 4) if target_value is not None else None,
            "weight_drift": round(float(drift) * 100.0, 4) if drift is not None else None,
            "daily_return_pct": round(float(daily_return or 0.0) * 100.0, 4),
            "contribution_pct": round(contribution, 6),
            "unrealized_pnl_pct": round(float(unrealized or 0.0) * 100.0, 4),
            "holding_days": int(getattr(factor, "holding_days", None) or 0),
            "holding_days_source": "qc_in_memory_counter",
            "target_source": target_source or "unavailable",
            "action": action_map.get(ticker, "normal_hold"),
            "quantity": detail.get("quantity"),
            "average_price": detail.get("average_price"),
            "market_value": detail.get("market_value"),
            "unrealized_pnl": detail.get("unrealized_pnl"),
            "price": detail.get("market_price") or (_json_safe_number(getattr(factor, "price", None)) if factor is not None else None),
            "recorded_at": _iso(getattr(factor, "recorded_at", None)) if factor is not None else None,
        })
    return sorted(rows, key=lambda row: (-float(row.get("contribution_pct") or 0.0), str(row.get("ticker") or "")))


def _account_holding_detail_by_ticker(latest_account: AccountStateSnapshot | None) -> dict[str, dict[str, Any]]:
    if not latest_account:
        return {}
    raw = latest_account.raw_snapshot if isinstance(getattr(latest_account, "raw_snapshot", None), dict) else {}
    containers = [
        raw.get("holdings_detail_rows"),
        raw.get("holdings"),
        raw.get("positions"),
    ]
    account_state = raw.get("account_state") if isinstance(raw.get("account_state"), dict) else {}
    containers.extend([
        account_state.get("holdings"),
        account_state.get("positions"),
    ])
    out: dict[str, dict[str, Any]] = {}
    for container in containers:
        if not isinstance(container, list):
            continue
        for row in container:
            if not isinstance(row, dict):
                continue
            ticker = str(row.get("ticker") or row.get("symbol") or "").upper().strip()
            if not ticker:
                continue
            out[ticker] = {
                "quantity": _first_json_number(row, ("quantity", "qty", "shares")),
                "average_price": _first_json_number(row, ("average_price", "avg_price", "averagePrice", "avgPrice")),
                "market_price": _first_json_number(row, ("market_price", "price", "current_price", "last_price")),
                "market_value": _first_json_number(row, ("market_value", "value", "holdings_value")),
                "unrealized_pnl": _first_json_number(row, ("unrealized_pnl", "unrealized", "unrealized_profit")),
            }
    return out


def _first_json_number(row: dict[str, Any], keys: tuple[str, ...]) -> float | None:
    for key in keys:
        value = _json_safe_number(row.get(key))
        if value is not None:
            return value
    return None


def _latest_command_target_weights(rows: list[ExecutionLog]) -> dict[str, float]:
    for row in rows or []:
        payload = row.command_payload if isinstance(row.command_payload, dict) else {}
        response = row.qc_response if isinstance(row.qc_response, dict) else {}
        for candidate in (
            response.get("actual_target_weights"),
            payload.get("sent_weights"),
            payload.get("proposed_weights"),
            response.get("target_weights"),
        ):
            weights = _weight_map_from_snapshot(candidate)
            if weights:
                return weights
    return {}


def _account_overview_from_rows(
    *,
    latest_account: AccountStateSnapshot | None,
    latest_portfolio: dict[str, Any],
    portfolio_series: list[dict[str, Any]],
    holdings: list[dict[str, Any]],
) -> dict[str, Any]:
    total_value = _json_safe_number(getattr(latest_account, "total_value", None))
    if total_value is None:
        total_value = latest_portfolio.get("total_value")
    cash_pct = _ratio_decimal(getattr(latest_account, "cash_pct", None))
    if cash_pct is None:
        cash_pct = latest_portfolio.get("cash_pct")
    snapshot_age = _age_minutes(getattr(latest_account, "recorded_at", None))
    return {
        "nav": total_value,
        "cash_pct": round(float(cash_pct or 0.0) * 100.0, 4),
        "buying_power": _json_safe_number(getattr(latest_account, "buying_power", None)),
        "daily_pnl_pct": _series_latest_pct(latest_portfolio, "daily_pnl_pct"),
        "week_pnl_pct": _series_window_return_pct(portfolio_series, 5),
        "month_pnl_pct": _series_window_return_pct(portfolio_series, 22),
        "drawdown_pct": _series_latest_pct(latest_portfolio, "current_drawdown_pct"),
        "open_orders": int(getattr(latest_account, "open_order_count", None) or 0),
        "snapshot_age_min": snapshot_age,
        "source_packet_type": getattr(latest_account, "source_packet_type", None),
        "last_command_id": getattr(latest_account, "last_command_id", None),
        "active_command_id": getattr(latest_account, "active_command_id", None),
        "active_execution_status": getattr(latest_account, "active_execution_status", None),
        "account_status": getattr(latest_account, "account_status", None),
        "data_status": getattr(latest_account, "data_status", None),
        "policy_version": getattr(latest_account, "policy_version", None),
        "holding_count": len([row for row in holdings if row.get("ticker") != "CASH"]),
        "total_contribution_pct": round(sum(float(row.get("contribution_pct") or 0.0) for row in holdings), 6),
    }


def _account_key_signals(holdings: list[dict[str, Any]]) -> dict[str, Any]:
    non_cash = [row for row in holdings if row.get("ticker") != "CASH"]
    positives = [row for row in non_cash if float(row.get("contribution_pct") or 0.0) > 0]
    negatives = [row for row in non_cash if float(row.get("contribution_pct") or 0.0) < 0]
    return {
        "top_contributor": max(positives, key=lambda row: float(row.get("contribution_pct") or 0.0), default=None),
        "top_dragger": min(negatives, key=lambda row: float(row.get("contribution_pct") or 0.0), default=None),
        "largest_drift": max(non_cash, key=lambda row: abs(float(row.get("weight_drift") or 0.0)), default=None),
        "longest_hold": max(non_cash, key=lambda row: int(row.get("holding_days") or 0), default=None),
        "contributor_count": len(positives),
        "dragger_count": len(negatives),
    }


def _governance_action_by_ticker(latest_analysis: dict[str, Any]) -> dict[str, str]:
    governance = latest_analysis.get("position_governance") or {}
    rows = governance.get("position_explanations") or []
    out: dict[str, str] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        ticker = str(row.get("ticker") or "").upper().strip()
        if not ticker:
            continue
        action = (
            row.get("position_state")
            or row.get("decision")
            or row.get("action_permission")
            or row.get("strategy_intent")
            or "normal_hold"
        )
        out[ticker] = str(action).lower().replace(" ", "_")
    return out


def _weight_map_from_snapshot(raw: Any) -> dict[str, float]:
    if not isinstance(raw, dict):
        return {}
    out: dict[str, float] = {}
    for key, value in raw.items():
        ticker = str(key or "").upper().strip()
        ratio = _ratio_decimal(value)
        if ticker and ratio is not None:
            out[ticker] = ratio
    return out


def _ratio_decimal(value: Any) -> float | None:
    number = _json_safe_number(value)
    if number is None:
        return None
    if abs(number) > 1.5:
        return number / 100.0
    return number


def _age_minutes(value: Any) -> float | None:
    if not hasattr(value, "replace"):
        return None
    try:
        return round(max((datetime.utcnow() - value).total_seconds(), 0.0) / 60.0, 2)
    except TypeError:
        return None


def _series_latest_pct(row: dict[str, Any], key: str) -> float | None:
    value = row.get(key)
    if value is None:
        return None
    return round(float(value) * 100.0, 4)


def _series_window_return_pct(rows: list[dict[str, Any]], lookback_rows: int) -> float | None:
    if len(rows) < 2:
        return None
    window = rows[-lookback_rows:] if len(rows) >= lookback_rows else rows
    first = _json_safe_number(window[0].get("total_value"))
    last = _json_safe_number(window[-1].get("total_value"))
    if not first or last is None:
        return None
    return round((last / first - 1.0) * 100.0, 4)


async def _latest_analysis() -> dict[str, Any]:
    async with AsyncSessionLocal() as db:
        row = (
            await db.execute(
                select(AgentAnalysis).order_by(desc(AgentAnalysis.analyzed_at)).limit(1)
            )
        ).scalar_one_or_none()
    if not row:
        return {"available": False}
    stage_metrics = await _latest_stage_metrics(int(row.id))
    strategy_evidence = await _latest_strategy_evidence(int(row.id))

    risk = row.risk_output or {}
    decision = row.decision or {}
    scorecard = (
        (risk.get("market_scorecard") if isinstance(risk, dict) else None)
        or (decision.get("market_scorecard") if isinstance(decision, dict) else None)
        or {}
    )
    governance = (risk.get("position_governance") if isinstance(risk, dict) else None) or {}
    strategy_detail = (
        (risk.get("data_quality_detail") if isinstance(risk, dict) else None)
        or (decision.get("data_quality_detail") if isinstance(decision, dict) else None)
        or {}
    )
    feature_source_summary = (
        (risk.get("feature_source_summary") if isinstance(risk, dict) else None)
        or strategy_detail.get("feature_source_summary")
        or {}
    )
    pc_payload = (
        (risk.get("portfolio_construction_candidate") if isinstance(risk, dict) else None)
        or (risk.get("portfolio_construction_shadow") if isinstance(risk, dict) else None)
        or {}
    )
    ledger = (risk.get("decision_ledger") if isinstance(risk, dict) else None) or {}
    compact_ledger = _compact_ledger(ledger)
    return {
        "available": True,
        "id": row.id,
        "analyzed_at": _iso(row.analyzed_at),
        "trigger_type": row.trigger_type,
        "risk_approved": bool(row.risk_approved),
        "execution_status": row.execution_status,
        "scorecard": _compact_scorecard(scorecard),
        "strategy_detail": strategy_detail,
        "feature_source_summary": feature_source_summary,
        "weight_source_contract": _weight_source_contract_dashboard(),
        "strategy_evidence": strategy_evidence,
        "position_governance": _compact_governance(governance, ledger),
        "decision_ledger": compact_ledger,
        "portfolio_construction_payload": _compact_portfolio_construction_payload(pc_payload),
        "portfolio_construction_evaluation": _compact_portfolio_construction_evaluation(
            (risk.get("portfolio_construction_evaluation") if isinstance(risk, dict) else None) or {}
        ),
        "portfolio_construction_promotion_gate": _compact_portfolio_construction_promotion_gate(
            (risk.get("portfolio_construction_promotion_gate") if isinstance(risk, dict) else None) or {}
        ),
        "hedge_intent_outcome": _compact_hedge_intent_outcome(
            (risk.get("hedge_intent_outcome") if isinstance(risk, dict) else None) or {}
        ),
        "final_validation": _compact_final_validation(
            (risk.get("final_validation") if isinstance(risk, dict) else None) or {}
        ),
        "target_path_visibility": build_target_path_visibility(risk),
        "transaction_cost_gate": _compact_transaction_cost_gate(
            (risk.get("transaction_cost_gate") if isinstance(risk, dict) else None) or {}
        ),
        "portfolio_risk_diagnostic": _compact_portfolio_risk_diagnostic(
            (risk.get("portfolio_risk_diagnostic") if isinstance(risk, dict) else None) or {}
        ),
        "account_state_guard": _compact_account_state_guard(
            (risk.get("account_state_guard") if isinstance(risk, dict) else None) or {}
        ),
        "auto_pause": _compact_auto_pause(
            (risk.get("auto_pause") if isinstance(risk, dict) else None) or {}
        ),
        "stage_metrics": stage_metrics,
        "rejection_reasons": (risk.get("rejection_reasons") if isinstance(risk, dict) else []) or [],
    }


async def _portfolio_construction_readiness() -> dict[str, Any]:
    try:
        from services.portfolio_construction_evaluator import load_portfolio_construction_readiness

        return await load_portfolio_construction_readiness(limit=20, min_cycles=20, min_pass_rate=0.90)
    except Exception as exc:
        return {"status": "unavailable", "error": str(exc), "execution_authority": "none"}


async def _latest_stage_metrics(analysis_id: int) -> list[dict[str, Any]]:
    async with AsyncSessionLocal() as db:
        rows = (
            await db.execute(
                select(AgentStepLog)
                .where(AgentStepLog.analysis_id == analysis_id)
                .order_by(AgentStepLog.created_at, AgentStepLog.id)
            )
        ).scalars().all()
    return [
        {
            "stage": row.stage,
            "agent": row.agent_name,
            "duration_ms": row.duration_ms,
            "model": row.model,
            "prompt_tokens": (row.token_usage or {}).get("prompt_tokens"),
            "completion_tokens": (row.token_usage or {}).get("completion_tokens"),
            "failed": bool(row.failed),
        }
        for row in rows
    ]


async def _latest_strategy_evidence(analysis_id: int) -> dict[str, Any]:
    try:
        async with AsyncSessionLocal() as db:
            row = (
                await db.execute(
                    select(AgentStepLog)
                    .where(AgentStepLog.analysis_id == analysis_id)
                    .where(AgentStepLog.stage == "2d_evidence_scorecard")
                    .order_by(desc(AgentStepLog.created_at), desc(AgentStepLog.id))
                    .limit(1)
                )
            ).scalar_one_or_none()
    except Exception as exc:
        return {"available": False, "reason": f"{type(exc).__name__}: {exc}"}

    if not row:
        return {"available": False, "reason": "2d_evidence_scorecard step not found"}
    output = row.output_data if isinstance(row.output_data, dict) else {}
    evidence = output.get("evidence_bundle") if isinstance(output.get("evidence_bundle"), dict) else {}
    strategies = evidence.get("strategies") if isinstance(evidence.get("strategies"), dict) else {}
    return _compact_strategy_evidence(strategies)


async def _latest_cron_runs() -> list[dict[str, Any]]:
    async with AsyncSessionLocal() as db:
        rows = (
            await db.execute(
                select(CronRunLog).order_by(desc(CronRunLog.started_at))
            )
        ).scalars().all()
    return [
        {
            "job_name": row.job_name,
            "status": row.status,
            "started_at": _iso(row.started_at),
            "finished_at": _iso(row.finished_at),
            "duration_ms": row.duration_ms,
            "summary": row.summary or {},
            "error_message": row.error_message or "",
        }
        for row in rows
    ]


async def _data_quality_audit_trend(limit: int = 20) -> dict[str, Any]:
    limit = max(min(int(limit or 20), 100), 1)
    try:
        async with AsyncSessionLocal() as db:
            exists = (
                await db.execute(text("select to_regclass('public.data_quality_audit')"))
            ).scalar_one_or_none()
            if not exists:
                return {
                    "available": False,
                    "reason": "data_quality_audit table not found",
                    "recent": [],
                    "trend": {},
                }
            rows = (
                await db.execute(
                    text("""
                        select id, created_at, audit_name, lookback_days, status, summary
                        from data_quality_audit
                        where audit_name = :audit_name
                        order by created_at desc
                        limit :limit
                    """),
                    {"audit_name": DATA_QUALITY_AUDIT_NAME, "limit": limit},
                )
            ).mappings().all()
    except Exception as exc:
        return {
            "available": False,
            "reason": f"{type(exc).__name__}: {exc}",
            "recent": [],
            "trend": {},
        }

    recent = [_compact_data_quality_audit_row(row) for row in rows]
    if not recent:
        return {
            "available": False,
            "reason": "no QC/yfinance audit rows",
            "recent": [],
            "trend": {},
        }
    return {
        "available": True,
        "latest": recent[0],
        "recent": recent,
        "trend": _data_quality_audit_trend_summary(recent),
    }


def _compact_data_quality_audit_row(row: Any) -> dict[str, Any]:
    summary = _coerce_json_dict(row.get("summary") if hasattr(row, "get") else None)
    packet_totals = summary.get("packet_totals") or {}
    unit_risks = summary.get("unit_risks") or []
    high_drift = summary.get("high_drift_classes") or []
    return {
        "id": row.get("id"),
        "created_at": _iso(row.get("created_at")),
        "status": row.get("status") or summary.get("status"),
        "lookback_days": row.get("lookback_days") or summary.get("lookback_days"),
        "joined_rows": sum(int(v or 0) for v in packet_totals.values()),
        "unit_risk_count": int(summary.get("unit_risk_count") or len(unit_risks)),
        "high_drift_classes": len(high_drift),
        "max_raw_momentum_error": summary.get("max_raw_momentum_error"),
        "max_normalized_momentum_error": summary.get("max_normalized_momentum_error"),
        "packet_totals": packet_totals,
        "unit_risk_fields": _audit_unit_risk_labels(unit_risks),
        "high_drift_labels": _audit_high_drift_labels(high_drift),
    }


def _data_quality_audit_trend_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "runs": len(rows),
        "latest_status": rows[0].get("status") if rows else None,
        "unit_risk_runs": sum(1 for row in rows if int(row.get("unit_risk_count") or 0) > 0),
        "high_drift_runs": sum(1 for row in rows if int(row.get("high_drift_classes") or 0) > 0),
        "max_joined_rows": max((int(row.get("joined_rows") or 0) for row in rows), default=0),
        "latest_unit_risk_count": rows[0].get("unit_risk_count") if rows else None,
    }


def _audit_unit_risk_labels(unit_risks: list[dict[str, Any]]) -> list[str]:
    labels = []
    for item in unit_risks[:8]:
        labels.append(
            "/".join(
                str(part)
                for part in (item.get("packet_type"), item.get("ticker_role"), item.get("field"))
                if part
            )
        )
    return labels


def _audit_high_drift_labels(high_drift: list[dict[str, Any]]) -> list[str]:
    labels = []
    for item in high_drift[:8]:
        labels.append(
            "/".join(
                str(part)
                for part in (item.get("packet_type"), item.get("ticker_role"))
                if part
            )
        )
    return labels


def _coerce_json_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


async def _latest_execution() -> dict[str, Any]:
    async with AsyncSessionLocal() as db:
        row = (
            await db.execute(
                select(ExecutionLog).order_by(desc(ExecutionLog.executed_at)).limit(1)
            )
        ).scalar_one_or_none()
    if not row:
        return {"available": False}
    return {
        "available": True,
        "analysis_id": row.analysis_id,
        "executed_at": _iso(row.executed_at),
        "command_id": row.command_id,
        "command_type": row.command_type,
        "status": row.status,
        "qc_status": row.qc_status,
        "qc_ack_at": _iso(row.qc_ack_at),
        "qc_rejection_reason": row.qc_rejection_reason,
        "retry_count": row.retry_count,
        "qc_response": row.qc_response or {},
    }


async def _execution_control_status(latest_analysis: dict[str, Any]) -> dict[str, Any]:
    """Return read-only execution trust diagnostics for the operator dashboard."""
    try:
        async with AsyncSessionLocal() as db:
            snapshot = (
                await db.execute(
                    select(AccountStateSnapshot)
                    .order_by(desc(AccountStateSnapshot.recorded_at), desc(AccountStateSnapshot.id))
                    .limit(1)
                )
            ).scalar_one_or_none()
            lifecycle_events = (
                await db.execute(
                    select(CommandLifecycleEvent)
                    .order_by(desc(CommandLifecycleEvent.event_time), desc(CommandLifecycleEvent.id))
                    .limit(50)
                )
            ).scalars().all()
            recent_commands = (
                await db.execute(
                    select(ExecutionLog)
                    .where(ExecutionLog.command_id.isnot(None))
                    .order_by(desc(func.coalesce(ExecutionLog.qc_ack_at, ExecutionLog.executed_at)))
                    .limit(20)
                )
            ).scalars().all()
            accepted_commands = (
                await db.execute(
                    select(ExecutionLog)
                    .where(ExecutionLog.command_type == "weight_adjustment")
                    .where(ExecutionLog.qc_status.in_((
                        "accepted",
                        "orders_submitted",
                        "partial",
                        "timeout_no_ack",
                    )))
                    .order_by(desc(func.coalesce(ExecutionLog.qc_ack_at, ExecutionLog.executed_at)))
                    .limit(50)
                )
            ).scalars().all()
            deferred_rows = (
                await db.execute(
                    select(DeferredExecutionLedger)
                    .order_by(desc(DeferredExecutionLedger.created_at), desc(DeferredExecutionLedger.id))
                    .limit(50)
                )
            ).scalars().all()
            lifecycle_cfg = (
                await db.execute(
                    select(SystemConfig).where(SystemConfig.key == "execution_lifecycle_config").limit(1)
                )
            ).scalar_one_or_none()
            active_command_id = _snapshot_active_command_id(snapshot)
            active_command = None
            active_command_events = []
            if active_command_id:
                active_command = (
                    await db.execute(
                        select(ExecutionLog)
                        .where(ExecutionLog.command_id == active_command_id)
                        .limit(1)
                    )
                ).scalar_one_or_none()
                active_command_events = (
                    await db.execute(
                        select(CommandLifecycleEvent)
                        .where(CommandLifecycleEvent.command_id == active_command_id)
                        .order_by(desc(CommandLifecycleEvent.event_time), desc(CommandLifecycleEvent.id))
                        .limit(20)
                    )
                ).scalars().all()
    except Exception as exc:
        return {
            "available": False,
            "reason": f"{type(exc).__name__}: {exc}",
            "account_state_guard": latest_analysis.get("account_state_guard") or {},
            "auto_pause": latest_analysis.get("auto_pause") or {},
            "latest_account_snapshot": {},
            "recent_command_events": [],
            "recent_commands": [],
            "active_execution": {},
            "deferred_execution": {},
            "reconciliation_lag": {},
        }

    deferred_rows_compact = [_compact_deferred_execution_row(row) for row in deferred_rows]
    open_deferred = [row for row in deferred_rows_compact if row.get("status") == "open"]
    lifecycle_rows = [_compact_lifecycle_event(row) for row in lifecycle_events]
    lifecycle_status_by_command = _lifecycle_status_by_command(lifecycle_rows)
    accepted_command_rows = [
        _compact_execution_row(row, lifecycle_status=lifecycle_status_by_command.get(row.command_id))
        for row in accepted_commands
    ]
    return {
        "available": True,
        "account_state_guard": latest_analysis.get("account_state_guard") or {},
        "auto_pause": latest_analysis.get("auto_pause") or {},
        "latest_account_snapshot": _compact_account_state_snapshot(snapshot),
        "active_execution": _compact_active_execution_panel(
            snapshot=snapshot,
            command_row=active_command,
            lifecycle_events=active_command_events,
            config=(lifecycle_cfg.value if lifecycle_cfg else {}) or {},
        ),
        "recent_command_events": lifecycle_rows,
        "recent_commands": [
            _compact_execution_row(row, lifecycle_status=lifecycle_status_by_command.get(row.command_id))
            for row in recent_commands
        ],
        "reconciliation_lag": build_reconciliation_lag_report(
            commands=accepted_command_rows,
            events=lifecycle_rows,
            max_age_minutes=30,
        ),
        "deferred_execution": {
            "available": True,
            "open_count": len(open_deferred),
            "open_buy_delta": round(sum(max(float(row.get("remaining_delta") or 0.0), 0.0) for row in open_deferred), 6),
            "open_sell_delta": round(sum(abs(min(float(row.get("remaining_delta") or 0.0), 0.0)) for row in open_deferred), 6),
            "open_tickers": sorted({str(row.get("ticker") or "") for row in open_deferred if row.get("ticker")}),
            "recent_rows": deferred_rows_compact,
        },
    }


async def _live_signal_conviction_dashboard() -> dict[str, Any]:
    """Load read-only FrozenSignal/Outcome/conviction dashboard summary."""
    try:
        from services.strategy_validation_dashboard import load_validation_dashboard_summary

        async with AsyncSessionLocal() as db:
            raw = await load_validation_dashboard_summary(
                db,
                profile_limit=20,
                row_limit=5000,
            )
        return _compact_live_signal_conviction_summary(raw)
    except Exception as exc:
        return {
            "available": False,
            "reason": f"{type(exc).__name__}: {exc}",
            "overview": {},
            "pending_outcomes": {},
            "pending_by_horizon_rows": [],
            "historical_prior_profiles": [],
            "live_paper_profiles": [],
            "combined_profiles": [],
            "profile_count_rows": [],
            "status_count_rows": [],
        }


async def _performance_attribution_dashboard(limit: int = 12) -> dict[str, Any]:
    """Load read-only beta/factor/residual attribution rows."""
    limit = max(min(int(limit or 12), 52), 1)
    monthly_report: dict[str, Any] = {
        "available": False,
        "reason": "monthly alpha report not loaded",
    }
    try:
        async with AsyncSessionLocal() as db:
            rows = (
                await db.execute(
                    select(PerformanceAttribution)
                    .order_by(desc(PerformanceAttribution.period_end), desc(PerformanceAttribution.id))
                    .limit(limit)
                )
            ).scalars().all()
            try:
                monthly_report = await load_monthly_alpha_attribution_report(db)
            except Exception as exc:
                monthly_report = {
                    "available": False,
                    "reason": f"{type(exc).__name__}: {exc}",
                    "execution_authority": "none",
                    "target_weight_mutation": "none",
                }
    except Exception as exc:
        return {
            "available": False,
            "reason": f"{type(exc).__name__}: {exc}",
            "latest": {},
            "return_breakdown_rows": [],
            "recent_rows": [],
            "status_rows": [],
            "monthly_alpha_report": monthly_report,
        }

    if not rows:
        return {
            "available": bool(monthly_report.get("available")),
            "reason": "no performance attribution rows",
            "latest": {},
            "return_breakdown_rows": [],
            "recent_rows": [],
            "status_rows": [],
            "monthly_alpha_report": monthly_report,
            "residual_contract": {
                "label": "residual_alpha_candidate",
                "meaning": "unexplained return candidate after factor adjustment",
                "not_proven_alpha": True,
                "execution_authority": "none",
            },
        }

    compact_rows = [_compact_performance_attribution_row(row) for row in rows]
    latest = compact_rows[0]
    return {
        "available": True,
        "latest": latest,
        "return_breakdown_rows": _performance_attribution_breakdown_rows(latest),
        "recent_rows": compact_rows,
        "status_rows": _count_rows(compact_rows, "status", label="status"),
        "monthly_alpha_report": monthly_report,
        "residual_contract": {
            "label": "residual_alpha_candidate",
            "meaning": "unexplained return candidate after SPY, QQQ, and momentum proxy",
            "not_proven_alpha": True,
            "execution_authority": "none",
        },
    }


async def _alpha_validation_trend_dashboard(limit: int = 30) -> dict[str, Any]:
    """Load persistent alpha validation snapshots."""
    limit = max(min(int(limit or 30), 100), 1)
    try:
        async with AsyncSessionLocal() as db:
            rows = (
                await db.execute(
                    select(AlphaValidationRun)
                    .order_by(desc(AlphaValidationRun.generated_at), desc(AlphaValidationRun.id))
                    .limit(limit)
                )
            ).scalars().all()
    except Exception as exc:
        return {
            "available": False,
            "reason": f"{type(exc).__name__}: {exc}",
            "latest": {},
            "recent_rows": [],
            "status_rows": [],
            "data_quality_rows": [],
            "trend_metrics": {},
        }

    if not rows:
        return {
            "available": False,
            "reason": "no alpha validation runs",
            "latest": {},
            "recent_rows": [],
            "status_rows": [],
            "data_quality_rows": [],
            "trend_metrics": {},
        }

    compact_rows = [_compact_alpha_validation_row(row) for row in rows]
    return {
        "available": True,
        "latest": compact_rows[0],
        "recent_rows": compact_rows,
        "status_rows": _count_rows(compact_rows, "status", label="status"),
        "data_quality_rows": _count_rows(compact_rows, "data_quality", label="data_quality"),
        "trend_metrics": _alpha_validation_trend_metrics(compact_rows),
        "contract": {
            "source_table": "alpha_validation_runs",
            "execution_authority": "none",
            "target_weight_mutation": "none",
            "sample_count": len(compact_rows),
        },
    }


async def _strategy_regime_gap_analysis_dashboard() -> dict[str, Any]:
    """Load read-only strategy family / regime gap diagnostics."""
    try:
        from services.strategy_regime_gap_analysis import load_strategy_regime_gap_analysis

        async with AsyncSessionLocal() as db:
            raw = await load_strategy_regime_gap_analysis(db, row_limit=5000)
        raw["available"] = True
        return raw
    except Exception as exc:
        return {
            "available": False,
            "reason": f"{type(exc).__name__}: {exc}",
            "contract_version": "strategy_regime_gap_analysis_v1",
            "execution_authority": "none",
            "target_weight_mutation": "none",
            "regime_rows": [],
            "family_rows": [],
            "weak_family_regime_rows": [],
            "research_queue": [],
            "warnings": [],
        }


async def _strategy_promotion_recommendations_dashboard() -> dict[str, Any]:
    """Load read-only promotion/degradation recommendations."""
    try:
        from services.strategy_promotion_recommendations import load_strategy_promotion_recommendations

        async with AsyncSessionLocal() as db:
            raw = await load_strategy_promotion_recommendations(db, row_limit=5000)
        raw["available"] = True
        return raw
    except Exception as exc:
        return {
            "available": False,
            "reason": f"{type(exc).__name__}: {exc}",
            "contract_version": "strategy_promotion_recommendations_v1",
            "execution_authority": "none",
            "target_weight_mutation": "none",
            "recommendation_only": True,
            "recommendations": [],
            "recommendation_counts": {},
            "warnings": [],
        }


async def _alpha_decision_profiles_dashboard() -> dict[str, Any]:
    """Load read-only alpha decision profiles."""
    try:
        from services.alpha_decision_profile import load_alpha_decision_profiles

        async with AsyncSessionLocal() as db:
            raw = await load_alpha_decision_profiles(db, row_limit=5000)
        raw["available"] = True
        return raw
    except Exception as exc:
        return {
            "available": False,
            "reason": f"{type(exc).__name__}: {exc}",
            "contract_version": "alpha_decision_profiles_v1",
            "execution_authority": "none",
            "target_weight_mutation": "none",
            "decision_input_only": True,
            "recommendation_only": True,
            "rows": [],
            "status_counts": {},
            "warnings": [],
        }


def _alpha_decision_policy_dashboard_status(
    config: dict[str, Any],
    alpha_decision_profiles: dict[str, Any],
    evidence_cap_calibration: dict[str, Any],
) -> dict[str, Any]:
    """Render current alpha-decision policy mode and gated blockers."""
    try:
        from services.alpha_decision_policy import evaluate_alpha_decision_policy

        return {
            "available": True,
            **evaluate_alpha_decision_policy(
                config or {},
                alpha_decision_summary=alpha_decision_profiles or {},
                evidence_cap_calibration=evidence_cap_calibration or {},
            ),
        }
    except Exception as exc:
        return {
            "available": False,
            "reason": f"{type(exc).__name__}: {exc}",
            "contract_version": "alpha_decision_policy_v1",
            "mode": "observe",
            "effective_mode": "observe",
            "gated_enabled": False,
            "recommendation_effect": False,
            "allocation_effect": False,
            "execution_authority": "none",
            "target_weight_mutation": "none",
            "blockers": ["policy_status_unavailable"],
            "warnings": [],
        }


def _alpha_readiness_report_dashboard(
    *,
    strategy_evidence: dict[str, Any],
    alpha_decision_profiles: dict[str, Any],
) -> dict[str, Any]:
    """Build the PR5 diagnostic alpha readiness handoff report."""
    try:
        from services.alpha_readiness_report import build_current_alpha_readiness_report

        return {
            "available": True,
            **build_current_alpha_readiness_report(
                strategy_evidence=strategy_evidence or {},
                alpha_decision_profiles=alpha_decision_profiles or {},
            ),
        }
    except Exception as exc:
        return {
            "available": False,
            "reason": f"{type(exc).__name__}: {exc}",
            "contract_version": "alpha_readiness_report_v1",
            "execution_authority": "none",
            "target_weight_mutation": "none",
            "diagnostic_only": True,
            "attribution_trade_authority": "none",
            "rows": [],
            "warnings": [],
        }


def _alpha_decision_review_surface_status(
    *,
    policy: dict[str, Any],
    profiles: dict[str, Any],
    recommendations: dict[str, Any],
    portfolio_construction: dict[str, Any],
    performance_attribution: dict[str, Any],
    strategy_evidence: dict[str, Any],
) -> dict[str, Any]:
    """Build one operator-facing alpha decision review surface."""
    profile_rows = [
        row for row in profiles.get("rows") or []
        if isinstance(row, dict)
    ]
    recommendation_rows = [
        row for row in recommendations.get("recommendations") or []
        if isinstance(row, dict)
    ]
    pc_alpha_rows = [
        row for row in portfolio_construction.get("alpha_decision_objective_rows") or []
        if isinstance(row, dict)
    ]
    cluster_rows = [
        row for row in portfolio_construction.get("strategy_cluster_exposure_rows") or []
        if isinstance(row, dict)
    ]
    independence = strategy_evidence.get("strategy_independence") or {}
    latest_attr = performance_attribution.get("latest") or {}
    checklist = {
        "mode_visible": bool(policy.get("effective_mode")),
        "no_naked_conviction_numbers": _profile_rows_have_sample_and_status(profile_rows),
        "promotion_rows_include_residual_alpha_and_cost_status": _recommendation_rows_have_alpha_cost_columns(recommendation_rows),
        "strategy_counts_show_effective_independent_count": (
            profiles.get("strategy_count") is not None
            and profiles.get("independence_adjusted_strategy_count") is not None
        ),
        "pc_raw_vs_adjusted_diagnostics_available": bool(pc_alpha_rows),
        "net_alpha_view_available": bool(profile_rows) or bool(latest_attr),
        "policy_prevents_target_builder_bypass": bool(policy.get("never_bypasses_target_builder")),
    }
    return {
        "available": True,
        "contract_version": "alpha_decision_review_surface_v1",
        "execution_authority": "none",
        "target_weight_mutation": "none",
        "mode": policy.get("mode"),
        "effective_mode": policy.get("effective_mode"),
        "gated_enabled": policy.get("gated_enabled"),
        "recommendation_effect": policy.get("recommendation_effect"),
        "allocation_effect": policy.get("allocation_effect"),
        "review_checklist": checklist,
        "statistical_maturity": {
            "profile_count": profiles.get("profile_count"),
            "strategy_count": profiles.get("strategy_count"),
            "raw_alpha_strategy_count": profiles.get("raw_alpha_strategy_count"),
            "independence_adjusted_strategy_count": profiles.get("independence_adjusted_strategy_count"),
            "status_counts": profiles.get("status_counts") or {},
            "residual_alpha_status_counts": profiles.get("residual_alpha_status_counts") or {},
            "net_edge_status_counts": profiles.get("net_edge_status_counts") or {},
        },
        "strategy_independence": {
            "status": independence.get("status"),
            "strategy_count": independence.get("strategy_count"),
            "alpha_strategy_count": independence.get("alpha_strategy_count"),
            "effective_independent_alpha_count": independence.get("effective_independent_alpha_count"),
            "high_correlation_pair_count": independence.get("high_correlation_pair_count"),
            "low_correlation_pair_count": independence.get("low_correlation_pair_count"),
            "operator_review_required": independence.get("operator_review_required"),
        },
        "promotion_review": {
            "status": recommendations.get("status"),
            "recommendation_count": recommendations.get("recommendation_count"),
            "high_priority_count": recommendations.get("high_priority_count"),
            "recommendation_counts": recommendations.get("recommendation_counts") or {},
            "alpha_decision_policy_effective_mode": (
                recommendations.get("alpha_decision_policy") or {}
            ).get("effective_mode"),
        },
        "pc_review": {
            "mode": portfolio_construction.get("mode"),
            "policy_effective_mode": portfolio_construction.get("alpha_decision_policy_effective_mode"),
            "allocation_effect": portfolio_construction.get("alpha_decision_policy_allocation_effect"),
            "independence_adjusted_net_signal_effective_n_before": portfolio_construction.get(
                "independence_adjusted_net_signal_effective_n_before"
            ),
            "independence_adjusted_net_signal_effective_n_after": portfolio_construction.get(
                "independence_adjusted_net_signal_effective_n_after"
            ),
            "independence_adjusted_net_signal_effective_n_delta": portfolio_construction.get(
                "independence_adjusted_net_signal_effective_n_delta"
            ),
            "target_builder_consumed": (portfolio_construction.get("safety_contract") or {}).get(
                "target_builder_consumed"
            ),
        },
        "latest_residual_alpha": {
            "period_key": latest_attr.get("period_key"),
            "residual_alpha_candidate": latest_attr.get("residual_alpha_candidate"),
            "r_squared": latest_attr.get("r_squared"),
            "sample_count": latest_attr.get("sample_count"),
            "data_quality": latest_attr.get("data_quality"),
        },
        "net_alpha_rows": _net_alpha_review_rows(profile_rows),
        "promotion_review_rows": recommendation_rows,
        "strategy_cluster_review_rows": cluster_rows,
        "pc_before_after_rows": pc_alpha_rows,
        "warnings": _alpha_decision_review_warnings(checklist, policy),
    }


def _profile_rows_have_sample_and_status(rows: list[dict[str, Any]]) -> bool:
    return all(
        row.get("sample_count") is not None and row.get("statistical_status") is not None
        for row in rows
    )


def _recommendation_rows_have_alpha_cost_columns(rows: list[dict[str, Any]]) -> bool:
    return all(
        "residual_alpha_status" in row
        and "residual_alpha" in row
        and "net_edge_status" in row
        and "cost_adjusted_edge" in row
        for row in rows
    )


def _net_alpha_review_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows:
        out.append({
            "strategy_id": row.get("strategy_id"),
            "strategy_family": row.get("strategy_family"),
            "regime": row.get("regime"),
            "construction_epoch_id": row.get("construction_epoch_id"),
            "sample_count": row.get("sample_count"),
            "statistical_status": row.get("statistical_status"),
            "residual_alpha_status": row.get("residual_alpha_status"),
            "residual_alpha": row.get("residual_alpha"),
            "gross_expected_edge": row.get("gross_expected_edge"),
            "estimated_ibkr_cost_pct": row.get("estimated_ibkr_cost_pct"),
            "cost_adjusted_edge": row.get("cost_adjusted_edge"),
            "edge_to_cost_ratio": row.get("edge_to_cost_ratio"),
            "net_edge_status": row.get("net_edge_status"),
            "redundancy_multiplier": row.get("redundancy_multiplier"),
            "decision_multiplier": row.get("decision_multiplier"),
            "decision_status": row.get("decision_status"),
        })
    return out


def _alpha_decision_review_warnings(checklist: dict[str, Any], policy: dict[str, Any]) -> list[str]:
    warnings = [
        f"review_check_failed:{key}"
        for key, value in checklist.items()
        if not value
    ]
    if policy.get("mode") == "gated" and not policy.get("allocation_effect"):
        warnings.append("gated_requested_but_policy_blocked")
    return sorted(set(warnings))


async def _evidence_cap_calibration_dashboard(config: dict[str, Any] | None = None) -> dict[str, Any]:
    """Load read-only evidence cap calibration diagnostics."""
    try:
        async with AsyncSessionLocal() as db:
            return await load_evidence_cap_calibration_report(
                db,
                current_config=config or {},
            )
    except Exception as exc:
        return {
            "contract_version": "evidence_cap_calibration_v1",
            "status": "unavailable",
            "reason": f"{type(exc).__name__}: {exc}",
            "recommendation_only": True,
            "execution_authority": "none",
            "target_weight_mutation": "none",
            "observe_summary": {},
            "young_etf_summary": {},
            "conviction_summary": {},
            "execution_feedback": {},
            "gated_readiness": {},
            "recommended_config": {},
            "recommended_vote_thresholds": {},
            "warnings": [],
        }


async def _dashboard_config() -> dict[str, Any]:
    async with AsyncSessionLocal() as db:
        playground = (
            await db.execute(
                select(SystemConfig).where(SystemConfig.key == "playground_config").limit(1)
            )
        ).scalar_one_or_none()
        circuit = (
            await db.execute(
                select(SystemConfig).where(SystemConfig.key == "circuit_state").limit(1)
            )
        ).scalar_one_or_none()
        pc_promotion = (
            await db.execute(
                select(SystemConfig).where(SystemConfig.key == "portfolio_construction_promotion_config").limit(1)
            )
        ).scalar_one_or_none()
        evidence_cap = (
            await db.execute(
                select(SystemConfig).where(SystemConfig.key == "evidence_cap_config").limit(1)
            )
        ).scalar_one_or_none()
        alpha_decision_policy = (
            await db.execute(
                select(SystemConfig).where(SystemConfig.key == "alpha_decision_policy_config").limit(1)
            )
        ).scalar_one_or_none()
        execution_lifecycle = (
            await db.execute(
                select(SystemConfig).where(SystemConfig.key == "execution_lifecycle_config").limit(1)
            )
        ).scalar_one_or_none()
        authorization_mode = (
            await db.execute(
                select(SystemConfig).where(SystemConfig.key == "authorization_mode").limit(1)
            )
        ).scalar_one_or_none()
        execution_command = (
            await db.execute(
                select(SystemConfig).where(SystemConfig.key == "execution_command_config").limit(1)
            )
        ).scalar_one_or_none()
    return {
        "authorization_mode": (authorization_mode.value if authorization_mode else {}) or {},
        "execution_command_config": (execution_command.value if execution_command else {}) or {},
        "playground_config": (playground.value if playground else {}) or {},
        "circuit_state": (circuit.value if circuit else {}) or {},
        "portfolio_construction_promotion_config": (pc_promotion.value if pc_promotion else {}) or {},
        "evidence_cap_config": (evidence_cap.value if evidence_cap else {}) or {},
        "alpha_decision_policy_config": (alpha_decision_policy.value if alpha_decision_policy else {}) or {},
        "execution_lifecycle_config": (execution_lifecycle.value if execution_lifecycle else {}) or {},
    }


async def _replay_diagnostics() -> dict[str, Any]:
    async with AsyncSessionLocal() as db:
        cfg = (
            await db.execute(
                select(SystemConfig).where(SystemConfig.key == "playground_config").limit(1)
            )
        ).scalar_one_or_none()
        lookback_days = int(((cfg.value if cfg else {}) or {}).get("lookback_days", 30))
        row_limit = _recent_snapshot_row_limit(lookback_days)

        raw_by_type = (
            await db.execute(
                select(
                    QCSnapshot.packet_type,
                    func.count(QCSnapshot.id),
                    func.count(func.distinct(QCSnapshot.trading_date)),
                    func.min(QCSnapshot.received_at),
                    func.max(QCSnapshot.received_at),
                )
                .where(QCSnapshot.received_at >= func.now() - text("interval '30 days'"))
                .where(QCSnapshot.packet_type.in_(("heartbeat", "daily_feature_snapshot")))
                .group_by(QCSnapshot.packet_type)
            )
        ).all()

        dedup_limited = await db.execute(text("""
            with recent_limited as (
                select id, received_at, trading_date, packet_type, raw_payload
                from qc_snapshots
                where received_at >= now() - (:days || ' days')::interval
                  and packet_type in ('daily_feature_snapshot','heartbeat')
                order by received_at desc
                limit :row_limit
            ),
            deduped as (
                select distinct on (coalesce(trading_date::text, raw_payload->>'trading_date', received_at::date::text))
                    coalesce(trading_date::text, raw_payload->>'trading_date', received_at::date::text) as replay_day,
                    packet_type,
                    received_at
                from recent_limited
                order by coalesce(trading_date::text, raw_payload->>'trading_date', received_at::date::text),
                    case packet_type when 'daily_feature_snapshot' then 2 when 'heartbeat' then 1 else 0 end desc,
                    received_at desc
            )
            select count(*) as replay_days, min(replay_day) as first_day, max(replay_day) as last_day
            from deduped
        """), {"days": str(lookback_days), "row_limit": row_limit})
        limited = dedup_limited.mappings().one()

        return_fields = await db.execute(text("""
            with all_recent as (
                select id, received_at, trading_date, packet_type, raw_payload
                from qc_snapshots
                where received_at >= now() - (:days || ' days')::interval
                  and packet_type in ('daily_feature_snapshot','heartbeat')
            ),
            deduped as (
                select distinct on (coalesce(trading_date::text, raw_payload->>'trading_date', received_at::date::text))
                    coalesce(trading_date::text, raw_payload->>'trading_date', received_at::date::text) as replay_day,
                    packet_type,
                    raw_payload
                from all_recent
                order by coalesce(trading_date::text, raw_payload->>'trading_date', received_at::date::text),
                    case packet_type when 'daily_feature_snapshot' then 2 when 'heartbeat' then 1 else 0 end desc,
                    received_at desc
            )
            select
                count(*) as all_replay_days,
                count(*) filter (
                    where exists (
                        select 1
                        from jsonb_array_elements(coalesce(nullif(raw_payload->'features','[]'::jsonb), raw_payload->'holdings', '[]'::jsonb)) h
                        where h ? 'daily_return_pct' or h ? 'return_1d'
                    )
                ) as replay_days_with_returns
            from deduped
        """), {"days": str(lookback_days)})
        fields = return_fields.mappings().one()

    return {
        "lookback_days": lookback_days,
        "row_limit_before_dedupe": row_limit,
        "raw_by_type": [
            {
                "packet_type": row[0],
                "rows": int(row[1] or 0),
                "trading_days": int(row[2] or 0),
                "first_received": _iso(row[3]),
                "last_received": _iso(row[4]),
            }
            for row in raw_by_type
        ],
        "deduped_with_limit": {
            "replay_days": int(limited["replay_days"] or 0),
            "first_day": str(limited["first_day"]) if limited["first_day"] else None,
            "last_day": str(limited["last_day"]) if limited["last_day"] else None,
        },
        "deduped_without_limit": {
            "replay_days": int(fields["all_replay_days"] or 0),
            "replay_days_with_returns": int(fields["replay_days_with_returns"] or 0),
        },
    }


def _compact_scorecard(scorecard: dict[str, Any]) -> dict[str, Any]:
    return {
        "market_condition": scorecard.get("market_condition"),
        "investment_permission": scorecard.get("investment_permission"),
        "data_quality": scorecard.get("data_quality"),
        "dominant_constraint": scorecard.get("dominant_constraint"),
        "require_human_confirmation": scorecard.get("require_human_confirmation"),
        "warnings": scorecard.get("warnings") or [],
        "reasons": scorecard.get("reasons") or [],
    }


def _compact_governance(governance: dict[str, Any], ledger: dict[str, Any] | None = None) -> dict[str, Any]:
    portfolio = governance.get("portfolio_summary") or {}
    explanations = _enrich_position_explanations_from_ledger(
        portfolio.get("position_explanations") or [],
        ledger or {},
    )
    return {
        "mode": governance.get("mode"),
        "trade_summary": governance.get("trade_summary") or {},
        "blocked_actions": governance.get("blocked_actions") or [],
        "forced_trims": governance.get("forced_trims") or [],
        "manual_action_hints": governance.get("manual_action_hints") or portfolio.get("manual_action_hints") or [],
        "basket_reviews": portfolio.get("basket_reviews") or [],
        "thesis_status_summary": portfolio.get("thesis_status_summary") or {},
        "position_explanations": _sort_by_current_weight(explanations),
    }


def _compact_portfolio_construction_evaluation(evaluation: dict[str, Any]) -> dict[str, Any]:
    if not evaluation:
        return {}
    metrics = evaluation.get("metrics") or {}
    criteria = evaluation.get("criteria") or {}
    return {
        "status": evaluation.get("status"),
        "promotion_ready": evaluation.get("promotion_ready"),
        "execution_authority": evaluation.get("execution_authority"),
        "blockers": evaluation.get("blockers") or [],
        "warnings": evaluation.get("warnings") or [],
        "mean_abs_weight_deviation": metrics.get("mean_abs_weight_deviation"),
        "max_abs_weight_deviation": metrics.get("max_abs_weight_deviation"),
        "shadow_turnover": metrics.get("shadow_turnover"),
        "actual_turnover": metrics.get("actual_turnover"),
        "turnover_delta": metrics.get("turnover_delta"),
        "max_material_diff": criteria.get("max_material_diff"),
        "max_turnover_delta": criteria.get("max_turnover_delta"),
        "shadow_policy_allowed": metrics.get("shadow_policy_allowed"),
        "actual_policy_allowed": metrics.get("actual_policy_allowed"),
        "candidate_policy_allowed": metrics.get("candidate_policy_allowed"),
        "basket_policy_ok": metrics.get("basket_policy_ok"),
        "turnover_ok": metrics.get("turnover_ok"),
        "subscale_count": metrics.get("subscale_count"),
        "candidate_active_count": metrics.get("candidate_active_count"),
        "candidate_within_target_active_count": metrics.get("candidate_within_target_active_count"),
        "shadow_high_risk_tickers_added": metrics.get("shadow_high_risk_tickers_added") or [],
    }


def _compact_hedge_intent_outcome(outcome: dict[str, Any]) -> dict[str, Any]:
    if not outcome:
        return {}
    return {
        "report_version": outcome.get("report_version"),
        "date": outcome.get("date"),
        "triggered": outcome.get("triggered"),
        "severity": outcome.get("severity"),
        "add_hedge_etf": outcome.get("add_hedge_etf"),
        "selected_instrument": outcome.get("selected_instrument"),
        "candidate_hedge_instrument": outcome.get("candidate_hedge_instrument"),
        "why_not_add_hedge": outcome.get("why_not_add_hedge"),
        "outcome_status": outcome.get("outcome_status"),
        "spy_return_5d": outcome.get("spy_return_5d"),
        "hedge_instrument_return_5d": outcome.get("hedge_instrument_return_5d"),
        "hedge_would_have_helped": outcome.get("hedge_would_have_helped"),
        "threshold_assessment": outcome.get("threshold_assessment"),
        "execution_authority": outcome.get("execution_authority"),
    }


def _compact_portfolio_construction_payload(payload: dict[str, Any]) -> dict[str, Any]:
    if not payload:
        return {}
    diagnostics = payload.get("diagnostics") if isinstance(payload.get("diagnostics"), dict) else {}
    objective = payload.get("objective") if isinstance(payload.get("objective"), dict) else {}
    if not objective:
        objective = diagnostics.get("objective") if isinstance(diagnostics.get("objective"), dict) else {}
    turnover = payload.get("turnover") if isinstance(payload.get("turnover"), dict) else {}
    target_weights = payload.get("target_weights") if isinstance(payload.get("target_weights"), dict) else {}
    signal_metrics = (
        payload.get("signal_objective_metrics")
        if isinstance(payload.get("signal_objective_metrics"), dict)
        else {}
    )
    alpha_metrics = (
        payload.get("alpha_decision_objective_metrics")
        if isinstance(payload.get("alpha_decision_objective_metrics"), dict)
        else {}
    )
    basket_evaluation = (
        payload.get("basket_evaluation")
        if isinstance(payload.get("basket_evaluation"), dict)
        else {}
    )
    return {
        "available": True,
        "portfolio_construction_mode": payload.get("portfolio_construction_mode") or diagnostics.get("runtime_mode"),
        "construction_source": payload.get("construction_source") or diagnostics.get("construction_source"),
        "execution_effect": diagnostics.get("execution_effect"),
        "target_builder_consumed": diagnostics.get("target_builder_consumed"),
        "deterministic": diagnostics.get("deterministic"),
        "consumes_raw_llm_adjusted_weights": diagnostics.get("consumes_raw_llm_adjusted_weights"),
        "objective": objective,
        "primary_objective": objective.get("primary"),
        "subject_to": objective.get("subject_to") or [],
        "rationale": objective.get("rationale"),
        "effective_n_target": objective.get("effective_n_target"),
        "effective_n_before": payload.get("effective_n_before"),
        "effective_n_after": payload.get("effective_n_after") or payload.get("effective_n"),
        "effective_n_delta": _number_delta(payload.get("effective_n_after") or payload.get("effective_n"), payload.get("effective_n_before")),
        "signal_weighted_effective_n_before": payload.get("signal_weighted_effective_n_before"),
        "signal_weighted_effective_n_after": payload.get("signal_weighted_effective_n_after"),
        "signal_weighted_effective_n_delta": _number_delta(payload.get("signal_weighted_effective_n_after"), payload.get("signal_weighted_effective_n_before")),
        "signal_alignment_score_before": payload.get("signal_alignment_score_before"),
        "signal_alignment_score_after": payload.get("signal_alignment_score_after"),
        "signal_alignment_score_delta": _number_delta(payload.get("signal_alignment_score_after"), payload.get("signal_alignment_score_before")),
        "independence_adjusted_net_signal_effective_n_before": payload.get("independence_adjusted_net_signal_effective_n_before"),
        "independence_adjusted_net_signal_effective_n_after": payload.get("independence_adjusted_net_signal_effective_n_after"),
        "independence_adjusted_net_signal_effective_n_delta": _number_delta(payload.get("independence_adjusted_net_signal_effective_n_after"), payload.get("independence_adjusted_net_signal_effective_n_before")),
        "independence_adjusted_signal_alignment_score_before": payload.get("independence_adjusted_signal_alignment_score_before"),
        "independence_adjusted_signal_alignment_score_after": payload.get("independence_adjusted_signal_alignment_score_after"),
        "independence_adjusted_signal_alignment_score_delta": _number_delta(payload.get("independence_adjusted_signal_alignment_score_after"), payload.get("independence_adjusted_signal_alignment_score_before")),
        "signal_objective_metrics": signal_metrics,
        "alpha_decision_objective_metrics": alpha_metrics,
        "alpha_decision_policy": alpha_metrics.get("alpha_decision_policy") or {},
        "alpha_decision_policy_effective_mode": alpha_metrics.get("policy_effective_mode") or diagnostics.get("alpha_decision_policy_effective_mode"),
        "alpha_decision_policy_allocation_effect": alpha_metrics.get("policy_allocation_effect") or diagnostics.get("alpha_decision_policy_allocation_effect"),
        "signal_objective_warnings": signal_metrics.get("warnings") or diagnostics.get("signal_objective_warnings") or [],
        "alpha_decision_objective_warnings": alpha_metrics.get("warnings") or diagnostics.get("alpha_decision_objective_warnings") or [],
        "turnover": turnover,
        "turnover_budget": turnover.get("budget") if turnover else objective.get("turnover_budget"),
        "turnover_estimated": turnover.get("estimated"),
        "turnover_before_budget": turnover.get("estimated_before_budget"),
        "turnover_within_budget": turnover.get("within_budget"),
        "basket_limit_multiplier": diagnostics.get("basket_limit_multiplier"),
        "active_basket_reviews": diagnostics.get("active_basket_reviews") or [],
        "active_basket_policy": {
            "contract_version": basket_evaluation.get("contract_version"),
            "execution_effect": basket_evaluation.get("execution_effect"),
            "active_count": basket_evaluation.get("active_count"),
            "target_active_count_min": basket_evaluation.get("target_active_count_min"),
            "target_active_count_max": basket_evaluation.get("target_active_count_max"),
            "within_target_active_count": basket_evaluation.get("within_target_active_count"),
            "subscale_count": basket_evaluation.get("subscale_count"),
            "floor_cleared_count": basket_evaluation.get("floor_cleared_count"),
            "estimated_independent_clusters": basket_evaluation.get("estimated_independent_clusters"),
            "warnings": basket_evaluation.get("warnings") or [],
        },
        "active_basket_calibration": basket_evaluation.get("active_basket_calibration") or {},
        "ticker_count": diagnostics.get("ticker_count"),
        "construction_steps": payload.get("construction_steps") or [],
        "violations": payload.get("violations") or [],
        "factor_exposure_rows": _exposure_rows(
            payload.get("factor_exposure_before") or {},
            payload.get("factor_exposure_after") or payload.get("factor_exposures") or {},
            label="factor",
        ),
        "basket_exposure_rows": _basket_exposure_rows(
            payload.get("basket_exposure_before") or {},
            payload.get("basket_exposure_after") or {},
        ),
        "target_weight_rows": _weight_rows(target_weights),
        "signal_objective_rows": payload.get("signal_objective_rows") or [],
        "alpha_decision_objective_rows": payload.get("alpha_decision_objective_rows") or [],
        "strategy_cluster_exposure_rows": payload.get("strategy_cluster_exposure_rows") or [],
    }


def _compact_portfolio_construction_promotion_gate(gate: dict[str, Any]) -> dict[str, Any]:
    if not gate:
        return {}
    return {
        "status": gate.get("status"),
        "eligible": gate.get("eligible"),
        "portfolio_construction_mode": gate.get("portfolio_construction_mode"),
        "enabled": gate.get("enabled"),
        "approval_mode": gate.get("approval_mode"),
        "blockers": gate.get("blockers") or [],
        "would_promote_to": gate.get("would_promote_to"),
        "rollout_phase": gate.get("rollout_phase"),
        "semi_auto_confirmed_cycles": gate.get("semi_auto_confirmed_cycles"),
        "min_gated_semi_auto_confirmed_cycles": gate.get("min_gated_semi_auto_confirmed_cycles"),
        "execution_authority": gate.get("execution_authority"),
    }


def _portfolio_construction_objective_status(
    latest_analysis: dict[str, Any],
    readiness: dict[str, Any],
    config: dict[str, Any],
) -> dict[str, Any]:
    payload = latest_analysis.get("portfolio_construction_payload") or {}
    evaluation = latest_analysis.get("portfolio_construction_evaluation") or {}
    gate = latest_analysis.get("portfolio_construction_promotion_gate") or {}
    ledger_rows = (latest_analysis.get("decision_ledger") or {}).get("top_decisions") or []
    if not payload:
        return {
            "available": False,
            "reason": "no portfolio_construction payload in latest analysis",
            "config": _compact_pc_config(config),
            "readiness": _compact_pc_readiness(readiness),
            "promotion_gate": gate,
            "evaluation": evaluation,
            "weight_change_reasons": [],
        }
    objective = payload.get("objective") or {}
    return {
        "available": True,
        "mode": payload.get("portfolio_construction_mode") or gate.get("portfolio_construction_mode"),
        "config": _compact_pc_config(config),
        "objective": {
            "primary": objective.get("primary"),
            "subject_to": objective.get("subject_to") or [],
            "turnover_budget": objective.get("turnover_budget"),
            "effective_n_target": objective.get("effective_n_target"),
            "allow_cash_raise": objective.get("allow_cash_raise"),
            "rationale": objective.get("rationale"),
        },
        "objective_metrics": {
            "effective_n_before": payload.get("effective_n_before"),
            "effective_n_after": payload.get("effective_n_after"),
            "effective_n_delta": payload.get("effective_n_delta"),
            "signal_weighted_effective_n_before": payload.get("signal_weighted_effective_n_before"),
            "signal_weighted_effective_n_after": payload.get("signal_weighted_effective_n_after"),
            "signal_weighted_effective_n_delta": payload.get("signal_weighted_effective_n_delta"),
            "independence_adjusted_net_signal_effective_n_before": payload.get("independence_adjusted_net_signal_effective_n_before"),
            "independence_adjusted_net_signal_effective_n_after": payload.get("independence_adjusted_net_signal_effective_n_after"),
            "independence_adjusted_net_signal_effective_n_delta": payload.get("independence_adjusted_net_signal_effective_n_delta"),
            "signal_alignment_score_before": payload.get("signal_alignment_score_before"),
            "signal_alignment_score_after": payload.get("signal_alignment_score_after"),
            "signal_alignment_score_delta": payload.get("signal_alignment_score_delta"),
            "independence_adjusted_signal_alignment_score_before": payload.get("independence_adjusted_signal_alignment_score_before"),
            "independence_adjusted_signal_alignment_score_after": payload.get("independence_adjusted_signal_alignment_score_after"),
            "independence_adjusted_signal_alignment_score_delta": payload.get("independence_adjusted_signal_alignment_score_delta"),
            "signal_objective_warnings": payload.get("signal_objective_warnings") or [],
            "alpha_decision_objective_warnings": payload.get("alpha_decision_objective_warnings") or [],
            "turnover_budget": payload.get("turnover_budget"),
            "turnover_before_budget": payload.get("turnover_before_budget"),
            "turnover_estimated": payload.get("turnover_estimated"),
            "turnover_within_budget": payload.get("turnover_within_budget"),
            "ticker_count": payload.get("ticker_count"),
            "active_basket_reviews": payload.get("active_basket_reviews") or [],
        },
        "safety_contract": {
            "construction_source": payload.get("construction_source"),
            "execution_effect": payload.get("execution_effect"),
            "target_builder_consumed": payload.get("target_builder_consumed"),
            "deterministic": payload.get("deterministic"),
            "consumes_raw_llm_adjusted_weights": payload.get("consumes_raw_llm_adjusted_weights"),
        },
        "active_basket_policy": payload.get("active_basket_policy") or {},
        "active_basket_calibration": payload.get("active_basket_calibration") or {},
        "readiness": _compact_pc_readiness(readiness),
        "promotion_gate": gate,
        "evaluation": evaluation,
        "construction_steps": payload.get("construction_steps") or [],
        "violations": payload.get("violations") or [],
        "factor_exposure_rows": payload.get("factor_exposure_rows") or [],
        "basket_exposure_rows": payload.get("basket_exposure_rows") or [],
        "target_weight_rows": payload.get("target_weight_rows") or [],
        "signal_objective_rows": payload.get("signal_objective_rows") or [],
        "alpha_decision_objective_rows": payload.get("alpha_decision_objective_rows") or [],
        "strategy_cluster_exposure_rows": payload.get("strategy_cluster_exposure_rows") or [],
        "weight_change_reasons": _pc_weight_change_rows(ledger_rows),
    }


def _compact_pc_config(config: dict[str, Any]) -> dict[str, Any]:
    return {
        "portfolio_construction_mode": config.get("portfolio_construction_mode"),
        "enabled": config.get("enabled"),
        "require_manual_approval": config.get("require_manual_approval"),
        "min_shadow_cycles": config.get("min_shadow_cycles"),
        "min_cycles": config.get("min_cycles"),
        "min_pass_rate": config.get("min_pass_rate"),
        "max_material_diff": config.get("max_material_diff"),
        "max_turnover_diff": config.get("max_turnover_diff"),
        "allow_full_auto_gated": config.get("allow_full_auto_gated"),
        "require_semi_auto_gated_before_full_auto": config.get("require_semi_auto_gated_before_full_auto"),
        "min_gated_semi_auto_confirmed_cycles": config.get("min_gated_semi_auto_confirmed_cycles"),
    }


def _compact_pc_readiness(readiness: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": readiness.get("status"),
        "promotion_ready": readiness.get("promotion_ready"),
        "ready": readiness.get("ready"),
        "cycles": readiness.get("cycles"),
        "ready_count": readiness.get("ready_count"),
        "pass_rate": readiness.get("pass_rate"),
        "min_cycles": readiness.get("min_cycles"),
        "min_pass_rate": readiness.get("min_pass_rate"),
        "basket_policy_ok_rate": readiness.get("basket_policy_ok_rate"),
        "min_basket_policy_ok_rate": readiness.get("min_basket_policy_ok_rate"),
        "policy_ok_rate": readiness.get("policy_ok_rate"),
        "min_policy_ok_rate": readiness.get("min_policy_ok_rate"),
        "turnover_ok_rate": readiness.get("turnover_ok_rate"),
        "min_turnover_ok_rate": readiness.get("min_turnover_ok_rate"),
        "subscale_position_rate": readiness.get("subscale_position_rate"),
        "max_subscale_position_rate": readiness.get("max_subscale_position_rate"),
        "unclassified_mutation_count": readiness.get("unclassified_mutation_count"),
        "blockers": readiness.get("blockers") or [],
        "blocker_counts": readiness.get("blocker_counts") or {},
        "warning_counts": readiness.get("warning_counts") or {},
        "mean_abs_weight_deviation_avg": readiness.get("mean_abs_weight_deviation_avg"),
        "max_mean_weight_deviation": readiness.get("max_mean_weight_deviation"),
        "turnover_delta_avg": readiness.get("turnover_delta_avg"),
    }


def _pc_weight_change_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        out.append({
            "ticker": row.get("ticker"),
            "portfolio_construction_target": row.get("portfolio_construction_target"),
            "target_builder_target": row.get("target_builder_target"),
            "final_target": row.get("final_target"),
            "changed_by": row.get("changed_by") or [],
            "construction_effect": row.get("construction_effect"),
            "risk_governance_effect": row.get("risk_governance_effect"),
            "final_explanation": row.get("final_explanation"),
        })
    return out


def _exposure_rows(before: dict[str, Any], after: dict[str, Any], *, label: str) -> list[dict[str, Any]]:
    rows = []
    for key in sorted(set(before) | set(after)):
        rows.append({
            label: key,
            "before": _json_safe_number(before.get(key)),
            "after": _json_safe_number(after.get(key)),
            "delta": _number_delta(after.get(key), before.get(key)),
        })
    return rows


def _basket_exposure_rows(before: dict[str, Any], after: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for group in sorted(set(before) | set(after)):
        before_row = before.get(group) if isinstance(before.get(group), dict) else {}
        after_row = after.get(group) if isinstance(after.get(group), dict) else {}
        rows.append({
            "basket": group,
            "before": _json_safe_number(before_row.get("exposure")),
            "after": _json_safe_number(after_row.get("exposure")),
            "delta": _number_delta(after_row.get("exposure"), before_row.get("exposure")),
            "limit": _json_safe_number(after_row.get("limit") or before_row.get("limit")),
            "reduced_limit": _json_safe_number(after_row.get("reduced_limit") or before_row.get("reduced_limit")),
            "violated": after_row.get("violated"),
        })
    return rows


def _weight_rows(weights: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for ticker, value in sorted(weights.items()):
        rows.append({"ticker": ticker, "target_weight": _json_safe_number(value)})
    return rows


def _number_delta(after: Any, before: Any) -> float | None:
    after_num = _json_safe_number(after)
    before_num = _json_safe_number(before)
    if after_num is None or before_num is None:
        return None
    return round(after_num - before_num, 6)


def _compact_strategy_evidence(strategies: dict[str, Any]) -> dict[str, Any]:
    if not strategies:
        return {"available": False, "reason": "strategy evidence unavailable"}
    strategy_results = [
        row for row in (strategies.get("strategy_results") or [])
        if isinstance(row, dict)
    ]
    strategy_diversity = (
        strategies.get("strategy_diversity")
        if isinstance(strategies.get("strategy_diversity"), dict)
        else {}
    )
    strategy_independence = _compact_strategy_independence(
        strategies.get("strategy_independence") if isinstance(strategies.get("strategy_independence"), dict) else {}
    )
    strategy_breadth = strategy_independence.get("strategy_breadth_calibration") or {}
    card_rows: list[dict[str, Any]] = []
    strategy_rows: list[dict[str, Any]] = []
    for row in strategy_results:
        strategy_name = str(row.get("strategy_name") or "").strip()
        cards = [
            _compact_evidence_card(card, fallback_strategy=strategy_name)
            for card in (row.get("evidence_cards") or [])
            if isinstance(card, dict)
        ]
        card_rows.extend(cards)
        summary = row.get("evidence_summary") if isinstance(row.get("evidence_summary"), dict) else {}
        strategy_rows.append({
            "strategy": strategy_name,
            "raw_family": row.get("raw_family"),
            "canonical_family": row.get("canonical_family"),
            "alpha_source": row.get("alpha_source"),
            "data_ready": row.get("data_ready"),
            "can_influence_allocation": row.get("can_influence_allocation"),
            "suggested_use": row.get("suggested_use"),
            "confidence_score": row.get("confidence_score"),
            "selected_tickers": row.get("selected_tickers") or [],
            "evidence_contract_version": row.get("evidence_contract_version"),
            "cards_generated": summary.get("cards_generated", len(cards)),
            "missing_mapping_count": summary.get("missing_mapping_count"),
            "fallback_count": summary.get("fallback_count"),
            "mapping_error_count": summary.get("mapping_error_count"),
            "watch_vote_count": summary.get("watch_vote_count"),
            "abstain_count": summary.get("abstain_count"),
            "actions": summary.get("actions") or {},
            "vote_statuses": summary.get("vote_statuses") or {},
            "conviction_statuses": summary.get("conviction_statuses") or {},
            "reason_codes": row.get("reason_codes") or [],
            "walk_forward_level": row.get("walk_forward_level"),
            "walk_forward_pass_rate": row.get("walk_forward_pass_rate"),
            "turnover": row.get("turnover"),
        })

    summary = _evidence_card_summary(card_rows, strategies.get("evidence_summary") or {})
    visible_card_rows = _default_visible_evidence_cards(card_rows)
    collapsed_card_rows = _default_collapsed_evidence_cards(card_rows)
    evidence_cap_observe = _compact_evidence_cap_observe(
        cap_diagnostics=strategies.get("evidence_cap_diagnostics") or {},
        vote_summary=strategies.get("evidence_vote_summary") or {},
        card_rows=card_rows,
    )
    return {
        "available": bool(strategy_results),
        "playground_available": strategies.get("playground_available"),
        "generated_at": strategies.get("generated_at"),
        "data_quality": strategies.get("data_quality"),
        "regime_label": strategies.get("regime_label"),
        "regime_confidence": strategies.get("regime_confidence"),
        "strategy_count": len(strategy_rows),
        "card_count": len(card_rows),
        "evidence_summary": summary,
        "strategy_diversity": strategy_diversity,
        "strategy_independence": strategy_independence,
        "strategy_breadth_calibration": strategy_breadth,
        "evidence_vote_summary": strategies.get("evidence_vote_summary") or {},
        "evidence_cap_observe": evidence_cap_observe,
        "diversity_family_rows": strategy_diversity.get("family_rows") or [],
        "diversity_strategy_rows": strategy_diversity.get("strategy_rows") or [],
        "independence_pair_rows": strategy_independence.get("pair_rows") or [],
        "independence_low_correlation_pairs": strategy_independence.get("low_correlation_pairs") or [],
        "independence_high_correlation_pairs": strategy_independence.get("high_correlation_pairs") or [],
        "breadth_duplicate_pairs": strategy_breadth.get("high_correlation_pairs") or [],
        "breadth_diversifying_pairs": strategy_breadth.get("diversifying_pairs") or [],
        "independence_family_rows": strategy_independence.get("family_correlation_rows") or [],
        "strategy_rows": strategy_rows,
        "evidence_matrix_display_policy": {
            "default_visible_vote_statuses": ["voted", "mapping_error"],
            "default_collapsed_vote_statuses": ["watch", "abstain"],
        },
        "evidence_matrix_rows": visible_card_rows,
        "evidence_matrix_collapsed_rows": collapsed_card_rows,
        "evidence_card_rows": card_rows,
        "mapping_warning_rows": _evidence_mapping_warning_rows(card_rows),
        "role_action_rows": _role_action_rows(card_rows),
        "conviction_status_rows": _count_rows(card_rows, "conviction_status", label="status"),
        "warnings": strategies.get("warnings") or [],
    }


def _strategy_evidence_dashboard_status(evidence: dict[str, Any]) -> dict[str, Any]:
    if not evidence:
        return {"available": False, "reason": "strategy evidence unavailable"}
    return evidence


def _compact_strategy_independence(raw: dict[str, Any]) -> dict[str, Any]:
    if not raw:
        return {
            "available": False,
            "reason": "strategy independence unavailable",
            "execution_authority": "none",
            "target_weight_mutation": "none",
            "pair_rows": [],
            "low_correlation_pairs": [],
            "high_correlation_pairs": [],
            "family_correlation_rows": [],
        }
    baseline = raw.get("baseline_review") if isinstance(raw.get("baseline_review"), dict) else {}
    breadth_report = build_strategy_breadth_calibration_report(raw)
    return {
        "available": True,
        "contract_version": raw.get("contract_version"),
        "status": raw.get("status"),
        "min_overlap": raw.get("min_overlap"),
        "strategy_count": raw.get("strategy_count"),
        "alpha_strategy_count": raw.get("alpha_strategy_count"),
        "effective_independent_alpha_count": raw.get("effective_independent_alpha_count"),
        "avg_positive_correlation": _json_safe_number(raw.get("avg_positive_correlation")),
        "avg_abs_correlation": _json_safe_number(raw.get("avg_abs_correlation")),
        "avg_alpha_positive_correlation": _json_safe_number(raw.get("avg_alpha_positive_correlation")),
        "baseline_established": baseline.get("baseline_established"),
        "operator_review_required": baseline.get("operator_review_required"),
        "operator_acceptance_supported": baseline.get("operator_acceptance_supported"),
        "baseline_reason": baseline.get("reason"),
        "correlation_matrix_available": baseline.get("correlation_matrix_available"),
        "low_correlation_pair_count": baseline.get("low_correlation_pair_count"),
        "low_abs_correlation_threshold": baseline.get("low_abs_correlation_threshold"),
        "high_correlation_pair_count": len(raw.get("high_correlation_pairs") or []),
        "inverse_correlation_pair_count": len(raw.get("inverse_correlation_pairs") or []),
        "warnings": raw.get("warnings") or [],
        "execution_authority": raw.get("execution_authority"),
        "target_weight_mutation": raw.get("target_weight_mutation"),
        "strategy_breadth_calibration": breadth_report,
        "pair_rows": raw.get("pair_rows") or [],
        "low_correlation_pairs": raw.get("low_correlation_pairs") or [],
        "high_correlation_pairs": raw.get("high_correlation_pairs") or [],
        "family_correlation_rows": raw.get("family_correlation_rows") or [],
    }


def _compact_evidence_card(card: dict[str, Any], *, fallback_strategy: str) -> dict[str, Any]:
    diagnostics = card.get("diagnostics") if isinstance(card.get("diagnostics"), dict) else {}
    threshold = diagnostics.get("threshold") if isinstance(diagnostics.get("threshold"), dict) else {}
    conviction_diag = diagnostics.get("conviction") if isinstance(diagnostics.get("conviction"), dict) else {}
    vote_diag = card.get("vote_diagnostics") if isinstance(card.get("vote_diagnostics"), dict) else {}
    conviction = _json_safe_number(card.get("conviction"))
    conviction_n = int(_json_safe_number(card.get("conviction_n")) or 0)
    return {
        "strategy": card.get("strategy") or fallback_strategy,
        "strategy_version": card.get("strategy_version"),
        "ticker": card.get("ticker"),
        "role": card.get("role"),
        "action": card.get("action"),
        "signal_type": card.get("signal_type"),
        "horizon": card.get("horizon"),
        "confidence": _json_safe_number(card.get("confidence")),
        "conviction_display": _format_conviction_display(conviction),
        "conviction": conviction,
        "conviction_status": card.get("conviction_status") or conviction_diag.get("status") or "missing_profile",
        "conviction_source_bucket": card.get("conviction_source_bucket") or conviction_diag.get("source_bucket"),
        "conviction_n": conviction_n,
        "effective_confidence": _json_safe_number(card.get("effective_confidence")),
        "raw_score": _json_safe_number(card.get("raw_score")),
        "normalized_score": _json_safe_number(card.get("normalized_score")),
        "max_reasonable_weight": _json_safe_number(card.get("max_reasonable_weight")),
        "risk_budget_cost": _json_safe_number(card.get("risk_budget_cost")),
        "branch": card.get("branch"),
        "reason": card.get("reason"),
        "vote_status": card.get("vote_status") or "voted",
        "abstain_reason": card.get("abstain_reason"),
        "mapping_role": diagnostics.get("mapping_role"),
        "threshold_gte": threshold.get("gte"),
        "threshold_lt": threshold.get("lt"),
        "weight_formula": diagnostics.get("weight_formula"),
        "base_cap": diagnostics.get("base_cap"),
        "max_weight_multiplier": diagnostics.get("max_weight_multiplier"),
        "missing_safety_fields": diagnostics.get("missing_safety_fields") or [],
        "allowed_actions": diagnostics.get("allowed_actions") or [],
        "vote_reason_code": vote_diag.get("reason_code"),
        "vote_dedupe_key": vote_diag.get("dedupe_key"),
        "vote_alert_class": vote_diag.get("alert_class"),
        "vote_missing_fields": vote_diag.get("missing_fields") or [],
        "effective_confidence_rule": conviction_diag.get("effective_confidence_rule"),
        "conviction_shadow_only": conviction_diag.get("shadow_only"),
    }


def _default_visible_evidence_cards(cards: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        row for row in cards
        if str(row.get("vote_status") or "voted") in {"voted", "mapping_error"}
    ]


def _default_collapsed_evidence_cards(cards: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        row for row in cards
        if str(row.get("vote_status") or "voted") in {"watch", "abstain"}
    ]


def _evidence_card_summary(cards: list[dict[str, Any]], fallback_summary: dict[str, Any]) -> dict[str, Any]:
    actions = _count_map(cards, "action")
    conviction_statuses = _count_map(cards, "conviction_status")
    warning_rows = _evidence_mapping_warning_rows(cards)
    return {
        "cards_generated": len(cards),
        "missing_mapping_count": sum(1 for row in cards if "missing_compatibility_mapping" in str(row.get("reason") or "")),
        "fallback_count": len(warning_rows),
        "mapping_error_count": sum(1 for row in cards if row.get("vote_status") == "mapping_error"),
        "watch_vote_count": sum(1 for row in cards if row.get("vote_status") == "watch"),
        "abstain_count": sum(1 for row in cards if row.get("vote_status") == "abstain"),
        "actions": actions or fallback_summary.get("actions") or {},
        "vote_statuses": _count_map(cards, "vote_status") or fallback_summary.get("vote_statuses") or {},
        "conviction_statuses": conviction_statuses or fallback_summary.get("conviction_statuses") or {},
        "max_weight_by_action": _max_weight_by_action(cards),
    }


def _compact_evidence_cap_observe(
    *,
    cap_diagnostics: dict[str, Any],
    vote_summary: dict[str, Any],
    card_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    if not cap_diagnostics:
        return {
            "available": False,
            "reason": "evidence cap diagnostics unavailable",
            "execution_effect": "diagnostic_only",
            "rows": [],
            "mapping_error_rows": [],
        }

    rows: list[dict[str, Any]] = []
    for ticker, raw in cap_diagnostics.items():
        if not isinstance(raw, dict):
            continue
        clean_ticker = str(raw.get("ticker") or ticker or "").upper().strip()
        if not clean_ticker:
            continue
        votes = vote_summary.get(clean_ticker) if isinstance(vote_summary, dict) else {}
        if not isinstance(votes, dict):
            votes = {}
        static_cap = _json_safe_number(raw.get("static_cap")) or 0.0
        adjusted_cap = _json_safe_number(raw.get("evidence_adjusted_cap")) or 0.0
        current_or_target = _json_safe_number(raw.get("current_or_target_weight")) or 0.0
        cap_reduction = max(static_cap - adjusted_cap, 0.0)
        rows.append({
            "ticker": clean_ticker,
            "static_cap": round(static_cap, 6),
            "evidence_adjusted_cap": round(adjusted_cap, 6),
            "cap_reduction": round(cap_reduction, 6),
            "current_or_target_weight": round(current_or_target, 6),
            "would_clip": bool(raw.get("would_clip")),
            "would_clip_to": raw.get("would_clip_to"),
            "coverage_ratio": _json_safe_number(raw.get("coverage_ratio")),
            "evidence_quality_multiplier": _json_safe_number(raw.get("evidence_quality_multiplier")),
            "conviction_status": raw.get("conviction_status"),
            "conviction_discount": _json_safe_number(raw.get("conviction_discount")),
            "history_days": raw.get("history_days"),
            "history_discount": _json_safe_number(raw.get("history_discount")),
            "voted_count": raw.get("voted_count", votes.get("voted_count")),
            "watch_count": votes.get("watch_count"),
            "abstain_count": raw.get("abstain_count", votes.get("abstain_count")),
            "mapping_error_count": raw.get("mapping_error_count", votes.get("mapping_error_count")),
            "main_abstain_reason": _main_abstain_reason(votes.get("abstain_reasons") or []),
            "execution_effect": raw.get("execution_effect") or "diagnostic_only",
        })

    rows.sort(
        key=lambda item: (
            not bool(item.get("would_clip")),
            -float(item.get("cap_reduction") or 0.0),
            str(item.get("ticker") or ""),
        )
    )
    mapping_error_rows = _evidence_mapping_error_rows(card_rows)
    return {
        "available": True,
        "contract_version": "evidence_cap_observe_dashboard_v1",
        "execution_effect": "diagnostic_only",
        "ticker_count": len(rows),
        "degraded_ticker_count": sum(1 for row in rows if float(row.get("cap_reduction") or 0.0) > 0),
        "would_clip_count": sum(1 for row in rows if row.get("would_clip")),
        "mapping_error_count": len(mapping_error_rows),
        "top_degraded_tickers": [row.get("ticker") for row in rows if float(row.get("cap_reduction") or 0.0) > 0],
        "rows": rows,
        "mapping_error_rows": mapping_error_rows,
    }


def _main_abstain_reason(rows: list[dict[str, Any]]) -> str | None:
    for row in rows:
        if not isinstance(row, dict):
            continue
        reason = str(row.get("reason") or "").strip()
        fields = [str(field) for field in row.get("fields") or [] if str(field)]
        if reason and fields:
            return f"{reason}:{','.join(fields)}"
        if reason:
            return reason
    return None


def _evidence_mapping_error_rows(cards: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for card in cards:
        if str(card.get("vote_status") or "") != "mapping_error":
            continue
        key = str(
            card.get("vote_dedupe_key")
            or f"{card.get('strategy')}:{card.get('ticker')}:{card.get('vote_reason_code')}"
        )
        if key in seen:
            continue
        seen.add(key)
        rows.append({
            "ticker": card.get("ticker"),
            "strategy": card.get("strategy"),
            "reason_code": card.get("vote_reason_code"),
            "reason": card.get("reason"),
            "dedupe_key": key,
            "alert_class": card.get("vote_alert_class"),
        })
    rows.sort(key=lambda item: (str(item.get("ticker") or ""), str(item.get("strategy") or "")))
    return rows


def _evidence_mapping_warning_rows(cards: list[dict[str, Any]]) -> list[dict[str, Any]]:
    warning_tokens = (
        "missing_",
        "not_allowed",
        "fallback",
        "unknown_weight_formula",
        "insufficient_conviction_samples",
        "historical_prior_requires_live_confirmation",
    )
    rows = []
    for card in cards:
        reason = str(card.get("reason") or "")
        if not any(token in reason for token in warning_tokens) and not card.get("missing_safety_fields"):
            continue
        rows.append({
            "strategy": card.get("strategy"),
            "ticker": card.get("ticker"),
            "role": card.get("role"),
            "action": card.get("action"),
            "reason": reason,
            "missing_safety_fields": card.get("missing_safety_fields") or [],
            "allowed_actions": card.get("allowed_actions") or [],
            "conviction_status": card.get("conviction_status"),
            "conviction_n": card.get("conviction_n"),
        })
    return rows


def _role_action_rows(cards: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    for card in cards:
        key = (str(card.get("role") or "unknown"), str(card.get("action") or "unknown"))
        row = grouped.setdefault(
            key,
            {
                "role": key[0],
                "action": key[1],
                "count": 0,
                "max_reasonable_weight_max": 0.0,
                "avg_confidence": 0.0,
                "tickers": [],
            },
        )
        row["count"] += 1
        row["max_reasonable_weight_max"] = max(
            float(row.get("max_reasonable_weight_max") or 0.0),
            float(card.get("max_reasonable_weight") or 0.0),
        )
        row["avg_confidence"] += float(card.get("confidence") or 0.0)
        ticker = card.get("ticker")
        if ticker:
            row["tickers"].append(ticker)
    out = []
    for row in grouped.values():
        count = int(row.get("count") or 0)
        out.append({
            **row,
            "avg_confidence": round(float(row.get("avg_confidence") or 0.0) / count, 6) if count else 0.0,
            "max_reasonable_weight_max": round(float(row.get("max_reasonable_weight_max") or 0.0), 6),
            "tickers": sorted(set(row.get("tickers") or [])),
        })
    return sorted(out, key=lambda row: (str(row.get("role") or ""), str(row.get("action") or "")))


def _count_rows(rows: list[dict[str, Any]], key: str, *, label: str) -> list[dict[str, Any]]:
    return [
        {label: name, "count": count}
        for name, count in sorted(_count_map(rows, key).items())
    ]


def _count_map(rows: list[dict[str, Any]], key: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        name = str(row.get(key) or "unknown")
        counts[name] = counts.get(name, 0) + 1
    return dict(sorted(counts.items()))


def _dict_rows(values: dict[str, Any], key_label: str, value_label: str) -> list[dict[str, Any]]:
    return [
        {key_label: key, value_label: value}
        for key, value in sorted((values or {}).items(), key=lambda item: str(item[0]))
    ]


def _max_weight_by_action(cards: list[dict[str, Any]]) -> dict[str, float]:
    out: dict[str, float] = {}
    for card in cards:
        action = str(card.get("action") or "unknown")
        out[action] = round(
            max(out.get(action, 0.0), float(card.get("max_reasonable_weight") or 0.0)),
            6,
        )
    return dict(sorted(out.items()))


def _format_conviction_display(conviction: float | None) -> str:
    if conviction is None:
        return "--"
    return f"{conviction:.1%}"


def _compact_live_signal_conviction_summary(raw: dict[str, Any]) -> dict[str, Any]:
    if not raw:
        return {"available": False, "reason": "live signal conviction summary unavailable"}
    pending = raw.get("pending_outcomes") if isinstance(raw.get("pending_outcomes"), dict) else {}
    return {
        "available": raw.get("status") == "available",
        "overview": {
            "contract_version": raw.get("contract_version"),
            "status": raw.get("status"),
            "as_of_date": raw.get("as_of_date"),
            "latest_profile_date": raw.get("latest_profile_date"),
            "signals_recorded_today": raw.get("signals_recorded_today"),
            "outcomes_labeled_today": raw.get("outcomes_labeled_today"),
            "signals_total": raw.get("signals_total"),
            "outcomes_total": raw.get("outcomes_total"),
            "requires_live_confirmation_count": raw.get("requires_live_confirmation_count"),
            "display_note": raw.get("display_note"),
        },
        "pending_outcomes": {
            "total": pending.get("total"),
            "mature": pending.get("mature"),
            "maturity_model": pending.get("maturity_model"),
        },
        "pending_by_horizon_rows": _pending_by_horizon_rows(pending.get("by_horizon") or {}),
        "historical_prior_profiles": _conviction_profile_display_rows(raw.get("historical_prior_profiles") or []),
        "live_paper_profiles": _conviction_profile_display_rows(raw.get("live_paper_profiles") or []),
        "combined_profiles": _conviction_profile_display_rows(raw.get("combined_profiles") or []),
        "regime_level_profiles": _conviction_profile_display_rows(raw.get("regime_level_profiles") or []),
        "regime_summary_rows": _regime_summary_display_rows(raw.get("regime_summary_rows") or []),
        "profile_count_rows": _dict_count_rows(raw.get("profile_counts") or {}, label="source_bucket"),
        "status_count_rows": _dict_count_rows(raw.get("status_counts") or {}, label="status"),
        "legacy_operational_status_count_rows": _dict_count_rows(
            raw.get("legacy_operational_status_counts") or raw.get("status_counts") or {},
            label="legacy_status",
        ),
        "statistical_status_count_rows": _dict_count_rows(
            raw.get("statistical_status_counts") or {},
            label="statistical_status",
        ),
        "display_contract": {
            "conviction_number_policy": "no_naked_conviction",
            "required_context": "conviction_display + source_bucket + n + statistical_status",
            "execution_authority": "none",
        },
    }


def _pending_by_horizon_rows(by_horizon: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for horizon, item in sorted(by_horizon.items(), key=lambda pair: int(pair[0]) if str(pair[0]).isdigit() else 999):
        row = item if isinstance(item, dict) else {}
        rows.append({
            "horizon_days": horizon,
            "missing": row.get("missing"),
            "mature": row.get("mature"),
        })
    return rows


def _conviction_profile_display_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        out.append({
            "strategy": row.get("strategy"),
            "ticker": row.get("ticker"),
            "branch": row.get("branch"),
            "action": row.get("action"),
            "regime_at_signal": row.get("regime_at_signal"),
            "horizon": row.get("horizon"),
            "source_bucket": row.get("source_bucket"),
            "n": row.get("n"),
            "status": row.get("status"),
            "conviction_display": row.get("conviction_display"),
            "hit_rate": row.get("hit_rate"),
            "avg_excess_vs_spy": row.get("avg_excess_vs_spy"),
            "ic": row.get("ic"),
            "last_signal_date": row.get("last_signal_date"),
            "data_lag_filtered": row.get("data_lag_filtered"),
            "requires_live_confirmation": row.get("requires_live_confirmation"),
            "source_counts": row.get("source_counts") or {},
        })
    return out


def _regime_summary_display_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        out.append({
            "regime_at_signal": row.get("regime_at_signal"),
            "source_bucket": row.get("source_bucket"),
            "profile_count": row.get("profile_count"),
            "total_n": row.get("total_n"),
            "operational_calibrated_profiles": row.get("calibrated_profiles"),
            "early_profiles": row.get("early_profiles"),
            "insufficient_profiles": row.get("insufficient_profiles"),
            "hit_rate": row.get("hit_rate"),
            "avg_excess_vs_spy": row.get("avg_excess_vs_spy"),
            "ic": row.get("ic"),
            "data_lag_filtered": row.get("data_lag_filtered"),
        })
    return out


def _dict_count_rows(counts: dict[str, Any], *, label: str) -> list[dict[str, Any]]:
    return [
        {label: key, "count": value}
        for key, value in sorted(counts.items())
    ]


def _compact_performance_attribution_row(row: Any) -> dict[str, Any]:
    source_tickers = row.source_tickers if isinstance(row.source_tickers, dict) else {}
    diagnostics = row.diagnostics if isinstance(row.diagnostics, dict) else {}
    return {
        "period_key": row.period_key,
        "period_start": _iso(row.period_start),
        "period_end": _iso(row.period_end),
        "generated_at": _iso(row.generated_at),
        "status": row.status,
        "attribution_method": row.attribution_method,
        "portfolio_return": _json_safe_number(row.portfolio_return),
        "arithmetic_portfolio_return": _json_safe_number(row.arithmetic_portfolio_return),
        "spy_beta": _json_safe_number(row.spy_beta),
        "spy_beta_contribution": _json_safe_number(row.spy_beta_contribution),
        "qqq_beta": _json_safe_number(row.qqq_beta),
        "qqq_beta_contribution": _json_safe_number(row.qqq_beta_contribution),
        "momentum_beta": _json_safe_number(row.momentum_beta),
        "momentum_factor_contribution": _json_safe_number(row.momentum_factor_contribution),
        "intercept_contribution": _json_safe_number(row.intercept_contribution),
        "residual_alpha_candidate": _json_safe_number(row.residual_alpha_candidate),
        "r_squared": _json_safe_number(row.r_squared),
        "sample_count": row.sample_count,
        "data_quality": row.data_quality,
        "benchmark_source": row.benchmark_source,
        "source_tickers": source_tickers,
        "momentum_proxy": source_tickers.get("momentum") or diagnostics.get("momentum_proxy"),
        "residual_label": diagnostics.get("residual_label"),
        "content_hash": row.content_hash,
    }


def _compact_alpha_validation_row(row: Any) -> dict[str, Any]:
    warnings = row.warnings if isinstance(row.warnings, list) else []
    return {
        "id": row.id,
        "analysis_id": row.analysis_id,
        "generated_at": _iso(row.generated_at),
        "analyzed_at": _iso(row.analyzed_at),
        "trigger_type": row.trigger_type,
        "risk_approved": row.risk_approved,
        "execution_status": row.execution_status,
        "status": row.status,
        "data_quality": row.data_quality,
        "cost_gate_status": row.cost_gate_status,
        "low_edge_trade_count": row.low_edge_trade_count,
        "min_edge_to_cost_ratio": _json_safe_number(row.min_edge_to_cost_ratio),
        "avg_edge_to_cost_ratio": _json_safe_number(row.avg_edge_to_cost_ratio),
        "var_95_loss": _json_safe_number(row.var_95_loss),
        "cvar_95_loss": _json_safe_number(row.cvar_95_loss),
        "max_scenario_loss": _json_safe_number(row.max_scenario_loss),
        "signal_weighted_effective_n": _json_safe_number(row.signal_weighted_effective_n),
        "signal_alignment_score": _json_safe_number(row.signal_alignment_score),
        "signal_objective_warning_count": row.signal_objective_warning_count,
        "independent_alpha_family_count": row.independent_alpha_family_count,
        "actionable_alpha_strategy_count": row.actionable_alpha_strategy_count,
        "calibrated_conviction_count": row.calibrated_conviction_count,
        "early_conviction_count": row.early_conviction_count,
        "insufficient_conviction_count": row.insufficient_conviction_count,
        "warning_count": len(warnings),
        "warnings": warnings,
        "content_hash": row.content_hash,
    }


def _alpha_validation_trend_metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "sample_count": len(rows),
        "avg_min_edge_to_cost_ratio": _mean_present(rows, "min_edge_to_cost_ratio"),
        "avg_var_95_loss": _mean_present(rows, "var_95_loss"),
        "avg_cvar_95_loss": _mean_present(rows, "cvar_95_loss"),
        "max_scenario_loss": max(
            (_json_safe_number(row.get("max_scenario_loss")) or 0.0 for row in rows),
            default=0.0,
        ),
        "avg_signal_alignment_score": _mean_present(rows, "signal_alignment_score"),
        "latest_independent_alpha_family_count": rows[0].get("independent_alpha_family_count") if rows else None,
        "latest_calibrated_conviction_count": rows[0].get("calibrated_conviction_count") if rows else None,
        "warning_runs": sum(1 for row in rows if int(row.get("warning_count") or 0) > 0),
    }


def _performance_attribution_breakdown_rows(latest: dict[str, Any]) -> list[dict[str, Any]]:
    source_tickers = latest.get("source_tickers") if isinstance(latest.get("source_tickers"), dict) else {}
    return [
        {
            "component": "SPY beta",
            "source": source_tickers.get("spy") or "SPY",
            "beta": latest.get("spy_beta"),
            "contribution": latest.get("spy_beta_contribution"),
        },
        {
            "component": "QQQ / growth beta",
            "source": source_tickers.get("qqq") or "QQQ",
            "beta": latest.get("qqq_beta"),
            "contribution": latest.get("qqq_beta_contribution"),
        },
        {
            "component": "momentum proxy",
            "source": source_tickers.get("momentum") or latest.get("momentum_proxy"),
            "beta": latest.get("momentum_beta"),
            "contribution": latest.get("momentum_factor_contribution"),
        },
        {
            "component": "residual alpha candidate",
            "source": "regression residual",
            "beta": None,
            "contribution": latest.get("residual_alpha_candidate"),
        },
    ]


def _compact_transaction_cost_gate(gate: dict[str, Any]) -> dict[str, Any]:
    if not gate:
        return {}
    rows = [
        row for row in (gate.get("rows") or [])
        if isinstance(row, dict)
    ]
    summary = gate.get("summary") if isinstance(gate.get("summary"), dict) else {}
    config = gate.get("config") if isinstance(gate.get("config"), dict) else {}
    return {
        "mode": gate.get("mode"),
        "broker": gate.get("broker"),
        "status": gate.get("status"),
        "execution_effect": gate.get("execution_effect"),
        "summary": summary,
        "warnings": gate.get("warnings") or [],
        "config": {
            "mode": config.get("mode"),
            "broker": config.get("broker"),
            "min_edge_to_cost_ratio": config.get("min_edge_to_cost_ratio"),
            "warn_on_buys_only": config.get("warn_on_buys_only"),
        },
        "rows": rows,
    }


def _compact_portfolio_risk_diagnostic(diagnostic: dict[str, Any]) -> dict[str, Any]:
    if not diagnostic:
        return {"available": False, "reason": "portfolio risk diagnostic unavailable"}
    return {
        "available": True,
        "contract_version": diagnostic.get("contract_version"),
        "status": diagnostic.get("status"),
        "mode": diagnostic.get("mode"),
        "execution_authority": diagnostic.get("execution_authority"),
        "target_weight_mutation": diagnostic.get("target_weight_mutation"),
        "confidence_level": diagnostic.get("confidence_level"),
        "lookback_days": diagnostic.get("lookback_days"),
        "source": diagnostic.get("source"),
        "data_quality": diagnostic.get("data_quality"),
        "summary": diagnostic.get("summary") or {},
        "target_historical": diagnostic.get("target_historical") or {},
        "current_historical": diagnostic.get("current_historical") or {},
        "target_scenarios": diagnostic.get("target_scenarios") or [],
        "current_scenarios": diagnostic.get("current_scenarios") or [],
        "target_scenario_stress": diagnostic.get("target_scenario_stress") or {},
        "current_scenario_stress": diagnostic.get("current_scenario_stress") or {},
        "target_beta_shock": diagnostic.get("target_beta_shock") or {},
        "current_beta_shock": diagnostic.get("current_beta_shock") or {},
        "warnings": diagnostic.get("warnings") or [],
        "error": diagnostic.get("error"),
    }


def _compact_final_validation(validation: dict[str, Any]) -> dict[str, Any]:
    if not validation:
        return {}
    drift = validation.get("drift") or {}
    policy = validation.get("policy_evaluation") or {}
    return {
        "mode": validation.get("mode"),
        "approved": validation.get("approved"),
        "execution_effect": validation.get("execution_effect"),
        "policy_allowed": policy.get("allowed"),
        "max_abs_drift": drift.get("max_abs_drift"),
        "material_drift_threshold": drift.get("material_drift_threshold"),
        "material_drift": drift.get("material_drift"),
        "mutation_types": validation.get("mutation_types") or [],
        "blocking_violations": validation.get("blocking_violations") or [],
        "severe_violations": validation.get("severe_violations") or [],
        "conditional_mutation_violations": validation.get("conditional_mutation_violations") or [],
    }


def _compact_account_state_guard(guard: dict[str, Any]) -> dict[str, Any]:
    if not guard:
        return {}
    checks = guard.get("checks") if isinstance(guard.get("checks"), dict) else {}
    return {
        "mode": guard.get("mode"),
        "status": guard.get("status"),
        "allowed": guard.get("allowed"),
        "would_block": guard.get("would_block"),
        "pipeline_enforcement": guard.get("pipeline_enforcement"),
        "pipeline_effect_status": guard.get("pipeline_effect_status"),
        "execution_effect": guard.get("execution_effect"),
        "primary_blockers": guard.get("blockers") or [],
        "warnings": guard.get("warnings") or [],
        "snapshot": guard.get("snapshot") or {},
        "config": guard.get("config") or {},
        "checks": [
            {
                "check": name,
                "pass": row.get("pass"),
                "actual": row.get("actual"),
                "threshold": row.get("threshold"),
                "reason": row.get("reason"),
            }
            for name, row in checks.items()
            if isinstance(row, dict)
        ],
    }


def _compact_auto_pause(auto_pause: dict[str, Any]) -> dict[str, Any]:
    if not auto_pause:
        return {}
    triggers = auto_pause.get("triggers") if isinstance(auto_pause.get("triggers"), list) else []
    return {
        "mode": auto_pause.get("mode"),
        "status": auto_pause.get("status"),
        "would_pause": auto_pause.get("would_pause"),
        "should_pause": auto_pause.get("should_pause"),
        "execution_effect": auto_pause.get("execution_effect"),
        "primary_trigger": auto_pause.get("primary_trigger"),
        "reason": auto_pause.get("reason"),
        "config": auto_pause.get("config") or {},
        "triggers": [
            {
                "trigger": row.get("name"),
                "triggered": row.get("triggered"),
                "value": row.get("value"),
                "threshold": row.get("threshold"),
                "severity": row.get("severity"),
                "details": row.get("details"),
            }
            for row in triggers
            if isinstance(row, dict)
        ],
    }


def _compact_account_state_snapshot(row: Any) -> dict[str, Any]:
    if not row:
        return {"available": False}
    raw = row.raw_snapshot if isinstance(row.raw_snapshot, dict) else {}
    holdings = row.holdings_weights if isinstance(row.holdings_weights, dict) else {}
    targets = row.target_weights if isinstance(row.target_weights, dict) else {}
    return {
        "available": True,
        "id": row.id,
        "qc_snapshot_id": row.qc_snapshot_id,
        "recorded_at": _iso(row.recorded_at),
        "account_timestamp": _iso(row.account_timestamp),
        "source_packet_type": row.source_packet_type,
        "contract_version": row.contract_version,
        "account_status": row.account_status,
        "data_status": row.data_status,
        "policy_version": row.policy_version,
        "total_value": _json_safe_number(row.total_value),
        "cash": _json_safe_number(row.cash),
        "cash_pct": _json_safe_number(row.cash_pct),
        "buying_power": _json_safe_number(row.buying_power),
        "open_order_count": row.open_order_count,
        "has_open_orders": row.has_open_orders,
        "is_market_open": row.is_market_open,
        "last_command_id": getattr(row, "last_command_id", None) or raw.get("last_command_id"),
        "active_command_id": getattr(row, "active_command_id", None) or raw.get("active_command_id"),
        "active_execution_status": getattr(row, "active_execution_status", None) or raw.get("active_execution_status"),
        "processed_command_count": getattr(row, "processed_command_count", None) or raw.get("processed_command_count"),
        "holdings_count": len(holdings),
        "target_count": len(targets),
        "explicit_account_state": raw.get("explicit_account_state"),
        "warnings": raw.get("warnings") or [],
    }


def _snapshot_active_command_id(row: Any) -> str:
    if not row:
        return ""
    raw = row.raw_snapshot if isinstance(getattr(row, "raw_snapshot", None), dict) else {}
    for value in (
        getattr(row, "active_command_id", None),
        raw.get("active_command_id"),
        getattr(row, "last_command_id", None),
        raw.get("last_command_id"),
    ):
        command_id = str(value or "").strip()
        if command_id:
            return command_id
    return ""


def _compact_active_execution_panel(
    *,
    snapshot: Any,
    command_row: Any,
    lifecycle_events: list[Any],
    config: dict[str, Any],
) -> dict[str, Any]:
    if not snapshot:
        return {"available": False, "status": "no_account_snapshot"}
    raw = snapshot.raw_snapshot if isinstance(getattr(snapshot, "raw_snapshot", None), dict) else {}
    command_id = _snapshot_active_command_id(snapshot)
    snapshot_status = str(
        getattr(snapshot, "active_execution_status", None)
        or raw.get("active_execution_status")
        or ""
    ).lower().strip()
    qc_status = str(getattr(command_row, "qc_status", "") or "").lower().strip()
    status = snapshot_status or qc_status or "idle"
    open_order_count = getattr(snapshot, "open_order_count", None)
    has_open_orders = bool(getattr(snapshot, "has_open_orders", False))
    if open_order_count is not None:
        try:
            has_open_orders = has_open_orders or int(open_order_count or 0) > 0
        except (TypeError, ValueError):
            pass
    active_statuses = {"accepted", "orders_submitted", "partial"}
    is_active = bool(command_id) and (status in active_statuses or has_open_orders)
    target_weights = snapshot.target_weights if isinstance(getattr(snapshot, "target_weights", None), dict) else {}
    holdings_weights = snapshot.holdings_weights if isinstance(getattr(snapshot, "holdings_weights", None), dict) else {}
    drift_rows = _target_actual_drift_rows(target_weights, holdings_weights)
    order_summary = _latest_order_summary_from_lifecycle(lifecycle_events, command_row)
    started_at = getattr(command_row, "qc_ack_at", None) or getattr(command_row, "executed_at", None)
    allow_reduce_only = bool((config or {}).get("allow_reduce_only_override", True))
    stale = _active_execution_stale_status(
        {
            "command_id": command_id,
            "status": status,
            "open_order_count": open_order_count,
            "has_open_orders": has_open_orders,
            "started_at": started_at,
            "recorded_at": getattr(snapshot, "recorded_at", None),
        },
        config,
    )
    return {
        "available": True,
        "active": is_active,
        "active_command_id": command_id or None,
        "status": status,
        "qc_status": qc_status or None,
        "submitted_order_count": order_summary.get("submitted_order_count"),
        "actual_order_count": order_summary.get("actual_order_count"),
        "filled_order_count": order_summary.get("filled_order_count"),
        "is_noop": order_summary.get("is_noop"),
        "open_order_count": open_order_count if open_order_count is not None else order_summary.get("open_order_count_after"),
        "has_open_orders": has_open_orders,
        "started_at": _iso(started_at),
        "elapsed_minutes": _elapsed_minutes(started_at),
        "latest_snapshot_at": _iso(getattr(snapshot, "recorded_at", None)),
        "target_count": len(target_weights),
        "holding_count": len(holdings_weights),
        "max_target_actual_drift": max((abs(float(row.get("diff") or 0.0)) for row in drift_rows), default=0.0),
        "stale": stale.get("is_stale"),
        "stale_reason": stale.get("reason"),
        "stale_elapsed_minutes": stale.get("elapsed_minutes"),
        "stale_threshold_minutes": stale.get("threshold_minutes"),
        "stale_auto_action": stale.get("auto_action"),
        "auto_cancel_stale_open_orders": stale.get("auto_cancel"),
        "stale_operator_action": stale.get("operator_action"),
        "can_ordinary_rebalance": not is_active,
        "can_reduce_only": bool(is_active and allow_reduce_only),
        "execution_contract": "accepted_is_not_reconciled",
        "operator_note": _active_execution_operator_note(is_active, status, has_open_orders),
        "drift_rows": drift_rows[:12],
        "recent_event_rows": [_compact_lifecycle_event(row) for row in lifecycle_events],
    }


def _active_execution_stale_status(active: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
    try:
        from services.execution_lifecycle import evaluate_stale_active_execution

        return evaluate_stale_active_execution(active, config)
    except Exception as exc:
        return {
            "is_stale": False,
            "reason": f"stale_check_unavailable:{type(exc).__name__}",
            "auto_action": "none",
            "auto_cancel": False,
        }


def _latest_order_summary_from_lifecycle(events: list[Any], command_row: Any) -> dict[str, Any]:
    for event in events or []:
        payload = event.payload if isinstance(getattr(event, "payload", None), dict) else {}
        order_summary = payload.get("order_summary") if isinstance(payload.get("order_summary"), dict) else {}
        if order_summary:
            return order_summary
    response = command_row.qc_response if command_row and isinstance(getattr(command_row, "qc_response", None), dict) else {}
    return response.get("order_summary") if isinstance(response.get("order_summary"), dict) else {}


def _target_actual_drift_rows(target_weights: dict[str, Any], holdings_weights: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for ticker in sorted((set(target_weights or {}) | set(holdings_weights or {})) - {"CASH"}):
        target = _json_safe_number((target_weights or {}).get(ticker)) or 0.0
        actual = _json_safe_number((holdings_weights or {}).get(ticker)) or 0.0
        diff = round(actual - target, 6)
        if abs(diff) <= 1e-9:
            continue
        rows.append({
            "ticker": ticker,
            "target": round(target, 6),
            "actual": round(actual, 6),
            "diff": diff,
        })
    rows.sort(key=lambda row: (-abs(float(row.get("diff") or 0.0)), str(row.get("ticker") or "")))
    return rows


def _elapsed_minutes(value: Any) -> float | None:
    if not value:
        return None
    if not isinstance(value, datetime):
        parsed = _datetime_from_text(str(value))
        if parsed is None:
            return None
        value = parsed
    if value.tzinfo is not None:
        value = value.replace(tzinfo=None)
    return round(max((datetime.utcnow() - value).total_seconds() / 60.0, 0.0), 1)


def _datetime_from_text(text: str) -> datetime | None:
    clean = str(text or "").strip()
    if not clean:
        return None
    if clean.endswith("Z"):
        clean = clean[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(clean)
    except ValueError:
        return None
    if parsed.tzinfo is not None:
        return parsed.replace(tzinfo=None)
    return parsed


def _active_execution_operator_note(is_active: bool, status: str, has_open_orders: bool) -> str:
    if not is_active:
        return "No active execution; ordinary rebalance may proceed if other gates pass."
    if has_open_orders:
        return "Active execution has open orders; ordinary rebalance should wait for reconciliation."
    if status in {"accepted", "orders_submitted", "partial"}:
        return "Command is non-terminal; wait for heartbeat reconciliation before ordinary rebalance."
    return "Review command lifecycle before sending another ordinary rebalance."


def _compact_lifecycle_event(row: Any) -> dict[str, Any]:
    payload = row.payload if isinstance(row.payload, dict) else {}
    response = payload.get("qc_response") if isinstance(payload.get("qc_response"), dict) else {}
    command_payload = payload.get("command_payload") if isinstance(payload.get("command_payload"), dict) else {}
    target_weights = command_payload.get("weights") if isinstance(command_payload.get("weights"), dict) else {}
    order_summary = payload.get("order_summary") if isinstance(payload.get("order_summary"), dict) else {}
    return {
        "event_time": _iso(row.event_time),
        "command_id": row.command_id,
        "analysis_id": row.analysis_id,
        "event_type": row.event_type,
        "event_status": row.event_status,
        "source": row.source,
        "reason": row.reason or payload.get("reason") or response.get("reason"),
        "qc_status": payload.get("qc_status") or response.get("status"),
        "execution_state": payload.get("execution_state") or response.get("execution_state"),
        "submitted_order_count": order_summary.get("submitted_order_count"),
        "actual_order_count": order_summary.get("actual_order_count"),
        "filled_order_count": order_summary.get("filled_order_count"),
        "is_noop": order_summary.get("is_noop"),
        "open_order_count": order_summary.get("open_order_count_after") or order_summary.get("open_order_count"),
        "max_abs_diff": payload.get("max_abs_diff"),
        "diff_count": len(payload.get("diffs") or []) if isinstance(payload.get("diffs"), list) else None,
        "policy_mismatch": response.get("policy_mismatch"),
        "policy_version": response.get("policy_version") or payload.get("policy_version"),
        "target_count": len(target_weights),
        "payload_keys": sorted(payload.keys()),
    }


TERMINAL_LIFECYCLE_STATES = {
    "reconciled",
    "reconciliation_drift",
    "failed_no_fill",
    "superseded",
    "timeout_no_execution_confirmed",
}

ACTIVE_LIFECYCLE_STATES = {
    "qc_accepted": "accepted",
    "accepted": "accepted",
    "orders_submitted": "orders_submitted",
    "partial": "partial",
    "filled": "filled",
}


def _lifecycle_status_by_command(events: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    terminal: dict[str, dict[str, Any]] = {}
    active: dict[str, dict[str, Any]] = {}
    for event in events or []:
        command_id = str(event.get("command_id") or "").strip()
        if not command_id:
            continue
        event_type = str(event.get("event_type") or "").strip()
        event_status = str(event.get("event_status") or "").strip()
        status_key = event_status or event_type
        if status_key in TERMINAL_LIFECYCLE_STATES or event_type in TERMINAL_LIFECYCLE_STATES:
            terminal.setdefault(command_id, _lifecycle_status_payload(event, status_key or event_type, "terminal_lifecycle"))
        elif status_key in ACTIVE_LIFECYCLE_STATES or event_type in ACTIVE_LIFECYCLE_STATES:
            display = ACTIVE_LIFECYCLE_STATES.get(status_key) or ACTIVE_LIFECYCLE_STATES.get(event_type) or status_key or event_type
            active.setdefault(command_id, _lifecycle_status_payload(event, display, "active_lifecycle"))
    out = dict(active)
    out.update(terminal)
    return out


def _lifecycle_status_payload(event: dict[str, Any], display_status: str, source: str) -> dict[str, Any]:
    return {
        "display_status": display_status,
        "source": source,
        "event_type": event.get("event_type"),
        "event_status": event.get("event_status"),
        "event_time": event.get("event_time"),
    }


def _compact_execution_row(row: Any, *, lifecycle_status: dict[str, Any] | None = None) -> dict[str, Any]:
    response = row.qc_response if isinstance(row.qc_response, dict) else {}
    order_summary = response.get("order_summary") if isinstance(response.get("order_summary"), dict) else {}
    lifecycle_status = lifecycle_status or {}
    fallback_status = row.qc_status or row.status
    display_status = lifecycle_status.get("display_status") or fallback_status
    return {
        "executed_at": _iso(row.executed_at),
        "command_id": row.command_id,
        "analysis_id": row.analysis_id,
        "command_type": row.command_type,
        "display_status": display_status,
        "lifecycle_display_status": lifecycle_status.get("display_status"),
        "lifecycle_status_source": lifecycle_status.get("source") or "execution_log",
        "latest_lifecycle_event": lifecycle_status.get("event_type"),
        "latest_lifecycle_event_status": lifecycle_status.get("event_status"),
        "latest_lifecycle_event_time": lifecycle_status.get("event_time"),
        "status": row.status,
        "qc_status": row.qc_status,
        "qc_ack_at": _iso(row.qc_ack_at),
        "qc_rejection_reason": row.qc_rejection_reason or response.get("reason"),
        "execution_state": response.get("execution_state"),
        "active_command_id": response.get("active_command_id"),
        "submitted_order_count": order_summary.get("submitted_order_count"),
        "actual_order_count": order_summary.get("actual_order_count"),
        "filled_order_count": order_summary.get("filled_order_count"),
        "is_noop": order_summary.get("is_noop"),
        "open_order_count": order_summary.get("open_order_count_after") or order_summary.get("open_order_count"),
        "superseded_command_id": response.get("superseded_command_id"),
        "canceled_order_count": response.get("canceled_order_count"),
        "policy_mismatch": response.get("policy_mismatch"),
        "retry_count": row.retry_count,
    }


def _compact_deferred_execution_row(row: Any) -> dict[str, Any]:
    review = row.review_payload if isinstance(row.review_payload, dict) else {}
    return {
        "created_at": _iso(row.created_at),
        "updated_at": _iso(row.updated_at),
        "resolved_at": _iso(row.resolved_at),
        "deferred_id": row.deferred_id,
        "analysis_id": row.analysis_id,
        "command_id": row.command_id,
        "status": row.status,
        "side": row.side,
        "ticker": row.ticker,
        "original_delta": _json_safe_number(row.original_delta),
        "remaining_delta": _json_safe_number(row.remaining_delta),
        "current_weight": _json_safe_number(row.current_weight),
        "desired_weight": _json_safe_number(row.desired_weight),
        "staged_weight": _json_safe_number(row.staged_weight),
        "latest_current_weight": _json_safe_number(row.latest_current_weight),
        "latest_desired_weight": _json_safe_number(row.latest_desired_weight),
        "latest_staged_weight": _json_safe_number(row.latest_staged_weight),
        "reason": row.reason,
        "resolution_reason": row.resolution_reason or review.get("reason"),
        "review_count": row.review_count,
    }


def _enrich_position_explanations_from_ledger(
    explanations: list[dict[str, Any]],
    ledger: dict[str, Any],
) -> list[dict[str, Any]]:
    ticker_rows = ledger.get("tickers") or {}
    if not ticker_rows:
        return explanations
    out = []
    seen = set()
    for row in explanations:
        if not isinstance(row, dict):
            continue
        ticker = str(row.get("ticker") or "").upper().strip()
        seen.add(ticker)
        ledger_explanation = ((ticker_rows.get(ticker) or {}).get("explanation") or {})
        merged = dict(row)
        for key in (
            "strategy_intent",
            "llm_effect",
            "construction_effect",
            "risk_governance_effect",
            "final_explanation",
        ):
            if ledger_explanation.get(key):
                merged[key] = ledger_explanation.get(key)
        out.append(merged)
    for ticker, raw in ticker_rows.items():
        if ticker in seen or not isinstance(raw, dict):
            continue
        explanation = raw.get("explanation") or {}
        if explanation:
            out.append(explanation)
    return out


def _sort_by_current_weight(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        rows,
        key=lambda row: (
            -float(row.get("current_weight") or 0.0),
            str(row.get("ticker") or ""),
        ),
    )


def _compact_ledger(ledger: dict[str, Any]) -> dict[str, Any]:
    if not ledger:
        return {"available": False}
    rows = ledger.get("top_decisions")
    if not rows:
        rows = _ledger_rows_from_tickers(ledger.get("tickers") or {})
    return {
        "available": True,
        "portfolio_summary": ledger.get("portfolio_summary") or {},
        "top_decisions": rows,
    }


def _ledger_rows_from_tickers(tickers: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for ticker, raw in tickers.items():
        if not isinstance(raw, dict):
            continue
        lifecycle = raw.get("trade_lifecycle") or {}
        advisory = raw.get("llm_advisory") or {}
        policy = raw.get("execution_policy") or {}
        hedge_path = raw.get("hedge_path") or {}
        final_target_label = classify_weight_column("final_target")
        pc_target_label = classify_weight_column("portfolio_construction_target")
        tb_target_label = classify_weight_column("target_builder_target")
        llm_target_label = classify_weight_column("diagnostic_llm_target")
        advisory_delta_label = classify_weight_column("validated_advisory_delta")
        rows.append({
            "ticker": raw.get("ticker") or ticker,
            "proposed_action": raw.get("proposed_action"),
            "final_action": raw.get("final_action"),
            "execution_status": raw.get("execution_status"),
            "cmd_id": raw.get("cmd_id"),
            "qc_status": raw.get("qc_status"),
            "qc_rejection_reason": raw.get("qc_rejection_reason"),
            "qc_timestamp": raw.get("qc_timestamp"),
            "risk_result": raw.get("risk_result"),
            "ticker_role": policy.get("ticker_role"),
            "single_cap": policy.get("single_cap"),
            "group_cap": policy.get("group_cap"),
            "policy_version": policy.get("policy_version"),
            "policy_cap_applied": policy.get("policy_cap_applied"),
            "policy_cap_original": policy.get("policy_cap_original"),
            "policy_group_scaled": policy.get("policy_group_scaled"),
            "cash_raised_by_policy_cap": policy.get("cash_raised_by_policy_cap"),
            "entered_via_hedge_path": hedge_path.get("entered_via_hedge_path"),
            "hedge_trigger_reasons": hedge_path.get("hedge_trigger_reasons") or [],
            "final_target": lifecycle.get("final_target"),
            "final_target_label": final_target_label.get("label"),
            "final_target_authority": final_target_label.get("authority"),
            "final_target_visual_class": final_target_label.get("visual_class"),
            "portfolio_construction_target": lifecycle.get("portfolio_construction_target"),
            "portfolio_construction_target_label": pc_target_label.get("label"),
            "portfolio_construction_target_authority": pc_target_label.get("authority"),
            "portfolio_construction_target_visual_class": pc_target_label.get("visual_class"),
            "target_builder_target": lifecycle.get("target_builder_target"),
            "target_builder_target_label": tb_target_label.get("label"),
            "target_builder_target_authority": tb_target_label.get("authority"),
            "target_builder_target_visual_class": tb_target_label.get("visual_class"),
            "diagnostic_llm_target": lifecycle.get("diagnostic_llm_target"),
            "diagnostic_llm_target_label": llm_target_label.get("label"),
            "diagnostic_llm_target_authority": llm_target_label.get("authority"),
            "diagnostic_llm_target_visual_class": llm_target_label.get("visual_class"),
            "validated_advisory_delta": lifecycle.get("validated_advisory_delta"),
            "validated_advisory_delta_label": advisory_delta_label.get("label"),
            "validated_advisory_delta_authority": advisory_delta_label.get("authority"),
            "validated_advisory_delta_visual_class": advisory_delta_label.get("visual_class"),
            "changed_by": lifecycle.get("changed_by") or [],
            "advisory_validator_result": advisory.get("validator_result"),
            "source_effects": raw.get("source_effects") or {},
        })
    return sorted(
        rows,
        key=lambda row: (
            str(row.get("final_action") or "") not in {"none", "unknown"},
            str(row.get("ticker") or ""),
        ),
    )


def render_dashboard(summary: dict[str, Any]) -> str:
    ops = summary["ops"]
    latest = summary["latest_analysis"]
    replay = summary["replay"]
    pc_readiness = summary.get("portfolio_construction_readiness") or {}
    sections = [
        ("execution", "Execution Control", _render_execution_control(summary.get("execution_control") or {}), True),
        ("latest", "Latest Decision", _render_latest_analysis(latest), True),
        ("weekend-review", "Weekend Trading Review", _render_weekend_review_operator(summary.get("weekend_review_operator") or {}), True),
        ("validation-overview", "Validation Overview", _render_validation_overview(summary.get("validation_overview") or {}), True),
        ("target-path", "Target Path", _render_target_path_visibility(latest.get("target_path_visibility") or {}), True),
        ("weight-source", "Weight Source Contract", _render_weight_source_contract(summary.get("weight_source_contract") or {}), False),
        ("pc", "Portfolio Construction Objective", _render_portfolio_construction_objective(summary.get("portfolio_construction_objective") or {}), False),
        ("evidence", "ETF / Strategy Evidence", _render_strategy_evidence(summary.get("strategy_evidence") or {}), False),
        ("risk", "Portfolio Risk Diagnostic", _render_portfolio_risk_diagnostic(summary.get("portfolio_risk_diagnostic") or {}), False),
        ("alpha-review", "Alpha Decision Review Surface", _render_alpha_decision_review_surface(summary.get("alpha_decision_review_surface") or {}), False),
        ("alpha-readiness", "Alpha Attribution Readiness", _render_alpha_readiness_report(summary.get("alpha_readiness_report") or {}), False),
        ("alpha-policy", "Alpha Decision Policy", _render_alpha_decision_policy(summary.get("alpha_decision_policy") or {}), False),
        ("alpha-profiles", "Alpha Decision Profiles", _render_alpha_decision_profiles(summary.get("alpha_decision_profiles") or {}), False),
        ("conviction", "Live Signal Conviction", _render_live_signal_conviction(summary.get("live_signal_conviction") or {}), False),
        ("attribution", "Performance Attribution", _render_performance_attribution(summary.get("performance_attribution") or {}), False),
        ("alpha-trend", "Alpha Validation Trend", _render_alpha_validation_trend(summary.get("alpha_validation_trend") or {}), False),
        ("regime-gap", "Strategy Family / Regime Gap Analysis", _render_strategy_regime_gap_analysis(summary.get("strategy_regime_gap_analysis") or {}), False),
        ("promotion", "Promotion / Degradation Recommendations", _render_strategy_promotion_recommendations(summary.get("strategy_promotion_recommendations") or {}), False),
        ("readiness", "Portfolio Construction Readiness", _render_kv(pc_readiness), False),
        ("replay", "Replay Diagnostics", _render_replay(replay), False),
        ("data-quality", "Data Quality Audit Trend", _render_data_quality_audit(summary.get("data_quality_audit") or {}), False),
        ("cron", "Cron Runs", _render_crons(summary.get("cron_runs") or []), False),
        ("execution-raw", "Execution", _render_kv(summary.get("execution") or {}), False),
    ]
    section_html = "\n".join(_render_dashboard_section(section_id, title, content, open_by_default=opened) for section_id, title, content, opened in sections)
    html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>QC Operator Dashboard</title>
  <style>{_css()}</style>
</head>
<body>
  <header>
    <div>
      <h1>QC Operator Dashboard</h1>
      <p>Generated {escape(summary.get("generated_at", ""))} UTC</p>
    </div>
    <span class="status {escape(str(ops.get("overall", "unknown")))}">{escape(str(ops.get("overall", "unknown")))}</span>
  </header>
  {_render_top_status_bar(summary)}
  <nav class="quick-nav" aria-label="Dashboard sections">
    <a href="#overview">Overview</a>
    <a href="#account-holdings">Account</a>
    <a href="#system-window">System</a>
    <a href="#actions-window">Actions</a>
    <a href="#data-window">Data</a>
    <a href="#alpha-window">Alpha</a>
    <a href="#execution">Execution</a>
    <a href="#weekend-review">Weekend</a>
    <a href="#latest">Decision</a>
    <a href="#validation-overview">Validation</a>
    <a href="#pc">Portfolio Construction</a>
    <a href="#evidence">Evidence</a>
    <a href="#alpha-review">Alpha</a>
    <a href="#data-quality">Data</a>
  </nav>
  <main class="dashboard-shell">
    <section id="overview" class="overview-panel">
      <div class="section-heading">
        <h2>Operator Overview</h2>
        <p>Focused on blockers, execution state, data freshness, and alpha evidence quality.</p>
      </div>
      {_render_metric_cards(summary)}
      {_render_operator_cockpit(summary)}
      {_render_visual_monitoring(summary)}
      {_render_operator_windows(summary)}
    </section>

    {_render_account_holdings_panel(summary.get("account_holdings") or {})}

    <section id="operational-health" class="panel">
      <div class="section-heading">
        <h2>Operational Health</h2>
        <p>Freshness checks and degraded research inputs.</p>
      </div>
      <div class="grid checks">{_render_checks(ops.get("checks") or {})}</div>
      {_render_list("Execution blockers", ops.get("execution_blockers") or [])}
      {_render_list("Research degradations", ops.get("research_degradations") or [])}
    </section>

    <section id="evidence-cap" class="panel">
      <div class="section-heading">
        <h2>Evidence Cap Calibration</h2>
        <p>Readiness and calibration state before evidence caps become allocation-affecting.</p>
      </div>
      {_render_evidence_cap_calibration(summary.get("evidence_cap_calibration") or {})}
    </section>

    {section_html}
  </main>
  <script>{_account_holdings_js()}</script>
</body>
</html>"""
    return html


def _render_account_holdings_panel(data: dict[str, Any]) -> str:
    if not data.get("available"):
        return """
        <section id="account-holdings" class="panel account-holdings-panel">
          <div class="section-heading">
            <h2>Account And Holdings</h2>
            <p>No account holdings data is available yet.</p>
          </div>
        </section>
        """
    account = data.get("account") or {}
    holdings = data.get("holdings") or []
    signals = data.get("signals") or {}
    contract = data.get("contract") or {}
    truth = data.get("truth") or {}
    return f"""
    <section id="account-holdings" class="panel account-holdings-panel">
      <div class="section-heading">
        <h2>Account And Holdings</h2>
        <p>QC account truth, expected-target reconciliation, daily contribution, drift, and per-position return monitoring.</p>
      </div>
      <div class="account-top-strip">
        {_render_account_stat("NAV", _format_money_like(account.get("nav")), "neutral")}
        {_render_account_stat("Cash", _fmt_percent(account.get("cash_pct")), "neutral")}
        {_render_account_stat("Buying Power", _format_money_like(account.get("buying_power")), "neutral")}
        {_render_account_stat("Day PnL", _fmt_percent(account.get("daily_pnl_pct"), sign=True), _value_tone(account.get("daily_pnl_pct")))}
        {_render_account_stat("Week PnL", _fmt_percent(account.get("week_pnl_pct"), sign=True), _value_tone(account.get("week_pnl_pct")))}
        {_render_account_stat("Drawdown", _fmt_percent(account.get("drawdown_pct"), sign=True), "warn" if abs(float(account.get("drawdown_pct") or 0.0)) > 5 else "neutral")}
        {_render_account_stat("Open Orders", account.get("open_orders"), "ok" if int(account.get("open_orders") or 0) == 0 else "warn")}
        {_render_account_stat("Snapshot Age", _fmt_minutes(account.get("snapshot_age_min")), "ok" if (account.get("snapshot_age_min") or 999) < 5 else "warn")}
      </div>
      {_render_account_truth_strip(truth)}
      <div class="account-chart-grid">
        <article class="account-card account-nav-card">
          <div class="account-card-title"><h3>NAV</h3><span>{escape(str(account.get("source_packet_type") or ""))}</span></div>
          {_render_account_nav_chart(data.get("nav_series") or [])}
        </article>
        <article class="account-card">
          <div class="account-card-title"><h3>Daily PnL</h3><span>{_fmt_percent(account.get("daily_pnl_pct"), sign=True)}</span></div>
          {_render_account_pnl_bars(data.get("pnl_series") or [])}
        </article>
        <article class="account-card">
          <div class="account-card-title"><h3>Contribution Today</h3><span>w x ret</span></div>
          {_render_contribution_bars(holdings)}
        </article>
      </div>
      {_render_account_key_signal_cards(signals)}
      <article class="account-card holdings-table-card">
        <div class="account-card-title">
          <h3>Holdings</h3>
          <span>{int(account.get("holding_count") or 0)} positions | total contribution {_fmt_percent(account.get("total_contribution_pct"), sign=True, digits=3)}</span>
        </div>
        {_render_holdings_sort_controls()}
        {_render_account_holdings_table(holdings)}
        <p class="account-contract-note">
          Contribution = weight_current * daily_return_pct * 100. Truth source: {escape(str(contract.get("truth_source") or ""))}; target source: {escape(str(contract.get("target_source") or ""))}; return source: {escape(str(contract.get("return_source") or ""))}.
          QC Days source: {escape(str(contract.get("holding_days_source") or ""))}.
        </p>
      </article>
    </section>
    """


def _render_account_truth_strip(truth: dict[str, Any]) -> str:
    if not truth.get("available"):
        return """
        <div class="account-truth-strip">
          <div class="account-truth-question"><span>QC actual holdings</span><strong>n/a</strong><em>No account truth snapshot yet.</em></div>
          <div class="account-truth-question"><span>FastAPI expected target reconciled?</span><strong>n/a</strong><em>No reconciliation truth available yet.</em></div>
        </div>
        """
    reconciliation = str(truth.get("reconciliation_status") or "unknown")
    execution_state = str(truth.get("execution_state") or "unknown")
    return f"""
      <div class="account-truth-strip" data-account-truth-view>
        <div class="account-truth-question">
          <span>QC actual holdings</span>
          <strong>{int(truth.get("holding_count") or 0)} positions | {escape(str(truth.get("source_packet_type") or "unknown"))}</strong>
          <em>Snapshot age {_fmt_minutes(truth.get("snapshot_age_min"))}; last command {escape(str(truth.get("last_command_id") or "none"))}</em>
        </div>
        <div class="account-truth-question">
          <span>FastAPI expected target reconciled?</span>
          <strong class="{escape(_truth_status_tone(reconciliation))}">{escape(reconciliation.replace("_", " "))}</strong>
          <em>Max target vs actual drift {_fmt_percent(truth.get("max_abs_drift_pct"), digits=2)}; cash treated as residual.</em>
        </div>
        <div class="account-truth-question">
          <span>Execution state</span>
          <strong class="{escape(_truth_status_tone(execution_state))}">{escape(execution_state.replace("_", " "))}</strong>
          <em>Active command {escape(str(truth.get("active_command_id") or "none"))}; ACK is not reconciliation.</em>
        </div>
      </div>
    """


def _truth_status_tone(status: str) -> str:
    normalized = str(status or "").lower().strip()
    if normalized in {"reconciled", "idle", "filled", "noop_reconciled"}:
        return "positive"
    if normalized in {"drift_present", "pending_reconciliation", "active_execution", "orders_submitted", "partial"}:
        return "warning"
    if normalized in {"diverged", "rejected", "failed", "failed_no_fill", "timeout_no_ack"}:
        return "negative"
    return "neutral"


def _render_account_stat(label: str, value: Any, tone: str) -> str:
    return f"""
      <div class="account-stat {escape(str(tone or 'neutral'))}">
        <span>{escape(str(label))}</span>
        <strong>{escape(_format_value(value))}</strong>
      </div>
    """


def _render_account_nav_chart(rows: list[dict[str, Any]]) -> str:
    clean = [row for row in rows if _json_safe_number(row.get("total_value")) is not None]
    if len(clean) < 2:
        return "<p class=\"muted\">No NAV series yet.</p>"
    width, height = 640, 96
    values = [float(row["total_value"]) for row in clean]
    low = min(values)
    high = max(values)
    pad = max((high - low) * 0.12, 1.0)
    low -= pad
    high += pad
    span = max(high - low, 1.0)

    def x_pos(idx: int) -> float:
        return 14 + idx / max(len(clean) - 1, 1) * (width - 28)

    def y_pos(value: float) -> float:
        return height - 10 - (value - low) / span * (height - 20)

    points = " ".join(f"{x_pos(idx):.2f},{y_pos(value):.2f}" for idx, value in enumerate(values))
    area = f"{points} {x_pos(len(values)-1):.2f},{height:.2f} 14,{height:.2f}"
    stroke = "#22d3a0" if values[-1] >= values[0] else "#f04a5a"
    labels = "".join(
        f"<text x=\"{x_pos(idx):.2f}\" y=\"{height + 14}\" text-anchor=\"middle\">{escape(str(row.get('date_label') or '')[-2:])}</text>"
        for idx, row in enumerate(clean)
    )
    hit_points = "".join(
        (
            f"<circle class=\"chart-hit-point\" cx=\"{x_pos(idx):.2f}\" cy=\"{y_pos(values[idx]):.2f}\" r=\"7\">"
            f"<title>{escape(str(row.get('date_label') or ''))} NAV {_format_money_like(values[idx])}</title>"
            "</circle>"
        )
        for idx, row in enumerate(clean)
    )
    return f"""
      <svg class="account-nav-chart" viewBox="0 0 {width} {height + 18}" role="img" aria-label="NAV line chart">
        <polygon points="{area}" fill="{stroke}20"></polygon>
        <polyline points="{points}" fill="none" stroke="{stroke}" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"></polyline>
        <circle cx="{x_pos(len(values)-1):.2f}" cy="{y_pos(values[-1]):.2f}" r="3.5" fill="{stroke}"></circle>
        {hit_points}
        <g class="account-axis-labels">{labels}</g>
      </svg>
    """


def _render_account_pnl_bars(rows: list[dict[str, Any]]) -> str:
    clean = [row for row in rows if _json_safe_number(row.get("daily_pnl_pct")) is not None]
    if not clean:
        return "<p class=\"muted\">No daily PnL series yet.</p>"
    width, height = 420, 76
    max_abs = max([abs(float(row.get("daily_pnl_pct") or 0.0) * 100.0) for row in clean] + [0.1])
    bar_gap = 4
    bar_width = max((width - 24) / max(len(clean), 1) - bar_gap, 3)
    mid = height / 2
    bars = []
    for idx, row in enumerate(clean):
        value = float(row.get("daily_pnl_pct") or 0.0) * 100.0
        bar_height = abs(value) / max_abs * (mid - 5)
        x = 12 + idx * (bar_width + bar_gap)
        y = mid - bar_height if value >= 0 else mid
        color = "#22d3a0" if value >= 0 else "#f04a5a"
        bars.append(
            (
                f"<rect x=\"{x:.2f}\" y=\"{y:.2f}\" width=\"{bar_width:.2f}\" height=\"{max(bar_height, 1):.2f}\" rx=\"2\" fill=\"{color}cc\">"
                f"<title>{escape(str(row.get('date_label') or ''))} Daily PnL {_fmt_percent(value, sign=True, digits=3)}</title>"
                "</rect>"
            )
        )
        bars.append(
            f"<text x=\"{x + bar_width / 2:.2f}\" y=\"{height + 14}\" text-anchor=\"middle\">{escape(str(row.get('date_label') or '')[-2:])}</text>"
        )
    return f"""
      <svg class="account-pnl-chart" viewBox="0 0 {width} {height + 18}" role="img" aria-label="Daily PnL bar chart">
        <line x1="12" y1="{mid:.2f}" x2="{width - 12}" y2="{mid:.2f}" class="account-zero-line"></line>
        {''.join(bars)}
      </svg>
    """


def _render_contribution_bars(holdings: list[dict[str, Any]]) -> str:
    rows = [row for row in holdings if row.get("ticker") != "CASH"]
    if not rows:
        return "<p class=\"muted\">No holdings contribution rows yet.</p>"
    max_abs = max([abs(float(row.get("contribution_pct") or 0.0)) for row in rows] + [0.001])
    rendered = []
    for row in rows:
        contribution = float(row.get("contribution_pct") or 0.0)
        width = min(abs(contribution) / max_abs * 42.0, 42.0)
        is_positive = contribution >= 0
        left = 50.0 if is_positive else 50.0 - width
        tooltip = (
            f"{row.get('ticker')}: contribution {_fmt_percent(contribution, sign=True, digits=3)}; "
            f"weight {_fmt_percent(row.get('weight_current'))}; "
            f"day return {_fmt_percent(row.get('daily_return_pct'), sign=True)}"
        )
        rendered.append(f"""
          <div class="contrib-row">
            <strong>{escape(str(row.get("ticker") or ""))}</strong>
            <div class="contrib-track">
              <span class="contrib-midline"></span>
              <span class="contrib-bar {'positive' if is_positive else 'negative'}" style="left:{left:.3f}%;width:{width:.3f}%" title="{escape(tooltip)}"></span>
            </div>
            <em class="{'positive' if is_positive else 'negative'}">{escape(_fmt_percent(contribution, sign=True, digits=3))}</em>
          </div>
        """)
    return f"<div class=\"contrib-bars\">{''.join(rendered)}</div>"


def _render_account_key_signal_cards(signals: dict[str, Any]) -> str:
    cards = [
        ("Top Contributor", signals.get("top_contributor"), "contribution_pct", "ok"),
        ("Top Dragger", signals.get("top_dragger"), "contribution_pct", "bad"),
        ("Largest Drift", signals.get("largest_drift"), "weight_drift", "warn"),
        ("Longest QC Hold", signals.get("longest_hold"), "holding_days", "info"),
    ]
    out = []
    for label, row, key, tone in cards:
        ticker = (row or {}).get("ticker") if isinstance(row, dict) else None
        raw = (row or {}).get(key) if isinstance(row, dict) else None
        if key == "holding_days":
            detail = f"{int(raw or 0)} QC days" if ticker else "n/a"
        else:
            detail = _fmt_percent(raw, sign=True, digits=3 if key == "contribution_pct" else 1) if ticker else "n/a"
        out.append(f"""
          <article class="account-signal-card {tone}">
            <span>{escape(label)}</span>
            <strong>{escape(str(ticker or 'None'))}</strong>
            <em>{escape(detail)}</em>
          </article>
        """)
    return f"<div class=\"account-signal-grid\">{''.join(out)}</div>"


def _render_holdings_sort_controls() -> str:
    controls = [
        ("contribution", "Contribution"),
        ("weight", "Weight"),
        ("quantity", "Quantity"),
        ("drift", "Drift"),
        ("return", "Day Return"),
        ("unrealized", "Unrealized PnL"),
        ("days", "QC Days"),
    ]
    buttons = "".join(
        f"<button type=\"button\" data-sort-key=\"{escape(key)}\">{escape(label)}</button>"
        for key, label in controls
    )
    return f"<div class=\"holdings-sort-controls\"><span>Sort by</span>{buttons}</div>"


def _render_account_holdings_table(holdings: list[dict[str, Any]]) -> str:
    if not holdings:
        return "<p class=\"muted\">No holdings rows.</p>"
    rows = []
    for row in holdings:
        contribution = float(row.get("contribution_pct") or 0.0)
        daily_return = float(row.get("daily_return_pct") or 0.0)
        unrealized = float(row.get("unrealized_pnl_pct") or 0.0)
        drift = float(row.get("weight_drift") or 0.0)
        rows.append(f"""
          <tr data-holding-row
              data-sort-contribution="{contribution:.8f}"
              data-sort-weight="{float(row.get('weight_current') or 0.0):.8f}"
              data-sort-target="{float(row.get('weight_target') or 0.0):.8f}"
              data-sort-quantity="{float(row.get('quantity') or 0.0):.8f}"
              data-sort-average-price="{float(row.get('average_price') or 0.0):.8f}"
              data-sort-drift="{drift:.8f}"
              data-sort-return="{daily_return:.8f}"
              data-sort-unrealized="{unrealized:.8f}"
              data-sort-days="{int(row.get('holding_days') or 0)}">
            <td><span class="role-chip {escape(str(row.get('role') or 'unknown'))}"></span><strong>{escape(str(row.get("ticker") or ""))}</strong></td>
            <td>{escape(str(row.get("role") or ""))}</td>
            <td class="num" title="QC reported quantity">{escape(_fmt_plain_number(row.get("quantity"), digits=3))}</td>
            <td class="num" title="QC reported average price">{escape(_format_money_like(row.get("average_price")) or "n/a")}</td>
            <td class="num">{escape(_fmt_percent(row.get("weight_current")))}</td>
            <td class="num muted-cell" title="target_source={escape(str(row.get('target_source') or 'unavailable'))}">{escape(_fmt_percent(row.get("weight_target")))}</td>
            <td class="num {escape(_value_tone_abs(drift, 0.5))}" title="target_source={escape(str(row.get('target_source') or 'unavailable'))}">{escape(_fmt_percent(drift, sign=True))}</td>
            <td class="num {escape(_value_tone(daily_return))}">{escape(_fmt_percent(daily_return, sign=True))}</td>
            <td class="num {escape(_value_tone(contribution))}">{escape(_fmt_percent(contribution, sign=True, digits=3))}</td>
            <td class="num {escape(_value_tone(unrealized))}">{escape(_fmt_percent(unrealized, sign=True))}</td>
            <td class="num" title="QC in-memory holding_days counter; can reset on QC redeploy">{int(row.get("holding_days") or 0) or ""}</td>
            <td><span class="action-label {escape(str(row.get('action') or 'normal_hold'))}">{escape(str(row.get("action") or "normal_hold").replace("_", " "))}</span></td>
          </tr>
        """)
    return f"""
      <div class="account-table-wrap">
        <table id="account-holdings-table" class="account-holdings-table">
          <thead>
            <tr>
              <th><button type="button" data-sort-key="ticker">Ticker</button></th>
              <th><button type="button" data-sort-key="role">Role</button></th>
              <th class="num"><button type="button" data-sort-key="quantity">Quantity</button></th>
              <th class="num"><button type="button" data-sort-key="average-price">Avg Price</button></th>
              <th class="num"><button type="button" data-sort-key="weight">Weight</button></th>
              <th class="num"><button type="button" data-sort-key="target">Target</button></th>
              <th class="num"><button type="button" data-sort-key="drift">Drift</button></th>
              <th class="num"><button type="button" data-sort-key="return">Day Return</button></th>
              <th class="num"><button type="button" data-sort-key="contribution">Contribution</button></th>
              <th class="num"><button type="button" data-sort-key="unrealized">Unrealized PnL</button></th>
              <th class="num"><button type="button" data-sort-key="days" title="QC in-memory holding_days counter; can reset on QC redeploy">QC Days</button></th>
              <th>Action</th>
            </tr>
          </thead>
          <tbody>{''.join(rows)}</tbody>
        </table>
      </div>
    """


def _fmt_plain_number(value: Any, *, digits: int = 2) -> str:
    number = _json_safe_number(value)
    if number is None:
        return "n/a"
    if abs(number - round(number)) < 1e-9:
        return f"{number:,.0f}"
    return f"{number:,.{digits}f}"


def _fmt_percent(value: Any, *, sign: bool = False, digits: int = 1) -> str:
    number = _json_safe_number(value)
    if number is None:
        return "n/a"
    prefix = "+" if sign and number > 0 else ""
    return f"{prefix}{number:.{digits}f}%"


def _fmt_minutes(value: Any) -> str:
    number = _json_safe_number(value)
    if number is None:
        return "n/a"
    return f"{number:.1f}m"


def _value_tone(value: Any) -> str:
    number = _json_safe_number(value) or 0.0
    if number > 0:
        return "positive"
    if number < 0:
        return "negative"
    return "neutral"


def _value_tone_abs(value: Any, threshold: float) -> str:
    number = abs(_json_safe_number(value) or 0.0)
    if number >= threshold:
        return "warning"
    return "neutral"


def _account_holdings_js() -> str:
    return """
    (function () {
      const table = document.getElementById("account-holdings-table");
      if (!table) return;
      const state = { key: "", dir: "desc" };
      function valueFor(row, key) {
        if (key === "ticker") return row.cells[0].innerText.trim();
        if (key === "role") return row.cells[1].innerText.trim();
        const raw = row.getAttribute("data-sort-" + key);
        const number = Number(raw);
        return Number.isFinite(number) ? number : raw;
      }
      function sortRows(key) {
        state.dir = state.key === key && state.dir === "desc" ? "asc" : "desc";
        state.key = key;
        const tbody = table.tBodies[0];
        const rows = Array.from(tbody.querySelectorAll("[data-holding-row]"));
        rows.sort(function (a, b) {
          const av = valueFor(a, key);
          const bv = valueFor(b, key);
          if (typeof av === "string" || typeof bv === "string") {
            return state.dir === "desc"
              ? String(bv).localeCompare(String(av))
              : String(av).localeCompare(String(bv));
          }
          return state.dir === "desc" ? bv - av : av - bv;
        });
        rows.forEach(function (row) { tbody.appendChild(row); });
        document.querySelectorAll("[data-sort-key]").forEach(function (button) {
          button.classList.toggle("active", button.getAttribute("data-sort-key") === key);
        });
      }
      document.querySelectorAll("[data-sort-key]").forEach(function (button) {
        button.addEventListener("click", function () {
          sortRows(button.getAttribute("data-sort-key"));
        });
      });
      sortRows("contribution");
    })();
    """


def _render_dashboard_section(section_id: str, title: str, content: str, *, open_by_default: bool = False) -> str:
    open_attr = " open" if open_by_default else ""
    return f"""
      <details id="{escape(section_id)}" class="panel detail-panel"{open_attr}>
        <summary><h2>{escape(title)}</h2><span>Open details</span></summary>
        <div class="detail-body">{content}</div>
      </details>
    """


def _render_top_status_bar(summary: dict[str, Any]) -> str:
    config = summary.get("config") or {}
    circuit = config.get("circuit_state") or {}
    auth_mode = _config_scalar(config.get("authorization_mode")) or "unknown"
    control = summary.get("execution_control") or {}
    snapshot = control.get("latest_account_snapshot") or {}
    guard = control.get("account_state_guard") or {}
    ops = summary.get("ops") or {}
    circuit_state = str(circuit.get("state") or circuit.get("value") or "unknown")
    can_trade = _dashboard_can_trade(summary)
    return f"""
      <div class="top-status-bar">
        <div class="brand-mark">A</div>
        <div class="top-status-main">
          <div class="top-status-title">Trading Operations Console</div>
          <div class="top-status-sub">Circuit, pipeline authority, account truth, and execution readiness.</div>
        </div>
        <div class="top-status-pills">
          {_render_status_pill("Circuit", circuit_state, circuit_state)}
          {_render_status_pill("Pipeline", auth_mode, "ok" if str(auth_mode).upper() == "FULL_AUTO" else "warn")}
          {_render_status_pill("Trade", "TRADEABLE" if can_trade else "BLOCKED", "ok" if can_trade else "error")}
          {_render_status_pill("Guard", guard.get("status") or "unknown", guard.get("status") or "unknown")}
          {_render_status_pill("Policy", snapshot.get("policy_version") or "unknown", "ok" if snapshot.get("policy_version") else "warn")}
          {_render_status_pill("Ops", ops.get("overall") or "unknown", ops.get("overall") or "unknown")}
        </div>
      </div>
    """


def _render_status_pill(label: str, value: Any, status: Any) -> str:
    return f"""
      <div class="top-pill {escape(str(status or 'unknown').lower())}">
        <span>{escape(label)}</span>
        <strong>{escape(_format_value(value))}</strong>
      </div>
    """


def _render_metric_cards(summary: dict[str, Any]) -> str:
    ops = summary.get("ops") or {}
    latest = summary.get("latest_analysis") or {}
    control = summary.get("execution_control") or {}
    snapshot = control.get("latest_account_snapshot") or {}
    active = control.get("active_execution") or {}
    guard = control.get("account_state_guard") or {}
    alpha_policy = summary.get("alpha_decision_policy") or {}
    alpha_profiles = summary.get("alpha_decision_profiles") or {}
    attribution = summary.get("performance_attribution") or {}
    config = summary.get("config") or {}
    command_cfg = config.get("execution_command_config") or {}
    command_rows = control.get("recent_commands") or []
    today_commands = _count_today_commands(command_rows)
    max_daily_commands = _json_safe_number(command_cfg.get("max_daily_commands")) or 0
    turnover = _latest_target_turnover(latest)
    command_status = "active" if active.get("active") else "idle"
    market_state = "open" if snapshot.get("is_market_open") else "closed"
    return f"""
      <div class="metric-grid">
        {_render_metric_card("System", ops.get("overall"), "Execution blocker" if ops.get("execution_blockers") else "No blocker", ops.get("overall"))}
        {_render_metric_card("Pipeline", latest.get("execution_status") or "unknown", f"risk={latest.get('risk_approved')}", latest.get("execution_status") or "unknown")}
        {_render_metric_card("Account", snapshot.get("account_status") or guard.get("status"), f"policy={snapshot.get('policy_version') or guard.get('policy_version')}", snapshot.get("account_status") or guard.get("status"))}
        {_render_metric_card("Open Orders", snapshot.get("open_order_count") or 0, f"market={market_state}", "ok" if int(snapshot.get("open_order_count") or 0) == 0 else "warn")}
        {_render_metric_card("Command", command_status, active.get("active_command_id") or "no active command", command_status)}
        {_render_metric_card("Commands Today", f"{today_commands}/{int(max_daily_commands or 0)}", f"turnover={_format_pct_value(turnover)}", "ok" if not max_daily_commands or today_commands < max_daily_commands else "warn")}
        {_render_metric_card("Alpha Mode", alpha_policy.get("effective_mode") or alpha_policy.get("mode") or "unknown", f"eligible={alpha_profiles.get('eligible_count')}", alpha_policy.get("effective_mode") or alpha_policy.get("mode") or "unknown")}
        {_render_metric_card("Residual Alpha", _format_pct_value((attribution.get("latest") or {}).get("residual_alpha_candidate")), f"R2={(attribution.get('latest') or {}).get('r_squared')}", "ok" if (_json_safe_number((attribution.get("latest") or {}).get("residual_alpha_candidate")) or 0) >= 0 else "warn")}
      </div>
    """


def _render_command_center(summary: dict[str, Any]) -> str:
    return _render_metric_cards(summary)


def _render_operator_cockpit(summary: dict[str, Any]) -> str:
    control = summary.get("execution_control") or {}
    snapshot = control.get("latest_account_snapshot") or {}
    command_cfg = (summary.get("config") or {}).get("execution_command_config") or {}
    latest = summary.get("latest_analysis") or {}
    active = control.get("active_execution") or {}
    alpha = summary.get("alpha_decision_profiles") or {}
    conviction = summary.get("live_signal_conviction") or {}
    attribution = summary.get("performance_attribution") or {}
    command_rows = control.get("recent_commands") or []
    today_commands = _count_today_commands(command_rows)
    max_daily_commands = int(_json_safe_number(command_cfg.get("max_daily_commands")) or 0)
    turnover = _latest_target_turnover(latest)
    max_turnover = _json_safe_number(command_cfg.get("max_gross_turnover_per_day")) or 0
    cash_pct = _json_safe_number(snapshot.get("cash_pct"))
    return f"""
      <div class="cockpit-grid">
        <article class="cockpit-panel priority-panel">
          <div class="window-title"><h3>Priority Queue</h3><span>Why attention is needed</span></div>
          {_render_priority_queue(summary)}
        </article>
        <article class="cockpit-panel account-execution-panel">
          <div class="window-title"><h3>Account + Execution</h3><span>Can the system move?</span></div>
          <div class="gauge-row">
            {_render_arc_gauge("Cash", cash_pct, sub=_format_money_like(snapshot.get("cash")))}
            {_render_arc_gauge("Turnover", _pct_ratio(turnover, max_turnover), sub=f"{_format_pct_value(turnover)} / {_format_pct_value(max_turnover)}")}
            {_render_arc_gauge("Commands", _pct_ratio(today_commands, max_daily_commands), sub=f"{today_commands}/{max_daily_commands or 0}")}
          </div>
          <div class="split-grid">
            <div>{_render_kv({
                "last_command_id": snapshot.get("last_command_id"),
                "active_command_id": snapshot.get("active_command_id"),
                "active_execution": snapshot.get("active_execution_status"),
                "processed_commands": snapshot.get("processed_command_count"),
                "open_orders": snapshot.get("open_order_count"),
            })}</div>
            <div>{_render_bar_chart("Target / Actual Drift", active.get("drift_rows") or [], label_key="ticker", value_key="diff")}</div>
          </div>
        </article>
        <article class="cockpit-panel alpha-panel">
          <div class="window-title"><h3>Alpha Quality</h3><span>Evidence, attribution, maturity</span></div>
          <div class="alpha-summary-grid">
            {_render_metric_card("Eligible Profiles", alpha.get("eligible_count"), f"total={alpha.get('profile_count')}", "info")}
            {_render_metric_card("Independent Strategies", alpha.get("independence_adjusted_strategy_count"), f"raw={alpha.get('raw_alpha_strategy_count')}", "info")}
            {_render_metric_card("Residual", _format_pct_value((attribution.get("latest") or {}).get("residual_alpha_candidate")), "beta/factor adjusted", "ok" if (_json_safe_number((attribution.get("latest") or {}).get("residual_alpha_candidate")) or 0) >= 0 else "warn")}
          </div>
          {_render_attribution_stack_chart(attribution.get("recent_rows") or [])}
          {_render_bar_chart("Conviction Status", conviction.get("status_count_rows") or [], label_key="status", value_key="count")}
        </article>
      </div>
    """


def _render_operator_windows(summary: dict[str, Any]) -> str:
    control = summary.get("execution_control") or {}
    snapshot = control.get("latest_account_snapshot") or {}
    active = control.get("active_execution") or {}
    guard = control.get("account_state_guard") or {}
    auto_pause = control.get("auto_pause") or {}
    ops = summary.get("ops") or {}
    checks = ops.get("checks") or {}
    data_audit = summary.get("data_quality_audit") or {}
    alpha_policy = summary.get("alpha_decision_policy") or {}
    alpha_profiles = summary.get("alpha_decision_profiles") or {}
    attribution = summary.get("performance_attribution") or {}
    conviction = summary.get("live_signal_conviction") or {}
    latest = summary.get("latest_analysis") or {}
    action_rows = _priority_rows(summary)
    command_rows = control.get("recent_commands") or []
    freshness_rows = [
        {"check": row.get("label") or key, "state": row.get("state"), "age_hours": row.get("age_hours"), "reason": row.get("reason")}
        for key, row in checks.items()
        if isinstance(row, dict)
    ]
    return f"""
      <div class="window-grid">
        <article id="account-window" class="operator-window">
          <div class="window-title"><h3>Account Window</h3><span>Truth source</span></div>
          {_render_kv({
              "account_status": snapshot.get("account_status"),
              "data_status": snapshot.get("data_status"),
              "policy_version": snapshot.get("policy_version"),
              "cash_pct": snapshot.get("cash_pct"),
              "open_orders": snapshot.get("open_order_count"),
              "last_command_id": snapshot.get("last_command_id"),
              "processed_commands": snapshot.get("processed_command_count"),
          })}
          <div class="mini-chart">{_render_bar_chart("Target / Actual Drift", active.get("drift_rows") or [], label_key="ticker", value_key="diff")}</div>
        </article>
        <article id="system-window" class="operator-window">
          <div class="window-title"><h3>System Window</h3><span>Health and guards</span></div>
          {_render_kv({
              "overall": ops.get("overall"),
              "guard": guard.get("status"),
              "guard_effect": guard.get("execution_effect"),
              "auto_pause": auto_pause.get("status"),
              "would_pause": auto_pause.get("would_pause"),
              "should_pause": auto_pause.get("should_pause"),
          })}
          {_render_table(freshness_rows, ["check", "state", "age_hours", "reason"])}
        </article>
        <article id="actions-window" class="operator-window">
          <div class="window-title"><h3>Actions Window</h3><span>What needs attention</span></div>
          {_render_table(action_rows, ["level", "source", "message"])}
          <h4>Recent Commands</h4>
          {_render_table(command_rows, ["executed_at", "command_id", "command_type", "qc_status", "execution_state", "qc_rejection_reason"])}
        </article>
        <article id="data-window" class="operator-window">
          <div class="window-title"><h3>Data Window</h3><span>Research inputs</span></div>
          {_render_kv({
              "audit_status": (data_audit.get("latest") or {}).get("status"),
              "joined_rows": (data_audit.get("latest") or {}).get("joined_rows"),
              "unit_risks": (data_audit.get("latest") or {}).get("unit_risk_count"),
              "high_drift": (data_audit.get("latest") or {}).get("high_drift_classes"),
              "yfinance_health": (checks.get("yfinance_ticker_health") or {}).get("state"),
              "yfinance_ok": f"{(checks.get('yfinance_ticker_health') or {}).get('ok_count', 0)}/{(checks.get('yfinance_ticker_health') or {}).get('ticker_count', 0)}",
          })}
          <div class="mini-chart">{_render_line_chart("Unit Risk Trend", data_audit.get("recent") or [], x_key="created_at", y_key="unit_risk_count")}</div>
        </article>
        <article id="alpha-window" class="operator-window">
          <div class="window-title"><h3>Alpha Window</h3><span>Evidence quality</span></div>
          {_render_kv({
              "policy_mode": alpha_policy.get("effective_mode") or alpha_policy.get("mode"),
              "profile_count": alpha_profiles.get("profile_count"),
              "eligible_count": alpha_profiles.get("eligible_count"),
              "latest_residual": (attribution.get("latest") or {}).get("residual_alpha_candidate"),
              "latest_r2": (attribution.get("latest") or {}).get("r_squared"),
              "signals": (conviction.get("overview") or {}).get("frozen_signal_count"),
          })}
          <div class="mini-chart">{_render_bar_chart("Conviction Status", conviction.get("status_count_rows") or [], label_key="status", value_key="count")}</div>
        </article>
        <article class="operator-window">
          <div class="window-title"><h3>Decision Window</h3><span>Latest run</span></div>
          {_render_kv({
              "analysis_id": latest.get("id"),
              "trigger": latest.get("trigger_type"),
              "execution_status": latest.get("execution_status"),
              "risk_approved": latest.get("risk_approved"),
              "scorecard": (latest.get("scorecard") or {}).get("investment_permission"),
              "data_quality": (latest.get("scorecard") or {}).get("data_quality"),
          })}
        </article>
      </div>
    """


def _priority_rows(summary: dict[str, Any]) -> list[dict[str, str]]:
    ops = summary.get("ops") or {}
    control = summary.get("execution_control") or {}
    latest = summary.get("latest_analysis") or {}
    risks: list[dict[str, str]] = []
    for item in ops.get("execution_blockers") or []:
        risks.append({"level": "blocker", "source": "Ops", "message": str(item)})
    for item in ops.get("research_degradations") or []:
        risks.append({"level": "warning", "source": "Research", "message": str(item)})
    guard = control.get("account_state_guard") or {}
    if guard.get("would_block"):
        risks.append({"level": "blocker", "source": "Account Guard", "message": str(guard.get("primary_blockers") or guard.get("blockers") or guard.get("reason") or "would block")})
    auto_pause = control.get("auto_pause") or {}
    if auto_pause.get("would_pause") or auto_pause.get("should_pause"):
        risks.append({"level": "blocker" if auto_pause.get("should_pause") else "warning", "source": "Auto Pause", "message": str(auto_pause.get("reason") or auto_pause.get("primary_trigger") or "would pause")})
    active = control.get("active_execution") or {}
    if active.get("stale"):
        risks.append({"level": "warning", "source": "Active Execution", "message": str(active.get("stale_operator_action") or active.get("stale_reason") or "stale active execution")})
    for reason in latest.get("rejection_reasons") or []:
        risks.append({"level": "warning", "source": "Latest Decision", "message": _format_value(reason)})
    return risks


def _render_priority_queue(summary: dict[str, Any]) -> str:
    risks = _priority_rows(summary)
    if not risks:
        return """
          <section class="priority-strip ok-strip">
            <h3>Priority Queue</h3>
            <p>No execution blocker or urgent degradation is currently reported.</p>
          </section>
        """
    rows = "\n".join(
        f"""<li class="{escape(row['level'])}">
          <span>{escape(row['level'].upper())}</span>
          <strong>{escape(row['source'])}</strong>
          <p>{escape(row['message'])}</p>
        </li>"""
        for row in risks
    )
    return f"""
      <section class="priority-strip">
        <h3>Priority Queue</h3>
        <ul class="risk-list">{rows}</ul>
      </section>
    """


def _render_visual_monitoring(summary: dict[str, Any]) -> str:
    ops = summary.get("ops") or {}
    checks = ops.get("checks") or {}
    age_rows = []
    for key in ("qc_heartbeat", "daily_feature_snapshot", "yfinance_backfill", "news_cache", "memory_write"):
        row = checks.get(key) or {}
        age_rows.append({
            "label": row.get("label") or key,
            "value": row.get("age_hours"),
            "status": row.get("state") or "unknown",
        })
    command_rows = summary.get("execution_control", {}).get("recent_commands") or []
    command_counts = _count_values(command_rows, "qc_status")
    attribution_rows = (summary.get("performance_attribution") or {}).get("recent_rows") or []
    alpha_rows = (summary.get("alpha_validation_trend") or {}).get("recent_rows") or []
    conviction_rows = (summary.get("live_signal_conviction") or {}).get("status_count_rows") or []
    return f"""
      <div class="visual-grid">
        {_render_bar_chart("Freshness Age (hours)", age_rows, value_label="hours")}
        {_render_bar_chart("Recent Command Status", command_counts, value_label="commands")}
        {_render_line_chart("Residual Alpha Candidate", attribution_rows, x_key="period_key", y_key="residual_alpha_candidate")}
        {_render_line_chart("Portfolio VaR 95 Loss", alpha_rows, x_key="generated_at", y_key="var_95_loss")}
        {_render_bar_chart("Conviction Profile Status", conviction_rows, label_key="status", value_key="count", value_label="profiles")}
      </div>
    """


def _render_status_card(title: str, state: Any, timestamp: Any, detail: Any) -> str:
    status = str(state or "unknown")
    return f"""
      <article class="status-card {escape(status)}">
        <div class="label">{escape(title)}</div>
        <div class="metric">{escape(status)}</div>
        <div class="muted">{escape(_format_value(detail))}</div>
        <div class="timestamp">{escape(_format_value(timestamp))}</div>
      </article>
    """


def _render_metric_card(label: str, value: Any, sub: Any, status: Any) -> str:
    status_text = str(status or "unknown").lower()
    return f"""
      <article class="metric-card {escape(status_text)}">
        <div class="metric-label">{escape(label)}</div>
        <div class="metric-main">
          <span class="status-dot {escape(status_text)}"></span>
          <strong>{escape(_format_value(value))}</strong>
        </div>
        <div class="metric-sub">{escape(_format_value(sub))}</div>
      </article>
    """


def _render_arc_gauge(label: str, value: Any, *, sub: str = "") -> str:
    pct = max(min(float(_json_safe_number(value) or 0.0), 100.0), 0.0)
    radius = 31
    circumference = 3.141592653589793 * radius
    dash = circumference * pct / 100.0
    status = "ok" if pct < 60 else "warn" if pct < 90 else "error"
    return f"""
      <div class="arc-gauge {escape(status)}">
        <svg viewBox="0 0 74 44" role="img" aria-label="{escape(label)} usage gauge">
          <path d="M 6 38 A 31 31 0 0 1 68 38" class="arc-bg" />
          <path d="M 6 38 A 31 31 0 0 1 68 38" class="arc-fg" stroke-dasharray="{dash:.2f} {circumference:.2f}" />
          <text x="37" y="35" text-anchor="middle">{pct:.0f}%</text>
        </svg>
        <div class="arc-label">{escape(label)}</div>
        <div class="arc-sub">{escape(sub)}</div>
      </div>
    """


def _render_attribution_stack_chart(rows: list[dict[str, Any]]) -> str:
    clean = []
    for row in list(reversed(rows))[-8:]:
        beta = max(_json_safe_number(row.get("spy_beta_contribution")) or 0.0, 0.0)
        factor = max(_json_safe_number(row.get("momentum_factor_contribution")) or 0.0, 0.0)
        residual = max(_json_safe_number(row.get("residual_alpha_candidate")) or 0.0, 0.0)
        total = beta + factor + residual
        clean.append({
            "label": str(row.get("period_key") or row.get("period_end") or ""),
            "beta": beta,
            "factor": factor,
            "residual": residual,
            "total": total,
        })
    if not clean:
        return "<article class=\"chart-card compact-chart\"><h3>Attribution Stack</h3><p class=\"muted\">No attribution data.</p></article>"
    max_total = max((row["total"] for row in clean), default=0.0) or 1.0
    bars = []
    for row in clean:
        total_height = max(row["total"] / max_total * 64.0, 2.0)
        beta_h = row["beta"] / max(row["total"], 1e-9) * total_height if row["total"] else 0
        factor_h = row["factor"] / max(row["total"], 1e-9) * total_height if row["total"] else 0
        residual_h = row["residual"] / max(row["total"], 1e-9) * total_height if row["total"] else 0
        bars.append(
            f"""<div class="stack-bar">
              <div class="stack-segments" style="height:{total_height:.1f}px">
                <span class="seg residual" style="height:{residual_h:.1f}px"></span>
                <span class="seg factor" style="height:{factor_h:.1f}px"></span>
                <span class="seg beta" style="height:{beta_h:.1f}px"></span>
              </div>
              <em>{escape(row['label'])}</em>
            </div>"""
        )
    return f"""
      <article class="chart-card compact-chart">
        <h3>Attribution Stack</h3>
        <div class="stack-chart">{''.join(bars)}</div>
        <div class="legend"><span class="beta"></span>Beta <span class="factor"></span>Factor <span class="residual"></span>Residual</div>
      </article>
    """


def _render_bar_chart(
    title: str,
    rows: list[dict[str, Any]],
    *,
    label_key: str = "label",
    value_key: str = "value",
    value_label: str = "",
) -> str:
    clean_rows = []
    for row in rows:
        value = _json_safe_number(row.get(value_key))
        if value is None:
            continue
        clean_rows.append({"label": str(row.get(label_key) or ""), "value": value, "status": str(row.get("status") or "")})
    if not clean_rows:
        return f"<article class=\"chart-card\"><h3>{escape(title)}</h3><p class=\"muted\">No chart data.</p></article>"
    max_value = max(abs(float(row["value"])) for row in clean_rows) or 1.0
    bars = []
    for row in clean_rows:
        width = min(abs(float(row["value"])) / max_value * 100.0, 100.0)
        bars.append(
            f"""<div class="bar-row">
              <span>{escape(row['label'])}</span>
              <div class="bar-track"><div class="bar-fill {escape(row['status'])}" style="width:{width:.1f}%"></div></div>
              <strong>{escape(_format_chart_number(row['value']))}{escape((' ' + value_label) if value_label else '')}</strong>
            </div>"""
        )
    return f"""
      <article class="chart-card">
        <h3>{escape(title)}</h3>
        <div class="bar-chart">{''.join(bars)}</div>
      </article>
    """


def _render_line_chart(title: str, rows: list[dict[str, Any]], *, x_key: str, y_key: str) -> str:
    points_raw = []
    for row in reversed(rows):
        value = _json_safe_number(row.get(y_key))
        if value is None:
            continue
        points_raw.append((str(row.get(x_key) or ""), float(value)))
    if len(points_raw) < 2:
        return f"<article class=\"chart-card\"><h3>{escape(title)}</h3><p class=\"muted\">Need at least two data points.</p></article>"
    width = 640
    height = 190
    pad_x = 34
    pad_y = 24
    values = [value for _, value in points_raw]
    min_y = min(values)
    max_y = max(values)
    if abs(max_y - min_y) < 1e-12:
        max_y += 1.0
        min_y -= 1.0
    plot_w = width - pad_x * 2
    plot_h = height - pad_y * 2
    coords = []
    for index, (_, value) in enumerate(points_raw):
        x = pad_x + (plot_w * index / max(len(points_raw) - 1, 1))
        y = pad_y + plot_h - ((value - min_y) / (max_y - min_y) * plot_h)
        coords.append((x, y))
    polyline = " ".join(f"{x:.1f},{y:.1f}" for x, y in coords)
    zero_line = ""
    if min_y < 0 < max_y:
        zero_y = pad_y + plot_h - ((0 - min_y) / (max_y - min_y) * plot_h)
        zero_line = f"<line x1=\"{pad_x}\" y1=\"{zero_y:.1f}\" x2=\"{width - pad_x}\" y2=\"{zero_y:.1f}\" class=\"zero-line\" />"
    first_label = points_raw[0][0]
    last_label = points_raw[-1][0]
    return f"""
      <article class="chart-card">
        <h3>{escape(title)}</h3>
        <svg class="line-chart" viewBox="0 0 {width} {height}" role="img" aria-label="{escape(title)} line chart">
          <rect x="{pad_x}" y="{pad_y}" width="{plot_w}" height="{plot_h}" class="plot-bg" />
          {zero_line}
          <polyline points="{polyline}" class="line-path" />
          <circle cx="{coords[-1][0]:.1f}" cy="{coords[-1][1]:.1f}" r="4" class="line-dot" />
          <text x="{pad_x}" y="{height - 5}" class="axis-label">{escape(first_label)}</text>
          <text x="{width - pad_x}" y="{height - 5}" class="axis-label end">{escape(last_label)}</text>
          <text x="{pad_x}" y="16" class="axis-label">{escape(_format_chart_number(max_y))}</text>
          <text x="{pad_x}" y="{height - 28}" class="axis-label">{escape(_format_chart_number(min_y))}</text>
        </svg>
      </article>
    """


def _count_values(rows: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    counts: dict[str, int] = {}
    for row in rows:
        label = str(row.get(key) or "unknown")
        counts[label] = counts.get(label, 0) + 1
    return [{"label": label, "value": count, "status": label} for label, count in sorted(counts.items())]


def _dashboard_can_trade(summary: dict[str, Any]) -> bool:
    ops = summary.get("ops") or {}
    control = summary.get("execution_control") or {}
    config = summary.get("config") or {}
    circuit = config.get("circuit_state") or {}
    snapshot = control.get("latest_account_snapshot") or {}
    guard = control.get("account_state_guard") or {}
    auto_pause = control.get("auto_pause") or {}
    circuit_state = str(circuit.get("state") or circuit.get("value") or "").upper()
    return (
        not bool(ops.get("execution_blockers"))
        and circuit_state in {"", "CLOSED"}
        and str(snapshot.get("account_status") or "").lower() in {"", "ok"}
        and int(snapshot.get("open_order_count") or 0) == 0
        and not bool(guard.get("would_block"))
        and not bool(auto_pause.get("should_pause"))
    )


def _config_scalar(value: Any) -> Any:
    if isinstance(value, dict) and "value" in value:
        return value.get("value")
    return value


def _count_today_commands(rows: list[dict[str, Any]]) -> int:
    today_prefix = datetime.utcnow().date().isoformat()
    count = 0
    for row in rows:
        executed_at = str(row.get("executed_at") or row.get("qc_ack_at") or "")
        if executed_at.startswith(today_prefix):
            count += 1
    return count


def _latest_target_turnover(latest: dict[str, Any]) -> float | None:
    rows = (latest.get("transaction_cost_gate") or {}).get("rows") or []
    values = [_json_safe_number(row.get("abs_delta")) for row in rows]
    clean = [float(value) for value in values if value is not None]
    if clean:
        return round(sum(clean), 6)
    actions = latest.get("rebalance_actions") or []
    action_values = [_json_safe_number(row.get("weight_delta")) for row in actions if isinstance(row, dict)]
    action_clean = [abs(float(value)) for value in action_values if value is not None]
    if action_clean:
        return round(sum(action_clean), 6)
    return None


def _pct_ratio(value: Any, max_value: Any) -> float:
    numerator = _json_safe_number(value)
    denominator = _json_safe_number(max_value)
    if numerator is None or denominator is None or denominator <= 0:
        return 0.0
    return max(min(numerator / denominator * 100.0, 100.0), 0.0)


def _format_pct_value(value: Any) -> str:
    num = _json_safe_number(value)
    if num is None:
        return "n/a"
    return f"{num:.1%}"


def _format_money_like(value: Any) -> str:
    num = _json_safe_number(value)
    if num is None:
        return ""
    if abs(num) >= 1_000_000:
        return f"${num / 1_000_000:.1f}M"
    if abs(num) >= 1_000:
        return f"${num / 1_000:.1f}K"
    return f"${num:.0f}"


def _format_chart_number(value: Any) -> str:
    num = _json_safe_number(value)
    if num is None:
        return ""
    if abs(num) < 0.01 and num != 0:
        return f"{num:.4f}"
    if abs(num) < 10:
        return f"{num:.2f}"
    return f"{num:.0f}"


def _render_checks(checks: dict[str, Any]) -> str:
    cards = []
    for key in ("qc_heartbeat", "daily_feature_snapshot", "yfinance_backfill", "news_cache", "memory_write", "pipeline_status"):
        row = checks.get(key) or {}
        state = str(row.get("state") or row.get("status") or "unknown")
        age = row.get("age_hours")
        age_text = "n/a" if age is None else f"{float(age):.1f}h"
        cards.append(
            f"""<article class="card">
              <div class="label">{escape(str(row.get("label") or key))}</div>
              <div class="metric {escape(state)}">{escape(state)}</div>
              <div class="muted">age {escape(age_text)} · {escape(str(row.get("reason") or row.get("as_of") or ""))}</div>
            </article>"""
        )
    return "\n".join(cards)


def _render_weight_source_contract(contract: dict[str, Any]) -> str:
    if not contract.get("available"):
        return "<p class=\"muted\">No weight source contract available.</p>"
    overview = {
        "contract_version": contract.get("contract_version"),
        "executable_target_key": contract.get("executable_target_key"),
        "pc_candidate_key": contract.get("pc_candidate_key"),
        "pc_shadow_key": contract.get("pc_shadow_key"),
        "llm_adjusted_key": contract.get("llm_adjusted_key"),
        "baseline_reference_key": contract.get("baseline_reference_key"),
        "execution_authority": contract.get("execution_authority"),
    }
    return f"""
      <div class="weight-source-contract">
        <div class="grid">
          <article class="card"><h3>Contract Overview</h3>{_render_kv(overview)}</article>
          <article class="card"><h3>Forbidden Target Builder Inputs</h3>{_render_list("", contract.get("forbidden_target_builder_input_keys") or [])}</article>
        </div>
        <h3>Weight Source Labels</h3>{_render_table(contract.get("labels") or [], ["column", "label", "authority", "visual_class", "may_enter_target_builder", "display_note"])}
      </div>
    """


def _render_latest_analysis(latest: dict[str, Any]) -> str:
    if not latest.get("available"):
        return "<p class=\"muted\">No analysis available.</p>"
    scorecard = latest.get("scorecard") or {}
    governance = latest.get("position_governance") or {}
    feature_sources = latest.get("feature_source_summary") or {}
    pc_eval = latest.get("portfolio_construction_evaluation") or {}
    pc_gate = latest.get("portfolio_construction_promotion_gate") or {}
    hedge_outcome = latest.get("hedge_intent_outcome") or {}
    final_validation = latest.get("final_validation") or {}
    target_path = latest.get("target_path_visibility") or {}
    transaction_cost_gate = latest.get("transaction_cost_gate") or {}
    portfolio_risk = latest.get("portfolio_risk_diagnostic") or {}
    thesis = (governance.get("thesis_status_summary") or {}).get("problem_tickers") or []
    hints = governance.get("manual_action_hints") or []
    return f"""
      <div class="grid">
        <article class="card">{_render_kv(latest, keys=["id", "analyzed_at", "trigger_type", "risk_approved", "execution_status"])}</article>
        <article class="card"><h3>Scorecard</h3>{_render_kv(scorecard)}</article>
        <article class="card"><h3>Feature Source Summary</h3>{_render_kv(feature_sources)}</article>
      </div>
      <h3>Rejection Reasons</h3>{_render_list("", latest.get("rejection_reasons") or [])}
      <h3>Portfolio Construction Evaluation</h3>{_render_kv(pc_eval)}
      <h3>Portfolio Construction Promotion Gate</h3>{_render_kv(pc_gate)}
      <h3>Hedge Intent Outcome Log</h3>{_render_kv(hedge_outcome)}
      <h3>Final Risk Validation</h3>{_render_kv(final_validation)}
      <h3>Target Path</h3>{_render_target_path_visibility(target_path)}
      <h3>Transaction Cost Gate</h3>{_render_kv(transaction_cost_gate, keys=["mode", "broker", "status", "execution_effect", "summary", "warnings"])}
      <h3>Transaction Cost Rows</h3>{_render_table(transaction_cost_gate.get("rows") or [], ["ticker", "trade_action", "asset_cost_bucket", "abs_delta", "estimated_cost_rate", "cost_drag", "confidence", "conviction_status", "conviction_discount", "expected_edge", "edge_to_cost_ratio", "verdict", "reason"])}
      <h3>Portfolio Risk Diagnostic</h3>{_render_kv(portfolio_risk, keys=["status", "mode", "execution_authority", "data_quality", "summary", "warnings"])}
      <h3>Manual Review Hints</h3>{_render_table(hints, ["ticker", "suggested_action", "current_weight", "suggested_target", "delta"])}
      <h3>Thesis Problems</h3>{_render_table(thesis, ["ticker", "status", "validator"])}
      <h3>Position Explanations</h3>{_render_table(governance.get("position_explanations") or [], ["ticker", "position_state", "decision", "current_weight", "target_after", "unrealized_pnl_pct", "risk_budget_status", "strategy_support", "action_permission", "strategy_intent", "llm_effect", "construction_effect", "risk_governance_effect", "final_explanation", "why_hold", "why_not_add", "why_not_exit", "next_trigger"])}
      <h3>Weight Source Contract</h3>{_render_weight_source_contract(latest.get("weight_source_contract") or {})}
      <h3>Decision Ledger</h3>{_render_table((latest.get("decision_ledger") or {}).get("top_decisions") or [], ["ticker", "proposed_action", "final_action", "execution_status", "qc_status", "qc_rejection_reason", "risk_result", "ticker_role", "single_cap", "group_cap", "policy_version", "policy_cap_applied", "policy_cap_original", "cash_raised_by_policy_cap", "entered_via_hedge_path", "hedge_trigger_reasons", "final_target", "final_target_authority", "target_builder_target", "target_builder_target_authority", "portfolio_construction_target", "portfolio_construction_target_authority", "diagnostic_llm_target", "diagnostic_llm_target_authority", "validated_advisory_delta", "validated_advisory_delta_authority", "advisory_validator_result", "changed_by"])}
      <h3>Pipeline Stage Telemetry</h3>{_render_table(latest.get("stage_metrics") or [], ["stage", "agent", "duration_ms", "model", "prompt_tokens", "completion_tokens", "failed"])}
    """


def _render_validation_overview(report: dict[str, Any]) -> str:
    if not report.get("available"):
        return "<p class=\"muted\">Validation overview unavailable.</p>"
    return f"""
      <div class="grid">
        <article class="card"><h3>Alpha Evidence Panel</h3>{_render_kv(report.get("alpha_evidence_panel") or {})}</article>
        <article class="card"><h3>Active Basket Panel</h3>{_render_kv(report.get("active_basket_panel") or {})}</article>
        <article class="card"><h3>Hedge Calibration Panel</h3>{_render_kv(report.get("hedge_calibration_panel") or {})}</article>
        <article class="card"><h3>Validation Observation Loop</h3>{_render_kv(report.get("validation_observation_panel") or {})}</article>
        <article class="card"><h3>Stress Diagnostics Panel</h3>{_render_kv(report.get("stress_diagnostic_panel") or {})}</article>
      </div>
      <h3>Recent Hedge Outcomes</h3>{_render_table(report.get("recent_hedge_outcome_rows") or [], ["analyzed_at", "analysis_id", "triggered", "severity", "add_hedge_etf", "selected_instrument", "candidate_hedge_instrument", "why_not_add_hedge", "outcome_status", "spy_return_5d", "hedge_instrument_return_5d", "threshold_assessment"])}
      <h3>Diagnostics-First Contract</h3>{_render_kv(report.get("diagnostics_first_contract") or {})}
    """


def _render_target_path_visibility(visibility: dict[str, Any]) -> str:
    if not visibility:
        return "<p class=\"muted\">No target path visibility payload.</p>"
    if not visibility.get("available"):
        return f"""
          <p class="muted">TargetEnvelope visibility is unavailable.</p>
          {_render_kv({
              "contract_version": visibility.get("contract_version"),
              "execution_authority": visibility.get("execution_authority"),
              "warnings": visibility.get("warnings") or [],
          })}
        """
    return f"""
      <div class="target-path-panel">
        <div class="grid">
          <article class="card">
            <h3>Executable Truths</h3>
            {_render_table(visibility.get("truth_rows") or [], ["key", "label", "authority", "executable", "visual_class", "weight_count", "top_weights", "note"])}
          </article>
          <article class="card">
            <h3>Diagnostic / Shadow Surfaces</h3>
            {_render_table(visibility.get("diagnostic_surface_rows") or [], ["key", "label", "authority", "executable", "visual_class", "weight_count", "top_weights", "note"])}
          </article>
        </div>
        <h3>Target Path Contract</h3>{_render_kv({
            "contract_version": visibility.get("contract_version"),
            "execution_authority": visibility.get("execution_authority"),
            "path": visibility.get("path"),
            "warnings": visibility.get("warnings") or [],
            "accounting": visibility.get("accounting") or {},
        })}
        <h3>Target Path Stages</h3>{_render_table(visibility.get("stage_rows") or [], ["stage", "changed_ticker_count", "mutation_count", "mutation_types", "safety_effects", "cash_actual", "cash_matches_requested", "boundary_only"])}
        <h3>Stage Mutation Attribution</h3>{_render_table(visibility.get("mutation_rows") or [], ["stage", "ticker", "mutation_type", "before", "after", "delta", "current", "risk_approved", "final", "stage_effect", "safety_effect", "tighten_only", "conditional", "reason"])}
        <h3>Executable vs Diagnostic Weights</h3>{_render_table(visibility.get("weight_rows") or [], ["ticker", "actual_holdings", "risk_approved_target", "envelope_final_target", "legacy_dict_final_target", "advisory_llm_weight", "pc_shadow_reference_weight", "final_vs_actual", "risk_reduction"])}
      </div>
    """


def _render_portfolio_construction_objective(pc: dict[str, Any]) -> str:
    if not pc.get("available"):
        reason = pc.get("reason") or "No Portfolio Construction payload available."
        return f"""
          <p class="muted">{escape(str(reason))}</p>
          <div class="grid">
            <article class="card"><h3>Config</h3>{_render_kv(pc.get("config") or {})}</article>
            <article class="card"><h3>Readiness</h3>{_render_kv(pc.get("readiness") or {})}</article>
            <article class="card"><h3>Promotion Gate</h3>{_render_kv(pc.get("promotion_gate") or {})}</article>
          </div>
        """
    return f"""
      <div class="grid">
        <article class="card"><h3>Objective</h3>{_render_kv(pc.get("objective") or {})}</article>
        <article class="card"><h3>Objective Metrics</h3>{_render_kv(pc.get("objective_metrics") or {})}</article>
        <article class="card"><h3>Safety Contract</h3>{_render_kv(pc.get("safety_contract") or {})}</article>
        <article class="card"><h3>Alpha Decision Policy</h3>{_render_kv(pc.get("alpha_decision_policy") or {})}</article>
      </div>
      <div class="grid">
        <article class="card"><h3>Readiness</h3>{_render_kv(pc.get("readiness") or {})}</article>
        <article class="card"><h3>Promotion Gate</h3>{_render_kv(pc.get("promotion_gate") or {})}</article>
        <article class="card"><h3>Evaluation</h3>{_render_kv(pc.get("evaluation") or {})}</article>
        <article class="card"><h3>Active Basket Policy</h3>{_render_kv(pc.get("active_basket_policy") or {})}</article>
        <article class="card"><h3>Active Basket Calibration</h3>{_render_kv(pc.get("active_basket_calibration") or {})}</article>
      </div>
      <h3>Construction Steps</h3>{_render_list("", pc.get("construction_steps") or [])}
      <h3>Violations</h3>{_render_list("", pc.get("violations") or [])}
      <h3>Factor Exposure Before / After</h3>{_render_table(pc.get("factor_exposure_rows") or [], ["factor", "before", "after", "delta"])}
      <h3>Basket Exposure Before / After</h3>{_render_table(pc.get("basket_exposure_rows") or [], ["basket", "before", "after", "delta", "limit", "reduced_limit", "violated"])}
      <h3>Signal-Weighted Objective Rows</h3>{_render_table(pc.get("signal_objective_rows") or [], ["ticker", "signal_strength", "weight_before", "weight_after", "weight_delta", "signal_weighted_before", "signal_weighted_after", "has_signal"])}
      <h3>Alpha Decision Objective Rows</h3>{_render_table(pc.get("alpha_decision_objective_rows") or [], ["ticker", "raw_signal_strength", "independence_adjusted_signal_strength", "weight_before", "weight_after", "weight_delta", "alpha_decision_weighted_before", "alpha_decision_weighted_after", "cluster_id", "redundancy_multiplier", "decision_multiplier", "net_edge_status", "gross_expected_edge", "estimated_ibkr_cost_pct", "cost_adjusted_edge", "edge_to_cost_ratio", "policy_effective_mode", "allocation_effect"])}
      <h3>Strategy Cluster Exposure Rows</h3>{_render_table(pc.get("strategy_cluster_exposure_rows") or [], ["cluster_id", "strategies", "tickers", "weight_before", "weight_after", "weight_delta", "cluster_equity_share_after", "avg_redundancy_multiplier"])}
      <h3>Target Weights</h3>{_render_table(pc.get("target_weight_rows") or [], ["ticker", "target_weight"])}
      <h3>Weight Change Reasons</h3>{_render_table(pc.get("weight_change_reasons") or [], ["ticker", "portfolio_construction_target", "target_builder_target", "final_target", "changed_by", "construction_effect", "risk_governance_effect", "final_explanation"])}
    """


def _render_strategy_evidence(evidence: dict[str, Any]) -> str:
    if not evidence.get("available"):
        reason = evidence.get("reason") or "No ETF / Strategy Evidence available."
        return f"<p class=\"muted\">{escape(str(reason))}</p>"
    summary = evidence.get("evidence_summary") or {}
    independence = evidence.get("strategy_independence") or {}
    overview = {
        "playground_available": evidence.get("playground_available"),
        "generated_at": evidence.get("generated_at"),
        "data_quality": evidence.get("data_quality"),
        "regime_label": evidence.get("regime_label"),
        "strategy_count": evidence.get("strategy_count"),
        "alpha_strategy_count": independence.get("alpha_strategy_count"),
        "effective_independent_alpha_count": independence.get("effective_independent_alpha_count"),
        "card_count": evidence.get("card_count"),
    }
    conviction_note = {
        "display_rule": "conviction_display requires status/source/n",
        "execution_authority": "none",
        "conviction_is_shadow_only": True,
    }
    diversity = evidence.get("strategy_diversity") or {}
    diversity_note = {
        "contract_version": diversity.get("contract_version"),
        "independent_alpha_family_count": diversity.get("independent_alpha_family_count"),
        "actionable_alpha_families": diversity.get("actionable_alpha_families") or [],
        "same_family_not_independent": diversity.get("same_family_not_independent"),
        "warnings": diversity.get("warnings") or [],
        "execution_authority": diversity.get("execution_authority"),
    }
    evidence_cap = evidence.get("evidence_cap_observe") or {}
    breadth = evidence.get("strategy_breadth_calibration") or {}
    return f"""
      <div class="grid">
        <article class="card"><h3>Overview</h3>{_render_kv(overview)}</article>
        <article class="card"><h3>Evidence Summary</h3>{_render_kv(summary)}</article>
        <article class="card"><h3>Conviction Display Contract</h3>{_render_kv(conviction_note)}</article>
        <article class="card"><h3>Strategy Diversity</h3>{_render_kv(diversity_note)}</article>
        <article class="card"><h3>Strategy Independence Baseline</h3>{_render_kv(independence, keys=["status", "baseline_established", "operator_review_required", "baseline_reason", "correlation_matrix_available", "strategy_count", "alpha_strategy_count", "effective_independent_alpha_count", "low_correlation_pair_count", "low_abs_correlation_threshold", "high_correlation_pair_count", "avg_alpha_positive_correlation", "execution_authority"])}</article>
        <article class="card"><h3>Strategy Breadth Calibration</h3>{_render_kv(breadth, keys=["report_version", "status", "total_strategies", "alpha_strategy_count", "eligible_alpha_strategy_count", "estimated_independent_clusters", "estimated_breadth_is_approximation", "duplication_ratio", "minimum_overlap", "insufficient_overlap_pairs", "execution_authority", "target_weight_mutation"])}</article>
        <article class="card"><h3>Evidence Cap Observe</h3>{_render_kv(evidence_cap, keys=["available", "execution_effect", "ticker_count", "degraded_ticker_count", "would_clip_count", "mapping_error_count", "top_degraded_tickers"])}</article>
      </div>
      <h3>Duplicate Alpha Pairs</h3>{_render_table(evidence.get("breadth_duplicate_pairs") or [], ["a", "b", "a_family", "b_family", "same_family", "overlap", "corr", "abs_corr"])}
      <h3>Diversifying Strategy Pairs</h3>{_render_table(evidence.get("breadth_diversifying_pairs") or [], ["a", "b", "a_family", "b_family", "same_family", "overlap", "corr", "abs_corr"])}
      <h3>Low-Correlation Strategy Pairs</h3>{_render_table(evidence.get("independence_low_correlation_pairs") or [], ["left", "right", "left_family", "right_family", "same_family", "overlap", "correlation", "abs_correlation", "status"])}
      <h3>High-Correlation Strategy Pairs</h3>{_render_table(evidence.get("independence_high_correlation_pairs") or [], ["left", "right", "left_family", "right_family", "same_family", "overlap", "correlation", "abs_correlation", "status"])}
      <h3>Strategy Correlation Pair Rows</h3>{_render_table(evidence.get("independence_pair_rows") or [], ["left", "right", "left_family", "right_family", "same_family", "overlap", "correlation", "abs_correlation", "status"])}
      <h3>Strategy Family Correlation Rows</h3>{_render_table(evidence.get("independence_family_rows") or [], ["left_family", "right_family", "pair_count", "available_pair_count", "avg_correlation", "avg_positive_correlation", "max_abs_correlation"])}
      <h3>Evidence Cap Observe Rows</h3>{_render_table(evidence_cap.get("rows") or [], ["ticker", "static_cap", "evidence_adjusted_cap", "cap_reduction", "current_or_target_weight", "would_clip", "would_clip_to", "coverage_ratio", "evidence_quality_multiplier", "voted_count", "watch_count", "abstain_count", "mapping_error_count", "main_abstain_reason", "conviction_status", "conviction_discount", "history_days", "history_discount", "execution_effect"])}
      <h3>Evidence Mapping Error Dedupe Rows</h3>{_render_table(evidence_cap.get("mapping_error_rows") or [], ["ticker", "strategy", "reason_code", "reason", "dedupe_key", "alert_class"])}
      <h3>Strategies</h3>{_render_table(evidence.get("strategy_rows") or [], ["strategy", "raw_family", "canonical_family", "alpha_source", "data_ready", "can_influence_allocation", "suggested_use", "confidence_score", "selected_tickers", "evidence_contract_version", "cards_generated", "missing_mapping_count", "fallback_count", "mapping_error_count", "watch_vote_count", "abstain_count", "actions", "vote_statuses", "conviction_statuses", "reason_codes", "walk_forward_level", "walk_forward_pass_rate", "turnover"])}
      <h3>Strategy Family Rows</h3>{_render_table(evidence.get("diversity_family_rows") or [], ["family", "strategy_count", "alpha_source_strategy_count", "actionable_strategy_count", "actionable_alpha_strategy_count", "independent_alpha_counted", "strategy_names", "actionable_alpha_strategy_names", "suggested_uses"])}
      <h3>Strategy Diversity Rows</h3>{_render_table(evidence.get("diversity_strategy_rows") or [], ["strategy_name", "raw_family", "canonical_family", "alpha_source", "suggested_use", "actionable", "confidence_score", "data_ready", "can_influence_allocation"])}
      <h3>Evidence Matrix Default View</h3>{_render_kv(evidence.get("evidence_matrix_display_policy") or {}, keys=["default_visible_vote_statuses", "default_collapsed_vote_statuses"])}{_render_table(evidence.get("evidence_matrix_rows") or [], ["strategy", "ticker", "role", "action", "vote_status", "abstain_reason", "signal_type", "horizon", "confidence", "conviction_display", "conviction_status", "conviction_source_bucket", "conviction_n", "effective_confidence", "raw_score", "normalized_score", "max_reasonable_weight", "risk_budget_cost", "branch", "reason", "vote_reason_code", "vote_dedupe_key", "vote_alert_class", "vote_missing_fields", "mapping_role", "weight_formula", "base_cap", "max_weight_multiplier", "effective_confidence_rule", "conviction_shadow_only"])}
      <h3>Evidence Matrix Collapsed Watch/Abstain</h3>{_render_table(evidence.get("evidence_matrix_collapsed_rows") or [], ["strategy", "ticker", "role", "action", "vote_status", "abstain_reason", "signal_type", "horizon", "confidence", "conviction_display", "conviction_status", "vote_reason_code", "vote_missing_fields"])}
      <h3>Mapping And Safety Warnings</h3>{_render_table(evidence.get("mapping_warning_rows") or [], ["strategy", "ticker", "role", "action", "reason", "missing_safety_fields", "allowed_actions", "conviction_status", "conviction_n"])}
      <h3>Role / Action Summary</h3>{_render_table(evidence.get("role_action_rows") or [], ["role", "action", "count", "avg_confidence", "max_reasonable_weight_max", "tickers"])}
      <h3>Conviction Status Summary</h3>{_render_table(evidence.get("conviction_status_rows") or [], ["status", "count"])}
      <h3>Warnings</h3>{_render_list("", evidence.get("warnings") or [])}
    """


def _render_evidence_cap_calibration(calibration: dict[str, Any]) -> str:
    if not calibration:
        return "<p>No evidence cap calibration report available.</p>"
    overview = {
        "contract_version": calibration.get("contract_version"),
        "status": calibration.get("status"),
        "recommendation_only": calibration.get("recommendation_only"),
        "execution_authority": calibration.get("execution_authority"),
        "target_weight_mutation": calibration.get("target_weight_mutation"),
        "operator_action": calibration.get("operator_action"),
        "latest_conviction_profile_date": calibration.get("latest_conviction_profile_date"),
        "warnings": calibration.get("warnings") or [],
    }
    readiness = calibration.get("gated_readiness") or {}
    recommended_config = calibration.get("recommended_config") or {}
    observe = calibration.get("observe_summary") or {}
    young = calibration.get("young_etf_summary") or {}
    conviction = calibration.get("conviction_summary") or {}
    execution = calibration.get("execution_feedback") or {}
    thresholds = calibration.get("recommended_vote_thresholds") or {}
    threshold_rows = [
        {"action": action, **(rule if isinstance(rule, dict) else {"rule": rule})}
        for action, rule in thresholds.items()
    ]
    return f"""
      <div class="grid">
        <article class="card"><h3>Overview</h3>{_render_kv(overview)}</article>
        <article class="card"><h3>Gated Readiness</h3>{_render_kv(readiness, keys=["criteria_met", "gate_blockers", "observe_cycles", "min_observe_cycles", "would_clip_rate", "max_would_clip_rate", "rejection_rate", "requires_operator_approval"])}</article>
        <article class="card"><h3>Recommended Config</h3>{_render_kv(recommended_config, keys=["mode", "min_observe_cycles", "observe_cycles", "max_would_clip_rate", "would_clip_rate", "min_multiplier", "coverage_weight", "conviction_weight", "history_weight", "enforcement_criteria_met", "young_etf_cap_within_expected_range"])}</article>
        <article class="card"><h3>Observe Summary</h3>{_render_kv(observe, keys=["observe_cycles", "cap_row_count", "would_clip_count", "would_clip_rate", "degraded_ticker_count", "mapping_error_count", "median_multiplier", "median_evidence_adjusted_cap", "top_would_clip_tickers"])}</article>
        <article class="card"><h3>Young ETF Summary</h3>{_render_kv(young, keys=["history_threshold_days", "expected_cap_range", "row_count", "would_clip_count", "would_clip_rate", "median_evidence_adjusted_cap", "cap_range_status", "top_young_tickers"])}</article>
        <article class="card"><h3>Conviction / Execution</h3>{_render_kv({"profile_count": conviction.get("profile_count"), "meaningful_profile_ratio": conviction.get("meaningful_profile_ratio"), "median_n": conviction.get("median_n"), "event_count": execution.get("event_count"), "rejection_rate": execution.get("rejection_rate"), "top_rejection_reasons": execution.get("top_rejection_reasons")})}</article>
      </div>
      <h3>Recommended Vote Thresholds</h3>{_render_table(threshold_rows, ["action", "min_voted_count", "or_single_conviction_status", "min_confidence", "requires_regime"])}
      <h3>Statistical Conviction Status Counts</h3>{_render_table(_dict_rows(conviction.get("statistical_status_counts") or {}, "statistical_status", "count"), ["statistical_status", "count"])}
      <h3>Legacy Operational Conviction Status Counts</h3>{_render_table(_dict_rows(conviction.get("status_counts") or {}, "legacy_status", "count"), ["legacy_status", "count"])}
      <h3>Conviction Source Counts</h3>{_render_table(_dict_rows(conviction.get("source_counts") or {}, "source_bucket", "count"), ["source_bucket", "count"])}
    """


def _render_live_signal_conviction(conviction: dict[str, Any]) -> str:
    if not conviction.get("available"):
        reason = conviction.get("reason") or "No live signal conviction summary available."
        return f"<p class=\"muted\">{escape(str(reason))}</p>"
    profile_columns = [
        "strategy",
        "ticker",
        "branch",
        "action",
        "regime_at_signal",
        "horizon",
        "source_bucket",
        "n",
        "status",
        "conviction_display",
        "hit_rate",
        "avg_excess_vs_spy",
        "ic",
        "last_signal_date",
        "data_lag_filtered",
        "requires_live_confirmation",
        "source_counts",
    ]
    return f"""
      <div class="grid">
        <article class="card"><h3>Frozen Signals / Outcomes</h3>{_render_kv(conviction.get("overview") or {})}</article>
        <article class="card"><h3>Pending Outcomes</h3>{_render_kv(conviction.get("pending_outcomes") or {})}</article>
        <article class="card"><h3>Conviction Display Contract</h3>{_render_kv(conviction.get("display_contract") or {})}</article>
      </div>
      <h3>Pending Outcomes By Horizon</h3>{_render_table(conviction.get("pending_by_horizon_rows") or [], ["horizon_days", "missing", "mature"])}
      <h3>Profile Counts</h3>{_render_table(conviction.get("profile_count_rows") or [], ["source_bucket", "count"])}
      <h3>Statistical Conviction Status Counts</h3>{_render_table(conviction.get("statistical_status_count_rows") or [], ["statistical_status", "count"])}
      <h3>Legacy Operational Conviction Status Counts</h3>{_render_table(conviction.get("legacy_operational_status_count_rows") or [], ["legacy_status", "count"])}
      <h3>Regime-Level Conviction Summary</h3>{_render_table(conviction.get("regime_summary_rows") or [], ["regime_at_signal", "source_bucket", "profile_count", "total_n", "operational_calibrated_profiles", "early_profiles", "insufficient_profiles", "hit_rate", "avg_excess_vs_spy", "ic", "data_lag_filtered"])}
      <h3>Regime-Level Conviction Profiles</h3>{_render_table(conviction.get("regime_level_profiles") or [], profile_columns)}
      <h3>Historical Prior Profiles</h3>{_render_table(conviction.get("historical_prior_profiles") or [], profile_columns)}
      <h3>Live Paper Profiles</h3>{_render_table(conviction.get("live_paper_profiles") or [], profile_columns)}
      <h3>Combined Profiles</h3>{_render_table(conviction.get("combined_profiles") or [], profile_columns)}
    """


def _render_performance_attribution(attribution: dict[str, Any]) -> str:
    if not attribution.get("available"):
        reason = attribution.get("reason") or "No performance attribution rows available."
        return f"<p class=\"muted\">{escape(str(reason))}</p>"
    latest = attribution.get("latest") or {}
    monthly = attribution.get("monthly_alpha_report") or {}
    return f"""
      <div class="grid">
        <article class="card"><h3>Latest Attribution</h3>{_render_kv(latest, keys=["period_key", "status", "portfolio_return", "arithmetic_portfolio_return", "residual_alpha_candidate", "r_squared", "sample_count", "data_quality"])}</article>
        <article class="card"><h3>Factor Model</h3>{_render_kv(latest, keys=["attribution_method", "benchmark_source", "momentum_proxy", "source_tickers"])}</article>
        <article class="card"><h3>Residual Contract</h3>{_render_kv(attribution.get("residual_contract") or {})}</article>
      </div>
      <h3>Monthly Alpha Report</h3>{_render_kv(monthly, keys=["report_version", "status", "sample_count", "sample_status", "factor_model", "beta_vs_spy", "alpha_daily", "alpha_annualized", "alpha_t_stat", "alpha_p_value", "r_squared", "honest_interpretation", "meets_t2_suggestive", "meets_harvey_t3_threshold", "execution_authority", "target_weight_mutation"])}
      <h3>Return Breakdown</h3>{_render_table(attribution.get("return_breakdown_rows") or [], ["component", "source", "beta", "contribution"])}
      <h3>Recent Attribution Runs</h3>{_render_table(attribution.get("recent_rows") or [], ["period_key", "period_start", "period_end", "status", "portfolio_return", "spy_beta_contribution", "qqq_beta_contribution", "momentum_factor_contribution", "residual_alpha_candidate", "r_squared", "sample_count", "data_quality"])}
      <h3>Status Counts</h3>{_render_table(attribution.get("status_rows") or [], ["status", "count"])}
    """


def _render_portfolio_risk_diagnostic(diagnostic: dict[str, Any]) -> str:
    if not diagnostic.get("available"):
        reason = diagnostic.get("reason") or "No portfolio risk diagnostic available."
        return f"<p class=\"muted\">{escape(str(reason))}</p>"
    overview = {
        "status": diagnostic.get("status"),
        "mode": diagnostic.get("mode"),
        "execution_authority": diagnostic.get("execution_authority"),
        "target_weight_mutation": diagnostic.get("target_weight_mutation"),
        "confidence_level": diagnostic.get("confidence_level"),
        "lookback_days": diagnostic.get("lookback_days"),
        "source": diagnostic.get("source"),
        "data_quality": diagnostic.get("data_quality"),
    }
    target_stress = (diagnostic.get("target_scenario_stress") or {}).get("scenarios") or []
    current_stress = (diagnostic.get("current_scenario_stress") or {}).get("scenarios") or []
    target_beta = _flatten_beta_shock_rows(diagnostic.get("target_beta_shock") or {})
    current_beta = _flatten_beta_shock_rows(diagnostic.get("current_beta_shock") or {})
    return f"""
      <div class="grid">
        <article class="card"><h3>Overview</h3>{_render_kv(overview)}</article>
        <article class="card"><h3>Summary</h3>{_render_kv(diagnostic.get("summary") or {})}</article>
        <article class="card"><h3>Target Historical VaR / CVaR</h3>{_render_kv(diagnostic.get("target_historical") or {})}</article>
        <article class="card"><h3>Current Historical VaR / CVaR</h3>{_render_kv(diagnostic.get("current_historical") or {})}</article>
      </div>
      <h3>Target Scenario Losses</h3>{_render_table(diagnostic.get("target_scenarios") or [], ["scenario", "portfolio_return", "estimated_loss", "description", "shock_returns"])}
      <h3>Current Scenario Losses</h3>{_render_table(diagnostic.get("current_scenarios") or [], ["scenario", "portfolio_return", "estimated_loss", "description", "shock_returns"])}
      <h3>Target Historical Scenario Stress</h3>{_render_table(target_stress, ["scenario", "portfolio_return", "spy_return", "relative_return", "estimated_loss", "top_loss_summary"])}
      <h3>Current Historical Scenario Stress</h3>{_render_table(current_stress, ["scenario", "portfolio_return", "spy_return", "relative_return", "estimated_loss", "top_loss_summary"])}
      <h3>Target Beta Shock</h3>{_render_table(target_beta, ["shock_group", "shock_name", "reference", "role", "portfolio_return", "estimated_loss", "top_loss_summary"])}
      <h3>Current Beta Shock</h3>{_render_table(current_beta, ["shock_group", "shock_name", "reference", "role", "portfolio_return", "estimated_loss", "top_loss_summary"])}
      <h3>Warnings</h3>{_render_list("", diagnostic.get("warnings") or [])}
    """


def _flatten_beta_shock_rows(report: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for key, label in (
        ("spy_shocks", "SPY"),
        ("qqq_shocks", "QQQ"),
        ("role_shocks", "role"),
    ):
        for row in report.get(key) or []:
            out = dict(row)
            out["shock_group"] = label
            rows.append(out)
    return rows


def _render_alpha_validation_trend(trend: dict[str, Any]) -> str:
    if not trend.get("available"):
        reason = trend.get("reason") or "No alpha validation trend available."
        return f"<p class=\"muted\">{escape(str(reason))}</p>"
    columns = [
        "generated_at",
        "analysis_id",
        "status",
        "data_quality",
        "risk_approved",
        "cost_gate_status",
        "low_edge_trade_count",
        "min_edge_to_cost_ratio",
        "var_95_loss",
        "cvar_95_loss",
        "max_scenario_loss",
        "signal_alignment_score",
        "signal_weighted_effective_n",
        "independent_alpha_family_count",
        "calibrated_conviction_count",
        "warning_count",
    ]
    return f"""
      <div class="grid">
        <article class="card"><h3>Latest Run</h3>{_render_kv(trend.get("latest") or {})}</article>
        <article class="card"><h3>Trend Metrics</h3>{_render_kv(trend.get("trend_metrics") or {})}</article>
        <article class="card"><h3>Contract</h3>{_render_kv(trend.get("contract") or {})}</article>
      </div>
      <h3>Recent Alpha Validation Runs</h3>{_render_table(trend.get("recent_rows") or [], columns)}
      <h3>Status Counts</h3>{_render_table(trend.get("status_rows") or [], ["status", "count"])}
      <h3>Data Quality Counts</h3>{_render_table(trend.get("data_quality_rows") or [], ["data_quality", "count"])}
    """


def _render_strategy_regime_gap_analysis(analysis: dict[str, Any]) -> str:
    if not analysis.get("available"):
        reason = analysis.get("reason") or "No strategy regime gap analysis available."
        return f"<p class=\"muted\">{escape(str(reason))}</p>"
    overview = {
        "status": analysis.get("status"),
        "as_of_date": analysis.get("as_of_date"),
        "latest_profile_date": analysis.get("latest_profile_date"),
        "profile_count": analysis.get("profile_count"),
        "statistically_mature_alpha_profile_count": analysis.get("statistically_mature_alpha_profile_count"),
        "actionable_alpha_family_count": analysis.get("actionable_alpha_family_count"),
        "actionable_alpha_families": analysis.get("actionable_alpha_families"),
        "momentum_overconcentration": analysis.get("momentum_overconcentration"),
    }
    contract = {
        "contract_version": analysis.get("contract_version"),
        "execution_authority": analysis.get("execution_authority"),
        "target_weight_mutation": analysis.get("target_weight_mutation"),
        "alpha_validation_sample_count": analysis.get("alpha_validation_sample_count"),
    }
    return f"""
      <div class="grid">
        <article class="card"><h3>Coverage Overview</h3>{_render_kv(overview)}</article>
        <article class="card"><h3>Diagnostics Contract</h3>{_render_kv(contract)}</article>
        <article class="card"><h3>Latest Alpha Validation</h3>{_render_kv(analysis.get("latest_alpha_validation") or {})}</article>
      </div>
      <h3>Regime Coverage Rows</h3>{_render_table(analysis.get("regime_rows") or [], ["regime", "coverage_status", "statistically_mature_profile_count", "statistically_mature_families", "expected_families", "missing_expected_families", "hit_rate", "avg_excess_vs_spy", "ic", "total_n"])}
      <h3>Family Coverage Rows</h3>{_render_table(analysis.get("family_rows") or [], ["family", "statistically_mature_profile_count", "covered_regimes", "weak_regimes", "hit_rate", "avg_excess_vs_spy", "ic", "total_n"])}
      <h3>Weak Family / Regime Rows</h3>{_render_table(analysis.get("weak_family_regime_rows") or [], ["family", "regime", "profile_count", "hit_rate", "avg_excess_vs_spy", "ic", "total_n", "reasons"])}
      <h3>Simultaneous Failure Regime Rows</h3>{_render_table(analysis.get("simultaneous_failure_regime_rows") or [], ["regime", "profile_count", "weak_profile_count", "families", "strategies", "hit_rate", "avg_excess_vs_spy", "ic", "reason"])}
      <h3>Research Queue</h3>{_render_table(analysis.get("research_queue") or [], ["priority", "regime", "suggested_family", "reason"])}
      <h3>Warnings</h3>{_render_list("", analysis.get("warnings") or [])}
    """


def _render_strategy_promotion_recommendations(recommendations: dict[str, Any]) -> str:
    if not recommendations.get("available"):
        reason = recommendations.get("reason") or "No promotion/degradation recommendations available."
        return f"<p class=\"muted\">{escape(str(reason))}</p>"
    overview = {
        "status": recommendations.get("status"),
        "as_of_date": recommendations.get("as_of_date"),
        "latest_profile_date": recommendations.get("latest_profile_date"),
        "latest_analysis_id": recommendations.get("latest_analysis_id"),
        "profile_count": recommendations.get("profile_count"),
        "strategy_count": recommendations.get("strategy_count"),
        "raw_alpha_strategy_count": (recommendations.get("alpha_decision_profiles") or {}).get("raw_alpha_strategy_count"),
        "independence_adjusted_strategy_count": (
            recommendations.get("alpha_decision_profiles") or {}
        ).get("independence_adjusted_strategy_count"),
        "recommendation_count": recommendations.get("recommendation_count"),
        "high_priority_count": recommendations.get("high_priority_count"),
    }
    contract = {
        "contract_version": recommendations.get("contract_version"),
        "execution_authority": recommendations.get("execution_authority"),
        "target_weight_mutation": recommendations.get("target_weight_mutation"),
        "recommendation_only": recommendations.get("recommendation_only"),
        "gap_status": recommendations.get("gap_status"),
    }
    return f"""
      <div class="grid">
        <article class="card"><h3>Recommendation Overview</h3>{_render_kv(overview)}</article>
        <article class="card"><h3>Recommendation Contract</h3>{_render_kv(contract)}</article>
        <article class="card"><h3>Recommendation Counts</h3>{_render_kv(recommendations.get("recommendation_counts") or {})}</article>
      </div>
      <h3>Recommendation Rows</h3>{_render_table(recommendations.get("recommendations") or [], ["priority", "recommendation", "strategy_id", "canonical_family", "regime", "current_use", "recommended_use", "sample_count", "profile_count", "hit_rate", "avg_excess_vs_spy", "ic", "conviction", "statistical_status_counts", "residual_alpha_status", "residual_alpha", "net_edge_status", "gross_expected_edge", "estimated_ibkr_cost_pct", "cost_adjusted_edge", "edge_to_cost_ratio", "redundancy_multiplier", "max_positive_correlation", "reasons", "blockers", "operator_action"])}
      <h3>Recommendation Policy</h3>{_render_kv(recommendations.get("policy") or {})}
      <h3>Warnings</h3>{_render_list("", recommendations.get("warnings") or [])}
    """


def _render_alpha_decision_policy(policy: dict[str, Any]) -> str:
    if not policy.get("available", True):
        return f"<p class=\"muted\">{escape(str(policy.get('reason') or 'Alpha decision policy unavailable.'))}</p>"
    overview = {
        "mode": policy.get("mode"),
        "effective_mode": policy.get("effective_mode"),
        "gated_enabled": policy.get("gated_enabled"),
        "recommendation_effect": policy.get("recommendation_effect"),
        "allocation_effect": policy.get("allocation_effect"),
        "would_affect_allocation": policy.get("would_affect_allocation"),
    }
    contract = {
        "contract_version": policy.get("contract_version"),
        "execution_authority": policy.get("execution_authority"),
        "target_weight_mutation": policy.get("target_weight_mutation"),
        "never_bypasses_target_builder": policy.get("never_bypasses_target_builder"),
        "full_auto_safety_preconditions_unchanged": policy.get("full_auto_safety_preconditions_unchanged"),
    }
    return f"""
      <div class="grid">
        <article class="card"><h3>Alpha Decision Policy Mode</h3>{_render_kv(overview)}</article>
        <article class="card"><h3>Alpha Decision Policy Contract</h3>{_render_kv(contract)}</article>
        <article class="card"><h3>Gated Criteria</h3>{_render_kv(policy.get("criteria") or {})}</article>
      </div>
      <h3>Decision Rules</h3>{_render_kv(policy.get("decision_rules") or {})}
      <h3>Gated Blockers</h3>{_render_list("", policy.get("blockers") or [])}
      <h3>Warnings</h3>{_render_list("", policy.get("warnings") or [])}
    """


def _render_alpha_decision_review_surface(review: dict[str, Any]) -> str:
    if not review.get("available"):
        return f"<p class=\"muted\">{escape(str(review.get('reason') or 'Alpha decision review surface unavailable.'))}</p>"
    mode = {
        "mode": review.get("mode"),
        "effective_mode": review.get("effective_mode"),
        "gated_enabled": review.get("gated_enabled"),
        "recommendation_effect": review.get("recommendation_effect"),
        "allocation_effect": review.get("allocation_effect"),
    }
    contract = {
        "contract_version": review.get("contract_version"),
        "execution_authority": review.get("execution_authority"),
        "target_weight_mutation": review.get("target_weight_mutation"),
    }
    return f"""
      <div class="grid">
        <article class="card"><h3>Review Mode</h3>{_render_kv(mode)}</article>
        <article class="card"><h3>Review Contract</h3>{_render_kv(contract)}</article>
        <article class="card"><h3>Review Checklist</h3>{_render_kv(review.get("review_checklist") or {})}</article>
      </div>
      <div class="grid">
        <article class="card"><h3>Statistical Maturity And Independence</h3>{_render_kv(review.get("statistical_maturity") or {})}</article>
        <article class="card"><h3>Strategy Independence</h3>{_render_kv(review.get("strategy_independence") or {})}</article>
        <article class="card"><h3>Promotion Review</h3>{_render_kv(review.get("promotion_review") or {})}</article>
      </div>
      <div class="grid">
        <article class="card"><h3>PC Before / After Allocation Diagnostics</h3>{_render_kv(review.get("pc_review") or {})}</article>
        <article class="card"><h3>Latest Residual Alpha</h3>{_render_kv(review.get("latest_residual_alpha") or {})}</article>
      </div>
      <h3>Net Alpha Review Rows</h3>{_render_table(review.get("net_alpha_rows") or [], ["strategy_id", "strategy_family", "regime", "construction_epoch_id", "sample_count", "statistical_status", "residual_alpha_status", "residual_alpha", "gross_expected_edge", "estimated_ibkr_cost_pct", "cost_adjusted_edge", "edge_to_cost_ratio", "net_edge_status", "redundancy_multiplier", "decision_multiplier", "decision_status"])}
      <h3>Promotion Review Rows</h3>{_render_table(review.get("promotion_review_rows") or [], ["priority", "recommendation", "strategy_id", "canonical_family", "regime", "current_use", "recommended_use", "sample_count", "statistical_status_counts", "residual_alpha_status", "residual_alpha", "net_edge_status", "gross_expected_edge", "estimated_ibkr_cost_pct", "cost_adjusted_edge", "edge_to_cost_ratio", "redundancy_multiplier", "max_positive_correlation", "reasons", "blockers", "operator_action"])}
      <h3>Strategy Cluster Review Rows</h3>{_render_table(review.get("strategy_cluster_review_rows") or [], ["cluster_id", "strategies", "tickers", "weight_before", "weight_after", "weight_delta", "cluster_equity_share_after", "avg_redundancy_multiplier"])}
      <h3>PC Alpha Decision Before / After Rows</h3>{_render_table(review.get("pc_before_after_rows") or [], ["ticker", "raw_signal_strength", "independence_adjusted_signal_strength", "weight_before", "weight_after", "weight_delta", "alpha_decision_weighted_before", "alpha_decision_weighted_after", "cluster_id", "redundancy_multiplier", "decision_multiplier", "net_edge_status", "gross_expected_edge", "estimated_ibkr_cost_pct", "cost_adjusted_edge", "edge_to_cost_ratio", "policy_effective_mode", "allocation_effect"])}
      <h3>Review Warnings</h3>{_render_list("", review.get("warnings") or [])}
    """


def _render_alpha_readiness_report(report: dict[str, Any]) -> str:
    if not report.get("available"):
        return f"<p class=\"muted\">{escape(str(report.get('reason') or 'Alpha readiness report unavailable.'))}</p>"
    overview = {
        "status": report.get("status"),
        "strategy_count": report.get("strategy_count"),
        "candidate_count": report.get("candidate_count"),
        "hard_mapping_error_count": report.get("hard_mapping_error_count"),
        "diagnostic_only": report.get("diagnostic_only"),
    }
    contract = {
        "contract_version": report.get("contract_version"),
        "execution_authority": report.get("execution_authority"),
        "target_weight_mutation": report.get("target_weight_mutation"),
        "attribution_trade_authority": report.get("attribution_trade_authority"),
        "gated_authority_out_of_scope": report.get("gated_authority_out_of_scope"),
    }
    return f"""
      <div class="grid">
        <article class="card"><h3>Readiness Overview</h3>{_render_kv(overview)}</article>
        <article class="card"><h3>Readiness Contract</h3>{_render_kv(contract)}</article>
        <article class="card"><h3>Candidate Criteria</h3>{_render_kv(report.get("criteria") or {})}</article>
      </div>
      <h3>Authority Counts</h3>{_render_kv(report.get("authority_counts") or {})}
      <h3>Strategy Readiness Rows</h3>{_render_table(report.get("rows") or [], ["suggested_authority", "strategy_id", "mapping_coverage_pct", "voted_signal_count", "mapping_error_count", "live_sample_count", "residual_alpha_latest", "redundancy_cluster", "max_positive_correlation", "readiness_reasons", "authority_blockers", "mapping_error_cycles_last_10"])}
      <h3>Readiness Warnings</h3>{_render_list("", report.get("warnings") or [])}
    """


def _render_alpha_decision_profiles(profiles: dict[str, Any]) -> str:
    if not profiles.get("available"):
        reason = profiles.get("reason") or "No alpha decision profiles available."
        return f"<p class=\"muted\">{escape(str(reason))}</p>"
    overview = {
        "status": profiles.get("status"),
        "as_of_date": profiles.get("as_of_date"),
        "latest_profile_date": profiles.get("latest_profile_date"),
        "profile_count": profiles.get("profile_count"),
        "source_profile_count": profiles.get("source_profile_count"),
        "strategy_count": profiles.get("strategy_count"),
        "raw_alpha_strategy_count": profiles.get("raw_alpha_strategy_count"),
        "independence_adjusted_strategy_count": profiles.get("independence_adjusted_strategy_count"),
        "eligible_count": profiles.get("eligible_count"),
    }
    contract = {
        "contract_version": profiles.get("contract_version"),
        "execution_authority": profiles.get("execution_authority"),
        "target_weight_mutation": profiles.get("target_weight_mutation"),
        "decision_input_only": profiles.get("decision_input_only"),
        "recommendation_only": profiles.get("recommendation_only"),
    }
    return f"""
      <div class="grid">
        <article class="card"><h3>Alpha Decision Overview</h3>{_render_kv(overview)}</article>
        <article class="card"><h3>Alpha Decision Contract</h3>{_render_kv(contract)}</article>
        <article class="card"><h3>Independence Consumption</h3>{_render_kv(profiles.get("independence_consumption") or {})}</article>
      </div>
      <h3>Decision Status Counts</h3>{_render_kv(profiles.get("status_counts") or {})}
      <h3>Residual Alpha Status Counts</h3>{_render_kv(profiles.get("residual_alpha_status_counts") or {})}
      <h3>Cost Status Counts</h3>{_render_kv(profiles.get("cost_status_counts") or {})}
      <h3>Net Edge Status Counts</h3>{_render_kv(profiles.get("net_edge_status_counts") or {})}
      <h3>Alpha Decision Rows</h3>{_render_table(profiles.get("rows") or [], ["decision_status", "strategy_id", "strategy_family", "regime", "sample_count", "statistical_status", "residual_alpha_status", "cost_status", "net_edge_status", "gross_expected_edge", "estimated_ibkr_cost_pct", "cost_adjusted_edge", "edge_to_cost_ratio", "independence_cluster_id", "redundancy_multiplier", "max_positive_correlation", "decision_multiplier"])}
      <h3>Warnings</h3>{_render_list("", profiles.get("warnings") or [])}
    """


def _render_replay(replay: dict[str, Any]) -> str:
    return f"""
      <div class="grid">
        <article class="card">{_render_kv(replay, keys=["lookback_days", "row_limit_before_dedupe"])}</article>
        <article class="card"><h3>Deduped With Limit</h3>{_render_kv(replay.get("deduped_with_limit") or {})}</article>
        <article class="card"><h3>Deduped Without Limit</h3>{_render_kv(replay.get("deduped_without_limit") or {})}</article>
      </div>
      <h3>Raw QC Rows</h3>{_render_table(replay.get("raw_by_type") or [], ["packet_type", "rows", "trading_days", "first_received", "last_received"])}
    """


def _render_data_quality_audit(audit: dict[str, Any]) -> str:
    if not audit.get("available"):
        return f"<p class=\"muted\">{escape(str(audit.get('reason') or 'No audit rows.'))}</p>"
    latest = audit.get("latest") or {}
    trend = audit.get("trend") or {}
    recent = audit.get("recent") or []
    return f"""
      <div class="grid">
        <article class="card"><h3>Latest Audit</h3>{_render_kv(latest, keys=["created_at", "status", "lookback_days", "joined_rows", "unit_risk_count", "high_drift_classes"])}</article>
        <article class="card"><h3>Trend</h3>{_render_kv(trend)}</article>
      </div>
      <h3>Recent Audit Runs</h3>{_render_table(recent, ["created_at", "status", "lookback_days", "joined_rows", "unit_risk_count", "high_drift_classes", "max_raw_momentum_error", "max_normalized_momentum_error", "unit_risk_fields", "high_drift_labels"])}
    """


def _render_execution_control(control: dict[str, Any]) -> str:
    if not control.get("available"):
        reason = control.get("reason") or "Execution control tables unavailable."
        return f"""
          <p class="muted">{escape(str(reason))}</p>
          <div class="grid">
            <article class="card"><h3>Account State Guard</h3>{_render_kv(control.get("account_state_guard") or {})}</article>
            <article class="card"><h3>Auto Pause</h3>{_render_kv(control.get("auto_pause") or {})}</article>
          </div>
        """
    guard = control.get("account_state_guard") or {}
    auto_pause = control.get("auto_pause") or {}
    latest_snapshot = control.get("latest_account_snapshot") or {}
    active_execution = control.get("active_execution") or {}
    deferred = control.get("deferred_execution") or {}
    reconciliation_lag = control.get("reconciliation_lag") or {}
    return f"""
      <div class="grid">
        <article class="card"><h3>Account State Guard</h3>{_render_kv(guard, keys=["mode", "status", "allowed", "would_block", "pipeline_enforcement", "pipeline_effect_status", "execution_effect", "primary_blockers", "warnings"])}</article>
        <article class="card"><h3>Auto Pause</h3>{_render_kv(auto_pause, keys=["mode", "status", "would_pause", "should_pause", "execution_effect", "primary_trigger", "reason"])}</article>
        <article class="card"><h3>Latest Account Snapshot</h3>{_render_kv(latest_snapshot, keys=["available", "recorded_at", "account_timestamp", "source_packet_type", "contract_version", "account_status", "data_status", "policy_version", "total_value", "cash_pct", "buying_power", "open_order_count", "has_open_orders", "is_market_open", "last_command_id", "active_command_id", "active_execution_status", "processed_command_count", "holdings_count", "target_count", "explicit_account_state"])}</article>
        <article class="card"><h3>Active Execution</h3>{_render_kv(active_execution, keys=["available", "active", "active_command_id", "status", "qc_status", "submitted_order_count", "actual_order_count", "filled_order_count", "is_noop", "open_order_count", "has_open_orders", "started_at", "elapsed_minutes", "latest_snapshot_at", "max_target_actual_drift", "can_ordinary_rebalance", "can_reduce_only", "execution_contract", "operator_note"])}</article>
        <article class="card"><h3>Deferred Execution Pressure</h3>{_render_kv(deferred, keys=["available", "open_count", "open_buy_delta", "open_sell_delta", "open_tickers"])}</article>
        <article class="card"><h3>Reconciliation Lag</h3>{_render_kv(reconciliation_lag, keys=["accepted_without_reconciled_count", "overdue_count", "pending_count", "max_age_minutes", "execution_effect"])}</article>
      </div>
      <h3>Active Execution Target vs Actual Drift</h3>{_render_table(active_execution.get("drift_rows") or [], ["ticker", "target", "actual", "diff"])}
      <h3>Active Execution Events</h3>{_render_table(active_execution.get("recent_event_rows") or [], ["event_time", "command_id", "event_type", "event_status", "source", "reason", "execution_state", "submitted_order_count", "actual_order_count", "filled_order_count", "is_noop", "open_order_count", "max_abs_diff", "diff_count"])}
      <h3>Account Guard Checks</h3>{_render_table(guard.get("checks") or [], ["check", "pass", "actual", "threshold", "reason"])}
      <h3>Auto Pause Triggers</h3>{_render_table(auto_pause.get("triggers") or [], ["trigger", "triggered", "value", "threshold", "severity", "details"])}
      <h3>Recent QC Commands</h3>{_render_table(control.get("recent_commands") or [], ["executed_at", "command_id", "analysis_id", "command_type", "display_status", "lifecycle_display_status", "lifecycle_status_source", "latest_lifecycle_event", "status", "qc_status", "execution_state", "qc_ack_at", "qc_rejection_reason", "active_command_id", "submitted_order_count", "actual_order_count", "filled_order_count", "is_noop", "open_order_count", "superseded_command_id", "canceled_order_count", "policy_mismatch", "retry_count"])}
      <h3>Command Lifecycle Events</h3>{_render_table(control.get("recent_command_events") or [], ["event_time", "command_id", "analysis_id", "event_type", "event_status", "source", "reason", "qc_status", "execution_state", "submitted_order_count", "actual_order_count", "filled_order_count", "is_noop", "open_order_count", "max_abs_diff", "diff_count", "policy_mismatch", "policy_version", "target_count", "payload_keys"])}
      <h3>Accepted Commands Without Reconciliation</h3>{_render_table(reconciliation_lag.get("rows") or [], ["command_id", "analysis_id", "qc_status", "accepted_at", "age_minutes", "max_age_minutes", "status", "latest_event_type", "latest_event_status", "reason"])}
      <h3>Deferred Execution Ledger</h3>{_render_table(deferred.get("recent_rows") or [], ["created_at", "updated_at", "resolved_at", "command_id", "analysis_id", "status", "side", "ticker", "original_delta", "remaining_delta", "current_weight", "desired_weight", "staged_weight", "latest_current_weight", "latest_desired_weight", "latest_staged_weight", "reason", "resolution_reason", "review_count"])}
    """


def _render_crons(rows: list[dict[str, Any]]) -> str:
    return _render_table(rows, ["job_name", "status", "started_at", "duration_ms", "error_message"])


def _render_weekend_review_operator(pack: dict[str, Any]) -> str:
    if not pack.get("available"):
        return f"<p class=\"muted\">{escape(str(pack.get('reason') or 'Weekend review unavailable.'))}</p>"
    view = pack.get("view") if isinstance(pack.get("view"), dict) else {}
    headline = view.get("headline") if isinstance(view.get("headline"), dict) else {}
    sections = view.get("sections") if isinstance(view.get("sections"), dict) else {}
    blocker_section = sections.get("blocker_distribution") if isinstance(sections.get("blocker_distribution"), dict) else {}
    labels = sections.get("label_maturity") if isinstance(sections.get("label_maturity"), dict) else {}
    hedge = sections.get("hedge_review") if isinstance(sections.get("hedge_review"), dict) else {}
    basket = sections.get("basket_portfolio") if isinstance(sections.get("basket_portfolio"), dict) else {}
    recommendations = view.get("recommendations") if isinstance(view.get("recommendations"), list) else []
    answers = view.get("acceptance_answers") if isinstance(view.get("acceptance_answers"), list) else []
    text_url = "/api/ops/weekend-review/latest/text"
    json_url = "/api/ops/weekend-review/latest?include_full_report=true"
    headline_payload = {
        "week_start": view.get("week_start"),
        "week_end": view.get("week_end"),
        "review_as_of": view.get("review_as_of"),
        "commands_sent": headline.get("commands_sent"),
        "filled": headline.get("filled_count"),
        "not_sent": headline.get("not_sent_count"),
        "preflight": headline.get("preflight_blocked_count"),
        "dedupe": headline.get("duplicate_target_count"),
        "timeout_ack": headline.get("timeout_no_ack_count"),
        "no_exec": headline.get("timeout_no_execution_confirmed_count"),
        "top_blocker": headline.get("top_blocker"),
    }
    label_payload = labels.get("metrics") if isinstance(labels.get("metrics"), dict) else {}
    hedge_payload = hedge.get("metrics") if isinstance(hedge.get("metrics"), dict) else {}
    basket_payload = basket.get("metrics") if isinstance(basket.get("metrics"), dict) else {}
    return f"""
      <div class="grid">
        <article class="card"><h3>Weekly Execution Truth</h3>{_render_kv(headline_payload)}</article>
        <article class="card"><h3>Label Maturity</h3>{_render_kv(label_payload, keys=["eligible_label_count", "excluded_immature_count", "label_1d_mature_count", "label_5d_mature_count", "label_5d_pending_count", "fallback_label_count"])}</article>
        <article class="card"><h3>Hedge Review</h3>{_render_kv(hedge_payload, keys=["hedge_trigger_count", "hedge_added_count", "false_negative_count", "missed_protection_count", "insufficient_counterfactual_count"])}</article>
        <article class="card"><h3>Basket Health</h3>{_render_kv(basket_payload, keys=["active_count_avg", "active_count_out_of_range_count", "floor_cleared_count", "subscale_position_count"])}</article>
      </div>
      <h3>Blocker Distribution</h3>{_render_table(_dict_rows(blocker_section.get("blocker_distribution") or {}), ["key", "value"])}
      <h3>Review-Only Recommendations</h3>{_render_table(recommendations[:8], ["label", "text", "execution_authority", "target_weight_mutation"])}
      <h3>Acceptance Questions</h3>{_render_table(answers, ["id", "question", "deterministic_source", "status", "answer_payload", "llm_computed"])}
      <p class="muted">Full JSON: <a href="{escape(json_url)}">{escape(json_url)}</a> | Text: <a href="{escape(text_url)}">{escape(text_url)}</a></p>
    """


def _dict_rows(data: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {"key": key, "value": value}
        for key, value in sorted(data.items(), key=lambda item: str(item[0]))
    ]


def _render_table(rows: list[dict[str, Any]], columns: list[str]) -> str:
    if not rows:
        return "<p class=\"muted\">No rows.</p>"
    head = "".join(f"<th>{escape(col)}</th>" for col in columns)
    body_rows = []
    for row in rows:
        cells = "".join(f"<td>{escape(_format_value(row.get(col)))}</td>" for col in columns)
        body_rows.append(f"<tr>{cells}</tr>")
    return f"<div class=\"table-wrap\"><table><thead><tr>{head}</tr></thead><tbody>{''.join(body_rows)}</tbody></table></div>"


def _render_kv(data: dict[str, Any], keys: list[str] | None = None) -> str:
    keys = keys or list(data.keys())
    rows = []
    for key in keys:
        value = data.get(key)
        if isinstance(value, (dict, list)):
            value = _compact_json(value)
        rows.append(f"<div class=\"kv\"><span>{escape(str(key))}</span><strong>{escape(_format_value(value))}</strong></div>")
    return "".join(rows)


def _render_list(title: str, items: list[Any]) -> str:
    if not items:
        return "" if title else "<p class=\"muted\">None.</p>"
    heading = f"<h3>{escape(title)}</h3>" if title else ""
    lis = "".join(f"<li>{escape(str(item))}</li>" for item in items)
    return f"{heading}<ul>{lis}</ul>"


def _format_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.4f}"
    return str(value)


def _json_safe_number(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _mean_present(rows: list[dict[str, Any]], key: str) -> float | None:
    values = [
        _json_safe_number(row.get(key))
        for row in rows
        if _json_safe_number(row.get(key)) is not None
    ]
    if not values:
        return None
    return round(sum(float(value) for value in values) / len(values), 6)


def _compact_json(value: Any) -> str:
    return str(value)


def _iso(value: Any) -> str | None:
    return value.isoformat() if hasattr(value, "isoformat") else None


def _css() -> str:
    return """
    :root { color-scheme: light; --bg:#f6f7f9; --ink:#111827; --muted:#6b7280; --line:#d8dde6; --card:#ffffff; --soft:#f9fafb; --ok:#0f766e; --bad:#b42318; --warn:#a16207; --info:#1d4ed8; --dark:#0c1524; --dark2:#080e1a; --dark-line:#1e293b; }
    * { box-sizing: border-box; }
    html { scroll-behavior:smooth; }
    body { margin:0; overflow-x:hidden; background:var(--bg); color:var(--ink); font:14px/1.45 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif; }
    header { display:flex; align-items:flex-end; justify-content:space-between; gap:24px; padding:22px clamp(16px,3vw,36px) 16px; border-bottom:1px solid var(--line); background:#fff; }
    h1 { margin:0; font-size:24px; letter-spacing:0; }
    h2 { margin:0; font-size:18px; }
    h3 { margin:16px 0 8px; font-size:14px; }
    p { margin:4px 0 0; }
    .quick-nav { position:sticky; top:0; z-index:5; display:flex; gap:6px; overflow:auto; padding:10px clamp(16px,3vw,36px); border-bottom:1px solid var(--line); background:rgba(255,255,255,.94); backdrop-filter:blur(10px); }
    .quick-nav a { flex:0 0 auto; color:var(--ink); text-decoration:none; padding:7px 10px; border:1px solid var(--line); border-radius:8px; background:#fff; font-size:13px; }
    .dashboard-shell { width:min(100%,1680px); margin:0 auto; padding:22px clamp(12px,2vw,28px) 48px; display:grid; gap:18px; }
    section, .panel { min-width:0; background:#fff; border:1px solid var(--line); padding:18px; border-radius:8px; }
    .overview-panel { display:grid; gap:16px; }
    .section-heading { display:flex; align-items:flex-end; justify-content:space-between; gap:16px; margin-bottom:14px; }
    .section-heading p { color:var(--muted); max-width:720px; }
    .grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(220px,1fr)); gap:12px; min-width:0; }
    .checks { grid-template-columns:repeat(auto-fit,minmax(180px,1fr)); }
    .card { border:1px solid var(--line); border-radius:8px; padding:14px; background:var(--card); min-width:0; }
    .command-grid { display:grid; grid-template-columns:repeat(6,minmax(0,1fr)); gap:12px; }
    .dashboard-focus { grid-template-columns:repeat(3,minmax(0,1fr)); }
    .status-card { min-width:0; border:1px solid var(--line); border-left:4px solid var(--info); border-radius:8px; padding:13px; background:#fff; }
    .status-card .metric { font-size:18px; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
    .status-card .timestamp { color:var(--muted); font-size:12px; margin-top:6px; overflow-wrap:anywhere; }
    .label, .muted { color:var(--muted); }
    .metric { margin-top:6px; font-size:22px; font-weight:700; }
    .ok, .healthy, .success, .approved, .closed, .idle, .pass { color:var(--ok); }
    .stale, .failed, .execution_blocked, .blocker, .rejected, .timeout_no_ack, .defensive { color:var(--bad); }
    .missing, .research_degraded, .skipped, .unknown, .warning, .pending, .alert, .tightened { color:var(--warn); }
    .status { padding:6px 10px; border:1px solid currentColor; border-radius:999px; font-weight:700; white-space:nowrap; }
    .priority-strip { border:1px solid var(--line); border-radius:8px; padding:14px; background:var(--soft); }
    .priority-strip h3 { margin-top:0; }
    .ok-strip { border-color:#b7e4dc; background:#effaf8; }
    .risk-list { display:grid; gap:8px; margin:8px 0 0; padding:0; list-style:none; }
    .risk-list li { display:grid; grid-template-columns:92px 160px 1fr; gap:10px; align-items:start; border:1px solid var(--line); border-left:4px solid currentColor; border-radius:8px; padding:10px; background:#fff; }
    .risk-list span { font-weight:700; font-size:12px; }
    .risk-list strong { color:var(--ink); }
    .risk-list p { margin:0; overflow-wrap:anywhere; }
    .visual-grid { display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:12px; }
    .window-grid { display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); gap:12px; }
    .operator-window { min-width:0; border:1px solid var(--line); border-radius:8px; padding:14px; background:#fff; box-shadow:0 1px 0 rgba(17,24,39,.03); }
    .window-title { display:flex; align-items:flex-start; justify-content:space-between; gap:12px; margin-bottom:10px; border-bottom:1px solid #edf0f4; padding-bottom:8px; }
    .window-title h3 { margin:0; }
    .window-title span { color:var(--muted); font-size:12px; white-space:nowrap; }
    .operator-window h4 { margin:14px 0 8px; font-size:13px; }
    .mini-chart { margin-top:12px; }
    .mini-chart .chart-card { border:0; padding:0; }
    .mini-chart .chart-card h3 { font-size:13px; }
    .chart-card { min-width:0; border:1px solid var(--line); border-radius:8px; padding:14px; background:#fff; }
    .chart-card h3 { margin-top:0; }
    .bar-chart { display:grid; gap:9px; }
    .bar-row { display:grid; grid-template-columns:minmax(92px,150px) minmax(120px,1fr) minmax(70px,max-content); gap:10px; align-items:center; }
    .bar-row span { color:var(--muted); overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
    .bar-row strong { text-align:right; font-variant-numeric:tabular-nums; }
    .bar-track { height:10px; background:#edf0f4; border-radius:999px; overflow:hidden; }
    .bar-fill { height:100%; min-width:2px; background:var(--info); border-radius:999px; }
    .bar-fill.ok, .bar-fill.healthy, .bar-fill.accepted, .bar-fill.reconciled { background:var(--ok); }
    .bar-fill.stale, .bar-fill.failed, .bar-fill.rejected, .bar-fill.timeout_no_ack { background:var(--bad); }
    .bar-fill.missing, .bar-fill.unknown, .bar-fill.pending { background:var(--warn); }
    .line-chart { width:100%; height:auto; display:block; }
    .plot-bg { fill:#fafbfc; stroke:#edf0f4; }
    .zero-line { stroke:#cbd5e1; stroke-dasharray:4 4; }
    .line-path { fill:none; stroke:var(--info); stroke-width:3; stroke-linecap:round; stroke-linejoin:round; }
    .line-dot { fill:var(--info); }
    .axis-label { fill:var(--muted); font-size:11px; }
    .axis-label.end { text-anchor:end; }
    details.detail-panel { padding:0; overflow:hidden; }
    details.detail-panel summary { cursor:pointer; display:flex; align-items:center; justify-content:space-between; gap:16px; padding:16px 18px; list-style:none; border-bottom:1px solid transparent; }
    details.detail-panel summary::-webkit-details-marker { display:none; }
    details.detail-panel summary span { color:var(--muted); font-size:13px; }
    details.detail-panel[open] summary { border-bottom-color:var(--line); }
    .detail-body { padding:18px; min-width:0; }
    .kv { display:flex; justify-content:space-between; gap:12px; padding:7px 0; border-bottom:1px solid #edf0f4; }
    .kv span { color:var(--muted); }
    .kv strong { text-align:right; overflow-wrap:anywhere; }
    .table-wrap { overflow:auto; max-width:100%; max-height:58vh; border:1px solid var(--line); border-radius:8px; }
    table { width:100%; border-collapse:separate; border-spacing:0; min-width:min(960px, 100%); }
    th, td { text-align:left; padding:9px 10px; border-bottom:1px solid #edf0f4; vertical-align:top; }
    th { position:sticky; top:0; z-index:1; color:var(--muted); font-weight:600; background:#fafbfc; }
    td { max-width:260px; overflow-wrap:anywhere; }
    .weight-source-contract .table-wrap { max-height:320px; }
    .weight-executable { color:var(--ok); font-weight:800; }
    .weight-advisory { color:var(--info); font-style:italic; }
    .weight-reference { color:var(--muted); }
    .weight-unknown { color:var(--warn); }
    ul { margin:8px 0 0; padding-left:20px; }
    @media (max-width: 1280px) { .metric-grid { grid-template-columns:repeat(4,minmax(0,1fr)); } .cockpit-grid { grid-template-columns:1fr; } }
    @media (max-width: 1180px) { .command-grid, .dashboard-focus, .visual-grid, .window-grid { grid-template-columns:repeat(2,minmax(0,1fr)); } .top-status-bar { align-items:flex-start; flex-direction:column; } .top-status-pills { justify-content:flex-start; width:100%; } }
    @media (max-width: 720px) { header { align-items:flex-start; flex-direction:column; padding:18px; } .dashboard-shell { padding:16px; } .section-heading, details.detail-panel summary, .window-title { align-items:flex-start; flex-direction:column; } .metric-grid, .command-grid, .dashboard-focus, .visual-grid, .window-grid, .risk-list li, .split-grid, .alpha-summary-grid, .gauge-row { grid-template-columns:1fr; } .bar-row { grid-template-columns:1fr; } }
    .top-status-bar { position:sticky; top:0; z-index:8; display:flex; align-items:center; gap:16px; min-width:0; padding:10px clamp(16px,3vw,36px); border-bottom:1px solid #1e293b; background:#080e1a; color:#e2e8f0; }
    .brand-mark { width:24px; height:24px; border-radius:6px; display:grid; place-items:center; flex:0 0 auto; color:#fff; font-weight:900; background:linear-gradient(135deg,#6366f1,#10b981); }
    .top-status-main { min-width:180px; }
    .top-status-title { font-size:13px; font-weight:800; letter-spacing:.08em; text-transform:uppercase; }
    .top-status-sub { color:#64748b; font-size:11px; }
    .top-status-pills { display:flex; align-items:center; justify-content:flex-end; gap:8px; flex:1; min-width:0; overflow:auto; }
    .top-pill { display:flex; align-items:center; gap:6px; flex:0 0 auto; padding:4px 8px; border:1px solid currentColor; border-radius:6px; background:rgba(255,255,255,.03); }
    .top-pill span { color:#64748b; font-size:10px; text-transform:uppercase; letter-spacing:.06em; }
    .top-pill strong { color:currentColor; font-size:11px; max-width:130px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
    .metric-grid { display:grid; grid-template-columns:repeat(8,minmax(0,1fr)); gap:8px; }
    .metric-card { min-width:0; border:1px solid #1e293b; border-radius:8px; padding:12px 14px; background:#0f172a; }
    .metric-label { color:#64748b; font-size:10px; font-weight:700; letter-spacing:.06em; text-transform:uppercase; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
    .metric-main { display:flex; align-items:center; gap:7px; margin-top:5px; }
    .metric-main strong { color:#e2e8f0; font-size:18px; font-weight:800; font-variant-numeric:tabular-nums; min-width:0; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
    .metric-sub { margin-top:3px; color:#64748b; font-size:10px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
    .status-dot { width:8px; height:8px; border-radius:50%; display:inline-block; flex:0 0 auto; background:#94a3b8; box-shadow:0 0 8px rgba(148,163,184,.35); }
    .status-dot.ok, .status-dot.healthy, .status-dot.closed, .status-dot.pass, .status-dot.approved, .status-dot.idle, .status-dot.reconciled { background:#10b981; box-shadow:0 0 8px rgba(16,185,129,.45); }
    .status-dot.warn, .status-dot.warning, .status-dot.alert, .status-dot.pending, .status-dot.partial, .status-dot.tightened { background:#f59e0b; box-shadow:0 0 8px rgba(245,158,11,.45); }
    .status-dot.error, .status-dot.blocked, .status-dot.rejected, .status-dot.defensive, .status-dot.timeout_no_ack, .status-dot.execution_blocked { background:#ef4444; box-shadow:0 0 8px rgba(239,68,68,.45); }
    .status-dot.info, .status-dot.observe { background:#6366f1; box-shadow:0 0 8px rgba(99,102,241,.45); }
    .cockpit-grid { display:grid; grid-template-columns:minmax(260px,1fr) minmax(360px,1.4fr) minmax(280px,1fr); gap:14px; }
    .cockpit-panel { min-width:0; border:1px solid #1e293b; border-radius:10px; padding:16px; background:#0c1524; color:#e2e8f0; }
    .cockpit-panel .kv { border-bottom-color:#1e293b; }
    .cockpit-panel .kv span { color:#64748b; }
    .cockpit-panel .kv strong { color:#cbd5e1; }
    .priority-panel .priority-strip { padding:0; border:0; background:transparent; }
    .priority-panel .priority-strip > h3 { display:none; }
    .priority-panel .risk-list li { border-color:#1e293b; background:#080e1a; }
    .priority-panel .risk-list li { grid-template-columns:84px 1fr; gap:6px 10px; }
    .priority-panel .risk-list li p { grid-column:1 / -1; }
    .account-execution-panel .chart-card, .alpha-panel .chart-card { border-color:#1e293b; background:#080e1a; color:#e2e8f0; }
    .account-execution-panel .kv strong { white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
    .gauge-row { display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); gap:12px; margin-bottom:12px; }
    .arc-gauge { display:grid; justify-items:center; gap:2px; min-width:0; color:#6366f1; }
    .arc-gauge.ok { color:#10b981; }
    .arc-gauge.warn { color:#f59e0b; }
    .arc-gauge.error { color:#ef4444; }
    .arc-gauge svg { width:74px; height:44px; display:block; }
    .arc-bg { fill:none; stroke:#1e293b; stroke-width:5; stroke-linecap:round; }
    .arc-fg { fill:none; stroke:currentColor; stroke-width:5; stroke-linecap:round; }
    .arc-gauge text { fill:#e2e8f0; font-size:10px; font-weight:800; font-family:monospace; }
    .arc-label { color:#64748b; font-size:9px; font-weight:700; letter-spacing:.06em; text-transform:uppercase; }
    .arc-sub { color:#475569; font-size:9px; text-align:center; min-height:12px; }
    .split-grid { display:grid; grid-template-columns:1fr 1fr; gap:12px; }
    .alpha-summary-grid { display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); gap:8px; margin-bottom:10px; }
    .stack-chart { height:72px; display:flex; align-items:flex-end; gap:6px; margin-top:8px; }
    .stack-bar { flex:1; min-width:12px; display:flex; flex-direction:column; align-items:center; gap:3px; }
    .stack-segments { width:100%; max-width:24px; display:flex; flex-direction:column; justify-content:flex-end; overflow:hidden; border-radius:3px; background:#111827; }
    .seg { display:block; width:100%; min-height:1px; }
    .seg.beta, .legend .beta { background:#334155; }
    .seg.factor, .legend .factor { background:#6366f1; }
    .seg.residual, .legend .residual { background:#10b981; }
    .stack-bar em { color:#64748b; font-size:9px; font-style:normal; max-width:44px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
    .legend { display:flex; align-items:center; gap:8px; margin-top:7px; color:#64748b; font-size:10px; }
    .legend span { width:8px; height:8px; border-radius:2px; display:inline-block; }
    .account-holdings-panel { display:grid; gap:14px; background:#07090f; color:#c8d8ec; border-color:#1e2535; }
    .account-holdings-panel .section-heading { margin-bottom:0; }
    .account-holdings-panel .section-heading h2 { color:#e8f2ff; }
    .account-holdings-panel .section-heading p { color:#6b7e9a; }
    .account-top-strip { display:grid; grid-template-columns:repeat(8,minmax(0,1fr)); gap:0; border:1px solid #1e2535; border-radius:8px; background:#0c1018; overflow:hidden; }
    .account-stat { min-width:0; padding:12px 14px; border-right:1px solid #1e2535; }
    .account-stat:last-child { border-right:0; }
    .account-stat span { display:block; color:#4a5f7a; font-size:9px; font-weight:800; text-transform:uppercase; letter-spacing:.07em; }
    .account-stat strong { display:block; margin-top:3px; color:#c8d8ec; font-size:15px; font-weight:800; font-variant-numeric:tabular-nums; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
    .account-stat.ok strong, .account-stat.positive strong { color:#22d3a0; }
    .account-stat.warn strong, .account-stat.warning strong { color:#f5c842; }
    .account-stat.negative strong { color:#f04a5a; }
    .account-truth-strip { display:grid; grid-template-columns:1.25fr 1fr 1fr; gap:10px; }
    .account-truth-question { min-width:0; border:1px solid #1e2535; border-radius:8px; padding:12px 14px; background:#0c1018; }
    .account-truth-question span { display:block; color:#4a5f7a; font-size:9px; font-weight:800; letter-spacing:.07em; text-transform:uppercase; }
    .account-truth-question strong { display:block; margin-top:4px; color:#c8d8ec; font-size:14px; font-family:monospace; overflow-wrap:anywhere; }
    .account-truth-question em { display:block; margin-top:3px; color:#6b7e9a; font-size:10px; font-style:normal; overflow-wrap:anywhere; }
    .account-chart-grid { display:grid; grid-template-columns:minmax(320px,1.55fr) minmax(260px,1fr) minmax(280px,1fr); gap:14px; }
    .account-card { min-width:0; border:1px solid #1e2535; border-radius:8px; padding:15px; background:#0c1018; }
    .account-card-title { display:flex; align-items:center; justify-content:space-between; gap:12px; margin-bottom:10px; }
    .account-card-title h3 { margin:0; color:#8fa3c0; font-size:10px; font-weight:800; text-transform:uppercase; letter-spacing:.08em; }
    .account-card-title span { color:#4a5f7a; font-size:10px; font-family:monospace; white-space:nowrap; }
    .account-nav-chart, .account-pnl-chart { display:block; width:100%; height:auto; overflow:visible; }
    .account-axis-labels text, .account-pnl-chart text { fill:#4a5f7a; font-size:8px; font-family:monospace; }
    .account-zero-line { stroke:#2a3348; stroke-width:1; }
    .chart-hit-point { fill:transparent; pointer-events:all; }
    .contrib-bars { display:grid; gap:4px; }
    .contrib-row { display:grid; grid-template-columns:42px minmax(120px,1fr) 56px; gap:7px; align-items:center; min-width:0; }
    .contrib-row strong { color:#c8d8ec; font-size:10px; font-family:monospace; text-align:right; }
    .contrib-track { position:relative; height:15px; min-width:0; }
    .contrib-midline { position:absolute; left:50%; top:0; width:1px; height:100%; background:#2a3348; }
    .contrib-bar { position:absolute; top:50%; transform:translateY(-50%); height:8px; min-width:1px; border-radius:2px; }
    .contrib-bar.positive { background:#22d3a0cc; }
    .contrib-bar.negative { background:#f04a5acc; }
    .contrib-row em { color:#6b7e9a; font-size:10px; font-family:monospace; font-style:normal; text-align:right; }
    .account-holdings-panel .positive { color:#22d3a0 !important; }
    .account-holdings-panel .negative { color:#f04a5a !important; }
    .account-holdings-panel .warning { color:#f5c842 !important; }
    .account-holdings-panel .neutral { color:#8fa3c0 !important; }
    .account-signal-grid { display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:10px; }
    .account-signal-card { min-width:0; border:1px solid #1e2535; border-radius:8px; padding:12px 14px; background:#0c1018; }
    .account-signal-card span { display:block; color:#4a5f7a; font-size:9px; font-weight:800; letter-spacing:.07em; text-transform:uppercase; }
    .account-signal-card strong { display:block; margin-top:4px; font-size:18px; font-family:monospace; color:#c8d8ec; }
    .account-signal-card em { display:block; margin-top:2px; color:#6b7e9a; font-size:10px; font-style:normal; }
    .account-signal-card.ok strong { color:#22d3a0; }
    .account-signal-card.bad strong { color:#f04a5a; }
    .account-signal-card.warn strong { color:#f5c842; }
    .account-signal-card.info strong { color:#4a9eff; }
    .holdings-sort-controls { display:flex; align-items:center; gap:7px; flex-wrap:wrap; margin-bottom:10px; }
    .holdings-sort-controls span { color:#4a5f7a; font-size:9px; font-weight:800; letter-spacing:.07em; text-transform:uppercase; }
    .holdings-sort-controls button, .account-holdings-table th button { appearance:none; border:1px solid #1e2535; border-radius:4px; background:#111620; color:#8fa3c0; cursor:pointer; font:600 10px/1.2 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif; padding:4px 8px; }
    .holdings-sort-controls button.active, .account-holdings-table th button.active { border-color:#4a9eff; color:#4a9eff; background:#4a9eff1f; }
    .account-table-wrap { overflow:auto; max-height:62vh; border:1px solid #1e2535; border-radius:8px; }
    .account-holdings-table { min-width:1220px; width:100%; border-collapse:separate; border-spacing:0; }
    .account-holdings-table th, .account-holdings-table td { border-bottom:1px solid #1e2535; padding:8px 10px; color:#8fa3c0; background:#0c1018; }
    .account-holdings-table th { position:sticky; top:0; z-index:2; background:#111620; }
    .account-holdings-table tr:nth-child(even) td { background:#0f141d; }
    .account-holdings-table td strong { color:#e8f2ff; font-family:monospace; font-size:12px; }
    .account-holdings-table .num { text-align:right; font-family:monospace; font-variant-numeric:tabular-nums; }
    .muted-cell { color:#6b7e9a !important; }
    .role-chip { display:inline-block; width:8px; height:8px; border-radius:2px; margin-right:8px; background:#6b7e9a; }
    .role-chip.core { background:#4a9eff; }
    .role-chip.sector { background:#9b7eff; }
    .role-chip.thematic { background:#22d3a0; }
    .role-chip.satellite { background:#6b7e9a; }
    .role-chip.hedge { background:#f04a5a; }
    .role-chip.cash { background:#2a3348; }
    .action-label { color:#8fa3c0; font-size:9px; font-weight:800; letter-spacing:.04em; text-transform:uppercase; }
    .action-label.supported_winner { color:#22d3a0; }
    .action-label.loss_review, .action-label.hard_risk_review, .action-label.forced_trim { color:#f04a5a; }
    .action-label.trim_candidate, .action-label.trim_review, .action-label.no_add { color:#f5c842; }
    .account-contract-note { margin-top:9px; color:#4a5f7a; font-size:10px; }
    @media (max-width: 1180px) { .account-top-strip { grid-template-columns:repeat(4,minmax(0,1fr)); } .account-truth-strip, .account-chart-grid { grid-template-columns:1fr; } }
    @media (max-width: 720px) { .account-top-strip, .account-signal-grid { grid-template-columns:1fr 1fr; } .account-stat { border-right:0; border-bottom:1px solid #1e2535; } }
    """
