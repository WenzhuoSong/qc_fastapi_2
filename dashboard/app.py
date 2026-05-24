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
    CommandLifecycleEvent,
    CronRunLog,
    ExecutionLog,
    QCSnapshot,
    SystemConfig,
)
from db.session import AsyncSessionLocal
from services.operational_health import build_operational_health_snapshot
from services.playground import _recent_snapshot_row_limit

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
    live_signal_conviction = await _live_signal_conviction_dashboard()
    cron_runs = await _latest_cron_runs()
    data_quality_audit = await _data_quality_audit_trend()
    execution = await _latest_execution()
    execution_control = await _execution_control_status(latest_analysis)
    replay = await _replay_diagnostics()
    return {
        "generated_at": datetime.utcnow().isoformat(),
        "ops": ops,
        "latest_analysis": latest_analysis,
        "portfolio_construction_objective": pc_objective,
        "portfolio_construction_readiness": pc_readiness,
        "strategy_evidence": strategy_evidence,
        "live_signal_conviction": live_signal_conviction,
        "cron_runs": cron_runs,
        "data_quality_audit": data_quality_audit,
        "execution": execution,
        "execution_control": execution_control,
        "replay": replay,
        "config": config,
    }


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
        "final_validation": _compact_final_validation(
            (risk.get("final_validation") if isinstance(risk, dict) else None) or {}
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
    except Exception as exc:
        return {
            "available": False,
            "reason": f"{type(exc).__name__}: {exc}",
            "account_state_guard": latest_analysis.get("account_state_guard") or {},
            "auto_pause": latest_analysis.get("auto_pause") or {},
            "latest_account_snapshot": {},
            "recent_command_events": [],
            "recent_commands": [],
        }

    return {
        "available": True,
        "account_state_guard": latest_analysis.get("account_state_guard") or {},
        "auto_pause": latest_analysis.get("auto_pause") or {},
        "latest_account_snapshot": _compact_account_state_snapshot(snapshot),
        "recent_command_events": [_compact_lifecycle_event(row) for row in lifecycle_events],
        "recent_commands": [_compact_execution_row(row) for row in recent_commands],
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
    return {
        "playground_config": (playground.value if playground else {}) or {},
        "circuit_state": (circuit.value if circuit else {}) or {},
        "portfolio_construction_promotion_config": (pc_promotion.value if pc_promotion else {}) or {},
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
        "shadow_high_risk_tickers_added": metrics.get("shadow_high_risk_tickers_added") or [],
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
        "turnover": turnover,
        "turnover_budget": turnover.get("budget") if turnover else objective.get("turnover_budget"),
        "turnover_estimated": turnover.get("estimated"),
        "turnover_before_budget": turnover.get("estimated_before_budget"),
        "turnover_within_budget": turnover.get("within_budget"),
        "basket_limit_multiplier": diagnostics.get("basket_limit_multiplier"),
        "active_basket_reviews": diagnostics.get("active_basket_reviews") or [],
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
        "readiness": _compact_pc_readiness(readiness),
        "promotion_gate": gate,
        "evaluation": evaluation,
        "construction_steps": payload.get("construction_steps") or [],
        "violations": payload.get("violations") or [],
        "factor_exposure_rows": payload.get("factor_exposure_rows") or [],
        "basket_exposure_rows": payload.get("basket_exposure_rows") or [],
        "target_weight_rows": payload.get("target_weight_rows") or [],
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
        "cycles": readiness.get("cycles"),
        "ready_count": readiness.get("ready_count"),
        "pass_rate": readiness.get("pass_rate"),
        "min_cycles": readiness.get("min_cycles"),
        "min_pass_rate": readiness.get("min_pass_rate"),
        "blocker_counts": readiness.get("blocker_counts") or {},
        "warning_counts": readiness.get("warning_counts") or {},
        "mean_abs_weight_deviation_avg": readiness.get("mean_abs_weight_deviation_avg"),
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
            "data_ready": row.get("data_ready"),
            "can_influence_allocation": row.get("can_influence_allocation"),
            "suggested_use": row.get("suggested_use"),
            "confidence_score": row.get("confidence_score"),
            "selected_tickers": row.get("selected_tickers") or [],
            "evidence_contract_version": row.get("evidence_contract_version"),
            "cards_generated": summary.get("cards_generated", len(cards)),
            "missing_mapping_count": summary.get("missing_mapping_count"),
            "fallback_count": summary.get("fallback_count"),
            "actions": summary.get("actions") or {},
            "conviction_statuses": summary.get("conviction_statuses") or {},
            "reason_codes": row.get("reason_codes") or [],
            "walk_forward_level": row.get("walk_forward_level"),
            "walk_forward_pass_rate": row.get("walk_forward_pass_rate"),
            "turnover": row.get("turnover"),
        })

    summary = _evidence_card_summary(card_rows, strategies.get("evidence_summary") or {})
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
        "strategy_rows": strategy_rows,
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


def _compact_evidence_card(card: dict[str, Any], *, fallback_strategy: str) -> dict[str, Any]:
    diagnostics = card.get("diagnostics") if isinstance(card.get("diagnostics"), dict) else {}
    threshold = diagnostics.get("threshold") if isinstance(diagnostics.get("threshold"), dict) else {}
    conviction_diag = diagnostics.get("conviction") if isinstance(diagnostics.get("conviction"), dict) else {}
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
        "mapping_role": diagnostics.get("mapping_role"),
        "threshold_gte": threshold.get("gte"),
        "threshold_lt": threshold.get("lt"),
        "weight_formula": diagnostics.get("weight_formula"),
        "base_cap": diagnostics.get("base_cap"),
        "max_weight_multiplier": diagnostics.get("max_weight_multiplier"),
        "missing_safety_fields": diagnostics.get("missing_safety_fields") or [],
        "allowed_actions": diagnostics.get("allowed_actions") or [],
        "effective_confidence_rule": conviction_diag.get("effective_confidence_rule"),
        "conviction_shadow_only": conviction_diag.get("shadow_only"),
    }


def _evidence_card_summary(cards: list[dict[str, Any]], fallback_summary: dict[str, Any]) -> dict[str, Any]:
    actions = _count_map(cards, "action")
    conviction_statuses = _count_map(cards, "conviction_status")
    warning_rows = _evidence_mapping_warning_rows(cards)
    return {
        "cards_generated": len(cards),
        "missing_mapping_count": sum(1 for row in cards if "missing_compatibility_mapping" in str(row.get("reason") or "")),
        "fallback_count": len(warning_rows),
        "actions": actions or fallback_summary.get("actions") or {},
        "conviction_statuses": conviction_statuses or fallback_summary.get("conviction_statuses") or {},
        "max_weight_by_action": _max_weight_by_action(cards),
    }


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
        "profile_count_rows": _dict_count_rows(raw.get("profile_counts") or {}, label="source_bucket"),
        "status_count_rows": _dict_count_rows(raw.get("status_counts") or {}, label="status"),
        "display_contract": {
            "conviction_number_policy": "no_naked_conviction",
            "required_context": "conviction_display + source_bucket + n + status",
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


def _dict_count_rows(counts: dict[str, Any], *, label: str) -> list[dict[str, Any]]:
    return [
        {label: key, "count": value}
        for key, value in sorted(counts.items())
    ]


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
        "holdings_count": len(holdings),
        "target_count": len(targets),
        "explicit_account_state": raw.get("explicit_account_state"),
        "warnings": raw.get("warnings") or [],
    }


def _compact_lifecycle_event(row: Any) -> dict[str, Any]:
    payload = row.payload if isinstance(row.payload, dict) else {}
    response = payload.get("qc_response") if isinstance(payload.get("qc_response"), dict) else {}
    command_payload = payload.get("command_payload") if isinstance(payload.get("command_payload"), dict) else {}
    target_weights = command_payload.get("weights") if isinstance(command_payload.get("weights"), dict) else {}
    return {
        "event_time": _iso(row.event_time),
        "command_id": row.command_id,
        "analysis_id": row.analysis_id,
        "event_type": row.event_type,
        "event_status": row.event_status,
        "source": row.source,
        "reason": row.reason or payload.get("reason") or response.get("reason"),
        "qc_status": payload.get("qc_status") or response.get("status"),
        "policy_mismatch": response.get("policy_mismatch"),
        "policy_version": response.get("policy_version") or payload.get("policy_version"),
        "target_count": len(target_weights),
        "payload_keys": sorted(payload.keys()),
    }


def _compact_execution_row(row: Any) -> dict[str, Any]:
    response = row.qc_response if isinstance(row.qc_response, dict) else {}
    return {
        "executed_at": _iso(row.executed_at),
        "command_id": row.command_id,
        "analysis_id": row.analysis_id,
        "command_type": row.command_type,
        "status": row.status,
        "qc_status": row.qc_status,
        "qc_ack_at": _iso(row.qc_ack_at),
        "qc_rejection_reason": row.qc_rejection_reason or response.get("reason"),
        "policy_mismatch": response.get("policy_mismatch"),
        "retry_count": row.retry_count,
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
            "portfolio_construction_target": lifecycle.get("portfolio_construction_target"),
            "target_builder_target": lifecycle.get("target_builder_target"),
            "diagnostic_llm_target": lifecycle.get("diagnostic_llm_target"),
            "validated_advisory_delta": lifecycle.get("validated_advisory_delta"),
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
  <main>
    <section>
      <h2>Operational Health</h2>
      <div class="grid checks">{_render_checks(ops.get("checks") or {})}</div>
      {_render_list("Execution blockers", ops.get("execution_blockers") or [])}
      {_render_list("Research degradations", ops.get("research_degradations") or [])}
    </section>

    <section>
      <h2>Latest Decision</h2>
      {_render_latest_analysis(latest)}
    </section>

    <section>
      <h2>Portfolio Construction Objective</h2>
      {_render_portfolio_construction_objective(summary.get("portfolio_construction_objective") or {})}
    </section>

    <section>
      <h2>ETF / Strategy Evidence</h2>
      {_render_strategy_evidence(summary.get("strategy_evidence") or {})}
    </section>

    <section>
      <h2>Live Signal Conviction</h2>
      {_render_live_signal_conviction(summary.get("live_signal_conviction") or {})}
    </section>

    <section>
      <h2>Portfolio Construction Readiness</h2>
      {_render_kv(pc_readiness)}
    </section>

    <section>
      <h2>Replay Diagnostics</h2>
      {_render_replay(replay)}
    </section>

    <section>
      <h2>Data Quality Audit Trend</h2>
      {_render_data_quality_audit(summary.get("data_quality_audit") or {})}
    </section>

    <section>
      <h2>Execution Control</h2>
      {_render_execution_control(summary.get("execution_control") or {})}
    </section>

    <section>
      <h2>Cron Runs</h2>
      {_render_crons(summary.get("cron_runs") or [])}
    </section>

    <section>
      <h2>Execution</h2>
      {_render_kv(summary.get("execution") or {})}
    </section>
  </main>
</body>
</html>"""
    return html


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


def _render_latest_analysis(latest: dict[str, Any]) -> str:
    if not latest.get("available"):
        return "<p class=\"muted\">No analysis available.</p>"
    scorecard = latest.get("scorecard") or {}
    governance = latest.get("position_governance") or {}
    feature_sources = latest.get("feature_source_summary") or {}
    pc_eval = latest.get("portfolio_construction_evaluation") or {}
    pc_gate = latest.get("portfolio_construction_promotion_gate") or {}
    final_validation = latest.get("final_validation") or {}
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
      <h3>Final Risk Validation</h3>{_render_kv(final_validation)}
      <h3>Manual Review Hints</h3>{_render_table(hints, ["ticker", "suggested_action", "current_weight", "suggested_target", "delta"])}
      <h3>Thesis Problems</h3>{_render_table(thesis, ["ticker", "status", "validator"])}
      <h3>Position Explanations</h3>{_render_table(governance.get("position_explanations") or [], ["ticker", "position_state", "decision", "current_weight", "target_after", "unrealized_pnl_pct", "risk_budget_status", "strategy_support", "action_permission", "strategy_intent", "llm_effect", "construction_effect", "risk_governance_effect", "final_explanation", "why_hold", "why_not_add", "why_not_exit", "next_trigger"])}
      <h3>Decision Ledger</h3>{_render_table((latest.get("decision_ledger") or {}).get("top_decisions") or [], ["ticker", "proposed_action", "final_action", "execution_status", "qc_status", "qc_rejection_reason", "risk_result", "ticker_role", "single_cap", "group_cap", "policy_version", "policy_cap_applied", "policy_cap_original", "cash_raised_by_policy_cap", "entered_via_hedge_path", "hedge_trigger_reasons", "final_target", "target_builder_target", "diagnostic_llm_target", "validated_advisory_delta", "advisory_validator_result", "changed_by"])}
      <h3>Pipeline Stage Telemetry</h3>{_render_table(latest.get("stage_metrics") or [], ["stage", "agent", "duration_ms", "model", "prompt_tokens", "completion_tokens", "failed"])}
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
      </div>
      <div class="grid">
        <article class="card"><h3>Readiness</h3>{_render_kv(pc.get("readiness") or {})}</article>
        <article class="card"><h3>Promotion Gate</h3>{_render_kv(pc.get("promotion_gate") or {})}</article>
        <article class="card"><h3>Evaluation</h3>{_render_kv(pc.get("evaluation") or {})}</article>
      </div>
      <h3>Construction Steps</h3>{_render_list("", pc.get("construction_steps") or [])}
      <h3>Violations</h3>{_render_list("", pc.get("violations") or [])}
      <h3>Factor Exposure Before / After</h3>{_render_table(pc.get("factor_exposure_rows") or [], ["factor", "before", "after", "delta"])}
      <h3>Basket Exposure Before / After</h3>{_render_table(pc.get("basket_exposure_rows") or [], ["basket", "before", "after", "delta", "limit", "reduced_limit", "violated"])}
      <h3>Target Weights</h3>{_render_table(pc.get("target_weight_rows") or [], ["ticker", "target_weight"])}
      <h3>Weight Change Reasons</h3>{_render_table(pc.get("weight_change_reasons") or [], ["ticker", "portfolio_construction_target", "target_builder_target", "final_target", "changed_by", "construction_effect", "risk_governance_effect", "final_explanation"])}
    """


def _render_strategy_evidence(evidence: dict[str, Any]) -> str:
    if not evidence.get("available"):
        reason = evidence.get("reason") or "No ETF / Strategy Evidence available."
        return f"<p class=\"muted\">{escape(str(reason))}</p>"
    summary = evidence.get("evidence_summary") or {}
    overview = {
        "playground_available": evidence.get("playground_available"),
        "generated_at": evidence.get("generated_at"),
        "data_quality": evidence.get("data_quality"),
        "regime_label": evidence.get("regime_label"),
        "strategy_count": evidence.get("strategy_count"),
        "card_count": evidence.get("card_count"),
    }
    conviction_note = {
        "display_rule": "conviction_display requires status/source/n",
        "execution_authority": "none",
        "conviction_is_shadow_only": True,
    }
    return f"""
      <div class="grid">
        <article class="card"><h3>Overview</h3>{_render_kv(overview)}</article>
        <article class="card"><h3>Evidence Summary</h3>{_render_kv(summary)}</article>
        <article class="card"><h3>Conviction Display Contract</h3>{_render_kv(conviction_note)}</article>
      </div>
      <h3>Strategies</h3>{_render_table(evidence.get("strategy_rows") or [], ["strategy", "data_ready", "can_influence_allocation", "suggested_use", "confidence_score", "selected_tickers", "evidence_contract_version", "cards_generated", "missing_mapping_count", "fallback_count", "actions", "conviction_statuses", "reason_codes", "walk_forward_level", "walk_forward_pass_rate", "turnover"])}
      <h3>EvidenceCards</h3>{_render_table(evidence.get("evidence_card_rows") or [], ["strategy", "ticker", "role", "action", "signal_type", "horizon", "confidence", "conviction_display", "conviction_status", "conviction_source_bucket", "conviction_n", "effective_confidence", "raw_score", "normalized_score", "max_reasonable_weight", "risk_budget_cost", "branch", "reason", "mapping_role", "weight_formula", "base_cap", "max_weight_multiplier", "effective_confidence_rule", "conviction_shadow_only"])}
      <h3>Mapping And Safety Warnings</h3>{_render_table(evidence.get("mapping_warning_rows") or [], ["strategy", "ticker", "role", "action", "reason", "missing_safety_fields", "allowed_actions", "conviction_status", "conviction_n"])}
      <h3>Role / Action Summary</h3>{_render_table(evidence.get("role_action_rows") or [], ["role", "action", "count", "avg_confidence", "max_reasonable_weight_max", "tickers"])}
      <h3>Conviction Status Summary</h3>{_render_table(evidence.get("conviction_status_rows") or [], ["status", "count"])}
      <h3>Warnings</h3>{_render_list("", evidence.get("warnings") or [])}
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
      <h3>Conviction Status Counts</h3>{_render_table(conviction.get("status_count_rows") or [], ["status", "count"])}
      <h3>Historical Prior Profiles</h3>{_render_table(conviction.get("historical_prior_profiles") or [], profile_columns)}
      <h3>Live Paper Profiles</h3>{_render_table(conviction.get("live_paper_profiles") or [], profile_columns)}
      <h3>Combined Profiles</h3>{_render_table(conviction.get("combined_profiles") or [], profile_columns)}
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
    return f"""
      <div class="grid">
        <article class="card"><h3>Account State Guard</h3>{_render_kv(guard, keys=["mode", "status", "allowed", "would_block", "pipeline_enforcement", "pipeline_effect_status", "execution_effect", "primary_blockers", "warnings"])}</article>
        <article class="card"><h3>Auto Pause</h3>{_render_kv(auto_pause, keys=["mode", "status", "would_pause", "should_pause", "execution_effect", "primary_trigger", "reason"])}</article>
        <article class="card"><h3>Latest Account Snapshot</h3>{_render_kv(latest_snapshot, keys=["available", "recorded_at", "account_timestamp", "source_packet_type", "contract_version", "account_status", "data_status", "policy_version", "total_value", "cash_pct", "buying_power", "open_order_count", "has_open_orders", "is_market_open", "holdings_count", "target_count", "explicit_account_state"])}</article>
      </div>
      <h3>Account Guard Checks</h3>{_render_table(guard.get("checks") or [], ["check", "pass", "actual", "threshold", "reason"])}
      <h3>Auto Pause Triggers</h3>{_render_table(auto_pause.get("triggers") or [], ["trigger", "triggered", "value", "threshold", "severity", "details"])}
      <h3>Recent QC Commands</h3>{_render_table(control.get("recent_commands") or [], ["executed_at", "command_id", "analysis_id", "command_type", "status", "qc_status", "qc_ack_at", "qc_rejection_reason", "policy_mismatch", "retry_count"])}
      <h3>Command Lifecycle Events</h3>{_render_table(control.get("recent_command_events") or [], ["event_time", "command_id", "analysis_id", "event_type", "event_status", "source", "reason", "qc_status", "policy_mismatch", "policy_version", "target_count", "payload_keys"])}
    """


def _render_crons(rows: list[dict[str, Any]]) -> str:
    return _render_table(rows, ["job_name", "status", "started_at", "duration_ms", "error_message"])


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


def _compact_json(value: Any) -> str:
    return str(value)


def _iso(value: Any) -> str | None:
    return value.isoformat() if hasattr(value, "isoformat") else None


def _css() -> str:
    return """
    :root { color-scheme: light; --bg:#f6f7f9; --ink:#111827; --muted:#6b7280; --line:#d8dde6; --card:#ffffff; --ok:#0f766e; --bad:#b42318; --warn:#a16207; }
    * { box-sizing: border-box; }
    body { margin:0; background:var(--bg); color:var(--ink); font:14px/1.45 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif; }
    header { display:flex; align-items:flex-end; justify-content:space-between; gap:24px; padding:24px 32px 16px; border-bottom:1px solid var(--line); background:#fff; }
    h1 { margin:0; font-size:24px; letter-spacing:0; }
    h2 { margin:0 0 12px; font-size:18px; }
    h3 { margin:16px 0 8px; font-size:14px; }
    p { margin:4px 0 0; }
    main { padding:24px 32px 48px; display:grid; gap:20px; }
    section { background:#fff; border:1px solid var(--line); padding:18px; border-radius:8px; }
    .grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(220px,1fr)); gap:12px; }
    .checks { grid-template-columns:repeat(auto-fit,minmax(180px,1fr)); }
    .card { border:1px solid var(--line); border-radius:8px; padding:14px; background:var(--card); min-width:0; }
    .label, .muted { color:var(--muted); }
    .metric { margin-top:6px; font-size:22px; font-weight:700; }
    .ok, .healthy, .success { color:var(--ok); }
    .stale, .failed, .execution_blocked { color:var(--bad); }
    .missing, .research_degraded, .skipped, .unknown { color:var(--warn); }
    .status { padding:6px 10px; border:1px solid currentColor; border-radius:999px; font-weight:700; white-space:nowrap; }
    .kv { display:flex; justify-content:space-between; gap:12px; padding:7px 0; border-bottom:1px solid #edf0f4; }
    .kv span { color:var(--muted); }
    .kv strong { text-align:right; overflow-wrap:anywhere; }
    .table-wrap { overflow:auto; max-height:70vh; border:1px solid var(--line); border-radius:8px; }
    table { width:100%; border-collapse:separate; border-spacing:0; min-width:1100px; }
    th, td { text-align:left; padding:9px 10px; border-bottom:1px solid #edf0f4; vertical-align:top; }
    th { position:sticky; top:0; z-index:1; color:var(--muted); font-weight:600; background:#fafbfc; }
    td { max-width:260px; overflow-wrap:anywhere; }
    ul { margin:8px 0 0; padding-left:20px; }
    @media (max-width: 720px) { header { align-items:flex-start; flex-direction:column; padding:18px; } main { padding:18px; } }
    """
