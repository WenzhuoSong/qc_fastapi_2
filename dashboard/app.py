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

from db.models import AgentAnalysis, AgentStepLog, CronRunLog, ExecutionLog, QCSnapshot, SystemConfig
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
    cron_runs = await _latest_cron_runs()
    data_quality_audit = await _data_quality_audit_trend()
    execution = await _latest_execution()
    replay = await _replay_diagnostics()
    config = await _dashboard_config()
    return {
        "generated_at": datetime.utcnow().isoformat(),
        "ops": ops,
        "latest_analysis": latest_analysis,
        "portfolio_construction_readiness": pc_readiness,
        "cron_runs": cron_runs,
        "data_quality_audit": data_quality_audit,
        "execution": execution,
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
        "position_governance": _compact_governance(governance, ledger),
        "decision_ledger": compact_ledger,
        "portfolio_construction_evaluation": _compact_portfolio_construction_evaluation(
            (risk.get("portfolio_construction_evaluation") if isinstance(risk, dict) else None) or {}
        ),
        "portfolio_construction_promotion_gate": _compact_portfolio_construction_promotion_gate(
            (risk.get("portfolio_construction_promotion_gate") if isinstance(risk, dict) else None) or {}
        ),
        "stage_metrics": stage_metrics,
        "rejection_reasons": (risk.get("rejection_reasons") if isinstance(risk, dict) else []) or [],
    }


async def _portfolio_construction_readiness() -> dict[str, Any]:
    try:
        from services.portfolio_construction_evaluator import load_portfolio_construction_readiness

        return await load_portfolio_construction_readiness(limit=20, min_cycles=20, min_pass_rate=0.80)
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
        "command_type": row.command_type,
        "status": row.status,
        "retry_count": row.retry_count,
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
    return {
        "playground_config": (playground.value if playground else {}) or {},
        "circuit_state": (circuit.value if circuit else {}) or {},
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
    return {
        "status": evaluation.get("status"),
        "promotion_ready": evaluation.get("promotion_ready"),
        "execution_authority": evaluation.get("execution_authority"),
        "blockers": evaluation.get("blockers") or [],
        "warnings": evaluation.get("warnings") or [],
        "mean_abs_weight_deviation": metrics.get("mean_abs_weight_deviation"),
        "turnover_delta": metrics.get("turnover_delta"),
        "shadow_policy_allowed": metrics.get("shadow_policy_allowed"),
        "actual_policy_allowed": metrics.get("actual_policy_allowed"),
        "shadow_high_risk_tickers_added": metrics.get("shadow_high_risk_tickers_added") or [],
    }


def _compact_portfolio_construction_promotion_gate(gate: dict[str, Any]) -> dict[str, Any]:
    if not gate:
        return {}
    return {
        "status": gate.get("status"),
        "eligible": gate.get("eligible"),
        "enabled": gate.get("enabled"),
        "approval_mode": gate.get("approval_mode"),
        "blockers": gate.get("blockers") or [],
        "would_promote_to": gate.get("would_promote_to"),
        "execution_authority": gate.get("execution_authority"),
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
      <h3>Manual Review Hints</h3>{_render_table(hints, ["ticker", "suggested_action", "current_weight", "suggested_target", "delta"])}
      <h3>Thesis Problems</h3>{_render_table(thesis, ["ticker", "status", "validator"])}
      <h3>Position Explanations</h3>{_render_table(governance.get("position_explanations") or [], ["ticker", "position_state", "decision", "current_weight", "target_after", "unrealized_pnl_pct", "risk_budget_status", "strategy_support", "action_permission", "strategy_intent", "llm_effect", "construction_effect", "risk_governance_effect", "final_explanation", "why_hold", "why_not_add", "why_not_exit", "next_trigger"])}
      <h3>Decision Ledger</h3>{_render_table((latest.get("decision_ledger") or {}).get("top_decisions") or [], ["ticker", "proposed_action", "final_action", "execution_status", "qc_status", "qc_rejection_reason", "risk_result", "ticker_role", "single_cap", "group_cap", "policy_version", "policy_cap_applied", "policy_cap_original", "cash_raised_by_policy_cap", "entered_via_hedge_path", "hedge_trigger_reasons", "final_target", "target_builder_target", "diagnostic_llm_target", "validated_advisory_delta", "advisory_validator_result", "changed_by"])}
      <h3>Pipeline Stage Telemetry</h3>{_render_table(latest.get("stage_metrics") or [], ["stage", "agent", "duration_ms", "model", "prompt_tokens", "completion_tokens", "failed"])}
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
