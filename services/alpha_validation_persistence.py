"""Persist alpha validation diagnostics for trend analysis.

This layer is analytics-only. It never mutates targets or changes execution
authority.
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Any


def build_alpha_validation_run_record(
    *,
    analysis_id: int | None,
    analyzed_at: datetime | None,
    trigger_type: str | None,
    risk_out: dict[str, Any],
    evidence_bundle: dict[str, Any] | None = None,
    execution_status: str | None = None,
) -> dict[str, Any]:
    """Build a deterministic DB record from current pipeline diagnostics."""
    risk = risk_out or {}
    evidence = evidence_bundle or {}
    strategies = evidence.get("strategies") if isinstance(evidence.get("strategies"), dict) else {}
    cost = _cost_summary(risk.get("transaction_cost_gate") or {})
    portfolio_risk = _portfolio_risk_summary(risk.get("portfolio_risk_diagnostic") or {})
    construction = _construction_summary(
        risk.get("portfolio_construction_candidate")
        or risk.get("portfolio_construction_shadow")
        or {}
    )
    diversity = _strategy_diversity_summary(strategies)
    conviction = _conviction_status_summary(strategies)
    warnings = _warnings(
        cost=cost,
        portfolio_risk=portfolio_risk,
        construction=construction,
        diversity=diversity,
        risk_out=risk,
    )
    status = _status_from_warnings(warnings, cost, portfolio_risk, construction, diversity)
    data_quality = _data_quality(cost, portfolio_risk, construction, diversity)
    generated_at = datetime.utcnow()
    diagnostic_payload = {
        "transaction_cost_gate": cost.get("raw"),
        "portfolio_risk_diagnostic": portfolio_risk.get("raw"),
        "portfolio_construction": construction.get("raw"),
        "strategy_diversity": diversity.get("raw"),
        "conviction_status_counts": conviction,
        "final_validation": risk.get("final_validation") or {},
    }
    record = {
        "analysis_id": analysis_id,
        "generated_at": generated_at,
        "analyzed_at": analyzed_at,
        "trigger_type": trigger_type,
        "risk_approved": bool(risk.get("approved")),
        "execution_status": execution_status,
        "status": status,
        "data_quality": data_quality,
        "cost_gate_status": cost.get("status"),
        "low_edge_trade_count": int(cost.get("low_edge_trade_count") or 0),
        "min_edge_to_cost_ratio": cost.get("min_edge_to_cost_ratio"),
        "avg_edge_to_cost_ratio": cost.get("avg_edge_to_cost_ratio"),
        "var_95_loss": portfolio_risk.get("var_95_loss"),
        "cvar_95_loss": portfolio_risk.get("cvar_95_loss"),
        "max_scenario_loss": portfolio_risk.get("max_scenario_loss"),
        "signal_weighted_effective_n": construction.get("signal_weighted_effective_n"),
        "signal_alignment_score": construction.get("signal_alignment_score"),
        "signal_objective_warning_count": int(construction.get("warning_count") or 0),
        "independent_alpha_family_count": int(diversity.get("independent_alpha_family_count") or 0),
        "actionable_alpha_strategy_count": int(diversity.get("actionable_alpha_strategy_count") or 0),
        "calibrated_conviction_count": int(conviction.get("calibrated") or 0),
        "early_conviction_count": int(conviction.get("early_estimate") or 0),
        "insufficient_conviction_count": int(conviction.get("insufficient_samples") or 0),
        "warnings": warnings,
        "diagnostic_payload": diagnostic_payload,
    }
    record["content_hash"] = _content_hash({
        key: value
        for key, value in record.items()
        if key not in {"generated_at", "content_hash"}
    })
    return record


async def persist_alpha_validation_run(
    db: Any,
    *,
    analysis_id: int,
    analyzed_at: datetime | None,
    trigger_type: str | None,
    risk_out: dict[str, Any],
    evidence_bundle: dict[str, Any] | None = None,
    execution_status: str | None = None,
) -> dict[str, Any]:
    """Upsert one alpha validation snapshot for an analysis."""
    from sqlalchemy.dialects.postgresql import insert

    from db.models import AlphaValidationRun

    record = build_alpha_validation_run_record(
        analysis_id=analysis_id,
        analyzed_at=analyzed_at,
        trigger_type=trigger_type,
        risk_out=risk_out,
        evidence_bundle=evidence_bundle,
        execution_status=execution_status,
    )
    stmt = insert(AlphaValidationRun).values(record)
    update_cols = {
        key: getattr(stmt.excluded, key)
        for key in record
        if key not in {"id", "analysis_id", "created_at"}
    }
    stmt = stmt.on_conflict_do_update(
        constraint="uq_alpha_validation_runs_analysis_id",
        set_=update_cols,
    )
    await db.execute(stmt)
    await db.commit()
    return _json_safe_record(record)


def _cost_summary(gate: dict[str, Any]) -> dict[str, Any]:
    rows = [row for row in gate.get("rows") or [] if isinstance(row, dict)]
    buy_rows = [row for row in rows if row.get("trade_action") == "buy"]
    ratios = [
        _to_float(row.get("edge_to_cost_ratio"))
        for row in buy_rows
        if _to_float(row.get("edge_to_cost_ratio")) is not None
    ]
    low_edge = [
        row for row in buy_rows
        if row.get("verdict") in {"low_edge_to_cost", "missing_signal_edge"}
    ]
    return {
        "status": gate.get("status"),
        "low_edge_trade_count": len(low_edge),
        "min_edge_to_cost_ratio": round(min(ratios), 6) if ratios else None,
        "avg_edge_to_cost_ratio": round(sum(ratios) / len(ratios), 6) if ratios else None,
        "warnings": gate.get("warnings") or [],
        "raw": gate,
    }


def _portfolio_risk_summary(diagnostic: dict[str, Any]) -> dict[str, Any]:
    summary = diagnostic.get("summary") if isinstance(diagnostic.get("summary"), dict) else {}
    return {
        "status": diagnostic.get("status"),
        "data_quality": diagnostic.get("data_quality"),
        "var_95_loss": _to_float(summary.get("target_var_95_loss")),
        "cvar_95_loss": _to_float(summary.get("target_cvar_95_loss")),
        "max_scenario_loss": _to_float(summary.get("max_target_scenario_loss")),
        "warnings": diagnostic.get("warnings") or [],
        "raw": diagnostic,
    }


def _construction_summary(payload: dict[str, Any]) -> dict[str, Any]:
    metrics = (
        payload.get("signal_objective_metrics")
        if isinstance(payload.get("signal_objective_metrics"), dict)
        else {}
    )
    after = metrics.get("after") if isinstance(metrics.get("after"), dict) else {}
    warnings = metrics.get("warnings") or (payload.get("diagnostics") or {}).get("signal_objective_warnings") or []
    return {
        "signal_weighted_effective_n": _to_float(
            payload.get("signal_weighted_effective_n_after")
            if payload.get("signal_weighted_effective_n_after") is not None
            else after.get("signal_weighted_effective_n")
        ),
        "signal_alignment_score": _to_float(
            payload.get("signal_alignment_score_after")
            if payload.get("signal_alignment_score_after") is not None
            else after.get("signal_alignment_score")
        ),
        "warning_count": len(warnings),
        "warnings": warnings,
        "raw": payload,
    }


def _strategy_diversity_summary(strategies: dict[str, Any]) -> dict[str, Any]:
    diversity = (
        strategies.get("strategy_diversity")
        if isinstance(strategies.get("strategy_diversity"), dict)
        else {}
    )
    return {
        "independent_alpha_family_count": int(diversity.get("independent_alpha_family_count") or 0),
        "actionable_alpha_strategy_count": int(diversity.get("actionable_alpha_strategy_count") or 0),
        "warnings": diversity.get("warnings") or [],
        "raw": diversity,
    }


def _conviction_status_summary(strategies: dict[str, Any]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for result in strategies.get("strategy_results") or []:
        if not isinstance(result, dict):
            continue
        for card in result.get("evidence_cards") or []:
            if not isinstance(card, dict):
                continue
            status = str(card.get("conviction_status") or "missing_profile")
            counts[status] = counts.get(status, 0) + 1
    return counts


def _warnings(
    *,
    cost: dict[str, Any],
    portfolio_risk: dict[str, Any],
    construction: dict[str, Any],
    diversity: dict[str, Any],
    risk_out: dict[str, Any],
) -> list[str]:
    out: list[str] = []
    out.extend(f"cost:{item}" for item in cost.get("warnings") or [])
    out.extend(f"risk:{item}" for item in portfolio_risk.get("warnings") or [])
    out.extend(f"construction:{item}" for item in construction.get("warnings") or [])
    out.extend(f"diversity:{item}" for item in diversity.get("warnings") or [])
    final_validation = risk_out.get("final_validation") or {}
    if final_validation and not final_validation.get("approved", True):
        out.append("final_validation:not_approved")
    return sorted(set(str(item) for item in out if item))


def _status_from_warnings(
    warnings: list[str],
    cost: dict[str, Any],
    portfolio_risk: dict[str, Any],
    construction: dict[str, Any],
    diversity: dict[str, Any],
) -> str:
    has_any = any(
        item.get("raw")
        for item in (cost, portfolio_risk, construction, diversity)
    )
    if not has_any:
        return "insufficient_data"
    if warnings:
        return "observe_warning"
    return "ok"


def _data_quality(
    cost: dict[str, Any],
    portfolio_risk: dict[str, Any],
    construction: dict[str, Any],
    diversity: dict[str, Any],
) -> str:
    if portfolio_risk.get("data_quality") in {"historical_supported", "ok"}:
        return "diagnostic_supported"
    if any(item.get("raw") for item in (cost, portfolio_risk, construction, diversity)):
        return "limited"
    return "missing"


def _content_hash(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _json_safe_record(record: dict[str, Any]) -> dict[str, Any]:
    out = dict(record)
    for key in ("generated_at", "analyzed_at"):
        if hasattr(out.get(key), "isoformat"):
            out[key] = out[key].isoformat()
    return out


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
