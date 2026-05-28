"""Read-only alpha decision profile diagnostics.

This layer combines conviction, attribution, cost, independence, regime, and
construction-epoch context into one operator-facing profile. It never mutates
targets and has no execution authority.
"""
from __future__ import annotations

import hashlib
import json
import math
from datetime import date, datetime, timezone
from typing import Any

from services.construction_epoch import construction_epoch_from_diagnostics
from services.conviction_decision import (
    STAT_STATUS_EARLY_SIGNAL,
    STAT_STATUS_INDICATIVE,
    STAT_STATUS_INSUFFICIENT,
    STAT_STATUS_STATISTICALLY_MEANINGFUL,
    decision_conviction_discount,
    decision_statistical_status,
)
from services.strategy_diversity import canonical_strategy_family, is_strategy_alpha_source


CONTRACT_VERSION = "alpha_decision_profiles_v1"
EXECUTION_AUTHORITY = "none"
TARGET_WEIGHT_MUTATION = "none"

SOURCE_BUCKET_PRIORITY = {
    "combined": 0,
    "live_paper": 1,
    "historical_prior": 2,
}

RESIDUAL_ALPHA_EPSILON = 0.0005
ATTRIBUTION_MIN_SAMPLE_COUNT = 4
LOW_EDGE_TO_COST_RATIO = 1.0
WATCH_EDGE_TO_COST_RATIO = 2.0
COMFORTABLE_EDGE_TO_COST_RATIO = 3.0
COST_MODEL = "IBKR_return_drag_v1"
COST_STATUS_CREDIT = {
    "ok": 1.0,
    "watch_costs": 0.6,
    "low_edge_after_cost": 0.25,
    "negative_after_cost": 0.0,
    "insufficient": 0.0,
}
COST_STATUS_RANK = {
    "negative_after_cost": 0,
    "low_edge_after_cost": 1,
    "watch_costs": 2,
    "insufficient": 3,
    "ok": 4,
}


async def load_alpha_decision_profiles(
    db: Any,
    *,
    as_of_date: date | None = None,
    row_limit: int = 5000,
    attribution_limit: int = 12,
    alpha_run_limit: int = 30,
) -> dict[str, Any]:
    """Load persisted diagnostics and build read-only alpha decision profiles."""
    from sqlalchemy import desc, func, select

    from db.models import (
        AgentAnalysis,
        AgentStepLog,
        AlphaValidationRun,
        PerformanceAttribution,
        StrategyConvictionProfile,
    )

    target_date = as_of_date or datetime.now(timezone.utc).date()
    latest_profile_date_result = await db.execute(
        select(func.max(StrategyConvictionProfile.as_of_date)).where(
            StrategyConvictionProfile.as_of_date <= target_date
        )
    )
    latest_profile_date = latest_profile_date_result.scalar_one_or_none()
    profile_rows: list[Any] = []
    if latest_profile_date is not None:
        profile_result = await db.execute(
            select(StrategyConvictionProfile)
            .where(StrategyConvictionProfile.as_of_date == latest_profile_date)
            .order_by(
                StrategyConvictionProfile.strategy_id,
                StrategyConvictionProfile.regime_at_signal,
                StrategyConvictionProfile.ticker,
                StrategyConvictionProfile.source_bucket,
            )
            .limit(max(int(row_limit or 5000), 1))
        )
        profile_rows = list(profile_result.scalars().all())

    attribution_result = await db.execute(
        select(PerformanceAttribution)
        .order_by(desc(PerformanceAttribution.period_end), desc(PerformanceAttribution.id))
        .limit(max(int(attribution_limit or 12), 1))
    )
    attribution_rows = list(attribution_result.scalars().all())

    alpha_result = await db.execute(
        select(AlphaValidationRun)
        .order_by(desc(AlphaValidationRun.generated_at), desc(AlphaValidationRun.id))
        .limit(max(int(alpha_run_limit or 30), 1))
    )
    alpha_rows = list(alpha_result.scalars().all())

    strategy_evidence: dict[str, Any] = {}
    strategy_independence: dict[str, Any] = {}
    latest_analysis_id = None
    latest_analysis = (
        await db.execute(
            select(AgentAnalysis).order_by(desc(AgentAnalysis.analyzed_at), desc(AgentAnalysis.id)).limit(1)
        )
    ).scalar_one_or_none()
    if latest_analysis is not None:
        latest_analysis_id = int(latest_analysis.id)
        step = (
            await db.execute(
                select(AgentStepLog)
                .where(AgentStepLog.analysis_id == latest_analysis_id)
                .where(AgentStepLog.stage == "2d_evidence_scorecard")
                .order_by(desc(AgentStepLog.created_at), desc(AgentStepLog.id))
                .limit(1)
            )
        ).scalar_one_or_none()
        output = step.output_data if step and isinstance(step.output_data, dict) else {}
        evidence = output.get("evidence_bundle") if isinstance(output.get("evidence_bundle"), dict) else {}
        strategy_evidence = evidence.get("strategies") if isinstance(evidence.get("strategies"), dict) else {}
        if isinstance(strategy_evidence.get("strategy_independence"), dict):
            strategy_independence = strategy_evidence["strategy_independence"]

    summary = build_alpha_decision_profiles(
        profiles=profile_rows,
        performance_attribution_rows=attribution_rows,
        alpha_validation_runs=alpha_rows,
        strategy_independence=strategy_independence,
        strategy_evidence=strategy_evidence,
        as_of_date=target_date,
    )
    if latest_profile_date is not None:
        summary["latest_profile_date"] = latest_profile_date.isoformat()
    if latest_analysis_id is not None:
        summary["latest_analysis_id"] = latest_analysis_id
    return summary


def build_alpha_decision_profiles(
    *,
    profiles: list[Any],
    performance_attribution_rows: list[Any] | None = None,
    alpha_validation_runs: list[Any] | None = None,
    strategy_independence: dict[str, Any] | None = None,
    strategy_evidence: dict[str, Any] | None = None,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    """Build read-only alpha decision profiles from existing diagnostics."""
    profile_rows = _dedupe_profiles([_profile_row(item) for item in profiles])
    attribution_rows = [_attribution_row(row) for row in (performance_attribution_rows or [])]
    alpha_rows = [_alpha_run_row(row) for row in (alpha_validation_runs or [])]
    evidence_index = _strategy_evidence_index(strategy_evidence or {})
    independence = _independence_context(strategy_independence or {})
    latest_attribution = attribution_rows[0] if attribution_rows else {}
    latest_alpha_validation = alpha_rows[0] if alpha_rows else {}

    grouped: dict[tuple[str, str, str, str], list[dict[str, Any]]] = {}
    for row in profile_rows:
        key = (
            row["strategy_id"],
            row["canonical_family"],
            row["regime"],
            row["construction_epoch_id"] or "unknown",
        )
        grouped.setdefault(key, []).append(row)

    rows = [
        _decision_profile_row(
            key=key,
            rows=items,
            evidence=evidence_index.get(key[0], {}),
            attribution=latest_attribution,
            alpha_validation=latest_alpha_validation,
            independence=independence,
            as_of_date=as_of_date,
        )
        for key, items in sorted(grouped.items())
    ]
    rows = sorted(rows, key=_decision_row_sort_key)

    status = "available" if rows else "insufficient_data"
    raw_alpha_strategy_count = len({
        row["strategy_id"] for row in rows
        if row.get("alpha_source")
    })
    independence_adjusted_count = round(
        sum(
            float(row.get("redundancy_multiplier") or 0.0)
            for row in rows
            if row.get("alpha_source")
        ),
        4,
    )
    warnings = _summary_warnings(rows, latest_attribution, latest_alpha_validation, independence)
    return {
        "contract_version": CONTRACT_VERSION,
        "status": status,
        "as_of_date": (as_of_date or datetime.now(timezone.utc).date()).isoformat(),
        "execution_authority": EXECUTION_AUTHORITY,
        "target_weight_mutation": TARGET_WEIGHT_MUTATION,
        "decision_input_only": True,
        "recommendation_only": True,
        "profile_count": len(rows),
        "source_profile_count": len(profile_rows),
        "strategy_count": len({row["strategy_id"] for row in rows if row["strategy_id"]}),
        "raw_alpha_strategy_count": raw_alpha_strategy_count,
        "independence_adjusted_strategy_count": independence_adjusted_count,
        "eligible_count": sum(1 for row in rows if row["decision_status"] == "eligible"),
        "needs_more_samples_count": sum(1 for row in rows if row["decision_status"] == "needs_more_samples"),
        "degraded_count": sum(1 for row in rows if row["decision_status"] == "degraded"),
        "watch_only_count": sum(1 for row in rows if row["decision_status"] == "watch_only"),
        "status_counts": _counts(rows, "decision_status"),
        "statistical_status_counts": _counts(rows, "statistical_status"),
        "residual_alpha_status_counts": _counts(rows, "residual_alpha_status"),
        "cost_status_counts": _counts(rows, "cost_status"),
        "net_edge_status_counts": _counts(rows, "net_edge_status"),
        "rows": rows,
        "latest_attribution": latest_attribution,
        "latest_alpha_validation": latest_alpha_validation,
        "independence_summary": independence.get("summary") or {},
        "independence_consumption": {
            "raw_alpha_strategy_count": raw_alpha_strategy_count,
            "independence_adjusted_strategy_count": independence_adjusted_count,
            "method": "sum_alpha_profile_redundancy_multipliers",
            "negative_correlation_penalty": False,
            "execution_authority": EXECUTION_AUTHORITY,
            "target_weight_mutation": TARGET_WEIGHT_MUTATION,
        },
        "warnings": warnings,
        "policy": {
            "statistical_decision_threshold": STAT_STATUS_INDICATIVE,
            "statistically_meaningful_threshold": STAT_STATUS_STATISTICALLY_MEANINGFUL,
            "operational_calibrated_not_sufficient_for_decision": True,
            "residual_alpha_epsilon": RESIDUAL_ALPHA_EPSILON,
            "attribution_min_sample_count": ATTRIBUTION_MIN_SAMPLE_COUNT,
            "redundancy_multiplier_formula": "piecewise_positive_correlation",
            "execution_authority": EXECUTION_AUTHORITY,
            "target_weight_mutation": TARGET_WEIGHT_MUTATION,
        },
    }


def _decision_profile_row(
    *,
    key: tuple[str, str, str, str],
    rows: list[dict[str, Any]],
    evidence: dict[str, Any],
    attribution: dict[str, Any],
    alpha_validation: dict[str, Any],
    independence: dict[str, Any],
    as_of_date: date | None,
) -> dict[str, Any]:
    strategy_id, family, regime, epoch_id = key
    sample_count = sum(max(int(row.get("n") or 0), 0) for row in rows)
    stat_status = decision_statistical_status(
        status=_best_operational_status(rows),
        n=sample_count,
        diagnostics={},
    )
    hit_rate = _weighted_average(rows, "hit_rate")
    hit_rate_ci = _wilson_hit_rate_interval(hit_rate, sample_count)
    avg_excess = _weighted_average(rows, "avg_excess_vs_spy")
    ic = _weighted_average(rows, "ic")
    conviction = _weighted_average(rows, "conviction")
    epoch = _representative_epoch(rows, epoch_id)
    residual = _residual_status(attribution)
    cost = _cost_status(evidence, alpha_validation, residual=residual)
    redundancy = _redundancy_status(strategy_id, independence)
    missing = _missing_components(
        rows=rows,
        attribution=attribution,
        residual=residual,
        evidence=evidence,
        alpha_validation=alpha_validation,
        independence=independence,
    )
    decision_status = _decision_status(stat_status, residual["status"], cost["status"])
    stat_credit = decision_conviction_discount(stat_status)
    residual_credit = residual["credit"]
    cost_credit = cost["credit"]
    redundancy_multiplier = redundancy["redundancy_multiplier"]
    decision_multiplier = round(stat_credit * residual_credit * cost_credit * redundancy_multiplier, 6)
    profile_id = _stable_hash({
        "strategy_id": strategy_id,
        "family": family,
        "regime": regime,
        "construction_epoch_id": epoch_id,
        "contract_version": CONTRACT_VERSION,
    })
    return {
        "alpha_decision_profile_id": profile_id,
        "contract_version": CONTRACT_VERSION,
        "as_of_date": (as_of_date or datetime.now(timezone.utc).date()).isoformat(),
        "strategy_id": strategy_id,
        "strategy_family": family,
        "alpha_source": bool(rows[0].get("alpha_source") if rows else False),
        "regime": regime,
        "construction_epoch_id": epoch_id,
        "pc_mode": epoch.get("pc_mode"),
        "construction_objective_version": epoch.get("construction_objective_version"),
        "policy_version": epoch.get("policy_version"),
        "sample_count": sample_count,
        "profile_count": len(rows),
        "tickers": sorted({row["ticker"] for row in rows if row["ticker"]}),
        "actions": sorted({row["action"] for row in rows if row["action"]}),
        "source_buckets": sorted({row["source_bucket"] for row in rows if row["source_bucket"]}),
        "operational_status_counts": _counts(rows, "status"),
        "statistical_status": stat_status,
        "statistical_credit": stat_credit,
        "hit_rate": hit_rate,
        "hit_rate_ci": hit_rate_ci,
        "hit_rate_ci_width": hit_rate_ci.get("width") if hit_rate_ci else None,
        "avg_excess_vs_spy": avg_excess,
        "ic": ic,
        "conviction": conviction,
        "residual_alpha": residual["residual_alpha"],
        "residual_alpha_status": residual["status"],
        "residual_alpha_credit": residual["credit"],
        "residual_alpha_source": residual["source"],
        "attribution_sample_count": residual["sample_count"],
        "cost_status": cost["status"],
        "cost_credit": cost["credit"],
        "gross_expected_edge": cost["gross_expected_edge"],
        "estimated_cost_pct": cost["estimated_cost_pct"],
        "estimated_ibkr_cost_pct": cost["estimated_ibkr_cost_pct"],
        "cost_adjusted_edge": cost["cost_adjusted_edge"],
        "net_edge_status": cost["net_edge_status"],
        "edge_to_cost_ratio": cost["edge_to_cost_ratio"],
        "cost_model": cost["cost_model"],
        "turnover": cost["turnover"],
        "min_edge_to_cost_ratio": cost["min_edge_to_cost_ratio"],
        "avg_edge_to_cost_ratio": cost["avg_edge_to_cost_ratio"],
        "independence_cluster_id": redundancy["cluster_id"],
        "max_positive_correlation": redundancy["max_positive_correlation"],
        "most_correlated_strategy": redundancy["most_correlated_strategy"],
        "redundancy_multiplier": redundancy_multiplier,
        "redundancy_penalty": round(1.0 - redundancy_multiplier, 6),
        "decision_multiplier": decision_multiplier,
        "decision_status": decision_status,
        "missing_components": missing,
        "diagnostics": {
            "component_credits": {
                "statistical": stat_credit,
                "residual_alpha": residual_credit,
                "cost": cost_credit,
                "redundancy": redundancy_multiplier,
            },
            "evidence_reason_codes": evidence.get("reason_codes") or [],
            "data_ready": evidence.get("data_ready"),
            "can_influence_allocation": evidence.get("can_influence_allocation"),
            "construction_epoch": epoch,
            "latest_attribution_period_key": attribution.get("period_key"),
            "latest_alpha_validation_analysis_id": alpha_validation.get("analysis_id"),
        },
        "execution_authority": EXECUTION_AUTHORITY,
        "target_weight_mutation": TARGET_WEIGHT_MUTATION,
    }


def _profile_row(value: Any) -> dict[str, Any]:
    strategy_id = str(_record_get(value, "strategy_id") or "").strip()
    family, alpha_source = _strategy_family(strategy_id)
    diagnostics = _record_get(value, "diagnostics")
    diagnostics = diagnostics if isinstance(diagnostics, dict) else {}
    n = _to_int(_record_get(value, "n"), 0)
    hit_rate = _to_float(_record_get(value, "hit_rate"))
    ci = diagnostics.get("hit_rate_ci") if isinstance(diagnostics.get("hit_rate_ci"), dict) else {}
    epoch = construction_epoch_from_diagnostics(diagnostics)
    stat_status = decision_statistical_status(
        status=str(_record_get(value, "status") or ""),
        n=n,
        diagnostics=diagnostics,
    )
    return {
        "strategy_id": strategy_id,
        "ticker": str(_record_get(value, "ticker") or "").upper().strip(),
        "branch": _record_get(value, "branch"),
        "action": str(_record_get(value, "action") or ""),
        "regime": str(_record_get(value, "regime_at_signal") or "unknown"),
        "horizon_days": _to_int(_record_get(value, "horizon_days"), 0),
        "source_bucket": str(_record_get(value, "source_bucket") or "unknown"),
        "status": str(_record_get(value, "status") or "unknown"),
        "n": n,
        "hit_rate": hit_rate,
        "statistical_status": stat_status,
        "hit_rate_ci": ci or None,
        "hit_rate_ci_width": _to_float(diagnostics.get("hit_rate_ci_width", ci.get("width"))),
        "construction_epoch": epoch,
        "construction_epoch_id": epoch.get("epoch_id") or "unknown",
        "avg_excess_vs_spy": _to_float(_record_get(value, "avg_excess_vs_spy")),
        "ic": _to_float(_record_get(value, "ic")),
        "conviction": _to_float(_record_get(value, "conviction")),
        "canonical_family": family,
        "alpha_source": alpha_source,
    }


def _dedupe_profiles(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    best: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in rows:
        key = (
            row["strategy_id"],
            row["ticker"],
            row["branch"],
            row["action"],
            row["regime"],
            row["horizon_days"],
            row.get("construction_epoch_id") or "unknown",
        )
        current = best.get(key)
        if current is None or _source_rank(row) < _source_rank(current):
            best[key] = row
    return list(best.values())


def _strategy_evidence_index(evidence: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows = evidence.get("strategy_results") or evidence.get("strategy_rows") or []
    index: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        name = str(row.get("strategy_name") or row.get("strategy") or "").strip()
        if not name:
            continue
        index[name] = {
            "suggested_use": row.get("suggested_use"),
            "confidence_score": _to_float(row.get("confidence_score")),
            "data_ready": row.get("data_ready"),
            "can_influence_allocation": row.get("can_influence_allocation"),
            "estimated_cost_pct": _to_float(row.get("estimated_cost_pct")),
            "turnover": _to_float(row.get("turnover")),
            "reason_codes": row.get("reason_codes") or [],
        }
    return index


def _attribution_row(row: Any) -> dict[str, Any]:
    return {
        "period_key": _record_get(row, "period_key"),
        "period_start": _iso(_record_get(row, "period_start")),
        "period_end": _iso(_record_get(row, "period_end")),
        "generated_at": _iso(_record_get(row, "generated_at")),
        "status": _record_get(row, "status"),
        "attribution_method": _record_get(row, "attribution_method"),
        "residual_alpha_candidate": _to_float(_record_get(row, "residual_alpha_candidate")),
        "sample_count": _to_int(_record_get(row, "sample_count"), 0),
        "r_squared": _to_float(_record_get(row, "r_squared")),
        "data_quality": _record_get(row, "data_quality"),
        "benchmark_source": _record_get(row, "benchmark_source"),
    }


def _alpha_run_row(row: Any) -> dict[str, Any]:
    return {
        "analysis_id": _record_get(row, "analysis_id"),
        "generated_at": _iso(_record_get(row, "generated_at")),
        "status": _record_get(row, "status"),
        "data_quality": _record_get(row, "data_quality"),
        "cost_gate_status": _record_get(row, "cost_gate_status"),
        "low_edge_trade_count": _to_int(_record_get(row, "low_edge_trade_count"), 0),
        "min_edge_to_cost_ratio": _to_float(_record_get(row, "min_edge_to_cost_ratio")),
        "avg_edge_to_cost_ratio": _to_float(_record_get(row, "avg_edge_to_cost_ratio")),
        "independent_alpha_family_count": _to_int(_record_get(row, "independent_alpha_family_count"), 0),
    }


def _residual_status(attribution: dict[str, Any]) -> dict[str, Any]:
    residual = _to_float(attribution.get("residual_alpha_candidate"))
    sample_count = _to_int(attribution.get("sample_count"), 0)
    status = str(attribution.get("status") or "")
    data_quality = str(attribution.get("data_quality") or "")
    if not attribution:
        return _residual_payload("insufficient", None, 0, 0.0, "missing_attribution")
    if status not in {"attributed", "ok", "available"} or sample_count < ATTRIBUTION_MIN_SAMPLE_COUNT:
        return _residual_payload("insufficient", residual, sample_count, 0.0, "portfolio_level_proxy")
    if residual is None:
        return _residual_payload("insufficient", None, sample_count, 0.0, "portfolio_level_proxy")
    if data_quality and data_quality not in {"ok", "diagnostic_supported", "historical_supported"}:
        return _residual_payload("insufficient", residual, sample_count, 0.0, "portfolio_level_proxy")
    if residual > RESIDUAL_ALPHA_EPSILON:
        return _residual_payload("positive", residual, sample_count, 1.0, "portfolio_level_proxy")
    if residual < -RESIDUAL_ALPHA_EPSILON:
        return _residual_payload("negative", residual, sample_count, 0.0, "portfolio_level_proxy")
    return _residual_payload("neutral", residual, sample_count, 0.5, "portfolio_level_proxy")


def _residual_payload(
    status: str,
    residual: float | None,
    sample_count: int,
    credit: float,
    source: str,
) -> dict[str, Any]:
    return {
        "status": status,
        "residual_alpha": residual,
        "sample_count": sample_count,
        "credit": credit,
        "source": source,
    }


def _cost_status(
    evidence: dict[str, Any],
    alpha_validation: dict[str, Any],
    *,
    residual: dict[str, Any] | None = None,
) -> dict[str, Any]:
    estimated_cost = _to_float(evidence.get("estimated_cost_pct"))
    turnover = _to_float(evidence.get("turnover"))
    gate = str(alpha_validation.get("cost_gate_status") or "")
    low_edge = _to_int(alpha_validation.get("low_edge_trade_count"), 0)
    min_ratio = _to_float(alpha_validation.get("min_edge_to_cost_ratio"))
    avg_ratio = _to_float(alpha_validation.get("avg_edge_to_cost_ratio"))
    gross_edge = _to_float((residual or {}).get("residual_alpha"))
    net_edge = _net_edge_status(gross_edge=gross_edge, estimated_cost=estimated_cost)

    status = "ok"
    if not evidence and not alpha_validation:
        status = "insufficient"
    elif gate in {"blocked", "negative_after_cost"}:
        status = "negative_after_cost"
    elif gate in {"warning", "low_edge_to_cost"} or low_edge > 0:
        status = "low_edge_after_cost"
    elif min_ratio is not None and min_ratio < LOW_EDGE_TO_COST_RATIO:
        status = "low_edge_after_cost"
    elif avg_ratio is not None and avg_ratio < WATCH_EDGE_TO_COST_RATIO:
        status = "watch_costs"
    elif estimated_cost is None and not alpha_validation:
        status = "insufficient"

    final_status = _worst_cost_status(status, net_edge["status"])
    credit = COST_STATUS_CREDIT.get(final_status, 0.0)

    return {
        "status": final_status,
        "credit": credit,
        "gross_expected_edge": gross_edge,
        "estimated_cost_pct": estimated_cost,
        "estimated_ibkr_cost_pct": estimated_cost,
        "cost_adjusted_edge": net_edge["cost_adjusted_edge"],
        "net_edge_status": net_edge["status"],
        "edge_to_cost_ratio": net_edge["edge_to_cost_ratio"],
        "cost_model": COST_MODEL,
        "turnover": turnover,
        "cost_gate_status": gate or None,
        "low_edge_trade_count": low_edge,
        "min_edge_to_cost_ratio": min_ratio,
        "avg_edge_to_cost_ratio": avg_ratio,
    }


def _net_edge_status(*, gross_edge: float | None, estimated_cost: float | None) -> dict[str, Any]:
    if gross_edge is None or estimated_cost is None:
        return {
            "status": "insufficient",
            "cost_adjusted_edge": None,
            "edge_to_cost_ratio": None,
        }
    net_edge = gross_edge - estimated_cost
    ratio = None
    if estimated_cost > 0:
        ratio = gross_edge / estimated_cost
    if net_edge < 0:
        status = "negative_after_cost"
    elif ratio is not None and ratio < WATCH_EDGE_TO_COST_RATIO:
        status = "low_edge_after_cost"
    elif ratio is not None and ratio < COMFORTABLE_EDGE_TO_COST_RATIO:
        status = "watch_costs"
    else:
        status = "ok"
    return {
        "status": status,
        "cost_adjusted_edge": round(net_edge, 8),
        "edge_to_cost_ratio": round(ratio, 6) if ratio is not None else None,
    }


def _worst_cost_status(*statuses: str) -> str:
    clean = [status for status in statuses if status]
    if not clean:
        return "insufficient"
    return sorted(clean, key=lambda status: COST_STATUS_RANK.get(status, 99))[0]


def _independence_context(payload: dict[str, Any]) -> dict[str, Any]:
    pair_rows = payload.get("pair_rows") if isinstance(payload.get("pair_rows"), list) else []
    high_rows = payload.get("high_correlation_pairs") if isinstance(payload.get("high_correlation_pairs"), list) else []
    pairs = [_pair_row(row) for row in pair_rows + high_rows if isinstance(row, dict)]
    by_strategy: dict[str, list[dict[str, Any]]] = {}
    graph: dict[str, set[str]] = {}
    for row in pairs:
        left = row.get("left_strategy")
        right = row.get("right_strategy")
        corr = row.get("correlation")
        if not left or not right or corr is None or corr <= 0:
            continue
        by_strategy.setdefault(left, []).append(row)
        by_strategy.setdefault(right, []).append(row)
        if corr >= 0.65:
            graph.setdefault(left, set()).add(right)
            graph.setdefault(right, set()).add(left)

    cluster_map: dict[str, str] = {}
    for component in _connected_components(graph):
        cluster_id = "corr_cluster:" + _stable_hash(sorted(component))
        for name in component:
            cluster_map[name] = cluster_id

    return {
        "available": bool(payload),
        "pairs_by_strategy": by_strategy,
        "cluster_map": cluster_map,
        "summary": {
            "status": payload.get("status"),
            "effective_independent_alpha_count": payload.get("effective_independent_alpha_count"),
            "avg_positive_correlation": payload.get("avg_positive_correlation"),
            "high_correlation_pair_count": len(high_rows),
        },
    }


def _pair_row(row: dict[str, Any]) -> dict[str, Any]:
    left = (
        row.get("left_strategy")
        or row.get("strategy_a")
        or row.get("left")
        or row.get("strategy_1")
    )
    right = (
        row.get("right_strategy")
        or row.get("strategy_b")
        or row.get("right")
        or row.get("strategy_2")
    )
    return {
        "left_strategy": str(left or "").strip(),
        "right_strategy": str(right or "").strip(),
        "correlation": _to_float(row.get("correlation")),
    }


def _redundancy_status(strategy_id: str, independence: dict[str, Any]) -> dict[str, Any]:
    pairs = independence.get("pairs_by_strategy", {}).get(strategy_id) or []
    max_corr = None
    most_correlated = None
    for row in pairs:
        corr = _to_float(row.get("correlation"))
        if corr is None:
            continue
        other = row.get("right_strategy") if row.get("left_strategy") == strategy_id else row.get("left_strategy")
        if max_corr is None or corr > max_corr:
            max_corr = corr
            most_correlated = other
    multiplier = redundancy_multiplier(max_corr if max_corr is not None else 0.0)
    cluster_id = independence.get("cluster_map", {}).get(strategy_id) or f"independent:{strategy_id}"
    return {
        "cluster_id": cluster_id,
        "max_positive_correlation": max_corr,
        "most_correlated_strategy": most_correlated,
        "redundancy_multiplier": multiplier,
    }


def redundancy_multiplier(correlation: float | None) -> float:
    """Piecewise positive-correlation redundancy discount."""
    if correlation is None:
        return 1.0
    corr = max(float(correlation), 0.0)
    if corr < 0.20:
        return 1.0
    if corr < 0.40:
        return 0.85
    if corr < 0.65:
        return 0.50
    if corr < 0.80:
        return 0.20
    return 0.05


def _decision_status(statistical_status: str, residual_status: str, cost_status: str) -> str:
    if statistical_status in {STAT_STATUS_INSUFFICIENT, STAT_STATUS_EARLY_SIGNAL}:
        return "needs_more_samples"
    if residual_status == "negative":
        return "degraded"
    if cost_status in {"negative_after_cost", "low_edge_after_cost", "watch_costs"}:
        return "watch_only"
    if (
        statistical_status in {STAT_STATUS_INDICATIVE, STAT_STATUS_STATISTICALLY_MEANINGFUL}
        and residual_status in {"positive", "neutral"}
        and cost_status == "ok"
    ):
        return "eligible"
    return "watch_only"


def _missing_components(
    *,
    rows: list[dict[str, Any]],
    attribution: dict[str, Any],
    residual: dict[str, Any],
    evidence: dict[str, Any],
    alpha_validation: dict[str, Any],
    independence: dict[str, Any],
) -> list[str]:
    missing: list[str] = []
    if not rows:
        missing.append("insufficient_conviction")
    if residual.get("status") == "insufficient" or not attribution:
        missing.append("insufficient_attribution")
    if not evidence and not alpha_validation:
        missing.append("insufficient_cost")
    if not independence.get("available"):
        missing.append("insufficient_independence")
    return sorted(set(missing))


def _representative_epoch(rows: list[dict[str, Any]], epoch_id: str) -> dict[str, Any]:
    for row in rows:
        epoch = row.get("construction_epoch")
        if isinstance(epoch, dict) and epoch.get("epoch_id") == epoch_id:
            return epoch
    return {
        "epoch_id": epoch_id,
        "pc_mode": "unknown",
        "construction_objective_version": "unknown",
        "policy_version": "unknown",
        "execution_authority": EXECUTION_AUTHORITY,
        "target_weight_mutation": TARGET_WEIGHT_MUTATION,
    }


def _best_operational_status(rows: list[dict[str, Any]]) -> str:
    priority = {"calibrated": 0, "early_estimate": 1, "insufficient_samples": 2}
    best = min(
        (str(row.get("status") or "unknown") for row in rows),
        key=lambda status: priority.get(status, 99),
        default="unknown",
    )
    return best


def _summary_warnings(
    rows: list[dict[str, Any]],
    attribution: dict[str, Any],
    alpha_validation: dict[str, Any],
    independence: dict[str, Any],
) -> list[str]:
    warnings: list[str] = []
    if not rows:
        warnings.append("no_alpha_decision_profiles")
    if not attribution:
        warnings.append("missing_performance_attribution")
    if not alpha_validation:
        warnings.append("missing_alpha_validation_run")
    if not independence.get("available"):
        warnings.append("missing_strategy_independence")
    for row in rows:
        if row.get("decision_status") == "degraded":
            warnings.append(f"degraded_alpha_profile:{row.get('strategy_id')}:{row.get('regime')}")
        if row.get("decision_status") == "needs_more_samples":
            warnings.append(f"needs_more_samples:{row.get('strategy_id')}:{row.get('regime')}")
    return sorted(set(warnings))


def _strategy_family(strategy_id: str) -> tuple[str, bool]:
    try:
        from strategies import get_strategy

        strategy = get_strategy(strategy_id)
        card = strategy.strategy_card()
        family = canonical_strategy_family(card.get("canonical_family") or card.get("family"))
        return family, bool(is_strategy_alpha_source(strategy_id, family, card.get("alpha_source")))
    except Exception:
        family = canonical_strategy_family(None)
        return family, bool(is_strategy_alpha_source(strategy_id, family, None))


def _source_rank(row: dict[str, Any]) -> int:
    return SOURCE_BUCKET_PRIORITY.get(str(row.get("source_bucket") or ""), 99)


def _weighted_average(rows: list[dict[str, Any]], field: str) -> float | None:
    total = 0.0
    weight_sum = 0
    for row in rows:
        value = _to_float(row.get(field))
        if value is None:
            continue
        weight = max(int(row.get("n") or 0), 1)
        total += value * weight
        weight_sum += weight
    if weight_sum <= 0:
        return None
    return round(total / weight_sum, 6)


def _counts(rows: list[dict[str, Any]], field: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        key = str(row.get(field) or "unknown")
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items()))


def _connected_components(graph: dict[str, set[str]]) -> list[list[str]]:
    seen: set[str] = set()
    out: list[list[str]] = []
    for node in sorted(graph):
        if node in seen:
            continue
        stack = [node]
        component: list[str] = []
        while stack:
            current = stack.pop()
            if current in seen:
                continue
            seen.add(current)
            component.append(current)
            stack.extend(sorted(graph.get(current, set()) - seen))
        out.append(sorted(component))
    return out


def _wilson_hit_rate_interval(hit_rate: float | None, n: int) -> dict[str, Any] | None:
    if hit_rate is None or n <= 0:
        return None
    p = max(min(float(hit_rate), 1.0), 0.0)
    z = 1.96
    denom = 1.0 + z * z / n
    center = (p + z * z / (2.0 * n)) / denom
    margin = z * math.sqrt((p * (1.0 - p) + z * z / (4.0 * n)) / n) / denom
    low = max(0.0, center - margin)
    high = min(1.0, center + margin)
    return {
        "low": round(low, 4),
        "high": round(high, 4),
        "width": round(high - low, 4),
        "method": "wilson_95",
    }


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(_json_ready(value), sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _decision_row_sort_key(row: dict[str, Any]) -> tuple[int, str, str, str]:
    rank = {"eligible": 0, "watch_only": 1, "needs_more_samples": 2, "degraded": 3}
    return (
        rank.get(str(row.get("decision_status") or ""), 9),
        str(row.get("strategy_family") or ""),
        str(row.get("strategy_id") or ""),
        str(row.get("regime") or ""),
    )


def _record_get(value: Any, key: str) -> Any:
    if isinstance(value, dict):
        return value.get(key)
    return getattr(value, key, None)


def _to_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        number = float(value)
        if not math.isfinite(number):
            return None
        return number
    except (TypeError, ValueError):
        return None


def _to_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _iso(value: Any) -> str | None:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if value is None:
        return None
    return str(value)


def _json_ready(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _json_ready(val) for key, val in sorted(value.items())}
    if isinstance(value, (list, tuple, set)):
        return [_json_ready(item) for item in value]
    return value
