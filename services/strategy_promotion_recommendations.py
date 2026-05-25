"""Strategy promotion and degradation recommendations.

This module turns conviction/profile diagnostics into operator review items.
It is recommendation-only and has no execution authority.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

from services.construction_epoch import construction_epoch_from_diagnostics
from services.strategy_conviction import (
    SOURCE_BUCKET_COMBINED,
    SOURCE_BUCKET_HISTORICAL_PRIOR,
    SOURCE_BUCKET_LIVE_PAPER,
    STATUS_CALIBRATED,
    STATUS_EARLY_ESTIMATE,
    STATUS_EARLY_LIVE_CONFIRMATION,
    STATUS_HISTORICAL_REQUIRES_LIVE,
    STATUS_INSUFFICIENT_SAMPLES,
    STAT_STATUS_INDICATIVE,
    STAT_STATUS_STATISTICALLY_MEANINGFUL,
    statistical_interpretation,
)
from services.strategy_diversity import (
    ALPHA_FAMILIES,
    canonical_strategy_family,
    is_strategy_alpha_source,
)
from services.strategy_regime_gap_analysis import build_strategy_regime_gap_analysis


PROMOTE_HIT_RATE_THRESHOLD = 0.55
DEGRADE_HIT_RATE_THRESHOLD = 0.45
ARCHIVE_HIT_RATE_THRESHOLD = 0.40
PROMOTE_MIN_TOTAL_N = 30
MAX_PROMOTION_ESTIMATED_COST_PCT = 0.003
MAX_PROMOTION_TURNOVER = 0.50
STATISTICAL_PROMOTION_STATUSES = {
    STAT_STATUS_INDICATIVE,
    STAT_STATUS_STATISTICALLY_MEANINGFUL,
}
SOURCE_BUCKET_PRIORITY = {
    SOURCE_BUCKET_COMBINED: 0,
    SOURCE_BUCKET_LIVE_PAPER: 1,
    SOURCE_BUCKET_HISTORICAL_PRIOR: 2,
}
NON_CALIBRATED_STATUSES = {
    STATUS_INSUFFICIENT_SAMPLES,
    STATUS_EARLY_ESTIMATE,
    STATUS_EARLY_LIVE_CONFIRMATION,
    STATUS_HISTORICAL_REQUIRES_LIVE,
}
ACTIONABLE_USES = {"primary", "advisory"}


async def load_strategy_promotion_recommendations(
    db: Any,
    *,
    as_of_date: date | None = None,
    row_limit: int = 5000,
) -> dict[str, Any]:
    """Load persisted diagnostics and build recommendation rows."""
    from sqlalchemy import desc, func, select

    from db.models import AgentAnalysis, AgentStepLog, AlphaValidationRun, StrategyConvictionProfile

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
            )
            .limit(row_limit)
        )
        profile_rows = list(profile_result.scalars().all())

    alpha_result = await db.execute(
        select(AlphaValidationRun)
        .order_by(desc(AlphaValidationRun.generated_at), desc(AlphaValidationRun.id))
        .limit(30)
    )
    alpha_rows = list(alpha_result.scalars().all())

    latest_analysis = (
        await db.execute(
            select(AgentAnalysis).order_by(desc(AgentAnalysis.analyzed_at), desc(AgentAnalysis.id)).limit(1)
        )
    ).scalar_one_or_none()
    strategy_evidence: dict[str, Any] = {}
    latest_analysis_id = None
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
        strategy_evidence = (
            evidence.get("strategies")
            if isinstance(evidence.get("strategies"), dict)
            else {}
        )

    summary = build_strategy_promotion_recommendations(
        profiles=profile_rows,
        strategy_evidence=strategy_evidence,
        alpha_validation_runs=alpha_rows,
        as_of_date=target_date,
    )
    if latest_profile_date is not None:
        summary["latest_profile_date"] = latest_profile_date.isoformat()
    if latest_analysis_id is not None:
        summary["latest_analysis_id"] = latest_analysis_id
    return summary


def build_strategy_promotion_recommendations(
    *,
    profiles: list[Any],
    strategy_evidence: dict[str, Any] | None = None,
    alpha_validation_runs: list[Any] | None = None,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    profile_rows = _dedupe_profiles([_profile_row(item) for item in profiles])
    alpha_rows = [_alpha_run_row(row) for row in (alpha_validation_runs or [])]
    evidence_index = _strategy_evidence_index(strategy_evidence or {})
    diagnostic_context = _promotion_diagnostic_context(strategy_evidence or {}, alpha_rows)
    gap_summary = build_strategy_regime_gap_analysis(
        profiles=profiles,
        alpha_validation_runs=alpha_validation_runs or [],
        as_of_date=as_of_date,
    )

    recommendations: list[dict[str, Any]] = []
    if profile_rows:
        recommendations.extend(_strategy_recommendations(profile_rows, evidence_index, diagnostic_context))
        recommendations.extend(_family_regime_recommendations(gap_summary))
        recommendations.extend(_research_gap_recommendations(gap_summary))

    recommendations = _sorted_unique_recommendations(recommendations)
    counts = _recommendation_counts(recommendations)
    high_priority = sum(1 for row in recommendations if row.get("priority") == "high")
    status = "recommendations_available"
    if not profile_rows:
        status = "insufficient_data"
    elif high_priority:
        status = "operator_review_required"
    elif not recommendations:
        status = "no_action"

    return {
        "contract_version": "strategy_promotion_recommendations_v1",
        "status": status,
        "as_of_date": (as_of_date or datetime.now(timezone.utc).date()).isoformat(),
        "execution_authority": "none",
        "target_weight_mutation": "none",
        "recommendation_only": True,
        "profile_count": len(profile_rows),
        "strategy_count": len({row["strategy_id"] for row in profile_rows if row["strategy_id"]}),
        "recommendation_count": len(recommendations),
        "high_priority_count": high_priority,
        "recommendation_counts": counts,
        "recommendations": recommendations,
        "gap_status": gap_summary.get("status"),
        "gap_warnings": gap_summary.get("warnings") or [],
        "latest_alpha_validation": alpha_rows[0] if alpha_rows else {},
        "policy": {
            "promote_hit_rate_threshold": PROMOTE_HIT_RATE_THRESHOLD,
            "degrade_hit_rate_threshold": DEGRADE_HIT_RATE_THRESHOLD,
            "archive_hit_rate_threshold": ARCHIVE_HIT_RATE_THRESHOLD,
            "promote_min_total_n": PROMOTE_MIN_TOTAL_N,
            "statistical_promotion_statuses": sorted(STATISTICAL_PROMOTION_STATUSES),
            "non_calibrated_statuses_require_more_samples": sorted(NON_CALIBRATED_STATUSES),
            "max_promotion_estimated_cost_pct": MAX_PROMOTION_ESTIMATED_COST_PCT,
            "max_promotion_turnover": MAX_PROMOTION_TURNOVER,
            "promotion_requires_independence_decay_liquidity_cost_alignment": True,
            "operator_approval_required": True,
        },
        "warnings": _summary_warnings(recommendations, gap_summary),
    }


def _strategy_recommendations(
    profiles: list[dict[str, Any]],
    evidence_index: dict[str, dict[str, Any]],
    diagnostic_context: dict[str, Any],
) -> list[dict[str, Any]]:
    by_strategy: dict[str, list[dict[str, Any]]] = {}
    for row in profiles:
        if not row["alpha_source"]:
            continue
        by_strategy.setdefault(row["strategy_id"], []).append(row)

    out: list[dict[str, Any]] = []
    for strategy_id, rows in sorted(by_strategy.items()):
        evidence = evidence_index.get(strategy_id, {})
        current_use = str(
            evidence.get("approved_use")
            or evidence.get("suggested_use")
            or "unknown"
        )
        calibrated = [row for row in rows if row["status"] == STATUS_CALIBRATED]
        non_calibrated = [row for row in rows if row["status"] in NON_CALIBRATED_STATUSES]
        weak = [row for row in calibrated if _is_weak_profile(row)]
        strong = [row for row in calibrated if _is_promotable_profile(row)]
        statistically_early = [
            row for row in calibrated
            if row.get("statistical_status") not in STATISTICAL_PROMOTION_STATUSES
            and _is_directionally_positive(row)
        ]
        family = _first(rows, "canonical_family")

        if weak and current_use in ACTIONABLE_USES:
            out.append(_recommendation_row(
                recommendation="demote_to_watch_only_review",
                priority="high",
                strategy_id=strategy_id,
                canonical_family=family,
                current_use=current_use,
                recommended_use="watch_only",
                profiles=weak,
                reasons=_weak_reasons(weak) + ["current_use_actionable"],
                operator_action="review strategy use; demote to watch_only if weakness is confirmed",
            ))
        elif strong and current_use not in ACTIONABLE_USES:
            evidence_checks = _promotion_evidence_checks(
                strategy_id=strategy_id,
                strong_profiles=strong,
                weak_profiles=weak,
                all_profiles=rows,
                evidence=evidence,
                evidence_index=evidence_index,
                diagnostic_context=diagnostic_context,
            )
            total_n = sum(int(row.get("n") or 0) for row in strong)
            priority = "medium" if total_n >= PROMOTE_MIN_TOTAL_N else "low"
            if evidence_checks["blockers"]:
                out.append(_recommendation_row(
                    recommendation="require_promotion_evidence_alignment",
                    priority="medium",
                    strategy_id=strategy_id,
                    canonical_family=family,
                    current_use=current_use,
                    recommended_use=current_use if current_use != "unknown" else "watch_only",
                    profiles=strong,
                    reasons=[
                        "calibrated_positive_conviction_but_evidence_gates_not_clear",
                        *evidence_checks["reasons"],
                    ],
                    operator_action=(
                        "resolve independence, regime, decay, liquidity, and cost diagnostics "
                        "before promoting this strategy"
                    ),
                    blockers=evidence_checks["blockers"],
                    evidence_checks=evidence_checks,
                ))
            else:
                out.append(_recommendation_row(
                    recommendation="promote_to_advisory_review",
                    priority=priority,
                    strategy_id=strategy_id,
                    canonical_family=family,
                    current_use=current_use,
                    recommended_use="advisory",
                    profiles=strong,
                    reasons=["calibrated_positive_conviction", "operator_approval_required"],
                    operator_action="review evidence before changing suggested_use to advisory",
                    evidence_checks=evidence_checks,
                ))
        elif statistically_early:
            out.append(_recommendation_row(
                recommendation="require_statistical_maturity",
                priority="medium" if current_use in ACTIONABLE_USES else "low",
                strategy_id=strategy_id,
                canonical_family=family,
                current_use=current_use,
                recommended_use=current_use if current_use != "unknown" else "watch_only",
                profiles=statistically_early,
                reasons=[
                    "operationally_calibrated_but_statistically_early",
                    *sorted({str(row.get("statistical_status") or "unknown") for row in statistically_early}),
                ],
                operator_action="do not promote from conviction until statistical_status is indicative or better",
            ))
        elif non_calibrated and not calibrated:
            out.append(_recommendation_row(
                recommendation="require_more_samples",
                priority="medium" if current_use in ACTIONABLE_USES else "low",
                strategy_id=strategy_id,
                canonical_family=family,
                current_use=current_use,
                recommended_use=current_use if current_use != "unknown" else "watch_only",
                profiles=non_calibrated,
                reasons=sorted({row["status"] for row in non_calibrated}),
                operator_action="do not promote or demote from conviction until samples mature",
            ))
        elif weak:
            out.append(_recommendation_row(
                recommendation="keep_watch_only_due_to_degradation",
                priority="medium",
                strategy_id=strategy_id,
                canonical_family=family,
                current_use=current_use,
                recommended_use="watch_only",
                profiles=weak,
                reasons=_weak_reasons(weak),
                operator_action="keep strategy out of advisory use in weak regimes",
            ))
    return out


def _family_regime_recommendations(gap_summary: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in gap_summary.get("weak_family_regime_rows") or []:
        family = str(row.get("family") or "unknown")
        regime = str(row.get("regime") or "unknown")
        hit_rate = _to_float(row.get("hit_rate"))
        avg_excess = _to_float(row.get("avg_excess_vs_spy"))
        ic = _to_float(row.get("ic"))
        archive = bool(
            (hit_rate is not None and hit_rate < ARCHIVE_HIT_RATE_THRESHOLD)
            or (
                avg_excess is not None
                and avg_excess < 0
                and ic is not None
                and ic < 0
            )
        )
        out.append({
            "recommendation": "archive_family_regime_review" if archive else "demote_family_regime_to_watch_review",
            "priority": "high" if archive else "medium",
            "strategy_id": "",
            "canonical_family": family,
            "regime": regime,
            "current_use": "family_level",
            "recommended_use": "archive_in_regime" if archive else "watch_only_in_regime",
            "sample_count": row.get("total_n"),
            "profile_count": row.get("profile_count"),
            "hit_rate": hit_rate,
            "avg_excess_vs_spy": avg_excess,
            "ic": ic,
            "conviction": None,
            "reasons": row.get("reasons") or [],
            "blockers": ["operator_approval_required"],
            "operator_action": "review whether this family should be disabled for the regime",
        })
    return out


def _research_gap_recommendations(gap_summary: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in gap_summary.get("research_queue") or []:
        out.append({
            "recommendation": "research_family_for_regime",
            "priority": row.get("priority") or "medium",
            "strategy_id": "",
            "canonical_family": row.get("suggested_family"),
            "regime": row.get("regime"),
            "current_use": "missing_or_weak",
            "recommended_use": "research_candidate",
            "sample_count": None,
            "profile_count": None,
            "hit_rate": None,
            "avg_excess_vs_spy": None,
            "ic": None,
            "conviction": None,
            "reasons": [row.get("reason")],
            "blockers": ["not_an_execution_change"],
            "operator_action": "prioritize strategy research for this regime/family",
        })
    return out


def _recommendation_row(
    *,
    recommendation: str,
    priority: str,
    strategy_id: str,
    canonical_family: str,
    current_use: str,
    recommended_use: str,
    profiles: list[dict[str, Any]],
    reasons: list[str],
    operator_action: str,
    blockers: list[str] | None = None,
    evidence_checks: dict[str, Any] | None = None,
) -> dict[str, Any]:
    regimes = sorted({str(row.get("regime") or "unknown") for row in profiles})
    blocker_list = _unique(["operator_approval_required", *(blockers or [])])
    return {
        "recommendation": recommendation,
        "priority": priority,
        "strategy_id": strategy_id,
        "canonical_family": canonical_family,
        "regime": ",".join(regimes),
        "current_use": current_use,
        "recommended_use": recommended_use,
        "sample_count": sum(int(row.get("n") or 0) for row in profiles),
        "profile_count": len(profiles),
        "construction_epoch_ids": sorted({
            str(row.get("construction_epoch_id") or "unknown")
            for row in profiles
        }),
        "hit_rate": _weighted_average(profiles, "hit_rate"),
        "avg_excess_vs_spy": _weighted_average(profiles, "avg_excess_vs_spy"),
        "ic": _weighted_average(profiles, "ic"),
        "conviction": _weighted_average(profiles, "conviction"),
        "statistical_status_counts": _status_counts(profiles, "statistical_status"),
        "max_hit_rate_ci_width": _max_float(profiles, "hit_rate_ci_width"),
        "reasons": _unique(reasons),
        "blockers": blocker_list,
        "evidence_checks": evidence_checks or {},
        "operator_action": operator_action,
    }


def _promotion_diagnostic_context(
    strategy_evidence: dict[str, Any],
    alpha_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    independence = (
        strategy_evidence.get("strategy_independence")
        if isinstance(strategy_evidence.get("strategy_independence"), dict)
        else {}
    )
    decay = (
        strategy_evidence.get("etf_decay_diagnostics")
        if isinstance(strategy_evidence.get("etf_decay_diagnostics"), dict)
        else {}
    )
    liquidity = (
        strategy_evidence.get("liquidity_proxy_diagnostics")
        if isinstance(strategy_evidence.get("liquidity_proxy_diagnostics"), dict)
        else {}
    )
    return {
        "strategy_independence": independence,
        "etf_decay_diagnostics": decay,
        "liquidity_proxy_diagnostics": liquidity,
        "latest_alpha_validation": alpha_rows[0] if alpha_rows else {},
    }


def _promotion_evidence_checks(
    *,
    strategy_id: str,
    strong_profiles: list[dict[str, Any]],
    weak_profiles: list[dict[str, Any]],
    all_profiles: list[dict[str, Any]],
    evidence: dict[str, Any],
    evidence_index: dict[str, dict[str, Any]],
    diagnostic_context: dict[str, Any],
) -> dict[str, Any]:
    blockers: list[str] = []
    reasons: list[str] = []
    checks = {
        "independence": _independence_check(
            strategy_id=strategy_id,
            evidence_index=evidence_index,
            diagnostics=diagnostic_context.get("strategy_independence") or {},
        ),
        "regime_coverage": _regime_coverage_check(
            strong_profiles=strong_profiles,
            weak_profiles=weak_profiles,
            all_profiles=all_profiles,
        ),
        "decay": _decay_check(
            tickers=_promotion_tickers(strong_profiles, evidence),
            diagnostics=diagnostic_context.get("etf_decay_diagnostics") or {},
        ),
        "liquidity": _liquidity_check(
            tickers=_promotion_tickers(strong_profiles, evidence),
            diagnostics=diagnostic_context.get("liquidity_proxy_diagnostics") or {},
        ),
        "cost": _cost_check(
            evidence=evidence,
            latest_alpha=diagnostic_context.get("latest_alpha_validation") or {},
        ),
    }
    for name, check in checks.items():
        if not check.get("passes"):
            blockers.append(f"{name}_diagnostics_not_clear")
            reasons.extend(str(item) for item in check.get("reasons") or [])
    return {
        "passes": not blockers,
        "blockers": _unique(blockers),
        "reasons": _unique(reasons),
        **checks,
    }


def _promotion_tickers(profiles: list[dict[str, Any]], evidence: dict[str, Any]) -> list[str]:
    tickers = {
        str(row.get("ticker") or "").upper().strip()
        for row in profiles
        if str(row.get("ticker") or "").upper().strip()
    }
    tickers.update(
        str(ticker or "").upper().strip()
        for ticker in (evidence.get("selected_tickers") or [])
        if str(ticker or "").upper().strip()
    )
    return sorted(tickers)


def _independence_check(
    *,
    strategy_id: str,
    evidence_index: dict[str, dict[str, Any]],
    diagnostics: dict[str, Any],
) -> dict[str, Any]:
    pairs = diagnostics.get("high_correlation_pairs") or []
    conflicts: list[dict[str, Any]] = []
    for pair in pairs:
        if not isinstance(pair, dict):
            continue
        left = str(pair.get("left") or "")
        right = str(pair.get("right") or "")
        if strategy_id not in {left, right}:
            continue
        other = right if left == strategy_id else left
        other_use = _effective_strategy_use(evidence_index.get(other, {}))
        if other_use in ACTIONABLE_USES:
            conflicts.append({
                "strategy": other,
                "other_use": other_use,
                "correlation": _to_float(pair.get("correlation")),
                "overlap": pair.get("overlap"),
            })
    return {
        "passes": not conflicts,
        "status": diagnostics.get("status") or "missing",
        "conflicts": conflicts,
        "reasons": [
            f"high_correlation_with_actionable:{item['strategy']}:{item.get('correlation')}"
            for item in conflicts
        ],
    }


def _regime_coverage_check(
    *,
    strong_profiles: list[dict[str, Any]],
    weak_profiles: list[dict[str, Any]],
    all_profiles: list[dict[str, Any]],
) -> dict[str, Any]:
    strong_regimes = sorted({str(row.get("regime") or "unknown") for row in strong_profiles})
    weak_regimes = sorted({str(row.get("regime") or "unknown") for row in weak_profiles})
    all_regimes = sorted({str(row.get("regime") or "unknown") for row in all_profiles})
    mixed = bool(strong_regimes and weak_regimes)
    return {
        "passes": not mixed,
        "strong_regimes": strong_regimes,
        "weak_regimes": weak_regimes,
        "observed_regimes": all_regimes,
        "reasons": [f"mixed_regime_coverage:weak={','.join(weak_regimes)}"] if mixed else [],
    }


def _decay_check(*, tickers: list[str], diagnostics: dict[str, Any]) -> dict[str, Any]:
    rows = diagnostics.get("rows") or []
    by_ticker = {
        str(row.get("ticker") or "").upper().strip(): row
        for row in rows
        if isinstance(row, dict)
    }
    conflicts = []
    for ticker in tickers:
        row = by_ticker.get(ticker)
        if not row:
            continue
        severity = str(row.get("severity") or "unknown")
        if severity in {"high", "extreme"} or row.get("max_hold_policy_warning"):
            conflicts.append({
                "ticker": ticker,
                "severity": severity,
                "max_hold_policy_warning": row.get("max_hold_policy_warning"),
                "reason": row.get("severity_reason"),
            })
    return {
        "passes": not conflicts,
        "status": diagnostics.get("status") or "missing",
        "conflicts": conflicts,
        "reasons": [
            f"decay_review:{item['ticker']}:{item.get('severity')}"
            for item in conflicts
        ],
    }


def _liquidity_check(*, tickers: list[str], diagnostics: dict[str, Any]) -> dict[str, Any]:
    rows = diagnostics.get("rows") or []
    by_ticker = {
        str(row.get("ticker") or "").upper().strip(): row
        for row in rows
        if isinstance(row, dict)
    }
    conflicts = []
    for ticker in tickers:
        row = by_ticker.get(ticker)
        if not row:
            continue
        quality = str(row.get("execution_quality") or "unknown")
        if quality in {"defer_weak_signals", "no_trade_review"}:
            conflicts.append({
                "ticker": ticker,
                "execution_quality": quality,
                "liquidity_bucket": row.get("liquidity_bucket"),
                "spread_cost_proxy_pct": row.get("spread_cost_proxy_pct"),
            })
    return {
        "passes": not conflicts,
        "status": diagnostics.get("status") or "missing",
        "conflicts": conflicts,
        "reasons": [
            f"liquidity_review:{item['ticker']}:{item.get('execution_quality')}"
            for item in conflicts
        ],
    }


def _cost_check(*, evidence: dict[str, Any], latest_alpha: dict[str, Any]) -> dict[str, Any]:
    conflicts: list[dict[str, Any]] = []
    estimated_cost = _to_float(evidence.get("estimated_cost_pct"))
    turnover = _to_float(evidence.get("turnover"))
    latest_low_edge = _to_int(latest_alpha.get("low_edge_trade_count"), 0)
    if estimated_cost is not None and estimated_cost > MAX_PROMOTION_ESTIMATED_COST_PCT:
        conflicts.append({
            "type": "strategy_estimated_cost_high",
            "estimated_cost_pct": estimated_cost,
            "threshold": MAX_PROMOTION_ESTIMATED_COST_PCT,
        })
    if turnover is not None and turnover > MAX_PROMOTION_TURNOVER:
        conflicts.append({
            "type": "strategy_turnover_high",
            "turnover": turnover,
            "threshold": MAX_PROMOTION_TURNOVER,
        })
    if latest_low_edge and latest_low_edge > 0:
        conflicts.append({
            "type": "recent_transaction_cost_gate_low_edge",
            "low_edge_trade_count": latest_low_edge,
            "min_edge_to_cost_ratio": latest_alpha.get("min_edge_to_cost_ratio"),
        })
    return {
        "passes": not conflicts,
        "conflicts": conflicts,
        "estimated_cost_pct": estimated_cost,
        "turnover": turnover,
        "latest_low_edge_trade_count": latest_low_edge,
        "reasons": [
            str(item["type"])
            for item in conflicts
        ],
    }


def _effective_strategy_use(evidence: dict[str, Any]) -> str:
    return str(
        evidence.get("approved_use")
        or evidence.get("suggested_use")
        or "unknown"
    )


def _profile_row(value: Any) -> dict[str, Any]:
    strategy_id = str(_record_get(value, "strategy_id") or "").strip()
    family, alpha_source = _strategy_family(strategy_id)
    diagnostics = _record_get(value, "diagnostics")
    diagnostics = diagnostics if isinstance(diagnostics, dict) else {}
    n = _to_int(_record_get(value, "n"), 0)
    hit_rate = _to_float(_record_get(value, "hit_rate"))
    stats = statistical_interpretation(n=n, hit_rate=hit_rate)
    hit_rate_ci = diagnostics.get("hit_rate_ci") or stats["hit_rate_ci"] or {}
    construction_epoch = construction_epoch_from_diagnostics(diagnostics)
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
        "statistical_status": diagnostics.get("statistical_status") or stats["statistical_status"],
        "hit_rate_ci": hit_rate_ci or None,
        "hit_rate_ci_width": _to_float(diagnostics.get("hit_rate_ci_width", hit_rate_ci.get("width"))),
        "construction_epoch_id": construction_epoch.get("epoch_id"),
        "pc_mode": construction_epoch.get("pc_mode"),
        "construction_objective_version": construction_epoch.get("construction_objective_version"),
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
            "confidence_score": row.get("confidence_score"),
            "data_ready": row.get("data_ready"),
            "can_influence_allocation": row.get("can_influence_allocation"),
            "selected_tickers": row.get("selected_tickers") or [],
            "estimated_cost_pct": _to_float(row.get("estimated_cost_pct")),
            "turnover": _to_float(row.get("turnover")),
            "reason_codes": row.get("reason_codes") or [],
        }

    certification = evidence.get("strategy_certification") if isinstance(evidence.get("strategy_certification"), dict) else {}
    for name, row in (certification.get("items") or {}).items():
        if not isinstance(row, dict):
            continue
        current = index.setdefault(str(name), {})
        current["certification_status"] = row.get("status")
        current["approved_use"] = row.get("approved_use")
        current["promotion_blockers"] = row.get("promotion_blockers") or []
        current["demotion_reasons"] = row.get("demotion_reasons") or []
    return index


def _strategy_family(strategy_id: str) -> tuple[str, bool]:
    try:
        from strategies import get_strategy

        strategy = get_strategy(strategy_id)
        card = strategy.strategy_card()
        family = canonical_strategy_family(card.get("canonical_family") or card.get("family"))
        alpha_source = is_strategy_alpha_source(strategy_id, family, card.get("alpha_source"))
        return family, bool(alpha_source)
    except Exception:
        return canonical_strategy_family(None), False


def _is_promotable_profile(row: dict[str, Any]) -> bool:
    hit_rate = row.get("hit_rate")
    avg_excess = row.get("avg_excess_vs_spy")
    ic = row.get("ic")
    return bool(
        hit_rate is not None
        and float(hit_rate) >= PROMOTE_HIT_RATE_THRESHOLD
        and (avg_excess is None or float(avg_excess) > 0)
        and (ic is None or float(ic) >= 0)
        and int(row.get("n") or 0) >= PROMOTE_MIN_TOTAL_N
        and row.get("statistical_status") in STATISTICAL_PROMOTION_STATUSES
    )


def _is_directionally_positive(row: dict[str, Any]) -> bool:
    hit_rate = row.get("hit_rate")
    avg_excess = row.get("avg_excess_vs_spy")
    ic = row.get("ic")
    return bool(
        hit_rate is not None
        and float(hit_rate) >= PROMOTE_HIT_RATE_THRESHOLD
        and (avg_excess is None or float(avg_excess) > 0)
        and (ic is None or float(ic) >= 0)
    )


def _is_weak_profile(row: dict[str, Any]) -> bool:
    hit_rate = row.get("hit_rate")
    avg_excess = row.get("avg_excess_vs_spy")
    ic = row.get("ic")
    return bool(
        (hit_rate is not None and float(hit_rate) < DEGRADE_HIT_RATE_THRESHOLD)
        or (avg_excess is not None and float(avg_excess) < 0)
        or (ic is not None and float(ic) < 0)
    )


def _weak_reasons(rows: list[dict[str, Any]]) -> list[str]:
    reasons: list[str] = []
    for row in rows:
        if row.get("hit_rate") is not None and float(row["hit_rate"]) < DEGRADE_HIT_RATE_THRESHOLD:
            reasons.append("hit_rate_below_45pct")
        if row.get("avg_excess_vs_spy") is not None and float(row["avg_excess_vs_spy"]) < 0:
            reasons.append("negative_excess_vs_spy")
        if row.get("ic") is not None and float(row["ic"]) < 0:
            reasons.append("negative_ic")
    return _unique(reasons)


def _summary_warnings(recommendations: list[dict[str, Any]], gap_summary: dict[str, Any]) -> list[str]:
    warnings = list(gap_summary.get("warnings") or [])
    for row in recommendations:
        recommendation = row.get("recommendation")
        if recommendation in {"demote_to_watch_only_review", "archive_family_regime_review"}:
            warnings.append(f"{recommendation}:{row.get('strategy_id') or row.get('canonical_family')}:{row.get('regime')}")
        if recommendation == "require_promotion_evidence_alignment":
            warnings.append(f"promotion_evidence_alignment:{row.get('strategy_id')}:{','.join(row.get('blockers') or [])}")
    return sorted(set(str(item) for item in warnings if item))


def _recommendation_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        key = str(row.get("recommendation") or "unknown")
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items()))


def _sorted_unique_recommendations(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rank = {"high": 0, "medium": 1, "low": 2}
    seen: set[tuple[Any, ...]] = set()
    out: list[dict[str, Any]] = []
    for row in rows:
        key = (
            row.get("recommendation"),
            row.get("strategy_id"),
            row.get("canonical_family"),
            row.get("regime"),
            row.get("recommended_use"),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return sorted(
        out,
        key=lambda row: (
            rank.get(str(row.get("priority") or ""), 9),
            str(row.get("recommendation") or ""),
            str(row.get("strategy_id") or row.get("canonical_family") or ""),
            str(row.get("regime") or ""),
        ),
    )


def _alpha_run_row(row: Any) -> dict[str, Any]:
    return {
        "analysis_id": _record_get(row, "analysis_id"),
        "generated_at": _iso(_record_get(row, "generated_at")),
        "status": _record_get(row, "status"),
        "independent_alpha_family_count": _record_get(row, "independent_alpha_family_count"),
        "calibrated_conviction_count": _record_get(row, "calibrated_conviction_count"),
        "cost_gate_status": _record_get(row, "cost_gate_status"),
        "low_edge_trade_count": _record_get(row, "low_edge_trade_count"),
        "min_edge_to_cost_ratio": _record_get(row, "min_edge_to_cost_ratio"),
        "avg_edge_to_cost_ratio": _record_get(row, "avg_edge_to_cost_ratio"),
    }


def _weighted_average(rows: list[dict[str, Any]], field: str) -> float | None:
    total = 0.0
    weight_sum = 0
    for row in rows:
        value = row.get(field)
        if value is None:
            continue
        weight = max(int(row.get("n") or 0), 1)
        total += float(value) * weight
        weight_sum += weight
    if weight_sum <= 0:
        return None
    return round(total / weight_sum, 6)


def _max_float(rows: list[dict[str, Any]], field: str) -> float | None:
    values = [_to_float(row.get(field)) for row in rows]
    clean = [value for value in values if value is not None]
    if not clean:
        return None
    return round(max(clean), 6)


def _status_counts(rows: list[dict[str, Any]], field: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        key = str(row.get(field) or "unknown")
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items()))


def _first(rows: list[dict[str, Any]], key: str) -> str:
    for row in rows:
        value = row.get(key)
        if value:
            return str(value)
    return "unknown"


def _source_rank(row: dict[str, Any]) -> int:
    return SOURCE_BUCKET_PRIORITY.get(str(row.get("source_bucket") or ""), 9)


def _record_get(value: Any, field: str) -> Any:
    if isinstance(value, dict):
        return value.get(field)
    return getattr(value, field, None)


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(value) if value is not None else default
    except (TypeError, ValueError):
        return default


def _unique(values: list[Any]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        text = str(value or "")
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


def _iso(value: Any) -> str | None:
    return value.isoformat() if hasattr(value, "isoformat") else None
