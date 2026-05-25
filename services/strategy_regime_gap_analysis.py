"""Strategy family and regime gap diagnostics.

This module answers whether validated strategy evidence covers the market
regimes we care about. It is read-only and has no execution authority.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

from services.strategy_conviction import (
    SOURCE_BUCKET_COMBINED,
    SOURCE_BUCKET_HISTORICAL_PRIOR,
    SOURCE_BUCKET_LIVE_PAPER,
    STATUS_CALIBRATED,
)
from services.strategy_diversity import (
    ALPHA_FAMILIES,
    canonical_strategy_family,
    is_strategy_alpha_source,
)


EXPECTED_REGIMES = ("trending_bull", "risk_on", "mean_reverting", "defensive", "high_vol")
EXPECTED_FAMILIES_BY_REGIME = {
    "trending_bull": ("momentum",),
    "risk_on": ("momentum",),
    "mean_reverting": ("mean_reversion", "low_vol_defensive"),
    "defensive": ("low_vol_defensive", "carry_or_cash_proxy", "volatility_hedge"),
    "high_vol": ("low_vol_defensive", "volatility_hedge", "carry_or_cash_proxy"),
}
SOURCE_BUCKET_PRIORITY = {
    SOURCE_BUCKET_COMBINED: 0,
    SOURCE_BUCKET_LIVE_PAPER: 1,
    SOURCE_BUCKET_HISTORICAL_PRIOR: 2,
}
WEAK_HIT_RATE_THRESHOLD = 0.45


async def load_strategy_regime_gap_analysis(
    db: Any,
    *,
    as_of_date: date | None = None,
    row_limit: int = 5000,
) -> dict[str, Any]:
    """Load latest conviction profiles and build gap analysis."""
    from sqlalchemy import desc, func, select

    from db.models import AlphaValidationRun, StrategyConvictionProfile

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
                StrategyConvictionProfile.regime_at_signal,
                StrategyConvictionProfile.strategy_id,
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
    summary = build_strategy_regime_gap_analysis(
        profiles=profile_rows,
        alpha_validation_runs=alpha_rows,
        as_of_date=target_date,
    )
    if latest_profile_date is not None:
        summary["latest_profile_date"] = latest_profile_date.isoformat()
    return summary


def build_strategy_regime_gap_analysis(
    *,
    profiles: list[Any],
    alpha_validation_runs: list[Any] | None = None,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    profile_rows = _dedupe_profiles([_profile_row(item) for item in profiles])
    alpha_rows = [_alpha_run_row(row) for row in (alpha_validation_runs or [])]
    calibrated_alpha = [
        row for row in profile_rows
        if row["alpha_source"] and row["status"] == STATUS_CALIBRATED
    ]
    regime_rows = _regime_rows(calibrated_alpha)
    family_rows = _family_rows(calibrated_alpha)
    weak_rows = _weak_family_regime_rows(calibrated_alpha)
    actionable_families = sorted({
        row["canonical_family"]
        for row in calibrated_alpha
        if row["canonical_family"] in ALPHA_FAMILIES
    })
    uncovered_regimes = [
        row["regime"]
        for row in regime_rows
        if row["coverage_status"] != "covered"
    ]
    momentum_overconcentration = actionable_families == ["momentum"]
    research_queue = _research_queue(
        regime_rows=regime_rows,
        weak_rows=weak_rows,
        actionable_families=actionable_families,
    )
    warnings = _warnings(
        uncovered_regimes=uncovered_regimes,
        weak_rows=weak_rows,
        momentum_overconcentration=momentum_overconcentration,
    )

    status = "gap_detected" if warnings else "covered"
    if not profile_rows:
        status = "insufficient_data"

    return {
        "contract_version": "strategy_regime_gap_analysis_v1",
        "status": status,
        "as_of_date": (as_of_date or datetime.now(timezone.utc).date()).isoformat(),
        "execution_authority": "none",
        "target_weight_mutation": "none",
        "expected_regimes": list(EXPECTED_REGIMES),
        "expected_alpha_families": list(ALPHA_FAMILIES),
        "profile_count": len(profile_rows),
        "calibrated_alpha_profile_count": len(calibrated_alpha),
        "actionable_alpha_families": actionable_families,
        "actionable_alpha_family_count": len(actionable_families),
        "momentum_overconcentration": momentum_overconcentration,
        "uncovered_regimes": uncovered_regimes,
        "regime_rows": regime_rows,
        "family_rows": family_rows,
        "weak_family_regime_rows": weak_rows,
        "research_queue": research_queue,
        "latest_alpha_validation": alpha_rows[0] if alpha_rows else {},
        "alpha_validation_sample_count": len(alpha_rows),
        "warnings": warnings,
    }


def _profile_row(value: Any) -> dict[str, Any]:
    strategy_id = str(_record_get(value, "strategy_id") or "").strip()
    family, alpha_source = _strategy_family(strategy_id)
    return {
        "strategy_id": strategy_id,
        "ticker": str(_record_get(value, "ticker") or "").upper().strip(),
        "branch": _record_get(value, "branch"),
        "action": str(_record_get(value, "action") or ""),
        "regime": str(_record_get(value, "regime_at_signal") or "unknown"),
        "horizon_days": _to_int(_record_get(value, "horizon_days"), 0),
        "source_bucket": str(_record_get(value, "source_bucket") or "unknown"),
        "status": str(_record_get(value, "status") or "unknown"),
        "n": _to_int(_record_get(value, "n"), 0),
        "hit_rate": _to_float(_record_get(value, "hit_rate")),
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
        )
        current = best.get(key)
        if current is None or _source_rank(row) < _source_rank(current):
            best[key] = row
    return sorted(best.values(), key=lambda row: (
        row["regime"],
        row["canonical_family"],
        row["strategy_id"],
        row["ticker"],
    ))


def _regime_rows(calibrated_alpha: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    by_regime: dict[str, list[dict[str, Any]]] = {}
    for row in calibrated_alpha:
        by_regime.setdefault(row["regime"], []).append(row)

    for regime in EXPECTED_REGIMES:
        items = by_regime.get(regime, [])
        families = sorted({row["canonical_family"] for row in items})
        expected = list(EXPECTED_FAMILIES_BY_REGIME.get(regime, ()))
        missing_expected = sorted(set(expected) - set(families))
        coverage_status = "covered" if families else "missing_calibrated_coverage"
        if families and not set(families) & set(expected):
            coverage_status = "covered_by_non_preferred_family"
        rows.append({
            "regime": regime,
            "coverage_status": coverage_status,
            "calibrated_profile_count": len(items),
            "calibrated_families": families,
            "expected_families": expected,
            "missing_expected_families": missing_expected,
            "hit_rate": _weighted_average(items, "hit_rate"),
            "avg_excess_vs_spy": _weighted_average(items, "avg_excess_vs_spy"),
            "ic": _weighted_average(items, "ic"),
            "total_n": sum(int(row.get("n") or 0) for row in items),
        })
    return rows


def _family_rows(calibrated_alpha: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_family: dict[str, list[dict[str, Any]]] = {}
    for row in calibrated_alpha:
        by_family.setdefault(row["canonical_family"], []).append(row)
    rows = []
    for family, items in sorted(by_family.items()):
        weak_regimes = sorted({
            row["regime"]
            for row in items
            if _is_weak_profile(row)
        })
        rows.append({
            "family": family,
            "calibrated_profile_count": len(items),
            "covered_regimes": sorted({row["regime"] for row in items}),
            "weak_regimes": weak_regimes,
            "hit_rate": _weighted_average(items, "hit_rate"),
            "avg_excess_vs_spy": _weighted_average(items, "avg_excess_vs_spy"),
            "ic": _weighted_average(items, "ic"),
            "total_n": sum(int(row.get("n") or 0) for row in items),
        })
    return rows


def _weak_family_regime_rows(calibrated_alpha: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in calibrated_alpha:
        grouped.setdefault((row["canonical_family"], row["regime"]), []).append(row)
    out = []
    for (family, regime), items in sorted(grouped.items()):
        hit_rate = _weighted_average(items, "hit_rate")
        avg_excess = _weighted_average(items, "avg_excess_vs_spy")
        ic = _weighted_average(items, "ic")
        reasons = []
        if hit_rate is not None and hit_rate < WEAK_HIT_RATE_THRESHOLD:
            reasons.append("hit_rate_below_45pct")
        if avg_excess is not None and avg_excess < 0:
            reasons.append("negative_excess_vs_spy")
        if ic is not None and ic < 0:
            reasons.append("negative_ic")
        if reasons:
            out.append({
                "family": family,
                "regime": regime,
                "profile_count": len(items),
                "hit_rate": hit_rate,
                "avg_excess_vs_spy": avg_excess,
                "ic": ic,
                "total_n": sum(int(row.get("n") or 0) for row in items),
                "reasons": reasons,
            })
    return out


def _research_queue(
    *,
    regime_rows: list[dict[str, Any]],
    weak_rows: list[dict[str, Any]],
    actionable_families: list[str],
) -> list[dict[str, Any]]:
    queue: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for row in regime_rows:
        if row["coverage_status"] == "covered":
            continue
        for family in row.get("missing_expected_families") or row.get("expected_families") or []:
            key = (row["regime"], family, "missing_regime_coverage")
            if key in seen:
                continue
            seen.add(key)
            queue.append({
                "priority": "high" if row["coverage_status"] == "missing_calibrated_coverage" else "medium",
                "regime": row["regime"],
                "suggested_family": family,
                "reason": row["coverage_status"],
            })
    for weak in weak_rows:
        suggested = _fallback_family_for_weak_regime(weak["regime"], actionable_families)
        key = (weak["regime"], suggested, "family_degraded")
        if key in seen:
            continue
        seen.add(key)
        queue.append({
            "priority": "high" if weak["family"] == "momentum" else "medium",
            "regime": weak["regime"],
            "suggested_family": suggested,
            "reason": f"{weak['family']}_degraded:{','.join(weak.get('reasons') or [])}",
        })
    return queue


def _fallback_family_for_weak_regime(regime: str, actionable_families: list[str]) -> str:
    expected = EXPECTED_FAMILIES_BY_REGIME.get(regime, ())
    for family in expected:
        if family not in actionable_families:
            return family
    return expected[0] if expected else "mean_reversion"


def _warnings(
    *,
    uncovered_regimes: list[str],
    weak_rows: list[dict[str, Any]],
    momentum_overconcentration: bool,
) -> list[str]:
    warnings = []
    for regime in uncovered_regimes:
        warnings.append(f"missing_calibrated_regime_coverage:{regime}")
    for row in weak_rows:
        warnings.append(f"family_regime_degraded:{row['family']}:{row['regime']}")
    if momentum_overconcentration:
        warnings.append("momentum_only_actionable_alpha_family")
    return sorted(set(warnings))


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


def _alpha_run_row(row: Any) -> dict[str, Any]:
    return {
        "analysis_id": _record_get(row, "analysis_id"),
        "generated_at": _iso(_record_get(row, "generated_at")),
        "status": _record_get(row, "status"),
        "independent_alpha_family_count": _record_get(row, "independent_alpha_family_count"),
        "calibrated_conviction_count": _record_get(row, "calibrated_conviction_count"),
    }


def _is_weak_profile(row: dict[str, Any]) -> bool:
    hit_rate = row.get("hit_rate")
    avg_excess = row.get("avg_excess_vs_spy")
    ic = row.get("ic")
    return bool(
        (hit_rate is not None and float(hit_rate) < WEAK_HIT_RATE_THRESHOLD)
        or (avg_excess is not None and float(avg_excess) < 0)
        or (ic is not None and float(ic) < 0)
    )


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


def _iso(value: Any) -> str | None:
    return value.isoformat() if hasattr(value, "isoformat") else None
