"""Read-only validation summaries for strategy signal conviction.

This module is the PR8 display layer. It does not compute new convictions and
does not authorize execution; it only reshapes frozen signals, outcomes, and
conviction profiles into operator-friendly summaries.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable

from services.construction_epoch import construction_epoch_from_diagnostics, construction_epoch_from_signal
from services.historical_signal_replay import DEFAULT_HORIZONS
from services.signal_outcome_labeler import frozen_signal_from_record
from services.strategy_conviction import (
    SOURCE_BUCKET_COMBINED,
    SOURCE_BUCKET_HISTORICAL_PRIOR,
    SOURCE_BUCKET_LIVE_PAPER,
    ConvictionProfile,
    signal_outcome_from_record,
    statistical_interpretation,
)


PROFILE_BUCKETS = (
    SOURCE_BUCKET_HISTORICAL_PRIOR,
    SOURCE_BUCKET_LIVE_PAPER,
    SOURCE_BUCKET_COMBINED,
)


def build_validation_dashboard_summary(
    *,
    signals: Iterable[Any],
    outcomes: Iterable[Any],
    profiles: Iterable[Any],
    as_of_date: date | None = None,
    horizons: tuple[int, ...] = DEFAULT_HORIZONS,
    profile_limit: int = 12,
) -> dict[str, Any]:
    """Build a compact validation summary for Playground/dashboard display."""
    signal_rows = [
        signal
        for signal in (frozen_signal_from_record(item) for item in signals)
        if signal is not None
    ]
    outcome_rows = [
        outcome
        for outcome in (signal_outcome_from_record(item) for item in outcomes)
        if outcome is not None
    ]
    profile_rows = [
        profile
        for profile in (_profile_from_record(item) for item in profiles)
        if profile is not None
    ]
    if as_of_date is None:
        as_of_date = _infer_as_of_date(signal_rows, outcome_rows, profile_rows)

    profile_display = [_profile_display_row(profile, signal_rows) for profile in profile_rows]
    by_bucket = {
        bucket: sorted(
            [row for row in profile_display if row.get("source_bucket") == bucket],
            key=_profile_display_sort_key,
        )[:profile_limit]
        for bucket in PROFILE_BUCKETS
    }
    pending = _pending_outcome_summary(signal_rows, outcome_rows, as_of_date, horizons)
    requires_live = sum(
        1
        for profile in profile_rows
        if profile.source_bucket == SOURCE_BUCKET_COMBINED and profile.requires_live_confirmation
    )
    status_counts: dict[str, int] = {}
    for profile in profile_rows:
        status_counts[profile.status] = status_counts.get(profile.status, 0) + 1

    return {
        "contract_version": "strategy_validation_dashboard_v1",
        "status": "available",
        "as_of_date": as_of_date.isoformat(),
        "signals_recorded_today": sum(1 for signal in signal_rows if signal.signal_date == as_of_date),
        "outcomes_labeled_today": sum(
            1 for outcome in outcome_rows if _date_of(outcome.created_at) == as_of_date
        ),
        "signals_total": len(signal_rows),
        "outcomes_total": len(outcome_rows),
        "pending_outcomes": pending,
        "historical_prior_profiles": by_bucket[SOURCE_BUCKET_HISTORICAL_PRIOR],
        "live_paper_profiles": by_bucket[SOURCE_BUCKET_LIVE_PAPER],
        "combined_profiles": by_bucket[SOURCE_BUCKET_COMBINED],
        "requires_live_confirmation_count": requires_live,
        "profile_counts": {
            "historical_prior": sum(1 for p in profile_rows if p.source_bucket == SOURCE_BUCKET_HISTORICAL_PRIOR),
            "live_paper": sum(1 for p in profile_rows if p.source_bucket == SOURCE_BUCKET_LIVE_PAPER),
            "combined": sum(1 for p in profile_rows if p.source_bucket == SOURCE_BUCKET_COMBINED),
        },
        "regime_level_profiles": sorted(profile_display, key=_regime_profile_sort_key)[: max(profile_limit * 3, profile_limit)],
        "regime_summary_rows": _regime_summary_rows(profile_display),
        "status_counts": dict(sorted(status_counts.items())),
        "display_note": "observe_only_no_execution_authority",
    }


async def load_validation_dashboard_summary(
    db: Any,
    *,
    as_of_date: date | None = None,
    profile_limit: int = 12,
    row_limit: int = 5000,
) -> dict[str, Any]:
    """Load the latest validation state from DB and render a dashboard summary."""
    from sqlalchemy import desc, func, select

    from db.models import StrategyConvictionProfile, StrategyFrozenSignal, StrategySignalOutcome

    target_date = as_of_date or datetime.now(timezone.utc).date()
    signals_result = await db.execute(
        select(StrategyFrozenSignal)
        .where(StrategyFrozenSignal.signal_date <= target_date)
        .order_by(desc(StrategyFrozenSignal.signal_date), desc(StrategyFrozenSignal.id))
        .limit(row_limit)
    )
    outcomes_result = await db.execute(
        select(StrategySignalOutcome)
        .where(StrategySignalOutcome.label_date <= target_date)
        .order_by(desc(StrategySignalOutcome.label_date), desc(StrategySignalOutcome.id))
        .limit(row_limit)
    )
    latest_profile_date_result = await db.execute(
        select(func.max(StrategyConvictionProfile.as_of_date)).where(
            StrategyConvictionProfile.as_of_date <= target_date
        )
    )
    latest_profile_date = latest_profile_date_result.scalar_one_or_none()
    profile_rows: list[Any] = []
    if latest_profile_date is not None:
        profiles_result = await db.execute(
            select(StrategyConvictionProfile)
            .where(StrategyConvictionProfile.as_of_date == latest_profile_date)
            .order_by(
                StrategyConvictionProfile.source_bucket,
                StrategyConvictionProfile.strategy_id,
                StrategyConvictionProfile.ticker,
            )
            .limit(row_limit)
        )
        profile_rows = list(profiles_result.scalars().all())

    summary = build_validation_dashboard_summary(
        signals=signals_result.scalars().all(),
        outcomes=outcomes_result.scalars().all(),
        profiles=profile_rows,
        as_of_date=target_date,
        profile_limit=profile_limit,
    )
    if latest_profile_date is not None:
        summary["latest_profile_date"] = latest_profile_date.isoformat()
    return summary


def _pending_outcome_summary(
    signals: list[Any],
    outcomes: list[Any],
    as_of_date: date,
    horizons: tuple[int, ...],
) -> dict[str, Any]:
    existing = {
        (outcome.signal_id, int(outcome.horizon_days), outcome.outcome_source)
        for outcome in outcomes
    }
    by_horizon: dict[str, dict[str, int]] = {
        str(horizon): {"missing": 0, "mature": 0}
        for horizon in horizons
    }
    total_missing = 0
    mature_missing = 0
    for signal in signals:
        for horizon in horizons:
            key = (signal.signal_id, int(horizon), "yfinance")
            if key in existing:
                continue
            total_missing += 1
            by_horizon[str(horizon)]["missing"] += 1
            mature_date = signal.tradable_from_date + timedelta(days=max(1, int(horizon)) - 1)
            if mature_date <= as_of_date:
                mature_missing += 1
                by_horizon[str(horizon)]["mature"] += 1
    return {
        "total": total_missing,
        "mature": mature_missing,
        "by_horizon": by_horizon,
        "maturity_model": "calendar_day_approximation_for_display",
    }


def _profile_display_row(profile: ConvictionProfile, signals: list[Any]) -> dict[str, Any]:
    last_signal_date = _last_signal_date_for_profile(profile, signals)
    diagnostics = profile.diagnostics or {}
    construction_epoch = construction_epoch_from_diagnostics(diagnostics)
    stats = statistical_interpretation(n=profile.n, hit_rate=profile.hit_rate)
    statistical_status = diagnostics.get("statistical_status") or stats["statistical_status"]
    hit_rate_ci = diagnostics.get("hit_rate_ci") or stats["hit_rate_ci"]
    hit_rate_ci_width = diagnostics.get("hit_rate_ci_width", stats["hit_rate_ci_width"])
    return {
        "strategy": profile.strategy_id,
        "ticker": profile.ticker,
        "branch": profile.branch,
        "action": profile.action,
        "regime_at_signal": profile.regime_at_signal,
        "horizon": profile.horizon_days,
        "source_bucket": profile.source_bucket,
        "n": profile.n,
        "status": profile.status,
        "hit_rate": profile.hit_rate,
        "hit_rate_ci": hit_rate_ci,
        "hit_rate_ci_width": hit_rate_ci_width,
        "avg_excess_vs_spy": profile.avg_excess_vs_spy,
        "ic": profile.ic,
        "conviction": profile.conviction,
        "conviction_display": _format_conviction(profile.conviction),
        "statistical_status": statistical_status,
        "construction_epoch": construction_epoch,
        "construction_epoch_id": construction_epoch.get("epoch_id"),
        "pc_mode": construction_epoch.get("pc_mode"),
        "construction_objective_version": construction_epoch.get("construction_objective_version"),
        "policy_version": construction_epoch.get("policy_version"),
        "promotion_config_hash": construction_epoch.get("promotion_config_hash"),
        "last_signal_date": last_signal_date.isoformat() if last_signal_date else None,
        "data_lag_filtered": profile.data_lag_filtered,
        "source_counts": dict(profile.source_counts or {}),
        "requires_live_confirmation": profile.requires_live_confirmation,
    }


def _last_signal_date_for_profile(profile: ConvictionProfile, signals: list[Any]) -> date | None:
    profile_epoch = construction_epoch_from_diagnostics(profile.diagnostics or {}).get("epoch_id")
    dates = [
        signal.signal_date
        for signal in signals
        if signal.strategy_id == profile.strategy_id
        and signal.ticker == profile.ticker
        and signal.branch == profile.branch
        and signal.action == profile.action
        and signal.regime_at_signal == profile.regime_at_signal
        and construction_epoch_from_signal(signal).get("epoch_id") == profile_epoch
    ]
    return max(dates) if dates else None


def _profile_display_sort_key(row: dict[str, Any]) -> tuple[int, int, float, str, str, str]:
    status_rank = {
        "calibrated": 0,
        "early_live_confirmation": 1,
        "early_estimate": 2,
        "historical_prior_requires_live_confirmation": 3,
        "insufficient_samples": 4,
    }
    conviction = row.get("conviction")
    conviction_sort = float(conviction) if isinstance(conviction, (int, float)) else -1.0
    return (
        status_rank.get(str(row.get("status") or ""), 9),
        -int(row.get("n") or 0),
        -conviction_sort,
        str(row.get("strategy") or ""),
        str(row.get("ticker") or ""),
        str(row.get("construction_epoch_id") or ""),
    )


def _regime_profile_sort_key(row: dict[str, Any]) -> tuple[str, str, str, int, int, float, str, str]:
    status_rank = {
        "calibrated": 0,
        "early_live_confirmation": 1,
        "early_estimate": 2,
        "historical_prior_requires_live_confirmation": 3,
        "insufficient_samples": 4,
    }
    conviction = row.get("conviction")
    conviction_sort = float(conviction) if isinstance(conviction, (int, float)) else -1.0
    return (
        str(row.get("regime_at_signal") or "unknown"),
        str(row.get("source_bucket") or ""),
        str(row.get("construction_epoch_id") or "unknown"),
        status_rank.get(str(row.get("status") or ""), 9),
        -int(row.get("n") or 0),
        -conviction_sort,
        str(row.get("strategy") or ""),
        str(row.get("ticker") or ""),
    )


def _regime_summary_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in rows:
        key = (
            str(row.get("regime_at_signal") or "unknown"),
            str(row.get("source_bucket") or "unknown"),
            str(row.get("construction_epoch_id") or "unknown"),
        )
        group = grouped.setdefault(
            key,
            {
                "regime_at_signal": key[0],
                "source_bucket": key[1],
                "construction_epoch_id": key[2],
                "pc_mode": row.get("pc_mode"),
                "construction_objective_version": row.get("construction_objective_version"),
                "policy_version": row.get("policy_version"),
                "profile_count": 0,
                "total_n": 0,
                "calibrated_profiles": 0,
                "early_profiles": 0,
                "insufficient_profiles": 0,
                "statistical_status_counts": {},
                "hit_rate_weighted_sum": 0.0,
                "avg_excess_weighted_sum": 0.0,
                "ic_weighted_sum": 0.0,
                "hit_rate_ci_width_weighted_sum": 0.0,
                "hit_rate_n": 0,
                "avg_excess_n": 0,
                "ic_n": 0,
                "hit_rate_ci_width_n": 0,
                "data_lag_filtered": 0,
            },
        )
        n = max(int(row.get("n") or 0), 0)
        weight = max(n, 1)
        status = str(row.get("status") or "")
        group["profile_count"] += 1
        group["total_n"] += n
        group["data_lag_filtered"] += int(row.get("data_lag_filtered") or 0)
        if status == "calibrated":
            group["calibrated_profiles"] += 1
        elif status == "insufficient_samples":
            group["insufficient_profiles"] += 1
        else:
            group["early_profiles"] += 1
        statistical_status = str(row.get("statistical_status") or "unknown")
        group["statistical_status_counts"][statistical_status] = (
            group["statistical_status_counts"].get(statistical_status, 0) + 1
        )
        if row.get("hit_rate") is not None:
            group["hit_rate_weighted_sum"] += float(row.get("hit_rate")) * weight
            group["hit_rate_n"] += weight
        if row.get("avg_excess_vs_spy") is not None:
            group["avg_excess_weighted_sum"] += float(row.get("avg_excess_vs_spy")) * weight
            group["avg_excess_n"] += weight
        if row.get("ic") is not None:
            group["ic_weighted_sum"] += float(row.get("ic")) * weight
            group["ic_n"] += weight
        if row.get("hit_rate_ci_width") is not None:
            group["hit_rate_ci_width_weighted_sum"] += float(row.get("hit_rate_ci_width")) * weight
            group["hit_rate_ci_width_n"] += weight

    out = []
    for group in grouped.values():
        out.append({
            "regime_at_signal": group["regime_at_signal"],
            "source_bucket": group["source_bucket"],
            "construction_epoch_id": group["construction_epoch_id"],
            "pc_mode": group.get("pc_mode"),
            "construction_objective_version": group.get("construction_objective_version"),
            "policy_version": group.get("policy_version"),
            "profile_count": group["profile_count"],
            "total_n": group["total_n"],
            "calibrated_profiles": group["calibrated_profiles"],
            "early_profiles": group["early_profiles"],
            "insufficient_profiles": group["insufficient_profiles"],
            "statistical_status_counts": dict(sorted(group["statistical_status_counts"].items())),
            "hit_rate": _weighted_average(group, "hit_rate"),
            "hit_rate_ci_width": _weighted_average(group, "hit_rate_ci_width"),
            "avg_excess_vs_spy": _weighted_average(group, "avg_excess"),
            "ic": _weighted_average(group, "ic"),
            "data_lag_filtered": group["data_lag_filtered"],
        })
    return sorted(
        out,
        key=lambda row: (
            str(row.get("regime_at_signal") or "unknown"),
            str(row.get("source_bucket") or "unknown"),
            str(row.get("construction_epoch_id") or "unknown"),
            -int(row.get("total_n") or 0),
        ),
    )


def _weighted_average(group: dict[str, Any], prefix: str) -> float | None:
    n = int(group.get(f"{prefix}_n") or 0)
    if n <= 0:
        return None
    return round(float(group.get(f"{prefix}_weighted_sum") or 0.0) / n, 6)


def _profile_from_record(value: Any) -> ConvictionProfile | None:
    if isinstance(value, ConvictionProfile):
        return value
    profile_id = _record_get(value, "profile_id")
    as_of = _parse_date(_record_get(value, "as_of_date"))
    if not profile_id or as_of is None:
        return None
    return ConvictionProfile(
        profile_id=str(profile_id),
        as_of_date=as_of,
        strategy_id=str(_record_get(value, "strategy_id") or ""),
        ticker=str(_record_get(value, "ticker") or "").upper().strip(),
        branch=_optional_str(_record_get(value, "branch")),
        action=str(_record_get(value, "action") or ""),
        regime_at_signal=str(_record_get(value, "regime_at_signal") or "unknown"),
        horizon_days=_to_int(_record_get(value, "horizon_days"), 0),
        source_bucket=str(_record_get(value, "source_bucket") or ""),
        conviction=_optional_float(_record_get(value, "conviction")),
        status=str(_record_get(value, "status") or "unknown"),
        n=_to_int(_record_get(value, "n"), 0),
        required_samples=_to_int(_record_get(value, "required_samples"), 30),
        hit_rate=_optional_float(_record_get(value, "hit_rate")),
        avg_forward_return=_optional_float(_record_get(value, "avg_forward_return")),
        avg_excess_vs_spy=_optional_float(_record_get(value, "avg_excess_vs_spy")),
        ic=_optional_float(_record_get(value, "ic")),
        max_adverse_drawdown=_optional_float(_record_get(value, "max_adverse_drawdown")),
        data_lag_filtered=_to_int(_record_get(value, "data_lag_filtered"), 0),
        requires_live_confirmation=bool(_record_get(value, "requires_live_confirmation")),
        hist_n=_to_int(_record_get(value, "hist_n"), 0),
        live_n=_to_int(_record_get(value, "live_n"), 0),
        hist_weight=_optional_float(_record_get(value, "hist_weight")),
        live_weight=_optional_float(_record_get(value, "live_weight")),
        source_counts=dict(_record_get(value, "source_counts") or {}),
        diagnostics=dict(_record_get(value, "diagnostics") or {}),
        created_at=_parse_datetime(_record_get(value, "created_at")) or datetime.now(timezone.utc),
    )


def _infer_as_of_date(signals: list[Any], outcomes: list[Any], profiles: list[ConvictionProfile]) -> date:
    dates = [
        *[signal.signal_date for signal in signals],
        *[outcome.label_date for outcome in outcomes],
        *[profile.as_of_date for profile in profiles],
    ]
    return max(dates) if dates else datetime.now(timezone.utc).date()


def _format_conviction(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value:.0%}"


def _date_of(value: datetime | date | None) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return None


def _record_get(value: Any, field: str) -> Any:
    if isinstance(value, dict):
        return value.get(field)
    return getattr(value, field, None)


def _parse_date(value: Any) -> date | None:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        try:
            return date.fromisoformat(value[:10])
        except ValueError:
            return None
    return None


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None


def _optional_float(value: Any) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(value) if value is not None else default
    except (TypeError, ValueError):
        return default
