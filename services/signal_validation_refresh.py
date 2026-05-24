"""Daily refresh for signal outcomes and conviction profiles.

This is the operational glue after PR5-PR9: read immutable frozen signals,
label mature yfinance outcomes, then recompute derived conviction profiles. It
is observe-only and never writes execution commands or target weights.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

from services.historical_signal_replay import DEFAULT_HORIZONS, FrozenSignal, SignalOutcome
from services.market_feature_store import get_market_daily_feature_rows, model_to_feature_dict
from services.signal_outcome_labeler import (
    label_mature_signal_outcomes,
    persist_signal_outcomes,
)
from services.strategy_conviction import (
    ConvictionProfile,
    compute_conviction_profiles,
    persist_conviction_profiles,
)


@dataclass(frozen=True)
class SignalValidationRefreshPlan:
    candidate_outcomes: list[SignalOutcome]
    profiles: list[ConvictionProfile]
    summary: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "candidate_outcomes": [item.to_dict() for item in self.candidate_outcomes],
            "profiles": [item.to_dict() for item in self.profiles],
            "summary": dict(self.summary),
        }


@dataclass(frozen=True)
class SignalValidationRefreshResult:
    candidate_outcomes: int
    outcomes_inserted: int
    outcome_duplicates: int
    outcome_conflicts: list[dict[str, Any]]
    profiles_generated: int
    profiles_inserted: int
    profiles_updated: int
    profile_duplicates: int
    summary: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "candidate_outcomes": self.candidate_outcomes,
            "outcomes_inserted": self.outcomes_inserted,
            "outcome_duplicates": self.outcome_duplicates,
            "outcome_conflict_count": len(self.outcome_conflicts),
            "outcome_conflicts": list(self.outcome_conflicts),
            "profiles_generated": self.profiles_generated,
            "profiles_inserted": self.profiles_inserted,
            "profiles_updated": self.profiles_updated,
            "profile_duplicates": self.profile_duplicates,
            "summary": dict(self.summary),
        }


def build_signal_validation_refresh_plan(
    *,
    signals: list[Any],
    existing_outcomes: list[Any],
    feature_rows: list[Any],
    as_of_date: date,
    horizons: tuple[int, ...] = DEFAULT_HORIZONS,
    created_at: datetime | None = None,
) -> SignalValidationRefreshPlan:
    """Pure refresh planning helper used by cron and tests."""
    created = created_at or datetime.now(timezone.utc)
    labeled = label_mature_signal_outcomes(
        signals,
        feature_rows,
        as_of_date=as_of_date,
        horizons=horizons,
        created_at=created,
    )
    all_outcomes = _dedupe_outcomes([*existing_outcomes, *labeled.outcomes])
    conviction = compute_conviction_profiles(
        signals,
        all_outcomes,
        as_of_date=as_of_date,
        created_at=created,
    )
    return SignalValidationRefreshPlan(
        candidate_outcomes=labeled.outcomes,
        profiles=conviction.profiles,
        summary={
            "as_of_date": as_of_date.isoformat(),
            "horizons": list(horizons),
            "signals_seen": len(signals),
            "existing_outcomes_seen": len(existing_outcomes),
            "candidate_outcomes": len(labeled.outcomes),
            "profiles_generated": len(conviction.profiles),
            "labeling": labeled.summary,
            "conviction": conviction.summary,
            "execution_authority": "none",
        },
    )


async def refresh_signal_validation(
    db: Any,
    *,
    as_of_date: date | None = None,
    horizons: tuple[int, ...] = DEFAULT_HORIZONS,
    signal_row_limit: int = 5000,
    feature_source: str = "yfinance",
) -> SignalValidationRefreshResult:
    """Refresh mature outcomes and conviction profiles from persisted rows."""
    from sqlalchemy import desc, select

    from db.models import StrategyFrozenSignal, StrategySignalOutcome

    target_date = as_of_date or datetime.now(timezone.utc).date()
    signals_result = await db.execute(
        select(StrategyFrozenSignal)
        .where(StrategyFrozenSignal.signal_date <= target_date)
        .order_by(desc(StrategyFrozenSignal.signal_date), desc(StrategyFrozenSignal.id))
        .limit(signal_row_limit)
    )
    signals = list(signals_result.scalars().all())
    if not signals:
        return SignalValidationRefreshResult(
            candidate_outcomes=0,
            outcomes_inserted=0,
            outcome_duplicates=0,
            outcome_conflicts=[],
            profiles_generated=0,
            profiles_inserted=0,
            profiles_updated=0,
            profile_duplicates=0,
            summary={
                "as_of_date": target_date.isoformat(),
                "signals_seen": 0,
                "reason": "no_frozen_signals",
                "execution_authority": "none",
            },
        )

    signal_ids = [row.signal_id for row in signals]
    existing_outcomes = await _read_signal_outcomes(db, signal_ids)
    feature_rows = await _read_feature_rows_for_signals(
        db,
        signals=signals,
        as_of_date=target_date,
        source=feature_source,
    )
    created = datetime.now(timezone.utc)
    label_plan = build_signal_validation_refresh_plan(
        signals=signals,
        existing_outcomes=existing_outcomes,
        feature_rows=feature_rows,
        as_of_date=target_date,
        horizons=horizons,
        created_at=created,
    )
    outcome_result = await persist_signal_outcomes(db, label_plan.candidate_outcomes)

    # Re-read persisted outcomes before conviction so profiles reflect DB truth,
    # not merely incoming candidates.
    persisted_outcomes = await _read_signal_outcomes(db, signal_ids)
    conviction = compute_conviction_profiles(
        signals,
        persisted_outcomes,
        as_of_date=target_date,
        created_at=created,
    )
    profile_result = await persist_conviction_profiles(db, conviction.profiles)
    summary = {
        **label_plan.summary,
        "persisted_outcomes_seen": len(persisted_outcomes),
        "candidate_outcomes": len(label_plan.candidate_outcomes),
        "outcomes_inserted": outcome_result.inserted,
        "outcome_duplicates": outcome_result.duplicates,
        "outcome_conflicts": len(outcome_result.conflicts),
        "profiles_generated": len(conviction.profiles),
        "profiles_inserted": profile_result.inserted,
        "profiles_updated": profile_result.updated,
        "profile_duplicates": profile_result.duplicates,
        "conviction": conviction.summary,
    }
    return SignalValidationRefreshResult(
        candidate_outcomes=len(label_plan.candidate_outcomes),
        outcomes_inserted=outcome_result.inserted,
        outcome_duplicates=outcome_result.duplicates,
        outcome_conflicts=outcome_result.conflicts,
        profiles_generated=len(conviction.profiles),
        profiles_inserted=profile_result.inserted,
        profiles_updated=profile_result.updated,
        profile_duplicates=profile_result.duplicates,
        summary=summary,
    )


async def _read_signal_outcomes(db: Any, signal_ids: list[str]) -> list[Any]:
    if not signal_ids:
        return []
    from sqlalchemy import select

    from db.models import StrategySignalOutcome

    result = await db.execute(
        select(StrategySignalOutcome).where(StrategySignalOutcome.signal_id.in_(signal_ids))
    )
    return list(result.scalars().all())


async def _read_feature_rows_for_signals(
    db: Any,
    *,
    signals: list[Any],
    as_of_date: date,
    source: str,
) -> list[dict[str, Any]]:
    tickers = sorted({str(getattr(signal, "ticker", "") or "").upper() for signal in signals} | {"SPY"})
    signal_dates = [getattr(signal, "signal_date", None) for signal in signals if getattr(signal, "signal_date", None)]
    start_date = min(signal_dates) if signal_dates else None
    rows = await get_market_daily_feature_rows(
        db,
        tickers=tickers,
        start_date=start_date,
        end_date=as_of_date,
        source=source,
    )
    return [model_to_feature_dict(row) for row in rows]


def _dedupe_outcomes(outcomes: list[Any]) -> list[Any]:
    by_id: dict[str, Any] = {}
    for outcome in outcomes:
        outcome_id = outcome.get("outcome_id") if isinstance(outcome, dict) else getattr(outcome, "outcome_id", None)
        if not outcome_id:
            continue
        by_id[str(outcome_id)] = outcome
    return list(by_id.values())
