"""Append-only outcome labeler for frozen strategy signals.

PR6 labels mature FrozenSignals with yfinance forward outcomes. It reuses the
central hit definitions from historical replay and writes labels idempotently.
FrozenSignal rows are never updated here.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from typing import Any, Iterable

from services.historical_signal_replay import (
    DEFAULT_HORIZONS,
    EXCESS_CALCULATION_RAW,
    OUTCOME_SOURCE_YFINANCE,
    FrozenSignal,
    SignalOutcome,
    _normalize_row,
    _price_index,
    label_date_for_horizon,
    label_signal_outcomes,
    outcome_tradable_from_date,
)


@dataclass(frozen=True)
class MatureOutcomeLabelingResult:
    outcomes: list[SignalOutcome]
    summary: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "outcomes": [item.to_dict() for item in self.outcomes],
            "summary": dict(self.summary),
        }


@dataclass(frozen=True)
class SignalOutcomeWritePlan:
    records_to_insert: list[dict[str, Any]]
    duplicate_outcome_ids: list[str]
    conflicts: list[dict[str, Any]]

    @property
    def insert_count(self) -> int:
        return len(self.records_to_insert)

    @property
    def duplicate_count(self) -> int:
        return len(self.duplicate_outcome_ids)

    @property
    def conflict_count(self) -> int:
        return len(self.conflicts)

    def summary(self) -> dict[str, Any]:
        return {
            "insert_count": self.insert_count,
            "duplicate_count": self.duplicate_count,
            "conflict_count": self.conflict_count,
            "duplicate_outcome_ids": list(self.duplicate_outcome_ids),
            "conflicts": list(self.conflicts),
        }


@dataclass(frozen=True)
class PersistSignalOutcomesResult:
    inserted: int
    duplicates: int
    conflicts: list[dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "inserted": self.inserted,
            "duplicates": self.duplicates,
            "conflict_count": len(self.conflicts),
            "conflicts": list(self.conflicts),
        }


def label_mature_signal_outcomes(
    signals: Iterable[Any],
    feature_rows: Iterable[Any],
    *,
    as_of_date: date,
    horizons: tuple[int, ...] = DEFAULT_HORIZONS,
    outcome_source: str = OUTCOME_SOURCE_YFINANCE,
    created_at: datetime | None = None,
) -> MatureOutcomeLabelingResult:
    """Label only horizons whose yfinance path is mature by `as_of_date`."""
    if outcome_source != OUTCOME_SOURCE_YFINANCE:
        raise ValueError(f"Unsupported outcome_source: {outcome_source}")

    created = created_at or datetime.now(timezone.utc)
    normalized_rows = [
        row
        for row in (_normalize_row(item) for item in feature_rows)
        if row.get("ticker") and row.get("trading_date") and row["trading_date"] <= as_of_date
    ]
    normalized_rows.sort(key=lambda row: (row["trading_date"], row["ticker"]))
    trading_dates = sorted({row["trading_date"] for row in normalized_rows})
    price_by_ticker = _price_index(normalized_rows)

    outcomes: list[SignalOutcome] = []
    skipped: dict[str, int] = {}
    converted_signals: list[FrozenSignal] = []
    for raw_signal in signals:
        signal = frozen_signal_from_record(raw_signal)
        if signal is None:
            skipped["invalid_signal"] = skipped.get("invalid_signal", 0) + 1
            continue
        converted_signals.append(signal)
        effective_tradable_from = outcome_tradable_from_date(
            signal,
            trading_dates=trading_dates,
        )
        if effective_tradable_from is None:
            skipped["tradable_from_not_available"] = skipped.get("tradable_from_not_available", 0) + 1
            continue
        for horizon in horizons:
            label_date = label_date_for_horizon(
                trading_dates=trading_dates,
                tradable_from_date=effective_tradable_from,
                horizon_days=horizon,
            )
            if label_date is None or label_date > as_of_date:
                skipped[f"h{horizon}:not_mature"] = skipped.get(f"h{horizon}:not_mature", 0) + 1
                continue
            if label_date <= signal.signal_date:
                skipped[f"h{horizon}:non_forward_label"] = skipped.get(
                    f"h{horizon}:non_forward_label",
                    0,
                ) + 1
                continue
            labeled = label_signal_outcomes(
                signal,
                price_by_ticker=price_by_ticker,
                trading_dates=trading_dates,
                horizons=(int(horizon),),
                created_at=created,
            )
            if labeled:
                outcomes.extend(labeled)
            else:
                skipped[f"h{horizon}:missing_price_path"] = skipped.get(
                    f"h{horizon}:missing_price_path",
                    0,
                ) + 1

    return MatureOutcomeLabelingResult(
        outcomes=outcomes,
        summary={
            "outcome_source": outcome_source,
            "excess_calculation_method": EXCESS_CALCULATION_RAW,
            "as_of_date": as_of_date.isoformat(),
            "signals_seen": len(converted_signals),
            "horizons": list(horizons),
            "outcomes_generated": len(outcomes),
            "skipped": dict(sorted(skipped.items())),
        },
    )


def frozen_signal_from_record(value: Any) -> FrozenSignal | None:
    """Convert a FrozenSignal DB row or dict into the immutable dataclass."""
    if isinstance(value, FrozenSignal):
        return value
    signal_id = _record_get(value, "signal_id")
    signal_date = _parse_date(_record_get(value, "signal_date"))
    tradable_from_date = _parse_date(_record_get(value, "tradable_from_date"))
    if not signal_id or signal_date is None or tradable_from_date is None:
        return None

    generated_at = (
        _parse_datetime(_record_get(value, "generated_at"))
        or _parse_datetime(_record_get(value, "created_at"))
        or datetime.now(timezone.utc)
    )
    created_at = _parse_datetime(_record_get(value, "created_at")) or generated_at
    return FrozenSignal(
        signal_id=str(signal_id),
        signal_source=str(_record_get(value, "signal_source") or ""),
        signal_date=signal_date,
        generated_at=generated_at,
        tradable_from_date=tradable_from_date,
        strategy_id=str(_record_get(value, "strategy_id") or ""),
        strategy_version=str(_record_get(value, "strategy_version") or ""),
        ticker=str(_record_get(value, "ticker") or "").upper().strip(),
        role=str(_record_get(value, "role") or "unknown"),
        branch=_optional_str(_record_get(value, "branch")),
        action=str(_record_get(value, "action") or "watch"),
        signal_type=str(_record_get(value, "signal_type") or "unspecified"),
        confidence=_to_float(_record_get(value, "confidence"), 0.0),
        raw_score=_optional_float(_record_get(value, "raw_score")),
        normalized_score=_to_float(_record_get(value, "normalized_score"), 0.0),
        max_reasonable_weight=_to_float(_record_get(value, "max_reasonable_weight"), 0.0),
        risk_budget_cost=_to_float(_record_get(value, "risk_budget_cost"), 1.0),
        feature_data_date=_parse_date(_record_get(value, "feature_data_date")),
        data_lag_days=_optional_int(_record_get(value, "data_lag_days")),
        feature_source=str(_record_get(value, "feature_source") or "unknown"),
        feature_authority=str(_record_get(value, "feature_authority") or "unknown"),
        regime_at_signal=str(_record_get(value, "regime_at_signal") or "unknown"),
        vix_at_signal=_optional_float(_record_get(value, "vix_at_signal")),
        evidence_contract_version=str(_record_get(value, "evidence_contract_version") or ""),
        diagnostics=_record_get(value, "diagnostics") or {},
        created_at=created_at,
    )


def signal_outcome_record(outcome: SignalOutcome) -> dict[str, Any]:
    return {
        "outcome_id": outcome.outcome_id,
        "signal_id": outcome.signal_id,
        "signal_source": outcome.signal_source,
        "signal_date": outcome.signal_date,
        "label_date": outcome.label_date,
        "strategy_id": outcome.strategy_id,
        "ticker": outcome.ticker,
        "branch": outcome.branch,
        "action": outcome.action,
        "horizon_days": outcome.horizon_days,
        "forward_return": outcome.forward_return,
        "spy_forward_return": outcome.spy_forward_return,
        "excess_vs_spy": outcome.excess_vs_spy,
        "drawdown_during_horizon": outcome.drawdown_during_horizon,
        "spy_drawdown_during_horizon": outcome.spy_drawdown_during_horizon,
        "target_pool_drawdown": outcome.target_pool_drawdown,
        "hit": outcome.hit,
        "hit_definition": outcome.hit_definition,
        "excess_calculation_method": outcome.excess_calculation_method,
        "outcome_source": outcome.outcome_source,
        "data_quality": outcome.data_quality,
        "content_hash": signal_outcome_content_hash(outcome),
        "created_at": _db_naive_datetime(outcome.created_at),
    }


def signal_outcome_content_hash(outcome: SignalOutcome) -> str:
    payload = asdict(outcome)
    payload.pop("created_at", None)
    return hashlib.sha256(
        json.dumps(_json_ready(payload), sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def plan_signal_outcome_writes(
    outcomes: list[SignalOutcome],
    existing_by_outcome_id: dict[str, Any] | None = None,
) -> SignalOutcomeWritePlan:
    existing = existing_by_outcome_id or {}
    records: list[dict[str, Any]] = []
    duplicates: list[str] = []
    conflicts: list[dict[str, Any]] = []
    seen_in_batch: dict[str, str] = {}
    for outcome in outcomes:
        record = signal_outcome_record(outcome)
        outcome_id = outcome.outcome_id
        content_hash = record["content_hash"]
        if outcome_id in seen_in_batch:
            if seen_in_batch[outcome_id] == content_hash:
                duplicates.append(outcome_id)
            else:
                conflicts.append({
                    "outcome_id": outcome_id,
                    "reason": "batch_content_hash_conflict",
                    "existing_hash": seen_in_batch[outcome_id],
                    "incoming_hash": content_hash,
                })
            continue
        seen_in_batch[outcome_id] = content_hash
        existing_hash = _existing_content_hash(existing.get(outcome_id))
        if existing_hash is None:
            records.append(record)
        elif existing_hash == content_hash:
            duplicates.append(outcome_id)
        else:
            conflicts.append({
                "outcome_id": outcome_id,
                "reason": "existing_content_hash_conflict",
                "existing_hash": existing_hash,
                "incoming_hash": content_hash,
            })
    return SignalOutcomeWritePlan(
        records_to_insert=records,
        duplicate_outcome_ids=duplicates,
        conflicts=conflicts,
    )


async def persist_signal_outcomes(
    db: Any,
    outcomes: list[SignalOutcome],
) -> PersistSignalOutcomesResult:
    """Persist outcomes idempotently without mutating frozen signals."""
    if not outcomes:
        return PersistSignalOutcomesResult(inserted=0, duplicates=0, conflicts=[])

    from sqlalchemy import select

    from db.models import StrategySignalOutcome

    outcome_ids = sorted({outcome.outcome_id for outcome in outcomes})
    result = await db.execute(
        select(StrategySignalOutcome).where(StrategySignalOutcome.outcome_id.in_(outcome_ids))
    )
    existing_rows = result.scalars().all()
    existing = {row.outcome_id: row for row in existing_rows}
    plan = plan_signal_outcome_writes(outcomes, existing)
    for record in plan.records_to_insert:
        db.add(StrategySignalOutcome(**record))
    if plan.records_to_insert:
        await db.commit()
    return PersistSignalOutcomesResult(
        inserted=plan.insert_count,
        duplicates=plan.duplicate_count,
        conflicts=plan.conflicts,
    )


def _existing_content_hash(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, dict):
        raw = value.get("content_hash")
    else:
        raw = getattr(value, "content_hash", None)
    return str(raw) if raw else None


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


def _to_float(value: Any, default: float = 0.0) -> float:
    parsed = _optional_float(value)
    return parsed if parsed is not None else default


def _optional_int(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def _db_naive_datetime(value: datetime) -> datetime:
    """Return UTC naive datetime for TIMESTAMP columns."""
    if value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)
