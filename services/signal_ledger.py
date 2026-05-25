"""Live/paper frozen signal ledger.

The ledger freezes EvidenceCards emitted by the running system. Frozen signals
are immutable: reruns are idempotent only when the content hash is identical.
Outcome labels are written elsewhere and must not update these records.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from typing import Any

from services.construction_epoch import build_construction_epoch
from services.historical_signal_replay import FrozenSignal, freeze_evidence_card
from services.strategy_evidence import EVIDENCE_CONTRACT_VERSION


SIGNAL_SOURCE_FASTAPI_LIVE_FREEZE = "fastapi_live_freeze"


@dataclass(frozen=True)
class FrozenSignalWritePlan:
    records_to_insert: list[dict[str, Any]]
    duplicate_signal_ids: list[str]
    conflicts: list[dict[str, Any]]

    @property
    def insert_count(self) -> int:
        return len(self.records_to_insert)

    @property
    def duplicate_count(self) -> int:
        return len(self.duplicate_signal_ids)

    @property
    def conflict_count(self) -> int:
        return len(self.conflicts)

    def summary(self) -> dict[str, Any]:
        return {
            "insert_count": self.insert_count,
            "duplicate_count": self.duplicate_count,
            "conflict_count": self.conflict_count,
            "duplicate_signal_ids": list(self.duplicate_signal_ids),
            "conflicts": list(self.conflicts),
        }


@dataclass(frozen=True)
class PersistFrozenSignalsResult:
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


def freeze_evidence_cards_for_live(
    evidence_cards: list[dict[str, Any]],
    *,
    signal_date: date,
    generated_at: datetime | None = None,
    tradable_from_date: date | None = None,
    feature_data_date: date | None = None,
    feature_source: str = "playground_bundle",
    feature_authority: str = "mixed",
    regime_at_signal: str = "unknown",
    vix_at_signal: float | None = None,
    signal_source: str = SIGNAL_SOURCE_FASTAPI_LIVE_FREEZE,
    qc_context: dict[str, Any] | None = None,
    portfolio_construction_config: dict[str, Any] | None = None,
    construction_epoch: dict[str, Any] | None = None,
) -> list[FrozenSignal]:
    """Freeze live/paper EvidenceCards into immutable signal objects."""
    generated = generated_at or datetime.now(timezone.utc)
    tradable_from = tradable_from_date or signal_date
    epoch = construction_epoch or _live_construction_epoch(
        qc_context=qc_context,
        portfolio_construction_config=portfolio_construction_config,
    )
    signals: list[FrozenSignal] = []
    for card in evidence_cards:
        diagnostics = dict(card.get("diagnostics") or {})
        diagnostics["source_bucket"] = "live_paper"
        diagnostics["construction_epoch"] = epoch
        diagnostics["signal_freeze"] = {
            "signal_source": signal_source,
            "feature_date_known": feature_data_date is not None,
        }
        if qc_context:
            diagnostics["qc_context"] = dict(qc_context)
        enriched = dict(card)
        enriched["diagnostics"] = diagnostics
        signal = freeze_evidence_card(
            enriched,
            signal_date=signal_date,
            tradable_from_date=tradable_from,
            generated_at=generated,
            signal_source=signal_source,
            feature_data_date=feature_data_date,
            feature_source=feature_source,
            feature_authority=feature_authority,
            regime_at_signal=regime_at_signal,
            vix_at_signal=vix_at_signal,
            construction_epoch=epoch,
        )
        signals.append(signal)
    return signals


def freeze_playground_bundle(
    playground_bundle: dict[str, Any],
    *,
    signal_date: date | None = None,
    generated_at: datetime | None = None,
    feature_data_date: date | None = None,
    feature_source: str = "playground_bundle",
    feature_authority: str = "mixed",
    signal_source: str = SIGNAL_SOURCE_FASTAPI_LIVE_FREEZE,
    qc_context: dict[str, Any] | None = None,
    portfolio_construction_config: dict[str, Any] | None = None,
    construction_epoch: dict[str, Any] | None = None,
) -> list[FrozenSignal]:
    """Extract EvidenceCards from a Playground bundle and freeze them."""
    generated = generated_at or _parse_datetime(playground_bundle.get("generated_at")) or datetime.now(timezone.utc)
    signal_dt = signal_date or generated.date()
    evidence_cards: list[dict[str, Any]] = []
    for strategy in playground_bundle.get("strategies") or []:
        if not isinstance(strategy, dict):
            continue
        if strategy.get("evidence_contract_version") != EVIDENCE_CONTRACT_VERSION:
            continue
        evidence_cards.extend(
            dict(card)
            for card in strategy.get("evidence_cards") or []
            if isinstance(card, dict)
        )
    return freeze_evidence_cards_for_live(
        evidence_cards,
        signal_date=signal_dt,
        generated_at=generated,
        feature_data_date=feature_data_date,
        feature_source=feature_source,
        feature_authority=feature_authority,
        regime_at_signal=str(playground_bundle.get("regime_label") or "unknown"),
        signal_source=signal_source,
        qc_context=qc_context,
        portfolio_construction_config=portfolio_construction_config,
        construction_epoch=construction_epoch,
    )


def _live_construction_epoch(
    *,
    qc_context: dict[str, Any] | None,
    portfolio_construction_config: dict[str, Any] | None,
) -> dict[str, Any]:
    context = qc_context if isinstance(qc_context, dict) else {}
    pc_config = portfolio_construction_config if isinstance(portfolio_construction_config, dict) else {}
    return build_construction_epoch(
        pc_mode=context.get("portfolio_construction_mode") or pc_config.get("portfolio_construction_mode"),
        construction_objective_version=context.get("construction_objective_version"),
        policy_version=(
            context.get("policy_version")
            or context.get("fastapi_policy_version")
            or context.get("policy_snapshot_version")
        ),
        promotion_config=pc_config,
        source=str(context.get("source") or "fastapi_live_freeze"),
    )


def frozen_signal_record(signal: FrozenSignal) -> dict[str, Any]:
    """Return a JSON/DB-friendly record for a FrozenSignal."""
    return {
        "signal_id": signal.signal_id,
        "signal_source": signal.signal_source,
        "signal_date": signal.signal_date,
        "generated_at": signal.generated_at,
        "tradable_from_date": signal.tradable_from_date,
        "strategy_id": signal.strategy_id,
        "strategy_version": signal.strategy_version,
        "ticker": signal.ticker,
        "role": signal.role,
        "branch": signal.branch,
        "action": signal.action,
        "signal_type": signal.signal_type,
        "confidence": signal.confidence,
        "raw_score": signal.raw_score,
        "normalized_score": signal.normalized_score,
        "max_reasonable_weight": signal.max_reasonable_weight,
        "risk_budget_cost": signal.risk_budget_cost,
        "feature_data_date": signal.feature_data_date,
        "data_lag_days": signal.data_lag_days,
        "feature_source": signal.feature_source,
        "feature_authority": signal.feature_authority,
        "regime_at_signal": signal.regime_at_signal,
        "vix_at_signal": signal.vix_at_signal,
        "evidence_contract_version": signal.evidence_contract_version,
        "diagnostics": _json_ready(signal.diagnostics),
        "content_hash": frozen_signal_content_hash(signal),
        "created_at": signal.created_at,
    }


def frozen_signal_content_hash(signal: FrozenSignal) -> str:
    payload = asdict(signal)
    payload.pop("generated_at", None)
    payload.pop("created_at", None)
    return hashlib.sha256(
        json.dumps(_json_ready(payload), sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def plan_frozen_signal_writes(
    signals: list[FrozenSignal],
    existing_by_signal_id: dict[str, Any] | None = None,
) -> FrozenSignalWritePlan:
    existing = existing_by_signal_id or {}
    records: list[dict[str, Any]] = []
    duplicates: list[str] = []
    conflicts: list[dict[str, Any]] = []
    seen_in_batch: dict[str, str] = {}
    for signal in signals:
        record = frozen_signal_record(signal)
        signal_id = signal.signal_id
        content_hash = record["content_hash"]
        if signal_id in seen_in_batch:
            if seen_in_batch[signal_id] == content_hash:
                duplicates.append(signal_id)
            else:
                conflicts.append({
                    "signal_id": signal_id,
                    "reason": "batch_content_hash_conflict",
                    "existing_hash": seen_in_batch[signal_id],
                    "incoming_hash": content_hash,
                })
            continue
        seen_in_batch[signal_id] = content_hash
        existing_hash = _existing_content_hash(existing.get(signal_id))
        if existing_hash is None:
            records.append(record)
        elif existing_hash == content_hash:
            duplicates.append(signal_id)
        else:
            conflicts.append({
                "signal_id": signal_id,
                "reason": "existing_content_hash_conflict",
                "existing_hash": existing_hash,
                "incoming_hash": content_hash,
            })
    return FrozenSignalWritePlan(
        records_to_insert=records,
        duplicate_signal_ids=duplicates,
        conflicts=conflicts,
    )


async def persist_frozen_signals(db: Any, signals: list[FrozenSignal]) -> PersistFrozenSignalsResult:
    """Persist frozen signals idempotently.

    Different content for an existing signal_id is treated as a conflict and is
    not overwritten.
    """
    if not signals:
        return PersistFrozenSignalsResult(inserted=0, duplicates=0, conflicts=[])

    from sqlalchemy import select

    from db.models import StrategyFrozenSignal

    signal_ids = sorted({signal.signal_id for signal in signals})
    result = await db.execute(
        select(StrategyFrozenSignal).where(StrategyFrozenSignal.signal_id.in_(signal_ids))
    )
    existing_rows = result.scalars().all()
    existing = {row.signal_id: row for row in existing_rows}
    plan = plan_frozen_signal_writes(signals, existing)
    for record in plan.records_to_insert:
        db.add(StrategyFrozenSignal(**record))
    if plan.records_to_insert:
        await db.commit()
    return PersistFrozenSignalsResult(
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


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            return None
    return None


def _json_ready(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in sorted(value.items())}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    return value
