"""Strategy conviction calibration from frozen signals and outcomes.

Conviction is a derived validation layer. It summarizes whether a strategy's
EvidenceCards have been useful historically or in live paper operation, but it
does not authorize execution.
"""
from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from typing import Any, Iterable, NamedTuple

from services.construction_epoch import (
    construction_epoch_from_diagnostics,
    construction_epoch_from_signal,
    construction_epoch_id_from_profile,
)
from services.historical_signal_replay import (
    SIGNAL_SOURCE_YFINANCE_REPLAY,
    FrozenSignal,
    SignalOutcome,
)
from services.signal_ledger import SIGNAL_SOURCE_FASTAPI_LIVE_FREEZE
from services.signal_outcome_labeler import frozen_signal_from_record


SOURCE_BUCKET_HISTORICAL_PRIOR = "historical_prior"
SOURCE_BUCKET_LIVE_PAPER = "live_paper"
SOURCE_BUCKET_COMBINED = "combined"

STATUS_INSUFFICIENT_SAMPLES = "insufficient_samples"
STATUS_EARLY_ESTIMATE = "early_estimate"
STATUS_CALIBRATED = "calibrated"
STATUS_HISTORICAL_REQUIRES_LIVE = "historical_prior_requires_live_confirmation"
STATUS_EARLY_LIVE_CONFIRMATION = "early_live_confirmation"

STAT_STATUS_INSUFFICIENT = "insufficient"
STAT_STATUS_MONITORING_READY = "monitoring_ready"
STAT_STATUS_EARLY_SIGNAL = "early_signal"
STAT_STATUS_INDICATIVE = "indicative"
STAT_STATUS_STATISTICALLY_MEANINGFUL = "statistically_meaningful"
STAT_INSUFFICIENT_SAMPLES = 30
STAT_EARLY_SIGNAL_SAMPLES = 100
STAT_INDICATIVE_SAMPLES = 300
STAT_MEANINGFUL_SAMPLES = 783

MIN_SAMPLES = 10
CALIBRATED_SAMPLES = 30
MAX_DATA_LAG_DAYS = 1
LIVE_CREDIBILITY_BONUS = 1.5
MAX_LIVE_WEIGHT = 0.80
VALID_DATA_QUALITY = {"ok", "valid"}


class _ProfileKey(NamedTuple):
    strategy_id: str
    ticker: str
    branch: str | None
    action: str
    regime_at_signal: str
    horizon_days: int
    source_bucket: str
    construction_epoch_id: str

    def without_source_bucket(self) -> tuple[str, str, str | None, str, str, int, str]:
        return (
            self.strategy_id,
            self.ticker,
            self.branch,
            self.action,
            self.regime_at_signal,
            self.horizon_days,
            self.construction_epoch_id,
        )


@dataclass(frozen=True)
class ConvictionProfile:
    profile_id: str
    as_of_date: date
    strategy_id: str
    ticker: str
    branch: str | None
    action: str
    regime_at_signal: str
    horizon_days: int
    source_bucket: str
    conviction: float | None
    status: str
    n: int
    required_samples: int
    hit_rate: float | None
    avg_forward_return: float | None
    avg_excess_vs_spy: float | None
    ic: float | None
    max_adverse_drawdown: float | None
    data_lag_filtered: int
    requires_live_confirmation: bool
    hist_n: int
    live_n: int
    hist_weight: float | None
    live_weight: float | None
    source_counts: dict[str, int]
    diagnostics: dict[str, Any]
    created_at: datetime

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        stats = statistical_interpretation(n=self.n, hit_rate=self.hit_rate)
        diagnostics = data.get("diagnostics") if isinstance(data.get("diagnostics"), dict) else {}
        data["legacy_operational_status"] = data.get("status")
        data["statistical_status"] = stats["statistical_status"]
        data["hit_rate_ci"] = diagnostics.get("hit_rate_ci") or stats["hit_rate_ci"]
        data["hit_rate_ci_width"] = diagnostics.get("hit_rate_ci_width", stats["hit_rate_ci_width"])
        data["statistical_sample_thresholds"] = stats["sample_thresholds"]
        epoch = construction_epoch_from_diagnostics(diagnostics)
        data["construction_epoch"] = dict(epoch)
        data["construction_epoch_id"] = epoch.get("epoch_id")
        return data


@dataclass(frozen=True)
class ConvictionComputationResult:
    profiles: list[ConvictionProfile]
    summary: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "profiles": [item.to_dict() for item in self.profiles],
            "summary": dict(self.summary),
        }


@dataclass(frozen=True)
class ConvictionProfileWritePlan:
    records_to_insert: list[dict[str, Any]]
    records_to_update: list[dict[str, Any]]
    duplicate_profile_ids: list[str]

    @property
    def insert_count(self) -> int:
        return len(self.records_to_insert)

    @property
    def update_count(self) -> int:
        return len(self.records_to_update)

    @property
    def duplicate_count(self) -> int:
        return len(self.duplicate_profile_ids)

    def summary(self) -> dict[str, Any]:
        return {
            "insert_count": self.insert_count,
            "update_count": self.update_count,
            "duplicate_count": self.duplicate_count,
            "duplicate_profile_ids": list(self.duplicate_profile_ids),
        }


@dataclass(frozen=True)
class PersistConvictionProfilesResult:
    inserted: int
    updated: int
    duplicates: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "inserted": self.inserted,
            "updated": self.updated,
            "duplicates": self.duplicates,
        }


@dataclass(frozen=True)
class _Sample:
    signal: FrozenSignal
    outcome: SignalOutcome
    source_bucket: str


def compute_conviction_profiles(
    signals: Iterable[Any],
    outcomes: Iterable[Any],
    *,
    as_of_date: date | None = None,
    include_combined: bool = True,
    max_data_lag_days: int = MAX_DATA_LAG_DAYS,
    min_samples: int = MIN_SAMPLES,
    calibrated_samples: int = CALIBRATED_SAMPLES,
    created_at: datetime | None = None,
) -> ConvictionComputationResult:
    """Compute source-bucket and combined conviction profiles."""
    created = created_at or datetime.now(timezone.utc)
    signal_map = {
        signal.signal_id: signal
        for signal in (frozen_signal_from_record(item) for item in signals)
        if signal is not None
    }
    parsed_outcomes = [
        outcome
        for outcome in (signal_outcome_from_record(item) for item in outcomes)
        if outcome is not None
    ]
    if as_of_date is None:
        as_of_date = max(
            (outcome.label_date for outcome in parsed_outcomes),
            default=created.date(),
        )

    samples_by_key: dict[_ProfileKey, list[_Sample]] = {}
    lag_filtered_by_key: dict[_ProfileKey, int] = {}
    skipped: dict[str, int] = {}

    for outcome in parsed_outcomes:
        signal = signal_map.get(outcome.signal_id)
        if signal is None:
            skipped["missing_signal"] = skipped.get("missing_signal", 0) + 1
            continue
        source_bucket = source_bucket_for_signal(signal)
        if source_bucket is None:
            skipped["unsupported_signal_source"] = skipped.get("unsupported_signal_source", 0) + 1
            continue
        key = _key_for(signal, outcome, source_bucket)
        if outcome.hit is None:
            skipped["hit_null"] = skipped.get("hit_null", 0) + 1
            continue
        if str(outcome.data_quality or "").lower() not in VALID_DATA_QUALITY:
            skipped["invalid_data_quality"] = skipped.get("invalid_data_quality", 0) + 1
            continue
        if signal.data_lag_days is None or signal.data_lag_days > max_data_lag_days:
            lag_filtered_by_key[key] = lag_filtered_by_key.get(key, 0) + 1
            skipped["data_lag_filtered"] = skipped.get("data_lag_filtered", 0) + 1
            continue
        samples_by_key.setdefault(key, []).append(_Sample(signal, outcome, source_bucket))

    profiles = [
        _build_source_profile(
            key,
            samples,
            lag_filtered=lag_filtered_by_key.get(key, 0),
            as_of_date=as_of_date,
            min_samples=min_samples,
            calibrated_samples=calibrated_samples,
            created_at=created,
        )
        for key, samples in sorted(samples_by_key.items(), key=lambda item: _sort_key(item[0]))
    ]
    if include_combined:
        profiles.extend(_build_combined_profiles(
            profiles,
            as_of_date=as_of_date,
            min_samples=min_samples,
            calibrated_samples=calibrated_samples,
            created_at=created,
        ))

    profiles.sort(key=lambda item: (
        item.strategy_id,
        item.ticker,
        item.branch or "",
        item.action,
        item.regime_at_signal,
        item.horizon_days,
        item.source_bucket,
        construction_epoch_id_from_profile(item),
    ))
    return ConvictionComputationResult(
        profiles=profiles,
        summary={
            "as_of_date": as_of_date.isoformat(),
            "signals_seen": len(signal_map),
            "outcomes_seen": len(parsed_outcomes),
            "profiles_generated": len(profiles),
            "max_data_lag_days": max_data_lag_days,
            "min_samples": min_samples,
            "calibrated_samples": calibrated_samples,
            "skipped": dict(sorted(skipped.items())),
        },
    )


def source_bucket_for_signal(signal: FrozenSignal) -> str | None:
    if signal.signal_source == SIGNAL_SOURCE_YFINANCE_REPLAY:
        return SOURCE_BUCKET_HISTORICAL_PRIOR
    if signal.signal_source == SIGNAL_SOURCE_FASTAPI_LIVE_FREEZE:
        return SOURCE_BUCKET_LIVE_PAPER
    diagnostics = signal.diagnostics or {}
    bucket = diagnostics.get("source_bucket")
    if bucket in {SOURCE_BUCKET_HISTORICAL_PRIOR, SOURCE_BUCKET_LIVE_PAPER}:
        return str(bucket)
    return None


def statistical_status_for_samples(n: int) -> str:
    """Statistical interpretation tier for conviction sample sizes."""
    count = max(int(n or 0), 0)
    if count < STAT_INSUFFICIENT_SAMPLES:
        return STAT_STATUS_INSUFFICIENT
    if count < STAT_EARLY_SIGNAL_SAMPLES:
        return STAT_STATUS_MONITORING_READY
    if count < STAT_INDICATIVE_SAMPLES:
        return STAT_STATUS_EARLY_SIGNAL
    if count < STAT_MEANINGFUL_SAMPLES:
        return STAT_STATUS_INDICATIVE
    return STAT_STATUS_STATISTICALLY_MEANINGFUL


def wilson_hit_rate_interval(
    *,
    hit_rate: float | None,
    n: int,
    z: float = 1.96,
) -> dict[str, Any] | None:
    """Return a Wilson score interval for a hit-rate point estimate."""
    count = max(int(n or 0), 0)
    if hit_rate is None or count <= 0:
        return None
    p_hat = _clamp(float(hit_rate), 0.0, 1.0)
    z2 = z * z
    denom = 1.0 + z2 / count
    center = (p_hat + z2 / (2.0 * count)) / denom
    margin = (
        z
        * math.sqrt((p_hat * (1.0 - p_hat) / count) + (z2 / (4.0 * count * count)))
        / denom
    )
    lower = _clamp(center - margin, 0.0, 1.0)
    upper = _clamp(center + margin, 0.0, 1.0)
    return {
        "method": "wilson_score",
        "confidence_level": 0.95 if abs(z - 1.96) < 1e-9 else None,
        "n": count,
        "point_estimate": round(p_hat, 4),
        "lower": round(lower, 4),
        "upper": round(upper, 4),
        "width": round(upper - lower, 4),
    }


def statistical_interpretation(*, n: int, hit_rate: float | None) -> dict[str, Any]:
    interval = wilson_hit_rate_interval(hit_rate=hit_rate, n=n)
    return {
        "statistical_status": statistical_status_for_samples(n),
        "hit_rate_ci": interval,
        "hit_rate_ci_width": interval.get("width") if interval else None,
        "sample_thresholds": {
            STAT_STATUS_INSUFFICIENT: f"<{STAT_INSUFFICIENT_SAMPLES}",
            STAT_STATUS_MONITORING_READY: f"{STAT_INSUFFICIENT_SAMPLES}-{STAT_EARLY_SIGNAL_SAMPLES - 1}",
            STAT_STATUS_EARLY_SIGNAL: f"{STAT_EARLY_SIGNAL_SAMPLES}-{STAT_INDICATIVE_SAMPLES - 1}",
            STAT_STATUS_INDICATIVE: f"{STAT_INDICATIVE_SAMPLES}-{STAT_MEANINGFUL_SAMPLES - 1}",
            STAT_STATUS_STATISTICALLY_MEANINGFUL: f">={STAT_MEANINGFUL_SAMPLES}",
        },
    }


def signal_outcome_from_record(value: Any) -> SignalOutcome | None:
    if isinstance(value, SignalOutcome):
        return value
    outcome_id = _record_get(value, "outcome_id")
    signal_id = _record_get(value, "signal_id")
    signal_date = _parse_date(_record_get(value, "signal_date"))
    label_date = _parse_date(_record_get(value, "label_date"))
    if not outcome_id or not signal_id or signal_date is None or label_date is None:
        return None
    return SignalOutcome(
        outcome_id=str(outcome_id),
        signal_id=str(signal_id),
        signal_source=str(_record_get(value, "signal_source") or ""),
        signal_date=signal_date,
        label_date=label_date,
        strategy_id=str(_record_get(value, "strategy_id") or ""),
        ticker=str(_record_get(value, "ticker") or "").upper().strip(),
        branch=_optional_str(_record_get(value, "branch")),
        action=str(_record_get(value, "action") or "watch"),
        horizon_days=_to_int(_record_get(value, "horizon_days"), 0),
        forward_return=_to_float(_record_get(value, "forward_return"), 0.0),
        spy_forward_return=_to_float(_record_get(value, "spy_forward_return"), 0.0),
        excess_vs_spy=_to_float(_record_get(value, "excess_vs_spy"), 0.0),
        drawdown_during_horizon=_to_float(_record_get(value, "drawdown_during_horizon"), 0.0),
        spy_drawdown_during_horizon=_to_float(
            _record_get(value, "spy_drawdown_during_horizon"),
            0.0,
        ),
        target_pool_drawdown=_optional_float(_record_get(value, "target_pool_drawdown")),
        hit=_optional_bool(_record_get(value, "hit")),
        hit_definition=str(_record_get(value, "hit_definition") or ""),
        excess_calculation_method=str(_record_get(value, "excess_calculation_method") or ""),
        outcome_source=str(_record_get(value, "outcome_source") or ""),
        data_quality=str(_record_get(value, "data_quality") or "unknown"),
        created_at=_parse_datetime(_record_get(value, "created_at")) or datetime.now(timezone.utc),
    )


def conviction_profile_record(profile: ConvictionProfile) -> dict[str, Any]:
    return {
        "profile_id": profile.profile_id,
        "as_of_date": profile.as_of_date,
        "strategy_id": profile.strategy_id,
        "ticker": profile.ticker,
        "branch": profile.branch,
        "action": profile.action,
        "regime_at_signal": profile.regime_at_signal,
        "horizon_days": profile.horizon_days,
        "source_bucket": profile.source_bucket,
        "conviction": profile.conviction,
        "status": profile.status,
        "n": profile.n,
        "required_samples": profile.required_samples,
        "hit_rate": profile.hit_rate,
        "avg_forward_return": profile.avg_forward_return,
        "avg_excess_vs_spy": profile.avg_excess_vs_spy,
        "ic": profile.ic,
        "max_adverse_drawdown": profile.max_adverse_drawdown,
        "data_lag_filtered": profile.data_lag_filtered,
        "requires_live_confirmation": profile.requires_live_confirmation,
        "hist_n": profile.hist_n,
        "live_n": profile.live_n,
        "hist_weight": profile.hist_weight,
        "live_weight": profile.live_weight,
        "source_counts": _json_ready(profile.source_counts),
        "diagnostics": _json_ready(profile.diagnostics),
        "content_hash": conviction_profile_content_hash(profile),
        "created_at": _db_naive_datetime(profile.created_at),
    }


def conviction_profile_content_hash(profile: ConvictionProfile) -> str:
    payload = asdict(profile)
    payload.pop("created_at", None)
    return hashlib.sha256(
        json.dumps(_json_ready(payload), sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def plan_conviction_profile_writes(
    profiles: list[ConvictionProfile],
    existing_by_profile_id: dict[str, Any] | None = None,
) -> ConvictionProfileWritePlan:
    existing = existing_by_profile_id or {}
    inserts: list[dict[str, Any]] = []
    updates: list[dict[str, Any]] = []
    duplicates: list[str] = []
    seen: set[str] = set()
    for profile in profiles:
        if profile.profile_id in seen:
            duplicates.append(profile.profile_id)
            continue
        seen.add(profile.profile_id)
        record = conviction_profile_record(profile)
        existing_hash = _existing_content_hash(existing.get(profile.profile_id))
        if existing_hash is None:
            inserts.append(record)
        elif existing_hash == record["content_hash"]:
            duplicates.append(profile.profile_id)
        else:
            updates.append(record)
    return ConvictionProfileWritePlan(
        records_to_insert=inserts,
        records_to_update=updates,
        duplicate_profile_ids=duplicates,
    )


async def persist_conviction_profiles(
    db: Any,
    profiles: list[ConvictionProfile],
) -> PersistConvictionProfilesResult:
    """Insert or update derived conviction profiles for the same profile key."""
    if not profiles:
        return PersistConvictionProfilesResult(inserted=0, updated=0, duplicates=0)

    from sqlalchemy import select

    from db.models import StrategyConvictionProfile

    profile_ids = sorted({profile.profile_id for profile in profiles})
    result = await db.execute(
        select(StrategyConvictionProfile).where(StrategyConvictionProfile.profile_id.in_(profile_ids))
    )
    existing_rows = result.scalars().all()
    existing = {row.profile_id: row for row in existing_rows}
    plan = plan_conviction_profile_writes(profiles, existing)
    for record in plan.records_to_insert:
        db.add(StrategyConvictionProfile(**record))
    for record in plan.records_to_update:
        row = existing.get(record["profile_id"])
        if row is None:
            continue
        for key, value in record.items():
            if key != "profile_id":
                setattr(row, key, value)
    if plan.records_to_insert or plan.records_to_update:
        await db.commit()
    return PersistConvictionProfilesResult(
        inserted=plan.insert_count,
        updated=plan.update_count,
        duplicates=plan.duplicate_count,
    )


def _build_source_profile(
    key: _ProfileKey,
    samples: list[_Sample],
    *,
    lag_filtered: int,
    as_of_date: date,
    min_samples: int,
    calibrated_samples: int,
    created_at: datetime,
) -> ConvictionProfile:
    n = len(samples)
    hit_rate = _mean(1.0 if sample.outcome.hit else 0.0 for sample in samples)
    avg_forward = _mean(sample.outcome.forward_return for sample in samples)
    avg_excess = _mean(sample.outcome.excess_vs_spy for sample in samples)
    ic = _pearson(
        [sample.signal.confidence for sample in samples],
        [sample.outcome.forward_return for sample in samples],
    )
    max_adverse = min(
        (sample.outcome.drawdown_during_horizon for sample in samples),
        default=None,
    )
    status = _sample_status(n=n, min_samples=min_samples, calibrated_samples=calibrated_samples)
    conviction = None
    if status != STATUS_INSUFFICIENT_SAMPLES:
        conviction = _conviction_score(hit_rate, avg_excess, ic)
    source_counts = {key.source_bucket: n}
    construction_epoch = (
        construction_epoch_from_signal(samples[0].signal)
        if samples
        else {"epoch_id": key.construction_epoch_id}
    )
    return _profile_from_metrics(
        key=key,
        as_of_date=as_of_date,
        conviction=conviction,
        status=status,
        n=n,
        required_samples=calibrated_samples,
        hit_rate=hit_rate,
        avg_forward_return=avg_forward,
        avg_excess_vs_spy=avg_excess,
        ic=ic,
        max_adverse_drawdown=max_adverse,
        data_lag_filtered=lag_filtered,
        requires_live_confirmation=key.source_bucket == SOURCE_BUCKET_HISTORICAL_PRIOR,
        hist_n=n if key.source_bucket == SOURCE_BUCKET_HISTORICAL_PRIOR else 0,
        live_n=n if key.source_bucket == SOURCE_BUCKET_LIVE_PAPER else 0,
        hist_weight=1.0 if key.source_bucket == SOURCE_BUCKET_HISTORICAL_PRIOR else 0.0,
        live_weight=1.0 if key.source_bucket == SOURCE_BUCKET_LIVE_PAPER else 0.0,
        source_counts=source_counts,
        diagnostics={
            "formula": "0.40*hit_rate+0.30*normalized_avg_excess+0.30*positive_ic",
            "min_samples": min_samples,
            "calibrated_samples": calibrated_samples,
            "naked_number_guard": True,
            "construction_epoch": construction_epoch,
            "construction_epoch_id": construction_epoch.get("epoch_id"),
            **statistical_interpretation(n=n, hit_rate=hit_rate),
        },
        created_at=created_at,
    )


def _build_combined_profiles(
    source_profiles: list[ConvictionProfile],
    *,
    as_of_date: date,
    min_samples: int,
    calibrated_samples: int,
    created_at: datetime,
) -> list[ConvictionProfile]:
    by_base_key: dict[tuple[str, str, str | None, str, str, int, str], dict[str, ConvictionProfile]] = {}
    for profile in source_profiles:
        if profile.source_bucket not in {SOURCE_BUCKET_HISTORICAL_PRIOR, SOURCE_BUCKET_LIVE_PAPER}:
            continue
        base = (
            profile.strategy_id,
            profile.ticker,
            profile.branch,
            profile.action,
            profile.regime_at_signal,
            profile.horizon_days,
            construction_epoch_id_from_profile(profile),
        )
        by_base_key.setdefault(base, {})[profile.source_bucket] = profile

    combined: list[ConvictionProfile] = []
    for base, buckets in sorted(by_base_key.items(), key=lambda item: (
        item[0][0],
        item[0][1],
        item[0][2] or "",
        item[0][3],
        item[0][4],
        item[0][5],
        item[0][6],
    )):
        hist = buckets.get(SOURCE_BUCKET_HISTORICAL_PRIOR)
        live = buckets.get(SOURCE_BUCKET_LIVE_PAPER)
        if hist is None and live is None:
            continue
        hist_n = hist.n if hist else 0
        live_n = live.n if live else 0
        hist_weight, live_weight = _combined_weights(hist_n, live_n)
        if live_n < min_samples and hist is not None:
            conviction = hist.conviction
            status = STATUS_HISTORICAL_REQUIRES_LIVE
            hist_weight = 1.0
            live_weight = 0.0
            requires_live_confirmation = True
        elif hist is None:
            conviction = live.conviction if live else None
            status = live.status if live else STATUS_INSUFFICIENT_SAMPLES
            requires_live_confirmation = False
        elif live is None:
            conviction = hist.conviction
            status = STATUS_HISTORICAL_REQUIRES_LIVE
            hist_weight = 1.0
            live_weight = 0.0
            requires_live_confirmation = True
        else:
            conviction = _weighted_metric(hist.conviction, live.conviction, hist_weight, live_weight)
            status = (
                STATUS_EARLY_LIVE_CONFIRMATION
                if live_n < calibrated_samples
                else STATUS_CALIBRATED
            )
            requires_live_confirmation = live_n < calibrated_samples

        key = _ProfileKey(
            strategy_id=base[0],
            ticker=base[1],
            branch=base[2],
            action=base[3],
            regime_at_signal=base[4],
            horizon_days=base[5],
            source_bucket=SOURCE_BUCKET_COMBINED,
            construction_epoch_id=base[6],
        )
        construction_epoch = _construction_epoch_for_combined_profile(hist, live)
        combined.append(_profile_from_metrics(
            key=key,
            as_of_date=as_of_date,
            conviction=round(conviction, 4) if conviction is not None else None,
            status=status,
            n=hist_n + live_n,
            required_samples=calibrated_samples,
            hit_rate=_weighted_metric(
                hist.hit_rate if hist else None,
                live.hit_rate if live else None,
                hist_weight,
                live_weight,
            ),
            avg_forward_return=_weighted_metric(
                hist.avg_forward_return if hist else None,
                live.avg_forward_return if live else None,
                hist_weight,
                live_weight,
            ),
            avg_excess_vs_spy=_weighted_metric(
                hist.avg_excess_vs_spy if hist else None,
                live.avg_excess_vs_spy if live else None,
                hist_weight,
                live_weight,
            ),
            ic=_weighted_metric(
                hist.ic if hist else None,
                live.ic if live else None,
                hist_weight,
                live_weight,
            ),
            max_adverse_drawdown=min(
                value for value in [
                    hist.max_adverse_drawdown if hist else None,
                    live.max_adverse_drawdown if live else None,
                ]
                if value is not None
            ) if any(
                value is not None
                for value in [
                    hist.max_adverse_drawdown if hist else None,
                    live.max_adverse_drawdown if live else None,
                ]
            ) else None,
            data_lag_filtered=(hist.data_lag_filtered if hist else 0) + (live.data_lag_filtered if live else 0),
            requires_live_confirmation=requires_live_confirmation,
            hist_n=hist_n,
            live_n=live_n,
            hist_weight=hist_weight,
            live_weight=live_weight,
            source_counts={
                SOURCE_BUCKET_HISTORICAL_PRIOR: hist_n,
                SOURCE_BUCKET_LIVE_PAPER: live_n,
            },
            diagnostics={
                "combine_rule": "sample_size_weighted_live_bonus",
                "live_credibility_bonus": LIVE_CREDIBILITY_BONUS,
                "max_live_weight": MAX_LIVE_WEIGHT,
                "min_samples": min_samples,
                "calibrated_samples": calibrated_samples,
                "naked_number_guard": True,
                "construction_epoch": construction_epoch,
                "construction_epoch_id": construction_epoch.get("epoch_id"),
                **statistical_interpretation(
                    n=hist_n + live_n,
                    hit_rate=_weighted_metric(
                        hist.hit_rate if hist else None,
                        live.hit_rate if live else None,
                        hist_weight,
                        live_weight,
                    ),
                ),
            },
            created_at=created_at,
        ))
    return combined


def _construction_epoch_for_combined_profile(
    hist: ConvictionProfile | None,
    live: ConvictionProfile | None,
) -> dict[str, Any]:
    for profile in (live, hist):
        if profile is None:
            continue
        diagnostics = profile.diagnostics if isinstance(profile.diagnostics, dict) else {}
        epoch = diagnostics.get("construction_epoch")
        if isinstance(epoch, dict) and epoch.get("epoch_id"):
            return dict(epoch)
    return {"epoch_id": "unknown"}


def _profile_from_metrics(
    *,
    key: _ProfileKey,
    as_of_date: date,
    conviction: float | None,
    status: str,
    n: int,
    required_samples: int,
    hit_rate: float | None,
    avg_forward_return: float | None,
    avg_excess_vs_spy: float | None,
    ic: float | None,
    max_adverse_drawdown: float | None,
    data_lag_filtered: int,
    requires_live_confirmation: bool,
    hist_n: int,
    live_n: int,
    hist_weight: float | None,
    live_weight: float | None,
    source_counts: dict[str, int],
    diagnostics: dict[str, Any],
    created_at: datetime,
) -> ConvictionProfile:
    profile_id = _stable_id(
        "conviction",
        as_of_date.isoformat(),
        key.strategy_id,
        key.ticker,
        key.branch or "",
        key.action,
        key.regime_at_signal,
        key.horizon_days,
        key.source_bucket,
        key.construction_epoch_id,
    )
    return ConvictionProfile(
        profile_id=profile_id,
        as_of_date=as_of_date,
        strategy_id=key.strategy_id,
        ticker=key.ticker,
        branch=key.branch,
        action=key.action,
        regime_at_signal=key.regime_at_signal,
        horizon_days=key.horizon_days,
        source_bucket=key.source_bucket,
        conviction=round(conviction, 4) if conviction is not None else None,
        status=status,
        n=n,
        required_samples=required_samples,
        hit_rate=round(hit_rate, 4) if hit_rate is not None else None,
        avg_forward_return=round(avg_forward_return, 8) if avg_forward_return is not None else None,
        avg_excess_vs_spy=round(avg_excess_vs_spy, 8) if avg_excess_vs_spy is not None else None,
        ic=round(ic, 4) if ic is not None else None,
        max_adverse_drawdown=round(max_adverse_drawdown, 8) if max_adverse_drawdown is not None else None,
        data_lag_filtered=data_lag_filtered,
        requires_live_confirmation=requires_live_confirmation,
        hist_n=hist_n,
        live_n=live_n,
        hist_weight=round(hist_weight, 4) if hist_weight is not None else None,
        live_weight=round(live_weight, 4) if live_weight is not None else None,
        source_counts=dict(source_counts),
        diagnostics=dict(diagnostics),
        created_at=created_at,
    )


def _key_for(signal: FrozenSignal, outcome: SignalOutcome, source_bucket: str) -> _ProfileKey:
    construction_epoch = construction_epoch_from_signal(signal)
    return _ProfileKey(
        strategy_id=signal.strategy_id or outcome.strategy_id,
        ticker=signal.ticker or outcome.ticker,
        branch=signal.branch if signal.branch is not None else outcome.branch,
        action=signal.action or outcome.action,
        regime_at_signal=signal.regime_at_signal or "unknown",
        horizon_days=outcome.horizon_days,
        source_bucket=source_bucket,
        construction_epoch_id=str(construction_epoch.get("epoch_id") or "unknown"),
    )


def _sort_key(key: _ProfileKey) -> tuple[str, str, str, str, str, int, str, str]:
    return (
        key.strategy_id,
        key.ticker,
        key.branch or "",
        key.action,
        key.regime_at_signal,
        key.horizon_days,
        key.source_bucket,
        key.construction_epoch_id,
    )


def _sample_status(*, n: int, min_samples: int, calibrated_samples: int) -> str:
    if n < min_samples:
        return STATUS_INSUFFICIENT_SAMPLES
    if n < calibrated_samples:
        return STATUS_EARLY_ESTIMATE
    return STATUS_CALIBRATED


def _conviction_score(
    hit_rate: float | None,
    avg_excess_vs_spy: float | None,
    ic: float | None,
) -> float:
    hit_component = _clamp(hit_rate or 0.0, 0.0, 1.0)
    excess_component = _clamp((avg_excess_vs_spy or 0.0) / 0.02, 0.0, 1.0)
    ic_component = _clamp(ic or 0.0, 0.0, 1.0)
    return round(
        0.40 * hit_component
        + 0.30 * excess_component
        + 0.30 * ic_component,
        4,
    )


def _combined_weights(hist_n: int, live_n: int) -> tuple[float, float]:
    total = hist_n + live_n
    if total <= 0:
        return 0.0, 0.0
    if hist_n <= 0:
        return 0.0, 1.0
    if live_n <= 0:
        return 1.0, 0.0
    live_base = live_n / total
    live_weight = min(live_base * LIVE_CREDIBILITY_BONUS, MAX_LIVE_WEIGHT)
    hist_weight = 1.0 - live_weight
    return hist_weight, live_weight


def _weighted_metric(
    hist_value: float | None,
    live_value: float | None,
    hist_weight: float,
    live_weight: float,
) -> float | None:
    parts: list[tuple[float, float]] = []
    if hist_value is not None and hist_weight > 0:
        parts.append((hist_value, hist_weight))
    if live_value is not None and live_weight > 0:
        parts.append((live_value, live_weight))
    if not parts:
        return None
    total_weight = sum(weight for _, weight in parts)
    if total_weight <= 0:
        return None
    return sum(value * weight for value, weight in parts) / total_weight


def _pearson(xs: list[float], ys: list[float]) -> float | None:
    if len(xs) < 2 or len(xs) != len(ys):
        return None
    mean_x = sum(xs) / len(xs)
    mean_y = sum(ys) / len(ys)
    num = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    den_x = math.sqrt(sum((x - mean_x) ** 2 for x in xs))
    den_y = math.sqrt(sum((y - mean_y) ** 2 for y in ys))
    if den_x == 0 or den_y == 0:
        return 0.0
    return num / (den_x * den_y)


def _mean(values: Iterable[float]) -> float | None:
    items = list(values)
    if not items:
        return None
    return sum(items) / len(items)


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


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


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(value) if value is not None else default
    except (TypeError, ValueError):
        return default


def _optional_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes"}:
            return True
        if lowered in {"false", "0", "no"}:
            return False
    return None


def _stable_id(*parts: Any) -> str:
    payload = json.dumps([str(part) for part in parts], sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:32]


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
