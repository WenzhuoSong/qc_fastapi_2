"""Observe-only evidence quality cap diagnostics.

This module turns ETF vote coverage and EvidenceCard reliability into a
diagnostic cap. It never mutates target weights or execution state.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from services.execution_policy import get_policy
from services.strategy_evidence import EvidenceCard


CONTRACT_VERSION = "evidence_quality_cap_v1"
DEFAULT_MIN_MULTIPLIER = 0.10
DEFAULT_CONVICTION_DISCOUNT = 0.30
DEFAULT_HISTORY_DISCOUNT_WHEN_UNKNOWN = 1.0

CONVICTION_DISCOUNT_MAP = {
    "statistically_meaningful": 1.0,
    "calibrated": 0.80,
    "indicative": 0.60,
    "early_signal": 0.45,
    "early_live_confirmation": 0.45,
    "early_estimate": 0.45,
    "historical_prior_requires_live_confirmation": 0.50,
    "insufficient": 0.30,
    "insufficient_samples": 0.30,
    "missing_profile": 0.30,
}

CONVICTION_STATUS_PRIORITY = {
    "statistically_meaningful": 80,
    "calibrated": 70,
    "indicative": 60,
    "historical_prior_requires_live_confirmation": 50,
    "early_signal": 40,
    "early_live_confirmation": 40,
    "early_estimate": 40,
    "insufficient": 30,
    "insufficient_samples": 30,
    "missing_profile": 10,
}


@dataclass(frozen=True)
class EvidenceQualityCapConfig:
    min_multiplier: float = DEFAULT_MIN_MULTIPLIER
    coverage_weight: float = 0.40
    conviction_weight: float = 0.40
    history_weight: float = 0.20
    unknown_history_discount: float = DEFAULT_HISTORY_DISCOUNT_WHEN_UNKNOWN


@dataclass(frozen=True)
class EvidenceQualityCapDiagnostic:
    ticker: str
    static_cap: float
    coverage_ratio: float
    conviction_status: str
    conviction_discount: float
    history_days: int | None
    history_discount: float
    evidence_quality_multiplier: float
    evidence_adjusted_cap: float
    would_clip: bool
    would_clip_to: float | None
    current_or_target_weight: float
    voted_count: int
    eligible_strategy_count: int
    abstain_count: int
    mapping_error_count: int
    execution_effect: str = "diagnostic_only"
    contract_version: str = CONTRACT_VERSION

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def evaluate_evidence_quality_caps(
    *,
    vote_summary: dict[str, dict[str, Any]] | None,
    evidence_cards: list[EvidenceCard | dict[str, Any]] | None,
    current_or_target_weights: dict[str, Any] | None = None,
    config: dict[str, Any] | EvidenceQualityCapConfig | None = None,
) -> dict[str, dict[str, Any]]:
    """Compute observe-only evidence-adjusted caps per ETF.

    The output is intended for diagnostics and later shadow consumption. It
    must not be used to mutate weights unless a later gated PR explicitly does
    so.
    """
    cfg = _normalize_config(config)
    cards_by_ticker = _cards_by_ticker(evidence_cards or [])
    weights = _clean_weight_map(current_or_target_weights or {})
    tickers = sorted(set(vote_summary or {}) | set(cards_by_ticker) | set(weights))
    out: dict[str, dict[str, Any]] = {}

    for ticker in tickers:
        if ticker == "CASH":
            continue
        row = dict((vote_summary or {}).get(ticker) or {})
        cards = cards_by_ticker.get(ticker) or []
        static_cap = _static_cap(ticker, cards)
        coverage_ratio = _clamp(_to_float(row.get("coverage_ratio"), 0.0))
        conviction_status = _best_conviction_status(cards)
        conviction_discount = get_conviction_discount(conviction_status)
        history_days = _history_days(row, cards)
        history_discount = _history_discount(history_days, cfg.unknown_history_discount)
        multiplier = evidence_quality_multiplier(
            coverage_ratio=coverage_ratio,
            conviction_discount=conviction_discount,
            history_discount=history_discount,
            config=cfg,
        )
        adjusted_cap = round(static_cap * multiplier, 6)
        current_or_target_weight = round(_to_float(weights.get(ticker), 0.0), 6)
        would_clip = current_or_target_weight > adjusted_cap + 1e-12
        diag = EvidenceQualityCapDiagnostic(
            ticker=ticker,
            static_cap=round(static_cap, 6),
            coverage_ratio=round(coverage_ratio, 6),
            conviction_status=conviction_status,
            conviction_discount=round(conviction_discount, 6),
            history_days=history_days,
            history_discount=round(history_discount, 6),
            evidence_quality_multiplier=round(multiplier, 6),
            evidence_adjusted_cap=adjusted_cap,
            would_clip=bool(would_clip),
            would_clip_to=adjusted_cap if would_clip else None,
            current_or_target_weight=current_or_target_weight,
            voted_count=_to_int(row.get("voted_count"), 0),
            eligible_strategy_count=_to_int(row.get("eligible_strategy_count"), 0),
            abstain_count=_to_int(row.get("abstain_count"), 0),
            mapping_error_count=_to_int(row.get("mapping_error_count"), 0),
        )
        out[ticker] = diag.to_dict()
    return out


def get_conviction_discount(status: str | None) -> float:
    return CONVICTION_DISCOUNT_MAP.get(str(status or "missing_profile"), DEFAULT_CONVICTION_DISCOUNT)


def evidence_quality_multiplier(
    *,
    coverage_ratio: float,
    conviction_discount: float,
    history_discount: float,
    config: EvidenceQualityCapConfig | None = None,
) -> float:
    cfg = config or EvidenceQualityCapConfig()
    raw = (
        cfg.coverage_weight * _clamp(coverage_ratio)
        + cfg.conviction_weight * _clamp(conviction_discount)
        + cfg.history_weight * _clamp(history_discount)
    )
    return round(max(_clamp(raw), _clamp(cfg.min_multiplier)), 6)


def _normalize_config(config: dict[str, Any] | EvidenceQualityCapConfig | None) -> EvidenceQualityCapConfig:
    if isinstance(config, EvidenceQualityCapConfig):
        return config
    raw = config or {}
    return EvidenceQualityCapConfig(
        min_multiplier=_clamp(_to_float(raw.get("min_multiplier"), DEFAULT_MIN_MULTIPLIER)),
        coverage_weight=max(0.0, _to_float(raw.get("coverage_weight"), 0.40)),
        conviction_weight=max(0.0, _to_float(raw.get("conviction_weight"), 0.40)),
        history_weight=max(0.0, _to_float(raw.get("history_weight"), 0.20)),
        unknown_history_discount=_clamp(
            _to_float(raw.get("unknown_history_discount"), DEFAULT_HISTORY_DISCOUNT_WHEN_UNKNOWN)
        ),
    )


def _cards_by_ticker(cards: list[EvidenceCard | dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {}
    for card in cards:
        row = card.to_dict() if isinstance(card, EvidenceCard) else dict(card or {})
        ticker = str(row.get("ticker") or "").upper().strip()
        if ticker:
            out.setdefault(ticker, []).append(row)
    return out


def _static_cap(ticker: str, cards: list[dict[str, Any]]) -> float:
    role_cap = float(get_policy(ticker).max_single_weight)
    base_caps: list[float] = []
    fallback_caps: list[float] = []
    for row in cards:
        diagnostics = row.get("diagnostics") if isinstance(row.get("diagnostics"), dict) else {}
        base = _optional_float(diagnostics.get("base_cap"))
        if base is not None:
            base_caps.append(base)
        fallback = _optional_float(row.get("max_reasonable_weight"))
        if fallback is not None:
            fallback_caps.append(fallback)
    if base_caps:
        asset_cap = max(base_caps)
    elif any(cap > 0 for cap in fallback_caps):
        asset_cap = max(cap for cap in fallback_caps if cap > 0)
    else:
        asset_cap = role_cap
    return max(0.0, min(float(asset_cap), role_cap))


def _best_conviction_status(cards: list[dict[str, Any]]) -> str:
    statuses = [str(row.get("conviction_status") or "missing_profile") for row in cards]
    if not statuses:
        return "missing_profile"
    return max(statuses, key=lambda item: CONVICTION_STATUS_PRIORITY.get(item, 0))


def _history_days(row: dict[str, Any], cards: list[dict[str, Any]]) -> int | None:
    candidates: list[int] = []
    for value in (
        row.get("history_days"),
        (row.get("diagnostics") or {}).get("history_days") if isinstance(row.get("diagnostics"), dict) else None,
    ):
        parsed = _optional_int(value)
        if parsed is not None:
            candidates.append(parsed)
    for card in cards:
        vote_diag = card.get("vote_diagnostics") if isinstance(card.get("vote_diagnostics"), dict) else {}
        diag = card.get("diagnostics") if isinstance(card.get("diagnostics"), dict) else {}
        for value in (vote_diag.get("history_days"), diag.get("history_days")):
            parsed = _optional_int(value)
            if parsed is not None:
                candidates.append(parsed)
    return max(candidates) if candidates else None


def _history_discount(history_days: int | None, unknown_discount: float) -> float:
    if history_days is None:
        return _clamp(unknown_discount)
    return _clamp(float(history_days) / 252.0)


def _clean_weight_map(weights: dict[str, Any]) -> dict[str, float]:
    out: dict[str, float] = {}
    for ticker, weight in weights.items():
        clean = str(ticker or "").upper().strip()
        if not clean:
            continue
        out[clean] = _to_float(weight, 0.0)
    return out


def _clamp(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def _optional_float(value: Any) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _to_float(value: Any, default: float = 0.0) -> float:
    parsed = _optional_float(value)
    return float(default) if parsed is None else parsed


def _optional_int(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _to_int(value: Any, default: int = 0) -> int:
    parsed = _optional_int(value)
    return int(default) if parsed is None else parsed
