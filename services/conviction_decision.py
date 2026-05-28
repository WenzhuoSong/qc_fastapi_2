"""Decision-facing conviction semantics.

Operational conviction status is useful for monitoring, but alpha decision
credit must use the stricter statistical interpretation.
"""
from __future__ import annotations

from typing import Any


STAT_STATUS_INSUFFICIENT = "insufficient"
STAT_STATUS_EARLY_SIGNAL = "early_signal"
STAT_STATUS_INDICATIVE = "indicative"
STAT_STATUS_STATISTICALLY_MEANINGFUL = "statistically_meaningful"
STAT_INSUFFICIENT_SAMPLES = 30
STAT_INDICATIVE_SAMPLES = 100
STAT_MEANINGFUL_SAMPLES = 300


STATISTICAL_STATUS_SET = {
    STAT_STATUS_INSUFFICIENT,
    STAT_STATUS_EARLY_SIGNAL,
    STAT_STATUS_INDICATIVE,
    STAT_STATUS_STATISTICALLY_MEANINGFUL,
}

DECISION_CONVICTION_DISCOUNT_MAP = {
    STAT_STATUS_STATISTICALLY_MEANINGFUL: 1.00,
    STAT_STATUS_INDICATIVE: 0.35,
    STAT_STATUS_EARLY_SIGNAL: 0.10,
    STAT_STATUS_INSUFFICIENT: 0.00,
    "missing_profile": 0.00,
}


def decision_statistical_status(
    *,
    status: str | None = None,
    n: int | None = None,
    diagnostics: dict[str, Any] | None = None,
) -> str:
    """Return the conservative decision-facing statistical status."""
    diag = diagnostics if isinstance(diagnostics, dict) else {}
    raw = str(
        diag.get("statistical_status")
        or status
        or ""
    ).strip()
    if raw in STATISTICAL_STATUS_SET:
        return raw
    if n is not None:
        return statistical_status_for_samples(max(int(n), 0))
    return STAT_STATUS_INSUFFICIENT


def statistical_status_for_samples(n: int) -> str:
    if n < STAT_INSUFFICIENT_SAMPLES:
        return STAT_STATUS_INSUFFICIENT
    if n < STAT_INDICATIVE_SAMPLES:
        return STAT_STATUS_EARLY_SIGNAL
    if n < STAT_MEANINGFUL_SAMPLES:
        return STAT_STATUS_INDICATIVE
    return STAT_STATUS_STATISTICALLY_MEANINGFUL


def decision_conviction_discount(status: str | None) -> float:
    """Return allocation/decision credit for a statistical status."""
    return DECISION_CONVICTION_DISCOUNT_MAP.get(
        str(status or STAT_STATUS_INSUFFICIENT),
        DECISION_CONVICTION_DISCOUNT_MAP[STAT_STATUS_INSUFFICIENT],
    )


def decision_effective_confidence(
    *,
    confidence: float,
    conviction: float | None,
    statistical_status: str | None,
) -> float:
    """Conservative shadow confidence used by diagnostics/cost decisions."""
    if conviction is None:
        return 0.0
    clean_confidence = max(0.0, min(float(confidence or 0.0), 1.0))
    clean_conviction = max(0.0, min(float(conviction or 0.0), 1.0))
    discount = decision_conviction_discount(statistical_status)
    return round(clean_confidence * clean_conviction * discount, 6)
