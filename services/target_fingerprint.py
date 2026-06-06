"""Deterministic target fingerprints for SetWeights idempotency."""
from __future__ import annotations

import hashlib
import json
import math
from typing import Any


DEFAULT_TARGET_FINGERPRINT_TOLERANCE = 0.0025
DEFAULT_TARGET_COMMAND_TYPE = "SetWeights"

METADATA_NOT_HASHED_KEYS = {
    "analysis_id",
    "command_id",
    "construction_epoch_id",
    "correlation_id",
    "created_at",
    "epoch_id",
    "recorded_at",
    "timestamp",
}


def build_target_fingerprint(
    target_weights: dict[str, Any] | None,
    *,
    command_type: str | None = DEFAULT_TARGET_COMMAND_TYPE,
    policy_version: str | None = None,
    tolerance: float | None = DEFAULT_TARGET_FINGERPRINT_TOLERANCE,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Return a stable fingerprint for a target command.

    The hash intentionally excludes lifecycle identifiers such as command_id,
    correlation_id, analysis_id, construction epoch, and timestamps. Those
    values are useful diagnostics, but including them would make every run
    unique and would defeat same-target dedupe.
    """
    clean_tolerance = _clean_tolerance(tolerance)
    normalized_weights = normalize_target_weights_for_fingerprint(
        target_weights,
        tolerance=clean_tolerance,
    )
    clean_command_type = _clean_command_type(command_type)
    clean_policy_version = str(policy_version or "").strip()
    canonical_payload = {
        "command_type": clean_command_type,
        "normalized_weights": normalized_weights,
        "policy_version": clean_policy_version,
    }
    canonical_json = json.dumps(
        canonical_payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )
    return {
        "fingerprint": hashlib.sha256(canonical_json.encode("utf-8")).hexdigest(),
        "normalized_weights": normalized_weights,
        "dedupe_tolerance": clean_tolerance,
        "policy_version": clean_policy_version,
        "command_type": clean_command_type,
        "metadata_not_hashed": _metadata_not_hashed(metadata or {}),
    }


def normalize_target_weights_for_fingerprint(
    target_weights: dict[str, Any] | None,
    *,
    tolerance: float | None = DEFAULT_TARGET_FINGERPRINT_TOLERANCE,
) -> dict[str, float]:
    """Clean, bucket, and sort target weights before hashing."""
    clean_tolerance = _clean_tolerance(tolerance)
    normalized: dict[str, float] = {}
    for raw_ticker, raw_weight in (target_weights or {}).items():
        ticker = str(raw_ticker or "").upper().strip()
        if not ticker or ticker == "CASH":
            continue
        try:
            weight = max(float(raw_weight or 0.0), 0.0)
        except (TypeError, ValueError):
            continue
        if weight < clean_tolerance:
            continue
        bucketed = _bucket_weight(weight, clean_tolerance)
        if bucketed < clean_tolerance:
            continue
        normalized[ticker] = bucketed
    return {ticker: normalized[ticker] for ticker in sorted(normalized)}


def _bucket_weight(weight: float, tolerance: float) -> float:
    bucket = math.floor((float(weight) / tolerance) + 0.5) * tolerance
    return round(bucket, 10)


def _clean_tolerance(tolerance: float | None) -> float:
    try:
        value = float(tolerance)
    except (TypeError, ValueError):
        value = DEFAULT_TARGET_FINGERPRINT_TOLERANCE
    if value <= 0:
        return DEFAULT_TARGET_FINGERPRINT_TOLERANCE
    return value


def _clean_command_type(command_type: str | None) -> str:
    value = str(command_type or DEFAULT_TARGET_COMMAND_TYPE).strip()
    if not value:
        return DEFAULT_TARGET_COMMAND_TYPE
    if value.lower() in {"weight_adjustment", "setweights", "set_weights"}:
        return DEFAULT_TARGET_COMMAND_TYPE
    return value


def _metadata_not_hashed(metadata: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in metadata.items():
        clean_key = str(key or "").strip()
        if not clean_key:
            continue
        if clean_key in METADATA_NOT_HASHED_KEYS:
            out[clean_key] = _json_safe_scalar(value)
    return out


def _json_safe_scalar(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)
