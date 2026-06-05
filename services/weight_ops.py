"""Shared weight arithmetic contract.

This module is the system's single home for reusable weight-map arithmetic.
It deliberately performs no policy decisions: callers decide *why* to cap,
normalize, or throttle; this module only applies the arithmetic and returns
diagnostics.

Internal calculations do not round. Round only at serialization, dashboard, or
ledger-display boundaries.
"""
from __future__ import annotations

from collections import defaultdict
from typing import Any


WeightMap = dict[str, float]
Diagnostics = dict[str, Any]


def normalize_cash_first(
    weights: WeightMap | dict[str, Any] | None,
    *,
    cash_key: str = "CASH",
) -> tuple[WeightMap, Diagnostics]:
    """Normalize by protecting non-cash weights and assigning CASH residual.

    Semantics:
    - non-cash weights are already the protected/capped risk weights
    - CASH is the residual, not an input value to preserve
    - if non-cash total is above 1.0, scale non-cash down to 1.0 and set CASH=0
    """
    cash_key = _clean_ticker(cash_key) or "CASH"
    clean = _clean_weights(weights)
    non_cash = {
        ticker: weight
        for ticker, weight in clean.items()
        if ticker != cash_key and weight > 0.0
    }
    non_cash_total = sum(non_cash.values())

    if non_cash_total <= 0.0:
        return {cash_key: 1.0}, {
            "normalized": False,
            "non_cash_total": 0.0,
            "cash_assigned": 1.0,
            "reason": "empty_non_cash",
        }

    if non_cash_total <= 1.0:
        result = dict(non_cash)
        cash_assigned = 1.0 - non_cash_total
        result[cash_key] = cash_assigned
        return result, {
            "normalized": False,
            "non_cash_total": non_cash_total,
            "cash_assigned": cash_assigned,
        }

    scale = 1.0 / non_cash_total
    result = {ticker: weight * scale for ticker, weight in non_cash.items()}
    result[cash_key] = 0.0
    return result, {
        "normalized": True,
        "scale": scale,
        "non_cash_total_before": non_cash_total,
        "cash_assigned": 0.0,
    }


def normalize_proportional(
    weights: WeightMap | dict[str, Any] | None,
    *,
    cash_key: str = "CASH",
) -> tuple[WeightMap, Diagnostics]:
    """Scale all positive weights, including CASH, to sum to 1.0.

    This is for initial construction only. Do not use it in cap/tighten paths.
    """
    cash_key = _clean_ticker(cash_key) or "CASH"
    clean = _clean_weights(weights)
    total = sum(weight for weight in clean.values() if weight > 0.0)
    if total <= 0.0:
        return {cash_key: 1.0}, {
            "normalized": False,
            "reason": "empty_weights",
        }

    result = {
        ticker: weight / total
        for ticker, weight in clean.items()
        if weight > 0.0
    }
    result.setdefault(cash_key, 0.0)
    return result, {
        "normalized": True,
        "scale": 1.0 / total,
        "total_before": total,
    }


def apply_single_caps_cash_first(
    weights: WeightMap | dict[str, Any] | None,
    caps: dict[str, Any] | None,
    *,
    cash_key: str = "CASH",
) -> tuple[WeightMap, Diagnostics]:
    """Apply per-ticker caps and move released weight to CASH."""
    cash_key = _clean_ticker(cash_key) or "CASH"
    result = _clean_weights(weights)
    cap_events: list[dict[str, float | str]] = []
    total_released = 0.0

    for raw_ticker, raw_cap in (caps or {}).items():
        ticker = _clean_ticker(raw_ticker)
        if not ticker or ticker == cash_key:
            continue
        cap = _optional_nonnegative_float(raw_cap)
        if cap is None:
            continue
        current = float(result.get(ticker, 0.0) or 0.0)
        if current > cap:
            released = current - cap
            result[ticker] = cap
            total_released += released
            cap_events.append(
                {
                    "ticker": ticker,
                    "before": current,
                    "after": cap,
                    "released": released,
                }
            )

    result[cash_key] = float(result.get(cash_key, 0.0) or 0.0) + total_released
    return result, {
        "cap_events": cap_events,
        "total_released": total_released,
    }


def apply_group_caps_cash_first(
    weights: WeightMap | dict[str, Any] | None,
    group_caps: dict[str, Any] | None,
    role_map: dict[str, str] | None,
    *,
    cash_key: str = "CASH",
) -> tuple[WeightMap, Diagnostics]:
    """Apply group caps by proportional reduction and move release to CASH."""
    cash_key = _clean_ticker(cash_key) or "CASH"
    result = _clean_weights(weights)
    clean_role_map = {
        _clean_ticker(ticker): str(role or "unknown").strip().lower()
        for ticker, role in (role_map or {}).items()
        if _clean_ticker(ticker)
    }
    clean_group_caps = {
        str(role or "").strip().lower(): cap
        for role, cap in (group_caps or {}).items()
        if str(role or "").strip()
    }
    group_totals: dict[str, float] = defaultdict(float)
    group_members: dict[str, list[str]] = defaultdict(list)

    for ticker, weight in result.items():
        if ticker == cash_key or weight <= 0.0:
            continue
        role = clean_role_map.get(ticker, "unknown")
        group_totals[role] += weight
        group_members[role].append(ticker)

    scale_events: list[dict[str, float | str]] = []
    total_released = 0.0
    for role, raw_cap in clean_group_caps.items():
        cap = _optional_nonnegative_float(raw_cap)
        if cap is None:
            continue
        total = group_totals.get(role, 0.0)
        if total <= cap:
            continue
        scale = cap / total if total > 0.0 else 1.0
        released = total * (1.0 - scale)
        for ticker in group_members.get(role, []):
            result[ticker] = result[ticker] * scale
        scale_events.append(
            {
                "role": role,
                "before_total": total,
                "cap": cap,
                "scale": scale,
                "released": released,
            }
        )
        total_released += released

    result[cash_key] = float(result.get(cash_key, 0.0) or 0.0) + total_released
    return result, {
        "group_scale_events": scale_events,
        "total_released": total_released,
    }


def tighten_buy_delta(
    target: WeightMap | dict[str, Any] | None,
    current: WeightMap | dict[str, Any] | None,
    max_buy_delta: float,
    *,
    cash_key: str = "CASH",
) -> tuple[WeightMap, Diagnostics]:
    """Limit positive buy deltas.

    This is tighten-only: non-cash output never exceeds target.
    """
    cash_key = _clean_ticker(cash_key) or "CASH"
    result = _clean_weights(target)
    current_clean = _clean_weights(current)
    max_delta = max(float(max_buy_delta or 0.0), 0.0)
    events: list[dict[str, float | str]] = []
    total_released = 0.0

    for ticker in sorted(set(result) - {cash_key}):
        target_weight = float(result.get(ticker, 0.0) or 0.0)
        current_weight = float(current_clean.get(ticker, 0.0) or 0.0)
        delta = target_weight - current_weight
        if delta <= max_delta:
            continue
        clipped = current_weight + max_delta
        released = target_weight - clipped
        result[ticker] = clipped
        total_released += released
        events.append(
            {
                "ticker": ticker,
                "mutation_type": "cap_single_buy_delta",
                "before": target_weight,
                "after": clipped,
                "delta_clipped": released,
            }
        )

    result[cash_key] = float(result.get(cash_key, 0.0) or 0.0) + total_released
    return result, {
        "events": events,
        "total_released": total_released,
    }


def tighten_sell_delta(
    target: WeightMap | dict[str, Any] | None,
    current: WeightMap | dict[str, Any] | None,
    max_sell_delta: float,
    *,
    cash_key: str = "CASH",
) -> tuple[WeightMap, Diagnostics]:
    """Limit negative sell deltas.

    This is conditional, not tighten-only: output can stay above the desired
    target while still reducing relative to current holdings.
    """
    cash_key = _clean_ticker(cash_key) or "CASH"
    result = _clean_weights(target)
    current_clean = _clean_weights(current)
    max_delta = max(float(max_sell_delta or 0.0), 0.0)
    events: list[dict[str, float | str]] = []

    for ticker in sorted((set(result) | set(current_clean)) - {cash_key}):
        target_weight = float(result.get(ticker, 0.0) or 0.0)
        current_weight = float(current_clean.get(ticker, 0.0) or 0.0)
        delta = target_weight - current_weight
        if delta >= -max_delta:
            continue
        clipped = max(current_weight - max_delta, 0.0)
        result[ticker] = clipped
        events.append(
            {
                "ticker": ticker,
                "mutation_type": "sell_delta_throttle",
                "before": target_weight,
                "after": clipped,
                "target_vs_after": clipped - target_weight,
            }
        )

    result.setdefault(cash_key, float(result.get(cash_key, 0.0) or 0.0))
    return result, {"events": events}


def apply_minimum_weight_floor(
    weights: WeightMap | dict[str, Any] | None,
    min_weight: float = 0.005,
    *,
    cash_key: str = "CASH",
) -> tuple[WeightMap, Diagnostics]:
    """Clear economically meaningless non-cash targets and move them to CASH.

    This is an execution-efficiency constraint: positive non-cash weights below
    the floor are too small to carry useful portfolio intent, but they still add
    monitoring and transaction noise.
    """
    cash_key = _clean_ticker(cash_key) or "CASH"
    floor = max(float(min_weight or 0.0), 0.0)
    result = _clean_weights(weights)
    result.setdefault(cash_key, float(result.get(cash_key, 0.0) or 0.0))
    cleared: list[dict[str, float | str]] = []
    total_released = 0.0

    if floor <= 0.0:
        return result, {
            "cleared_positions": [],
            "total_released": 0.0,
            "min_weight": floor,
        }

    for ticker in sorted(set(result) - {cash_key}):
        weight = float(result.get(ticker, 0.0) or 0.0)
        if weight <= 0.0 or weight >= floor:
            continue
        result[ticker] = 0.0
        total_released += weight
        cleared.append(
            {
                "ticker": ticker,
                "before": weight,
                "after": 0.0,
                "released": weight,
                "reason": f"below_min_{floor:.2%}",
            }
        )

    result[cash_key] = float(result.get(cash_key, 0.0) or 0.0) + total_released
    return result, {
        "cleared_positions": cleared,
        "total_released": total_released,
        "min_weight": floor,
    }


def assert_invariants(
    weights: WeightMap | dict[str, Any] | None,
    *,
    cash_key: str = "CASH",
    label: str = "",
    tolerance: float = 1e-6,
) -> None:
    """Assert execution-bound weight-map invariants."""
    cash_key = _clean_ticker(cash_key) or "CASH"
    clean = _clean_weights(weights)
    total = sum(value for value in clean.values() if value > 0.0)
    assert total <= 1.0 + tolerance, (
        f"[{label}] weight sum {total:.6f} > 1.0 - normalization bug upstream"
    )
    for ticker, weight in clean.items():
        assert weight >= -tolerance, (
            f"[{label}] {ticker} has negative weight {weight:.6f}"
        )
    assert cash_key in clean, f"[{label}] '{cash_key}' missing from weight map"
    assert clean[cash_key] >= -tolerance, (
        f"[{label}] CASH is negative: {clean[cash_key]:.6f}"
    )


def _clean_weights(weights: WeightMap | dict[str, Any] | None) -> WeightMap:
    out: WeightMap = {}
    for raw_ticker, raw_weight in (weights or {}).items():
        ticker = _clean_ticker(raw_ticker)
        if not ticker:
            continue
        value = _optional_nonnegative_float(raw_weight)
        if value is None:
            continue
        out[ticker] = value
    return out


def _clean_ticker(value: Any) -> str:
    return str(value or "").upper().strip()


def _optional_nonnegative_float(value: Any) -> float | None:
    try:
        parsed = float(value or 0.0)
    except (TypeError, ValueError):
        return None
    if parsed != parsed:
        return None
    return max(parsed, 0.0)
