"""Pre-risk proposal shaping.

This layer makes PM proposals respect obvious execution-space constraints before
Risk Manager performs final validation. It does not approve trades and does not
replace position governance.
"""
from __future__ import annotations

from typing import Any


LOSS_REVIEW_PCT = -0.04
HUMAN_OR_LIMITED_MAX_TURNOVER = 0.05
HUMAN_OR_LIMITED_MAX_SINGLE_DELTA = 0.015


def shape_proposal_before_risk(
    *,
    adjusted_weights: dict[str, Any],
    current_weights: dict[str, Any],
    holdings_meta: list[dict[str, Any]],
    market_scorecard: dict[str, Any] | None,
    decision_style: dict[str, Any] | None,
) -> dict[str, Any]:
    """Return a clipped proposal plus observability metadata.

    Current scope:
    - no add into loss-review holdings
    - cap single-ticker deltas under human_required/data-limited/small-overweight
    - cap total turnover before Risk Manager sees the proposal
    """
    current = _clean(current_weights)
    proposed = _clean(adjusted_weights)
    scorecard = market_scorecard or {}
    style = decision_style or {}

    work = dict(proposed)
    work.setdefault("CASH", proposed.get("CASH", current.get("CASH", 0.0)))
    clip_log: list[str] = []

    loss_review = _loss_review_tickers(holdings_meta)
    for ticker in sorted(loss_review):
        if ticker == "CASH":
            continue
        target = float(work.get(ticker, 0.0) or 0.0)
        current_w = float(current.get(ticker, 0.0) or 0.0)
        if current_w > 0 and target > current_w + 1e-9:
            work[ticker] = current_w
            work["CASH"] = float(work.get("CASH", 0.0) or 0.0) + (target - current_w)
            clip_log.append(f"loss_review_no_add:{ticker} {target:.2%}->{current_w:.2%}")

    constrained = _is_constrained(scorecard, style)
    single_delta_cap = _single_delta_cap(scorecard, style) if constrained else None
    turnover_cap = _turnover_cap(scorecard, style) if constrained else None

    if single_delta_cap is not None:
        work = _cap_single_deltas(work, current, single_delta_cap, clip_log)
    if turnover_cap is not None:
        work = _cap_turnover(work, current, turnover_cap, clip_log)

    shaped = _cash_first_normalize(work)
    return {
        "applied": bool(clip_log),
        "adjusted_weights": shaped,
        "clip_log": clip_log,
        "constraints": {
            "loss_review_no_add": sorted(loss_review),
            "constrained": constrained,
            "max_single_delta": single_delta_cap,
            "max_turnover": turnover_cap,
            "scorecard_permission": scorecard.get("investment_permission"),
            "scorecard_data_quality": scorecard.get("data_quality"),
            "human_required": bool(scorecard.get("require_human_confirmation")),
            "trade_style": style.get("trade_style"),
        },
    }


def _is_constrained(scorecard: dict[str, Any], style: dict[str, Any]) -> bool:
    permission = str(scorecard.get("investment_permission") or "")
    data_quality = str(scorecard.get("data_quality") or "")
    trade_style = str(style.get("trade_style") or "")
    return (
        bool(scorecard.get("require_human_confirmation"))
        or permission in {"small_overweight_only", "hold_or_trim", "reduce_risk_only", "defensive_only", "cash_only"}
        or data_quality in {"limited", "missing", "stale", "unknown"}
        or trade_style in {"hold_unless_strong", "risk_reduce_fast", "cash_only"}
    )


def _single_delta_cap(scorecard: dict[str, Any], style: dict[str, Any]) -> float:
    values = [HUMAN_OR_LIMITED_MAX_SINGLE_DELTA]
    scorecard_delta = _optional_float(scorecard.get("max_adjustment_from_base"))
    if scorecard_delta is not None:
        values.append(scorecard_delta)
    limits = style.get("style_limits") or {}
    style_single = _optional_float(limits.get("max_single_trade_pct"))
    style_buy = _optional_float(limits.get("max_buy_trade_pct"))
    if style_single is not None:
        values.append(style_single)
    if style_buy is not None:
        values.append(style_buy)
    return min(values)


def _turnover_cap(scorecard: dict[str, Any], style: dict[str, Any]) -> float:
    values = [HUMAN_OR_LIMITED_MAX_TURNOVER]
    scorecard_turnover = _optional_float(scorecard.get("max_turnover_per_cycle"))
    style_turnover = _optional_float((style.get("style_limits") or {}).get("max_turnover_per_cycle"))
    if scorecard_turnover is not None:
        values.append(scorecard_turnover)
    if style_turnover is not None:
        values.append(style_turnover)
    return min(values)


def _loss_review_tickers(holdings_meta: list[dict[str, Any]]) -> set[str]:
    tickers: set[str] = set()
    for row in holdings_meta or []:
        if not isinstance(row, dict):
            continue
        ticker = str(row.get("ticker") or row.get("symbol") or "").upper().strip()
        if not ticker:
            continue
        pnl = _optional_float(row.get("unrealized_pnl_pct"))
        if pnl is not None and pnl <= LOSS_REVIEW_PCT:
            tickers.add(ticker)
    return tickers


def _cap_single_deltas(
    weights: dict[str, float],
    current: dict[str, float],
    cap: float,
    clip_log: list[str],
) -> dict[str, float]:
    out = dict(weights)
    for ticker in sorted((set(out) | set(current)) - {"CASH"}):
        target = float(out.get(ticker, 0.0) or 0.0)
        current_w = float(current.get(ticker, 0.0) or 0.0)
        upper = current_w + cap
        lower = max(current_w - cap, 0.0)
        clipped = min(max(target, lower), upper)
        if abs(clipped - target) <= 1e-9:
            continue
        out[ticker] = clipped
        direction = "add" if target > current_w else "sell"
        clip_log.append(f"proposal_{direction}_cap:{ticker} {target:.2%}->{clipped:.2%}")
    return out


def _cap_turnover(
    weights: dict[str, float],
    current: dict[str, float],
    cap: float,
    clip_log: list[str],
) -> dict[str, float]:
    turnover = _turnover(weights, current)
    if turnover <= cap + 1e-9:
        return weights
    scale = cap / turnover if turnover > 0 else 1.0
    out: dict[str, float] = {}
    for ticker in set(weights) | set(current):
        out[ticker] = float(current.get(ticker, 0.0) or 0.0) + (
            float(weights.get(ticker, 0.0) or 0.0) - float(current.get(ticker, 0.0) or 0.0)
        ) * scale
    clip_log.append(f"proposal_turnover_scaled:{turnover:.2%}->{cap:.2%}")
    return out


def _cash_first_normalize(weights: dict[str, Any]) -> dict[str, float]:
    clean = _clean(weights)
    equity = sum(weight for ticker, weight in clean.items() if ticker != "CASH")
    if equity >= 1.0:
        scale = 1.0 / equity if equity > 0 else 0.0
        out = {
            ticker: round(weight * scale, 6)
            for ticker, weight in clean.items()
            if ticker != "CASH" and weight > 1e-9
        }
        out["CASH"] = round(max(1.0 - sum(out.values()), 0.0), 6)
        return out
    out = {
        ticker: round(weight, 6)
        for ticker, weight in clean.items()
        if ticker != "CASH" and weight > 1e-9
    }
    out["CASH"] = round(max(1.0 - sum(out.values()), 0.0), 6)
    return out


def _turnover(target: dict[str, Any], current: dict[str, Any]) -> float:
    keys = set(target) | set(current)
    return sum(
        abs(float(target.get(ticker, 0.0) or 0.0) - float(current.get(ticker, 0.0) or 0.0))
        for ticker in keys
    ) / 2.0


def _clean(weights: dict[str, Any] | None) -> dict[str, float]:
    out: dict[str, float] = {}
    for ticker, weight in (weights or {}).items():
        clean_ticker = str(ticker or "").upper().strip()
        if not clean_ticker:
            continue
        value = _optional_float(weight)
        out[clean_ticker] = max(value if value is not None else 0.0, 0.0)
    return out


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
