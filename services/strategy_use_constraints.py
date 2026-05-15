"""
Hard constraints derived from Strategy Playground suggested_use.

This layer does not choose a strategy and does not create buy signals. It only
tightens PM-proposed weights when the structured playground evidence says the
strategy layer is advisory-only or non-actionable.
"""
from __future__ import annotations

from typing import Any


def apply_strategy_use_constraints(
    *,
    base_weights: dict[str, float],
    adjusted_weights: dict[str, float],
    strategy_evidence: dict[str, Any] | None,
) -> tuple[dict[str, float], list[str]]:
    strategies = strategy_evidence or {}
    if not strategies.get("playground_available"):
        return _normalize(adjusted_weights), []

    use_summary = strategies.get("strategy_use_summary") or {}
    primary = list(use_summary.get("primary") or [])
    advisory = list(use_summary.get("advisory") or [])
    if primary:
        return _normalize(adjusted_weights), []

    supported_tickers = _selected_tickers_for_uses(
        strategies.get("strategy_results") or [],
        allowed_uses={"advisory"} if advisory else set(),
    )
    if advisory:
        return _clip_to_strategy_permission(
            base_weights=base_weights,
            adjusted_weights=adjusted_weights,
            max_delta=0.03,
            allow_new_supported=True,
            supported_tickers=supported_tickers,
            reason_prefix="strategy_advisory_only",
        )

    return _clip_to_strategy_permission(
        base_weights=base_weights,
        adjusted_weights=adjusted_weights,
        max_delta=0.01,
        allow_new_supported=False,
        supported_tickers=set(),
        reason_prefix="no_actionable_strategy",
    )


def _clip_to_strategy_permission(
    *,
    base_weights: dict[str, float],
    adjusted_weights: dict[str, float],
    max_delta: float,
    allow_new_supported: bool,
    supported_tickers: set[str],
    reason_prefix: str,
) -> tuple[dict[str, float], list[str]]:
    base = _clean(base_weights)
    adjusted = _clean(adjusted_weights)
    work = dict(adjusted)
    work.setdefault("CASH", 0.0)
    clip_log: list[str] = []

    for ticker in sorted((set(base) | set(adjusted)) - {"CASH"}):
        target = float(work.get(ticker, 0.0) or 0.0)
        base_w = float(base.get(ticker, 0.0) or 0.0)
        if target <= 0:
            continue

        is_new = base_w <= 0.01 and target > 0.01
        if is_new and (not allow_new_supported or ticker not in supported_tickers):
            replacement = base_w
            work[ticker] = replacement
            work["CASH"] += max(target - replacement, 0.0)
            clip_log.append(f"{reason_prefix}:new_position_blocked:{ticker} {target:.2%}->{replacement:.2%}")
            target = replacement

        upper = base_w + max_delta
        lower = max(base_w - max_delta, 0.0)
        if target > upper:
            work[ticker] = upper
            work["CASH"] += target - upper
            clip_log.append(f"{reason_prefix}:max_delta:{ticker} {target:.2%}->{upper:.2%}")
        elif target < lower:
            needed = lower - target
            available_cash = max(float(work.get("CASH", 0.0) or 0.0), 0.0)
            add_back = min(needed, available_cash)
            if add_back > 0:
                work[ticker] = target + add_back
                work["CASH"] = available_cash - add_back
                clip_log.append(f"{reason_prefix}:sell_delta:{ticker} {target:.2%}->{work[ticker]:.2%}")

    return _normalize(work), clip_log


def _selected_tickers_for_uses(
    strategy_results: list[dict[str, Any]],
    *,
    allowed_uses: set[str],
) -> set[str]:
    tickers: set[str] = set()
    if not allowed_uses:
        return tickers
    for row in strategy_results:
        if not isinstance(row, dict):
            continue
        if str(row.get("suggested_use") or "") not in allowed_uses:
            continue
        for ticker in row.get("selected_tickers") or []:
            clean = str(ticker or "").upper().strip()
            if clean and clean != "CASH":
                tickers.add(clean)
    return tickers


def _clean(weights: dict[str, Any] | None) -> dict[str, float]:
    out: dict[str, float] = {}
    for ticker, weight in (weights or {}).items():
        clean_ticker = str(ticker or "").upper().strip()
        if not clean_ticker:
            continue
        try:
            out[clean_ticker] = max(float(weight), 0.0)
        except (TypeError, ValueError):
            out[clean_ticker] = 0.0
    return out


def _normalize(weights: dict[str, Any] | None) -> dict[str, float]:
    clean = _clean(weights)
    total = sum(clean.values())
    if total <= 0:
        return {"CASH": 1.0}
    normalized = {ticker: round(weight / total, 6) for ticker, weight in clean.items() if weight > 1e-9}
    diff = round(1.0 - sum(normalized.values()), 6)
    if abs(diff) > 1e-9:
        target = "CASH" if "CASH" in normalized else max(normalized, key=normalized.get)
        normalized[target] = round(normalized.get(target, 0.0) + diff, 6)
    return normalized
