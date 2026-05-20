"""Deterministic portfolio construction layer.

This module operates at portfolio level before per-ticker target governance. It
does not consume raw LLM weights and does not approve execution.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from services.group_contract import GROUP_DEFINITIONS, calc_factor_exposure, get_factor_tags


NO_ADD_PERMISSIONS = {"hold_or_trim", "reduce_risk_only", "defensive_only", "cash_only"}


@dataclass
class PortfolioConstructionResult:
    target_weights: dict[str, float]
    factor_exposures: dict[str, float]
    effective_n: float
    turnover: dict[str, Any]
    construction_steps: list[str]
    violations: list[str]
    diagnostics: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class PortfolioConstructionModel:
    """Construct portfolio-level target weights from deterministic inputs."""

    def __init__(self, *, basket_limit_multiplier: float = 0.70) -> None:
        self.basket_limit_multiplier = max(min(float(basket_limit_multiplier), 1.0), 0.0)

    def construct(
        self,
        *,
        base_weights: dict[str, Any],
        current_weights: dict[str, Any],
        signal_strengths: dict[str, Any] | None = None,
        basket_reviews: dict[str, Any] | list[dict[str, Any]] | None = None,
        scorecard_permission: str | None = None,
        turnover_budget: float | None = None,
    ) -> PortfolioConstructionResult:
        base = _normalize_cash_first(_clean_weights(base_weights))
        current = _normalize_cash_first(_clean_weights(current_weights))
        signals = _clean_signals(signal_strengths or {})
        active_baskets = _active_basket_groups(basket_reviews)
        budget = _optional_float(turnover_budget)
        steps: list[str] = ["base_weights"]
        violations: list[str] = []

        weights = dict(base)
        weights, factor_violations = self._apply_factor_limits(weights)
        violations.extend(factor_violations)
        steps.append("factor_limits")

        weights, basket_violations = self._apply_basket_constraints(weights, active_baskets)
        violations.extend(basket_violations)
        steps.append("basket_constraints")

        if str(scorecard_permission or "") in NO_ADD_PERMISSIONS:
            weights, no_add_violations = _clip_adds_to_current(weights, current)
            violations.extend(no_add_violations)
            steps.append("scorecard_no_add")

        turnover_before = _turnover(weights, current)
        if budget is not None and turnover_before > budget + 1e-9:
            weights = self._allocate_turnover_budget(
                target=weights,
                current=current,
                signal_strengths=signals,
                budget=budget,
            )
            violations.append(f"turnover_budget:{turnover_before:.2%}->{budget:.2%}")
            steps.append("turnover_budget")

        weights = _normalize_cash_first(weights)
        turnover_after = _turnover(weights, current)
        factor_exposures = {
            key: round(value, 6)
            for key, value in sorted(calc_factor_exposure(weights).items())
        }

        return PortfolioConstructionResult(
            target_weights=weights,
            factor_exposures=factor_exposures,
            effective_n=round(_effective_n(weights), 6),
            turnover={
                "estimated_before_budget": round(turnover_before, 6),
                "estimated": round(turnover_after, 6),
                "budget": budget,
                "within_budget": True if budget is None else turnover_after <= budget + 1e-9,
            },
            construction_steps=steps + ["normalization"],
            violations=violations + self._check_violations(weights, active_baskets),
            diagnostics={
                "mode": "portfolio_construction",
                "deterministic": True,
                "consumes_raw_llm_adjusted_weights": False,
                "basket_limit_multiplier": self.basket_limit_multiplier,
                "active_basket_reviews": sorted(active_baskets),
                "ticker_count": len([ticker for ticker in weights if ticker != "CASH" and weights[ticker] > 1e-9]),
            },
        )

    def _apply_factor_limits(self, weights: dict[str, float]) -> tuple[dict[str, float], list[str]]:
        out = dict(weights)
        violations: list[str] = []
        for group_name, definition in sorted(GROUP_DEFINITIONS.items()):
            exposure = _factor_exposure_for_group(out, group_name)
            if exposure <= definition.limit_pct + 1e-9:
                continue
            scale = definition.limit_pct / exposure if exposure > 0 else 1.0
            released = 0.0
            for ticker in _tickers_with_factor_tag(out, group_name):
                before = out.get(ticker, 0.0)
                after = before * scale
                out[ticker] = after
                released += before - after
            out["CASH"] = float(out.get("CASH", 0.0) or 0.0) + released
            violations.append(f"factor_limit:{group_name} {exposure:.2%}->{definition.limit_pct:.2%}")
        return _normalize_cash_first(out), violations

    def _apply_basket_constraints(
        self,
        weights: dict[str, float],
        active_baskets: set[str],
    ) -> tuple[dict[str, float], list[str]]:
        out = dict(weights)
        violations: list[str] = []
        for group_name in sorted(active_baskets):
            definition = GROUP_DEFINITIONS.get(group_name)
            if not definition:
                continue
            reduced_limit = definition.limit_pct * self.basket_limit_multiplier
            exposure = sum(float(out.get(ticker, 0.0) or 0.0) for ticker in definition.tickers)
            if exposure <= reduced_limit + 1e-9:
                continue
            scale = reduced_limit / exposure if exposure > 0 else 1.0
            released = 0.0
            for ticker in definition.tickers:
                before = float(out.get(ticker, 0.0) or 0.0)
                if before <= 0:
                    continue
                after = before * scale
                out[ticker] = after
                released += before - after
            out["CASH"] = float(out.get("CASH", 0.0) or 0.0) + released
            violations.append(f"basket_limit:{group_name} {exposure:.2%}->{reduced_limit:.2%}")
        return _normalize_cash_first(out), violations

    def _allocate_turnover_budget(
        self,
        *,
        target: dict[str, float],
        current: dict[str, float],
        signal_strengths: dict[str, float],
        budget: float,
    ) -> dict[str, float]:
        if budget <= 0:
            return dict(current)

        keys = sorted((set(target) | set(current)) - {"CASH"})
        deltas = {
            ticker: float(target.get(ticker, 0.0) or 0.0) - float(current.get(ticker, 0.0) or 0.0)
            for ticker in keys
        }
        priority = sorted(
            keys,
            key=lambda ticker: (
                abs(signal_strengths.get(ticker, 0.0)) * abs(deltas[ticker]),
                abs(deltas[ticker]),
                ticker,
            ),
            reverse=True,
        )

        out = dict(current)
        remaining = budget
        for ticker in priority:
            delta = deltas[ticker]
            if abs(delta) <= 1e-12 or remaining <= 1e-12:
                continue
            allowed_abs_delta = min(abs(delta), remaining)
            out[ticker] = float(current.get(ticker, 0.0) or 0.0) + (1 if delta > 0 else -1) * allowed_abs_delta
            remaining -= allowed_abs_delta
        out["CASH"] = max(1.0 - sum(value for ticker, value in out.items() if ticker != "CASH"), 0.0)
        return _normalize_cash_first(out)

    def _check_violations(self, weights: dict[str, float], active_baskets: set[str]) -> list[str]:
        violations: list[str] = []
        for group_name, definition in sorted(GROUP_DEFINITIONS.items()):
            exposure = _factor_exposure_for_group(weights, group_name)
            if exposure > definition.limit_pct + 1e-6:
                violations.append(f"factor_limit_remaining:{group_name} {exposure:.2%}>{definition.limit_pct:.2%}")
        for group_name in sorted(active_baskets):
            definition = GROUP_DEFINITIONS.get(group_name)
            if not definition:
                continue
            reduced_limit = definition.limit_pct * self.basket_limit_multiplier
            exposure = sum(float(weights.get(ticker, 0.0) or 0.0) for ticker in definition.tickers)
            if exposure > reduced_limit + 1e-6:
                violations.append(f"basket_limit_remaining:{group_name} {exposure:.2%}>{reduced_limit:.2%}")
        return violations


def build_construction_signal_strengths(evidence_bundle: dict | None) -> dict[str, float]:
    """Merge deterministic strategy and rotation signals for construction."""
    bundle = evidence_bundle or {}
    strategy_signals = _strategy_signal_strengths(bundle.get("strategies") or {})
    rotation_signals = _clean_signals((bundle.get("rotation") or {}).get("signals") or {})
    return _merge_signal_strengths(strategy_signals, rotation_signals)


def _active_basket_groups(raw: dict[str, Any] | list[dict[str, Any]] | None) -> set[str]:
    if not raw:
        return set()
    if isinstance(raw, dict):
        return {str(group).strip() for group, value in raw.items() if str(group).strip() and value}
    groups: set[str] = set()
    for row in raw:
        if not isinstance(row, dict):
            continue
        group = str(row.get("group") or "").strip()
        if group:
            groups.add(group)
    return groups


def _clean_weights(raw: dict[str, Any] | None) -> dict[str, float]:
    out: dict[str, float] = {}
    for ticker, value in (raw or {}).items():
        clean = str(ticker or "").upper().strip()
        if not clean:
            continue
        parsed = _optional_float(value)
        out[clean] = max(parsed if parsed is not None else 0.0, 0.0)
    return out


def _clean_signals(raw: dict[str, Any]) -> dict[str, float]:
    out: dict[str, float] = {}
    for ticker, value in raw.items():
        clean = str(ticker or "").upper().strip()
        parsed = _optional_float(value)
        if clean and parsed is not None:
            out[clean] = max(min(parsed, 1.0), -1.0)
    return out


def _strategy_signal_strengths(strategies: dict | None) -> dict[str, float]:
    out: dict[str, float] = {}
    for row in (strategies or {}).get("strategy_results") or []:
        if not isinstance(row, dict):
            continue
        use = str(row.get("suggested_use") or "")
        if use not in {"primary", "advisory"}:
            continue
        confidence = _optional_float(row.get("confidence_score")) or 0.0
        if confidence <= 0:
            continue
        for ticker in row.get("selected_tickers") or []:
            clean = str(ticker or "").upper().strip()
            if not clean or clean == "CASH":
                continue
            out[clean] = max(out.get(clean, 0.0), min(confidence, 1.0))
    return out


def _merge_signal_strengths(
    strategy_signals: dict[str, float],
    rotation_signals: dict[str, float],
    *,
    strategy_weight: float = 0.60,
    rotation_weight: float = 0.40,
) -> dict[str, float]:
    out: dict[str, float] = {}
    for ticker in sorted(set(strategy_signals) | set(rotation_signals)):
        score = (
            strategy_weight * float(strategy_signals.get(ticker, 0.0) or 0.0)
            + rotation_weight * float(rotation_signals.get(ticker, 0.0) or 0.0)
        )
        out[ticker] = round(max(min(score, 1.0), -1.0), 6)
    return out


def _clip_adds_to_current(target: dict[str, float], current: dict[str, float]) -> tuple[dict[str, float], list[str]]:
    out = dict(target)
    violations: list[str] = []
    released = 0.0
    for ticker in sorted((set(out) | set(current)) - {"CASH"}):
        target_w = float(out.get(ticker, 0.0) or 0.0)
        current_w = float(current.get(ticker, 0.0) or 0.0)
        if target_w > current_w + 1e-9:
            out[ticker] = current_w
            released += target_w - current_w
            violations.append(f"scorecard_no_add:{ticker} {target_w:.2%}->{current_w:.2%}")
    out["CASH"] = float(out.get("CASH", 0.0) or 0.0) + released
    return _normalize_cash_first(out), violations


def _factor_exposure_for_group(weights: dict[str, float], group_name: str) -> float:
    return sum(
        float(weights.get(ticker, 0.0) or 0.0)
        for ticker in weights
        if group_name in get_factor_tags(ticker)
    )


def _tickers_with_factor_tag(weights: dict[str, float], group_name: str) -> list[str]:
    return [
        ticker
        for ticker in sorted(weights)
        if ticker != "CASH" and group_name in get_factor_tags(ticker)
    ]


def _normalize_cash_first(weights: dict[str, Any]) -> dict[str, float]:
    clean = _clean_weights(weights)
    equity = sum(value for ticker, value in clean.items() if ticker != "CASH")
    if equity >= 1.0:
        scale = 1.0 / equity if equity > 0 else 0.0
        out = {
            ticker: round(value * scale, 6)
            for ticker, value in clean.items()
            if ticker != "CASH" and value > 1e-9
        }
        out["CASH"] = round(max(1.0 - sum(out.values()), 0.0), 6)
        return out
    out = {
        ticker: round(value, 6)
        for ticker, value in clean.items()
        if ticker != "CASH" and value > 1e-9
    }
    out["CASH"] = round(max(1.0 - sum(out.values()), 0.0), 6)
    return out


def _turnover(target: dict[str, Any], current: dict[str, Any]) -> float:
    keys = set(target) | set(current)
    return sum(
        abs(float(target.get(ticker, 0.0) or 0.0) - float(current.get(ticker, 0.0) or 0.0))
        for ticker in keys
    ) / 2.0


def _effective_n(weights: dict[str, float]) -> float:
    equity_weights = [
        float(value or 0.0)
        for ticker, value in weights.items()
        if ticker != "CASH" and float(value or 0.0) > 0
    ]
    denom = sum(value * value for value in equity_weights)
    return 1.0 / denom if denom > 0 else 0.0


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
