"""
RiskParityLite v1.0

Inverse-volatility allocation across ETFs. This is a mainstream allocation
benchmark for comparing signal-driven strategies against a volatility-budgeted
portfolio.
"""
from __future__ import annotations

from typing import Any

from strategies.base import ScoredTicker, Strategy


class RiskParityLite(Strategy):
    name = "risk_parity_lite"
    version = "1.0"
    description = "Inverse-volatility risk parity allocation"
    required_fields = ("hist_vol_20d",)
    optional_fields = ("atr_pct", "daily_return_pct")
    family = "risk_budgeting"
    core_idea = "Allocates more weight to lower-volatility ETFs to create a simple volatility-budgeted benchmark."
    best_regimes = ("uncertain", "mixed_rotation", "risk_management_focus")
    bad_regimes = ("strong_single-theme_trend", "volatility_regime_break")
    signals_used = ("hist_vol_20d",)
    failure_modes = (
        "Ignores expected return and may overweight low-volatility laggards.",
        "Backward-looking volatility can underestimate newly emerging risks.",
        "Can look stable while missing the actual market leadership theme.",
    )
    agent_guidance = "Use as a neutral risk-budget benchmark, not as a directional alpha signal."

    DEFAULT_PARAMS: dict[str, Any] = {
        "max_holdings": 10,
        "min_vol_floor": 0.05,
    }

    def __init__(self, params: dict[str, Any] | None = None):
        super().__init__({**self.DEFAULT_PARAMS, **(params or {})})

    def score(self, holdings: list[dict], context: dict[str, Any]) -> list[ScoredTicker]:
        valid = [
            h for h in holdings
            if h.get("ticker") and h.get("hist_vol_20d") is not None
        ]
        scored: list[ScoredTicker] = []
        for h in valid:
            vol = max(float(h.get("hist_vol_20d") or 0.15), float(self.params["min_vol_floor"]))
            score = 1.0 / vol
            scored.append(ScoredTicker(
                ticker=h["ticker"],
                score=score,
                factor_breakdown={"inverse_hist_vol": round(score, 4)},
                raw_factors={"hist_vol_20d": float(h.get("hist_vol_20d") or 0.0)},
            ))
        scored.sort(key=lambda item: item.score, reverse=True)
        return scored

    def optimize(self, scored: list[ScoredTicker], context: dict[str, Any]) -> dict[str, float]:
        if not scored:
            return {"CASH": 1.0}
        risk = context.get("risk_params", {})
        max_pos = float(risk.get("max_single_position", 0.20))
        min_cash = float(risk.get("min_cash_pct", 0.05))
        selected = scored[:max(1, min(int(self.params["max_holdings"]), len(scored)))]
        total_score = sum(item.score for item in selected)
        raw = {item.ticker: item.score / total_score for item in selected}
        capped = {ticker: min(weight, max_pos) for ticker, weight in raw.items()}
        total = sum(capped.values())
        equity_budget = min(total, 1.0 - min_cash)
        scale = equity_budget / total if total > 0 else 1.0
        out = {ticker: round(weight * scale, 4) for ticker, weight in capped.items()}
        out["CASH"] = round(max(1.0 - sum(out.values()), 0.0), 4)
        return out
