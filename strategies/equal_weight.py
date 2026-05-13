"""
EqualWeightBenchmark v1.0

Simple benchmark strategy: equal-weight the ETF universe with cash floor and
single-position cap. Useful as a baseline in Playground comparisons.
"""
from __future__ import annotations

from typing import Any

from strategies.base import ScoredTicker, Strategy


class EqualWeightBenchmark(Strategy):
    name = "equal_weight_benchmark"
    version = "1.0"
    description = "Equal-weight benchmark across the ETF universe"
    required_fields: tuple[str, ...] = ()
    optional_fields = ("daily_return_pct",)

    DEFAULT_PARAMS: dict[str, Any] = {
        "max_holdings": 12,
    }

    def __init__(self, params: dict[str, Any] | None = None):
        super().__init__({**self.DEFAULT_PARAMS, **(params or {})})

    def score(self, holdings: list[dict], context: dict[str, Any]) -> list[ScoredTicker]:
        valid = [
            h for h in holdings
            if h.get("ticker") and str(h.get("ticker")).upper() != "CASH"
        ]
        return [
            ScoredTicker(
                ticker=h["ticker"],
                score=1.0,
                factor_breakdown={"equal_weight": 1.0},
                raw_factors={},
            )
            for h in valid
        ]

    def optimize(self, scored: list[ScoredTicker], context: dict[str, Any]) -> dict[str, float]:
        if not scored:
            return {"CASH": 1.0}
        risk = context.get("risk_params", {})
        max_pos = float(risk.get("max_single_position", 0.20))
        min_cash = float(risk.get("min_cash_pct", 0.05))
        n = max(1, min(int(self.params["max_holdings"]), len(scored)))
        selected = scored[:n]
        raw_weight = (1.0 - min_cash) / n
        weight = min(raw_weight, max_pos)
        out = {item.ticker: round(weight, 4) for item in selected}
        out["CASH"] = round(max(1.0 - sum(out.values()), 0.0), 4)
        return out
