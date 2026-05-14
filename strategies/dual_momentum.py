"""
DualMomentumRotation v1.0

Mainstream ETF rotation strategy: rank by absolute/relative momentum and hold
the strongest names only when momentum is positive. Otherwise leave capital in
CASH. This is intentionally simple and deterministic for Playground research.
"""
from __future__ import annotations

import statistics
from typing import Any

from strategies.base import ScoredTicker, Strategy


class DualMomentumRotation(Strategy):
    name = "dual_momentum_rotation"
    version = "1.0"
    description = "Relative and absolute momentum ETF rotation"
    required_fields = ("mom_60d", "mom_252d", "hist_vol_20d")
    optional_fields = ("mom_20d", "daily_return_pct")
    family = "dual_momentum"
    core_idea = "Ranks ETFs by relative momentum but only holds names with positive absolute momentum; otherwise leaves capital in cash."
    best_regimes = ("trending_bull", "sector_rotation", "persistent_relative_strength")
    bad_regimes = ("mean_reverting", "violent_whipsaw", "sideways_chop")
    signals_used = ("mom_60d", "mom_252d", "hist_vol_20d")
    failure_modes = (
        "Can whipsaw when leadership changes rapidly.",
        "May move to cash after losses because absolute momentum is lagging.",
        "Can miss early reversals before medium and long momentum confirm.",
    )
    agent_guidance = "Use as a clean leadership detector; confirm that selected ETFs also fit current macro, news, and rotation evidence."

    DEFAULT_PARAMS: dict[str, Any] = {
        "w_mom_60d": 0.35,
        "w_mom_252d": 0.55,
        "w_low_vol": 0.10,
        "zscore_clip": 3.0,
        "max_holdings": 5,
        "absolute_momentum_floor": 0.0,
    }

    def __init__(self, params: dict[str, Any] | None = None):
        super().__init__({**self.DEFAULT_PARAMS, **(params or {})})

    def score(self, holdings: list[dict], context: dict[str, Any]) -> list[ScoredTicker]:
        valid = [
            h for h in holdings
            if h.get("ticker")
            and h.get("mom_60d") is not None
            and h.get("mom_252d") is not None
            and h.get("hist_vol_20d") is not None
        ]
        if not valid:
            return []

        p = self.params
        clip = float(p["zscore_clip"])
        z_mom60 = _zscore([float(h["mom_60d"]) for h in valid], clip)
        z_mom252 = _zscore([float(h["mom_252d"]) for h in valid], clip)
        z_low_vol = _zscore([-float(h["hist_vol_20d"]) for h in valid], clip)

        scored: list[ScoredTicker] = []
        for i, h in enumerate(valid):
            score = (
                float(p["w_mom_60d"]) * z_mom60[i]
                + float(p["w_mom_252d"]) * z_mom252[i]
                + float(p["w_low_vol"]) * z_low_vol[i]
            )
            scored.append(ScoredTicker(
                ticker=h["ticker"],
                score=score,
                factor_breakdown={
                    "z_mom_60d": round(z_mom60[i], 4),
                    "z_mom_252d": round(z_mom252[i], 4),
                    "z_low_vol": round(z_low_vol[i], 4),
                },
                raw_factors={
                    "mom_60d": float(h["mom_60d"]),
                    "mom_252d": float(h["mom_252d"]),
                    "hist_vol_20d": float(h["hist_vol_20d"]),
                },
            ))
        scored.sort(key=lambda item: item.score, reverse=True)
        return scored

    def optimize(self, scored: list[ScoredTicker], context: dict[str, Any]) -> dict[str, float]:
        if not scored:
            return {"CASH": 1.0}
        risk = context.get("risk_params", {})
        max_pos = float(risk.get("max_single_position", 0.20))
        min_cash = float(risk.get("min_cash_pct", 0.05))
        floor = float(self.params["absolute_momentum_floor"])
        selected = [
            item for item in scored
            if float(item.raw_factors.get("mom_252d") or 0.0) > floor
        ][:max(1, int(self.params["max_holdings"]))]
        if not selected:
            return {"CASH": 1.0}

        min_score = min(item.score for item in selected)
        shifted = {item.ticker: item.score - min_score + 0.1 for item in selected}
        total = sum(shifted.values())
        raw = {ticker: value / total for ticker, value in shifted.items()}
        capped = {ticker: min(weight, max_pos) for ticker, weight in raw.items()}
        capped_total = sum(capped.values())
        equity_budget = min(capped_total, 1.0 - min_cash)
        scale = equity_budget / capped_total if capped_total > 0 else 1.0
        out = {ticker: round(weight * scale, 4) for ticker, weight in capped.items()}
        out["CASH"] = round(max(1.0 - sum(out.values()), 0.0), 4)
        return out


def _zscore(values: list[float], clip: float) -> list[float]:
    if len(values) < 2:
        return [0.0] * len(values)
    mean = statistics.fmean(values)
    std = statistics.stdev(values)
    if std == 0:
        return [0.0] * len(values)
    return [max(-clip, min(clip, (value - mean) / std)) for value in values]
