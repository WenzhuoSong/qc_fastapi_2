"""
MeanReversionLite v1.0

Lightweight sandbox strategy for the Playground. It favors oversold,
lower-Bollinger-band names with acceptable volatility. This is research-only
unless explicitly wired into the main strategy registry by config.
"""
from __future__ import annotations

import logging
import statistics
from typing import Any

from strategies.base import ScoredTicker, Strategy

logger = logging.getLogger("qc_fastapi_2.strategy.mean_reversion_lite")


class MeanReversionLite(Strategy):
    name = "mean_reversion_lite"
    version = "1.0"
    description = "RSI/Bollinger mean-reversion strategy with volatility penalty"
    required_fields = ("rsi_14", "bb_position", "hist_vol_20d", "mom_20d")
    optional_fields = ("daily_return_pct",)

    DEFAULT_PARAMS: dict[str, Any] = {
        "w_rsi_oversold": 0.45,
        "w_bb_lower": 0.35,
        "w_vol": 0.15,
        "w_mom_20d_reversal": 0.05,
        "zscore_clip": 3.0,
        "max_holdings": 6,
        "vol_blend_alpha": 0.80,
    }

    def __init__(self, params: dict[str, Any] | None = None):
        super().__init__({**self.DEFAULT_PARAMS, **(params or {})})

    def score(self, holdings: list[dict], context: dict[str, Any]) -> list[ScoredTicker]:
        required = ("rsi_14", "bb_position", "hist_vol_20d", "mom_20d")
        valid = [
            h for h in holdings
            if h.get("ticker") and all(h.get(field) is not None for field in required)
        ]
        if not valid:
            logger.warning("mean_reversion_lite: no tickers with complete factor data")
            return []

        p = self.params
        clip = float(p["zscore_clip"])
        z_rsi_oversold = _zscore([100.0 - float(h["rsi_14"]) for h in valid], clip)
        z_bb_lower = _zscore([1.0 - float(h["bb_position"]) for h in valid], clip)
        z_low_vol = _zscore([-float(h["hist_vol_20d"]) for h in valid], clip)
        z_mom_reversal = _zscore([-float(h["mom_20d"]) for h in valid], clip)

        scored: list[ScoredTicker] = []
        for i, h in enumerate(valid):
            score = (
                float(p["w_rsi_oversold"]) * z_rsi_oversold[i]
                + float(p["w_bb_lower"]) * z_bb_lower[i]
                + float(p["w_vol"]) * z_low_vol[i]
                + float(p["w_mom_20d_reversal"]) * z_mom_reversal[i]
            )
            scored.append(ScoredTicker(
                ticker=h["ticker"],
                score=score,
                factor_breakdown={
                    "z_rsi_oversold": round(z_rsi_oversold[i], 4),
                    "z_bb_lower": round(z_bb_lower[i], 4),
                    "z_low_vol": round(z_low_vol[i], 4),
                    "z_mom_20d_reversal": round(z_mom_reversal[i], 4),
                },
                raw_factors={
                    "rsi_14": float(h["rsi_14"]),
                    "bb_position": float(h["bb_position"]),
                    "hist_vol_20d": float(h["hist_vol_20d"]),
                    "mom_20d": float(h["mom_20d"]),
                },
            ))

        scored.sort(key=lambda item: item.score, reverse=True)
        return scored

    def optimize(self, scored: list[ScoredTicker], context: dict[str, Any]) -> dict[str, float]:
        if not scored:
            return {"CASH": 1.0}

        p = self.params
        risk = context.get("risk_params", {})
        max_pos = float(risk.get("max_single_position", 0.20))
        min_cash = float(risk.get("min_cash_pct", 0.05))
        n = max(3, min(int(p["max_holdings"]), len(scored)))
        selected = [s for s in scored[:n] if s.score > 0]
        if not selected:
            return {"CASH": 1.0}

        min_score = min(s.score for s in selected)
        shifted = {s.ticker: s.score - min_score + 0.1 for s in selected}
        total_shifted = sum(shifted.values())
        score_weights = {ticker: value / total_shifted for ticker, value in shifted.items()}

        inv_vol = {
            s.ticker: 1.0 / max(float(s.raw_factors.get("hist_vol_20d") or 0.15), 0.05)
            for s in selected
        }
        total_inv = sum(inv_vol.values())
        vol_weights = {ticker: value / total_inv for ticker, value in inv_vol.items()}

        alpha = float(p["vol_blend_alpha"])
        mixed = {
            ticker: alpha * score_weights[ticker] + (1.0 - alpha) * vol_weights[ticker]
            for ticker in score_weights
        }
        return _cap_and_cash(mixed, max_pos=max_pos, min_cash=min_cash)


def _zscore(values: list[float], clip: float) -> list[float]:
    if len(values) < 2:
        return [0.0] * len(values)
    mean = statistics.fmean(values)
    std = statistics.stdev(values)
    if std == 0:
        return [0.0] * len(values)
    return [max(-clip, min(clip, (value - mean) / std)) for value in values]


def _cap_and_cash(weights: dict[str, float], max_pos: float, min_cash: float) -> dict[str, float]:
    capped = {ticker: min(weight, max_pos) for ticker, weight in weights.items()}
    total = sum(capped.values())
    if total <= 0:
        return {"CASH": 1.0}
    equity_budget = min(total, 1.0 - min_cash)
    scale = equity_budget / total
    out = {ticker: round(weight * scale, 4) for ticker, weight in capped.items()}
    out["CASH"] = round(max(1.0 - sum(out.values()), 0.0), 4)
    return out
