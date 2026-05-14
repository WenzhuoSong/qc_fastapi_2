"""
LowVolFactor v1.0

Lightweight sandbox strategy for the Playground. It prefers low realized
volatility, low ATR, and stable positive long-term momentum.
"""
from __future__ import annotations

import logging
import statistics
from typing import Any

from strategies.base import ScoredTicker, Strategy

logger = logging.getLogger("qc_fastapi_2.strategy.low_vol_factor")


class LowVolFactor(Strategy):
    name = "low_vol_factor"
    version = "1.0"
    description = "Low-volatility factor strategy with long-momentum confirmation"
    required_fields = ("hist_vol_20d", "atr_pct", "mom_252d")
    optional_fields = ("beta_vs_spy", "unrealized_pnl_pct", "daily_return_pct")
    family = "defensive_factor"
    core_idea = "Prefers ETFs with lower realized volatility, lower ATR, and positive long-term momentum as a defensive quality tilt."
    best_regimes = ("defensive", "high_vol", "late_cycle", "risk_off_rotation")
    bad_regimes = ("strong_risk_on", "speculative_melt_up", "early_cycle_reacceleration")
    signals_used = ("hist_vol_20d", "atr_pct", "mom_252d", "beta_vs_spy", "unrealized_pnl_pct")
    failure_modes = (
        "Can underperform when high-beta sectors lead a broad risk-on rally.",
        "Low volatility can be backward-looking and slow to adapt after regime shifts.",
        "May crowd into defensive assets just as risk appetite recovers.",
    )
    agent_guidance = "Use as a capital-preservation anchor; do not overrule strong breadth and rotation evidence with this strategy alone."

    DEFAULT_PARAMS: dict[str, Any] = {
        "w_hist_vol": 0.40,
        "w_atr": 0.25,
        "w_beta": 0.15,
        "w_mom_252d": 0.15,
        "w_drawdown_proxy": 0.05,
        "zscore_clip": 3.0,
        "max_holdings": 8,
    }

    def __init__(self, params: dict[str, Any] | None = None):
        super().__init__({**self.DEFAULT_PARAMS, **(params or {})})

    def score(self, holdings: list[dict], context: dict[str, Any]) -> list[ScoredTicker]:
        required = ("hist_vol_20d", "atr_pct", "mom_252d")
        valid = [
            h for h in holdings
            if h.get("ticker") and all(h.get(field) is not None for field in required)
        ]
        if not valid:
            logger.warning("low_vol_factor: no tickers with complete factor data")
            return []

        p = self.params
        clip = float(p["zscore_clip"])
        z_hist_vol = _zscore([-float(h["hist_vol_20d"]) for h in valid], clip)
        z_atr = _zscore([-float(h["atr_pct"]) for h in valid], clip)
        z_beta = _zscore([-float(h.get("beta_vs_spy") or 1.0) for h in valid], clip)
        z_mom252 = _zscore([float(h["mom_252d"]) for h in valid], clip)
        z_pnl = _zscore([float(h.get("unrealized_pnl_pct") or 0.0) for h in valid], clip)

        scored: list[ScoredTicker] = []
        for i, h in enumerate(valid):
            score = (
                float(p["w_hist_vol"]) * z_hist_vol[i]
                + float(p["w_atr"]) * z_atr[i]
                + float(p["w_beta"]) * z_beta[i]
                + float(p["w_mom_252d"]) * z_mom252[i]
                + float(p["w_drawdown_proxy"]) * z_pnl[i]
            )
            scored.append(ScoredTicker(
                ticker=h["ticker"],
                score=score,
                factor_breakdown={
                    "z_hist_vol": round(z_hist_vol[i], 4),
                    "z_atr": round(z_atr[i], 4),
                    "z_beta": round(z_beta[i], 4),
                    "z_mom_252d": round(z_mom252[i], 4),
                    "z_unrealized_pnl": round(z_pnl[i], 4),
                },
                raw_factors={
                    "hist_vol_20d": float(h["hist_vol_20d"]),
                    "atr_pct": float(h["atr_pct"]),
                    "beta_vs_spy": float(h.get("beta_vs_spy") or 1.0),
                    "mom_252d": float(h["mom_252d"]),
                    "unrealized_pnl_pct": float(h.get("unrealized_pnl_pct") or 0.0),
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
        n = max(3, min(int(self.params["max_holdings"]), len(scored)))
        selected = [s for s in scored[:n] if s.score > 0]
        if not selected:
            return {"CASH": 1.0}

        inv_vol = {
            s.ticker: 1.0 / max(float(s.raw_factors.get("hist_vol_20d") or 0.15), 0.05)
            for s in selected
        }
        total = sum(inv_vol.values())
        raw = {ticker: value / total for ticker, value in inv_vol.items()}
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
