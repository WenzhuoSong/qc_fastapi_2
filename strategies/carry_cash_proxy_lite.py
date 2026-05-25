"""CarryCashProxyLite.

Defensive carry/cash-proxy allocator for short-duration and Treasury ETFs.
It is a real Strategy implementation for Playground/watch-only validation; all
execution authority still comes from certification, policy, and risk gates.
"""
from __future__ import annotations

import logging
import statistics
from typing import Any

from strategies.base import ScoredTicker, Strategy

logger = logging.getLogger("qc_fastapi_2.strategy.carry_cash_proxy_lite")


class CarryCashProxyLite(Strategy):
    name = "carry_cash_proxy_lite"
    version = "1.0"
    description = "Carry/cash-proxy defensive allocator using low-volatility Treasury and bond ETFs"
    required_fields = ("hist_vol_20d", "atr_pct", "mom_20d", "mom_60d")
    optional_fields = ("return_5d", "rsi_14")
    family = "carry_or_cash_proxy"
    core_idea = "Ranks cash-like and Treasury/bond ETFs by low volatility, low ATR, and positive short/intermediate trend as a defensive carry proxy."
    best_regimes = ("defensive", "high_vol", "risk_off", "cash_only")
    bad_regimes = ("trending_bull", "strong_risk_on", "inflation_shock")
    signals_used = ("hist_vol_20d", "atr_pct", "mom_20d", "mom_60d")
    failure_modes = (
        "Can lag equities badly in persistent risk-on rallies.",
        "Long-duration bond proxies can lose money during rising-rate shocks.",
        "Cash-like carry is not true alpha; it is defensive risk compensation.",
    )
    agent_guidance = "Use as a defensive capital-preservation sleeve, not as proof of alpha. Prefer it when risk gates favor de-risking or cash-like exposure."
    universe_tickers = ("SGOV", "BND", "IEF", "TLT")

    DEFAULT_PARAMS: dict[str, Any] = {
        "w_low_vol": 0.35,
        "w_low_atr": 0.25,
        "w_mom_60d": 0.25,
        "w_mom_20d": 0.15,
        "zscore_clip": 3.0,
        "max_holdings": 3,
        "max_single_weight": 0.05,
        "max_total_weight": 0.20,
        "min_cash_pct": 0.80,
    }

    def __init__(self, params: dict[str, Any] | None = None):
        super().__init__({**self.DEFAULT_PARAMS, **(params or {})})

    def score(self, holdings: list[dict], context: dict[str, Any]) -> list[ScoredTicker]:
        valid = [
            h for h in self.eligible_rows(holdings)
            if h.get("ticker")
            and all(h.get(field) is not None for field in self.required_fields)
        ]
        if not valid:
            logger.warning("carry_cash_proxy_lite: no tickers with complete factor data")
            return []

        p = self.params
        clip = float(p["zscore_clip"])
        z_low_vol = _zscore([-float(h["hist_vol_20d"]) for h in valid], clip)
        z_low_atr = _zscore([-float(h["atr_pct"]) for h in valid], clip)
        z_mom60 = _zscore([float(h["mom_60d"]) for h in valid], clip)
        z_mom20 = _zscore([float(h["mom_20d"]) for h in valid], clip)

        regime = str(context.get("regime") or "")
        defensive_boost = 0.15 if regime in {"defensive", "high_vol", "risk_off", "cash_only"} else 0.0
        risk_on_penalty = -0.20 if regime in {"trending_bull", "risk_on"} else 0.0

        scored: list[ScoredTicker] = []
        for i, row in enumerate(valid):
            ticker = str(row["ticker"]).upper().strip()
            duration_penalty = -0.10 if ticker == "TLT" and regime in {"inflation_shock", "risk_on"} else 0.0
            cash_like_boost = 0.10 if ticker == "SGOV" and regime in {"defensive", "high_vol", "cash_only"} else 0.0
            score = (
                float(p["w_low_vol"]) * z_low_vol[i]
                + float(p["w_low_atr"]) * z_low_atr[i]
                + float(p["w_mom_60d"]) * z_mom60[i]
                + float(p["w_mom_20d"]) * z_mom20[i]
                + defensive_boost
                + risk_on_penalty
                + duration_penalty
                + cash_like_boost
            )
            scored.append(ScoredTicker(
                ticker=ticker,
                score=score,
                factor_breakdown={
                    "z_low_vol": round(z_low_vol[i], 4),
                    "z_low_atr": round(z_low_atr[i], 4),
                    "z_mom_60d": round(z_mom60[i], 4),
                    "z_mom_20d": round(z_mom20[i], 4),
                    "regime_adjustment": round(defensive_boost + risk_on_penalty + duration_penalty + cash_like_boost, 4),
                },
                raw_factors={
                    "hist_vol_20d": float(row["hist_vol_20d"]),
                    "atr_pct": float(row["atr_pct"]),
                    "mom_20d": float(row["mom_20d"]),
                    "mom_60d": float(row["mom_60d"]),
                    "branch": f"{regime or 'unknown'}_carry_cash_proxy",
                },
            ))

        scored.sort(key=lambda item: item.score, reverse=True)
        return scored

    def optimize(self, scored: list[ScoredTicker], context: dict[str, Any]) -> dict[str, float]:
        if not scored:
            return {"CASH": 1.0}
        p = self.params
        selected = [item for item in scored if item.score > 0][: int(p["max_holdings"])]
        if not selected:
            return {"CASH": 1.0}

        shifted = {item.ticker: max(item.score, 0.0) + 0.05 for item in selected}
        total_score = sum(shifted.values())
        max_single = min(float(p["max_single_weight"]), float((context.get("risk_params") or {}).get("max_single_position", p["max_single_weight"])))
        max_total = float(p["max_total_weight"])
        raw = {ticker: value / total_score * max_total for ticker, value in shifted.items()}
        capped = {ticker: min(weight, max_single) for ticker, weight in raw.items()}
        total = sum(capped.values())
        if total > max_total and total > 0:
            capped = {ticker: weight * max_total / total for ticker, weight in capped.items()}
        out = {ticker: round(weight, 4) for ticker, weight in capped.items() if weight > 0}
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
