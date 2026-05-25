"""VolatilityHedgeLite.

Short-horizon volatility hedge signal for UVXY/VIXY plus defensive fallback
assets. This is intentionally capped and should only become actionable through
existing certification and risk gates.
"""
from __future__ import annotations

import logging
import statistics
from typing import Any

from strategies.base import ScoredTicker, Strategy

logger = logging.getLogger("qc_fastapi_2.strategy.volatility_hedge_lite")


class VolatilityHedgeLite(Strategy):
    name = "volatility_hedge_lite"
    version = "1.0"
    description = "Tail-risk hedge strategy using volatility ETPs and defensive Treasury fallbacks"
    required_fields = ("hist_vol_20d", "atr_pct", "mom_20d", "mom_60d", "rsi_14")
    optional_fields = ("return_5d", "sma_200", "close_price")
    family = "volatility_hedge"
    core_idea = "Scores volatility hedges only when market stress is elevated, while favoring cash-like fallback assets when stress is not acute."
    best_regimes = ("high_vol", "defensive", "acute_risk_off", "volatility_spike")
    bad_regimes = ("trending_bull", "calm_market", "high_vol_chop_after_spike")
    signals_used = ("market_stress", "vol_etp_momentum", "hist_vol_20d", "atr_pct", "rsi_14")
    failure_modes = (
        "Volatility ETPs decay quickly in contango and should not be treated as ordinary longs.",
        "Late hedge entries after a volatility spike can lose even if market risk remains high.",
        "False positives are common in choppy high-volatility markets.",
    )
    agent_guidance = "Treat UVXY/VIXY output as very short-term hedge evidence only. Never use it to justify ordinary risk-on exposure."
    universe_tickers = ("UVXY", "VIXY", "SGOV", "TLT")
    allow_hedge_research_tickers = True

    DEFAULT_PARAMS: dict[str, Any] = {
        "w_market_stress": 0.45,
        "w_vol_momentum": 0.25,
        "w_low_vol_fallback": 0.15,
        "w_low_atr_fallback": 0.15,
        "zscore_clip": 3.0,
        "max_vol_hedge_weight": 0.03,
        "max_defensive_fallback_weight": 0.05,
        "max_total_weight": 0.11,
        "stress_threshold": 0.35,
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
            logger.warning("volatility_hedge_lite: no tickers with complete factor data")
            return []

        p = self.params
        clip = float(p["zscore_clip"])
        z_mom20 = _zscore([float(h["mom_20d"]) for h in valid], clip)
        z_mom60 = _zscore([float(h["mom_60d"]) for h in valid], clip)
        z_low_vol = _zscore([-float(h["hist_vol_20d"]) for h in valid], clip)
        z_low_atr = _zscore([-float(h["atr_pct"]) for h in valid], clip)
        market_stress = _market_stress_score(holdings, context)

        scored: list[ScoredTicker] = []
        for i, row in enumerate(valid):
            ticker = str(row["ticker"]).upper().strip()
            is_vol_etp = ticker in {"UVXY", "VIXY"}
            vol_signal = 0.50 * z_mom20[i] + 0.50 * z_mom60[i]
            fallback_signal = 0.50 * z_low_vol[i] + 0.50 * z_low_atr[i]
            if is_vol_etp:
                decay_penalty = 0.20 if ticker == "UVXY" else 0.12
                score = (
                    float(p["w_market_stress"]) * market_stress
                    + float(p["w_vol_momentum"]) * vol_signal
                    - decay_penalty
                )
                branch = "acute_vol_hedge" if market_stress >= float(p["stress_threshold"]) else "no_acute_vol_hedge"
            else:
                score = (
                    0.35 * market_stress
                    + float(p["w_low_vol_fallback"]) * fallback_signal
                    + float(p["w_low_atr_fallback"]) * z_low_atr[i]
                )
                branch = "defensive_fallback_hedge"
            scored.append(ScoredTicker(
                ticker=ticker,
                score=score,
                factor_breakdown={
                    "market_stress": round(market_stress, 4),
                    "z_mom_20d": round(z_mom20[i], 4),
                    "z_mom_60d": round(z_mom60[i], 4),
                    "z_low_vol": round(z_low_vol[i], 4),
                    "z_low_atr": round(z_low_atr[i], 4),
                },
                raw_factors={
                    "hist_vol_20d": float(row["hist_vol_20d"]),
                    "atr_pct": float(row["atr_pct"]),
                    "mom_20d": float(row["mom_20d"]),
                    "mom_60d": float(row["mom_60d"]),
                    "rsi_14": float(row["rsi_14"]),
                    "market_stress": market_stress,
                    "branch": branch,
                },
            ))

        scored.sort(key=lambda item: item.score, reverse=True)
        return scored

    def optimize(self, scored: list[ScoredTicker], context: dict[str, Any]) -> dict[str, float]:
        if not scored:
            return {"CASH": 1.0}
        p = self.params
        stress = max(float(item.raw_factors.get("market_stress") or 0.0) for item in scored)
        if stress < float(p["stress_threshold"]):
            return {"CASH": 1.0}

        max_total = float(p["max_total_weight"])
        weights: dict[str, float] = {}
        for item in scored:
            if item.score <= 0:
                continue
            ticker = item.ticker
            cap = float(p["max_vol_hedge_weight"]) if ticker in {"UVXY", "VIXY"} else float(p["max_defensive_fallback_weight"])
            weights[ticker] = min(cap, max(item.score, 0.0) * cap)
            if len(weights) >= 3:
                break
        total = sum(weights.values())
        if total > max_total and total > 0:
            weights = {ticker: weight * max_total / total for ticker, weight in weights.items()}
        out = {ticker: round(weight, 4) for ticker, weight in weights.items() if weight > 0}
        out["CASH"] = round(max(1.0 - sum(out.values()), 0.0), 4)
        return out


def _market_stress_score(holdings: list[dict], context: dict[str, Any]) -> float:
    regime = str(context.get("regime") or "")
    score = {
        "high_vol": 0.75,
        "defensive": 0.65,
        "risk_off": 0.70,
        "cash_only": 0.60,
        "trending_bull": 0.10,
        "risk_on": 0.10,
    }.get(regime, 0.25)
    by_ticker = {
        str(row.get("ticker") or "").upper().strip(): row
        for row in holdings
        if row.get("ticker")
    }
    spy = by_ticker.get("SPY") or {}
    if _to_float(spy.get("mom_20d")) is not None and float(spy.get("mom_20d")) < -0.03:
        score += 0.15
    if _to_float(spy.get("mom_60d")) is not None and float(spy.get("mom_60d")) < -0.06:
        score += 0.15
    close = _to_float(spy.get("close_price") or spy.get("price"))
    sma200 = _to_float(spy.get("sma_200"))
    if close is not None and sma200 is not None and close < sma200:
        score += 0.15
    if _to_float(spy.get("rsi_14")) is not None and float(spy.get("rsi_14")) < 35:
        score += 0.10
    return max(0.0, min(1.0, score))


def _zscore(values: list[float], clip: float) -> list[float]:
    if len(values) < 2:
        return [0.0] * len(values)
    mean = statistics.fmean(values)
    std = statistics.stdev(values)
    if std == 0:
        return [0.0] * len(values)
    return [max(-clip, min(clip, (value - mean) / std)) for value in values]


def _to_float(value: Any) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None
