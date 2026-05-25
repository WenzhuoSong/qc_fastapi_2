"""InverseEquityHedgeLite.

Hedge-only scoring for inverse equity ETFs. This strategy never treats inverse
ETFs as ordinary long alpha positions; it only produces tiny short-horizon
hedge sleeves when equity breakdown evidence is clear.
"""
from __future__ import annotations

import logging
import statistics
from typing import Any

from strategies.base import ScoredTicker, Strategy

logger = logging.getLogger("qc_fastapi_2.strategy.inverse_equity_hedge_lite")


class InverseEquityHedgeLite(Strategy):
    name = "inverse_equity_hedge_lite"
    version = "1.0"
    description = "Hedge-only inverse equity ETF signal with strict short-horizon caps"
    required_fields = ("hist_vol_20d", "atr_pct", "mom_20d", "mom_60d", "rsi_14")
    optional_fields = ("return_5d", "underlying", "underlying_mom_20d", "underlying_mom_60d")
    family = "event_risk_avoidance"
    core_idea = "Uses inverse equity ETFs only as very small tactical hedge candidates when underlying equity trend, market stress, and inverse ETF momentum align."
    best_regimes = ("high_vol", "defensive", "risk_off", "crash_breakdown")
    bad_regimes = ("trending_bull", "risk_on", "sideways_chop", "high_vol_chop")
    signals_used = ("market_stress", "underlying_breakdown", "inverse_momentum", "hist_vol_20d", "atr_pct")
    failure_modes = (
        "Inverse leveraged ETFs decay rapidly because of daily reset compounding.",
        "Whipsaw losses can be large in choppy markets.",
        "Late entries after a selloff can lose even if broad risk remains high.",
    )
    agent_guidance = "Hedge-only. Do not use this strategy to justify ordinary long exposure or persistent inverse ETF holdings."
    universe_tickers = ("SQQQ", "SPXS", "SOXS", "TECS")
    allow_hedge_research_tickers = True

    DEFAULT_PARAMS: dict[str, Any] = {
        "w_market_stress": 0.40,
        "w_underlying_breakdown": 0.30,
        "w_inverse_momentum": 0.20,
        "w_low_atr": 0.10,
        "zscore_clip": 3.0,
        "stress_threshold": 0.55,
        "max_single_weight": 0.02,
        "max_total_weight": 0.05,
        "min_cash_pct": 0.95,
    }

    def __init__(self, params: dict[str, Any] | None = None):
        super().__init__({**self.DEFAULT_PARAMS, **(params or {})})

    def score(self, holdings: list[dict], context: dict[str, Any]) -> list[ScoredTicker]:
        valid = [
            row for row in self.eligible_rows(holdings)
            if row.get("ticker")
            and all(row.get(field) is not None for field in self.required_fields)
        ]
        if not valid:
            logger.warning("inverse_equity_hedge_lite: no inverse ETFs with complete factor data")
            return []

        p = self.params
        clip = float(p["zscore_clip"])
        z_mom20 = _zscore([float(row["mom_20d"]) for row in valid], clip)
        z_mom60 = _zscore([float(row["mom_60d"]) for row in valid], clip)
        z_low_atr = _zscore([-float(row["atr_pct"]) for row in valid], clip)
        market_stress = _market_stress_score(holdings, context)
        regime = str(context.get("regime") or "")

        scored: list[ScoredTicker] = []
        for idx, row in enumerate(valid):
            ticker = str(row["ticker"]).upper().strip()
            underlying = UNDERLYING_BY_INVERSE.get(ticker, "")
            breakdown = _underlying_breakdown_score(underlying, holdings, context)
            inverse_momentum = _clamp(0.50 + 0.25 * z_mom20[idx] + 0.25 * z_mom60[idx])
            chop_penalty = -0.25 if regime in {"trending_bull", "risk_on", "mean_reverting", "high_vol_chop"} else 0.0
            decay_penalty = 0.18 if ticker in {"SQQQ", "SPXS"} else 0.22
            score = _clamp(
                float(p["w_market_stress"]) * market_stress
                + float(p["w_underlying_breakdown"]) * breakdown
                + float(p["w_inverse_momentum"]) * inverse_momentum
                + float(p["w_low_atr"]) * _clamp(0.50 + 0.25 * z_low_atr[idx])
                + chop_penalty
                - decay_penalty
            )
            branch = "inverse_equity_hedge" if score >= float(p["stress_threshold"]) else "inverse_equity_watch_only"
            scored.append(ScoredTicker(
                ticker=ticker,
                score=score,
                factor_breakdown={
                    "market_stress": round(market_stress, 4),
                    "underlying_breakdown": round(breakdown, 4),
                    "inverse_momentum": round(inverse_momentum, 4),
                    "z_low_atr": round(z_low_atr[idx], 4),
                    "decay_penalty": round(-decay_penalty, 4),
                    "regime_adjustment": round(chop_penalty, 4),
                },
                raw_factors={
                    "hist_vol_20d": float(row["hist_vol_20d"]),
                    "atr_pct": float(row["atr_pct"]),
                    "mom_20d": float(row["mom_20d"]),
                    "mom_60d": float(row["mom_60d"]),
                    "rsi_14": float(row["rsi_14"]),
                    "market_stress": market_stress,
                    "underlying": underlying,
                    "underlying_breakdown": breakdown,
                    "branch": f"{branch}_{regime or 'unknown'}",
                },
            ))

        scored.sort(key=lambda item: item.score, reverse=True)
        return scored

    def optimize(self, scored: list[ScoredTicker], context: dict[str, Any]) -> dict[str, float]:
        if not scored:
            return {"CASH": 1.0}

        p = self.params
        selected = [
            item for item in scored
            if item.score >= float(p["stress_threshold"])
            and float(item.raw_factors.get("market_stress") or 0.0) >= float(p["stress_threshold"])
            and float(item.raw_factors.get("underlying_breakdown") or 0.0) >= 0.50
        ][:2]
        if not selected:
            return {"CASH": 1.0}

        max_single = float(p["max_single_weight"])
        max_total = min(float(p["max_total_weight"]), 1.0 - float(p["min_cash_pct"]))
        shifted = {item.ticker: max(item.score, 0.0) for item in selected}
        total_score = sum(shifted.values())
        raw = {ticker: value / total_score * max_total for ticker, value in shifted.items()}
        capped = {ticker: min(weight, max_single) for ticker, weight in raw.items()}
        total = sum(capped.values())
        if total > max_total and total > 0:
            capped = {ticker: weight * max_total / total for ticker, weight in capped.items()}
        out = {ticker: round(weight, 4) for ticker, weight in capped.items() if weight > 0}
        out["CASH"] = round(max(1.0 - sum(out.values()), 0.0), 4)
        return out


UNDERLYING_BY_INVERSE = {
    "SQQQ": "QQQ",
    "SPXS": "SPY",
    "SOXS": "SOXX",
    "TECS": "XLK",
}


def _market_stress_score(holdings: list[dict], context: dict[str, Any]) -> float:
    regime = str(context.get("regime") or "")
    score = {
        "high_vol": 0.75,
        "defensive": 0.70,
        "risk_off": 0.75,
        "crash_breakdown": 0.90,
        "trending_bull": 0.10,
        "risk_on": 0.10,
        "mean_reverting": 0.25,
    }.get(regime, 0.30)
    spy_breakdown = _underlying_breakdown_score("SPY", holdings, context)
    qqq_breakdown = _underlying_breakdown_score("QQQ", holdings, context)
    score += 0.20 * max(spy_breakdown, qqq_breakdown)
    return _clamp(score)


def _underlying_breakdown_score(underlying: str, holdings: list[dict], context: dict[str, Any]) -> float:
    rows = {
        str(row.get("ticker") or "").upper().strip(): row
        for row in holdings
        if row.get("ticker")
    }
    row = rows.get(underlying) or {}
    score = 0.0
    mom20 = _to_float(row.get("mom_20d"))
    mom60 = _to_float(row.get("mom_60d"))
    close = _to_float(row.get("close_price") or row.get("price"))
    sma200 = _to_float(row.get("sma_200"))
    rsi = _to_float(row.get("rsi_14"))
    if mom20 is not None and mom20 < -0.03:
        score += 0.30
    if mom60 is not None and mom60 < -0.06:
        score += 0.30
    if close is not None and sma200 is not None and close < sma200:
        score += 0.25
    if rsi is not None and rsi < 38:
        score += 0.15
    if context.get("market_breakdown") is True:
        score += 0.15
    return _clamp(score)


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


def _clamp(value: float) -> float:
    return max(0.0, min(1.0, float(value)))
