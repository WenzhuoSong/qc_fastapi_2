"""AbsoluteTrendFollowingLite.

Non-leveraged time-series trend following with defensive fallback. It ranks
ordinary ETFs by absolute trend strength and keeps most capital in CASH unless
trend evidence is positive.
"""
from __future__ import annotations

import logging
import statistics
from typing import Any

from strategies.base import ScoredTicker, Strategy

logger = logging.getLogger("qc_fastapi_2.strategy.absolute_trend_following_lite")


class AbsoluteTrendFollowingLite(Strategy):
    name = "absolute_trend_following_lite"
    version = "1.0"
    description = "Non-leveraged absolute trend-following strategy with defensive fallback"
    required_fields = ("mom_60d", "mom_252d", "hist_vol_20d", "atr_pct")
    optional_fields = ("close_price", "price", "sma_200", "return_5d", "rsi_14")
    family = "trend_following"
    core_idea = "Holds ordinary ETFs only when medium and long trend are positive; otherwise prefers cash-like defensive exposure."
    best_regimes = ("trending_bull", "risk_on", "persistent_trend")
    bad_regimes = ("mean_reverting", "sideways_chop", "violent_whipsaw")
    signals_used = ("mom_60d", "mom_252d", "hist_vol_20d", "atr_pct", "sma_200")
    failure_modes = (
        "Can whipsaw around trend thresholds.",
        "May enter after a large move and miss early reversals.",
        "Defensive fallback can lag when risk appetite recovers quickly.",
    )
    agent_guidance = "Use as a clean non-leveraged trend baseline. Discount it in choppy mean-reverting regimes and do not confuse it with leveraged momentum."
    universe_tickers = ("SPY", "QQQ", "IWM", "SGOV", "IEF")

    DEFAULT_PARAMS: dict[str, Any] = {
        "w_mom_60d": 0.35,
        "w_mom_252d": 0.35,
        "w_low_vol": 0.15,
        "w_low_atr": 0.10,
        "w_sma200": 0.05,
        "zscore_clip": 3.0,
        "max_holdings": 3,
        "max_single_weight": 0.08,
        "max_total_weight": 0.30,
        "min_cash_pct": 0.70,
        "absolute_momentum_floor": 0.0,
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
            logger.warning("absolute_trend_following_lite: no tickers with complete factor data")
            return []

        p = self.params
        clip = float(p["zscore_clip"])
        z_mom60 = _zscore([float(row["mom_60d"]) for row in valid], clip)
        z_mom252 = _zscore([float(row["mom_252d"]) for row in valid], clip)
        z_low_vol = _zscore([-float(row["hist_vol_20d"]) for row in valid], clip)
        z_low_atr = _zscore([-float(row["atr_pct"]) for row in valid], clip)
        z_sma = _zscore([_sma_distance(row) for row in valid], clip)

        regime = str(context.get("regime") or "")
        trend_regime_boost = 0.12 if regime in {"trending_bull", "risk_on"} else 0.0
        chop_penalty = -0.15 if regime in {"mean_reverting", "high_vol", "defensive"} else 0.0

        scored: list[ScoredTicker] = []
        for idx, row in enumerate(valid):
            ticker = str(row["ticker"]).upper().strip()
            trend_ok = float(row["mom_60d"]) > float(p["absolute_momentum_floor"]) and float(row["mom_252d"]) > float(p["absolute_momentum_floor"])
            defensive_ticker = ticker in {"SGOV", "IEF"}
            defensive_adjustment = 0.10 if defensive_ticker and regime in {"defensive", "high_vol"} else 0.0
            trend_break_penalty = -0.35 if not trend_ok and not defensive_ticker else 0.0
            score = (
                float(p["w_mom_60d"]) * z_mom60[idx]
                + float(p["w_mom_252d"]) * z_mom252[idx]
                + float(p["w_low_vol"]) * z_low_vol[idx]
                + float(p["w_low_atr"]) * z_low_atr[idx]
                + float(p["w_sma200"]) * z_sma[idx]
                + trend_regime_boost
                + chop_penalty
                + defensive_adjustment
                + trend_break_penalty
            )
            scored.append(ScoredTicker(
                ticker=ticker,
                score=score,
                factor_breakdown={
                    "z_mom_60d": round(z_mom60[idx], 4),
                    "z_mom_252d": round(z_mom252[idx], 4),
                    "z_low_vol": round(z_low_vol[idx], 4),
                    "z_low_atr": round(z_low_atr[idx], 4),
                    "z_sma200_distance": round(z_sma[idx], 4),
                    "regime_adjustment": round(trend_regime_boost + chop_penalty + defensive_adjustment + trend_break_penalty, 4),
                },
                raw_factors={
                    "mom_60d": float(row["mom_60d"]),
                    "mom_252d": float(row["mom_252d"]),
                    "hist_vol_20d": float(row["hist_vol_20d"]),
                    "atr_pct": float(row["atr_pct"]),
                    "sma_200_distance": _sma_distance(row),
                    "trend_ok": 1.0 if trend_ok else 0.0,
                    "branch": f"{regime or 'unknown'}_absolute_trend_following",
                },
            ))

        scored.sort(key=lambda item: item.score, reverse=True)
        return scored

    def optimize(self, scored: list[ScoredTicker], context: dict[str, Any]) -> dict[str, float]:
        if not scored:
            return {"CASH": 1.0}

        p = self.params
        floor = float(p["absolute_momentum_floor"])
        selected = [
            item for item in scored
            if item.score > 0
            and (
                item.ticker in {"SGOV", "IEF"}
                or (
                    float(item.raw_factors.get("mom_60d") or 0.0) > floor
                    and float(item.raw_factors.get("mom_252d") or 0.0) > floor
                )
            )
        ][: int(p["max_holdings"])]
        if not selected:
            return {"CASH": 1.0}

        risk = context.get("risk_params") or {}
        max_single = min(float(p["max_single_weight"]), float(risk.get("max_single_position", p["max_single_weight"])))
        max_total = min(float(p["max_total_weight"]), 1.0 - float(p["min_cash_pct"]))
        shifted = {item.ticker: max(item.score, 0.0) + 0.05 for item in selected}
        total_score = sum(shifted.values())
        raw = {ticker: value / total_score * max_total for ticker, value in shifted.items()}
        capped = {ticker: min(weight, max_single) for ticker, weight in raw.items()}
        total = sum(capped.values())
        if total > max_total and total > 0:
            capped = {ticker: weight * max_total / total for ticker, weight in capped.items()}
        out = {ticker: round(weight, 4) for ticker, weight in capped.items() if weight > 0}
        out["CASH"] = round(max(1.0 - sum(out.values()), 0.0), 4)
        return out


def _sma_distance(row: dict[str, Any]) -> float:
    price = row.get("close_price", row.get("price"))
    sma = row.get("sma_200")
    try:
        price_f = float(price)
        sma_f = float(sma)
    except (TypeError, ValueError):
        return 0.0
    if sma_f <= 0:
        return 0.0
    return (price_f / sma_f) - 1.0


def _zscore(values: list[float], clip: float) -> list[float]:
    if len(values) < 2:
        return [0.0] * len(values)
    mean = statistics.fmean(values)
    std = statistics.stdev(values)
    if std == 0:
        return [0.0] * len(values)
    return [max(-clip, min(clip, (value - mean) / std)) for value in values]
