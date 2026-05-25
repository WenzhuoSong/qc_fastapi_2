"""LeveragedLongAmplifierLite.

Tiny risk-on amplifier signal for leveraged long ETFs. This is deliberately
separate from the high-risk full allocator and never treats leveraged ETFs as
core holdings.
"""
from __future__ import annotations

import logging
import statistics
from typing import Any

from strategies.base import ScoredTicker, Strategy

logger = logging.getLogger("qc_fastapi_2.strategy.leveraged_long_amplifier_lite")


class LeveragedLongAmplifierLite(Strategy):
    name = "leveraged_long_amplifier_lite"
    version = "1.0"
    description = "Tiny risk-on amplifier signal for leveraged long ETFs"
    required_fields = ("mom_20d", "mom_60d", "mom_252d", "hist_vol_20d", "atr_pct", "rsi_14")
    optional_fields = ("return_5d", "underlying", "underlying_mom_60d")
    family = "leveraged_rotation"
    core_idea = "Scores leveraged long ETFs only when risk-on regime, leveraged ETF trend, and underlying ETF trend all confirm."
    best_regimes = ("trending_bull", "risk_on", "sector_rotation")
    bad_regimes = ("high_vol", "defensive", "risk_off", "mean_reverting", "violent_whipsaw")
    signals_used = ("mom_20d", "mom_60d", "mom_252d", "hist_vol_20d", "atr_pct", "rsi_14", "underlying_trend")
    failure_modes = (
        "Daily reset compounding can erode value in volatile sideways regimes.",
        "Gap risk is amplified by leverage.",
        "Late entries after crowded rallies can reverse sharply.",
    )
    agent_guidance = "Use only as a small risk-on amplifier evidence layer. Do not substitute it for core equity exposure or the playground-only allocator."
    universe_tickers = ("TQQQ", "SOXL", "TECL", "SPXL")

    DEFAULT_PARAMS: dict[str, Any] = {
        "w_leveraged_momentum": 0.40,
        "w_underlying_confirmation": 0.30,
        "w_low_atr": 0.15,
        "w_not_overbought": 0.15,
        "zscore_clip": 3.0,
        "score_threshold": 0.58,
        "max_holdings": 2,
        "max_single_weight": 0.02,
        "max_total_weight": 0.06,
        "min_cash_pct": 0.94,
        "max_atr_pct": 0.08,
        "max_rsi": 78.0,
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
            logger.warning("leveraged_long_amplifier_lite: no leveraged ETFs with complete factor data")
            return []

        p = self.params
        clip = float(p["zscore_clip"])
        z_mom20 = _zscore([float(row["mom_20d"]) for row in valid], clip)
        z_mom60 = _zscore([float(row["mom_60d"]) for row in valid], clip)
        z_mom252 = _zscore([float(row["mom_252d"]) for row in valid], clip)
        z_low_atr = _zscore([-float(row["atr_pct"]) for row in valid], clip)
        regime = str(context.get("regime") or "")
        risk_on_gate = _risk_on_gate(regime)

        scored: list[ScoredTicker] = []
        for idx, row in enumerate(valid):
            ticker = str(row["ticker"]).upper().strip()
            underlying = UNDERLYING_BY_LEVERAGED.get(ticker, "")
            underlying_confirmation = _underlying_confirmation(underlying, holdings)
            leveraged_momentum = _clamp(
                0.50
                + 0.20 * z_mom20[idx]
                + 0.25 * z_mom60[idx]
                + 0.20 * z_mom252[idx]
            )
            atr = float(row["atr_pct"])
            rsi = float(row["rsi_14"])
            overbought_quality = _clamp(1.0 - max(rsi - 65.0, 0.0) / 25.0)
            atr_block_penalty = -0.35 if atr > float(p["max_atr_pct"]) else 0.0
            overbought_penalty = -0.20 if rsi > float(p["max_rsi"]) else 0.0
            regime_penalty = -0.45 if not risk_on_gate else 0.0
            score = _clamp(
                float(p["w_leveraged_momentum"]) * leveraged_momentum
                + float(p["w_underlying_confirmation"]) * underlying_confirmation
                + float(p["w_low_atr"]) * _clamp(0.50 + 0.25 * z_low_atr[idx])
                + float(p["w_not_overbought"]) * overbought_quality
                + atr_block_penalty
                + overbought_penalty
                + regime_penalty
            )
            branch = "risk_on_leveraged_amplifier" if risk_on_gate else "no_leveraged_amplifier"
            scored.append(ScoredTicker(
                ticker=ticker,
                score=score,
                factor_breakdown={
                    "leveraged_momentum": round(leveraged_momentum, 4),
                    "underlying_confirmation": round(underlying_confirmation, 4),
                    "z_low_atr": round(z_low_atr[idx], 4),
                    "overbought_quality": round(overbought_quality, 4),
                    "risk_on_gate": 1.0 if risk_on_gate else 0.0,
                    "risk_adjustment": round(atr_block_penalty + overbought_penalty + regime_penalty, 4),
                },
                raw_factors={
                    "mom_20d": float(row["mom_20d"]),
                    "mom_60d": float(row["mom_60d"]),
                    "mom_252d": float(row["mom_252d"]),
                    "hist_vol_20d": float(row["hist_vol_20d"]),
                    "atr_pct": atr,
                    "rsi_14": rsi,
                    "underlying": underlying,
                    "underlying_confirmation": underlying_confirmation,
                    "risk_on_gate": 1.0 if risk_on_gate else 0.0,
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
            if item.score >= float(p["score_threshold"])
            and float(item.raw_factors.get("risk_on_gate") or 0.0) >= 1.0
            and float(item.raw_factors.get("underlying_confirmation") or 0.0) >= 0.65
            and float(item.raw_factors.get("atr_pct") or 0.0) <= float(p["max_atr_pct"])
            and float(item.raw_factors.get("rsi_14") or 100.0) <= float(p["max_rsi"])
        ][: int(p["max_holdings"])]
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


UNDERLYING_BY_LEVERAGED = {
    "TQQQ": "QQQ",
    "SOXL": "SOXX",
    "TECL": "XLK",
    "SPXL": "SPY",
}


def _risk_on_gate(regime: str) -> bool:
    return regime in {"trending_bull", "risk_on", "sector_rotation"}


def _underlying_confirmation(underlying: str, holdings: list[dict]) -> float:
    row = {
        str(item.get("ticker") or "").upper().strip(): item
        for item in holdings
        if item.get("ticker")
    }.get(underlying) or {}
    score = 0.0
    mom20 = _to_float(row.get("mom_20d"))
    mom60 = _to_float(row.get("mom_60d"))
    mom252 = _to_float(row.get("mom_252d"))
    close = _to_float(row.get("close_price") or row.get("price"))
    sma200 = _to_float(row.get("sma_200"))
    rsi = _to_float(row.get("rsi_14"))
    if mom20 is not None and mom20 > 0:
        score += 0.20
    if mom60 is not None and mom60 > 0:
        score += 0.30
    if mom252 is not None and mom252 > 0:
        score += 0.25
    if close is not None and sma200 is not None and close > sma200:
        score += 0.15
    if rsi is not None and rsi < 78:
        score += 0.10
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
