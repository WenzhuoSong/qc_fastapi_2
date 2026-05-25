"""MacroRateDurationLite.

Defensive duration selector across cash-like, short-duration, aggregate, and
Treasury duration ETFs. Macro/rate context is optional; ETF-implied trend and
risk features remain the primary deterministic inputs.
"""
from __future__ import annotations

import logging
import statistics
from typing import Any

from strategies.base import ScoredTicker, Strategy

logger = logging.getLogger("qc_fastapi_2.strategy.macro_rate_duration_lite")


class MacroRateDurationLite(Strategy):
    name = "macro_rate_duration_lite"
    version = "1.0"
    description = "Rate/duration defensive selector for cash-like and bond ETFs"
    required_fields = ("hist_vol_20d", "atr_pct", "mom_20d", "mom_60d")
    optional_fields = ("return_5d", "rsi_14", "duration_bucket", "rate_regime_label")
    family = "macro_rate"
    core_idea = "Selects between cash-like, short-duration, aggregate, intermediate-duration, and long-duration bond ETFs using rate-regime context plus ETF-implied trend and risk."
    best_regimes = ("defensive", "high_vol", "risk_off", "falling_rate_expectation", "stable_rates")
    bad_regimes = ("strong_risk_on", "inflation_shock", "rapid_rate_rise", "credit_stress")
    signals_used = ("rate_regime_label", "hist_vol_20d", "atr_pct", "mom_20d", "mom_60d")
    failure_modes = (
        "Duration ETFs can lose money during inflation or rate shocks even when equities are weak.",
        "Macro labels can lag market-implied rate moves.",
        "Credit-sensitive bond ETFs can fail as defensive assets during spread stress.",
    )
    agent_guidance = "Use to choose defensive duration exposure. Treat it as carry/cash-proxy evidence, not a separate independent alpha family."
    universe_tickers = ("SGOV", "BSV", "BND", "IEF", "TLT")

    DEFAULT_PARAMS: dict[str, Any] = {
        "w_low_vol": 0.15,
        "w_low_atr": 0.10,
        "w_mom_60d": 0.25,
        "w_mom_20d": 0.15,
        "w_duration_fit": 0.35,
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
            row for row in self.eligible_rows(holdings)
            if row.get("ticker")
            and all(row.get(field) is not None for field in self.required_fields)
        ]
        if not valid:
            logger.warning("macro_rate_duration_lite: no tickers with complete factor data")
            return []

        p = self.params
        clip = float(p["zscore_clip"])
        z_low_vol = _zscore([-float(row["hist_vol_20d"]) for row in valid], clip)
        z_low_atr = _zscore([-float(row["atr_pct"]) for row in valid], clip)
        z_mom60 = _zscore([float(row["mom_60d"]) for row in valid], clip)
        z_mom20 = _zscore([float(row["mom_20d"]) for row in valid], clip)
        rate_regime = _rate_regime(context)
        market_regime = str(context.get("regime") or "")

        scored: list[ScoredTicker] = []
        for idx, row in enumerate(valid):
            ticker = str(row["ticker"]).upper().strip()
            bucket = _duration_bucket(ticker, row)
            duration_fit = _duration_fit(bucket, rate_regime, market_regime)
            duration_risk_penalty = _duration_risk_penalty(
                bucket=bucket,
                mom60=float(row["mom_60d"]),
                atr=float(row["atr_pct"]),
                rate_regime=rate_regime,
            )
            score = (
                float(p["w_low_vol"]) * z_low_vol[idx]
                + float(p["w_low_atr"]) * z_low_atr[idx]
                + float(p["w_mom_60d"]) * z_mom60[idx]
                + float(p["w_mom_20d"]) * z_mom20[idx]
                + float(p["w_duration_fit"]) * duration_fit
                + duration_risk_penalty
            )
            scored.append(ScoredTicker(
                ticker=ticker,
                score=score,
                factor_breakdown={
                    "z_low_vol": round(z_low_vol[idx], 4),
                    "z_low_atr": round(z_low_atr[idx], 4),
                    "z_mom_60d": round(z_mom60[idx], 4),
                    "z_mom_20d": round(z_mom20[idx], 4),
                    "duration_fit": round(duration_fit, 4),
                    "duration_risk_penalty": round(duration_risk_penalty, 4),
                },
                raw_factors={
                    "hist_vol_20d": float(row["hist_vol_20d"]),
                    "atr_pct": float(row["atr_pct"]),
                    "mom_20d": float(row["mom_20d"]),
                    "mom_60d": float(row["mom_60d"]),
                    "duration_bucket": bucket,
                    "rate_regime_label": rate_regime,
                    "branch": f"{rate_regime}_{market_regime or 'unknown'}_duration_selector",
                },
            ))

        scored.sort(key=lambda item: item.score, reverse=True)
        return scored

    def optimize(self, scored: list[ScoredTicker], context: dict[str, Any]) -> dict[str, float]:
        if not scored:
            return {"CASH": 1.0}

        p = self.params
        rate_regime = _rate_regime(context)
        selected = [
            item for item in scored
            if item.score > 0
            and _duration_allowed(
                bucket=str(item.raw_factors.get("duration_bucket") or ""),
                mom60=float(item.raw_factors.get("mom_60d") or 0.0),
                rate_regime=rate_regime,
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


DURATION_BUCKET_BY_TICKER = {
    "SGOV": "cash_like",
    "BSV": "short_duration",
    "BND": "aggregate_bond",
    "IEF": "intermediate_duration",
    "TLT": "long_duration",
}

RISING_RATE_REGIMES = {"rising_rate_expectation", "inflation_shock", "rapid_rate_rise"}
FALLING_RATE_REGIMES = {"falling_rate_expectation", "growth_scare", "risk_off"}
STABLE_DEFENSIVE_REGIMES = {"stable_rates", "defensive", "high_vol", "cash_only", "unknown"}


def _duration_bucket(ticker: str, row: dict[str, Any]) -> str:
    return str(row.get("duration_bucket") or DURATION_BUCKET_BY_TICKER.get(ticker, "unknown"))


def _rate_regime(context: dict[str, Any]) -> str:
    for key in ("rate_regime_label", "rate_regime", "macro_rate_regime"):
        value = context.get(key)
        if value:
            return str(value).strip()
    macro = context.get("macro_context") if isinstance(context.get("macro_context"), dict) else {}
    for key in ("rate_regime_label", "rate_regime", "rates"):
        value = macro.get(key)
        if value:
            return str(value).strip()
    regime = str(context.get("regime") or "")
    if regime in {"defensive", "high_vol", "risk_off"}:
        return regime
    return "unknown"


def _duration_fit(bucket: str, rate_regime: str, market_regime: str) -> float:
    if rate_regime in RISING_RATE_REGIMES:
        return {
            "cash_like": 1.00,
            "short_duration": 0.80,
            "aggregate_bond": 0.25,
            "intermediate_duration": -0.25,
            "long_duration": -0.75,
        }.get(bucket, 0.0)
    if rate_regime in FALLING_RATE_REGIMES:
        return {
            "cash_like": 0.15,
            "short_duration": 0.30,
            "aggregate_bond": 0.55,
            "intermediate_duration": 0.80,
            "long_duration": 1.00,
        }.get(bucket, 0.0)
    if rate_regime in STABLE_DEFENSIVE_REGIMES or market_regime in {"defensive", "high_vol"}:
        return {
            "cash_like": 0.85,
            "short_duration": 0.75,
            "aggregate_bond": 0.35,
            "intermediate_duration": 0.10,
            "long_duration": -0.20,
        }.get(bucket, 0.0)
    if market_regime in {"trending_bull", "risk_on", "strong_risk_on"}:
        return {
            "cash_like": 0.25,
            "short_duration": 0.15,
            "aggregate_bond": -0.05,
            "intermediate_duration": -0.10,
            "long_duration": -0.25,
        }.get(bucket, 0.0)
    return 0.0


def _duration_risk_penalty(*, bucket: str, mom60: float, atr: float, rate_regime: str) -> float:
    penalty = 0.0
    if bucket in {"intermediate_duration", "long_duration"} and mom60 < 0:
        penalty -= 0.20
    if bucket == "long_duration" and atr > 0.025:
        penalty -= 0.15
    if bucket in {"aggregate_bond", "intermediate_duration", "long_duration"} and rate_regime in RISING_RATE_REGIMES:
        penalty -= 0.20
    return penalty


def _duration_allowed(*, bucket: str, mom60: float, rate_regime: str) -> bool:
    if bucket == "long_duration" and rate_regime in RISING_RATE_REGIMES:
        return False
    if bucket == "intermediate_duration" and rate_regime in RISING_RATE_REGIMES and mom60 <= 0:
        return False
    if bucket in {"aggregate_bond", "intermediate_duration", "long_duration"} and mom60 < -0.08:
        return False
    return True


def _zscore(values: list[float], clip: float) -> list[float]:
    if len(values) < 2:
        return [0.0] * len(values)
    mean = statistics.fmean(values)
    std = statistics.stdev(values)
    if std == 0:
        return [0.0] * len(values)
    return [max(-clip, min(clip, (value - mean) / std)) for value in values]
