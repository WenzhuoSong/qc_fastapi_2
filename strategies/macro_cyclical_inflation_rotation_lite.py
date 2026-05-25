"""MacroCyclicalInflationRotationLite.

Macro-cycle lens for cyclical, inflation-sensitive, rate-sensitive, and
defensive ETF sleeves. It complements sector relative strength with explicit
regime semantics.
"""
from __future__ import annotations

import logging
import statistics
from typing import Any

from strategies.base import ScoredTicker, Strategy

logger = logging.getLogger("qc_fastapi_2.strategy.macro_cyclical_inflation_rotation_lite")


class MacroCyclicalInflationRotationLite(Strategy):
    name = "macro_cyclical_inflation_rotation_lite"
    version = "1.0"
    description = "Macro-cycle rotation across cyclical, inflation-sensitive, rate-sensitive, and defensive ETFs"
    required_fields = ("mom_20d", "mom_60d", "mom_252d", "hist_vol_20d", "atr_pct", "rsi_14")
    optional_fields = ("return_5d", "rate_regime_label", "inflation_regime_label", "macro_context", "sector_group")
    family = "macro_cycle_rotation"
    core_idea = "Scores XLE/XLI/IWM/XLRE against macro-cycle fit, using SGOV/TLT as defensive fallbacks when cycle evidence is weak or risk is elevated."
    best_regimes = ("risk_on", "trending_bull", "inflationary_growth", "broadening_bull", "falling_rate_expectation", "stable_growth")
    bad_regimes = ("high_vol", "defensive", "risk_off", "credit_stress", "recession_risk")
    signals_used = ("regime", "rate_regime_label", "inflation_regime_label", "mom_20d", "mom_60d", "mom_252d", "hist_vol_20d", "atr_pct", "rsi_14")
    failure_modes = (
        "Macro labels can lag price-implied regime changes.",
        "Cyclical ETFs can reverse sharply when growth expectations deteriorate.",
        "Real estate and duration exposure can lose during rate shocks.",
    )
    agent_guidance = "Use as a macro-regime interpretation layer for cyclicals, energy, real estate, small caps, and defensive fallback. Do not treat it as a pure momentum strategy."
    universe_tickers = ("XLE", "XLI", "IWM", "XLRE", "SGOV", "TLT")

    DEFAULT_PARAMS: dict[str, Any] = {
        "w_macro_fit": 0.45,
        "w_momentum": 0.25,
        "w_low_atr": 0.10,
        "w_low_vol": 0.10,
        "w_not_overbought": 0.10,
        "zscore_clip": 3.0,
        "score_threshold": 0.52,
        "max_holdings": 3,
        "max_single_weight": 0.04,
        "max_total_weight": 0.14,
        "min_cash_pct": 0.86,
        "max_atr_pct": 0.04,
        "cyclical_max_atr_pct": 0.035,
        "max_rsi": 76.0,
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
            logger.warning("macro_cyclical_inflation_rotation_lite: no tickers with complete factor data")
            return []

        p = self.params
        clip = float(p["zscore_clip"])
        z_mom20 = _zscore([float(row["mom_20d"]) for row in valid], clip)
        z_mom60 = _zscore([float(row["mom_60d"]) for row in valid], clip)
        z_mom252 = _zscore([float(row["mom_252d"]) for row in valid], clip)
        z_low_atr = _zscore([-float(row["atr_pct"]) for row in valid], clip)
        z_low_vol = _zscore([-float(row["hist_vol_20d"]) for row in valid], clip)
        macro = _macro_state(context)

        scored: list[ScoredTicker] = []
        for idx, row in enumerate(valid):
            ticker = str(row["ticker"]).upper().strip()
            sleeve = SLEEVE_BY_TICKER.get(ticker, "unknown")
            macro_fit = _macro_fit(ticker=ticker, sleeve=sleeve, macro=macro)
            momentum_quality = _clamp(
                0.50
                + 0.18 * z_mom20[idx]
                + 0.22 * z_mom60[idx]
                + 0.15 * z_mom252[idx]
            )
            atr = float(row["atr_pct"])
            rsi = float(row["rsi_14"])
            overbought_quality = _clamp(1.0 - max(rsi - 66.0, 0.0) / 24.0)
            risk_penalty = _risk_penalty(
                ticker=ticker,
                sleeve=sleeve,
                atr=atr,
                rsi=rsi,
                mom60=float(row["mom_60d"]),
                macro=macro,
                params=p,
            )
            score = _clamp(
                float(p["w_macro_fit"]) * macro_fit
                + float(p["w_momentum"]) * momentum_quality
                + float(p["w_low_atr"]) * _clamp(0.50 + 0.25 * z_low_atr[idx])
                + float(p["w_low_vol"]) * _clamp(0.50 + 0.25 * z_low_vol[idx])
                + float(p["w_not_overbought"]) * overbought_quality
                + risk_penalty
            )
            scored.append(ScoredTicker(
                ticker=ticker,
                score=score,
                factor_breakdown={
                    "macro_fit": round(macro_fit, 4),
                    "momentum_quality": round(momentum_quality, 4),
                    "z_low_atr": round(z_low_atr[idx], 4),
                    "z_low_vol": round(z_low_vol[idx], 4),
                    "overbought_quality": round(overbought_quality, 4),
                    "risk_penalty": round(risk_penalty, 4),
                },
                raw_factors={
                    "mom_20d": float(row["mom_20d"]),
                    "mom_60d": float(row["mom_60d"]),
                    "mom_252d": float(row["mom_252d"]),
                    "hist_vol_20d": float(row["hist_vol_20d"]),
                    "atr_pct": atr,
                    "rsi_14": rsi,
                    "macro_fit": macro_fit,
                    "sleeve": sleeve,
                    "regime": macro["regime"],
                    "rate_regime_label": macro["rate_regime"],
                    "inflation_regime_label": macro["inflation_regime"],
                    "branch": _branch_label(ticker=ticker, sleeve=sleeve, macro=macro),
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
            and _allowed(item, p)
        ][: int(p["max_holdings"])]
        if not selected:
            return {"CASH": 1.0}

        risk = context.get("risk_params") or {}
        max_single = min(float(p["max_single_weight"]), float(risk.get("max_single_position", p["max_single_weight"])))
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


SLEEVE_BY_TICKER = {
    "XLE": "inflation_energy",
    "XLI": "industrial_cycle",
    "IWM": "small_cap_cycle",
    "XLRE": "rate_sensitive_real_estate",
    "SGOV": "cash_defensive",
    "TLT": "duration_defensive",
}

RISK_ON_REGIMES = {"risk_on", "trending_bull", "broadening_bull", "sector_rotation"}
STRESS_REGIMES = {"high_vol", "defensive", "risk_off", "credit_stress", "recession_risk"}
INFLATION_REGIMES = {"inflationary_growth", "commodity_strength", "sticky_inflation", "inflation_shock"}
FALLING_RATE_REGIMES = {"falling_rate_expectation", "growth_scare", "risk_off"}
RISING_RATE_REGIMES = {"rising_rate_expectation", "rapid_rate_rise", "inflation_shock"}


def _macro_state(context: dict[str, Any]) -> dict[str, str]:
    macro = context.get("macro_context") if isinstance(context.get("macro_context"), dict) else {}
    regime = _first_text(context, macro, ("regime", "market_regime")) or "unknown"
    rate_regime = _first_text(context, macro, ("rate_regime_label", "rate_regime", "rates")) or "unknown"
    inflation_regime = _first_text(context, macro, ("inflation_regime_label", "inflation_regime", "inflation")) or "unknown"
    growth_regime = _first_text(context, macro, ("growth_regime_label", "growth_regime", "growth")) or "unknown"
    return {
        "regime": regime,
        "rate_regime": rate_regime,
        "inflation_regime": inflation_regime,
        "growth_regime": growth_regime,
    }


def _first_text(context: dict[str, Any], macro: dict[str, Any], keys: tuple[str, ...]) -> str | None:
    for key in keys:
        value = context.get(key)
        if value:
            return str(value).strip()
        value = macro.get(key)
        if value:
            return str(value).strip()
    return None


def _macro_fit(*, ticker: str, sleeve: str, macro: dict[str, str]) -> float:
    regime = macro["regime"]
    rate = macro["rate_regime"]
    inflation = macro["inflation_regime"]
    growth = macro["growth_regime"]
    if sleeve == "inflation_energy":
        if inflation in INFLATION_REGIMES or regime in {"inflationary_growth", "commodity_strength"}:
            return 1.00
        if regime in RISK_ON_REGIMES:
            return 0.70
        if growth in {"recession_risk", "growth_scare"}:
            return 0.20
        return 0.45
    if sleeve in {"industrial_cycle", "small_cap_cycle"}:
        if regime in {"broadening_bull", "risk_on"} or growth in {"reacceleration", "stable_growth"}:
            return 0.90 if ticker == "XLI" else 0.85
        if regime == "trending_bull":
            return 0.65
        if regime in STRESS_REGIMES or growth in {"recession_risk", "growth_scare"}:
            return 0.10
        return 0.45
    if sleeve == "rate_sensitive_real_estate":
        if rate in FALLING_RATE_REGIMES or regime in {"falling_rate_expectation", "stable_growth"}:
            return 0.90
        if rate in RISING_RATE_REGIMES or inflation in {"inflation_shock", "sticky_inflation"}:
            return 0.10
        if regime in RISK_ON_REGIMES:
            return 0.50
        return 0.35
    if sleeve == "cash_defensive":
        if regime in STRESS_REGIMES or rate in RISING_RATE_REGIMES:
            return 0.85
        if regime in RISK_ON_REGIMES:
            return 0.20
        return 0.55
    if sleeve == "duration_defensive":
        if rate in FALLING_RATE_REGIMES or growth in {"growth_scare", "recession_risk"}:
            return 0.90
        if rate in RISING_RATE_REGIMES or inflation in {"inflation_shock", "sticky_inflation"}:
            return 0.05
        if regime in STRESS_REGIMES:
            return 0.55
        return 0.25
    return 0.0


def _risk_penalty(
    *,
    ticker: str,
    sleeve: str,
    atr: float,
    rsi: float,
    mom60: float,
    macro: dict[str, str],
    params: dict[str, Any],
) -> float:
    penalty = 0.0
    regime = macro["regime"]
    rate = macro["rate_regime"]
    inflation = macro["inflation_regime"]
    if atr > float(params["max_atr_pct"]):
        penalty -= 0.20
    if sleeve in {"inflation_energy", "industrial_cycle", "small_cap_cycle", "rate_sensitive_real_estate"}:
        if atr > float(params["cyclical_max_atr_pct"]):
            penalty -= 0.15
        if regime in STRESS_REGIMES:
            penalty -= 0.30
    if sleeve == "rate_sensitive_real_estate" and rate in RISING_RATE_REGIMES:
        penalty -= 0.30
    if sleeve == "duration_defensive" and (rate in RISING_RATE_REGIMES or inflation in {"inflation_shock", "sticky_inflation"}):
        penalty -= 0.35
    if ticker == "IWM" and macro["growth_regime"] in {"credit_stress", "recession_risk"}:
        penalty -= 0.30
    if rsi > float(params["max_rsi"]):
        penalty -= 0.15
    if mom60 < -0.08:
        penalty -= 0.15
    return penalty


def _allowed(item: ScoredTicker, params: dict[str, Any]) -> bool:
    sleeve = str(item.raw_factors.get("sleeve") or "")
    regime = str(item.raw_factors.get("regime") or "")
    rate = str(item.raw_factors.get("rate_regime_label") or "")
    inflation = str(item.raw_factors.get("inflation_regime_label") or "")
    atr = float(item.raw_factors.get("atr_pct") or 0.0)
    rsi = float(item.raw_factors.get("rsi_14") or 100.0)
    mom60 = float(item.raw_factors.get("mom_60d") or 0.0)
    macro_fit = float(item.raw_factors.get("macro_fit") or 0.0)
    if macro_fit < 0.45:
        return False
    if atr > float(params["max_atr_pct"]) or rsi > float(params["max_rsi"]):
        return False
    if sleeve in {"inflation_energy", "industrial_cycle", "small_cap_cycle", "rate_sensitive_real_estate"}:
        if regime in STRESS_REGIMES:
            return False
        if atr > float(params["cyclical_max_atr_pct"]):
            return False
        if mom60 <= 0:
            return False
    if sleeve == "rate_sensitive_real_estate" and rate in RISING_RATE_REGIMES:
        return False
    if sleeve == "duration_defensive" and (rate in RISING_RATE_REGIMES or inflation in {"inflation_shock", "sticky_inflation"}):
        return False
    return True


def _branch_label(*, ticker: str, sleeve: str, macro: dict[str, str]) -> str:
    regime = macro["regime"] or "unknown"
    if sleeve == "inflation_energy":
        return f"{regime}_inflation_energy_cycle"
    if sleeve in {"industrial_cycle", "small_cap_cycle"}:
        return f"{regime}_cyclical_broadening"
    if sleeve == "rate_sensitive_real_estate":
        return f"{regime}_rate_sensitive_real_estate"
    if sleeve in {"cash_defensive", "duration_defensive"}:
        return f"{regime}_{ticker.lower()}_macro_defensive_fallback"
    return f"{regime}_macro_cycle_rotation"


def _zscore(values: list[float], clip: float) -> list[float]:
    if len(values) < 2:
        return [0.0] * len(values)
    mean = statistics.fmean(values)
    std = statistics.stdev(values)
    if std == 0:
        return [0.0] * len(values)
    return [max(-clip, min(clip, (value - mean) / std)) for value in values]


def _clamp(value: float) -> float:
    return max(0.0, min(1.0, float(value)))
