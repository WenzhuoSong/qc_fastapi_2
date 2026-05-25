"""DefensiveQualityRotationLite.

Low-vol defensive rotation across cash-like and Treasury/bond ETFs. This is a
small, capped defensive sleeve and does not bypass any downstream risk gates.
"""
from __future__ import annotations

import logging
import statistics
from typing import Any

from strategies.base import ScoredTicker, Strategy

logger = logging.getLogger("qc_fastapi_2.strategy.defensive_quality_rotation_lite")


class DefensiveQualityRotationLite(Strategy):
    name = "defensive_quality_rotation_lite"
    version = "1.0"
    description = "Low-volatility defensive quality rotation across cash and Treasury ETFs"
    required_fields = ("hist_vol_20d", "atr_pct", "mom_60d", "return_5d", "rsi_14")
    optional_fields = ("mom_20d", "mom_252d", "beta_vs_spy")
    family = "low_vol_defensive"
    core_idea = "Favors defensive ETFs with low realized volatility, low ATR, stable recent returns, and positive intermediate trend."
    best_regimes = ("defensive", "high_vol", "risk_off", "late_cycle")
    bad_regimes = ("trending_bull", "strong_risk_on", "inflation_shock")
    signals_used = ("hist_vol_20d", "atr_pct", "mom_60d", "return_5d", "rsi_14")
    failure_modes = (
        "Can sit in low-return assets while equities trend higher.",
        "Duration-heavy defensive ETFs can lose during inflation or rate shocks.",
        "Backward-looking volatility can miss a sudden correlation break.",
    )
    agent_guidance = "Use as a low-vol defensive sleeve. It can support de-risking but should not be interpreted as a high-return alpha signal."
    universe_tickers = ("SGOV", "BND", "IEF", "TLT")

    DEFAULT_PARAMS: dict[str, Any] = {
        "w_low_vol": 0.35,
        "w_low_atr": 0.25,
        "w_mom_60d": 0.20,
        "w_stable_return_5d": 0.10,
        "w_rsi_mid": 0.10,
        "zscore_clip": 3.0,
        "max_holdings": 3,
        "max_single_weight": 0.05,
        "max_total_weight": 0.18,
        "min_cash_pct": 0.82,
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
            logger.warning("defensive_quality_rotation_lite: no tickers with complete factor data")
            return []

        p = self.params
        clip = float(p["zscore_clip"])
        z_low_vol = _zscore([-float(row["hist_vol_20d"]) for row in valid], clip)
        z_low_atr = _zscore([-float(row["atr_pct"]) for row in valid], clip)
        z_mom60 = _zscore([float(row["mom_60d"]) for row in valid], clip)
        z_stable5d = _zscore([-abs(float(row["return_5d"])) for row in valid], clip)
        z_rsi_mid = _zscore([-abs(float(row["rsi_14"]) - 50.0) for row in valid], clip)

        regime = str(context.get("regime") or "")
        defensive_boost = 0.15 if regime in {"defensive", "high_vol", "risk_off", "cash_only"} else 0.0
        risk_on_penalty = -0.15 if regime in {"trending_bull", "risk_on"} else 0.0

        scored: list[ScoredTicker] = []
        for idx, row in enumerate(valid):
            ticker = str(row["ticker"]).upper().strip()
            duration_penalty = -0.12 if ticker == "TLT" and regime in {"risk_on", "inflation_shock"} else 0.0
            cash_quality_boost = 0.08 if ticker == "SGOV" and regime in {"defensive", "high_vol", "cash_only"} else 0.0
            score = (
                float(p["w_low_vol"]) * z_low_vol[idx]
                + float(p["w_low_atr"]) * z_low_atr[idx]
                + float(p["w_mom_60d"]) * z_mom60[idx]
                + float(p["w_stable_return_5d"]) * z_stable5d[idx]
                + float(p["w_rsi_mid"]) * z_rsi_mid[idx]
                + defensive_boost
                + risk_on_penalty
                + duration_penalty
                + cash_quality_boost
            )
            scored.append(ScoredTicker(
                ticker=ticker,
                score=score,
                factor_breakdown={
                    "z_low_vol": round(z_low_vol[idx], 4),
                    "z_low_atr": round(z_low_atr[idx], 4),
                    "z_mom_60d": round(z_mom60[idx], 4),
                    "z_stable_return_5d": round(z_stable5d[idx], 4),
                    "z_rsi_mid": round(z_rsi_mid[idx], 4),
                    "regime_adjustment": round(defensive_boost + risk_on_penalty + duration_penalty + cash_quality_boost, 4),
                },
                raw_factors={
                    "hist_vol_20d": float(row["hist_vol_20d"]),
                    "atr_pct": float(row["atr_pct"]),
                    "mom_60d": float(row["mom_60d"]),
                    "return_5d": float(row["return_5d"]),
                    "rsi_14": float(row["rsi_14"]),
                    "branch": f"{regime or 'unknown'}_defensive_quality_rotation",
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


def _zscore(values: list[float], clip: float) -> list[float]:
    if len(values) < 2:
        return [0.0] * len(values)
    mean = statistics.fmean(values)
    std = statistics.stdev(values)
    if std == 0:
        return [0.0] * len(values)
    return [max(-clip, min(clip, (value - mean) / std)) for value in values]
