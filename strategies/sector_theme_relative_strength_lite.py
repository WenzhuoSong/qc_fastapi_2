"""SectorThemeRelativeStrengthLite.

Relative-strength rotation across sector and thematic ETFs. It is deliberately
canonicalized as momentum, not a new independent alpha family.
"""
from __future__ import annotations

import logging
import statistics
from typing import Any

from strategies.base import ScoredTicker, Strategy

logger = logging.getLogger("qc_fastapi_2.strategy.sector_theme_relative_strength_lite")


class SectorThemeRelativeStrengthLite(Strategy):
    name = "sector_theme_relative_strength_lite"
    version = "1.0"
    description = "Capped sector/theme ETF relative-strength rotation"
    required_fields = ("mom_20d", "mom_60d", "mom_252d", "hist_vol_20d", "atr_pct", "rsi_14")
    optional_fields = ("return_5d", "beta_vs_spy", "sector_group")
    family = "sector_theme_rotation"
    core_idea = "Ranks sector and thematic ETFs by multi-horizon relative strength with volatility and concentration controls."
    best_regimes = ("trending_bull", "risk_on", "sector_rotation")
    bad_regimes = ("high_vol", "defensive", "violent_whipsaw")
    signals_used = ("mom_20d", "mom_60d", "mom_252d", "hist_vol_20d", "atr_pct", "rsi_14")
    failure_modes = (
        "Can concentrate in crowded technology or semiconductor themes.",
        "Can buy late after a sharp thematic rally.",
        "Can whipsaw when sector leadership rotates quickly.",
    )
    agent_guidance = "Use as a sector/theme leadership lens. Treat it as momentum-family evidence and keep caps small because ETF themes are highly correlated."
    universe_tickers = ("XLK", "SOXX", "XSD", "PSI", "FTXL", "AIQ", "CIBR", "BOTZ", "XLE", "XLI", "XLRE")

    DEFAULT_PARAMS: dict[str, Any] = {
        "w_mom_20d": 0.25,
        "w_mom_60d": 0.35,
        "w_mom_252d": 0.20,
        "w_low_vol": 0.10,
        "w_low_atr": 0.05,
        "w_rsi_not_overbought": 0.05,
        "zscore_clip": 3.0,
        "max_holdings": 4,
        "max_single_weight": 0.05,
        "max_total_weight": 0.18,
        "max_group_weight": 0.10,
        "min_cash_pct": 0.82,
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
            logger.warning("sector_theme_relative_strength_lite: no tickers with complete factor data")
            return []

        p = self.params
        clip = float(p["zscore_clip"])
        z_mom20 = _zscore([float(row["mom_20d"]) for row in valid], clip)
        z_mom60 = _zscore([float(row["mom_60d"]) for row in valid], clip)
        z_mom252 = _zscore([float(row["mom_252d"]) for row in valid], clip)
        z_low_vol = _zscore([-float(row["hist_vol_20d"]) for row in valid], clip)
        z_low_atr = _zscore([-float(row["atr_pct"]) for row in valid], clip)
        z_rsi = _zscore([-max(float(row["rsi_14"]) - 70.0, 0.0) for row in valid], clip)

        regime = str(context.get("regime") or "")
        regime_adjustment = 0.12 if regime in {"trending_bull", "risk_on", "sector_rotation"} else 0.0
        if regime in {"high_vol", "defensive", "risk_off"}:
            regime_adjustment -= 0.25

        scored: list[ScoredTicker] = []
        for idx, row in enumerate(valid):
            ticker = str(row["ticker"]).upper().strip()
            mom60 = float(row["mom_60d"])
            mom252 = float(row["mom_252d"])
            trend_ok = mom60 > float(p["absolute_momentum_floor"]) and mom252 > float(p["absolute_momentum_floor"])
            trend_break_penalty = -0.35 if not trend_ok else 0.0
            crowded_theme_penalty = -0.08 if _sector_group(ticker, row) in {"semiconductors", "tech_growth"} and regime in {"high_vol", "defensive"} else 0.0
            score = (
                float(p["w_mom_20d"]) * z_mom20[idx]
                + float(p["w_mom_60d"]) * z_mom60[idx]
                + float(p["w_mom_252d"]) * z_mom252[idx]
                + float(p["w_low_vol"]) * z_low_vol[idx]
                + float(p["w_low_atr"]) * z_low_atr[idx]
                + float(p["w_rsi_not_overbought"]) * z_rsi[idx]
                + regime_adjustment
                + trend_break_penalty
                + crowded_theme_penalty
            )
            group = _sector_group(ticker, row)
            scored.append(ScoredTicker(
                ticker=ticker,
                score=score,
                factor_breakdown={
                    "z_mom_20d": round(z_mom20[idx], 4),
                    "z_mom_60d": round(z_mom60[idx], 4),
                    "z_mom_252d": round(z_mom252[idx], 4),
                    "z_low_vol": round(z_low_vol[idx], 4),
                    "z_low_atr": round(z_low_atr[idx], 4),
                    "z_rsi_not_overbought": round(z_rsi[idx], 4),
                    "regime_adjustment": round(regime_adjustment + trend_break_penalty + crowded_theme_penalty, 4),
                },
                raw_factors={
                    "mom_20d": float(row["mom_20d"]),
                    "mom_60d": mom60,
                    "mom_252d": mom252,
                    "hist_vol_20d": float(row["hist_vol_20d"]),
                    "atr_pct": float(row["atr_pct"]),
                    "rsi_14": float(row["rsi_14"]),
                    "sector_group": group,
                    "trend_ok": 1.0 if trend_ok else 0.0,
                    "branch": f"{regime or 'unknown'}_sector_theme_relative_strength",
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
            and float(item.raw_factors.get("mom_60d") or 0.0) > floor
            and float(item.raw_factors.get("mom_252d") or 0.0) > floor
        ][: int(p["max_holdings"])]
        if not selected:
            return {"CASH": 1.0}

        risk = context.get("risk_params") or {}
        max_single = min(float(p["max_single_weight"]), float(risk.get("max_single_position", p["max_single_weight"])))
        max_total = min(float(p["max_total_weight"]), 1.0 - float(p["min_cash_pct"]))
        max_group = min(float(p["max_group_weight"]), max_total)
        shifted = {item.ticker: max(item.score, 0.0) + 0.05 for item in selected}
        total_score = sum(shifted.values())
        raw = {ticker: value / total_score * max_total for ticker, value in shifted.items()}
        capped = {ticker: min(weight, max_single) for ticker, weight in raw.items()}

        group_totals: dict[str, float] = {}
        group_capped: dict[str, float] = {}
        for item in selected:
            group = str(item.raw_factors.get("sector_group") or "unknown")
            proposed = capped.get(item.ticker, 0.0)
            available = max(max_group - group_totals.get(group, 0.0), 0.0)
            final_weight = min(proposed, available)
            group_totals[group] = group_totals.get(group, 0.0) + final_weight
            group_capped[item.ticker] = final_weight

        total = sum(group_capped.values())
        if total > max_total and total > 0:
            group_capped = {ticker: weight * max_total / total for ticker, weight in group_capped.items()}
        out = {ticker: round(weight, 4) for ticker, weight in group_capped.items() if weight > 0}
        out["CASH"] = round(max(1.0 - sum(out.values()), 0.0), 4)
        return out


SECTOR_GROUP_BY_TICKER = {
    "XLK": "tech_growth",
    "AIQ": "tech_growth",
    "CIBR": "tech_growth",
    "BOTZ": "tech_growth",
    "SOXX": "semiconductors",
    "XSD": "semiconductors",
    "PSI": "semiconductors",
    "FTXL": "semiconductors",
    "XLE": "cyclicals",
    "XLI": "cyclicals",
    "XLRE": "real_estate",
}


def _sector_group(ticker: str, row: dict[str, Any]) -> str:
    return str(row.get("sector_group") or SECTOR_GROUP_BY_TICKER.get(ticker, "unknown"))


def _zscore(values: list[float], clip: float) -> list[float]:
    if len(values) < 2:
        return [0.0] * len(values)
    mean = statistics.fmean(values)
    std = statistics.stdev(values)
    if std == 0:
        return [0.0] * len(values)
    return [max(-clip, min(clip, (value - mean) / std)) for value in values]
