"""RelativeValueReversionLite.

Long-only relative-value mean reversion across broad equity ETFs. It looks for
short-term relative underperformance that may revert when the medium-term trend
is not badly broken.
"""
from __future__ import annotations

import logging
import statistics
from typing import Any

from strategies.base import ScoredTicker, Strategy

logger = logging.getLogger("qc_fastapi_2.strategy.relative_value_reversion_lite")


class RelativeValueReversionLite(Strategy):
    name = "relative_value_reversion_lite"
    version = "1.0"
    description = "Broad-market relative-value mean-reversion strategy"
    required_fields = ("return_5d", "mom_20d", "mom_60d", "hist_vol_20d", "rsi_14")
    optional_fields = ("atr_pct", "bb_position")
    family = "mean_reversion"
    core_idea = "Favors broad-market ETFs that underperformed peers over 5-20 days while their 60-day trend and volatility remain acceptable."
    best_regimes = ("mean_reverting", "range_bound", "risk_on_chop")
    bad_regimes = ("trending_bull", "crash_breakdown", "persistent_trend")
    signals_used = ("return_5d", "mom_20d", "mom_60d", "hist_vol_20d", "rsi_14")
    failure_modes = (
        "Can buy early in a trend breakdown when relative weakness keeps extending.",
        "Can underperform momentum during persistent leadership regimes.",
        "Small ETF universe can make relative ranks noisy.",
    )
    agent_guidance = "Use as a small tactical reversion sleeve only when regime evidence is choppy or range-bound; discount it in strong trend regimes."
    universe_tickers = ("SPY", "QQQ", "IWM")

    DEFAULT_PARAMS: dict[str, Any] = {
        "w_return_5d_reversal": 0.35,
        "w_mom_20d_reversal": 0.25,
        "w_trend_60d": 0.20,
        "w_low_vol": 0.15,
        "w_rsi_mid": 0.05,
        "zscore_clip": 3.0,
        "max_holdings": 2,
        "max_single_weight": 0.05,
        "max_total_weight": 0.15,
        "min_cash_pct": 0.85,
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
            logger.warning("relative_value_reversion_lite: no tickers with complete factor data")
            return []

        p = self.params
        clip = float(p["zscore_clip"])
        z_return5_reversal = _zscore([-float(row["return_5d"]) for row in valid], clip)
        z_mom20_reversal = _zscore([-float(row["mom_20d"]) for row in valid], clip)
        z_mom60 = _zscore([float(row["mom_60d"]) for row in valid], clip)
        z_low_vol = _zscore([-float(row["hist_vol_20d"]) for row in valid], clip)
        z_rsi_mid = _zscore([-abs(float(row["rsi_14"]) - 50.0) for row in valid], clip)

        regime = str(context.get("regime") or "")
        regime_adjustment = 0.15 if regime in {"mean_reverting", "range_bound"} else 0.0
        if regime in {"trending_bull", "risk_on"}:
            regime_adjustment -= 0.10
        if regime in {"high_vol", "defensive", "risk_off"}:
            regime_adjustment -= 0.20

        scored: list[ScoredTicker] = []
        for idx, row in enumerate(valid):
            trend_penalty = -0.30 if float(row["mom_60d"]) < -0.08 else 0.0
            high_vol_penalty = -0.15 if float(row["hist_vol_20d"]) > 0.45 else 0.0
            score = (
                float(p["w_return_5d_reversal"]) * z_return5_reversal[idx]
                + float(p["w_mom_20d_reversal"]) * z_mom20_reversal[idx]
                + float(p["w_trend_60d"]) * z_mom60[idx]
                + float(p["w_low_vol"]) * z_low_vol[idx]
                + float(p["w_rsi_mid"]) * z_rsi_mid[idx]
                + regime_adjustment
                + trend_penalty
                + high_vol_penalty
            )
            ticker = str(row["ticker"]).upper().strip()
            scored.append(ScoredTicker(
                ticker=ticker,
                score=score,
                factor_breakdown={
                    "z_return_5d_reversal": round(z_return5_reversal[idx], 4),
                    "z_mom_20d_reversal": round(z_mom20_reversal[idx], 4),
                    "z_mom_60d": round(z_mom60[idx], 4),
                    "z_low_vol": round(z_low_vol[idx], 4),
                    "z_rsi_mid": round(z_rsi_mid[idx], 4),
                    "regime_adjustment": round(regime_adjustment + trend_penalty + high_vol_penalty, 4),
                },
                raw_factors={
                    "return_5d": float(row["return_5d"]),
                    "mom_20d": float(row["mom_20d"]),
                    "mom_60d": float(row["mom_60d"]),
                    "hist_vol_20d": float(row["hist_vol_20d"]),
                    "rsi_14": float(row["rsi_14"]),
                    "branch": f"{regime or 'unknown'}_relative_value_reversion",
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
