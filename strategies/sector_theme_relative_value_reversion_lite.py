"""SectorThemeRelativeValueReversionLite.

Cluster-relative mean-reversion strategy across sector and thematic ETFs. It
looks for short-term underperformance within a related ETF group while the
medium-term trend remains intact.
"""
from __future__ import annotations

import logging
import statistics
from typing import Any

from strategies.base import ScoredTicker, Strategy

logger = logging.getLogger("qc_fastapi_2.strategy.sector_theme_relative_value_reversion_lite")


class SectorThemeRelativeValueReversionLite(Strategy):
    name = "sector_theme_relative_value_reversion_lite"
    version = "1.0"
    description = "Cluster-relative mean reversion across sector and thematic ETFs"
    required_fields = ("return_5d", "mom_20d", "mom_60d", "hist_vol_20d", "atr_pct", "rsi_14")
    optional_fields = ("sector_group", "bb_position", "mom_252d")
    family = "mean_reversion"
    core_idea = "Finds sector/theme ETFs that sold off versus close peers over the last week while their medium-term trend and volatility remain acceptable."
    best_regimes = ("mean_reverting", "risk_on_chop", "sector_rotation", "range_bound")
    bad_regimes = ("high_vol", "risk_off", "defensive", "violent_whipsaw", "persistent_trend")
    signals_used = ("return_5d", "mom_20d", "mom_60d", "hist_vol_20d", "atr_pct", "rsi_14", "sector_group")
    failure_modes = (
        "Can catch falling themes if short-term weakness is the start of a real breakdown.",
        "Can underperform relative strength during persistent leadership regimes.",
        "Highly related thematic ETFs can share hidden macro or factor risk.",
    )
    agent_guidance = "Use as a small tactical reversion lens for related sector/theme ETFs. It complements relative strength and should be discounted in high-volatility or persistent-trend regimes."
    universe_tickers = ("XLK", "QQQ", "SOXX", "XSD", "PSI", "FTXL", "AIQ", "CIBR", "BOTZ", "XLE", "XLI", "XLRE")

    DEFAULT_PARAMS: dict[str, Any] = {
        "w_group_return_5d_reversal": 0.40,
        "w_group_mom_20d_reversal": 0.20,
        "w_trend_60d": 0.18,
        "w_low_atr": 0.10,
        "w_low_vol": 0.07,
        "w_rsi_reversion": 0.05,
        "zscore_clip": 3.0,
        "score_threshold": 0.55,
        "max_holdings": 3,
        "max_single_weight": 0.035,
        "max_total_weight": 0.12,
        "max_group_weight": 0.07,
        "min_cash_pct": 0.88,
        "min_mom_20d": -0.03,
        "min_mom_60d": 0.0,
        "max_atr_pct": 0.04,
        "max_rsi": 68.0,
        "min_rsi": 32.0,
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
            logger.warning("sector_theme_relative_value_reversion_lite: no tickers with complete factor data")
            return []

        p = self.params
        clip = float(p["zscore_clip"])
        group_stats = _group_stats(valid)
        group_return_reversal = [
            _relative_weakness(row, group_stats, field="return_5d")
            for row in valid
        ]
        group_mom20_reversal = [
            _relative_weakness(row, group_stats, field="mom_20d")
            for row in valid
        ]
        z_group_return = _zscore(group_return_reversal, clip)
        z_group_mom20 = _zscore(group_mom20_reversal, clip)
        z_mom60 = _zscore([float(row["mom_60d"]) for row in valid], clip)
        z_low_atr = _zscore([-float(row["atr_pct"]) for row in valid], clip)
        z_low_vol = _zscore([-float(row["hist_vol_20d"]) for row in valid], clip)
        z_rsi_reversion = _zscore([-abs(float(row["rsi_14"]) - 45.0) for row in valid], clip)

        regime = str(context.get("regime") or "")
        regime_adjustment = _regime_adjustment(regime)
        scored: list[ScoredTicker] = []
        for idx, row in enumerate(valid):
            ticker = str(row["ticker"]).upper().strip()
            group = _sector_group(ticker, row)
            mom20 = float(row["mom_20d"])
            mom60 = float(row["mom_60d"])
            atr = float(row["atr_pct"])
            rsi = float(row["rsi_14"])
            trend_ok = mom20 >= float(p["min_mom_20d"]) and mom60 >= float(p["min_mom_60d"])
            trend_penalty = 0.0 if trend_ok else -0.35
            atr_penalty = -0.20 if atr > float(p["max_atr_pct"]) else 0.0
            rsi_penalty = -0.15 if rsi < float(p["min_rsi"]) or rsi > float(p["max_rsi"]) else 0.0
            score = _clamp(
                0.50
                + float(p["w_group_return_5d_reversal"]) * z_group_return[idx] / 3.0
                + float(p["w_group_mom_20d_reversal"]) * z_group_mom20[idx] / 3.0
                + float(p["w_trend_60d"]) * z_mom60[idx] / 3.0
                + float(p["w_low_atr"]) * z_low_atr[idx] / 3.0
                + float(p["w_low_vol"]) * z_low_vol[idx] / 3.0
                + float(p["w_rsi_reversion"]) * z_rsi_reversion[idx] / 3.0
                + regime_adjustment
                + trend_penalty
                + atr_penalty
                + rsi_penalty
            )
            scored.append(ScoredTicker(
                ticker=ticker,
                score=score,
                factor_breakdown={
                    "z_group_return_5d_reversal": round(z_group_return[idx], 4),
                    "z_group_mom_20d_reversal": round(z_group_mom20[idx], 4),
                    "z_mom_60d": round(z_mom60[idx], 4),
                    "z_low_atr": round(z_low_atr[idx], 4),
                    "z_low_vol": round(z_low_vol[idx], 4),
                    "z_rsi_reversion": round(z_rsi_reversion[idx], 4),
                    "regime_adjustment": round(regime_adjustment + trend_penalty + atr_penalty + rsi_penalty, 4),
                },
                raw_factors={
                    "return_5d": float(row["return_5d"]),
                    "mom_20d": mom20,
                    "mom_60d": mom60,
                    "hist_vol_20d": float(row["hist_vol_20d"]),
                    "atr_pct": atr,
                    "rsi_14": rsi,
                    "sector_group": group,
                    "group_return_5d_reversal": group_return_reversal[idx],
                    "group_mom_20d_reversal": group_mom20_reversal[idx],
                    "trend_ok": 1.0 if trend_ok else 0.0,
                    "branch": f"{regime or 'unknown'}_{group}_relative_value_reversion",
                },
            ))

        scored.sort(key=lambda item: item.score, reverse=True)
        return scored

    def optimize(self, scored: list[ScoredTicker], context: dict[str, Any]) -> dict[str, float]:
        if not scored:
            return {"CASH": 1.0}

        p = self.params
        regime = str(context.get("regime") or "")
        if regime in {"high_vol", "defensive", "risk_off", "crash_breakdown", "violent_whipsaw"}:
            return {"CASH": 1.0}

        selected = [
            item for item in scored
            if item.score >= float(p["score_threshold"])
            and float(item.raw_factors.get("trend_ok") or 0.0) >= 1.0
            and float(item.raw_factors.get("group_return_5d_reversal") or 0.0) > 0
            and float(item.raw_factors.get("atr_pct") or 0.0) <= float(p["max_atr_pct"])
            and float(p["min_rsi"]) <= float(item.raw_factors.get("rsi_14") or 100.0) <= float(p["max_rsi"])
        ][: int(p["max_holdings"])]
        if not selected:
            return {"CASH": 1.0}

        risk = context.get("risk_params") or {}
        max_single = min(float(p["max_single_weight"]), float(risk.get("max_single_position", p["max_single_weight"])))
        max_total = min(float(p["max_total_weight"]), 1.0 - float(p["min_cash_pct"]))
        max_group = min(float(p["max_group_weight"]), max_total)
        shifted = {item.ticker: max(item.score, 0.0) for item in selected}
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
    "QQQ": "tech_growth",
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


def _group_stats(rows: list[dict[str, Any]]) -> dict[str, dict[str, float]]:
    groups: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        ticker = str(row.get("ticker") or "").upper().strip()
        groups.setdefault(_sector_group(ticker, row), []).append(row)
    overall = {
        "return_5d": statistics.fmean(float(row["return_5d"]) for row in rows),
        "mom_20d": statistics.fmean(float(row["mom_20d"]) for row in rows),
    }
    out: dict[str, dict[str, float]] = {}
    for group, group_rows in groups.items():
        base_rows = group_rows if len(group_rows) >= 2 else rows
        out[group] = {
            "return_5d": statistics.fmean(float(row["return_5d"]) for row in base_rows),
            "mom_20d": statistics.fmean(float(row["mom_20d"]) for row in base_rows),
        }
    out["__overall__"] = overall
    return out


def _relative_weakness(row: dict[str, Any], group_stats: dict[str, dict[str, float]], *, field: str) -> float:
    ticker = str(row.get("ticker") or "").upper().strip()
    group = _sector_group(ticker, row)
    base = group_stats.get(group) or group_stats["__overall__"]
    return float(base.get(field, 0.0)) - float(row[field])


def _regime_adjustment(regime: str) -> float:
    if regime in {"mean_reverting", "range_bound", "risk_on_chop", "sector_rotation"}:
        return 0.12
    if regime in {"risk_on", "trending_bull"}:
        return 0.02
    if regime in {"high_vol", "defensive", "risk_off", "crash_breakdown", "violent_whipsaw"}:
        return -0.30
    return 0.0


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
