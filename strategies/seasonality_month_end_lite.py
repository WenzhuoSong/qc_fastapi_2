"""SeasonalityMonthEndLite.

Small capped turn-of-month structural-flow strategy. It scores ordinary broad
equity ETFs only when the calendar window is supportive and fails back to CASH
outside that window.
"""
from __future__ import annotations

import calendar
import logging
from datetime import date, datetime
from typing import Any

from strategies.base import ScoredTicker, Strategy

logger = logging.getLogger("qc_fastapi_2.strategy.seasonality_month_end_lite")


class SeasonalityMonthEndLite(Strategy):
    name = "seasonality_month_end_lite"
    version = "1.0"
    description = "Capped turn-of-month seasonality and structural-flow strategy"
    required_fields = ("mom_20d", "mom_60d", "hist_vol_20d", "atr_pct")
    optional_fields = ("return_5d", "rsi_14")
    family = "seasonality_flow"
    core_idea = "Uses a deterministic turn-of-month window as a small, liquid broad-market structural-flow sleeve when trend and volatility filters are acceptable."
    best_regimes = ("risk_on", "trending_bull", "mean_reverting")
    bad_regimes = ("high_vol", "defensive", "acute_risk_off")
    signals_used = ("signal_date", "mom_20d", "mom_60d", "hist_vol_20d", "atr_pct")
    failure_modes = (
        "Calendar effects can disappear or invert around macro shocks.",
        "Exact calendar windows are vulnerable to overfitting.",
        "A small broad-market universe can make the signal look more stable than it is.",
    )
    agent_guidance = "Use as an independent structural-flow candidate, not as another momentum signal. Keep the sleeve small and require conviction before promotion."
    universe_tickers = ("SPY", "QQQ", "IWM")

    DEFAULT_PARAMS: dict[str, Any] = {
        "w_calendar": 0.50,
        "w_momentum_quality": 0.25,
        "w_low_vol": 0.15,
        "w_low_atr": 0.10,
        "min_calendar_score_to_trade": 0.75,
        "min_score_to_trade": 0.55,
        "max_holdings": 2,
        "max_single_weight": 0.04,
        "max_total_weight": 0.12,
        "min_cash_pct": 0.88,
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
            logger.warning("seasonality_month_end_lite: no tickers with complete factor data")
            return []

        signal_date = _context_date(context)
        calendar_score, calendar_branch = _turn_of_month_score(signal_date)
        regime = str(context.get("regime") or "")
        regime_adjustment = 0.05 if regime in {"risk_on", "trending_bull", "mean_reverting"} else 0.0
        if regime in {"high_vol", "defensive", "acute_risk_off"}:
            regime_adjustment -= 0.20

        p = self.params
        scored: list[ScoredTicker] = []
        for row in valid:
            ticker = str(row["ticker"]).upper().strip()
            momentum_quality = _momentum_quality(float(row["mom_20d"]), float(row["mom_60d"]))
            low_vol_quality = _low_risk_quality(float(row["hist_vol_20d"]), ceiling=0.35)
            low_atr_quality = _low_risk_quality(float(row["atr_pct"]), ceiling=0.035)
            trend_break_penalty = -0.15 if float(row["mom_60d"]) < -0.05 else 0.0
            score = _clamp(
                float(p["w_calendar"]) * calendar_score
                + float(p["w_momentum_quality"]) * momentum_quality
                + float(p["w_low_vol"]) * low_vol_quality
                + float(p["w_low_atr"]) * low_atr_quality
                + regime_adjustment
                + trend_break_penalty
            )
            scored.append(ScoredTicker(
                ticker=ticker,
                score=score,
                factor_breakdown={
                    "calendar_score": round(calendar_score, 4),
                    "momentum_quality": round(momentum_quality, 4),
                    "low_vol_quality": round(low_vol_quality, 4),
                    "low_atr_quality": round(low_atr_quality, 4),
                    "regime_adjustment": round(regime_adjustment + trend_break_penalty, 4),
                },
                raw_factors={
                    "signal_date": signal_date.isoformat(),
                    "calendar_branch": calendar_branch,
                    "calendar_score": round(calendar_score, 4),
                    "mom_20d": float(row["mom_20d"]),
                    "mom_60d": float(row["mom_60d"]),
                    "hist_vol_20d": float(row["hist_vol_20d"]),
                    "atr_pct": float(row["atr_pct"]),
                    "branch": f"{calendar_branch}_{regime or 'unknown'}",
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
            if item.score >= float(p["min_score_to_trade"])
            and float(item.raw_factors.get("calendar_score") or 0.0) >= float(p["min_calendar_score_to_trade"])
            and float(item.raw_factors.get("mom_60d") or 0.0) > -0.05
        ][: int(p["max_holdings"])]
        if not selected:
            return {"CASH": 1.0}

        risk = context.get("risk_params") or {}
        max_single = min(float(p["max_single_weight"]), float(risk.get("max_single_position", p["max_single_weight"])))
        max_total = min(float(p["max_total_weight"]), 1.0 - float(p["min_cash_pct"]))
        shifted = {item.ticker: max(item.score, 0.0) + 0.01 for item in selected}
        total_score = sum(shifted.values())
        raw = {ticker: value / total_score * max_total for ticker, value in shifted.items()}
        capped = {ticker: min(weight, max_single) for ticker, weight in raw.items()}
        total = sum(capped.values())
        if total > max_total and total > 0:
            capped = {ticker: weight * max_total / total for ticker, weight in capped.items()}
        out = {ticker: round(weight, 4) for ticker, weight in capped.items() if weight > 0}
        out["CASH"] = round(max(1.0 - sum(out.values()), 0.0), 4)
        return out


def _context_date(context: dict[str, Any]) -> date:
    for key in ("signal_date", "as_of_date", "trade_date", "date"):
        parsed = _to_date(context.get(key))
        if parsed is not None:
            return parsed
    return datetime.utcnow().date()


def _turn_of_month_score(value: date) -> tuple[float, str]:
    last_day = calendar.monthrange(value.year, value.month)[1]
    days_to_month_end = last_day - value.day
    if days_to_month_end <= 2:
        return 1.0, "month_end_flow"
    if value.day <= 3:
        return 0.85, "new_month_flow"
    if days_to_month_end <= 4 or value.day <= 5:
        return 0.50, "soft_turn_of_month_watch"
    return 0.0, "outside_turn_of_month"


def _momentum_quality(mom_20d: float, mom_60d: float) -> float:
    return _clamp(0.50 + 2.0 * mom_20d + 1.5 * mom_60d)


def _low_risk_quality(value: float, *, ceiling: float) -> float:
    if ceiling <= 0:
        return 0.0
    return _clamp(1.0 - max(value, 0.0) / ceiling)


def _to_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str) and value.strip():
        try:
            return datetime.fromisoformat(value.strip().replace("Z", "+00:00")).date()
        except ValueError:
            return None
    return None


def _clamp(value: float) -> float:
    return max(0.0, min(1.0, float(value)))
