"""
Leveraged ETF Momentum Allocator v1.0

Playground-only port of the supplied QuantConnect conditional sector rotation
strategy. It uses a fixed leveraged/inverse ETF universe and a deterministic
RSI/SMA decision tree to select exactly one target asset.
"""
from __future__ import annotations

from typing import Any

from strategies.base import ScoredTicker, Strategy


class LeveragedETFMomentumAllocator(Strategy):
    name = "leveraged_etf_momentum_allocator"
    version = "1.0"
    description = "Conditional leveraged ETF allocator using SPY/QQQ trend and RSI branches"
    required_fields = ("close_price", "sma_20", "sma_200", "rsi_10")
    optional_fields = ("return_1d", "hist_vol_20d", "dollar_volume")
    min_required_coverage = 1.0
    family = "leveraged_rotation"
    core_idea = (
        "Uses SPY 200-day trend as the root regime gate, then chooses among leveraged "
        "long, inverse, volatility, bond, and technology ETFs using short RSI and 20-day SMA checks."
    )
    best_regimes = ("strong_trend", "fast_tactical_rotation", "volatility_spike_reversal")
    bad_regimes = ("choppy_whipsaw", "volatility_decay", "leveraged_etf_decay", "gap_risk")
    signals_used = ("close_price", "sma_20", "sma_200", "rsi_10")
    failure_modes = (
        "Single-asset 100% allocations can create large drawdowns and severe gap risk.",
        "Leveraged and inverse ETF decay can dominate signal quality in volatile sideways regimes.",
        "UVXY and inverse ETF branches are highly path-dependent and may not transfer well outside QC backtests.",
        "Backtest showed very high drawdown despite high CAGR, so this must remain playground-only until separately certified.",
    )
    agent_guidance = (
        "Treat as a playground-only high-risk research signal. Do not use its weights directly for production allocation; "
        "inspect selected_tickers and branch diagnostics as tactical evidence."
    )
    universe_tickers = ("SPY", "QQQ", "TQQQ", "UVXY", "TECL", "SPXL", "SQQQ", "TECS", "BSV")
    allow_hedge_research_tickers = True

    DEFAULT_PARAMS: dict[str, Any] = {
        "target_weight": 1.0,
        "qqq_overbought_rsi": 81.0,
        "spy_overbought_rsi": 80.0,
        "tqqq_oversold_rsi": 30.0,
        "spy_oversold_rsi": 30.0,
        "uvxy_high_rsi": 74.0,
        "uvxy_extreme_rsi": 84.0,
        "sqqq_extreme_branch_rsi": 31.0,
        "sqqq_normal_branch_rsi": 34.0,
    }

    def __init__(self, params: dict[str, Any] | None = None):
        super().__init__({**self.DEFAULT_PARAMS, **(params or {})})

    def eligible_rows(self, holdings: list[dict]) -> list[dict]:
        by_ticker = {
            (row.get("ticker") or "").upper().strip(): row
            for row in holdings
            if (row.get("ticker") or "").upper().strip()
        }
        return [
            by_ticker[ticker]
            for ticker in self.universe_tickers
            if ticker in by_ticker
        ]

    def data_readiness(self, holdings: list[dict]) -> dict[str, Any]:
        rows = self.eligible_rows(holdings)
        by_ticker = {
            (row.get("ticker") or "").upper().strip(): row
            for row in rows
        }
        missing_tickers = [ticker for ticker in self.universe_tickers if ticker not in by_ticker]
        field_coverage: dict[str, float] = {}
        missing_fields: list[str] = []
        for field in self.required_fields:
            covered = sum(1 for ticker in self.universe_tickers if _value(by_ticker.get(ticker), field) is not None)
            coverage = covered / len(self.universe_tickers)
            field_coverage[field] = round(coverage, 4)
            if coverage < 1.0:
                missing_fields.append(field)

        ready = not missing_tickers and not missing_fields
        return {
            "ready": ready,
            "coverage": 1.0 if ready else min(field_coverage.values()) if field_coverage else 0.0,
            "missing_fields": missing_fields,
            "field_coverage": field_coverage,
            "eligible_tickers": [ticker for ticker in self.universe_tickers if ticker in by_ticker],
            "missing_tickers": missing_tickers,
        }

    def score(self, holdings: list[dict], context: dict[str, Any]) -> list[ScoredTicker]:
        rows = {
            (row.get("ticker") or "").upper().strip(): row
            for row in self.eligible_rows(holdings)
        }
        target, branch = self._select_target(rows)
        if not target:
            return []
        scored: list[ScoredTicker] = []
        for ticker in self.universe_tickers:
            row = rows.get(ticker) or {}
            is_target = ticker == target
            scored.append(ScoredTicker(
                ticker=ticker,
                score=1.0 if is_target else 0.0,
                factor_breakdown={
                    "selected": 1.0 if is_target else 0.0,
                    "branch_rank": 1.0 if is_target else 0.0,
                },
                raw_factors={
                    "branch": branch if is_target else None,
                    "close_price": _value(row, "close_price"),
                    "sma_20": _value(row, "sma_20"),
                    "sma_200": _value(row, "sma_200"),
                    "rsi_10": _value(row, "rsi_10"),
                },
            ))
        scored.sort(key=lambda item: item.score, reverse=True)
        return scored

    def optimize(self, scored: list[ScoredTicker], context: dict[str, Any]) -> dict[str, float]:
        if not scored or scored[0].score <= 0:
            return {"CASH": 1.0}
        target = scored[0].ticker
        weight = max(0.0, min(float(self.params.get("target_weight", 1.0)), 1.0))
        return {target: round(weight, 4), "CASH": round(1.0 - weight, 4)}

    def _select_target(self, rows: dict[str, dict]) -> tuple[str | None, str]:
        p = self.params
        price_spy = _value(rows.get("SPY"), "close_price")
        price_qqq = _value(rows.get("QQQ"), "close_price")
        price_tqqq = _value(rows.get("TQQQ"), "close_price")
        sma_spy_200 = _value(rows.get("SPY"), "sma_200")
        sma_qqq_20 = _value(rows.get("QQQ"), "sma_20")
        sma_tqqq_20 = _value(rows.get("TQQQ"), "sma_20")
        rsi_qqq = _value(rows.get("QQQ"), "rsi_10")
        rsi_spy = _value(rows.get("SPY"), "rsi_10")
        rsi_tqqq = _value(rows.get("TQQQ"), "rsi_10")
        rsi_sqqq = _value(rows.get("SQQQ"), "rsi_10")
        rsi_uvxy = _value(rows.get("UVXY"), "rsi_10")
        if None in {
            price_spy, price_qqq, price_tqqq, sma_spy_200, sma_qqq_20, sma_tqqq_20,
            rsi_qqq, rsi_spy, rsi_tqqq, rsi_sqqq, rsi_uvxy,
        }:
            return None, "missing_required_signal"

        if price_spy > sma_spy_200:
            if rsi_qqq > float(p["qqq_overbought_rsi"]):
                return "UVXY", "bull_qqq_overbought_to_uvxy"
            if rsi_spy > float(p["spy_overbought_rsi"]):
                return "UVXY", "bull_spy_overbought_to_uvxy"
            return "TQQQ", "bull_trend_to_tqqq"

        if rsi_tqqq < float(p["tqqq_oversold_rsi"]):
            return "TECL", "bear_tqqq_oversold_to_tecl"
        if rsi_spy < float(p["spy_oversold_rsi"]):
            return "SPXL", "bear_spy_oversold_to_spxl"
        if rsi_uvxy > float(p["uvxy_high_rsi"]):
            if rsi_uvxy > float(p["uvxy_extreme_rsi"]):
                if price_qqq > sma_qqq_20:
                    if rsi_sqqq < float(p["sqqq_extreme_branch_rsi"]):
                        return "TECS", "uvxy_extreme_qqq_above_sma_sqqq_oversold_to_tecs"
                    return "TECL", "uvxy_extreme_qqq_above_sma_to_tecl"
                return self._max_rsi_asset(rows, ["TECS", "BSV"], "uvxy_extreme_qqq_below_sma")
            return "UVXY", "uvxy_high_to_uvxy"

        if price_tqqq > sma_tqqq_20:
            if rsi_sqqq < float(p["sqqq_normal_branch_rsi"]):
                return "TECS", "tqqq_above_sma_sqqq_oversold_to_tecs"
            return "TECL", "tqqq_above_sma_to_tecl"
        return self._max_rsi_asset(rows, ["TECS", "BSV"], "tqqq_below_sma")

    def _max_rsi_asset(self, rows: dict[str, dict], tickers: list[str], branch: str) -> tuple[str | None, str]:
        best_ticker = None
        best_rsi = None
        for ticker in tickers:
            rsi = _value(rows.get(ticker), "rsi_10")
            if rsi is None:
                continue
            if best_rsi is None or rsi > best_rsi:
                best_rsi = rsi
                best_ticker = ticker
        return best_ticker, f"{branch}_max_rsi_{best_ticker.lower() if best_ticker else 'missing'}"


def _value(row: dict | None, field: str) -> float | None:
    if not row:
        return None
    value = row.get(field)
    if value is None and field == "close_price":
        value = row.get("price")
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None
