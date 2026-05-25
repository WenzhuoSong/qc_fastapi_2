"""Portfolio VaR/CVaR and scenario diagnostics.

This module is read-only. It estimates risk for current and target weights but
does not approve, reject, or mutate execution targets.
"""
from __future__ import annotations

from datetime import date, timedelta
from math import ceil
from typing import Any

from services.market_feature_store import get_market_daily_feature_rows, model_to_feature_dict


DEFAULT_CONFIDENCE_LEVEL = 0.95
DEFAULT_MIN_SAMPLES = 60
DEFAULT_MIN_COVERAGE = 0.80
DEFAULT_LOOKBACK_DAYS = 504


SCENARIO_DEFINITIONS: tuple[dict[str, Any], ...] = (
    {
        "scenario": "spy_minus_3_growth_shock",
        "description": "SPY -3%, QQQ -5%, leveraged growth ETFs hit harder, volatility hedges spike.",
        "shock_returns": {
            "SPY": -0.03,
            "QQQ": -0.05,
            "IWM": -0.04,
            "TQQQ": -0.15,
            "TECL": -0.15,
            "SOXL": -0.18,
            "SPXL": -0.09,
            "SQQQ": 0.12,
            "TECS": 0.12,
            "SOXS": 0.15,
            "UVXY": 0.20,
            "VIXY": 0.14,
            "BSV": 0.002,
            "SGOV": 0.001,
            "CASH": 0.0,
        },
    },
    {
        "scenario": "leveraged_etf_flush",
        "description": "Leveraged long ETFs gap down; inverse/volatility hedges rise.",
        "shock_returns": {
            "SPY": -0.02,
            "QQQ": -0.035,
            "TQQQ": -0.12,
            "TECL": -0.12,
            "SOXL": -0.15,
            "SPXL": -0.08,
            "SQQQ": 0.10,
            "TECS": 0.10,
            "SOXS": 0.12,
            "UVXY": 0.15,
            "VIXY": 0.10,
            "CASH": 0.0,
        },
    },
    {
        "scenario": "uvxy_decay_day",
        "description": "Market flat-to-up while volatility ETPs decay.",
        "shock_returns": {
            "SPY": 0.005,
            "QQQ": 0.008,
            "TQQQ": 0.024,
            "TECL": 0.024,
            "SOXL": 0.030,
            "SPXL": 0.015,
            "SQQQ": -0.024,
            "TECS": -0.024,
            "SOXS": -0.030,
            "UVXY": -0.08,
            "VIXY": -0.05,
            "CASH": 0.0,
        },
    },
)


TICKER_BETA_FALLBACKS = {
    "SPY": 1.0,
    "QQQ": 1.25,
    "IWM": 1.15,
    "RSP": 1.0,
    "XLK": 1.20,
    "XLY": 1.10,
    "XLC": 1.05,
    "XLF": 1.05,
    "XLI": 1.00,
    "XLE": 1.05,
    "XLV": 0.75,
    "XLP": 0.60,
    "XLU": 0.55,
    "XLRE": 0.80,
    "XLB": 1.00,
    "SOXX": 1.45,
    "SMH": 1.45,
    "TQQQ": 3.0,
    "TECL": 3.0,
    "SOXL": 3.5,
    "SPXL": 3.0,
    "SQQQ": -3.0,
    "TECS": -3.0,
    "SOXS": -3.5,
    "UVXY": -5.0,
    "VIXY": -3.0,
    "TLT": -0.30,
    "IEF": -0.15,
    "BND": -0.10,
    "GLD": 0.05,
    "BSV": -0.05,
    "SGOV": 0.0,
}


async def load_portfolio_var_cvar_diagnostic(
    db: Any,
    *,
    target_weights: dict[str, Any],
    current_weights: dict[str, Any] | None = None,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    source: str = "yfinance",
    confidence_level: float = DEFAULT_CONFIDENCE_LEVEL,
    min_samples: int = DEFAULT_MIN_SAMPLES,
) -> dict[str, Any]:
    tickers = sorted(
        {
            ticker
            for weights in (target_weights or {}, current_weights or {})
            for ticker, weight in _clean_weights(weights).items()
            if ticker != "CASH" and weight > 0
        }
    )
    start = date.today() - timedelta(days=max(int(lookback_days * 1.7), lookback_days + 30))
    rows = await get_market_daily_feature_rows(
        db,
        tickers=tickers,
        start_date=start,
        source=source,
        limit=max(len(tickers) * int(lookback_days * 1.5), 100),
    )
    return evaluate_portfolio_var_cvar(
        target_weights=target_weights,
        current_weights=current_weights or {},
        historical_return_rows=[model_to_feature_dict(row) for row in rows],
        lookback_days=lookback_days,
        source=source,
        confidence_level=confidence_level,
        min_samples=min_samples,
    )


def evaluate_portfolio_var_cvar(
    *,
    target_weights: dict[str, Any],
    current_weights: dict[str, Any] | None = None,
    historical_return_rows: list[Any] | None = None,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    source: str = "provided",
    confidence_level: float = DEFAULT_CONFIDENCE_LEVEL,
    min_samples: int = DEFAULT_MIN_SAMPLES,
    min_coverage: float = DEFAULT_MIN_COVERAGE,
) -> dict[str, Any]:
    target = _normalize_cash_first(_clean_weights(target_weights))
    current = _normalize_cash_first(_clean_weights(current_weights or {}))
    rows = historical_return_rows or []

    target_historical = _historical_var_cvar(
        target,
        rows,
        confidence_level=confidence_level,
        min_samples=min_samples,
        min_coverage=min_coverage,
    )
    current_historical = _historical_var_cvar(
        current,
        rows,
        confidence_level=confidence_level,
        min_samples=min_samples,
        min_coverage=min_coverage,
    )
    target_scenarios = _scenario_results(target)
    current_scenarios = _scenario_results(current)
    warnings = _risk_warnings(target_historical, target_scenarios, min_samples=min_samples)
    status = "ok" if target_historical["sample_count"] >= min_samples else "insufficient_data"
    if _equity_weight(target) <= 0:
        status = "cash_only"

    return {
        "contract_version": "portfolio_var_cvar_v1",
        "status": status,
        "mode": "diagnostic_only",
        "execution_authority": "none",
        "target_weight_mutation": "none",
        "confidence_level": confidence_level,
        "lookback_days": lookback_days,
        "source": source,
        "min_samples": min_samples,
        "min_coverage": min_coverage,
        "data_quality": target_historical["data_quality"],
        "target_historical": target_historical,
        "current_historical": current_historical,
        "target_scenarios": target_scenarios,
        "current_scenarios": current_scenarios,
        "summary": {
            "target_var_95_loss": target_historical.get("var_95_loss"),
            "target_cvar_95_loss": target_historical.get("cvar_95_loss"),
            "current_var_95_loss": current_historical.get("var_95_loss"),
            "current_cvar_95_loss": current_historical.get("cvar_95_loss"),
            "max_target_scenario_loss": _max_scenario_loss(target_scenarios),
            "max_current_scenario_loss": _max_scenario_loss(current_scenarios),
            "sample_count": target_historical["sample_count"],
            "covered_date_count": target_historical["covered_date_count"],
        },
        "warnings": warnings,
    }


def _historical_var_cvar(
    weights: dict[str, float],
    rows: list[Any],
    *,
    confidence_level: float,
    min_samples: int,
    min_coverage: float,
) -> dict[str, Any]:
    equity_weight = _equity_weight(weights)
    if equity_weight <= 0:
        return {
            "status": "cash_only",
            "data_quality": "cash_only",
            "sample_count": 0,
            "covered_date_count": 0,
            "skipped_date_count": 0,
            "var_95_return": 0.0,
            "var_95_loss": 0.0,
            "cvar_95_return": 0.0,
            "cvar_95_loss": 0.0,
            "worst_return": 0.0,
            "best_return": 0.0,
            "avg_return": 0.0,
            "tail_count": 0,
        }

    by_date: dict[str, dict[str, float]] = {}
    for row in rows:
        ticker = _record_get(row, "ticker")
        trading_date = _record_get(row, "trading_date")
        return_1d = _to_float(_record_get(row, "return_1d"))
        if not ticker or trading_date is None or return_1d is None:
            continue
        clean_ticker = str(ticker).upper().strip()
        if clean_ticker not in weights or weights.get(clean_ticker, 0.0) <= 0:
            continue
        by_date.setdefault(str(trading_date), {})[clean_ticker] = return_1d

    portfolio_returns: list[float] = []
    skipped = 0
    for date_key in sorted(by_date):
        day_returns = by_date[date_key]
        covered_weight = sum(
            weight
            for ticker, weight in weights.items()
            if ticker != "CASH" and weight > 0 and ticker in day_returns
        )
        coverage = covered_weight / equity_weight if equity_weight > 0 else 0.0
        if coverage < min_coverage:
            skipped += 1
            continue
        portfolio_returns.append(
            sum(
                float(weights.get(ticker, 0.0) or 0.0) * float(day_returns.get(ticker, 0.0) or 0.0)
                for ticker in weights
                if ticker != "CASH"
            )
        )

    sample_count = len(portfolio_returns)
    if sample_count <= 0:
        return {
            "status": "insufficient_data",
            "data_quality": "missing",
            "sample_count": 0,
            "covered_date_count": 0,
            "skipped_date_count": skipped,
            "var_95_return": None,
            "var_95_loss": None,
            "cvar_95_return": None,
            "cvar_95_loss": None,
            "worst_return": None,
            "best_return": None,
            "avg_return": None,
            "tail_count": 0,
        }

    sorted_returns = sorted(portfolio_returns)
    tail_probability = max(min(1.0 - confidence_level, 0.50), 0.001)
    var_index = max(0, min(sample_count - 1, ceil(round(tail_probability * sample_count, 10)) - 1))
    var_return = sorted_returns[var_index]
    tail = [value for value in sorted_returns if value <= var_return + 1e-12]
    cvar_return = sum(tail) / len(tail) if tail else var_return
    data_quality = "historical_supported" if sample_count >= min_samples else "limited"
    status = "ok" if sample_count >= min_samples else "insufficient_data"

    return {
        "status": status,
        "data_quality": data_quality,
        "sample_count": sample_count,
        "covered_date_count": len(by_date),
        "skipped_date_count": skipped,
        "var_95_return": round(var_return, 6),
        "var_95_loss": round(max(0.0, -var_return), 6),
        "cvar_95_return": round(cvar_return, 6),
        "cvar_95_loss": round(max(0.0, -cvar_return), 6),
        "worst_return": round(sorted_returns[0], 6),
        "best_return": round(sorted_returns[-1], 6),
        "avg_return": round(sum(portfolio_returns) / sample_count, 6),
        "tail_count": len(tail),
    }


def _scenario_results(weights: dict[str, float]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for scenario in SCENARIO_DEFINITIONS:
        shock_returns: dict[str, float] = {}
        portfolio_return = 0.0
        for ticker, weight in sorted(weights.items()):
            if weight <= 0:
                continue
            shock = _scenario_ticker_return(ticker, scenario)
            shock_returns[ticker] = round(shock, 6)
            portfolio_return += weight * shock
        rows.append({
            "scenario": scenario["scenario"],
            "description": scenario["description"],
            "portfolio_return": round(portfolio_return, 6),
            "estimated_loss": round(max(0.0, -portfolio_return), 6),
            "shock_returns": shock_returns,
        })
    return rows


def _scenario_ticker_return(ticker: str, scenario: dict[str, Any]) -> float:
    clean = str(ticker or "").upper().strip()
    shocks = scenario.get("shock_returns") or {}
    if clean in shocks:
        return float(shocks[clean])
    if clean == "CASH":
        return 0.0
    beta = TICKER_BETA_FALLBACKS.get(clean, 1.0)
    spy_shock = float(shocks.get("SPY", 0.0) or 0.0)
    return beta * spy_shock


def _risk_warnings(
    historical: dict[str, Any],
    scenario_rows: list[dict[str, Any]],
    *,
    min_samples: int,
) -> list[str]:
    warnings: list[str] = []
    if historical.get("sample_count", 0) < min_samples:
        warnings.append("historical_var_cvar_insufficient_samples")
    if _max_scenario_loss(scenario_rows) > 0.10:
        warnings.append("scenario_loss_exceeds_10pct")
    if historical.get("cvar_95_loss") is not None and float(historical.get("cvar_95_loss") or 0.0) > 0.05:
        warnings.append("historical_cvar_95_loss_exceeds_5pct")
    return warnings


def _max_scenario_loss(rows: list[dict[str, Any]]) -> float | None:
    if not rows:
        return None
    return round(max(float(row.get("estimated_loss") or 0.0) for row in rows), 6)


def _normalize_cash_first(weights: dict[str, float]) -> dict[str, float]:
    clean = _clean_weights(weights)
    equity = sum(value for ticker, value in clean.items() if ticker != "CASH")
    if equity >= 1.0:
        scale = 1.0 / equity if equity > 0 else 0.0
        out = {
            ticker: round(value * scale, 6)
            for ticker, value in clean.items()
            if ticker != "CASH" and value > 1e-12
        }
        out["CASH"] = round(max(1.0 - sum(out.values()), 0.0), 6)
        return out
    out = {
        ticker: round(value, 6)
        for ticker, value in clean.items()
        if ticker != "CASH" and value > 1e-12
    }
    out["CASH"] = round(max(1.0 - sum(out.values()), 0.0), 6)
    return out


def _clean_weights(raw: dict[str, Any] | None) -> dict[str, float]:
    out: dict[str, float] = {}
    for ticker, value in (raw or {}).items():
        clean = str(ticker or "").upper().strip()
        parsed = _to_float(value)
        if clean and parsed is not None:
            out[clean] = max(parsed, 0.0)
    return out


def _equity_weight(weights: dict[str, float]) -> float:
    return sum(float(weight or 0.0) for ticker, weight in weights.items() if ticker != "CASH")


def _record_get(row: Any, key: str) -> Any:
    if isinstance(row, dict):
        return row.get(key)
    return getattr(row, key, None)


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
