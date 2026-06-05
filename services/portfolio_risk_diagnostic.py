"""Portfolio VaR/CVaR and scenario diagnostics.

This module is read-only. It estimates risk for current and target weights but
does not approve, reject, or mutate execution targets.
"""
from __future__ import annotations

from datetime import date, timedelta
from math import ceil
from typing import Any

from services.execution_policy import TickerRole, get_role
from services.market_feature_store import get_market_daily_feature_rows, model_to_feature_dict
from services.weight_ops import normalize_cash_first


DEFAULT_CONFIDENCE_LEVEL = 0.95
DEFAULT_MIN_SAMPLES = 60
DEFAULT_MIN_COVERAGE = 0.80
DEFAULT_LOOKBACK_DAYS = 504
SCENARIO_STRESS_REPORT_VERSION = "scenario_stress_v1"
BETA_SHOCK_REPORT_VERSION = "beta_shock_v1"


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


HISTORICAL_SCENARIO_DEFINITIONS: tuple[dict[str, Any], ...] = (
    {
        "scenario": "covid_crash_2020_03",
        "description": "COVID crash proxy: broad equity selloff, energy stress, Treasuries and inverse hedges rally.",
        "shock_returns": {
            "SPY": -0.124,
            "QQQ": -0.105,
            "IWM": -0.180,
            "RSP": -0.135,
            "XLK": -0.120,
            "XLF": -0.220,
            "XLE": -0.300,
            "XLV": -0.085,
            "XLI": -0.150,
            "XLY": -0.135,
            "XLP": -0.070,
            "XLU": -0.080,
            "XLRE": -0.150,
            "XLB": -0.145,
            "XLC": -0.110,
            "SOXX": -0.160,
            "SMH": -0.160,
            "PSI": -0.160,
            "FTXL": -0.160,
            "XSD": -0.180,
            "TLT": 0.070,
            "IEF": 0.035,
            "BND": 0.020,
            "SGOV": 0.001,
            "GLD": 0.020,
            "SH": 0.124,
            "PSQ": 0.105,
            "RWM": 0.180,
            "DOG": 0.095,
            "MYY": 0.130,
            "SBB": 0.115,
            "UVXY": 0.600,
            "VIXY": 0.350,
            "CASH": 0.0,
        },
    },
    {
        "scenario": "rate_shock_2022",
        "description": "2022 rate shock proxy: growth and duration assets sell off while energy holds up.",
        "shock_returns": {
            "SPY": -0.080,
            "QQQ": -0.140,
            "IWM": -0.100,
            "RSP": -0.075,
            "XLK": -0.160,
            "XLF": -0.050,
            "XLE": 0.050,
            "XLV": -0.030,
            "XLI": -0.060,
            "XLY": -0.120,
            "XLP": -0.015,
            "XLU": -0.040,
            "XLRE": -0.120,
            "XLB": -0.060,
            "XLC": -0.130,
            "SOXX": -0.180,
            "SMH": -0.180,
            "PSI": -0.160,
            "FTXL": -0.160,
            "XSD": -0.160,
            "TLT": -0.120,
            "IEF": -0.060,
            "BND": -0.050,
            "SGOV": 0.001,
            "GLD": -0.040,
            "SH": 0.080,
            "PSQ": 0.140,
            "RWM": 0.100,
            "TBF": 0.060,
            "TBX": 0.055,
            "UVXY": 0.180,
            "VIXY": 0.100,
            "CASH": 0.0,
        },
    },
    {
        "scenario": "q4_selloff_2018",
        "description": "2018 Q4 selloff proxy: equity risk-off with small caps and semis hit harder.",
        "shock_returns": {
            "SPY": -0.090,
            "QQQ": -0.110,
            "IWM": -0.140,
            "RSP": -0.095,
            "XLK": -0.120,
            "XLF": -0.100,
            "XLE": -0.130,
            "XLV": -0.060,
            "XLI": -0.115,
            "XLY": -0.110,
            "XLP": -0.030,
            "XLU": 0.020,
            "XLRE": -0.060,
            "XLB": -0.110,
            "XLC": -0.100,
            "SOXX": -0.150,
            "SMH": -0.150,
            "PSI": -0.140,
            "FTXL": -0.140,
            "XSD": -0.160,
            "TLT": 0.040,
            "IEF": 0.020,
            "BND": 0.015,
            "SGOV": 0.001,
            "GLD": 0.025,
            "SH": 0.090,
            "PSQ": 0.110,
            "RWM": 0.140,
            "UVXY": 0.250,
            "VIXY": 0.160,
            "CASH": 0.0,
        },
    },
    {
        "scenario": "tech_rebound_2023",
        "description": "2023 tech rebound proxy: growth and semis lead, inverse hedges drag.",
        "shock_returns": {
            "SPY": 0.060,
            "QQQ": 0.120,
            "IWM": 0.050,
            "RSP": 0.045,
            "XLK": 0.130,
            "XLF": 0.030,
            "XLE": -0.030,
            "XLV": 0.020,
            "XLI": 0.040,
            "XLY": 0.080,
            "XLP": 0.010,
            "XLU": -0.020,
            "XLRE": 0.020,
            "XLB": 0.030,
            "XLC": 0.090,
            "SOXX": 0.160,
            "SMH": 0.160,
            "PSI": 0.160,
            "FTXL": 0.160,
            "XSD": 0.130,
            "TLT": 0.020,
            "IEF": 0.010,
            "BND": 0.008,
            "SGOV": 0.001,
            "GLD": 0.000,
            "SH": -0.060,
            "PSQ": -0.120,
            "RWM": -0.050,
            "UVXY": -0.180,
            "VIXY": -0.100,
            "CASH": 0.0,
        },
    },
)


ROLE_SHOCK_DEFINITIONS: tuple[dict[str, Any], ...] = (
    {
        "shock_name": "core_role_minus_10pct",
        "role": TickerRole.CORE.value,
        "shock_return": -0.10,
        "description": "Core broad-market sleeve shock.",
    },
    {
        "shock_name": "sector_role_minus_12pct",
        "role": TickerRole.SECTOR.value,
        "shock_return": -0.12,
        "description": "Sector sleeve shock.",
    },
    {
        "shock_name": "thematic_role_minus_15pct",
        "role": TickerRole.THEMATIC.value,
        "shock_return": -0.15,
        "description": "Thematic/high-beta sleeve shock.",
    },
    {
        "shock_name": "satellite_role_minus_6pct",
        "role": TickerRole.SATELLITE.value,
        "shock_return": -0.06,
        "description": "Satellite diversifier shock.",
    },
    {
        "shock_name": "hedge_role_minus_10pct",
        "role": TickerRole.HEDGE.value,
        "shock_return": -0.10,
        "description": "Hedge sleeve adverse move shock.",
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
    "SH": -1.0,
    "PSQ": -1.25,
    "RWM": -1.15,
    "DOG": -0.90,
    "MYY": -1.10,
    "SBB": -0.80,
    "SEF": -0.70,
    "REK": -0.50,
    "EUM": -0.80,
    "EFZ": -0.70,
    "YXI": -0.50,
    "SJB": 0.20,
    "TBF": 0.25,
    "TBX": 0.20,
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
    target, _ = normalize_cash_first(_clean_weights(target_weights))
    current, _ = normalize_cash_first(_clean_weights(current_weights or {}))
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
    target_scenario_stress = build_scenario_stress_report(target)
    current_scenario_stress = build_scenario_stress_report(current)
    target_beta_shock = build_beta_shock_report(target)
    current_beta_shock = build_beta_shock_report(current)
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
        "target_scenario_stress": target_scenario_stress,
        "current_scenario_stress": current_scenario_stress,
        "target_beta_shock": target_beta_shock,
        "current_beta_shock": current_beta_shock,
        "summary": {
            "target_var_95_loss": target_historical.get("var_95_loss"),
            "target_cvar_95_loss": target_historical.get("cvar_95_loss"),
            "current_var_95_loss": current_historical.get("var_95_loss"),
            "current_cvar_95_loss": current_historical.get("cvar_95_loss"),
            "max_target_scenario_loss": _max_scenario_loss(target_scenarios),
            "max_current_scenario_loss": _max_scenario_loss(current_scenarios),
            "max_target_historical_scenario_loss": _max_scenario_loss(
                target_scenario_stress.get("scenarios") or []
            ),
            "max_current_historical_scenario_loss": _max_scenario_loss(
                current_scenario_stress.get("scenarios") or []
            ),
            "max_target_beta_shock_loss": _max_beta_shock_loss(target_beta_shock),
            "max_current_beta_shock_loss": _max_beta_shock_loss(current_beta_shock),
            "sample_count": target_historical["sample_count"],
            "covered_date_count": target_historical["covered_date_count"],
        },
        "warnings": warnings,
    }


def build_scenario_stress_report(
    weights: dict[str, Any],
    *,
    scenario_definitions: tuple[dict[str, Any], ...] = HISTORICAL_SCENARIO_DEFINITIONS,
) -> dict[str, Any]:
    """Build deterministic historical-window stress diagnostics.

    This is scenario analysis only: no covariance matrix, no CVaR estimation,
    no execution approval, and no target mutation.
    """
    clean, _ = normalize_cash_first(_clean_weights(weights))
    scenarios = [_scenario_stress_result(clean, scenario) for scenario in scenario_definitions]
    return {
        "report_version": SCENARIO_STRESS_REPORT_VERSION,
        "execution_authority": "none",
        "target_weight_mutation": "none",
        "method": "deterministic_historical_window_proxy",
        "current_weights": clean,
        "scenarios": scenarios,
        "summary": {
            "scenario_count": len(scenarios),
            "max_estimated_loss": _max_scenario_loss(scenarios),
            "worst_scenario": _worst_scenario_name(scenarios),
        },
    }


def build_beta_shock_report(weights: dict[str, Any]) -> dict[str, Any]:
    """Build simple SPY/QQQ/role shock diagnostics without covariance estimation."""
    clean, _ = normalize_cash_first(_clean_weights(weights))
    spy_shocks = [
        _beta_shock_result(clean, reference="SPY", shock_return=shock)
        for shock in (-0.10, -0.20, -0.30)
    ]
    qqq_shocks = [
        _beta_shock_result(clean, reference="QQQ", shock_return=shock)
        for shock in (-0.10, -0.20, -0.30)
    ]
    role_shocks = [_role_shock_result(clean, definition) for definition in ROLE_SHOCK_DEFINITIONS]
    return {
        "report_version": BETA_SHOCK_REPORT_VERSION,
        "execution_authority": "none",
        "target_weight_mutation": "none",
        "method": "deterministic_beta_and_role_shocks",
        "current_weights": clean,
        "spy_shocks": spy_shocks,
        "qqq_shocks": qqq_shocks,
        "role_shocks": role_shocks,
        "summary": {
            "max_estimated_loss": _max_beta_shock_loss(
                {
                    "spy_shocks": spy_shocks,
                    "qqq_shocks": qqq_shocks,
                    "role_shocks": role_shocks,
                }
            ),
            "worst_shock": _worst_beta_shock_name(
                {
                    "spy_shocks": spy_shocks,
                    "qqq_shocks": qqq_shocks,
                    "role_shocks": role_shocks,
                }
            ),
        },
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


def _scenario_stress_result(weights: dict[str, float], scenario: dict[str, Any]) -> dict[str, Any]:
    shock_returns: dict[str, float] = {}
    contributors: list[dict[str, Any]] = []
    portfolio_return = 0.0
    for ticker, weight in sorted(weights.items()):
        if weight <= 0:
            continue
        shock = _scenario_ticker_return(ticker, scenario)
        contribution = weight * shock
        shock_returns[ticker] = round(shock, 6)
        portfolio_return += contribution
        if ticker != "CASH":
            contributors.append(_contributor_row(ticker, weight, shock, contribution))

    spy_return = float((scenario.get("shock_returns") or {}).get("SPY", 0.0) or 0.0)
    top_loss = _top_loss_contributors(contributors)
    top_gain = _top_gain_contributors(contributors)
    return {
        "scenario": scenario["scenario"],
        "name": scenario["scenario"],
        "description": scenario["description"],
        "portfolio_return": round(portfolio_return, 6),
        "spy_return": round(spy_return, 6),
        "relative_return": round(portfolio_return - spy_return, 6),
        "estimated_loss": round(max(0.0, -portfolio_return), 6),
        "top_loss_contributors": top_loss,
        "top_gain_contributors": top_gain,
        "top_loss_summary": _contributor_summary(top_loss),
        "shock_returns": shock_returns,
    }


def _beta_shock_result(
    weights: dict[str, float],
    *,
    reference: str,
    shock_return: float,
) -> dict[str, Any]:
    reference_clean = str(reference or "SPY").upper().strip()
    shock_returns: dict[str, float] = {}
    contributors: list[dict[str, Any]] = []
    portfolio_return = 0.0
    for ticker, weight in sorted(weights.items()):
        if weight <= 0:
            continue
        shock = _beta_shock_ticker_return(ticker, reference_clean, shock_return)
        contribution = weight * shock
        shock_returns[ticker] = round(shock, 6)
        portfolio_return += contribution
        if ticker != "CASH":
            contributors.append(_contributor_row(ticker, weight, shock, contribution))

    top_loss = _top_loss_contributors(contributors)
    return {
        "shock_name": f"{reference_clean.lower()}_{int(abs(shock_return) * 100)}pct_down",
        "reference": reference_clean,
        "reference_return": round(shock_return, 6),
        "portfolio_return": round(portfolio_return, 6),
        "estimated_loss": round(max(0.0, -portfolio_return), 6),
        "top_loss_contributors": top_loss,
        "top_loss_summary": _contributor_summary(top_loss),
        "shock_returns": shock_returns,
    }


def _role_shock_result(weights: dict[str, float], definition: dict[str, Any]) -> dict[str, Any]:
    role = str(definition.get("role") or "").lower()
    shock = float(definition.get("shock_return") or 0.0)
    shock_returns: dict[str, float] = {}
    contributors: list[dict[str, Any]] = []
    affected: list[str] = []
    portfolio_return = 0.0
    for ticker, weight in sorted(weights.items()):
        if weight <= 0:
            continue
        ticker_role = get_role(ticker).value if ticker != "CASH" else "cash"
        ticker_shock = shock if ticker_role == role else 0.0
        contribution = weight * ticker_shock
        shock_returns[ticker] = round(ticker_shock, 6)
        portfolio_return += contribution
        if ticker_role == role and ticker != "CASH":
            affected.append(ticker)
            contributors.append(_contributor_row(ticker, weight, ticker_shock, contribution))

    top_loss = _top_loss_contributors(contributors)
    return {
        "shock_name": definition["shock_name"],
        "role": role,
        "description": definition.get("description"),
        "role_shock_return": round(shock, 6),
        "portfolio_return": round(portfolio_return, 6),
        "estimated_loss": round(max(0.0, -portfolio_return), 6),
        "affected_tickers": affected,
        "top_loss_contributors": top_loss,
        "top_loss_summary": _contributor_summary(top_loss),
        "shock_returns": shock_returns,
    }


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


def _beta_shock_ticker_return(ticker: str, reference: str, shock_return: float) -> float:
    clean = str(ticker or "").upper().strip()
    if clean == "CASH":
        return 0.0
    beta = _ticker_beta(clean)
    if reference == "QQQ":
        reference_beta = max(abs(_ticker_beta("QQQ")), 1e-9)
        return (beta / reference_beta) * shock_return
    return beta * shock_return


def _ticker_beta(ticker: str) -> float:
    clean = str(ticker or "").upper().strip()
    if clean in TICKER_BETA_FALLBACKS:
        return float(TICKER_BETA_FALLBACKS[clean])
    role = get_role(clean)
    if role == TickerRole.CORE:
        return 1.0
    if role == TickerRole.SECTOR:
        return 1.05
    if role == TickerRole.THEMATIC:
        return 1.35
    if role == TickerRole.SATELLITE:
        return 0.30
    if role == TickerRole.HEDGE:
        return -1.0
    return 1.0


def _contributor_row(ticker: str, weight: float, shock_return: float, contribution: float) -> dict[str, Any]:
    return {
        "ticker": ticker,
        "weight": round(weight, 6),
        "shock_return": round(shock_return, 6),
        "contribution": round(contribution, 6),
    }


def _top_loss_contributors(contributors: list[dict[str, Any]], limit: int = 5) -> list[dict[str, Any]]:
    return [
        row
        for row in sorted(contributors, key=lambda item: float(item.get("contribution") or 0.0))
        if float(row.get("contribution") or 0.0) < 0.0
    ][:limit]


def _top_gain_contributors(contributors: list[dict[str, Any]], limit: int = 5) -> list[dict[str, Any]]:
    return [
        row
        for row in sorted(
            contributors,
            key=lambda item: float(item.get("contribution") or 0.0),
            reverse=True,
        )
        if float(row.get("contribution") or 0.0) > 0.0
    ][:limit]


def _contributor_summary(contributors: list[dict[str, Any]]) -> str:
    if not contributors:
        return ""
    return "; ".join(
        f"{row.get('ticker')} {float(row.get('contribution') or 0.0):+.2%}"
        for row in contributors[:5]
    )


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


def _max_beta_shock_loss(report: dict[str, Any]) -> float | None:
    rows = _beta_shock_rows(report)
    if not rows:
        return None
    return round(max(float(row.get("estimated_loss") or 0.0) for row in rows), 6)


def _beta_shock_rows(report: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for key in ("spy_shocks", "qqq_shocks", "role_shocks"):
        rows.extend(report.get(key) or [])
    return rows


def _worst_scenario_name(rows: list[dict[str, Any]]) -> str | None:
    if not rows:
        return None
    worst = max(rows, key=lambda row: float(row.get("estimated_loss") or 0.0))
    return str(worst.get("scenario") or worst.get("name") or "") or None


def _worst_beta_shock_name(report: dict[str, Any]) -> str | None:
    rows = _beta_shock_rows(report)
    if not rows:
        return None
    worst = max(rows, key=lambda row: float(row.get("estimated_loss") or 0.0))
    return str(worst.get("shock_name") or "") or None


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
