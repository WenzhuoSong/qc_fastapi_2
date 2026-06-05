"""Monthly residual-alpha attribution report.

This module is diagnostics-only. It estimates whether recent portfolio returns
contain residual alpha after a simple SPY beta adjustment, but it never grants
execution authority or mutates target weights.
"""
from __future__ import annotations

import math
from datetime import date, datetime
from typing import Any

from services.conviction_decision import statistical_status_for_samples
from services.performance_attribution import FactorReturnPoint, PortfolioReturnPoint


REPORT_VERSION = "alpha_attribution_report_v1"
FACTOR_MODEL = "spy_single_factor_v1"
MIN_REPORT_SAMPLES = 5


def build_monthly_alpha_attribution_report(
    *,
    portfolio_returns: list[PortfolioReturnPoint | dict[str, Any]],
    factor_returns: list[FactorReturnPoint | dict[str, Any]],
    period_start: date,
    period_end: date,
    min_samples: int = MIN_REPORT_SAMPLES,
) -> dict[str, Any]:
    """Build a diagnostics-only single-factor residual alpha report."""
    joined = _joined_rows(portfolio_returns, factor_returns, period_start, period_end)
    sample_count = len(joined)
    base = {
        "available": True,
        "report_version": REPORT_VERSION,
        "execution_authority": "none",
        "target_weight_mutation": "none",
        "period_key": f"{period_start.isoformat()}_{period_end.isoformat()}",
        "period_start": period_start.isoformat(),
        "period_end": period_end.isoformat(),
        "sample_count": sample_count,
        "sample_status": statistical_status_for_samples(sample_count),
        "factor_model": FACTOR_MODEL,
        "benchmark_ticker": "SPY",
        "diagnostics_only": True,
    }

    if sample_count < max(int(min_samples), 3):
        return {
            **base,
            "status": "insufficient_data",
            "data_quality": "insufficient_data",
            "beta_vs_spy": None,
            "alpha_daily": None,
            "alpha_annualized": None,
            "alpha_t_stat": None,
            "alpha_p_value": None,
            "r_squared": None,
            "meets_t2_suggestive": False,
            "meets_harvey_t3_threshold": False,
            "honest_interpretation": "insufficient_samples",
            "reason": "not_enough_joined_portfolio_and_spy_returns",
            "required_samples": max(int(min_samples), 3),
            "joined_rows": joined,
        }

    x = [row["spy_return"] for row in joined]
    y = [row["portfolio_return"] for row in joined]
    regression = _single_factor_regression(x, y)
    if regression is None:
        return {
            **base,
            "status": "insufficient_variance",
            "data_quality": "insufficient_data",
            "beta_vs_spy": None,
            "alpha_daily": None,
            "alpha_annualized": None,
            "alpha_t_stat": None,
            "alpha_p_value": None,
            "r_squared": None,
            "meets_t2_suggestive": False,
            "meets_harvey_t3_threshold": False,
            "honest_interpretation": "insufficient_samples",
            "reason": "spy_return_variance_too_low_for_regression",
            "joined_rows": joined,
        }

    alpha_t_stat = regression["alpha_t_stat"]
    interpretation = _honest_interpretation(alpha_t_stat)
    return {
        **base,
        "status": "attributed",
        "data_quality": "ok",
        "portfolio_return": round(_compound_returns(y), 6),
        "arithmetic_portfolio_return": round(sum(y), 6),
        "spy_return": round(_compound_returns(x), 6),
        "beta_vs_spy": round(regression["beta"], 6),
        "alpha_daily": round(regression["alpha"], 8),
        "alpha_annualized": round(regression["alpha"] * 252.0, 6),
        "alpha_t_stat": round(alpha_t_stat, 6) if alpha_t_stat is not None else None,
        "alpha_p_value": round(_normal_approx_two_sided_p(alpha_t_stat), 6) if alpha_t_stat is not None else None,
        "r_squared": round(regression["r_squared"], 6),
        "residual_std_error": round(regression["residual_std_error"], 8),
        "meets_t2_suggestive": bool(alpha_t_stat is not None and abs(alpha_t_stat) >= 2.0),
        "meets_harvey_t3_threshold": bool(alpha_t_stat is not None and abs(alpha_t_stat) >= 3.0),
        "honest_interpretation": interpretation,
        "interpretation_contract": {
            "t_stat_lt_2": "early_monitoring",
            "t_stat_2_to_3": "suggestive_not_proven",
            "t_stat_gte_3": "statistically_meaningful_with_multiple_testing_caution",
            "not_execution_authority": True,
        },
        "joined_rows": joined,
    }


async def load_monthly_alpha_attribution_report(
    db: Any,
    *,
    period_end: date | None = None,
    min_samples: int = MIN_REPORT_SAMPLES,
) -> dict[str, Any]:
    """Load current-month returns from DB and build the report."""
    from sqlalchemy import select

    from db.models import MarketDailyFeature, PortfolioTimeseries

    end = period_end or date.today()
    start = date(end.year, end.month, 1)
    portfolio_result = await db.execute(
        select(PortfolioTimeseries)
        .where(PortfolioTimeseries.recorded_at >= datetime.combine(start, datetime.min.time()))
        .where(PortfolioTimeseries.recorded_at <= datetime.combine(end, datetime.max.time()))
        .order_by(PortfolioTimeseries.recorded_at)
    )
    factor_result = await db.execute(
        select(MarketDailyFeature)
        .where(MarketDailyFeature.trading_date >= start)
        .where(MarketDailyFeature.trading_date <= end)
        .where(MarketDailyFeature.ticker == "SPY")
        .order_by(MarketDailyFeature.trading_date)
    )
    return build_monthly_alpha_attribution_report(
        portfolio_returns=_portfolio_points_from_rows(list(portfolio_result.scalars().all())),
        factor_returns=_factor_points_from_rows(list(factor_result.scalars().all())),
        period_start=start,
        period_end=end,
        min_samples=min_samples,
    )


def _joined_rows(
    portfolio_returns: list[PortfolioReturnPoint | dict[str, Any]],
    factor_returns: list[FactorReturnPoint | dict[str, Any]],
    start: date,
    end: date,
) -> list[dict[str, Any]]:
    portfolio = _portfolio_map(portfolio_returns, start, end)
    spy = _spy_map(factor_returns, start, end)
    rows: list[dict[str, Any]] = []
    for trading_date in sorted(set(portfolio) & set(spy)):
        rows.append({
            "date": trading_date.isoformat(),
            "portfolio_return": round(portfolio[trading_date], 8),
            "spy_return": round(spy[trading_date], 8),
        })
    return rows


def _portfolio_points_from_rows(rows: list[Any]) -> list[PortfolioReturnPoint]:
    points: list[PortfolioReturnPoint] = []
    last_value: float | None = None
    for row in rows:
        trading_date = row.recorded_at.date() if getattr(row, "recorded_at", None) else None
        if trading_date is None:
            continue
        daily = _float_or_none(getattr(row, "daily_pnl_pct", None))
        total_value = _float_or_none(getattr(row, "total_value", None))
        if daily is None and total_value is not None and last_value and last_value > 0:
            daily = total_value / last_value - 1.0
        if total_value is not None:
            last_value = total_value
        if daily is not None:
            points.append(PortfolioReturnPoint(trading_date=trading_date, portfolio_return=daily))
    return points


def _factor_points_from_rows(rows: list[Any]) -> list[FactorReturnPoint]:
    points: list[FactorReturnPoint] = []
    for row in rows:
        value = _float_or_none(getattr(row, "return_1d", None))
        trading_date = getattr(row, "trading_date", None)
        ticker = str(getattr(row, "ticker", "") or "").upper().strip()
        if trading_date and ticker == "SPY" and value is not None:
            points.append(FactorReturnPoint(
                trading_date=trading_date,
                ticker=ticker,
                return_1d=value,
                source=str(getattr(row, "source", "") or "market_daily_features"),
            ))
    return points


def _portfolio_map(rows: list[PortfolioReturnPoint | dict[str, Any]], start: date, end: date) -> dict[date, float]:
    out: dict[date, float] = {}
    for row in rows:
        trading_date = _date_value(_record_get(row, "trading_date"))
        value = _float_or_none(_record_get(row, "portfolio_return"))
        if trading_date and value is not None and start <= trading_date <= end:
            out[trading_date] = value
    return out


def _spy_map(rows: list[FactorReturnPoint | dict[str, Any]], start: date, end: date) -> dict[date, float]:
    out: dict[date, float] = {}
    for row in rows:
        trading_date = _date_value(_record_get(row, "trading_date"))
        ticker = str(_record_get(row, "ticker") or "").upper().strip()
        value = _float_or_none(_record_get(row, "return_1d"))
        if trading_date and ticker == "SPY" and value is not None and start <= trading_date <= end:
            out[trading_date] = value
    return out


def _single_factor_regression(x: list[float], y: list[float]) -> dict[str, float | None] | None:
    n = len(x)
    if n != len(y) or n < 3:
        return None
    mean_x = sum(x) / n
    mean_y = sum(y) / n
    sxx = sum((value - mean_x) ** 2 for value in x)
    if sxx <= 1e-12:
        return None
    sxy = sum((x_value - mean_x) * (y_value - mean_y) for x_value, y_value in zip(x, y))
    beta = sxy / sxx
    alpha = mean_y - beta * mean_x
    predicted = [alpha + beta * value for value in x]
    residuals = [actual - fitted for actual, fitted in zip(y, predicted)]
    sse = sum(value * value for value in residuals)
    dof = n - 2
    sigma2 = sse / dof if dof > 0 else 0.0
    se_alpha = math.sqrt(max(sigma2 * (1.0 / n + (mean_x * mean_x) / sxx), 0.0))
    alpha_t_stat = alpha / se_alpha if se_alpha > 1e-12 else None
    return {
        "alpha": alpha,
        "beta": beta,
        "alpha_t_stat": alpha_t_stat,
        "r_squared": _r_squared(y, predicted),
        "residual_std_error": math.sqrt(max(sigma2, 0.0)),
    }


def _honest_interpretation(alpha_t_stat: float | None) -> str:
    if alpha_t_stat is None:
        return "early_monitoring"
    value = abs(float(alpha_t_stat))
    if value >= 3.0:
        return "statistically_meaningful_with_multiple_testing_caution"
    if value >= 2.0:
        return "suggestive_not_proven"
    return "early_monitoring"


def _normal_approx_two_sided_p(t_stat: float | None) -> float | None:
    if t_stat is None:
        return None
    return math.erfc(abs(float(t_stat)) / math.sqrt(2.0))


def _r_squared(y: list[float], predicted: list[float]) -> float:
    mean_y = sum(y) / len(y) if y else 0.0
    total = sum((value - mean_y) ** 2 for value in y)
    if total <= 1e-12:
        return 0.0
    residual = sum((actual - fitted) ** 2 for actual, fitted in zip(y, predicted))
    return max(min(1.0 - residual / total, 1.0), 0.0)


def _compound_returns(values: list[float]) -> float:
    total = 1.0
    for value in values:
        total *= 1.0 + float(value)
    return total - 1.0


def _record_get(row: Any, field: str) -> Any:
    if isinstance(row, dict):
        return row.get(field)
    return getattr(row, field, None)


def _date_value(value: Any) -> date | None:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        try:
            return date.fromisoformat(value[:10])
        except ValueError:
            return None
    return None


def _float_or_none(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number or number in {float("inf"), float("-inf")}:
        return None
    return number
