"""Performance attribution MVP.

This module explains portfolio return using simple daily factor attribution.
The residual is a residual alpha candidate, not proof of alpha.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any


ATTRIBUTION_METHOD = "ols_daily_spy_qqq_momentum_v1"
DEFAULT_FACTOR_TICKERS = ("SPY", "QQQ", "MTUM")
MIN_ATTRIBUTION_SAMPLES = 5


@dataclass(frozen=True)
class PortfolioReturnPoint:
    trading_date: date
    portfolio_return: float


@dataclass(frozen=True)
class FactorReturnPoint:
    trading_date: date
    ticker: str
    return_1d: float
    source: str = "market_daily_features"


@dataclass(frozen=True)
class PerformanceAttributionResult:
    period_key: str
    period_start: date
    period_end: date
    generated_at: datetime
    status: str
    attribution_method: str
    portfolio_return: float | None
    arithmetic_portfolio_return: float | None
    spy_beta: float | None
    spy_beta_contribution: float | None
    qqq_beta: float | None
    qqq_beta_contribution: float | None
    momentum_beta: float | None
    momentum_factor_contribution: float | None
    intercept_contribution: float | None
    residual_alpha_candidate: float | None
    r_squared: float | None
    sample_count: int
    data_quality: str
    benchmark_source: str
    source_tickers: dict[str, str]
    diagnostics: dict[str, Any]
    raw_payload: dict[str, Any]
    content_hash: str

    def to_record(self) -> dict[str, Any]:
        out = asdict(self)
        out["period_start"] = self.period_start
        out["period_end"] = self.period_end
        out["generated_at"] = _strip_tz(self.generated_at)
        return out

    def to_dict(self) -> dict[str, Any]:
        out = asdict(self)
        out["period_start"] = self.period_start.isoformat()
        out["period_end"] = self.period_end.isoformat()
        out["generated_at"] = self.generated_at.isoformat()
        return out


def build_performance_attribution(
    *,
    portfolio_returns: list[PortfolioReturnPoint | dict[str, Any]],
    factor_returns: list[FactorReturnPoint | dict[str, Any]],
    period_start: date,
    period_end: date,
    min_samples: int = MIN_ATTRIBUTION_SAMPLES,
    benchmark_source: str = "market_daily_features",
) -> PerformanceAttributionResult:
    """Build a weekly factor attribution result."""
    generated_at = datetime.now(UTC)
    clean_portfolio = _portfolio_map(portfolio_returns, period_start, period_end)
    factor_map = _factor_map(factor_returns, period_start, period_end)
    joined = _joined_rows(clean_portfolio, factor_map)
    source_tickers = _source_tickers(factor_map)
    base = {
        "period_key": f"{period_start.isoformat()}_{period_end.isoformat()}",
        "period_start": period_start,
        "period_end": period_end,
        "generated_at": generated_at,
        "attribution_method": ATTRIBUTION_METHOD,
        "benchmark_source": benchmark_source,
        "source_tickers": source_tickers,
    }

    if len(joined) < int(min_samples):
        return _result(
            **base,
            status="insufficient_data",
            portfolio_return=_compound_returns([row["portfolio_return"] for row in joined]) if joined else None,
            arithmetic_portfolio_return=round(sum(row["portfolio_return"] for row in joined), 6) if joined else None,
            sample_count=len(joined),
            data_quality="insufficient_data",
            diagnostics={
                "required_samples": int(min_samples),
                "joined_samples": len(joined),
                "reason": "not_enough_joined_portfolio_and_factor_returns",
            },
            raw_payload={"joined_rows": joined},
        )

    y = [row["portfolio_return"] for row in joined]
    factor_names = ("spy", "qqq", "momentum")
    x = [
        [1.0, row["spy"], row["qqq"], row["momentum"]]
        for row in joined
    ]
    coefficients = _ols_coefficients(x, y)
    intercept, spy_beta, qqq_beta, momentum_beta = coefficients
    predicted = [
        intercept + spy_beta * row["spy"] + qqq_beta * row["qqq"] + momentum_beta * row["momentum"]
        for row in joined
    ]
    arithmetic_return = sum(y)
    spy_contribution = sum(spy_beta * row["spy"] for row in joined)
    qqq_contribution = sum(qqq_beta * row["qqq"] for row in joined)
    momentum_contribution = sum(momentum_beta * row["momentum"] for row in joined)
    intercept_contribution = intercept * len(joined)
    residual_only = sum(actual - fitted for actual, fitted in zip(y, predicted))
    residual_alpha_candidate = intercept_contribution + residual_only
    r_squared = _r_squared(y, predicted)

    return _result(
        **base,
        status="attributed",
        portfolio_return=_compound_returns(y),
        arithmetic_portfolio_return=round(arithmetic_return, 6),
        spy_beta=round(spy_beta, 6),
        spy_beta_contribution=round(spy_contribution, 6),
        qqq_beta=round(qqq_beta, 6),
        qqq_beta_contribution=round(qqq_contribution, 6),
        momentum_beta=round(momentum_beta, 6),
        momentum_factor_contribution=round(momentum_contribution, 6),
        intercept_contribution=round(intercept_contribution, 6),
        residual_alpha_candidate=round(residual_alpha_candidate, 6),
        r_squared=round(r_squared, 6),
        sample_count=len(joined),
        data_quality="ok",
        diagnostics={
            "factor_names": factor_names,
            "momentum_proxy": source_tickers.get("momentum"),
            "residual_label": "residual_alpha_candidate_not_proven_alpha",
            "residual_definition": "intercept_contribution_plus_unexplained_residual",
            "intercept_folded_into_residual_alpha_candidate": True,
            "residual_after_intercept": round(residual_only, 6),
            "identity_check": round(
                spy_contribution + qqq_contribution + momentum_contribution + residual_alpha_candidate,
                6,
            ),
        },
        raw_payload={
            "joined_rows": joined,
            "coefficients": {
                "intercept": round(intercept, 8),
                "spy": round(spy_beta, 8),
                "qqq": round(qqq_beta, 8),
                "momentum": round(momentum_beta, 8),
            },
        },
    )


async def build_and_persist_weekly_attribution(
    db: Any,
    *,
    period_end: date | None = None,
    lookback_days: int = 7,
    min_samples: int = MIN_ATTRIBUTION_SAMPLES,
) -> PerformanceAttributionResult:
    """Load recent DB rows, compute attribution, and upsert the result."""
    from sqlalchemy import select
    from sqlalchemy.dialects.postgresql import insert

    from db.models import MarketDailyFeature, PerformanceAttribution, PortfolioTimeseries

    end = period_end or date.today()
    start = end - timedelta(days=max(int(lookback_days), 1) - 1)
    portfolio_result = await db.execute(
        select(PortfolioTimeseries)
        .where(PortfolioTimeseries.recorded_at >= datetime.combine(start, datetime.min.time()))
        .where(PortfolioTimeseries.recorded_at <= datetime.combine(end, datetime.max.time()))
        .order_by(PortfolioTimeseries.recorded_at)
    )
    feature_result = await db.execute(
        select(MarketDailyFeature)
        .where(MarketDailyFeature.trading_date >= start)
        .where(MarketDailyFeature.trading_date <= end)
        .where(MarketDailyFeature.ticker.in_(DEFAULT_FACTOR_TICKERS))
        .order_by(MarketDailyFeature.trading_date, MarketDailyFeature.ticker)
    )
    attribution = build_performance_attribution(
        portfolio_returns=_portfolio_points_from_rows(portfolio_result.scalars().all()),
        factor_returns=_factor_points_from_rows(feature_result.scalars().all()),
        period_start=start,
        period_end=end,
        min_samples=min_samples,
    )
    record = attribution.to_record()
    stmt = insert(PerformanceAttribution).values(record)
    update_cols = {
        key: getattr(stmt.excluded, key)
        for key in record
        if key not in {"id", "period_key", "created_at"}
    }
    stmt = stmt.on_conflict_do_update(
        constraint="uq_performance_attribution_period_key",
        set_=update_cols,
    )
    await db.execute(stmt)
    await db.commit()
    return attribution


def _portfolio_points_from_rows(rows: list[Any]) -> list[PortfolioReturnPoint]:
    points: list[PortfolioReturnPoint] = []
    last_value: float | None = None
    for row in rows:
        trading_date = row.recorded_at.date() if row.recorded_at else None
        if trading_date is None:
            continue
        daily = _float_or_none(row.daily_pnl_pct)
        total_value = _float_or_none(row.total_value)
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
        value = _float_or_none(row.return_1d)
        if row.trading_date and row.ticker and value is not None:
            points.append(FactorReturnPoint(
                trading_date=row.trading_date,
                ticker=str(row.ticker).upper(),
                return_1d=value,
                source=str(row.source or "market_daily_features"),
            ))
    return points


def _result(**kwargs: Any) -> PerformanceAttributionResult:
    payload = {
        key: value
        for key, value in kwargs.items()
        if key not in {"content_hash", "generated_at"}
    }
    defaults = {
        "spy_beta": None,
        "spy_beta_contribution": None,
        "qqq_beta": None,
        "qqq_beta_contribution": None,
        "momentum_beta": None,
        "momentum_factor_contribution": None,
        "intercept_contribution": None,
        "residual_alpha_candidate": None,
        "r_squared": None,
    }
    values = {**defaults, **kwargs, "content_hash": _content_hash(payload)}
    return PerformanceAttributionResult(
        **values,
    )


def _portfolio_map(
    rows: list[PortfolioReturnPoint | dict[str, Any]],
    start: date,
    end: date,
) -> dict[date, float]:
    out: dict[date, float] = {}
    for row in rows:
        trading_date = _date_value(_record_get(row, "trading_date"))
        value = _float_or_none(_record_get(row, "portfolio_return"))
        if trading_date and value is not None and start <= trading_date <= end:
            out[trading_date] = value
    return out


def _factor_map(
    rows: list[FactorReturnPoint | dict[str, Any]],
    start: date,
    end: date,
) -> dict[date, dict[str, Any]]:
    raw: dict[date, dict[str, float]] = {}
    for row in rows:
        trading_date = _date_value(_record_get(row, "trading_date"))
        ticker = str(_record_get(row, "ticker") or "").upper().strip()
        value = _float_or_none(_record_get(row, "return_1d"))
        if trading_date and ticker and value is not None and start <= trading_date <= end:
            raw.setdefault(trading_date, {})[ticker] = value

    out: dict[date, dict[str, Any]] = {}
    for trading_date, ticker_returns in raw.items():
        spy = ticker_returns.get("SPY")
        qqq = ticker_returns.get("QQQ")
        if spy is None or qqq is None:
            continue
        if ticker_returns.get("MTUM") is not None:
            momentum = ticker_returns["MTUM"]
            momentum_source = "MTUM"
        else:
            momentum = qqq - spy
            momentum_source = "QQQ_minus_SPY"
        out[trading_date] = {
            "spy": spy,
            "qqq": qqq,
            "momentum": momentum,
            "momentum_source": momentum_source,
        }
    return out


def _joined_rows(portfolio: dict[date, float], factors: dict[date, dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for trading_date in sorted(set(portfolio) & set(factors)):
        factor = factors[trading_date]
        rows.append({
            "date": trading_date.isoformat(),
            "portfolio_return": round(portfolio[trading_date], 8),
            "spy": round(float(factor["spy"]), 8),
            "qqq": round(float(factor["qqq"]), 8),
            "momentum": round(float(factor["momentum"]), 8),
            "momentum_source": factor["momentum_source"],
        })
    return rows


def _source_tickers(factors: dict[date, dict[str, Any]]) -> dict[str, str]:
    momentum_source = "QQQ_minus_SPY"
    for row in factors.values():
        if row.get("momentum_source") == "MTUM":
            momentum_source = "MTUM"
            break
    return {"spy": "SPY", "qqq": "QQQ", "momentum": momentum_source}


def _ols_coefficients(x: list[list[float]], y: list[float]) -> list[float]:
    width = len(x[0])
    xtx = [[0.0 for _ in range(width)] for _ in range(width)]
    xty = [0.0 for _ in range(width)]
    for row, target in zip(x, y):
        for i in range(width):
            xty[i] += row[i] * target
            for j in range(width):
                xtx[i][j] += row[i] * row[j]
    for i in range(width):
        xtx[i][i] += 1e-8
    return _solve_linear_system(xtx, xty)


def _solve_linear_system(a: list[list[float]], b: list[float]) -> list[float]:
    n = len(b)
    matrix = [row[:] + [b[i]] for i, row in enumerate(a)]
    for col in range(n):
        pivot = max(range(col, n), key=lambda row: abs(matrix[row][col]))
        if abs(matrix[pivot][col]) < 1e-12:
            continue
        if pivot != col:
            matrix[col], matrix[pivot] = matrix[pivot], matrix[col]
        pivot_value = matrix[col][col]
        matrix[col] = [value / pivot_value for value in matrix[col]]
        for row in range(n):
            if row == col:
                continue
            factor = matrix[row][col]
            matrix[row] = [
                value - factor * matrix[col][idx]
                for idx, value in enumerate(matrix[row])
            ]
    return [matrix[row][-1] for row in range(n)]


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
    return round(total - 1.0, 6)


def _content_hash(payload: dict[str, Any]) -> str:
    def default(value: Any) -> str:
        if isinstance(value, (date, datetime)):
            return value.isoformat()
        return str(value)

    body = json.dumps(payload, sort_keys=True, default=default, separators=(",", ":"))
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


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


def _strip_tz(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(UTC).replace(tzinfo=None)
