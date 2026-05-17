"""
ETF empirical profile builder.

Phase 2 MVP is intentionally storage-agnostic: callers pass normalized daily
feature rows from yfinance/QC/DB, and this module returns derived profiles.
"""
from __future__ import annotations

import math
from datetime import date, datetime, timezone
from typing import Any


MIN_SAMPLES_OK = 60
MIN_SAMPLES_LIMITED = 20


def build_empirical_profiles(
    rows: list[Any],
    *,
    tickers: list[str] | None = None,
    source: str = "yfinance",
    lookback_days: int | None = None,
    benchmark_ticker: str = "SPY",
    top_correlations: int = 3,
) -> dict[str, dict[str, Any]]:
    """Build empirical behavior profiles from daily feature rows."""
    wanted = {ticker.upper().strip() for ticker in (tickers or []) if ticker}
    by_ticker: dict[str, list[dict[str, Any]]] = {}
    for raw in rows or []:
        row = _row_to_dict(raw)
        ticker = str(row.get("ticker") or "").upper().strip()
        if not ticker:
            continue
        if source and row.get("source") and row.get("source") != source:
            continue
        if row.get("return_1d") is None and row.get("close_price") is None and row.get("adj_close_price") is None:
            continue
        by_ticker.setdefault(ticker, []).append(row)

    returns_by_ticker = {
        ticker: _returns_from_rows(items)
        for ticker, items in by_ticker.items()
    }

    profiles: dict[str, dict[str, Any]] = {}
    profile_tickers = sorted(wanted or set(by_ticker.keys()))
    for ticker in profile_tickers:
        items = sorted(by_ticker.get(ticker) or [], key=_date_key)
        returns = returns_by_ticker.get(ticker) or {}
        values = [value for value in returns.values() if value is not None]
        close_values = [_to_float(row.get("adj_close_price"), _to_float(row.get("close_price"))) for row in items]
        close_values = [value for value in close_values if value is not None]
        latest_date = _latest_date(items)
        samples = len(values)
        profile = {
            "ticker": ticker,
            "source": source,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "lookback_days": lookback_days,
            "samples": samples,
            "latest_date": latest_date.isoformat() if latest_date else None,
            "avg_return": _mean(values),
            "volatility": _stddev(values),
            "max_drawdown": _max_drawdown(close_values),
            "correlation_top": _top_correlations(
                ticker=ticker,
                returns_by_ticker=returns_by_ticker,
                n=top_correlations,
            ),
            "benchmark_correlation": _correlation(returns, returns_by_ticker.get(benchmark_ticker) or {}),
            "data_quality": _data_quality(samples=samples, latest_date=latest_date),
        }
        profiles[ticker] = profile
    return profiles


def _row_to_dict(row: Any) -> dict[str, Any]:
    if isinstance(row, dict):
        return row
    out: dict[str, Any] = {}
    for key in (
        "ticker",
        "trading_date",
        "source",
        "close_price",
        "adj_close_price",
        "return_1d",
    ):
        out[key] = getattr(row, key, None)
    return out


def _returns_from_rows(rows: list[dict[str, Any]]) -> dict[date, float]:
    ordered = sorted(rows, key=_date_key)
    out: dict[date, float] = {}
    previous_close: float | None = None
    for row in ordered:
        dt = _parse_date(row.get("trading_date"))
        if dt is None:
            continue
        ret = _to_float(row.get("return_1d"), None)
        close = _to_float(row.get("adj_close_price"), _to_float(row.get("close_price"), None))
        if ret is None and previous_close not in (None, 0) and close is not None:
            ret = (close / previous_close) - 1.0
        if close is not None:
            previous_close = close
        if ret is not None and math.isfinite(ret):
            out[dt] = ret
    return out


def _top_correlations(
    *,
    ticker: str,
    returns_by_ticker: dict[str, dict[date, float]],
    n: int,
) -> dict[str, float]:
    base = returns_by_ticker.get(ticker) or {}
    scores: list[tuple[str, float]] = []
    for other, returns in returns_by_ticker.items():
        if other == ticker:
            continue
        corr = _correlation(base, returns)
        if corr is None:
            continue
        scores.append((other, corr))
    scores.sort(key=lambda item: abs(item[1]), reverse=True)
    return {other: round(score, 4) for other, score in scores[:n]}


def _correlation(a: dict[date, float], b: dict[date, float]) -> float | None:
    shared = sorted(set(a) & set(b))
    if len(shared) < 5:
        return None
    xs = [a[dt] for dt in shared]
    ys = [b[dt] for dt in shared]
    mean_x = _mean(xs)
    mean_y = _mean(ys)
    if mean_x is None or mean_y is None:
        return None
    numerator = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    denom_x = math.sqrt(sum((x - mean_x) ** 2 for x in xs))
    denom_y = math.sqrt(sum((y - mean_y) ** 2 for y in ys))
    denom = denom_x * denom_y
    if denom == 0:
        return None
    return round(numerator / denom, 4)


def _max_drawdown(values: list[float]) -> float | None:
    if not values:
        return None
    peak = values[0]
    max_dd = 0.0
    for value in values:
        if value > peak:
            peak = value
        if peak:
            max_dd = min(max_dd, (value / peak) - 1.0)
    return round(max_dd, 6)


def _data_quality(*, samples: int, latest_date: date | None) -> str:
    if samples <= 0:
        return "missing"
    if samples < MIN_SAMPLES_LIMITED:
        return "limited"
    if samples < MIN_SAMPLES_OK:
        return "limited"
    if latest_date is not None and (date.today() - latest_date).days > 10:
        return "stale"
    return "fresh"


def _latest_date(rows: list[dict[str, Any]]) -> date | None:
    dates = [_parse_date(row.get("trading_date")) for row in rows]
    dates = [dt for dt in dates if dt is not None]
    return max(dates) if dates else None


def _date_key(row: dict[str, Any]) -> date:
    return _parse_date(row.get("trading_date")) or date.min


def _parse_date(value: Any) -> date | None:
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


def _mean(values: list[float]) -> float | None:
    clean = [value for value in values if math.isfinite(value)]
    if not clean:
        return None
    return round(sum(clean) / len(clean), 8)


def _stddev(values: list[float]) -> float | None:
    clean = [value for value in values if math.isfinite(value)]
    if len(clean) < 2:
        return None
    mean = sum(clean) / len(clean)
    variance = sum((value - mean) ** 2 for value in clean) / (len(clean) - 1)
    return round(math.sqrt(variance), 8)


def _to_float(value: Any, default: float | None = 0.0) -> float | None:
    try:
        return float(value) if value is not None else default
    except (TypeError, ValueError):
        return default
