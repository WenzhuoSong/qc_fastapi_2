"""Historical liquidity and spread proxy diagnostics for ETF execution quality.

This module uses daily yfinance-style OHLCV feature rows to estimate whether
weak strategy signals may be eroded by spread, volatility, and thin liquidity.
It is diagnostics-only and has no execution authority.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from statistics import median
from typing import Any, Iterable

from services.knowledge_base import load_knowledge_base
from services.market_feature_store import get_market_daily_feature_rows


CONTRACT_VERSION = "liquidity_proxy_diagnostics_v1"
DEFAULT_LOOKBACK_DAYS = 252
DEFAULT_MIN_SAMPLES = 60


async def load_liquidity_proxy_diagnostics(
    db: Any,
    *,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    source: str = "yfinance",
    tickers: list[str] | None = None,
    min_samples: int = DEFAULT_MIN_SAMPLES,
) -> dict[str, Any]:
    clean_tickers = _target_tickers(tickers)
    start = datetime.now(timezone.utc).date() - timedelta(days=max(int(lookback_days * 1.5), lookback_days + 30))
    rows = await get_market_daily_feature_rows(
        db,
        tickers=sorted(clean_tickers),
        start_date=start,
        source=source,
    )
    return evaluate_liquidity_proxy_diagnostics(
        historical_feature_rows=rows,
        tickers=sorted(clean_tickers),
        min_samples=min_samples,
    )


def evaluate_liquidity_proxy_diagnostics(
    *,
    historical_feature_rows: Iterable[Any],
    tickers: list[str] | None = None,
    asset_profiles: dict[str, dict[str, Any]] | None = None,
    min_samples: int = DEFAULT_MIN_SAMPLES,
) -> dict[str, Any]:
    profiles = asset_profiles or _asset_profiles()
    rows_by_ticker = _feature_rows_by_ticker(historical_feature_rows)
    target_tickers = _target_tickers(tickers, fallback_tickers=sorted(rows_by_ticker))
    rows = [
        _liquidity_row(
            ticker=ticker,
            feature_rows=rows_by_ticker.get(ticker, []),
            profile=profiles.get(ticker, {}),
            min_samples=min_samples,
        )
        for ticker in sorted(target_tickers)
    ]
    available = [row for row in rows if row["status"] == "available"]
    low_liquidity = [
        row for row in available
        if row.get("liquidity_bucket") in {"thin", "illiquid"}
    ]
    wide_spread = [
        row for row in available
        if _to_float(row.get("spread_cost_proxy_pct"), 0.0) >= 0.0025
    ]
    warnings = _warnings(rows)
    return {
        "contract_version": CONTRACT_VERSION,
        "status": "available" if available else "insufficient_data",
        "mode": "diagnostic_only",
        "execution_authority": "none",
        "target_weight_mutation": "none",
        "min_samples": int(min_samples),
        "ticker_count": len(rows),
        "available_ticker_count": len(available),
        "low_liquidity_ticker_count": len(low_liquidity),
        "wide_spread_proxy_ticker_count": len(wide_spread),
        "rows": rows,
        "low_liquidity_tickers": [
            {
                "ticker": row["ticker"],
                "liquidity_bucket": row.get("liquidity_bucket"),
                "median_dollar_volume": row.get("median_dollar_volume"),
                "spread_cost_proxy_pct": row.get("spread_cost_proxy_pct"),
            }
            for row in low_liquidity
        ],
        "execution_review_rows": [
            row for row in rows
            if row.get("execution_quality") in {"defer_weak_signals", "no_trade_review"}
        ],
        "warnings": warnings,
        "method": {
            "dollar_volume_bucket": "median_dollar_volume_over_lookback",
            "spread_cost_proxy_pct": "max(range_pct*liquidity_multiplier, atr_pct*0.03, bucket_floor)",
            "opening_window_risk": "p95_absolute_open_gap_and_atr_proxy",
            "closing_window_risk": "p95_intraday_range_and_atr_proxy",
            "not_live_quote_spread": True,
            "not_execution_authority": True,
        },
    }


def evaluate_liquidity_proxy_diagnostics_from_snapshots(
    snapshots: list[dict[str, Any]],
    *,
    tickers: list[str] | None = None,
    min_samples: int = DEFAULT_MIN_SAMPLES,
) -> dict[str, Any]:
    rows = []
    for snapshot in snapshots or []:
        trading_date = snapshot.get("trading_date")
        for row in snapshot.get("holdings") or snapshot.get("features") or []:
            item = dict(row)
            item.setdefault("trading_date", trading_date)
            rows.append(item)
    return evaluate_liquidity_proxy_diagnostics(
        historical_feature_rows=rows,
        tickers=tickers,
        min_samples=min_samples,
    )


def empty_liquidity_proxy_diagnostics(reason: str = "no_historical_feature_rows") -> dict[str, Any]:
    return {
        "contract_version": CONTRACT_VERSION,
        "status": "insufficient_data",
        "reason": reason,
        "mode": "diagnostic_only",
        "execution_authority": "none",
        "target_weight_mutation": "none",
        "rows": [],
        "low_liquidity_tickers": [],
        "execution_review_rows": [],
        "warnings": [reason],
    }


def _liquidity_row(
    *,
    ticker: str,
    feature_rows: list[dict[str, Any]],
    profile: dict[str, Any],
    min_samples: int,
) -> dict[str, Any]:
    rows = sorted(feature_rows, key=lambda item: item["trading_date"])
    usable = [row for row in rows if row.get("close_price") is not None]
    dollar_volumes = [
        value for value in (_dollar_volume(row) for row in usable)
        if value is not None and value > 0
    ]
    ranges = [
        value for value in (_range_pct(row) for row in usable)
        if value is not None and value >= 0
    ]
    atrs = [
        value for value in (_to_float(row.get("atr_pct")) for row in usable)
        if value is not None and value >= 0
    ]
    amihud_values = [
        value for value in (_amihud_proxy(row) for row in usable)
        if value is not None and value >= 0
    ]
    open_gaps = _open_gap_abs(rows)
    sample_count = len(usable)
    status = "available" if sample_count >= min_samples and dollar_volumes else "insufficient_data"
    median_dv = median(dollar_volumes) if dollar_volumes else None
    p10_dv = _percentile(dollar_volumes, 0.10)
    median_range = median(ranges) if ranges else None
    p95_range = _percentile(ranges, 0.95)
    median_atr = median(atrs) if atrs else None
    p95_open_gap = _percentile(open_gaps, 0.95)
    bucket = _dollar_volume_bucket(median_dv)
    spread_proxy = _spread_proxy_pct(
        bucket=bucket,
        median_range_pct=median_range,
        median_atr_pct=median_atr,
    )
    opening_risk, opening_reason = _opening_window_risk(
        p95_open_gap_abs=p95_open_gap,
        median_atr_pct=median_atr,
    )
    closing_risk, closing_reason = _closing_window_risk(
        p95_range_pct=p95_range,
        median_atr_pct=median_atr,
    )
    execution_quality = _execution_quality(
        status=status,
        bucket=bucket,
        spread_proxy=spread_proxy,
        opening_risk=opening_risk,
        closing_risk=closing_risk,
    )
    return {
        "ticker": ticker,
        "asset_class": profile.get("asset_class"),
        "role": profile.get("role"),
        "allowed_actions": profile.get("allowed_actions") or [],
        "sample_count": sample_count,
        "status": status,
        "median_dollar_volume": round(median_dv, 2) if median_dv is not None else None,
        "p10_dollar_volume": round(p10_dv, 2) if p10_dv is not None else None,
        "median_volume": round(median([_to_float(row.get("volume"), 0.0) for row in usable]), 2) if usable else None,
        "liquidity_bucket": bucket if status == "available" else "unknown",
        "median_atr_pct": round(median_atr, 6) if median_atr is not None else None,
        "median_intraday_range_pct": round(median_range, 6) if median_range is not None else None,
        "p95_intraday_range_pct": round(p95_range, 6) if p95_range is not None else None,
        "amihud_proxy_per_1m": round(median(amihud_values), 8) if amihud_values else None,
        "spread_cost_proxy_pct": round(spread_proxy, 6) if status == "available" else None,
        "opening_gap_p95_abs": round(p95_open_gap, 6) if p95_open_gap is not None else None,
        "opening_window_risk": opening_risk if status == "available" else "unknown",
        "opening_window_reason": opening_reason,
        "closing_window_risk": closing_risk if status == "available" else "unknown",
        "closing_window_reason": closing_reason,
        "execution_quality": execution_quality,
        "weak_signal_guidance": _weak_signal_guidance(execution_quality),
    }


def _execution_quality(
    *,
    status: str,
    bucket: str,
    spread_proxy: float,
    opening_risk: str,
    closing_risk: str,
) -> str:
    if status != "available":
        return "insufficient_data"
    if bucket == "illiquid" or spread_proxy >= 0.005:
        return "no_trade_review"
    if bucket == "thin" or spread_proxy >= 0.0025 or opening_risk == "high" or closing_risk == "high":
        return "defer_weak_signals"
    if spread_proxy >= 0.0015 or opening_risk == "medium" or closing_risk == "medium":
        return "watch_costs"
    return "robust"


def _weak_signal_guidance(execution_quality: str) -> str:
    if execution_quality == "no_trade_review":
        return "manual review before any weak or exploratory signal"
    if execution_quality == "defer_weak_signals":
        return "weak signals should be deferred unless conviction and risk budget are strong"
    if execution_quality == "watch_costs":
        return "execution costs should be checked before marginal rebalances"
    if execution_quality == "robust":
        return "liquidity proxy is unlikely to dominate ordinary signal edge"
    return "insufficient liquidity history"


def _dollar_volume_bucket(value: float | None) -> str:
    if value is None:
        return "unknown"
    if value >= 5_000_000_000:
        return "mega_liquid"
    if value >= 1_000_000_000:
        return "highly_liquid"
    if value >= 250_000_000:
        return "liquid"
    if value >= 50_000_000:
        return "usable"
    if value >= 10_000_000:
        return "thin"
    return "illiquid"


def _spread_proxy_pct(
    *,
    bucket: str,
    median_range_pct: float | None,
    median_atr_pct: float | None,
) -> float:
    multiplier = {
        "mega_liquid": 0.025,
        "highly_liquid": 0.035,
        "liquid": 0.050,
        "usable": 0.080,
        "thin": 0.120,
        "illiquid": 0.180,
        "unknown": 0.120,
    }.get(bucket, 0.120)
    floor = {
        "mega_liquid": 0.00005,
        "highly_liquid": 0.00008,
        "liquid": 0.00015,
        "usable": 0.00035,
        "thin": 0.00100,
        "illiquid": 0.00300,
        "unknown": 0.00100,
    }.get(bucket, 0.00100)
    range_component = (median_range_pct or 0.0) * multiplier
    atr_component = (median_atr_pct or 0.0) * 0.03
    return max(range_component, atr_component, floor)


def _opening_window_risk(
    *,
    p95_open_gap_abs: float | None,
    median_atr_pct: float | None,
) -> tuple[str, str]:
    gap = p95_open_gap_abs or 0.0
    atr = median_atr_pct or 0.0
    if gap >= 0.035 or atr >= 0.055:
        return "high", "large open-gap or ATR proxy"
    if gap >= 0.018 or atr >= 0.030:
        return "medium", "moderate open-gap or ATR proxy"
    return "low", "limited historical open-gap proxy"


def _closing_window_risk(
    *,
    p95_range_pct: float | None,
    median_atr_pct: float | None,
) -> tuple[str, str]:
    price_range = p95_range_pct or 0.0
    atr = median_atr_pct or 0.0
    if price_range >= 0.090 or atr >= 0.055:
        return "high", "large intraday range or ATR proxy"
    if price_range >= 0.050 or atr >= 0.030:
        return "medium", "moderate intraday range or ATR proxy"
    return "low", "limited closing-window volatility proxy"


def _warnings(rows: list[dict[str, Any]]) -> list[str]:
    warnings: list[str] = []
    for row in rows:
        ticker = row.get("ticker")
        if row.get("status") != "available":
            warnings.append(f"insufficient_liquidity_samples:{ticker}:{row.get('sample_count')}")
            continue
        if row.get("liquidity_bucket") in {"thin", "illiquid"}:
            warnings.append(f"low_liquidity:{ticker}:{row.get('liquidity_bucket')}")
        if _to_float(row.get("spread_cost_proxy_pct"), 0.0) >= 0.0025:
            warnings.append(f"wide_spread_proxy:{ticker}:{row.get('spread_cost_proxy_pct')}")
        if row.get("opening_window_risk") == "high":
            warnings.append(f"opening_window_risk:{ticker}:high")
        if row.get("closing_window_risk") == "high":
            warnings.append(f"closing_window_risk:{ticker}:high")
    return sorted(set(warnings))


def _feature_rows_by_ticker(rows: Iterable[Any]) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        ticker = str(_record_get(row, "ticker") or "").upper().strip()
        trading_date = _parse_date(_record_get(row, "trading_date"))
        if not ticker or trading_date is None:
            continue
        out.setdefault(ticker, []).append({
            "trading_date": trading_date,
            "open_price": _to_float(_record_get(row, "open_price")),
            "high_price": _to_float(_record_get(row, "high_price")),
            "low_price": _to_float(_record_get(row, "low_price")),
            "close_price": _first_number(_record_get(row, "close_price"), _record_get(row, "price")),
            "volume": _to_float(_record_get(row, "volume")),
            "dollar_volume": _to_float(_record_get(row, "dollar_volume")),
            "return_1d": _first_number(_record_get(row, "return_1d"), _record_get(row, "daily_return_pct")),
            "atr_pct": _to_float(_record_get(row, "atr_pct")),
        })
    return out


def _dollar_volume(row: dict[str, Any]) -> float | None:
    direct = _to_float(row.get("dollar_volume"))
    if direct is not None and direct > 0:
        return direct
    close = _to_float(row.get("close_price"))
    volume = _to_float(row.get("volume"))
    if close is None or volume is None or close <= 0 or volume <= 0:
        return None
    return close * volume


def _range_pct(row: dict[str, Any]) -> float | None:
    high = _to_float(row.get("high_price"))
    low = _to_float(row.get("low_price"))
    close = _to_float(row.get("close_price"))
    if high is None or low is None or close is None or close <= 0:
        return None
    return max(high - low, 0.0) / close


def _amihud_proxy(row: dict[str, Any]) -> float | None:
    ret = _to_float(row.get("return_1d"))
    dollar_volume = _dollar_volume(row)
    if ret is None or dollar_volume is None or dollar_volume <= 0:
        return None
    return abs(ret) / (dollar_volume / 1_000_000.0)


def _open_gap_abs(rows: list[dict[str, Any]]) -> list[float]:
    gaps: list[float] = []
    previous_close: float | None = None
    for row in sorted(rows, key=lambda item: item["trading_date"]):
        open_price = _to_float(row.get("open_price"))
        if previous_close is not None and previous_close > 0 and open_price is not None:
            gaps.append(abs(open_price / previous_close - 1.0))
        close = _to_float(row.get("close_price"))
        if close is not None and close > 0:
            previous_close = close
    return gaps


def _asset_profiles() -> dict[str, dict[str, Any]]:
    try:
        kb = load_knowledge_base()
        assets = kb.get("assets") if isinstance(kb, dict) else {}
        return {
            str(ticker).upper(): dict(profile)
            for ticker, profile in (assets or {}).items()
            if isinstance(profile, dict)
        }
    except Exception:
        return {}


def _target_tickers(
    tickers: list[str] | None,
    *,
    fallback_tickers: list[str] | None = None,
) -> set[str]:
    if tickers:
        raw = tickers
    elif fallback_tickers is not None:
        raw = list(fallback_tickers)
    else:
        raw = sorted(_asset_profiles())
    return {
        str(ticker or "").upper().strip()
        for ticker in raw
        if str(ticker or "").upper().strip()
    }


def _percentile(values: list[float], q: float) -> float | None:
    clean = sorted(float(value) for value in values if _to_float(value) is not None)
    if not clean:
        return None
    if len(clean) == 1:
        return clean[0]
    pos = max(min(float(q), 1.0), 0.0) * (len(clean) - 1)
    lower = int(pos)
    upper = min(lower + 1, len(clean) - 1)
    frac = pos - lower
    return clean[lower] * (1.0 - frac) + clean[upper] * frac


def _first_number(*values: Any) -> float | None:
    for value in values:
        parsed = _to_float(value)
        if parsed is not None:
            return parsed
    return None


def _record_get(row: Any, field: str) -> Any:
    if isinstance(row, dict):
        return row.get(field)
    return getattr(row, field, None)


def _parse_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _to_float(value: Any, default: float | None = None) -> float | None:
    try:
        return float(value) if value is not None else default
    except (TypeError, ValueError):
        return default
