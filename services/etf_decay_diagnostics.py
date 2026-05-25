"""Leveraged and volatility ETF decay diagnostics.

This module quantifies daily-reset and volatility-ETP drag from historical
feature rows. It is diagnostics-only and cannot approve or mutate execution.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from statistics import median
from typing import Any, Iterable

from services.knowledge_base import load_knowledge_base
from services.market_feature_store import get_market_daily_feature_rows


CONTRACT_VERSION = "etf_decay_diagnostics_v1"
DEFAULT_LOOKBACK_DAYS = 504
DEFAULT_MIN_SAMPLES = 60
VOLATILITY_ETPS = {"UVXY", "VIXY", "VXX", "UVIX", "SVXY"}
LEVERAGED_PROXY_MAP = {
    "TQQQ": ("QQQ", 3.0),
    "SQQQ": ("QQQ", -3.0),
    "SPXL": ("SPY", 3.0),
    "SPXS": ("SPY", -3.0),
    "TECL": ("XLK", 3.0),
    "TECS": ("XLK", -3.0),
    "SOXL": ("SOXX", 3.0),
    "SOXS": ("SOXX", -3.0),
}
DEFAULT_DECAY_TICKERS = tuple(sorted(set(LEVERAGED_PROXY_MAP) | VOLATILITY_ETPS))


async def load_etf_decay_diagnostics(
    db: Any,
    *,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    source: str = "yfinance",
    tickers: list[str] | None = None,
    min_samples: int = DEFAULT_MIN_SAMPLES,
) -> dict[str, Any]:
    clean_tickers = _target_tickers(tickers)
    proxies = {
        proxy
        for ticker in clean_tickers
        for proxy, _ in [LEVERAGED_PROXY_MAP.get(ticker, ("SPY", 0.0))]
        if ticker in LEVERAGED_PROXY_MAP and proxy
    }
    if clean_tickers & VOLATILITY_ETPS:
        proxies.add("SPY")
    start = datetime.now(timezone.utc).date() - timedelta(days=max(int(lookback_days * 1.7), lookback_days + 30))
    rows = await get_market_daily_feature_rows(
        db,
        tickers=sorted(clean_tickers | proxies),
        start_date=start,
        source=source,
    )
    return evaluate_etf_decay_diagnostics(
        historical_return_rows=rows,
        tickers=sorted(clean_tickers),
        min_samples=min_samples,
    )


def evaluate_etf_decay_diagnostics(
    *,
    historical_return_rows: Iterable[Any],
    tickers: list[str] | None = None,
    asset_profiles: dict[str, dict[str, Any]] | None = None,
    min_samples: int = DEFAULT_MIN_SAMPLES,
) -> dict[str, Any]:
    profiles = asset_profiles or _asset_profiles()
    target_tickers = _target_tickers(tickers)
    returns_by_ticker = _returns_by_ticker(historical_return_rows)
    rows: list[dict[str, Any]] = []
    for ticker in sorted(target_tickers):
        profile = profiles.get(ticker, {})
        if ticker in LEVERAGED_PROXY_MAP:
            rows.append(_leveraged_decay_row(ticker, returns_by_ticker, profile, min_samples))
        elif ticker in VOLATILITY_ETPS:
            rows.append(_volatility_decay_row(ticker, returns_by_ticker, profile, min_samples))

    available = [row for row in rows if row["status"] == "available"]
    high = [
        row for row in available
        if row.get("severity") in {"high", "extreme"}
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
        "high_decay_ticker_count": len(high),
        "rows": rows,
        "high_decay_tickers": [
            {
                "ticker": row["ticker"],
                "severity": row["severity"],
                "reason": row.get("severity_reason"),
                "max_hold_days": row.get("max_hold_days"),
            }
            for row in high
        ],
        "policy_review_rows": [
            row for row in rows
            if row.get("max_hold_policy_warning")
        ],
        "warnings": warnings,
        "method": {
            "leveraged_drag": "etf_return - leverage * underlying_proxy_return",
            "volatility_drag": "rolling_return_and_spy_up_market_decay",
            "not_execution_authority": True,
        },
    }


def evaluate_etf_decay_diagnostics_from_snapshots(
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
    return evaluate_etf_decay_diagnostics(
        historical_return_rows=rows,
        tickers=tickers,
        min_samples=min_samples,
    )


def empty_etf_decay_diagnostics(reason: str = "no_historical_return_rows") -> dict[str, Any]:
    return {
        "contract_version": CONTRACT_VERSION,
        "status": "insufficient_data",
        "reason": reason,
        "mode": "diagnostic_only",
        "execution_authority": "none",
        "target_weight_mutation": "none",
        "rows": [],
        "high_decay_tickers": [],
        "policy_review_rows": [],
        "warnings": [reason],
    }


def _leveraged_decay_row(
    ticker: str,
    returns_by_ticker: dict[str, dict[date, float]],
    profile: dict[str, Any],
    min_samples: int,
) -> dict[str, Any]:
    proxy, leverage = LEVERAGED_PROXY_MAP[ticker]
    own = returns_by_ticker.get(ticker, {})
    base = returns_by_ticker.get(proxy, {})
    common = sorted(set(own) & set(base))
    drags = [
        own[item] - leverage * base[item]
        for item in common
    ]
    max_hold_days = _profile_int(profile, "max_hold_days")
    hold_drags = _rolling_sums(drags, max_hold_days or 10)
    avg_drag = _avg(drags)
    avg_hold_drag = _avg(hold_drags)
    severity, reason = _leveraged_severity(avg_drag, avg_hold_drag, max_hold_days)
    status = "available" if len(drags) >= min_samples else "insufficient_data"
    warning = _max_hold_warning(
        severity=severity,
        max_hold_days=max_hold_days,
        avg_hold_drag=avg_hold_drag,
        decay_risk=str(profile.get("decay_risk") or ""),
    )
    return {
        "ticker": ticker,
        "instrument_type": "leveraged_inverse_etf" if leverage < 0 else "leveraged_etf",
        "asset_class": profile.get("asset_class"),
        "role": profile.get("role"),
        "underlying_proxy": proxy,
        "leverage": leverage,
        "decay_risk": profile.get("decay_risk"),
        "max_hold_days": max_hold_days,
        "auto_reduce_after_days": _profile_int(profile, "auto_reduce_after_days"),
        "sample_count": len(drags),
        "status": status,
        "avg_daily_drag": round(avg_drag, 8) if avg_drag is not None else None,
        "median_daily_drag": round(median(drags), 8) if drags else None,
        "annualized_drag_proxy": round(avg_drag * 252, 6) if avg_drag is not None else None,
        "avg_hold_period_drag": round(avg_hold_drag, 6) if avg_hold_drag is not None else None,
        "hold_period_days": max_hold_days or 10,
        "negative_drag_rate": round(sum(1 for value in drags if value < 0) / len(drags), 4) if drags else None,
        "positive_drag_rate": round(sum(1 for value in drags if value > 0) / len(drags), 4) if drags else None,
        "severity": severity if status == "available" else "unknown",
        "severity_reason": reason,
        "max_hold_policy_warning": warning,
    }


def _volatility_decay_row(
    ticker: str,
    returns_by_ticker: dict[str, dict[date, float]],
    profile: dict[str, Any],
    min_samples: int,
) -> dict[str, Any]:
    own = returns_by_ticker.get(ticker, {})
    spy = returns_by_ticker.get("SPY", {})
    common = sorted(own)
    values = [own[item] for item in common]
    spy_common = sorted(set(own) & set(spy))
    up_market_values = [
        own[item]
        for item in spy_common
        if spy[item] >= 0
    ]
    five_day = _rolling_compound(values, 5)
    ten_day = _rolling_compound(values, 10)
    max_hold_days = _profile_int(profile, "max_hold_days")
    hold_returns = _rolling_compound(values, max_hold_days or 10)
    avg_hold = _avg(hold_returns)
    avg_up = _avg(up_market_values)
    severity, reason = _volatility_severity(avg_hold, avg_up)
    status = "available" if len(values) >= min_samples else "insufficient_data"
    warning = _max_hold_warning(
        severity=severity,
        max_hold_days=max_hold_days,
        avg_hold_drag=avg_hold,
        decay_risk=str(profile.get("decay_risk") or ""),
    )
    return {
        "ticker": ticker,
        "instrument_type": "volatility_etp",
        "asset_class": profile.get("asset_class"),
        "role": profile.get("role"),
        "underlying_proxy": "SPY_up_market_decay_proxy",
        "leverage": _to_float(profile.get("leverage")),
        "decay_risk": profile.get("decay_risk"),
        "max_hold_days": max_hold_days,
        "auto_reduce_after_days": _profile_int(profile, "auto_reduce_after_days"),
        "sample_count": len(values),
        "status": status,
        "avg_daily_return": round(_avg(values), 8) if values else None,
        "median_daily_return": round(median(values), 8) if values else None,
        "negative_day_rate": round(sum(1 for value in values if value < 0) / len(values), 4) if values else None,
        "avg_5d_return": round(_avg(five_day), 6) if five_day else None,
        "avg_10d_return": round(_avg(ten_day), 6) if ten_day else None,
        "avg_hold_period_return": round(avg_hold, 6) if avg_hold is not None else None,
        "hold_period_days": max_hold_days or 10,
        "spy_up_sample_count": len(up_market_values),
        "avg_return_when_spy_up": round(avg_up, 6) if avg_up is not None else None,
        "severity": severity if status == "available" else "unknown",
        "severity_reason": reason,
        "max_hold_policy_warning": warning,
    }


def _leveraged_severity(
    avg_daily_drag: float | None,
    avg_hold_drag: float | None,
    max_hold_days: int | None,
) -> tuple[str, str]:
    hold = avg_hold_drag if avg_hold_drag is not None else 0.0
    daily = avg_daily_drag if avg_daily_drag is not None else 0.0
    if hold <= -0.05 or daily <= -0.004:
        return "extreme", "large negative drag versus daily leverage proxy"
    if hold <= -0.025 or daily <= -0.002:
        return "high", "material negative drag versus daily leverage proxy"
    if hold <= -0.01 or daily <= -0.001:
        return "moderate", "measurable negative drag versus daily leverage proxy"
    if max_hold_days and max_hold_days > 10:
        return "moderate", "policy hold window is long for daily-reset ETF"
    return "low", "limited realized drag in lookback window"


def _volatility_severity(
    avg_hold_return: float | None,
    avg_return_when_spy_up: float | None,
) -> tuple[str, str]:
    hold = avg_hold_return if avg_hold_return is not None else 0.0
    up = avg_return_when_spy_up if avg_return_when_spy_up is not None else 0.0
    if hold <= -0.10 or up <= -0.015:
        return "extreme", "large hold-period or calm-market volatility ETP decay"
    if hold <= -0.06 or up <= -0.010:
        return "high", "material volatility ETP decay during ordinary/up markets"
    if hold <= -0.03 or up <= -0.005:
        return "moderate", "measurable volatility ETP decay"
    return "low", "limited decay in lookback window"


def _max_hold_warning(
    *,
    severity: str,
    max_hold_days: int | None,
    avg_hold_drag: float | None,
    decay_risk: str,
) -> str | None:
    if max_hold_days is None:
        return "missing_max_hold_days_for_decay_sensitive_etf"
    high_risk = decay_risk.lower() in {"high", "extreme"} or severity in {"high", "extreme"}
    if high_risk and max_hold_days > 10:
        return f"max_hold_days {max_hold_days} may be too wide for {severity} decay"
    if high_risk and avg_hold_drag is not None and avg_hold_drag <= -0.05 and max_hold_days >= 10:
        return f"avg hold-period drag {avg_hold_drag:.2%} suggests shorter review window"
    return None


def _warnings(rows: list[dict[str, Any]]) -> list[str]:
    warnings: list[str] = []
    for row in rows:
        if row.get("status") != "available":
            warnings.append(f"insufficient_decay_samples:{row.get('ticker')}:{row.get('sample_count')}")
        if row.get("severity") in {"high", "extreme"}:
            warnings.append(f"{row.get('severity')}_decay:{row.get('ticker')}:{row.get('severity_reason')}")
        if row.get("max_hold_policy_warning"):
            warnings.append(f"max_hold_policy:{row.get('ticker')}:{row.get('max_hold_policy_warning')}")
    return sorted(set(warnings))


def _returns_by_ticker(rows: Iterable[Any]) -> dict[str, dict[date, float]]:
    out: dict[str, dict[date, float]] = {}
    for row in rows:
        ticker = str(_record_get(row, "ticker") or "").upper().strip()
        trading_date = _parse_date(_record_get(row, "trading_date"))
        ret = _first_number(
            _record_get(row, "return_1d"),
            _record_get(row, "daily_return_pct"),
        )
        if not ticker or trading_date is None or ret is None:
            continue
        out.setdefault(ticker, {})[trading_date] = float(ret)
    return out


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


def _target_tickers(tickers: list[str] | None) -> set[str]:
    raw = tickers or list(DEFAULT_DECAY_TICKERS)
    return {
        str(ticker or "").upper().strip()
        for ticker in raw
        if str(ticker or "").upper().strip() in set(DEFAULT_DECAY_TICKERS)
    }


def _rolling_sums(values: list[float], window: int) -> list[float]:
    size = max(int(window or 0), 1)
    if len(values) < size:
        return []
    return [sum(values[idx: idx + size]) for idx in range(0, len(values) - size + 1)]


def _rolling_compound(values: list[float], window: int) -> list[float]:
    size = max(int(window or 0), 1)
    if len(values) < size:
        return []
    out = []
    for idx in range(0, len(values) - size + 1):
        compounded = 1.0
        for value in values[idx: idx + size]:
            compounded *= 1.0 + float(value)
        out.append(compounded - 1.0)
    return out


def _profile_int(profile: dict[str, Any], key: str) -> int | None:
    value = profile.get(key)
    policy = profile.get("holding_policy") if isinstance(profile.get("holding_policy"), dict) else {}
    if value is None:
        value = policy.get(key)
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _avg(values: list[float]) -> float | None:
    clean = [float(value) for value in values if _to_float(value) is not None]
    if not clean:
        return None
    return sum(clean) / len(clean)


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


def _to_float(value: Any) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None

