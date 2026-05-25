"""Deterministic macro-regime labels from price proxies.

The labels produced here are context for strategy scoring. They do not bypass
feature contracts, conviction, risk validation, or execution policy.
"""
from __future__ import annotations

from statistics import fmean
from typing import Any


CONTRACT_VERSION = "macro_regime_context_v1"


def build_deterministic_macro_regime(
    holdings: list[dict[str, Any]],
    *,
    news_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    rows = {
        str(row.get("ticker") or "").upper().strip(): row
        for row in holdings
        if row.get("ticker")
    }
    rate = _classify_rate_regime(rows)
    inflation = _classify_inflation_regime(rows)
    growth = _classify_growth_regime(rows)
    data_points = rate["data_points"] + inflation["data_points"] + growth["data_points"]
    confidence = _confidence(data_points)
    news_overlay = _news_overlay(news_context or {})
    warnings = []
    if confidence == "low":
        warnings.append("macro regime labels have limited price-proxy coverage")
    if news_overlay.get("data_quality") in {"missing", "stale"}:
        warnings.append("news overlay unavailable or stale; price proxies remain primary")

    return {
        "contract_version": CONTRACT_VERSION,
        "source": "deterministic_price_proxy",
        "has_data": bool(data_points),
        "confidence": confidence,
        "rate_regime_label": rate["label"],
        "rate_regime": rate["label"],
        "inflation_regime_label": inflation["label"],
        "inflation_regime": inflation["label"],
        "growth_regime_label": growth["label"],
        "growth_regime": growth["label"],
        "news_overlay": news_overlay,
        "diagnostics": {
            "rate": rate,
            "inflation": inflation,
            "growth": growth,
            "data_point_count": len(data_points),
            "available_tickers": sorted(rows),
        },
        "warnings": warnings,
    }


def _classify_rate_regime(rows: dict[str, dict[str, Any]]) -> dict[str, Any]:
    tlt60 = _field(rows, "TLT", "mom_60d", "return_60d")
    tlt20 = _field(rows, "TLT", "mom_20d", "return_20d")
    ief60 = _field(rows, "IEF", "mom_60d", "return_60d")
    sgov60 = _field(rows, "SGOV", "mom_60d", "return_60d") or 0.0
    duration_mom = _first_number(tlt60, ief60)
    duration_spread = None if duration_mom is None else duration_mom - sgov60
    data = [value for value in (tlt60, tlt20, ief60) if value is not None]

    if duration_mom is None:
        label = "unknown"
    elif (
        duration_mom <= -0.04
        or (tlt20 is not None and tlt20 <= -0.025)
        or (duration_spread is not None and duration_spread <= -0.05)
    ):
        label = "rising_rate_expectation"
    elif (
        duration_mom >= 0.04
        and (tlt20 is None or tlt20 >= -0.005)
    ) or (duration_spread is not None and duration_spread >= 0.05):
        label = "falling_rate_expectation"
    else:
        label = "stable_rates"

    return {
        "label": label,
        "data_points": data,
        "tlt_mom_60d": tlt60,
        "tlt_mom_20d": tlt20,
        "ief_mom_60d": ief60,
        "sgov_mom_60d": sgov60,
        "duration_spread_vs_sgov": duration_spread,
    }


def _classify_inflation_regime(rows: dict[str, dict[str, Any]]) -> dict[str, Any]:
    xle60 = _field(rows, "XLE", "mom_60d", "return_60d")
    xle20 = _field(rows, "XLE", "mom_20d", "return_20d")
    xli60 = _field(rows, "XLI", "mom_60d", "return_60d")
    spy60 = _field(rows, "SPY", "mom_60d", "return_60d") or 0.0
    tlt60 = _field(rows, "TLT", "mom_60d", "return_60d")
    cyclicals = _mean_present([xle60, xli60])
    xle_spread = None if xle60 is None else xle60 - spy60
    data = [value for value in (xle60, xle20, xli60, spy60, tlt60) if value is not None]

    if xle60 is None and cyclicals is None:
        label = "unknown"
    elif xle_spread is not None and xle_spread >= 0.03 and (xle20 is None or xle20 > 0):
        label = "commodity_strength"
    elif xle_spread is not None and xle_spread <= -0.05 and tlt60 is not None and tlt60 >= 0.03:
        label = "disinflationary"
    elif cyclicals is not None and cyclicals > 0.04 and (tlt60 is None or tlt60 <= 0.02):
        label = "sticky_inflation"
    else:
        label = "neutral_inflation"

    return {
        "label": label,
        "data_points": data,
        "xle_mom_60d": xle60,
        "xle_mom_20d": xle20,
        "xli_mom_60d": xli60,
        "spy_mom_60d": spy60,
        "tlt_mom_60d": tlt60,
        "xle_spread_vs_spy": xle_spread,
    }


def _classify_growth_regime(rows: dict[str, dict[str, Any]]) -> dict[str, Any]:
    spy60 = _field(rows, "SPY", "mom_60d", "return_60d")
    qqq60 = _field(rows, "QQQ", "mom_60d", "return_60d")
    iwm60 = _field(rows, "IWM", "mom_60d", "return_60d")
    xli60 = _field(rows, "XLI", "mom_60d", "return_60d")
    xlre60 = _field(rows, "XLRE", "mom_60d", "return_60d")
    atr_values = [
        _field(rows, ticker, "atr_pct")
        for ticker in ("SPY", "QQQ", "IWM", "XLI")
    ]
    avg_atr = _mean_present(atr_values)
    broad = _mean_present([iwm60, xli60])
    growth_core = _mean_present([spy60, qqq60])
    data = [value for value in (spy60, qqq60, iwm60, xli60, xlre60, avg_atr) if value is not None]

    if growth_core is None:
        label = "unknown"
    elif growth_core <= -0.05 or (spy60 is not None and spy60 <= -0.06):
        label = "recession_risk" if avg_atr is not None and avg_atr >= 0.035 else "growth_scare"
    elif broad is not None and broad >= 0.04 and growth_core >= 0.04:
        label = "reacceleration"
    elif qqq60 is not None and iwm60 is not None and qqq60 - iwm60 >= 0.06 and qqq60 > 0:
        label = "narrow_growth"
    else:
        label = "stable_growth"

    return {
        "label": label,
        "data_points": data,
        "spy_mom_60d": spy60,
        "qqq_mom_60d": qqq60,
        "iwm_mom_60d": iwm60,
        "xli_mom_60d": xli60,
        "xlre_mom_60d": xlre60,
        "avg_atr_proxy": avg_atr,
        "broad_cyclical_mom_60d": broad,
        "growth_core_mom_60d": growth_core,
    }


def _news_overlay(news_context: dict[str, Any]) -> dict[str, Any]:
    signals = news_context.get("macro_signals") or []
    return {
        "source": "news_context",
        "signal_count": len(signals) if isinstance(signals, list) else 0,
        "data_quality": (
            "missing"
            if news_context.get("_fallback")
            else "stale"
            if news_context.get("_stale_warning")
            else "fresh"
            if signals
            else "limited"
        ),
        "processed_at": news_context.get("processed_at"),
        "stale_warning": news_context.get("_stale_warning"),
    }


def _field(rows: dict[str, dict[str, Any]], ticker: str, *fields: str) -> float | None:
    row = rows.get(ticker) or {}
    for field in fields:
        value = _to_float(row.get(field))
        if value is not None:
            return value
    return None


def _first_number(*values: float | None) -> float | None:
    for value in values:
        if value is not None:
            return value
    return None


def _mean_present(values: list[float | None]) -> float | None:
    clean = [value for value in values if value is not None]
    if not clean:
        return None
    return float(fmean(clean))


def _confidence(data_points: list[float]) -> str:
    if len(data_points) >= 12:
        return "high"
    if len(data_points) >= 7:
        return "medium"
    if len(data_points) >= 3:
        return "low"
    return "missing"


def _to_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
