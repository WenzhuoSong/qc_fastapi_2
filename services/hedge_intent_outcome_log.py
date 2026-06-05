"""Diagnostic hedge-intent outcome records.

This module records hedge decisions for later threshold calibration. It does
not change hedge thresholds, weights, or execution authority.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import date, datetime
from typing import Any


REPORT_VERSION = "hedge_intent_outcome_v1"
EXECUTION_AUTHORITY = "none"
TARGET_WEIGHT_MUTATION = "none"
OUTCOME_PENDING = "pending_t5"
OUTCOME_COMPLETED = "completed_t5"

TECH_BETA_TICKERS = {"QQQ", "XLK", "SOXX", "PSI", "FTXL", "SMH", "XSD", "VUG", "TQQQ", "SOXL"}


def build_hedge_intent_outcome_record(
    *,
    hedge_intent: dict[str, Any] | None,
    market_context: dict[str, Any] | None = None,
    current_weights: dict[str, Any] | None = None,
    as_of: date | datetime | str | None = None,
    portfolio_beta_estimate: float | None = None,
) -> dict[str, Any]:
    """Return a diagnostic outcome row for one hedge-intent decision."""
    intent = hedge_intent if isinstance(hedge_intent, dict) else {}
    market = market_context if isinstance(market_context, dict) else {}
    weights = _clean_weights(current_weights)
    triggered = bool(intent.get("triggered"))
    severity = _safe_float(intent.get("severity"), 0.0)
    add_hedge = bool(intent.get("add_hedge_etf"))
    selected = _clean_ticker(intent.get("hedge_instrument") or intent.get("selected_hedge"))
    candidate = selected or infer_candidate_hedge_instrument(
        current_weights=weights,
        vix=_safe_float(market.get("vix"), _safe_float(intent.get("vix_level"), 20.0)),
    )
    cash_raise = _safe_float(
        intent.get("cash_raise_pct")
        or intent.get("target_cash_raise_pct")
        or intent.get("cash_raise_target"),
        0.0,
    )
    return {
        "report_version": REPORT_VERSION,
        "execution_authority": EXECUTION_AUTHORITY,
        "target_weight_mutation": TARGET_WEIGHT_MUTATION,
        "date": _date_str(as_of),
        "triggered": triggered,
        "severity": round(severity, 6),
        "add_hedge_etf": add_hedge,
        "selected_instrument": selected or None,
        "candidate_hedge_instrument": candidate or None,
        "why_not_add_hedge": explain_hedge_decision(
            triggered=triggered,
            add_hedge_etf=add_hedge,
            severity=severity,
        ),
        "trigger_reasons": list(intent.get("reasons") or intent.get("trigger_reasons") or []),
        "trim_targets": [_clean_ticker(ticker) for ticker in intent.get("trim_targets") or [] if _clean_ticker(ticker)],
        "cash_raise_pct": round(cash_raise, 6),
        "regime": market.get("regime") or intent.get("regime_context") or market.get("market_regime"),
        "vix": _safe_float(market.get("vix"), _safe_float(intent.get("vix_level"), 20.0)),
        "breadth": _safe_float(market.get("breadth_pct", market.get("breadth")), 0.5),
        "portfolio_beta_estimate": round(
            float(portfolio_beta_estimate)
            if portfolio_beta_estimate is not None
            else estimate_portfolio_beta(weights),
            6,
        ),
        "outcome_status": OUTCOME_PENDING,
        "spy_return_5d": None,
        "hedge_instrument_return_5d": None,
        "hedge_would_have_helped": None,
        "threshold_assessment": None,
    }


def backfill_hedge_intent_outcome(
    record: dict[str, Any],
    *,
    spy_return_5d: float,
    hedge_instrument_return_5d: float | None = None,
    outcome_date: date | datetime | str | None = None,
) -> dict[str, Any]:
    """Fill T+5 outcome fields once.

    The function is idempotent: completed records are returned unchanged.
    """
    if not isinstance(record, dict):
        record = {}
    if record.get("outcome_status") == OUTCOME_COMPLETED:
        return deepcopy(record)

    out = deepcopy(record)
    spy_ret = _safe_float(spy_return_5d, 0.0)
    hedge_ret = None if hedge_instrument_return_5d is None else _safe_float(hedge_instrument_return_5d, 0.0)
    out["outcome_status"] = OUTCOME_COMPLETED
    out["outcome_date"] = _date_str(outcome_date)
    out["spy_return_5d"] = round(spy_ret, 6)
    out["hedge_instrument_return_5d"] = None if hedge_ret is None else round(hedge_ret, 6)
    out["hedge_would_have_helped"] = bool(hedge_ret is not None and hedge_ret > 0.0)
    out["threshold_assessment"] = assess_hedge_threshold(
        triggered=bool(out.get("triggered")),
        add_hedge_etf=bool(out.get("add_hedge_etf")),
        spy_return_5d=spy_ret,
    )
    return out


def assess_hedge_threshold(
    *,
    triggered: bool,
    add_hedge_etf: bool,
    spy_return_5d: float,
) -> str:
    """Apply the PR7 threshold-assessment rules."""
    spy_ret = _safe_float(spy_return_5d, 0.0)
    if not triggered and spy_ret <= -0.03:
        return "too_conservative"
    if add_hedge_etf and spy_ret >= 0.02:
        return "too_aggressive"
    if triggered and not add_hedge_etf and spy_ret <= -0.05:
        return "severity_threshold_too_high"
    return "appropriate_or_inconclusive"


def summarize_hedge_threshold_assessments(records: list[dict[str, Any]] | None, *, limit: int = 20) -> dict[str, Any]:
    """Compact recent hedge-outcome assessments for dashboards/Telegram."""
    rows = [row for row in (records or []) if isinstance(row, dict)]
    recent = rows[: max(int(limit or 20), 1)]
    counts: dict[str, int] = {}
    pending = 0
    for row in recent:
        if row.get("outcome_status") == OUTCOME_PENDING:
            pending += 1
        assessment = str(row.get("threshold_assessment") or "pending")
        counts[assessment] = counts.get(assessment, 0) + 1
    return {
        "report_version": REPORT_VERSION,
        "execution_authority": EXECUTION_AUTHORITY,
        "target_weight_mutation": TARGET_WEIGHT_MUTATION,
        "sampled": len(recent),
        "pending_count": pending,
        "assessment_counts": dict(sorted(counts.items())),
        "recent_assessments": [
            {
                "date": row.get("date"),
                "triggered": row.get("triggered"),
                "add_hedge_etf": row.get("add_hedge_etf"),
                "selected_instrument": row.get("selected_instrument"),
                "candidate_hedge_instrument": row.get("candidate_hedge_instrument"),
                "outcome_status": row.get("outcome_status"),
                "threshold_assessment": row.get("threshold_assessment"),
            }
            for row in recent[:5]
        ],
    }


def explain_hedge_decision(*, triggered: bool, add_hedge_etf: bool, severity: float) -> str:
    if not triggered:
        return "hedge_intent_not_triggered"
    if add_hedge_etf:
        return "hedge_etf_selected"
    return f"severity_{severity:.2f}_below_threshold_0.70" if severity < 0.70 else "unknown"


def infer_candidate_hedge_instrument(*, current_weights: dict[str, Any] | None, vix: float = 20.0) -> str:
    weights = _clean_weights(current_weights)
    ranked = sorted(
        ((ticker, weight) for ticker, weight in weights.items() if ticker != "CASH" and weight > 0),
        key=lambda item: (-item[1], item[0]),
    )
    top_tickers = {ticker for ticker, _ in ranked[:4]}
    if top_tickers & TECH_BETA_TICKERS:
        return "PSQ"
    if "IWM" in top_tickers:
        return "RWM"
    return "SH"


def estimate_portfolio_beta(weights: dict[str, Any] | None) -> float:
    clean = _clean_weights(weights)
    beta_map = {
        "CASH": 0.0,
        "SGOV": 0.05,
        "TLT": -0.20,
        "GLD": 0.10,
        "SPY": 1.0,
        "QQQ": 1.15,
        "XLK": 1.20,
        "SOXX": 1.35,
        "PSI": 1.30,
        "FTXL": 1.25,
        "IWM": 1.20,
        "PSQ": -1.0,
        "SH": -1.0,
        "RWM": -1.0,
    }
    beta = 0.0
    for ticker, weight in clean.items():
        beta += float(weight or 0.0) * beta_map.get(ticker, 1.0)
    return max(min(beta, 2.0), -1.0)


def _clean_weights(raw: dict[str, Any] | None) -> dict[str, float]:
    out: dict[str, float] = {}
    for raw_ticker, raw_weight in (raw or {}).items():
        ticker = _clean_ticker(raw_ticker)
        if not ticker:
            continue
        out[ticker] = max(_safe_float(raw_weight, 0.0), 0.0)
    return out


def _clean_ticker(value: Any) -> str:
    return str(value or "").upper().strip()


def _safe_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _date_str(value: date | datetime | str | None) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if value:
        return str(value)[:10]
    return date.today().isoformat()
