"""Deterministic hedge intent and risk-reduction planning."""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


HIGH_BETA_TICKERS = {"SOXX", "PSI", "FTXL", "SMH", "XSD", "QQQ", "XLK", "VUG", "TQQQ", "SOXL"}
DEFENSIVE_REGIMES = {"defensive", "alert", "risk_off", "bear", "high_vol", "DEFENSIVE", "ALERT"}


@dataclass(frozen=True)
class RiskReductionPlan:
    triggered: bool
    trigger_reasons: list[str]
    severity: float
    trim_high_beta: bool
    trim_targets: list[str]
    target_cash_raise_pct: float
    add_defensive: bool
    defensive_candidates: list[str]
    add_hedge_etf: bool
    hedge_instrument: str | None
    hedge_weight: float
    regime_context: str
    vix_level: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def evaluate_hedge_intent(
    *,
    vix_level: float,
    portfolio_drawdown_pct: float,
    net_long_exposure: float,
    market_regime_raw: str,
    current_holdings: dict[str, Any],
    scorecard_requires_human: bool,
    market_breadth_pct: float,
) -> RiskReductionPlan:
    vix = _safe_float(vix_level, 20.0)
    drawdown = _safe_float(portfolio_drawdown_pct, 0.0)
    breadth = _safe_float(market_breadth_pct, 0.5)
    net_long = _safe_float(net_long_exposure, 0.0)
    regime = str(market_regime_raw or "normal")

    reasons: list[str] = []
    if vix > 25.0 and breadth < 0.35:
        reasons.append(f"VIX={vix:.1f}>25 + breadth={breadth:.2f}<0.35")
    if drawdown < -0.05 and _is_defensive_regime(regime):
        reasons.append(f"drawdown={drawdown:.1%} + regime={regime}")
    if scorecard_requires_human and net_long > 0.70:
        reasons.append(f"human_required + net_long={net_long:.1%}>70%")

    if not reasons:
        return RiskReductionPlan(
            triggered=False,
            trigger_reasons=[],
            severity=0.0,
            trim_high_beta=False,
            trim_targets=[],
            target_cash_raise_pct=0.0,
            add_defensive=False,
            defensive_candidates=[],
            add_hedge_etf=False,
            hedge_instrument=None,
            hedge_weight=0.0,
            regime_context=regime,
            vix_level=vix,
        )

    severity = _severity_score(vix, drawdown, breadth)
    trim_targets = _identify_high_beta(current_holdings)
    cash_raise = _cash_raise_by_severity(severity)
    add_defensive = severity < 0.70
    add_hedge = severity >= 0.70
    hedge_instrument, hedge_weight = _select_hedge_instrument(vix) if add_hedge else (None, 0.0)

    return RiskReductionPlan(
        triggered=True,
        trigger_reasons=reasons,
        severity=round(severity, 6),
        trim_high_beta=bool(trim_targets),
        trim_targets=trim_targets,
        target_cash_raise_pct=cash_raise,
        add_defensive=add_defensive,
        defensive_candidates=["TLT", "SGOV", "GLD"] if add_defensive else [],
        add_hedge_etf=add_hedge,
        hedge_instrument=hedge_instrument,
        hedge_weight=hedge_weight,
        regime_context=regime,
        vix_level=vix,
    )


def _identify_high_beta(holdings: dict[str, Any]) -> list[str]:
    rows = []
    for ticker, raw_weight in (holdings or {}).items():
        ticker = str(ticker or "").upper().strip()
        weight = _safe_float(raw_weight, 0.0)
        if ticker in HIGH_BETA_TICKERS and weight > 0.02:
            rows.append((ticker, weight))
    rows.sort(key=lambda item: item[1], reverse=True)
    return [ticker for ticker, _ in rows]


def _severity_score(vix: float, drawdown: float, breadth: float) -> float:
    vix_score = min(max((vix - 20.0) / 30.0, 0.0), 1.0)
    drawdown_score = min(abs(min(drawdown, 0.0)) / 0.15, 1.0)
    breadth_score = min(max(1.0 - breadth * 2.0, 0.0), 1.0)
    return (vix_score + drawdown_score + breadth_score) / 3.0


def _cash_raise_by_severity(severity: float) -> float:
    if severity < 0.30:
        return 0.05
    if severity < 0.60:
        return 0.10
    return 0.15


def _select_hedge_instrument(vix: float) -> tuple[str, float]:
    if vix > 35.0:
        return "UVXY", 0.02
    return "SQQQ", 0.015


def _is_defensive_regime(regime_raw: str) -> bool:
    return str(regime_raw or "") in DEFENSIVE_REGIMES


def _safe_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default
