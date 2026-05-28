"""Observe-only transaction cost diagnostics.

The default model is an IBKR-style internal return-drag proxy. It is not a
live broker fee schedule and does not mutate target weights.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from services.conviction_decision import (
    decision_conviction_discount,
    decision_statistical_status,
)

DEFAULT_BROKER = "IBKR"
DEFAULT_MODE = "observe"
DEFAULT_MIN_EDGE_TO_COST_RATIO = 2.0

DEFAULT_COST_RATES = {
    "ordinary_etf": 0.0002,
    "leveraged_etf": 0.0005,
    "volatility_etp": 0.0015,
}

DEFAULT_EXPECTED_HORIZON_RETURN_PROXY = {
    "ordinary_etf": 0.0100,
    "leveraged_etf": 0.0200,
    "volatility_etp": 0.0300,
}
BUY_SUPPORT_ACTIONS = {"increase", "hedge"}

VOLATILITY_ETPS = {"UVXY", "VXX", "VIXY", "UVIX", "SVXY"}
LEVERAGED_ETFS = {
    "TQQQ",
    "SQQQ",
    "SOXL",
    "SOXS",
    "SPXL",
    "SPXS",
    "UPRO",
    "SPXU",
    "TECL",
    "TECS",
    "TNA",
    "TZA",
    "FNGU",
    "FNGD",
    "TYP",
}


@dataclass(frozen=True)
class CostGateEvidence:
    strategy: str | None
    role: str | None
    action: str | None
    confidence: float
    conviction: float | None
    conviction_status: str
    conviction_statistical_status: str
    effective_confidence: float | None
    conviction_discount: float


def format_transaction_cost_gate_summary(gate: dict[str, Any] | None) -> str:
    """Return a compact non-blocking cost diagnostic line for operator messages."""
    if not isinstance(gate, dict) or not gate:
        return ""
    summary = gate.get("summary") or {}
    broker = gate.get("broker") or DEFAULT_BROKER
    mode = gate.get("mode") or DEFAULT_MODE
    total_drag = summary.get("total_cost_drag")
    warning_count = int(summary.get("warning_count") or 0)
    min_ratio = summary.get("min_edge_to_cost_ratio")
    cost_model = summary.get("cost_model") or f"{broker}_return_drag_v1"
    parts = [f"Cost gate: {mode} {broker}"]
    if total_drag is not None:
        parts.append(f"drag {float(total_drag or 0.0):.3%}")
    if min_ratio is not None:
        parts.append(f"min edge/cost {float(min_ratio):.2f}x")
    parts.append(f"warnings {warning_count}")
    parts.append(f"model {cost_model}")
    return " | ".join(parts)


def default_transaction_cost_gate_config(config: dict[str, Any] | None = None) -> dict[str, Any]:
    raw = dict(config or {})
    mode = str(raw.get("mode") or DEFAULT_MODE).strip().lower()
    if mode not in {"observe", "active", "disabled"}:
        mode = DEFAULT_MODE
    broker = str(raw.get("broker") or DEFAULT_BROKER).strip().upper() or DEFAULT_BROKER
    cost_rates = {**DEFAULT_COST_RATES, **_float_map(raw.get("cost_rates"))}
    expected = {
        **DEFAULT_EXPECTED_HORIZON_RETURN_PROXY,
        **_float_map(raw.get("expected_horizon_return_proxy")),
    }
    return {
        "mode": mode,
        "broker": broker,
        "min_edge_to_cost_ratio": _to_float(
            raw.get("min_edge_to_cost_ratio"),
            DEFAULT_MIN_EDGE_TO_COST_RATIO,
        ),
        "cost_rates": cost_rates,
        "expected_horizon_return_proxy": expected,
        "warn_on_buys_only": bool(raw.get("warn_on_buys_only", True)),
    }


def evaluate_transaction_cost_gate(
    *,
    target_weights: dict[str, Any],
    current_weights: dict[str, Any],
    rebalance_actions: list[dict[str, Any]] | None,
    strategy_evidence: dict[str, Any] | None = None,
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    cfg = default_transaction_cost_gate_config(config)
    if cfg["mode"] == "disabled":
        return {
            "mode": "disabled",
            "broker": cfg["broker"],
            "status": "disabled",
            "execution_effect": "none",
            "rows": [],
            "warnings": [],
            "summary": {},
            "config": cfg,
        }

    actions = list(rebalance_actions or [])
    if not actions:
        actions = _actions_from_weights(target_weights or {}, current_weights or {})
    cards_by_ticker = _evidence_by_ticker(strategy_evidence or {})
    rows = [
        _evaluate_action(action, cards_by_ticker.get(_ticker(action)), cfg)
        for action in actions
        if _ticker(action)
    ]
    warnings = [
        row["reason"]
        for row in rows
        if row.get("verdict") in {"low_edge_to_cost", "missing_signal_edge"}
    ]
    total_cost_drag = sum(float(row.get("cost_drag") or 0.0) for row in rows)
    buy_rows = [row for row in rows if row.get("trade_action") == "buy"]
    ratios = [
        float(row["edge_to_cost_ratio"])
        for row in buy_rows
        if row.get("edge_to_cost_ratio") is not None
    ]
    return {
        "mode": cfg["mode"],
        "broker": cfg["broker"],
        "status": "ok" if not warnings else "observe_warning",
        "execution_effect": "diagnostic_only",
        "rows": rows,
        "warnings": warnings,
        "summary": {
            "action_count": len(rows),
            "buy_action_count": len(buy_rows),
            "warning_count": len(warnings),
            "total_cost_drag": round(total_cost_drag, 8),
            "min_edge_to_cost_ratio": round(min(ratios), 6) if ratios else None,
            "cost_model": f"{cfg['broker']}_return_drag_v1",
        },
        "config": cfg,
    }


def _evaluate_action(
    action: dict[str, Any],
    evidence: CostGateEvidence | None,
    cfg: dict[str, Any],
) -> dict[str, Any]:
    ticker = _ticker(action)
    trade_action = str(action.get("action") or "").strip().lower()
    delta = _to_float(action.get("weight_delta"), 0.0)
    abs_delta = abs(delta)
    bucket = _cost_bucket(ticker, evidence)
    cost_rate = float((cfg.get("cost_rates") or {}).get(bucket, DEFAULT_COST_RATES[bucket]))
    expected_proxy = float(
        (cfg.get("expected_horizon_return_proxy") or {}).get(
            bucket,
            DEFAULT_EXPECTED_HORIZON_RETURN_PROXY[bucket],
        )
    )
    cost_drag = abs_delta * cost_rate
    confidence = evidence.confidence if evidence else 0.0
    conviction_discount = evidence.conviction_discount if evidence else 0.0
    expected_edge = abs_delta * confidence * conviction_discount * expected_proxy
    edge_to_cost = (expected_edge / cost_drag) if cost_drag > 0 else None

    verdict, reason = _cost_verdict(
        ticker=ticker,
        trade_action=trade_action,
        edge_to_cost=edge_to_cost,
        evidence=evidence,
        min_ratio=float(cfg.get("min_edge_to_cost_ratio") or DEFAULT_MIN_EDGE_TO_COST_RATIO),
        warn_on_buys_only=bool(cfg.get("warn_on_buys_only", True)),
    )
    return {
        "ticker": ticker,
        "trade_action": trade_action,
        "weight_current": _to_float(action.get("weight_current"), 0.0),
        "weight_target": _to_float(action.get("weight_target"), 0.0),
        "weight_delta": round(delta, 6),
        "abs_delta": round(abs_delta, 6),
        "asset_cost_bucket": bucket,
        "estimated_cost_rate": round(cost_rate, 8),
        "cost_drag": round(cost_drag, 8),
        "strategy": evidence.strategy if evidence else None,
        "role": evidence.role if evidence else None,
        "evidence_action": evidence.action if evidence else None,
        "confidence": round(confidence, 6),
        "conviction": evidence.conviction if evidence else None,
        "conviction_status": evidence.conviction_status if evidence else "missing_profile",
        "conviction_statistical_status": (
            evidence.conviction_statistical_status if evidence else "insufficient"
        ),
        "conviction_discount": round(conviction_discount, 6),
        "expected_horizon_return_proxy": round(expected_proxy, 6),
        "expected_edge": round(expected_edge, 8),
        "edge_to_cost_ratio": round(edge_to_cost, 6) if edge_to_cost is not None else None,
        "verdict": verdict,
        "reason": reason,
    }


def _cost_verdict(
    *,
    ticker: str,
    trade_action: str,
    edge_to_cost: float | None,
    evidence: CostGateEvidence | None,
    min_ratio: float,
    warn_on_buys_only: bool,
) -> tuple[str, str]:
    if warn_on_buys_only and trade_action != "buy":
        return "diagnostic_sell", f"{ticker}: sell/reduce action is diagnostic-only for cost gate"
    if evidence is None:
        return "missing_signal_edge", f"{ticker}: missing EvidenceCard signal for buy cost check"
    evidence_action = str(evidence.action or "").strip().lower()
    if trade_action == "buy" and evidence_action not in BUY_SUPPORT_ACTIONS:
        return "missing_signal_edge", (
            f"{ticker}: EvidenceCard action {evidence_action or 'unknown'} does not support buy"
        )
    if evidence.confidence <= 0 or evidence.conviction_discount <= 0:
        return "missing_signal_edge", (
            f"{ticker}: insufficient signal/conviction for cost check "
            f"({evidence.conviction_status})"
        )
    if edge_to_cost is None:
        return "no_cost", f"{ticker}: no measurable cost drag"
    if edge_to_cost < min_ratio:
        return "low_edge_to_cost", (
            f"{ticker}: edge_to_cost_ratio {edge_to_cost:.2f} below {min_ratio:.2f}"
        )
    return "cost_supported", f"{ticker}: edge_to_cost_ratio {edge_to_cost:.2f} >= {min_ratio:.2f}"


def _evidence_by_ticker(strategy_evidence: dict[str, Any]) -> dict[str, CostGateEvidence]:
    cards: list[dict[str, Any]] = []
    if isinstance(strategy_evidence.get("evidence_cards"), list):
        cards.extend(card for card in strategy_evidence.get("evidence_cards") or [] if isinstance(card, dict))
    for result in strategy_evidence.get("strategy_results") or []:
        if not isinstance(result, dict):
            continue
        strategy_name = str(result.get("strategy_name") or "")
        for card in result.get("evidence_cards") or []:
            if not isinstance(card, dict):
                continue
            enriched = dict(card)
            enriched.setdefault("strategy", strategy_name)
            cards.append(enriched)

    out: dict[str, CostGateEvidence] = {}
    for card in cards:
        ticker = str(card.get("ticker") or "").upper().strip()
        if not ticker:
            continue
        evidence = _normalize_evidence_card(card)
        current = out.get(ticker)
        if current is None or _evidence_rank(evidence) > _evidence_rank(current):
            out[ticker] = evidence
    return out


def _normalize_evidence_card(card: dict[str, Any]) -> CostGateEvidence:
    confidence = _to_float(card.get("confidence"), 0.0)
    conviction = _optional_float(card.get("conviction"))
    effective_confidence = _optional_float(card.get("effective_confidence"))
    status = str(card.get("conviction_status") or "missing_profile")
    statistical_status = _decision_status_from_card(card, status=status)
    if effective_confidence is not None and confidence > 0:
        discount = max(min(effective_confidence / confidence, 1.0), 0.0)
    else:
        discount = _conviction_discount(statistical_status, conviction)
    return CostGateEvidence(
        strategy=card.get("strategy"),
        role=card.get("role"),
        action=card.get("action"),
        confidence=round(max(confidence, 0.0), 6),
        conviction=conviction,
        conviction_status=status,
        conviction_statistical_status=statistical_status,
        effective_confidence=effective_confidence,
        conviction_discount=round(discount, 6),
    )


def _decision_status_from_card(card: dict[str, Any], *, status: str) -> str:
    diagnostics = card.get("diagnostics") if isinstance(card.get("diagnostics"), dict) else {}
    conviction_diag = diagnostics.get("conviction") if isinstance(diagnostics.get("conviction"), dict) else {}
    n = _optional_int(card.get("conviction_n"))
    if n is None:
        n = _optional_int(conviction_diag.get("n"))
    return decision_statistical_status(
        status=(
            card.get("conviction_statistical_status")
            or card.get("statistical_status")
            or conviction_diag.get("statistical_status")
            or status
        ),
        n=n,
        diagnostics=conviction_diag,
    )


def _conviction_discount(statistical_status: str, conviction: float | None) -> float:
    if conviction is None:
        return 0.0
    base = max(min(float(conviction or 0.0), 1.0), 0.0)
    return base * decision_conviction_discount(statistical_status)


def _evidence_rank(evidence: CostGateEvidence) -> tuple[float, float, str]:
    return (
        evidence.confidence * evidence.conviction_discount,
        evidence.confidence,
        str(evidence.strategy or ""),
    )


def _cost_bucket(ticker: str, evidence: CostGateEvidence | None) -> str:
    if ticker in VOLATILITY_ETPS:
        return "volatility_etp"
    if ticker in LEVERAGED_ETFS:
        return "leveraged_etf"
    role = str((evidence.role if evidence else "") or "").lower()
    if "vol" in role and "hedge" in role:
        return "volatility_etp"
    if "leveraged" in role:
        return "leveraged_etf"
    return "ordinary_etf"


def _actions_from_weights(
    target_weights: dict[str, Any],
    current_weights: dict[str, Any],
) -> list[dict[str, Any]]:
    rows = []
    for ticker in sorted(set(target_weights) | set(current_weights)):
        ticker_key = str(ticker or "").upper().strip()
        if not ticker_key or ticker_key == "CASH":
            continue
        target = _to_float(target_weights.get(ticker), 0.0)
        current = _to_float(current_weights.get(ticker), 0.0)
        delta = target - current
        if abs(delta) <= 1e-9:
            continue
        rows.append({
            "ticker": ticker_key,
            "action": "buy" if delta > 0 else "sell",
            "weight_current": round(current, 6),
            "weight_target": round(target, 6),
            "weight_delta": round(delta, 6),
        })
    return rows


def _ticker(action: dict[str, Any]) -> str:
    return str(action.get("ticker") or "").upper().strip()


def _float_map(value: Any) -> dict[str, float]:
    if not isinstance(value, dict):
        return {}
    return {
        str(key): float(raw)
        for key, raw in value.items()
        if _optional_float(raw) is not None
    }


def _optional_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number or number in {float("inf"), float("-inf")}:
        return None
    return number


def _optional_int(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _to_float(value: Any, default: float) -> float:
    parsed = _optional_float(value)
    return default if parsed is None else parsed
