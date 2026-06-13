"""
Market condition scorecard.

This module is intentionally deterministic. It converts a structured evidence
bundle into action permissions and risk limits that downstream LLM agents can
reference and Python risk controls can enforce.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


PERMISSION_SEVERITY = {
    "aggressive_allowed": 0,
    "normal_rebalance": 1,
    "small_overweight_only": 2,
    "hold_or_trim": 3,
    "defensive_only": 4,
    "reduce_risk_only": 5,
    "cash_only": 6,
}

CONFIRMATION_DATA_QUALITY = "data_quality"
CONFIRMATION_MARKET_STRESS = "market_stress"
CONFIRMATION_STRATEGY_CONFLICT = "strategy_conflict"

DEFAULT_SCORECARD: dict[str, Any] = {
    "market_condition": "normal",
    "regime": "unknown",
    "confidence": "low",
    "trend": "unknown",
    "volatility": "normal",
    "breadth": "unknown",
    "risk_appetite": "unknown",
    "rotation": "unknown",
    "macro_risk": "unknown",
    "data_quality": "unknown",
    "investment_permission": "normal_rebalance",
    "max_adjustment_from_base": 0.05,
    "max_equity_weight": 0.90,
    "min_cash_weight": 0.05,
    "max_turnover_per_cycle": 0.30,
    "max_single_position": 0.20,
    "allow_new_positions": True,
    "require_human_confirmation": False,
    "confirmation_classes": [],
    "prefer_hedges": False,
    "reasons": [],
    "warnings": [],
}


def build_market_scorecard(evidence_bundle: dict[str, Any] | None) -> dict[str, Any]:
    """
    Build a deterministic market scorecard from an evidence bundle.

    The input is expected to follow docs/agent_evidence_scorecard_plan.md, but
    missing fields degrade conservatively instead of raising.
    """
    evidence = evidence_bundle or {}
    market = evidence.get("market") or {}
    rotation = evidence.get("rotation") or {}
    strategies = evidence.get("strategies") or {}
    data_quality = evidence.get("data_quality") or {}
    news = evidence.get("news") or {}

    base = dict(DEFAULT_SCORECARD)
    regime = str(market.get("regime") or "unknown")
    rotation_label = str(rotation.get("rotation_label") or "unknown")

    base.update(
        {
            "regime": regime,
            "confidence": str(market.get("regime_confidence") or "low"),
            "trend": _classify_trend(market),
            "volatility": _classify_volatility(market),
            "breadth": _classify_breadth(market.get("breadth_pct")),
            "risk_appetite": _classify_risk_appetite(rotation, market),
            "rotation": rotation_label,
            "macro_risk": _classify_macro_risk(news),
            "data_quality": str(data_quality.get("overall") or strategies.get("data_quality") or "unknown"),
        }
    )

    triggered = [_base_rule(base)]
    triggered.extend(_staleness_rules(evidence))
    triggered.extend(_data_quality_rules(strategies, data_quality))
    triggered.extend(_market_conflict_rules(
        regime,
        rotation_label,
        rotation,
        regime_subtype=str(market.get("regime_subtype") or ""),
    ))
    triggered.extend(_volatility_rules(market))
    triggered.extend(_drawdown_rules(market))
    triggered.extend(_turnover_rules(strategies))
    triggered.extend(_strategy_confidence_rules(strategies))

    resolved = resolve_conflicts(triggered)
    scorecard = {**base, **resolved}
    scorecard["strategy_execution_evidence"] = _strategy_execution_evidence_summary(strategies)
    scorecard["market_condition"] = _market_condition(scorecard, regime, rotation_label)
    scorecard["reasons"] = _unique_list(scorecard.get("reasons") or [])
    scorecard["warnings"] = _unique_list(scorecard.get("warnings") or [])
    return scorecard


def resolve_conflicts(triggered_rules: list[dict[str, Any]]) -> dict[str, Any]:
    """Resolve triggered scorecard rules using the most conservative limits."""
    rules = [r for r in triggered_rules if r]
    if not rules:
        rules = [_base_rule(DEFAULT_SCORECARD)]

    out: dict[str, Any] = {
        "max_adjustment_from_base": _min_value(rules, "max_adjustment_from_base", 0.05),
        "max_equity_weight": _min_value(rules, "max_equity_weight", 0.90),
        "max_turnover_per_cycle": _min_value(rules, "max_turnover_per_cycle", 0.30),
        "max_single_position": _min_value(rules, "max_single_position", 0.20),
        "min_cash_weight": _max_value(rules, "min_cash_weight", 0.05),
        "allow_new_positions": all(bool(r.get("allow_new_positions", True)) for r in rules),
        "require_human_confirmation": any(bool(r.get("require_human_confirmation", False)) for r in rules),
        "confirmation_classes": _collect_confirmation_classes(rules),
        "prefer_hedges": any(bool(r.get("prefer_hedges", False)) for r in rules),
        "investment_permission": _most_restrictive_permission(rules),
        "triggered_rules": [str(r.get("name", "unnamed_rule")) for r in rules],
        "dominant_constraint": _dominant_constraint(rules),
        "reasons": _collect(rules, "reasons"),
        "warnings": _collect(rules, "warnings"),
    }
    return out


def is_evidence_stale(evidence_bundle: dict[str, Any] | None, now: datetime | None = None) -> bool:
    evidence = evidence_bundle or {}
    generated_at = _parse_dt(evidence.get("generated_at"))
    if generated_at is None:
        return True
    max_age = _safe_float(evidence.get("max_age_seconds"), 1800.0)
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    return (current - generated_at).total_seconds() > max_age


def _base_rule(base: dict[str, Any]) -> dict[str, Any]:
    return {
        "name": "base_permissions",
        "investment_permission": base.get("investment_permission", "normal_rebalance"),
        "max_adjustment_from_base": base.get("max_adjustment_from_base", 0.05),
        "max_equity_weight": base.get("max_equity_weight", 0.90),
        "min_cash_weight": base.get("min_cash_weight", 0.05),
        "max_turnover_per_cycle": base.get("max_turnover_per_cycle", 0.30),
        "max_single_position": base.get("max_single_position", 0.20),
        "allow_new_positions": base.get("allow_new_positions", True),
        "require_human_confirmation": base.get("require_human_confirmation", False),
        "confirmation_classes": list(base.get("confirmation_classes") or []),
        "prefer_hedges": base.get("prefer_hedges", False),
        "reasons": ["Base scorecard permissions applied"],
    }


def _staleness_rules(evidence: dict[str, Any]) -> list[dict[str, Any]]:
    if not is_evidence_stale(evidence):
        return []
    return [
        {
            "name": "stale_evidence",
            "investment_permission": "hold_or_trim",
            "max_adjustment_from_base": 0.01,
            "max_turnover_per_cycle": 0.10,
            "require_human_confirmation": True,
            "confirmation_class": CONFIRMATION_DATA_QUALITY,
            "warnings": ["Evidence bundle is stale or missing freshness metadata"],
            "reasons": ["Stale evidence limits allocation changes"],
        }
    ]


def _data_quality_rules(strategies: dict[str, Any], data_quality: dict[str, Any]) -> list[dict[str, Any]]:
    rules: list[dict[str, Any]] = []
    playground_available = bool(strategies.get("playground_available", False))
    has_historical_samples = "historical_forward_return_samples" in strategies
    historical_sample_value = strategies.get(
        "historical_forward_return_samples",
        strategies.get("forward_return_samples", 0),
    )
    historical_samples = int(_safe_float(historical_sample_value, 0))
    min_samples = 30 if has_historical_samples else 10
    overall = str(data_quality.get("overall") or strategies.get("data_quality") or "").lower()

    if not playground_available:
        rules.append(
            {
                "name": "playground_missing",
                "investment_permission": "small_overweight_only",
                "max_adjustment_from_base": 0.03,
                "require_human_confirmation": True,
                "confirmation_class": CONFIRMATION_DATA_QUALITY,
                "warnings": ["No recent Playground result available"],
                "reasons": ["Strategy comparison cannot influence allocation"],
            }
        )

    if historical_samples < min_samples or overall in {"limited", "missing", "stale"}:
        reasons = []
        if historical_samples < min_samples:
            reasons.append(f"Only {historical_samples} historical forward return samples available")
        if overall in {"limited", "missing", "stale"}:
            reasons.append(f"Overall data quality is {overall}")
        rules.append(
            {
                "name": "limited_data_quality",
                "investment_permission": "small_overweight_only",
                "max_adjustment_from_base": 0.03,
                "max_turnover_per_cycle": 0.20,
                "require_human_confirmation": True,
                "confirmation_class": CONFIRMATION_DATA_QUALITY,
                "reasons": reasons,
            }
        )

    return rules


def _market_conflict_rules(
    regime: str,
    rotation_label: str,
    rotation: dict[str, Any],
    *,
    regime_subtype: str = "",
) -> list[dict[str, Any]]:
    if regime != "trending_bull":
        return []
    if regime_subtype == "bull_with_defensive_rotation":
        return []
    leader_tickers = {str((item or {}).get("ticker", "")).upper() for item in rotation.get("leaders") or []}
    bond_heavy = bool(leader_tickers & {"IEF", "TLT", "BND", "SGOV", "GLD"})
    defensive = rotation_label in {"defensive_rotation", "risk_off_rotation"}
    if not defensive and not bond_heavy:
        return []
    return [
        {
            "name": "bullish_but_mixed_rotation",
            "investment_permission": "small_overweight_only",
            "max_adjustment_from_base": 0.03,
            "max_equity_weight": 0.85,
            "require_human_confirmation": True,
            "confirmation_class": CONFIRMATION_STRATEGY_CONFLICT,
            "reasons": ["Bullish regime conflicts with defensive or bond-heavy rotation"],
        }
    ]


def _volatility_rules(market: dict[str, Any]) -> list[dict[str, Any]]:
    vix = _safe_float(market.get("vix"))
    atr = _safe_float(market.get("spy_atr_pct") or market.get("avg_atr_pct"))
    if vix > 50:
        return [
            {
                "name": "extreme_volatility",
                "investment_permission": "cash_only",
                "max_adjustment_from_base": 0.0,
                "max_equity_weight": 0.0,
                "min_cash_weight": 1.0,
                "max_turnover_per_cycle": 0.10,
                "max_single_position": 0.0,
                "allow_new_positions": False,
                "require_human_confirmation": True,
                "confirmation_class": CONFIRMATION_MARKET_STRESS,
                "prefer_hedges": True,
                "reasons": [f"VIX {vix:.1f} exceeds extreme threshold 50"],
            }
        ]
    if vix > 30 or atr > 0.025:
        reason = f"High volatility: VIX={vix:.1f}, SPY ATR={atr:.2%}"
        return [
            {
                "name": "high_volatility",
                "investment_permission": "defensive_only",
                "max_adjustment_from_base": 0.03,
                "max_equity_weight": 0.65,
                "min_cash_weight": 0.15,
                "max_single_position": 0.15,
                "require_human_confirmation": True,
                "confirmation_class": CONFIRMATION_MARKET_STRESS,
                "prefer_hedges": True,
                "reasons": [reason],
            }
        ]
    return []


def _drawdown_rules(market: dict[str, Any]) -> list[dict[str, Any]]:
    drawdown = abs(_safe_float(market.get("drawdown_pct") or market.get("current_drawdown_pct")))
    if drawdown > 0.20:
        return [
            {
                "name": "extreme_drawdown",
                "investment_permission": "cash_only",
                "max_adjustment_from_base": 0.0,
                "max_equity_weight": 0.0,
                "min_cash_weight": 1.0,
                "max_turnover_per_cycle": 0.10,
                "max_single_position": 0.0,
                "allow_new_positions": False,
                "require_human_confirmation": True,
                "confirmation_class": CONFIRMATION_MARKET_STRESS,
                "reasons": [f"Drawdown {drawdown:.1%} exceeds extreme threshold 20%"],
            }
        ]
    if drawdown > 0.10:
        return [
            {
                "name": "defensive_drawdown",
                "investment_permission": "reduce_risk_only",
                "max_adjustment_from_base": 0.02,
                "max_equity_weight": 0.50,
                "min_cash_weight": 0.20,
                "max_single_position": 0.12,
                "allow_new_positions": False,
                "require_human_confirmation": True,
                "confirmation_class": CONFIRMATION_MARKET_STRESS,
                "reasons": [f"Drawdown {drawdown:.1%} exceeds defensive threshold 10%"],
            }
        ]
    return []


def _turnover_rules(strategies: dict[str, Any]) -> list[dict[str, Any]]:
    turnovers: list[float] = []
    for item in strategies.get("strategy_results") or []:
        if not isinstance(item, dict):
            continue
        turnovers.append(_safe_float(item.get("turnover") or item.get("expected_turnover")))
    preferred = strategies.get("preferred_strategy") or {}
    if isinstance(preferred, dict):
        turnovers.append(_safe_float(preferred.get("turnover") or preferred.get("expected_turnover")))
    max_turnover = max(turnovers) if turnovers else _safe_float(strategies.get("max_turnover"), 0.0)
    if max_turnover <= 0.50:
        return []
    return [
        {
            "name": "high_strategy_turnover",
            "investment_permission": "small_overweight_only",
            "max_turnover_per_cycle": 0.20,
            "require_human_confirmation": True,
            "confirmation_class": CONFIRMATION_STRATEGY_CONFLICT,
            "warnings": [f"Strategy turnover {max_turnover:.1%} may erode returns"],
            "reasons": ["High-turnover strategy output limits action size"],
        }
    ]


def _strategy_confidence_rules(strategies: dict[str, Any]) -> list[dict[str, Any]]:
    confidence = strategies.get("strategy_confidence") or {}
    if not isinstance(confidence, dict) or not confidence:
        return []

    named_rows = [
        (str(name), row)
        for name, row in confidence.items()
        if isinstance(row, dict)
    ]
    rows = [row for _, row in named_rows]
    primary = [row for _, row in named_rows if row.get("suggested_use") == "primary"]
    advisory = [row for _, row in named_rows if row.get("suggested_use") == "advisory"]
    actionable = [
        (name, row)
        for name, row in named_rows
        if row.get("suggested_use") in {"primary", "advisory"}
    ]
    execution_grade = _execution_grade_strategy_rows(strategies, actionable)
    consensus_conflict = any(bool(row.get("consensus_conflict")) for row in rows)
    best = max((_safe_float(row.get("confidence_score")) for row in rows), default=0.0)

    rules: list[dict[str, Any]] = []
    if consensus_conflict:
        rules.append({
            "name": "strategy_consensus_regime_conflict",
            "investment_permission": "small_overweight_only",
            "max_adjustment_from_base": 0.03,
            "max_turnover_per_cycle": 0.20,
            "require_human_confirmation": True,
            "confirmation_class": CONFIRMATION_STRATEGY_CONFLICT,
            "warnings": ["Strategy consensus conflicts with current regime; do not follow consensus weights directly"],
            "reasons": ["Playground consensus is defensive while regime evidence is risk-on"],
        })

    if not actionable:
        rules.append({
            "name": "no_actionable_strategy_confidence",
            "investment_permission": "hold_or_trim",
            "max_adjustment_from_base": 0.01,
            "max_turnover_per_cycle": 0.10,
            "require_human_confirmation": True,
            "confirmation_class": CONFIRMATION_DATA_QUALITY,
            "warnings": ["No strategy has actionable confidence"],
            "reasons": [f"Best strategy confidence={best:.2f}"],
        })
    elif not execution_grade:
        has_certification = bool(((strategies.get("strategy_certification") or {}).get("items") or {}))
        rule_name = "insufficient_execution_evidence" if has_certification else "strategy_advisory_only"
        warnings = (
            ["Strategy evidence is not certified for automatic execution; use direction, not full target weights"]
            if has_certification
            else ["Best strategy confidence is advisory-only; use direction, not full target weights"]
        )
        reasons = [f"No execution-grade strategy evidence; best confidence={best:.2f}"]
        failures = _execution_evidence_failures(strategies, actionable)
        if failures:
            reasons.append("Failed evidence checks: " + ", ".join(failures[:5]))
        rules.append({
            "name": rule_name,
            "investment_permission": "small_overweight_only",
            "max_adjustment_from_base": 0.03,
            "max_turnover_per_cycle": 0.20,
            "require_human_confirmation": True,
            "confirmation_class": CONFIRMATION_DATA_QUALITY,
            "warnings": warnings,
            "reasons": reasons,
        })

    return rules


def _execution_grade_strategy_rows(
    strategies: dict[str, Any],
    actionable: list[tuple[str, dict[str, Any]]],
) -> list[dict[str, Any]]:
    cert_items = ((strategies.get("strategy_certification") or {}).get("items") or {})
    has_certification = isinstance(cert_items, dict) and bool(cert_items)
    rows: list[dict[str, Any]] = []
    for name, row in actionable:
        if has_certification:
            cert = cert_items.get(name) if isinstance(cert_items.get(name), dict) else {}
            if (
                cert.get("approved_use") == "advisory"
                and cert.get("execution_evidence_status") == "execution_grade_validated"
            ):
                rows.append(row)
        elif row.get("suggested_use") == "primary":
            rows.append(row)
    return rows


def _execution_evidence_failures(
    strategies: dict[str, Any],
    actionable: list[tuple[str, dict[str, Any]]],
) -> list[str]:
    cert_items = ((strategies.get("strategy_certification") or {}).get("items") or {})
    if not isinstance(cert_items, dict) or not cert_items:
        return []
    failures: list[str] = []
    for name, _row in actionable:
        cert = cert_items.get(name) if isinstance(cert_items.get(name), dict) else {}
        checks = cert.get("evidence_checks") if isinstance(cert.get("evidence_checks"), dict) else {}
        for item in checks.get("failed") or []:
            text = f"{name}:{item}"
            if text not in failures:
                failures.append(text)
    return failures


def _strategy_execution_evidence_summary(strategies: dict[str, Any]) -> dict[str, Any]:
    cert_items = ((strategies.get("strategy_certification") or {}).get("items") or {})
    confidence = strategies.get("strategy_confidence") or {}
    if not isinstance(cert_items, dict) or not cert_items:
        return {
            "schema_version": "strategy_execution_evidence_summary_v1",
            "available": False,
            "execution_grade_strategy_count": 0,
            "insufficient_execution_evidence_count": 0,
            "rows": [],
        }
    rows: list[dict[str, Any]] = []
    for name, cert in sorted(cert_items.items()):
        if not isinstance(cert, dict):
            continue
        confidence_row = confidence.get(name) if isinstance(confidence.get(name), dict) else {}
        suggested_use = str(confidence_row.get("suggested_use") or cert.get("suggested_use") or "watch_only")
        execution_status = str(cert.get("execution_evidence_status") or "unknown")
        evidence_checks = cert.get("evidence_checks") if isinstance(cert.get("evidence_checks"), dict) else {}
        rows.append({
            "strategy_name": name,
            "suggested_use": suggested_use,
            "certification_status": cert.get("status"),
            "approved_use": cert.get("approved_use"),
            "execution_evidence_status": execution_status,
            "failed_checks": evidence_checks.get("failed") or [],
            "evidence_checks": evidence_checks,
        })
    return {
        "schema_version": "strategy_execution_evidence_summary_v1",
        "available": True,
        "execution_grade_strategy_count": sum(
            1 for row in rows if row.get("execution_evidence_status") == "execution_grade_validated"
        ),
        "insufficient_execution_evidence_count": sum(
            1 for row in rows if row.get("execution_evidence_status") == "insufficient_execution_evidence"
        ),
        "rows": rows,
    }


def _market_condition(scorecard: dict[str, Any], regime: str, rotation_label: str) -> str:
    permission = scorecard.get("investment_permission")
    if permission == "cash_only":
        return "cash_only"
    if permission == "reduce_risk_only":
        return "defensive"
    if scorecard.get("volatility") == "high":
        return "high_volatility"
    if regime == "trending_bull" and scorecard.get("dominant_constraint") == "bullish_but_mixed_rotation":
        return "bullish_but_mixed"
    if regime == "trending_bull" and rotation_label in {"defensive_rotation", "risk_off_rotation"}:
        return "bullish_but_mixed"
    if regime == "trending_bull":
        return "bullish"
    if regime == "trending_bear":
        return "bearish"
    return regime or "unknown"


def _classify_trend(market: dict[str, Any]) -> str:
    mom20 = _safe_float(market.get("spy_mom_20d"))
    mom60 = _safe_float(market.get("spy_mom_60d"))
    if mom20 > 0 and mom60 > 0:
        return "positive"
    if mom20 < 0 and mom60 < 0:
        return "negative"
    return "mixed"


def _classify_volatility(market: dict[str, Any]) -> str:
    vix = _safe_float(market.get("vix"))
    atr = _safe_float(market.get("spy_atr_pct") or market.get("avg_atr_pct"))
    if vix > 50:
        return "extreme"
    if vix > 30 or atr > 0.025:
        return "high"
    return "normal"


def _classify_breadth(value: Any) -> str:
    breadth = _safe_float(value, -1.0)
    if breadth < 0:
        return "unknown"
    if breadth >= 0.65:
        return "broad"
    if breadth >= 0.45:
        return "moderate"
    return "weak"


def _classify_risk_appetite(rotation: dict[str, Any], market: dict[str, Any]) -> str:
    label = str(rotation.get("rotation_label") or "")
    if label in {"risk_on_rotation"}:
        return "risk_on"
    if label in {"defensive_rotation", "risk_off_rotation"}:
        return "risk_off"
    score = _safe_float(rotation.get("risk_appetite_score") or market.get("risk_on_score"))
    if score > 0.015:
        return "risk_on"
    if score < -0.015:
        return "risk_off"
    return "mixed"


def _classify_macro_risk(news: dict[str, Any]) -> str:
    signals = news.get("macro_signals") or []
    if not signals:
        return "unknown"
    negative = 0
    for item in signals:
        if not isinstance(item, dict):
            continue
        direction = str(item.get("direction") or item.get("impact_bias") or "").lower()
        if direction in {"negative", "bearish", "risk_off"}:
            negative += 1
    if negative >= 2:
        return "high"
    if negative == 1:
        return "medium"
    return "low"


def _most_restrictive_permission(rules: list[dict[str, Any]]) -> str:
    permissions = [str(r.get("investment_permission", "normal_rebalance")) for r in rules]
    return max(permissions, key=lambda p: PERMISSION_SEVERITY.get(p, 1))


def _dominant_constraint(rules: list[dict[str, Any]]) -> str:
    return max(
        rules,
        key=lambda r: PERMISSION_SEVERITY.get(str(r.get("investment_permission", "normal_rebalance")), 1),
    ).get("name", "base_permissions")


def _min_value(rules: list[dict[str, Any]], key: str, default: float) -> float:
    vals = [_safe_float(r.get(key), default) for r in rules if r.get(key) is not None]
    return min(vals) if vals else default


def _max_value(rules: list[dict[str, Any]], key: str, default: float) -> float:
    vals = [_safe_float(r.get(key), default) for r in rules if r.get(key) is not None]
    return max(vals) if vals else default


def _collect(rules: list[dict[str, Any]], key: str) -> list[str]:
    out: list[str] = []
    for rule in rules:
        value = rule.get(key) or []
        if isinstance(value, str):
            out.append(value)
        else:
            out.extend(str(item) for item in value)
    return _unique_list(out)


def _collect_confirmation_classes(rules: list[dict[str, Any]]) -> list[str]:
    out: list[str] = []
    for rule in rules:
        if not bool(rule.get("require_human_confirmation", False)):
            continue
        value = rule.get("confirmation_classes", rule.get("confirmation_class"))
        if isinstance(value, str):
            out.append(value)
        elif isinstance(value, (list, tuple, set)):
            out.extend(str(item) for item in value)
    return _unique_list([item for item in out if item])


def _unique_list(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        try:
            dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value) if value is not None else default
    except (TypeError, ValueError):
        return default
