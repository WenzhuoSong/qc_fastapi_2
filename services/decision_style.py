"""
Deterministic decision style resolver.

This layer chooses how agents should interpret evidence and execute changes.
It does not create target weights and it does not loosen scorecard or risk
manager constraints.
"""
from __future__ import annotations

from typing import Any


CONVICTION_THRESHOLDS = {
    "normal_rebalance": 0.60,
    "step_in": 0.30,
    "hold_unless_strong": 0.10,
    "risk_reduce_fast": -0.20,
    "cash_only": -0.50,
}

ANALYSIS_STYLE_SEVERITY = {
    "balanced": 0,
    "momentum_confirmed": 1,
    "low_turnover": 2,
    "conservative": 3,
    "macro_defensive": 4,
}

TRADE_STYLE_SEVERITY = {
    "normal_rebalance": 0,
    "step_in": 1,
    "hold_unless_strong": 2,
    "risk_reduce_fast": 3,
    "cash_only": 4,
}

PERMISSION_TRADE_CAP = {
    "aggressive_allowed": "normal_rebalance",
    "normal_rebalance": "normal_rebalance",
    "small_overweight_only": "step_in",
    "hold_or_trim": "hold_unless_strong",
    "defensive_only": "risk_reduce_fast",
    "reduce_risk_only": "risk_reduce_fast",
    "cash_only": "cash_only",
}

STYLE_WEIGHTS = {
    "balanced": {"quant_weight": 1.0, "news_weight": 1.0, "macro_weight": 1.0, "risk_weight": 1.0},
    "conservative": {"quant_weight": 0.9, "news_weight": 1.2, "macro_weight": 1.1, "risk_weight": 1.4},
    "momentum_confirmed": {"quant_weight": 1.3, "news_weight": 0.8, "macro_weight": 0.8, "risk_weight": 1.0},
    "macro_defensive": {"quant_weight": 0.8, "news_weight": 1.4, "macro_weight": 1.5, "risk_weight": 1.4},
    "low_turnover": {"quant_weight": 0.9, "news_weight": 0.9, "macro_weight": 1.0, "risk_weight": 1.1},
}

BASE_STYLE_LIMITS = {
    "max_adjustment_multiplier": 1.0,
    "max_turnover_per_cycle": 0.30,
    "max_single_trade_pct": 0.05,
    "max_new_buys_per_cycle": 3,
    "min_cash_floor_addition": 0.0,
    "rebalance_threshold_boost": 0.0,
    "allow_new_positions": True,
    "prefer_hedges": False,
    "sell_priority": False,
}


def resolve_decision_style(
    *,
    market_scorecard: dict[str, Any] | None,
    news_evidence: dict[str, Any] | None = None,
    strategy_evidence: dict[str, Any] | None = None,
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Select analysis and trade styles from scorecard, news, and strategy evidence.

    This function is intentionally standalone for Phase 2; pipeline wiring comes
    later.
    """
    scorecard = market_scorecard or {}
    news = news_evidence or {}
    strategies = strategy_evidence or {}
    cfg = config or {}

    component_scores = compute_component_scores(scorecard, news, strategies)
    forced_analysis = str(cfg.get("force_analysis_style") or "")
    weight_style = forced_analysis if forced_analysis in STYLE_WEIGHTS else "balanced"
    weighted_conviction = compute_weighted_conviction(component_scores, analysis_style=weight_style)

    base_trade = conviction_to_style(weighted_conviction, scorecard)["trade_style"]
    if base_trade == "cash_only" and scorecard.get("investment_permission") != "cash_only":
        base_trade = "risk_reduce_fast"
    if str(cfg.get("force_trade_style") or "") in TRADE_STYLE_SEVERITY:
        base_trade = str(cfg.get("force_trade_style"))

    triggered = [
        _style_rule(
            "base_style",
            "balanced",
            base_trade,
            source="weighted_conviction",
            reasons=["Base style from weighted conviction"],
        )
    ]
    triggered.extend(_scorecard_rules(scorecard))
    triggered.extend(_news_rules(news))
    triggered.extend(_strategy_rules(strategies))
    triggered.extend(_momentum_rule(scorecard, news, strategies))
    triggered.extend(_forced_rules(cfg))

    resolved = resolve_style_conflicts(triggered)
    resolved["weighted_conviction"] = round(weighted_conviction, 4)
    resolved["component_scores"] = component_scores
    resolved["style_weights"] = STYLE_WEIGHTS.get(resolved["analysis_style"], STYLE_WEIGHTS["balanced"])
    resolved["causal_sources"] = _causal_sources(triggered)
    resolved["news_style_influence"] = _news_style_influence(triggered)
    resolved["style_reason"] = _style_reason(resolved)
    return resolved


def compute_weighted_conviction(
    component_scores: dict[str, float],
    *,
    analysis_style: str = "balanced",
    weights: dict[str, float] | None = None,
) -> float:
    style_weights = dict(STYLE_WEIGHTS.get(analysis_style, STYLE_WEIGHTS["balanced"]))
    if weights:
        style_weights.update({k: _safe_float(v, style_weights.get(k, 1.0)) for k, v in weights.items()})

    value = (
        component_scores.get("quant_score", 0.0) * style_weights.get("quant_weight", 1.0)
        + component_scores.get("news_score", 0.0) * style_weights.get("news_weight", 1.0)
        + component_scores.get("macro_score", 0.0) * style_weights.get("macro_weight", 1.0)
        - component_scores.get("risk_penalty", 0.0) * style_weights.get("risk_weight", 1.0)
    )
    return _clamp(value, -1.0, 1.0)


def compute_component_scores(
    scorecard: dict[str, Any] | None,
    news_evidence: dict[str, Any] | None,
    strategy_evidence: dict[str, Any] | None,
) -> dict[str, float]:
    scorecard = scorecard or {}
    news_evidence = news_evidence or {}
    strategy_evidence = strategy_evidence or {}
    return {
        "quant_score": round(_quant_score(scorecard, strategy_evidence), 4),
        "news_score": round(_news_score(news_evidence), 4),
        "macro_score": round(_macro_score(news_evidence), 4),
        "risk_penalty": round(_risk_penalty(scorecard, strategy_evidence, news_evidence), 4),
    }


def conviction_to_style(weighted_conviction: float, scorecard: dict[str, Any] | None) -> dict[str, Any]:
    """Map weighted conviction to a trade style, capped by scorecard permission."""
    scorecard = scorecard or {}
    conviction = _safe_float(weighted_conviction)
    if conviction >= CONVICTION_THRESHOLDS["normal_rebalance"]:
        raw = "normal_rebalance"
    elif conviction >= CONVICTION_THRESHOLDS["step_in"]:
        raw = "step_in"
    elif conviction >= CONVICTION_THRESHOLDS["risk_reduce_fast"]:
        raw = "hold_unless_strong"
    elif conviction >= CONVICTION_THRESHOLDS["cash_only"]:
        raw = "risk_reduce_fast"
    else:
        raw = "cash_only"

    cap = PERMISSION_TRADE_CAP.get(str(scorecard.get("investment_permission") or "normal_rebalance"), "normal_rebalance")
    capped = _more_restrictive_trade(raw, cap)
    return {
        "raw_trade_style": raw,
        "trade_style": capped,
        "scorecard_cap": cap,
        "capped_by_scorecard": capped != raw,
    }


def resolve_style_conflicts(triggered_styles: list[dict[str, Any]]) -> dict[str, Any]:
    """Resolve multiple style rules using conservative intersections."""
    rules = [rule for rule in triggered_styles if rule]
    if not rules:
        rules = [_style_rule("base_style", "balanced", "normal_rebalance")]

    limits = dict(BASE_STYLE_LIMITS)
    limits.update(
        {
            "max_adjustment_multiplier": _min_limit(rules, "max_adjustment_multiplier", BASE_STYLE_LIMITS["max_adjustment_multiplier"]),
            "max_turnover_per_cycle": _min_limit(rules, "max_turnover_per_cycle", BASE_STYLE_LIMITS["max_turnover_per_cycle"]),
            "max_single_trade_pct": _min_limit(rules, "max_single_trade_pct", BASE_STYLE_LIMITS["max_single_trade_pct"]),
            "max_new_buys_per_cycle": int(_min_limit(rules, "max_new_buys_per_cycle", BASE_STYLE_LIMITS["max_new_buys_per_cycle"])),
            "min_cash_floor_addition": _max_limit(rules, "min_cash_floor_addition", BASE_STYLE_LIMITS["min_cash_floor_addition"]),
            "rebalance_threshold_boost": _max_limit(rules, "rebalance_threshold_boost", BASE_STYLE_LIMITS["rebalance_threshold_boost"]),
            "allow_new_positions": all(bool(_style_limits(rule).get("allow_new_positions", True)) for rule in rules),
            "prefer_hedges": any(bool(_style_limits(rule).get("prefer_hedges", False)) for rule in rules),
            "sell_priority": any(bool(_style_limits(rule).get("sell_priority", False)) for rule in rules),
        }
    )

    analysis_style = max(
        (str(rule.get("analysis_style") or "balanced") for rule in rules),
        key=lambda style: ANALYSIS_STYLE_SEVERITY.get(style, 0),
    )
    trade_style = max(
        (str(rule.get("trade_style") or "normal_rebalance") for rule in rules),
        key=lambda style: TRADE_STYLE_SEVERITY.get(style, 0),
    )
    dominant = max(
        rules,
        key=lambda rule: (
            TRADE_STYLE_SEVERITY.get(str(rule.get("trade_style") or "normal_rebalance"), 0),
            ANALYSIS_STYLE_SEVERITY.get(str(rule.get("analysis_style") or "balanced"), 0),
        ),
    )

    return {
        "analysis_style": analysis_style,
        "trade_style": trade_style,
        "style_limits": limits,
        "dominant_style_constraint": str(dominant.get("name") or "base_style"),
        "triggered_style_rules": [str(rule.get("name") or "unnamed_style_rule") for rule in rules],
        "reasons": _collect(rules, "reasons"),
        "warnings": _collect(rules, "warnings"),
    }


def apply_style_limits(base_limits: dict[str, Any] | None, decision_style: dict[str, Any] | None) -> dict[str, Any]:
    """
    Apply style limits to a scorecard/risk limit dict.

    Style limits can only tighten. `min_cash_floor_addition` is additive against
    the existing cash floor.
    """
    base = dict(base_limits or {})
    style = (decision_style or {}).get("style_limits") or {}
    out = dict(base)
    if "max_adjustment_from_base" in out:
        out["max_adjustment_from_base"] = _safe_float(out["max_adjustment_from_base"]) * _safe_float(
            style.get("max_adjustment_multiplier"), 1.0
        )
    for key in ("max_turnover_per_cycle", "max_single_trade_pct", "max_new_buys_per_cycle"):
        if key in style:
            base_key = key if key in out else key.replace("max_single_trade_pct", "max_single_position")
            current = _safe_float(out.get(base_key), _safe_float(style.get(key)))
            out[base_key] = min(current, _safe_float(style.get(key)))
    if "min_cash_weight" in out:
        out["min_cash_weight"] = min(
            1.0,
            _safe_float(out.get("min_cash_weight")) + _safe_float(style.get("min_cash_floor_addition")),
        )
    if style.get("allow_new_positions") is False:
        out["allow_new_positions"] = False
    if style.get("prefer_hedges"):
        out["prefer_hedges"] = True
    return out


def _scorecard_rules(scorecard: dict[str, Any]) -> list[dict[str, Any]]:
    permission = str(scorecard.get("investment_permission") or "normal_rebalance")
    rules: list[dict[str, Any]] = []
    if permission == "cash_only":
        rules.append(
            _style_rule(
                "scorecard_cash_only",
                "macro_defensive",
                "cash_only",
                source="scorecard",
                limits={
                    "max_adjustment_multiplier": 0.0,
                    "max_turnover_per_cycle": 0.10,
                    "max_single_trade_pct": 0.0,
                    "max_new_buys_per_cycle": 0,
                    "min_cash_floor_addition": 1.0,
                    "allow_new_positions": False,
                    "prefer_hedges": True,
                    "sell_priority": True,
                },
                reasons=["Market scorecard requires cash-only positioning"],
            )
        )
    elif permission in {"reduce_risk_only", "defensive_only"}:
        rules.append(
            _style_rule(
                "scorecard_defensive_permission",
                "macro_defensive",
                "risk_reduce_fast",
                source="scorecard",
                limits={
                    "max_adjustment_multiplier": 0.5,
                    "max_turnover_per_cycle": 0.10,
                    "max_single_trade_pct": 0.03,
                    "max_new_buys_per_cycle": 0,
                    "min_cash_floor_addition": 0.08,
                    "allow_new_positions": False,
                    "prefer_hedges": True,
                    "sell_priority": True,
                },
                reasons=[f"Market scorecard permission is {permission}"],
            )
        )
    elif permission == "hold_or_trim":
        rules.append(
            _style_rule(
                "scorecard_hold_or_trim",
                "conservative",
                "hold_unless_strong",
                source="scorecard",
                limits={"max_adjustment_multiplier": 0.5, "max_turnover_per_cycle": 0.10, "max_new_buys_per_cycle": 0},
                reasons=["Market scorecard only allows hold-or-trim actions"],
            )
        )
    elif permission == "small_overweight_only":
        rules.append(
            _style_rule(
                "scorecard_small_overweight",
                "conservative",
                "step_in",
                source="scorecard",
                limits={
                    "max_adjustment_multiplier": 0.7,
                    "max_turnover_per_cycle": 0.15,
                    "max_single_trade_pct": 0.04,
                    "max_new_buys_per_cycle": 2,
                    "min_cash_floor_addition": 0.03,
                },
                reasons=["Market scorecard limits changes to small overweights"],
            )
        )

    if str(scorecard.get("data_quality") or "").lower() in {"limited", "missing", "stale", "unknown"}:
        rules.append(_limited_data_rule("scorecard_data_quality", f"Scorecard data quality is {scorecard.get('data_quality')}"))
    if str(scorecard.get("volatility") or "").lower() in {"high", "extreme"}:
        rules.append(
            _style_rule(
                "scorecard_high_volatility",
                "macro_defensive",
                "risk_reduce_fast",
                source="scorecard",
                limits={"max_adjustment_multiplier": 0.6, "min_cash_floor_addition": 0.08, "prefer_hedges": True},
                reasons=["High volatility shifts decision style to macro defensive"],
            )
        )
    return rules


def _news_rules(news: dict[str, Any]) -> list[dict[str, Any]]:
    rules: list[dict[str, Any]] = []
    macro = news.get("macro_news_score") or {}
    if macro.get("overall_bias") == "negative" and macro.get("market_impact") == "high":
        rules.append(
            _style_rule(
                "macro_negative_high_impact",
                "macro_defensive",
                "hold_unless_strong",
                source="news",
                limits={
                    "max_adjustment_multiplier": 0.6,
                    "max_turnover_per_cycle": 0.10,
                    "max_single_trade_pct": 0.03,
                    "max_buy_trade_pct": 0.02,
                    "max_new_buys_per_cycle": 1,
                    "min_cash_floor_addition": 0.05,
                    "prefer_hedges": True,
                    "sell_priority": True,
                },
                reasons=["High-impact negative macro news tightens risk expansion without hard-blocking new positions"],
                warnings=["news_advisory_tightening_only"],
            )
        )
    if news.get("hard_risk_events"):
        rules.append(
            _style_rule(
                "hard_risk_news_event",
                "macro_defensive",
                "risk_reduce_fast",
                source="news_hard_risk",
                limits={
                    "max_adjustment_multiplier": 0.5,
                    "max_new_buys_per_cycle": 0,
                    "allow_new_positions": False,
                    "prefer_hedges": True,
                    "sell_priority": True,
                },
                reasons=["Hard risk news event detected"],
            )
        )
    if macro.get("data_quality") in {"limited", "missing", "stale"}:
        rules.append(_limited_data_rule("news_data_quality", f"News data quality is {macro.get('data_quality')}"))
    return rules


def _strategy_rules(strategies: dict[str, Any]) -> list[dict[str, Any]]:
    rules: list[dict[str, Any]] = []
    data_quality = str(strategies.get("data_quality") or "").lower()
    has_historical_samples = "historical_forward_return_samples" in strategies
    historical_sample_value = strategies.get(
        "historical_forward_return_samples",
        strategies.get("forward_return_samples", 999),
    )
    historical_samples = int(_safe_float(historical_sample_value, 999))
    min_samples = 30 if has_historical_samples else 10
    historical_supported = data_quality == "historical_supported"
    if (
        data_quality in {"limited", "missing", "stale"}
        or (historical_samples < min_samples and not historical_supported)
    ):
        reason = f"Strategy evidence limited: historical_forward_samples={historical_samples}, data_quality={data_quality or 'unknown'}"
        rules.append(_limited_data_rule("strategy_data_quality", reason))

    max_turnover = _max_strategy_turnover(strategies)
    if max_turnover > 0.50:
        rules.append(
            _style_rule(
                "high_turnover_strategy",
                "low_turnover",
                "hold_unless_strong",
                source="strategy",
                limits={
                    "max_adjustment_multiplier": 0.7,
                    "max_turnover_per_cycle": 0.10,
                    "rebalance_threshold_boost": 0.02,
                },
                reasons=[f"Strategy turnover {max_turnover:.1%} is high"],
            )
        )
    return rules


def _momentum_rule(scorecard: dict[str, Any], news: dict[str, Any], strategies: dict[str, Any]) -> list[dict[str, Any]]:
    macro = news.get("macro_news_score") or {}
    if (
        scorecard.get("regime") == "trending_bull"
        and scorecard.get("breadth") == "broad"
        and scorecard.get("risk_appetite") == "risk_on"
        and macro.get("overall_bias") not in {"negative"}
        and _strategy_quality_ok(strategies)
    ):
        return [
            _style_rule(
                "momentum_confirmed",
                "momentum_confirmed",
                "normal_rebalance",
                source="cross_evidence",
                limits={"max_adjustment_multiplier": 1.1},
                reasons=["Bullish trend has breadth, risk appetite, and usable strategy confirmation"],
            )
        ]
    return []


def _forced_rules(config: dict[str, Any]) -> list[dict[str, Any]]:
    rules: list[dict[str, Any]] = []
    analysis = str(config.get("force_analysis_style") or "")
    trade = str(config.get("force_trade_style") or "")
    if analysis in ANALYSIS_STYLE_SEVERITY or trade in TRADE_STYLE_SEVERITY:
        rules.append(
            _style_rule(
                "forced_style_config",
                analysis if analysis in ANALYSIS_STYLE_SEVERITY else "balanced",
                trade if trade in TRADE_STYLE_SEVERITY else "normal_rebalance",
                source="config",
                limits=_limits_for_forced_style(analysis, trade),
                reasons=["Style selected by system configuration"],
            )
        )
    return rules


def _limits_for_forced_style(analysis: str, trade: str) -> dict[str, Any]:
    limits: dict[str, Any] = {}
    if analysis == "conservative":
        limits.update({"max_adjustment_multiplier": 0.6, "min_cash_floor_addition": 0.05})
    elif analysis == "macro_defensive":
        limits.update({"max_adjustment_multiplier": 0.7, "min_cash_floor_addition": 0.08, "prefer_hedges": True})
    elif analysis == "low_turnover":
        limits.update({"max_turnover_per_cycle": 0.10, "rebalance_threshold_boost": 0.02})
    elif analysis == "momentum_confirmed":
        limits.update({"max_adjustment_multiplier": 1.1})

    if trade == "step_in":
        limits.update({"max_single_trade_pct": 0.04, "max_new_buys_per_cycle": 2, "max_turnover_per_cycle": 0.15})
    elif trade == "hold_unless_strong":
        limits.update({"max_adjustment_multiplier": 0.5, "max_turnover_per_cycle": 0.10})
    elif trade == "risk_reduce_fast":
        limits.update({"max_buy_trade_pct": 0.03, "allow_new_positions": False, "sell_priority": True})
    elif trade == "cash_only":
        limits.update({"max_adjustment_multiplier": 0.0, "max_new_buys_per_cycle": 0, "allow_new_positions": False})
    return limits


def _limited_data_rule(name: str, reason: str) -> dict[str, Any]:
    return _style_rule(
        name,
        "conservative",
        "step_in",
        source="data_quality",
        limits={
            "max_adjustment_multiplier": 0.6,
            "max_turnover_per_cycle": 0.15,
            "max_single_trade_pct": 0.04,
            "max_new_buys_per_cycle": 2,
            "min_cash_floor_addition": 0.05,
        },
        reasons=[reason],
    )


def _style_rule(
    name: str,
    analysis_style: str,
    trade_style: str,
    *,
    source: str = "system",
    limits: dict[str, Any] | None = None,
    reasons: list[str] | None = None,
    warnings: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "name": name,
        "source": source,
        "analysis_style": analysis_style,
        "trade_style": trade_style,
        "style_limits": limits or {},
        "reasons": reasons or [],
        "warnings": warnings or [],
    }


def _causal_sources(rules: list[dict[str, Any]]) -> dict[str, Any]:
    distribution: dict[str, int] = {}
    for rule in rules:
        source = str(rule.get("source") or "system")
        distribution[source] = distribution.get(source, 0) + 1
    return {
        "sources": sorted(distribution),
        "source_distribution": dict(sorted(distribution.items())),
        "triggered_rules": [str(rule.get("name") or "") for rule in rules if rule.get("name")],
    }


def _news_style_influence(rules: list[dict[str, Any]]) -> dict[str, Any]:
    news_rules = [
        rule for rule in rules
        if str(rule.get("source") or "").startswith("news")
    ]
    hard_rules = [
        rule for rule in news_rules
        if str(rule.get("source") or "") == "news_hard_risk"
        or bool((rule.get("style_limits") or {}).get("allow_new_positions") is False)
    ]
    advisory_rules = [rule for rule in news_rules if rule not in hard_rules]
    return {
        "present": bool(news_rules),
        "triggered_rules": [str(rule.get("name") or "") for rule in news_rules],
        "advisory_tightening_rules": [str(rule.get("name") or "") for rule in advisory_rules],
        "hard_blocking_rules": [str(rule.get("name") or "") for rule in hard_rules],
        "can_block_new_positions": bool(hard_rules),
        "effect": (
            "hard_risk_defensive"
            if hard_rules
            else "advisory_tightening_only"
            if advisory_rules
            else "none"
        ),
        "contract": (
            "ordinary news may tighten style but only hard_risk news events may hard-block new positions"
        ),
    }


def _quant_score(scorecard: dict[str, Any], strategies: dict[str, Any]) -> float:
    score = 0.0
    if scorecard.get("regime") == "trending_bull":
        score += 0.35
    elif scorecard.get("regime") == "trending_bear":
        score -= 0.35
    if scorecard.get("breadth") == "broad":
        score += 0.20
    elif scorecard.get("breadth") == "weak":
        score -= 0.20
    if scorecard.get("risk_appetite") == "risk_on":
        score += 0.20
    elif scorecard.get("risk_appetite") == "risk_off":
        score -= 0.25
    if _strategy_quality_ok(strategies):
        score += 0.15
    if _max_strategy_turnover(strategies) > 0.50:
        score -= 0.15
    return _clamp(score, -1.0, 1.0)


def _news_score(news: dict[str, Any]) -> float:
    scores = []
    for item in (news.get("ticker_news_scores") or {}).values():
        if not isinstance(item, dict):
            continue
        bias = str(item.get("action_bias") or "ignore")
        credibility = _safe_float(item.get("effective_credibility") or item.get("source_credibility"), 0.0)
        if bias == "allow_overweight":
            scores.append(1.0 * credibility)
        elif bias == "confirm_existing_signal":
            scores.append(0.4 * credibility)
        elif bias == "reduce_or_wait":
            scores.append(-0.7 * credibility)
        elif bias == "block_new_buy":
            scores.append(-1.0 * max(credibility, 0.6))
    if not scores:
        return 0.0
    return _clamp(sum(scores) / max(sum(abs(s) for s in scores), 1.0), -1.0, 1.0)


def _macro_score(news: dict[str, Any]) -> float:
    macro = news.get("macro_news_score") or {}
    base = {
        "positive": 0.6,
        "negative": -0.6,
        "mixed": 0.0,
        "neutral": 0.0,
    }.get(str(macro.get("overall_bias") or "neutral"), 0.0)
    if macro.get("market_impact") == "high":
        base *= 1.25
    if macro.get("confidence") == "low":
        base *= 0.6
    return _clamp(base, -1.0, 1.0)


def _risk_penalty(scorecard: dict[str, Any], strategies: dict[str, Any], news: dict[str, Any]) -> float:
    penalty = 0.0
    if scorecard.get("volatility") == "high":
        penalty += 0.35
    elif scorecard.get("volatility") == "extreme":
        penalty += 0.70
    if str(scorecard.get("data_quality") or "").lower() in {"limited", "missing", "stale", "unknown"}:
        penalty += 0.25
    permission = str(scorecard.get("investment_permission") or "")
    if permission in {"defensive_only", "reduce_risk_only"}:
        penalty += 0.45
    elif permission == "cash_only":
        penalty += 1.0
    if bool(scorecard.get("require_human_confirmation")):
        penalty += 0.15
    if str(strategies.get("data_quality") or "").lower() in {"limited", "missing", "stale"}:
        penalty += 0.20
    if _max_strategy_turnover(strategies) > 0.50:
        penalty += 0.15
    if news.get("hard_risk_events"):
        penalty += 0.50
    return _clamp(penalty, 0.0, 1.0)


def _strategy_quality_ok(strategies: dict[str, Any]) -> bool:
    if not strategies:
        return True
    data_quality = str(strategies.get("data_quality") or "fresh").lower()
    if data_quality == "historical_supported":
        return True
    snapshots = int(_safe_float(strategies.get("snapshot_count"), 999))
    samples = int(_safe_float(strategies.get("forward_return_samples"), 999))
    return data_quality not in {"limited", "missing", "stale"} and snapshots >= 20 and samples >= 10


def _max_strategy_turnover(strategies: dict[str, Any]) -> float:
    turnovers = [_safe_float(strategies.get("max_turnover"), 0.0)]
    for item in strategies.get("strategy_results") or []:
        if isinstance(item, dict):
            turnovers.append(_safe_float(item.get("turnover") or item.get("expected_turnover"), 0.0))
    preferred = strategies.get("preferred_strategy") or {}
    if isinstance(preferred, dict):
        turnovers.append(_safe_float(preferred.get("turnover") or preferred.get("expected_turnover"), 0.0))
    return max(turnovers) if turnovers else 0.0


def _more_restrictive_trade(a: str, b: str) -> str:
    return max((a, b), key=lambda style: TRADE_STYLE_SEVERITY.get(style, 0))


def _style_limits(rule: dict[str, Any]) -> dict[str, Any]:
    return rule.get("style_limits") or {}


def _min_limit(rules: list[dict[str, Any]], key: str, default: float) -> float:
    vals = [_safe_float(_style_limits(rule).get(key), default) for rule in rules if _style_limits(rule).get(key) is not None]
    return min(vals) if vals else default


def _max_limit(rules: list[dict[str, Any]], key: str, default: float) -> float:
    vals = [_safe_float(_style_limits(rule).get(key), default) for rule in rules if _style_limits(rule).get(key) is not None]
    return max(vals) if vals else default


def _collect(rules: list[dict[str, Any]], key: str) -> list[str]:
    out: list[str] = []
    for rule in rules:
        value = rule.get(key) or []
        if isinstance(value, str):
            out.append(value)
        else:
            out.extend(str(item) for item in value)
    return _unique(out)


def _unique(values: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _style_reason(result: dict[str, Any]) -> str:
    reasons = result.get("reasons") or []
    if reasons:
        return "; ".join(str(r) for r in reasons[:3])
    return f"{result.get('analysis_style')} analysis with {result.get('trade_style')} execution"


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value) if value is not None else default
    except (TypeError, ValueError):
        return default


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))
