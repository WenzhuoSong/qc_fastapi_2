"""
Evidence bundle builder.

This module gathers existing pipeline outputs into one factual contract for
downstream agents. It does not decide target weights; market_scorecard.py turns
this evidence into action permissions.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from services.knowledge_base import build_knowledge_context
from services.knowledge_resolver import resolve_knowledge
from services.news_evidence import build_news_evidence
from services.etf_decay_diagnostics import empty_etf_decay_diagnostics
from services.liquidity_proxy_diagnostics import empty_liquidity_proxy_diagnostics
from services.execution_gateway import build_execution_gateway
from services.strategy_confidence_calibrator import calibrate_strategy_confidence
from services.strategy_certification import certify_strategies
from services.strategy_diversity import (
    build_strategy_diversity_summary,
    canonical_strategy_family,
    is_strategy_alpha_source,
)
from services.strategy_independence import empty_strategy_independence_summary


DEFAULT_MAX_AGE_SECONDS = 1800


def build_evidence_bundle(
    *,
    brief: dict[str, Any] | None,
    quant_baseline: dict[str, Any] | None,
    playground_bundle: dict[str, Any] | None = None,
    news_evidence: dict[str, Any] | None = None,
    empirical_profiles: dict[str, Any] | None = None,
    strategy_execution_evidence_config: dict[str, Any] | None = None,
    max_age_seconds: int = DEFAULT_MAX_AGE_SECONDS,
) -> dict[str, Any]:
    brief = brief or {}
    quant = quant_baseline or {}
    playground = playground_bundle or None

    market = _build_market_section(brief, quant)
    rotation = brief.get("sector_rotation") or {}
    news = _build_news_section(brief)
    structured_news_evidence = news_evidence or build_news_evidence(brief)
    strategies = _build_strategy_section(playground)
    strategies["strategy_execution_evidence_config"] = strategy_execution_evidence_config or {}
    knowledge = _build_knowledge_section(
        brief=brief,
        market=market,
        strategies=strategies,
        news_evidence=structured_news_evidence,
        empirical_profiles=empirical_profiles or brief.get("empirical_profiles") or {},
    )
    calibration = calibrate_strategy_confidence(
        strategy_confidence=strategies.get("strategy_confidence") or {},
        knowledge_resolution=knowledge.get("resolution") or {},
    )
    strategies = _with_calibrated_strategy_confidence(
        strategies=strategies,
        calibration=calibration,
    )
    strategy_certification = certify_strategies(strategies)
    strategies["strategy_certification"] = strategy_certification
    strategies = _with_strategy_certification(
        strategies=strategies,
        certification=strategy_certification,
    )
    strategies["execution_gateway"] = build_execution_gateway(strategies)
    knowledge["strategy_confidence_calibration"] = {
        "records": calibration.get("records") or [],
        "summary": calibration.get("summary") or {},
    }
    memory = _build_memory_section(brief)
    data_quality = _build_data_quality_section(
        news=news,
        news_evidence=structured_news_evidence,
        strategies=strategies,
        memory=memory,
        brief=brief,
    )

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "max_age_seconds": int(max_age_seconds),
        "source_timestamps": _build_source_timestamps(brief, playground),
        "market": market,
        "rotation": rotation,
        "news": news,
        "news_evidence": structured_news_evidence,
        "strategies": strategies,
        "knowledge": knowledge,
        "memory": memory,
        "data_quality": data_quality,
    }


def _build_market_section(brief: dict[str, Any], quant: dict[str, Any]) -> dict[str, Any]:
    regime_result = quant.get("regime_result") or {}
    signals = regime_result.get("signals") or {}
    key_facts = brief.get("key_facts") or {}
    portfolio = brief.get("portfolio") or {}

    return {
        "regime": regime_result.get("regime") or "unknown",
        "regime_subtype": signals.get("regime_subtype"),
        "regime_confidence": regime_result.get("confidence") or "low",
        "regime_bond_adjusted": bool(signals.get("regime_bond_adjusted")),
        "regime_reasoning": regime_result.get("reasoning"),
        "spy_mom_20d": _first_number(signals, "spy_mom_20d"),
        "spy_mom_60d": _first_number(signals, "spy_mom_60d", fallback=key_facts.get("spy_mom_60d")),
        "spy_mom_252d": _first_number(signals, "spy_mom_252d"),
        "spy_rsi": _first_number(signals, "spy_rsi"),
        "spy_atr_pct": _first_number(signals, "spy_atr_pct"),
        "vix": _first_number(signals, "vix", fallback=portfolio.get("vix")),
        "drawdown_pct": _first_number(
            signals,
            "drawdown",
            fallback=key_facts.get("drawdown_pct", portfolio.get("current_drawdown_pct")),
        ),
        "breadth_pct": _to_float(key_facts.get("breadth_pct")),
        "avg_atr_pct": _to_float(key_facts.get("avg_atr_pct")),
        "risk_on_score": _to_float(key_facts.get("risk_on_score")),
        "top5_momentum": key_facts.get("top5_momentum") or [],
        "bottom5_momentum": key_facts.get("bottom5_momentum") or [],
        "n_etfs": key_facts.get("n_etfs"),
    }


def _build_news_section(brief: dict[str, Any]) -> dict[str, Any]:
    context = brief.get("news_context") or {}
    warnings: list[str] = []
    if context.get("_stale_warning"):
        warnings.append(str(context["_stale_warning"]))
    warnings.extend(str(item) for item in context.get("data_gaps") or [])

    data_quality = "fresh"
    if context.get("_fallback") or context.get("data_gaps"):
        data_quality = "limited"
    if context.get("_stale_warning"):
        data_quality = "stale"
    if not context and not brief.get("macro_news_section"):
        data_quality = "missing"
        warnings.append("No structured news context or macro news section available")

    return {
        "macro_signals": context.get("macro_signals") or [],
        "ticker_signals": context.get("ticker_signals") or {},
        "calendar_events": brief.get("calendar_section") or "",
        "macro_news_section_present": bool(brief.get("macro_news_section")),
        "per_ticker_news_count": sum(len(v) for v in (brief.get("per_ticker_news") or {}).values()),
        "hard_risk_tickers": sorted((brief.get("hard_risks_map") or {}).keys()),
        "data_quality": data_quality,
        "warnings": _unique(warnings),
    }


def _build_strategy_section(playground: dict[str, Any] | None) -> dict[str, Any]:
    if not playground:
        return {
            "playground_available": False,
            "snapshot_count": 0,
            "forward_return_samples": 0,
            "execution_intel": {
                "qc_snapshot_count": 0,
                "forward_return_samples": 0,
                "status": "insufficient_data",
                "reason": "No recent Playground result available",
            },
            "consensus_top5": [],
            "strategy_results": [],
            "strategy_diversity": build_strategy_diversity_summary([]),
            "strategy_independence": empty_strategy_independence_summary("no_recent_playground_result"),
            "etf_decay_diagnostics": empty_etf_decay_diagnostics("no_recent_playground_result"),
            "liquidity_proxy_diagnostics": empty_liquidity_proxy_diagnostics("no_recent_playground_result"),
            "evidence_vote_summary": {},
            "evidence_cap_diagnostics": {},
            "conviction_profile_summary": {
                "contract_version": "conviction_profile_availability_v1",
                "total_profiles": 0,
                "matched_profile_count": 0,
                "latest_as_of_date": None,
                "statuses": {},
                "source_buckets": {},
                "statistical_statuses": {},
            },
            "turnover_warnings": [],
            "data_quality": "missing",
            "evidence_summary": {
                "historical_evidence": "missing",
                "live_fit": "insufficient",
                "execution_intel_status": "insufficient_data",
                "execution_permission": "blocked",
                "summary_reasons": ["No recent Playground result available"],
            },
            "warnings": [
                "No recent Playground result available; strategy comparison cannot influence allocation"
            ],
        }

    replay_metrics = playground.get("replay_metrics") or {}
    historical_metrics = playground.get("historical_replay_metrics") or {}
    walk_forward_validation = playground.get("walk_forward_validation") or {}
    forward_samples = _max_forward_samples(replay_metrics)
    historical_samples = _max_forward_samples(historical_metrics)
    strategy_results = _strategy_results(playground)
    strategy_diversity = build_strategy_diversity_summary(strategy_results)
    strategy_independence = playground.get("strategy_independence") or empty_strategy_independence_summary(
        "strategy_independence_missing_from_playground"
    )
    etf_decay_diagnostics = playground.get("etf_decay_diagnostics") or empty_etf_decay_diagnostics(
        "etf_decay_diagnostics_missing_from_playground"
    )
    liquidity_proxy_diagnostics = playground.get("liquidity_proxy_diagnostics") or empty_liquidity_proxy_diagnostics(
        "liquidity_proxy_diagnostics_missing_from_playground"
    )
    max_turnover = max(
        [_to_float(item.get("turnover"), 0.0) for item in strategy_results] or [0.0]
    )
    warnings = list(playground.get("data_gaps") or [])
    turnover_warnings: list[str] = []
    if max_turnover > 0.50:
        turnover_warnings.append(f"Strategy turnover {max_turnover:.1%} may erode returns")

    snapshot_count = int(_to_float(playground.get("snapshot_count"), 0))
    data_quality = "fresh"
    if historical_samples < 30:
        data_quality = "limited"
    if historical_samples >= 30:
        data_quality = "historical_supported"
    if not strategy_results:
        data_quality = "missing"
        warnings.append("Playground has no strategy results")

    return {
        "playground_available": True,
        "generated_at": playground.get("generated_at"),
        "regime_label": playground.get("regime_label"),
        "regime_confidence": playground.get("regime_confidence"),
        "snapshot_count": snapshot_count,
        "forward_return_samples": forward_samples,
        "execution_intel": _execution_intel_section(playground, snapshot_count, forward_samples),
        "historical_snapshot_count": int(_to_float(playground.get("historical_snapshot_count"), 0)),
        "historical_forward_return_samples": historical_samples,
        "consensus_top5": _top_weights(playground.get("consensus_weights") or {}),
        "consensus_weights": playground.get("consensus_weights") or {},
        "strategy_confidence": playground.get("strategy_confidence") or {},
        "walk_forward_validation": walk_forward_validation,
        "strategy_use_summary": _strategy_use_summary(playground.get("strategy_confidence") or {}),
        "evidence_summary": playground.get("evidence_summary") or {},
        "strategy_results": strategy_results,
        "strategy_diversity": strategy_diversity,
        "strategy_independence": strategy_independence,
        "etf_decay_diagnostics": etf_decay_diagnostics,
        "liquidity_proxy_diagnostics": liquidity_proxy_diagnostics,
        "evidence_vote_summary": playground.get("evidence_vote_summary") or {},
        "evidence_cap_diagnostics": playground.get("evidence_cap_diagnostics") or {},
        "conviction_profile_summary": playground.get("conviction_profile_summary") or {},
        "turnover_warnings": turnover_warnings,
        "data_quality": data_quality,
        "warnings": _unique([str(item) for item in warnings] + turnover_warnings),
    }


def _strategy_results(playground: dict[str, Any]) -> list[dict[str, Any]]:
    replay_metrics = playground.get("replay_metrics") or {}
    historical_metrics = playground.get("historical_replay_metrics") or {}
    confidence = playground.get("strategy_confidence") or {}
    walk_forward_items = (playground.get("walk_forward_validation") or {}).get("items") or {}
    out: list[dict[str, Any]] = []
    for item in playground.get("strategies") or []:
        if not isinstance(item, dict):
            continue
        name = item.get("strategy_name")
        metrics = replay_metrics.get(name) or {}
        hist_metrics = historical_metrics.get(name) or {}
        confidence_row = confidence.get(name) or {}
        walk_forward_row = walk_forward_items.get(name) or {}
        risk_profile = item.get("risk_profile") or {}
        strategy_card = item.get("strategy_card") if isinstance(item.get("strategy_card"), dict) else {}
        raw_family = (
            strategy_card.get("family")
            or item.get("family")
            or item.get("strategy_family")
        )
        canonical_family = (
            strategy_card.get("canonical_family")
            or item.get("canonical_family")
            or canonical_strategy_family(raw_family)
        )
        alpha_source = is_strategy_alpha_source(
            name,
            canonical_family,
            item.get("alpha_source", strategy_card.get("alpha_source")),
        )
        data_ready = bool(item.get("data_ready"))
        turnover_status = str(risk_profile.get("turnover_status") or item.get("turnover_status") or "")
        turnover = None
        if data_ready and turnover_status != "not_scored":
            turnover = _to_float(
                risk_profile.get("turnover"),
                _to_float(item.get("expected_turnover_pct"), _to_float(metrics.get("avg_turnover"))),
            )
        out.append(
            {
                "strategy_name": name,
                "strategy_card": strategy_card,
                "raw_family": raw_family or "unknown",
                "canonical_family": canonical_family,
                "alpha_source": bool(alpha_source),
                "data_ready": data_ready,
                "can_influence_allocation": bool(
                    (item.get("feature_contract") or {}).get("can_influence_allocation", item.get("data_ready"))
                ),
                "regime_fit": item.get("regime_fit"),
                "turnover": turnover,
                "turnover_status": turnover_status or None,
                "diagnostic_turnover": _to_float(risk_profile.get("diagnostic_turnover"), None),
                "fallback_cash_turnover": _to_float(risk_profile.get("fallback_cash_turnover"), None),
                "estimated_cost_pct": _to_float(item.get("estimated_cost_pct")),
                "selected_tickers": item.get("selected_tickers") or [],
                "evidence_contract_version": item.get("evidence_contract_version"),
                "evidence_cards": item.get("evidence_cards") or [],
                "evidence_summary": item.get("evidence_summary") or {},
                "metric_reliability": metrics.get("metric_reliability") or {},
                "n_forward_return_samples": metrics.get("n_forward_return_samples"),
                "historical_metric_reliability": hist_metrics.get("metric_reliability") or {},
                "historical_forward_return_samples": hist_metrics.get("n_forward_return_samples"),
                "historical_sharpe": hist_metrics.get("sharpe"),
                "historical_hit_rate": hist_metrics.get("hit_rate"),
                "walk_forward_level": walk_forward_row.get("level"),
                "walk_forward_valid_folds": walk_forward_row.get("valid_fold_count"),
                "walk_forward_pass_rate": walk_forward_row.get("pass_rate"),
                "walk_forward_stability_score": walk_forward_row.get("stability_score"),
                "confidence_score": confidence_row.get("confidence_score"),
                "suggested_use": confidence_row.get("suggested_use"),
                "reason_codes": confidence_row.get("reason_codes") or [],
            }
        )
    return out


def _execution_intel_section(
    playground: dict[str, Any],
    snapshot_count: int,
    forward_samples: int,
) -> dict[str, Any]:
    summary = playground.get("evidence_summary") or {}
    prebuilt = summary.get("execution_intel") or playground.get("execution_intel") or {}
    if isinstance(prebuilt, dict) and prebuilt:
        status = str(prebuilt.get("status") or summary.get("execution_intel_status") or "live_available")
        return {
            **prebuilt,
            "qc_snapshot_count": int(_to_float(prebuilt.get("qc_snapshot_count"), snapshot_count) or 0),
            "forward_return_samples": int(_to_float(prebuilt.get("forward_return_samples"), forward_samples) or 0),
            "status": status,
        }
    status = str(summary.get("execution_intel_status") or "")
    if not status:
        status = "live_available" if snapshot_count > 0 else "insufficient_data"
    return {
        "qc_snapshot_count": snapshot_count,
        "forward_return_samples": forward_samples,
        "status": status,
        "reason": None if status == "live_available" else "QC live data is not sufficient for execution monitoring",
    }


def _strategy_use_summary(confidence: dict[str, Any]) -> dict[str, Any]:
    summary = {
        "primary": [],
        "advisory": [],
        "watch_only": [],
        "ignore": [],
    }
    for name, row in confidence.items():
        if not isinstance(row, dict):
            continue
        use = str(row.get("suggested_use") or "watch_only")
        if use not in summary:
            use = "watch_only"
        summary[use].append({
            "strategy_name": name,
            "confidence_score": row.get("confidence_score"),
            "reason_codes": row.get("reason_codes") or [],
        })
    for use in summary:
        summary[use].sort(
            key=lambda item: float(item.get("confidence_score") or 0.0),
            reverse=True,
        )
    return {
        **summary,
        "actionable_count": len(summary["primary"]) + len(summary["advisory"]),
        "best_actionable": (summary["primary"] or summary["advisory"] or [None])[0],
    }


def _build_memory_section(brief: dict[str, Any]) -> dict[str, Any]:
    memory = brief.get("memory_context") or {}
    warnings = list(memory.get("data_gaps") or [])
    return {
        "has_memory": bool(memory.get("has_memory")),
        "recent_regime_trend": memory.get("regime_trend"),
        "recent_days": memory.get("recent_days") or [],
        "recent_weeks": memory.get("recent_weeks") or [],
        "warnings": _unique(str(item) for item in warnings),
    }


def _build_knowledge_section(
    *,
    brief: dict[str, Any],
    market: dict[str, Any],
    strategies: dict[str, Any],
    news_evidence: dict[str, Any],
    empirical_profiles: dict[str, Any],
) -> dict[str, Any]:
    try:
        tickers = _knowledge_tickers(brief=brief, strategies=strategies)
        strategy_names = [
            str(item.get("strategy_name"))
            for item in strategies.get("strategy_results") or []
            if item.get("strategy_name")
        ]
        reason_codes: list[str] = []
        for item in strategies.get("strategy_results") or []:
            reason_codes.extend(str(code) for code in item.get("reason_codes") or [])
        evidence_summary = strategies.get("evidence_summary") or {}
        permission = evidence_summary.get("execution_permission")
        if permission:
            reason_codes.append(str(permission))
        context = build_knowledge_context(
            tickers=tickers,
            strategy_names=strategy_names,
            regime=market.get("regime") or strategies.get("regime_label"),
            reason_codes=reason_codes,
        )
        computed_facts_available = {
            "news_evidence": bool(news_evidence),
            "scorecard": False,
            "position_governance": False,
            "empirical_profiles": bool(empirical_profiles),
        }
        resolution = resolve_knowledge(
            knowledge_context=context,
            computed_facts={
                "market": market,
                "strategies": strategies,
                "positions": {
                    "holdings": brief.get("holdings") or [],
                    "current_weights": brief.get("current_weights") or {},
                    "target_weights": brief.get("target_weights") or {},
                },
                "news_evidence": news_evidence,
                "empirical_profiles": empirical_profiles,
                "computed_facts_available": computed_facts_available,
            },
        )
        return {
            **context,
            "computed_facts_available": computed_facts_available,
            "resolution": resolution,
        }
    except Exception as exc:  # pragma: no cover - defensive: keep pipeline alive.
        return {
            "available": False,
            "warnings": [f"knowledge_base_unavailable: {exc}"],
        }


def _knowledge_tickers(*, brief: dict[str, Any], strategies: dict[str, Any]) -> list[str]:
    tickers: list[str] = []
    for row in brief.get("holdings") or []:
        if isinstance(row, dict):
            tickers.append(str(row.get("ticker") or row.get("symbol") or ""))
    for key in ("current_weights", "target_weights", "weights"):
        weights = brief.get(key) or {}
        if isinstance(weights, dict):
            tickers.extend(str(ticker) for ticker in weights.keys())
    for row in strategies.get("consensus_top5") or []:
        if isinstance(row, dict):
            tickers.append(str(row.get("ticker") or ""))
    consensus_weights = strategies.get("consensus_weights") or {}
    if isinstance(consensus_weights, dict):
        tickers.extend(str(ticker) for ticker in consensus_weights.keys())
    for row in strategies.get("strategy_results") or []:
        tickers.extend(str(ticker) for ticker in row.get("selected_tickers") or [])
    return _unique(ticker.upper() for ticker in tickers if ticker and ticker != "CASH")


def _with_calibrated_strategy_confidence(
    *,
    strategies: dict[str, Any],
    calibration: dict[str, Any],
) -> dict[str, Any]:
    calibrated_confidence = calibration.get("strategy_confidence") or {}
    if not calibrated_confidence:
        return strategies
    out = dict(strategies)
    original_confidence = strategies.get("strategy_confidence") or {}
    out["strategy_confidence_pre_calibration"] = original_confidence
    out["strategy_confidence"] = calibrated_confidence
    out["strategy_confidence_calibration"] = {
        "records": calibration.get("records") or [],
        "summary": calibration.get("summary") or {},
    }
    out["strategy_use_summary"] = _strategy_use_summary(calibrated_confidence)

    calibrated_results: list[dict[str, Any]] = []
    for item in strategies.get("strategy_results") or []:
        row = dict(item)
        name = row.get("strategy_name")
        confidence_row = calibrated_confidence.get(name) or {}
        if confidence_row:
            row["confidence_score_pre_calibration"] = item.get("confidence_score")
            row["confidence_score"] = confidence_row.get("confidence_score")
            row["calibration_reason_codes"] = confidence_row.get("calibration_reason_codes") or []
        calibrated_results.append(row)
    out["strategy_results"] = calibrated_results
    out["strategy_diversity"] = build_strategy_diversity_summary(calibrated_results)
    return out


def _with_strategy_certification(
    *,
    strategies: dict[str, Any],
    certification: dict[str, Any],
) -> dict[str, Any]:
    items = certification.get("items") if isinstance(certification.get("items"), dict) else {}
    if not items:
        return strategies
    out = dict(strategies)

    certified_results: list[dict[str, Any]] = []
    for item in strategies.get("strategy_results") or []:
        row = dict(item)
        name = str(row.get("strategy_name") or "")
        cert = items.get(name) if isinstance(items.get(name), dict) else {}
        if cert:
            row["certification_status"] = cert.get("status")
            row["approved_use"] = cert.get("approved_use")
            row["execution_evidence_status"] = cert.get("execution_evidence_status")
            row["promotion_blockers"] = cert.get("promotion_blockers") or []
            row["demotion_reasons"] = cert.get("demotion_reasons") or []
            row["evidence_checks"] = cert.get("evidence_checks") or {}
        certified_results.append(row)
    out["strategy_results"] = certified_results

    certified_confidence: dict[str, Any] = {}
    for name, value in (strategies.get("strategy_confidence") or {}).items():
        row = dict(value) if isinstance(value, dict) else {}
        cert = items.get(name) if isinstance(items.get(name), dict) else {}
        if cert:
            row["certification_status"] = cert.get("status")
            row["approved_use"] = cert.get("approved_use")
            row["execution_evidence_status"] = cert.get("execution_evidence_status")
            row["promotion_blockers"] = cert.get("promotion_blockers") or []
            row["demotion_reasons"] = cert.get("demotion_reasons") or []
            row["evidence_checks"] = cert.get("evidence_checks") or {}
        certified_confidence[name] = row
    out["strategy_confidence"] = certified_confidence
    out["strategy_diversity"] = build_strategy_diversity_summary(certified_results)
    return out


def _build_data_quality_section(
    *,
    news: dict[str, Any],
    news_evidence: dict[str, Any],
    strategies: dict[str, Any],
    memory: dict[str, Any],
    brief: dict[str, Any],
) -> dict[str, Any]:
    warnings: list[str] = []
    warnings.extend(news.get("warnings") or [])
    warnings.extend(str(item) for item in news_evidence.get("data_gaps") or [])
    warnings.extend(str(item) for item in strategies.get("warnings") or [])
    warnings.extend(str(item) for item in memory.get("warnings") or [])
    if not brief.get("holdings"):
        warnings.append("No holdings available in market brief")

    levels = {
        str(news.get("data_quality") or "unknown"),
        str((news_evidence.get("macro_news_score") or {}).get("data_quality") or "unknown"),
        str(strategies.get("data_quality") or "unknown"),
    }
    if "missing" in levels:
        overall = "missing"
    elif "stale" in levels:
        overall = "stale"
    elif "limited" in levels:
        overall = "limited"
    else:
        overall = "fresh"

    return {
        "overall": overall,
        "warnings": _unique(warnings),
    }


def _build_source_timestamps(brief: dict[str, Any], playground: dict[str, Any] | None) -> dict[str, Any]:
    macro_context = brief.get("news_context") or {}
    return {
        "macro_news_cache": macro_context.get("processed_at"),
        "playground": (playground or {}).get("generated_at"),
    }


def _top_weights(weights: dict[str, Any], n: int = 5) -> list[dict[str, Any]]:
    rows = [
        {"ticker": str(ticker), "weight": round(_to_float(weight), 4)}
        for ticker, weight in (weights or {}).items()
        if ticker != "CASH" and _to_float(weight) > 0
    ]
    rows.sort(key=lambda item: item["weight"], reverse=True)
    return rows[:n]


def _max_forward_samples(replay_metrics: dict[str, Any]) -> int:
    samples = []
    for item in replay_metrics.values():
        if isinstance(item, dict):
            samples.append(int(_to_float(item.get("n_forward_return_samples"), 0)))
    return max(samples) if samples else 0


def _first_number(mapping: dict[str, Any], key: str, fallback: Any = None) -> float | None:
    value = mapping.get(key)
    if value is None:
        value = fallback
    return _to_float(value, None)


def _to_float(value: Any, default: float | None = 0.0) -> float | None:
    try:
        return float(value) if value is not None else default
    except (TypeError, ValueError):
        return default


def _unique(values) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        text = str(value)
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out
