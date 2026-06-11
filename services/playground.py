"""
Strategy Playground sandbox.

Research-only comparison layer: runs multiple deterministic strategies against
recent QC snapshots, computes divergence/consensus diagnostics, and optionally
asks an LLM to summarize the comparison. It never writes target weights and has
no execution authority.
"""
from __future__ import annotations

import json
import logging
import math
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta
from html import escape
from typing import Any

from openai import AsyncOpenAI
from sqlalchemy import desc, select

from config import get_settings
from db.models import MarketDailyFeature, QCSnapshot
from db.session import AsyncSessionLocal
from services.feature_authority import authority_for_field, canonical_field_name
from services.feature_provenance import summarize_feature_provenance
from services.knowledge_base import build_knowledge_context
from services.etf_decay_diagnostics import (
    empty_etf_decay_diagnostics,
    evaluate_etf_decay_diagnostics_from_snapshots,
)
from services.liquidity_proxy_diagnostics import (
    empty_liquidity_proxy_diagnostics,
    evaluate_liquidity_proxy_diagnostics_from_snapshots,
)
from services.macro_regime_builder import build_deterministic_macro_regime
from services.quant_baseline import classify_market_regime
from services.sector_rotation import detect_sector_rotation
from services.strategy_evidence import (
    EVIDENCE_CONTRACT_VERSION,
    build_evidence_cards,
    summarize_evidence_cards,
)
from services.evidence_vote_aggregation import (
    aggregate_etf_evidence,
    evidence_cards_from_strategy_results,
    input_builder_exclusions_from_strategy_results,
)
from services.evidence_quality_cap import evaluate_evidence_quality_caps
from services.strategy_feature_contract import build_strategy_feature_contract
from services.strategy_input_builder import build_strategy_input
from services.strategy_independence import (
    build_strategy_independence_diagnostics_from_snapshots,
    empty_strategy_independence_summary,
)
from services.strategy_breadth_calibration import build_strategy_breadth_calibration_report
from services.strategy_validation_dashboard import load_validation_dashboard_summary
from services.universe_policy import filter_tradable_research_rows
from services.walk_forward_validation import validate_walk_forward
from strategies import ScoredTicker, compute_rebalance_actions, estimate_cost_pct, get_strategy

logger = logging.getLogger("qc_fastapi_2.playground")
settings = get_settings()

DEFAULT_PLAYGROUND_STRATEGIES = [
    "momentum_lite_v1",
    "absolute_trend_following_lite",
    "seasonality_month_end_lite",
    "sector_theme_relative_strength_lite",
    "leveraged_long_amplifier_lite",
    "dual_momentum_rotation",
    "mean_reversion_lite",
    "relative_value_reversion_lite",
    "sector_theme_relative_value_reversion_lite",
    "low_vol_factor",
    "defensive_quality_rotation_lite",
    "macro_rate_duration_lite",
    "macro_cyclical_inflation_rotation_lite",
    "carry_cash_proxy_lite",
    "volatility_hedge_lite",
    "inverse_equity_hedge_lite",
    "risk_parity_lite",
    "equal_weight_benchmark",
]

MIN_REPLAY_SAMPLES_FOR_PERFORMANCE = 10
MIN_REPLAY_SAMPLES_FOR_STRONG_EVIDENCE = 30


@dataclass
class StrategyResult:
    strategy_name: str
    strategy_version: str
    description: str
    strategy_card: dict[str, Any]
    weights: dict[str, float] | None
    score_breakdown: list[dict[str, Any]]
    selected_tickers: list[str]
    expected_turnover_pct: float | None
    estimated_cost_pct: float | None
    diagnostic_turnover_pct: float | None
    turnover_status: str
    score_status: str
    regime_fit: str
    data_ready: bool
    data_readiness: dict[str, Any]
    feature_contract: dict[str, Any]
    data_quality: dict[str, Any]
    risk_profile: dict[str, Any]
    memory_feedback: dict[str, Any]
    agent_interpretation: dict[str, Any]
    scored_tickers: list[dict[str, Any]] = field(default_factory=list)
    evidence_contract_version: str = EVIDENCE_CONTRACT_VERSION
    evidence_cards: list[dict[str, Any]] = field(default_factory=list)
    evidence_summary: dict[str, Any] = field(default_factory=dict)
    scorable_ticker_count: int = 0
    excluded_tickers: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    not_scored_reason: str | None = None


@dataclass
class PlaygroundBundle:
    generated_at: str
    regime_label: str
    regime_confidence: str
    snapshot_count: int
    strategies: list[StrategyResult]
    divergence_map: list[dict[str, Any]]
    consensus_weights: dict[str, float]
    replay_metrics: dict[str, dict[str, Any]]
    historical_replay_metrics: dict[str, dict[str, Any]]
    historical_snapshot_count: int
    strategy_confidence: dict[str, dict[str, Any]]
    evidence_summary: dict[str, Any]
    data_gaps: list[str]
    walk_forward_validation: dict[str, Any] | None = None
    validation_summary: dict[str, Any] = field(default_factory=dict)
    macro_regime_context: dict[str, Any] = field(default_factory=dict)
    strategy_independence: dict[str, Any] = field(default_factory=dict)
    etf_decay_diagnostics: dict[str, Any] = field(default_factory=dict)
    liquidity_proxy_diagnostics: dict[str, Any] = field(default_factory=dict)
    evidence_vote_summary: dict[str, Any] = field(default_factory=dict)
    evidence_cap_diagnostics: dict[str, Any] = field(default_factory=dict)
    conviction_profile_summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["strategies"] = [asdict(item) for item in self.strategies]
        return data


async def run_playground(
    brief: dict[str, Any],
    strategy_names: list[str] | None = None,
    include_historical: bool = True,
    evidence_cap_config: dict[str, Any] | None = None,
) -> PlaygroundBundle:
    holdings = brief.get("holdings") or []
    holdings, enrichment = await _ensure_playground_features(holdings, strategy_names)
    feature_matrix = enrichment.get("feature_matrix") or {}
    portfolio = brief.get("portfolio") or {}
    current_weights = brief.get("current_weights") or _extract_current_weights(holdings)
    sector_rotation = brief.get("sector_rotation") or detect_sector_rotation(holdings)
    spy_holding = next((h for h in holdings if (h.get("ticker") or "").upper() == "SPY"), {})
    regime = classify_market_regime(portfolio, spy_holding, holdings=holdings)
    macro_regime_context = brief.get("macro_regime_context") or build_deterministic_macro_regime(
        holdings,
        news_context=brief.get("news_context") or {},
    )
    context = {
        "regime": regime.regime.value,
        "confidence": _confidence_to_float(regime.confidence),
        "uncertainty_flag": regime.confidence == "low",
        "stance": _stance_for_regime(regime.regime.value),
        "direction_bias": _direction_bias_for_regime(regime.regime.value),
        "risk_params": brief.get("risk_params") or {},
        "current_weights": current_weights,
        "sector_rotation": sector_rotation,
        "macro_context": macro_regime_context,
        "rate_regime_label": macro_regime_context.get("rate_regime_label"),
        "inflation_regime_label": macro_regime_context.get("inflation_regime_label"),
        "growth_regime_label": macro_regime_context.get("growth_regime_label"),
    }

    names = strategy_names or DEFAULT_PLAYGROUND_STRATEGIES
    memory_feedback = await _load_strategy_memory_feedback(regime.regime.value, names)
    conviction_profiles = await _load_latest_conviction_profiles_for_evidence()
    conviction_profile_summary = _summarize_conviction_profiles(conviction_profiles)
    results = [
        _run_one_strategy(
            name,
            holdings,
            context,
            current_weights,
            memory_feedback=memory_feedback.get(name),
            conviction_profiles=conviction_profiles,
            feature_matrix=feature_matrix,
        )
        for name in names
    ]

    historical_snapshots: list[dict[str, Any]] = []
    historical_metrics: dict[str, dict[str, Any]] = {}
    walk_forward_validation: dict[str, Any] = {}
    strategy_independence: dict[str, Any] = empty_strategy_independence_summary("historical_replay_not_loaded")
    etf_decay_diagnostics: dict[str, Any] = empty_etf_decay_diagnostics("historical_replay_not_loaded")
    liquidity_proxy_diagnostics: dict[str, Any] = empty_liquidity_proxy_diagnostics("historical_replay_not_loaded")
    strategy_confidence: dict[str, dict[str, Any]] = {}
    data_gaps = list(enrichment.get("data_gaps", []))
    if include_historical:
        try:
            historical_snapshots = await _read_yfinance_feature_snapshots(days=420)
            historical_metrics = _compute_replay_metrics(historical_snapshots, names) if historical_snapshots else {}
            walk_forward_validation = _compute_walk_forward_validation(historical_snapshots, names) if historical_snapshots else {}
            strategy_independence = (
                build_strategy_independence_diagnostics_from_snapshots(
                    snapshots=historical_snapshots,
                    strategy_names=names,
                )
                if historical_snapshots
                else empty_strategy_independence_summary("no_yfinance_historical_replay_rows")
            )
            etf_decay_diagnostics = (
                evaluate_etf_decay_diagnostics_from_snapshots(historical_snapshots)
                if historical_snapshots
                else empty_etf_decay_diagnostics("no_yfinance_historical_replay_rows")
            )
            liquidity_proxy_diagnostics = (
                evaluate_liquidity_proxy_diagnostics_from_snapshots(historical_snapshots)
                if historical_snapshots
                else empty_liquidity_proxy_diagnostics("no_yfinance_historical_replay_rows")
            )
            data_gaps.extend(_detect_historical_data_gaps(historical_snapshots))
        except Exception as exc:
            logger.warning("[playground] historical replay for live bundle failed: %s", exc)
            data_gaps.append(f"yfinance historical replay unavailable: {type(exc).__name__}")

    consensus_weights = compute_consensus_weights(results)
    evidence_vote_summary = _build_evidence_vote_summary(results)
    evidence_cap_diagnostics = _build_evidence_cap_diagnostics(
        results,
        evidence_vote_summary=evidence_vote_summary,
        reference_weights=consensus_weights,
        config=evidence_cap_config,
    )
    strategy_confidence = _compute_strategy_confidence(
        results,
        {},
        historical_metrics,
        regime.regime.value,
        consensus_weights,
        walk_forward_validation=walk_forward_validation,
    ) if include_historical else {}

    return PlaygroundBundle(
        generated_at=datetime.utcnow().isoformat(),
        regime_label=regime.regime.value,
        regime_confidence=regime.confidence,
        snapshot_count=1,
        strategies=results,
        divergence_map=compute_weight_divergence(results),
        consensus_weights=consensus_weights,
        replay_metrics={},
        historical_replay_metrics=historical_metrics,
        historical_snapshot_count=len(historical_snapshots),
        strategy_confidence=strategy_confidence,
        evidence_summary=_build_playground_evidence_summary(
            snapshot_count=1,
            historical_snapshot_count=len(historical_snapshots),
            replay_metrics={},
            historical_replay_metrics=historical_metrics,
            strategy_confidence=strategy_confidence,
            walk_forward_validation=walk_forward_validation,
            data_gaps=data_gaps,
        ),
        data_gaps=list(dict.fromkeys(data_gaps)),
        walk_forward_validation=walk_forward_validation,
        macro_regime_context=macro_regime_context,
        strategy_independence=strategy_independence,
        etf_decay_diagnostics=etf_decay_diagnostics,
        liquidity_proxy_diagnostics=liquidity_proxy_diagnostics,
        evidence_vote_summary=evidence_vote_summary,
        evidence_cap_diagnostics=evidence_cap_diagnostics,
        conviction_profile_summary=conviction_profile_summary,
    )


async def run_playground_analysis(
    days: int = 30,
    strategy_names: list[str] | None = None,
) -> PlaygroundBundle:
    names = strategy_names or DEFAULT_PLAYGROUND_STRATEGIES
    historical_snapshots = await _read_yfinance_feature_snapshots(days=max(days, 420))
    snapshots = await _read_recent_snapshots(days=days)
    if not snapshots:
        if historical_snapshots:
            latest_brief = _brief_from_snapshot(historical_snapshots[-1])
            bundle = await run_playground(latest_brief, strategy_names=strategy_names, include_historical=False)
            bundle.snapshot_count = 0
            bundle.historical_snapshot_count = len(historical_snapshots)
            bundle.historical_replay_metrics = _compute_replay_metrics(historical_snapshots, names)
            bundle.walk_forward_validation = _compute_walk_forward_validation(historical_snapshots, names)
            bundle.strategy_independence = build_strategy_independence_diagnostics_from_snapshots(
                snapshots=historical_snapshots,
                strategy_names=names,
            )
            bundle.etf_decay_diagnostics = evaluate_etf_decay_diagnostics_from_snapshots(historical_snapshots)
            bundle.liquidity_proxy_diagnostics = evaluate_liquidity_proxy_diagnostics_from_snapshots(historical_snapshots)
            bundle.strategy_confidence = _compute_strategy_confidence(
                bundle.strategies,
                bundle.replay_metrics,
                bundle.historical_replay_metrics,
                bundle.regime_label,
                bundle.consensus_weights,
                walk_forward_validation=bundle.walk_forward_validation,
            )
            bundle.data_gaps = list(dict.fromkeys(
                (bundle.data_gaps or [])
                + ["no QC snapshots available; live fit uses latest yfinance historical row"]
                + _detect_historical_data_gaps(historical_snapshots)
            ))
            bundle.evidence_summary = _build_playground_evidence_summary(
                snapshot_count=bundle.snapshot_count,
                historical_snapshot_count=bundle.historical_snapshot_count,
                replay_metrics=bundle.replay_metrics,
                historical_replay_metrics=bundle.historical_replay_metrics,
                strategy_confidence=bundle.strategy_confidence,
                walk_forward_validation=bundle.walk_forward_validation,
                data_gaps=bundle.data_gaps,
            )
            bundle.validation_summary = await _load_strategy_validation_summary()
            return bundle
        validation_summary = await _load_strategy_validation_summary()
        return PlaygroundBundle(
            generated_at=datetime.utcnow().isoformat(),
            regime_label="unknown",
            regime_confidence="low",
            snapshot_count=0,
            strategies=[],
            divergence_map=[],
            consensus_weights={"CASH": 1.0},
            replay_metrics={},
            historical_replay_metrics={},
            historical_snapshot_count=0,
            strategy_confidence={},
            evidence_summary={
                "historical_evidence": "missing",
                "walk_forward_validation": "missing",
                "live_fit": "insufficient",
                "execution_permission": "blocked",
                "summary_reasons": ["no QC snapshots available", "no yfinance historical replay rows available"],
            },
            data_gaps=["no QC snapshots available"],
            walk_forward_validation={},
            validation_summary=validation_summary,
            strategy_independence=empty_strategy_independence_summary("no_yfinance_historical_replay_rows"),
            etf_decay_diagnostics=empty_etf_decay_diagnostics("no_yfinance_historical_replay_rows"),
            liquidity_proxy_diagnostics=empty_liquidity_proxy_diagnostics("no_yfinance_historical_replay_rows"),
        )

    latest_brief = _brief_from_snapshot(snapshots[-1])
    bundle = await run_playground(latest_brief, strategy_names=strategy_names, include_historical=False)
    bundle.snapshot_count = len(snapshots)
    bundle.replay_metrics = _compute_replay_metrics(snapshots, names)
    bundle.historical_snapshot_count = len(historical_snapshots)
    bundle.historical_replay_metrics = _compute_replay_metrics(historical_snapshots, names) if historical_snapshots else {}
    bundle.walk_forward_validation = _compute_walk_forward_validation(historical_snapshots, names) if historical_snapshots else {}
    bundle.strategy_independence = (
        build_strategy_independence_diagnostics_from_snapshots(
            snapshots=historical_snapshots,
            strategy_names=names,
        )
        if historical_snapshots
        else empty_strategy_independence_summary("no_yfinance_historical_replay_rows")
    )
    bundle.etf_decay_diagnostics = (
        evaluate_etf_decay_diagnostics_from_snapshots(historical_snapshots)
        if historical_snapshots
        else empty_etf_decay_diagnostics("no_yfinance_historical_replay_rows")
    )
    bundle.liquidity_proxy_diagnostics = (
        evaluate_liquidity_proxy_diagnostics_from_snapshots(historical_snapshots)
        if historical_snapshots
        else empty_liquidity_proxy_diagnostics("no_yfinance_historical_replay_rows")
    )
    bundle.strategy_confidence = _compute_strategy_confidence(
        bundle.strategies,
        bundle.replay_metrics,
        bundle.historical_replay_metrics,
        bundle.regime_label,
        bundle.consensus_weights,
        walk_forward_validation=bundle.walk_forward_validation,
    )
    bundle.evidence_summary = _build_playground_evidence_summary(
        snapshot_count=bundle.snapshot_count,
        historical_snapshot_count=bundle.historical_snapshot_count,
        replay_metrics=bundle.replay_metrics,
        historical_replay_metrics=bundle.historical_replay_metrics,
        strategy_confidence=bundle.strategy_confidence,
        walk_forward_validation=bundle.walk_forward_validation,
        data_gaps=bundle.data_gaps,
    )
    bundle.data_gaps = list(dict.fromkeys(
        (bundle.data_gaps or [])
        + _detect_data_gaps(snapshots)
        + _detect_historical_data_gaps(historical_snapshots)
        + _detect_consensus_regime_conflicts(bundle.regime_label, bundle.consensus_weights)
    ))
    bundle.evidence_summary = _build_playground_evidence_summary(
        snapshot_count=bundle.snapshot_count,
        historical_snapshot_count=bundle.historical_snapshot_count,
        replay_metrics=bundle.replay_metrics,
        historical_replay_metrics=bundle.historical_replay_metrics,
        strategy_confidence=bundle.strategy_confidence,
        walk_forward_validation=bundle.walk_forward_validation,
        data_gaps=bundle.data_gaps,
    )
    bundle.validation_summary = await _load_strategy_validation_summary()
    return bundle


async def _ensure_playground_features(
    holdings: list[dict[str, Any]],
    strategy_names: list[str] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """
    Preload daily research features once. This function is read-only.

    Strategy-specific readiness and ticker exclusions are handled by
    StrategyInputBuilder; playground should not fetch or write yfinance rows.
    """
    clean_holdings = _rows_with_strategy_universe(holdings, strategy_names or DEFAULT_PLAYGROUND_STRATEGIES)
    tickers = sorted({(row.get("ticker") or "").upper().strip() for row in clean_holdings if row.get("ticker")})
    if not tickers:
        return clean_holdings, {
            "data_gaps": ["no tradable research tickers after universe filtering"],
            "feature_matrix": {},
        }

    data_gaps: list[str] = []
    feature_matrix: dict[str, dict[str, Any]] = {}

    try:
        from services.market_feature_store import latest_feature_map
        async with AsyncSessionLocal() as db:
            feature_matrix = await latest_feature_map(db, tickers=tickers, source="yfinance", max_age_days=14)
    except Exception as exc:
        logger.warning("[playground] yfinance feature-store enrichment failed: %s", exc)
        data_gaps.append(f"yfinance feature-store enrichment failed: {type(exc).__name__}")

    if not feature_matrix:
        data_gaps.append("no yfinance daily research features available for playground universe")

    return clean_holdings, {"data_gaps": data_gaps, "feature_matrix": feature_matrix}


async def _load_strategy_validation_summary() -> dict[str, Any]:
    try:
        async with AsyncSessionLocal() as db:
            return await load_validation_dashboard_summary(db)
    except Exception as exc:
        logger.warning("[playground] strategy validation summary unavailable: %s", exc)
        return {
            "contract_version": "strategy_validation_dashboard_v1",
            "status": "unavailable",
            "reason": type(exc).__name__,
            "display_note": "observe_only_no_execution_authority",
        }


async def _load_latest_conviction_profiles_for_evidence(limit: int = 5000) -> list[dict[str, Any]]:
    try:
        from sqlalchemy import func, select

        from db.models import StrategyConvictionProfile

        async with AsyncSessionLocal() as db:
            latest_result = await db.execute(select(func.max(StrategyConvictionProfile.as_of_date)))
            latest_date = latest_result.scalar_one_or_none()
            if latest_date is None:
                return []
            result = await db.execute(
                select(StrategyConvictionProfile)
                .where(StrategyConvictionProfile.as_of_date == latest_date)
                .order_by(
                    StrategyConvictionProfile.source_bucket,
                    StrategyConvictionProfile.strategy_id,
                    StrategyConvictionProfile.ticker,
                )
                .limit(limit)
            )
            rows = result.scalars().all()
        return [
            {
                "strategy_id": row.strategy_id,
                "ticker": row.ticker,
                "branch": row.branch,
                "action": row.action,
                "horizon_days": row.horizon_days,
                "source_bucket": row.source_bucket,
                "conviction": row.conviction,
                "status": row.status,
                "n": row.n,
                "data_lag_filtered": row.data_lag_filtered,
                "requires_live_confirmation": row.requires_live_confirmation,
                "source_counts": row.source_counts or {},
                "hit_rate": row.hit_rate,
                "avg_excess_vs_spy": row.avg_excess_vs_spy,
                "ic": row.ic,
                "as_of_date": row.as_of_date.isoformat() if row.as_of_date else None,
            }
            for row in rows
        ]
    except Exception as exc:
        logger.warning("[playground] conviction profiles unavailable for evidence cards: %s", exc)
        return []


def _summarize_conviction_profiles(rows: list[dict[str, Any]]) -> dict[str, Any]:
    statuses: dict[str, int] = {}
    source_buckets: dict[str, int] = {}
    statistical_statuses: dict[str, int] = {}
    latest_as_of_date: str | None = None
    matched_profile_count = 0
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        status = str(row.get("status") or "unknown")
        bucket = str(row.get("source_bucket") or "unknown")
        stat = str(row.get("statistical_status") or row.get("status") or "unknown")
        statuses[status] = statuses.get(status, 0) + 1
        source_buckets[bucket] = source_buckets.get(bucket, 0) + 1
        statistical_statuses[stat] = statistical_statuses.get(stat, 0) + 1
        if row.get("conviction") is not None:
            matched_profile_count += 1
        as_of = row.get("as_of_date")
        if as_of and (latest_as_of_date is None or str(as_of) > latest_as_of_date):
            latest_as_of_date = str(as_of)
    return {
        "contract_version": "conviction_profile_availability_v1",
        "total_profiles": len(rows or []),
        "matched_profile_count": matched_profile_count,
        "latest_as_of_date": latest_as_of_date,
        "statuses": dict(sorted(statuses.items())),
        "source_buckets": dict(sorted(source_buckets.items())),
        "statistical_statuses": dict(sorted(statistical_statuses.items())),
    }


async def generate_playground_report(bundle: PlaygroundBundle) -> str:
    data = _compact_bundle_for_llm(bundle)
    if not bundle.strategies:
        return "🧪 <b>Playground Sandbox</b>\nNo QC snapshots available, skipped."

    prompt = {
        "task": (
            "Analyze this strategy comparison bundle for a research-only trading sandbox. "
            "No execution is allowed. Use the embedded English strategy_card and "
            "agent_interpretation fields to understand each strategy's meaning, regime fit, "
            "failure modes, and how downstream agents should use it. Distinguish yfinance "
            "historical replay evidence from live QC snapshot evidence."
        ),
        "output_language": "English",
        "required_sections": [
            "best_strategy_or_blend",
            "strategy_confidence_summary",
            "signal_validation_state",
            "historical_replay_evidence",
            "live_qc_fit",
            "key_divergences",
            "turnover_and_cost_risk",
            "data_gaps",
            "next_research_actions",
        ],
        "review_rules": [
            "Do not choose a strategy solely because it has the highest replay Sharpe.",
            "Evaluate bundle.replay_metrics as QC live-snapshot replay only; small sample warnings there do not apply to bundle.historical_replay_metrics.",
            "Evaluate bundle.historical_replay_metrics as yfinance historical replay evidence; if metric_reliability.level is high, do not call it sample-size insufficient.",
            "Treat any metric with metric_reliability.level != high as weak evidence.",
            "If n_forward_return_samples is below the stated minimum for a specific metric block, explicitly name whether that block is QC live replay or yfinance historical replay.",
            "Use bundle.strategy_confidence as the primary summary for confidence_score and suggested_use.",
            "Use bundle.validation_summary only as validation visibility; it has no execution authority.",
            "Distinguish current expected_turnover_pct from historical replay avg_turnover; do not call historical avg_turnover high when it is below 0.20.",
            "When discussing costs, format estimated_cost_pct and avg_turnover as percentages, not raw decimals.",
            "Check regime compatibility, data quality, yfinance-filled fields, turnover, and macro/news consistency.",
            "Discount strategies with weak memory_feedback in the same regime; this is advisory and cannot bypass Risk Manager.",
            "Explicitly mention if a strategy should be discounted due to its failure modes.",
            "If live consensus top weights conflict with the current regime, call out the conflict instead of presenting consensus as an action plan.",
        ],
        "bundle": data,
    }
    try:
        client = AsyncOpenAI(api_key=settings.openai_api_key)
        response = await client.chat.completions.create(
            model=settings.openai_model,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are a quant strategy reviewer. Be concise, skeptical, and practical. "
                        "Do not imply any trade should be executed; this is sandbox research only."
                    ),
                },
                {"role": "user", "content": json.dumps(prompt, ensure_ascii=False)},
            ],
            temperature=0.1,
            max_tokens=900,
        )
        text = response.choices[0].message.content or ""
        return _format_report_for_telegram(text, bundle)
    except Exception as exc:
        logger.warning(f"playground LLM report failed: {exc}")
        return _fallback_report(bundle, error=str(exc))


def _compact_bundle_for_llm(bundle: PlaygroundBundle) -> dict[str, Any]:
    """Return a bounded report payload for the playground LLM reviewer."""
    return {
        "generated_at": bundle.generated_at,
        "regime_label": bundle.regime_label,
        "regime_confidence": bundle.regime_confidence,
        "snapshot_count": bundle.snapshot_count,
        "historical_snapshot_count": bundle.historical_snapshot_count,
        "consensus_top10": _top_weight_rows(bundle.consensus_weights, limit=10),
        "evidence_summary": bundle.evidence_summary,
        "strategy_confidence": bundle.strategy_confidence,
        "strategies": [_compact_strategy_result_for_llm(row) for row in bundle.strategies],
        "replay_metrics": _compact_metrics_map(bundle.replay_metrics),
        "historical_replay_metrics": _compact_metrics_map(bundle.historical_replay_metrics),
        "walk_forward_validation": _compact_walk_forward_for_llm(bundle.walk_forward_validation or {}),
        "validation_summary": _compact_validation_summary_for_llm(bundle.validation_summary),
        "strategy_independence": _compact_strategy_independence_for_llm(bundle.strategy_independence),
        "evidence_vote_summary": _compact_vote_summary_for_llm(bundle.evidence_vote_summary),
        "evidence_cap_diagnostics": _compact_evidence_cap_for_llm(bundle.evidence_cap_diagnostics),
        "largest_divergences": bundle.divergence_map[:10],
        "data_gaps": list(bundle.data_gaps or [])[:12],
    }


def _compact_strategy_result_for_llm(result: StrategyResult) -> dict[str, Any]:
    readiness = result.data_readiness or {}
    risk = result.risk_profile or {}
    memory = result.memory_feedback or {}
    interpretation = result.agent_interpretation or {}
    return {
        "strategy_name": result.strategy_name,
        "strategy_version": result.strategy_version,
        "description": result.description,
        "family": (result.strategy_card or {}).get("family"),
        "alpha_source": (result.strategy_card or {}).get("alpha_source"),
        "regime_fit": result.regime_fit,
        "data_ready": result.data_ready,
        "score_status": result.score_status,
        "not_scored_reason": result.not_scored_reason,
        "selected_tickers": result.selected_tickers,
        "expected_turnover_pct": result.expected_turnover_pct,
        "estimated_cost_pct": result.estimated_cost_pct,
        "diagnostic_turnover_pct": result.diagnostic_turnover_pct,
        "scorable_ticker_count": result.scorable_ticker_count,
        "excluded_ticker_count": len(result.excluded_tickers or {}),
        "readiness": {
            "status": readiness.get("status"),
            "candidate_ticker_count": readiness.get("candidate_ticker_count"),
            "scorable_ticker_count": readiness.get("scorable_ticker_count"),
            "excluded_ticker_count": readiness.get("excluded_ticker_count"),
            "coverage": readiness.get("coverage"),
            "exclusion_counts": readiness.get("exclusion_counts") or {},
        },
        "risk_profile": {
            "turnover": risk.get("turnover"),
            "estimated_cost_pct": risk.get("estimated_cost_pct"),
            "turnover_status": risk.get("turnover_status"),
            "warnings": (risk.get("warnings") or [])[:5],
        },
        "memory_feedback": {
            "discount_multiplier": memory.get("discount_multiplier"),
            "advisory_note": memory.get("advisory_note"),
            "sample_count": memory.get("sample_count"),
        },
        "agent_interpretation": {
            "core_signal": interpretation.get("core_signal"),
            "regime_fit": interpretation.get("regime_fit"),
            "failure_modes": (interpretation.get("failure_modes") or [])[:5],
            "downstream_use": interpretation.get("downstream_use"),
        },
        "top_scores": result.score_breakdown[:8],
        "evidence_summary": result.evidence_summary,
        "evidence_cards": [
            _compact_evidence_card_for_llm(card)
            for card in (result.evidence_cards or [])[:8]
        ],
    }


def _compact_evidence_card_for_llm(card: dict[str, Any]) -> dict[str, Any]:
    return {
        "ticker": card.get("ticker"),
        "role": card.get("role"),
        "action": card.get("action"),
        "vote_status": card.get("vote_status"),
        "abstain_reason": card.get("abstain_reason"),
        "confidence": card.get("confidence"),
        "conviction": card.get("conviction"),
        "conviction_status": card.get("conviction_status"),
        "max_reasonable_weight": card.get("max_reasonable_weight"),
        "risk_budget_cost": card.get("risk_budget_cost"),
        "reason": card.get("reason"),
    }


def _compact_metrics_map(metrics: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    compact: dict[str, dict[str, Any]] = {}
    for name, row in (metrics or {}).items():
        reliability = row.get("metric_reliability") or {}
        compact[name] = {
            "avg_turnover": row.get("avg_turnover"),
            "max_turnover": row.get("max_turnover"),
            "avg_position_count": row.get("avg_position_count"),
            "avg_cash_weight": row.get("avg_cash_weight"),
            "sharpe": row.get("sharpe"),
            "ic": row.get("ic"),
            "hit_rate": row.get("hit_rate"),
            "max_drawdown_pct": row.get("max_drawdown_pct"),
            "n_forward_return_samples": row.get("n_forward_return_samples"),
            "n_ic_samples": row.get("n_ic_samples"),
            "metric_reliability": {
                "level": reliability.get("level"),
                "sample_count": reliability.get("sample_count"),
                "ic_sample_count": reliability.get("ic_sample_count"),
                "strategy_ready_samples": reliability.get("strategy_ready_samples"),
                "reasons": (reliability.get("reasons") or [])[:5],
            },
            "top_signal_leaders": (row.get("top_signal_leaders") or [])[:5],
            "selection_guardrail": row.get("selection_guardrail"),
        }
    return compact


def _compact_walk_forward_for_llm(validation: dict[str, Any]) -> dict[str, Any]:
    items = validation.get("items") or {}
    return {
        "contract_version": validation.get("contract_version"),
        "items": {
            name: {
                "level": row.get("level"),
                "valid_fold_count": row.get("valid_fold_count"),
                "pass_rate": row.get("pass_rate"),
                "stability_score": row.get("stability_score"),
                "reason_codes": row.get("reason_codes") or [],
            }
            for name, row in items.items()
            if isinstance(row, dict)
        },
    }


def _compact_validation_summary_for_llm(summary: dict[str, Any]) -> dict[str, Any]:
    return {
        "signals_today": summary.get("signals_today"),
        "outcomes_today": summary.get("outcomes_today"),
        "pending_mature": summary.get("pending_mature"),
        "profiles": summary.get("profiles") or {},
        "warnings": (summary.get("warnings") or [])[:8],
    }


def _compact_strategy_independence_for_llm(summary: dict[str, Any]) -> dict[str, Any]:
    breadth = build_strategy_breadth_calibration_report(summary)
    return {
        "status": summary.get("status"),
        "strategy_count": summary.get("strategy_count"),
        "alpha_strategy_count": summary.get("alpha_strategy_count"),
        "effective_independent_alpha_count": summary.get("effective_independent_alpha_count"),
        "estimated_independent_clusters": breadth.get("estimated_independent_clusters"),
        "estimated_breadth_is_approximation": breadth.get("estimated_breadth_is_approximation"),
        "duplicate_alpha_pair_count": len(breadth.get("high_correlation_pairs") or []),
        "diversifying_pair_count": len(breadth.get("diversifying_pairs") or []),
        "high_correlation_pair_count": summary.get("high_correlation_pair_count"),
        "low_correlation_pair_count": summary.get("low_correlation_pair_count"),
        "warnings": (summary.get("warnings") or [])[:8],
    }


def _compact_vote_summary_for_llm(summary: dict[str, Any]) -> dict[str, Any]:
    return {
        "ticker_count": summary.get("ticker_count"),
        "voted_count": summary.get("voted_count"),
        "abstain_count": summary.get("abstain_count"),
        "watch_count": summary.get("watch_count"),
        "mapping_error_count": summary.get("mapping_error_count"),
        "warnings": (summary.get("warnings") or [])[:8],
    }


def _compact_evidence_cap_for_llm(summary: dict[str, Any]) -> dict[str, Any]:
    rows = [
        row for row in (summary.get("rows") or [])
        if isinstance(row, dict)
    ]
    rows = sorted(rows, key=lambda row: float(row.get("cap_reduction") or 0.0), reverse=True)
    return {
        "available": summary.get("available"),
        "execution_effect": summary.get("execution_effect"),
        "ticker_count": summary.get("ticker_count"),
        "degraded_ticker_count": summary.get("degraded_ticker_count"),
        "would_clip_count": summary.get("would_clip_count"),
        "mapping_error_count": summary.get("mapping_error_count"),
        "top_degraded_rows": rows[:10],
        "warnings": (summary.get("warnings") or [])[:8],
    }


def _top_weight_rows(weights: dict[str, float], *, limit: int) -> list[dict[str, Any]]:
    return [
        {"ticker": ticker, "weight": round(float(weight or 0.0), 6)}
        for ticker, weight in sorted(
            (weights or {}).items(),
            key=lambda item: float(item[1] or 0.0),
            reverse=True,
        )[:limit]
    ]


def _run_one_strategy(
    name: str,
    holdings: list[dict],
    context: dict[str, Any],
    current_weights: dict[str, float],
    memory_feedback: dict[str, Any] | None = None,
    conviction_profiles: list[dict[str, Any]] | None = None,
    feature_matrix: dict[str, dict[str, Any]] | None = None,
    as_of_date: date | None = None,
) -> StrategyResult:
    strategy = get_strategy(name)
    strategy_input = build_strategy_input(
        strategy=strategy,
        live_rows=holdings,
        feature_matrix=feature_matrix or {},
        as_of=as_of_date or datetime.utcnow().date(),
    )
    scoring_rows = strategy_input.scorable_rows
    readiness = strategy_input.readiness_summary
    feature_contract = build_strategy_feature_contract(
        strategy,
        scoring_rows,
        as_of=as_of_date or datetime.utcnow().date(),
    )
    can_score = bool(strategy_input.can_score and feature_contract.get("can_influence_allocation"))
    if can_score:
        scored = strategy.score(scoring_rows, context)
        weights = strategy.optimize(scored, context) if scored else None
    else:
        scored = []
        weights = None
    if can_score and weights:
        actions = compute_rebalance_actions(weights, current_weights, threshold=1e-9)
        turnover = _turnover(weights, current_weights)
        expected_turnover = round(turnover, 6)
        estimated_cost = estimate_cost_pct(actions)
        turnover_status = "actionable"
        score_status = strategy_input.status
        not_scored_reason = None
    else:
        turnover = None
        expected_turnover = None
        estimated_cost = None
        turnover_status = "not_scored"
        score_status = "not_scored"
        not_scored_reason = strategy_input.not_scored_reason or "strategy_returned_no_scores"
    score_breakdown = [_score_to_dict(item) for item in scored]
    evidence_cards = _build_strategy_evidence_cards(
        strategy=strategy,
        scored=scored,
        context=context,
        conviction_profiles=conviction_profiles or [],
    )
    return StrategyResult(
        strategy_name=name,
        strategy_version=strategy.version,
        description=strategy.description,
        strategy_card=strategy.strategy_card(),
        weights=weights,
        score_breakdown=score_breakdown,
        selected_tickers=[
            ticker for ticker, weight in (weights or {}).items()
            if ticker != "CASH" and weight > 0.01
        ],
        expected_turnover_pct=expected_turnover,
        estimated_cost_pct=estimated_cost,
        diagnostic_turnover_pct=round(turnover, 6) if turnover is not None else None,
        turnover_status=turnover_status,
        score_status=score_status,
        regime_fit=_strategy_regime_fit(name, context.get("regime", "")),
        data_ready=can_score and bool(weights),
        data_readiness={
            **readiness,
            "requirements": strategy.data_requirements(),
        },
        feature_contract={
            **feature_contract,
            "input_status": strategy_input.status,
            "excluded_tickers": strategy_input.excluded_tickers,
            "field_provenance": strategy_input.field_provenance,
        },
        data_quality=_build_data_quality(readiness, scoring_rows, strategy.required_fields),
        risk_profile=_build_risk_profile(
            weights,
            expected_turnover,
            estimated_cost,
            diagnostic_turnover=turnover,
            turnover_status=turnover_status,
        ),
        memory_feedback=memory_feedback or _neutral_memory_feedback(name, context.get("regime", "")),
        agent_interpretation=_build_agent_interpretation(
            strategy, scored, weights, context, readiness, feature_contract, memory_feedback
        ),
        scored_tickers=score_breakdown,
        evidence_contract_version=EVIDENCE_CONTRACT_VERSION,
        evidence_cards=evidence_cards,
        evidence_summary=summarize_evidence_cards(evidence_cards),
        scorable_ticker_count=int(readiness.get("scorable_ticker_count") or 0),
        excluded_tickers=strategy_input.excluded_tickers,
        not_scored_reason=not_scored_reason,
    )


def _build_strategy_evidence_cards(
    *,
    strategy,
    scored: list[ScoredTicker],
    context: dict[str, Any],
    conviction_profiles: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    if not scored:
        return []
    tickers = [item.ticker for item in scored if item.ticker]
    try:
        knowledge_context = build_knowledge_context(
            tickers=tickers,
            strategy_names=[strategy.name],
            regime=context.get("regime"),
            max_assets=max(12, len(tickers)),
        )
        cards = build_evidence_cards(
            strategy=strategy,
            scored=scored,
            knowledge_context=knowledge_context,
            mode="playground",
            conviction_profiles=conviction_profiles or [],
        )
        return [card.to_dict() for card in cards]
    except Exception as exc:
        logger.warning("[playground] evidence card generation failed for %s: %s", strategy.name, exc)
        return []


def _build_evidence_vote_summary(results: list[StrategyResult]) -> dict[str, Any]:
    """Build observe-only ETF vote aggregation diagnostics."""
    return aggregate_etf_evidence(
        evidence_cards=evidence_cards_from_strategy_results(results),
        input_builder_exclusions=input_builder_exclusions_from_strategy_results(results),
    )


def _build_evidence_cap_diagnostics(
    results: list[StrategyResult],
    *,
    evidence_vote_summary: dict[str, Any],
    reference_weights: dict[str, float],
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build observe-only evidence-adjusted cap diagnostics."""
    return evaluate_evidence_quality_caps(
        vote_summary=evidence_vote_summary,
        evidence_cards=evidence_cards_from_strategy_results(results),
        current_or_target_weights=reference_weights,
        config=config,
    )


async def _load_strategy_memory_feedback(
    regime: str,
    strategy_names: list[str],
) -> dict[str, dict[str, Any]]:
    try:
        from services.memory_feedback import build_strategy_memory_feedback

        return await build_strategy_memory_feedback(regime, strategy_names)
    except Exception as exc:
        logger.warning("[playground] strategy memory feedback failed: %s", exc)
        return {
            name: _neutral_memory_feedback(name, regime, "memory feedback unavailable")
            for name in strategy_names
        }


def _neutral_memory_feedback(
    strategy_name: str,
    regime: str,
    reason: str = "not evaluated",
) -> dict[str, Any]:
    return {
        "strategy_name": strategy_name,
        "regime": regime,
        "sample_size": 0,
        "avg_decision_quality_score": None,
        "discount_multiplier": 1.0,
        "confidence": "low",
        "advisory_note": f"neutral memory feedback: {reason}",
        "can_bypass_risk_manager": False,
    }


def _required_fields_for_strategies(strategy_names: list[str]) -> set[str]:
    required: set[str] = set()
    for name in strategy_names:
        try:
            required.update(get_strategy(name).required_fields)
        except Exception as exc:
            logger.warning("[playground] could not read requirements for %s: %s", name, exc)
    return required


def _required_universe_for_strategies(strategy_names: list[str]) -> set[str]:
    tickers: set[str] = set()
    for name in strategy_names:
        try:
            tickers.update(get_strategy(name).universe_tickers)
        except Exception as exc:
            logger.warning("[playground] could not read universe for %s: %s", name, exc)
    return {ticker.upper().strip() for ticker in tickers if ticker}


def _rows_with_strategy_universe(
    holdings: list[dict[str, Any]],
    strategy_names: list[str],
) -> list[dict[str, Any]]:
    rows = list(filter_tradable_research_rows(holdings))
    by_ticker = {
        (row.get("ticker") or "").upper().strip(): dict(row)
        for row in holdings
        if (row.get("ticker") or "").upper().strip()
    }
    existing = {
        (row.get("ticker") or "").upper().strip()
        for row in rows
        if (row.get("ticker") or "").upper().strip()
    }
    for ticker in sorted(_required_universe_for_strategies(strategy_names)):
        if ticker in existing:
            continue
        row = dict(by_ticker.get(ticker) or {"ticker": ticker})
        row.setdefault("ticker", ticker)
        row.setdefault("universe_role", "strategy_playground")
        rows.append(row)
        existing.add(ticker)
    return rows


def _build_data_quality(
    readiness: dict[str, Any],
    holdings: list[dict[str, Any]],
    required_fields: tuple[str, ...],
) -> dict[str, Any]:
    filled_by_source: dict[str, set[str]] = {}
    for row in holdings:
        for source_info in row.get("feature_sources") or []:
            source = str(source_info.get("source") or "unknown")
            fields = set(source_info.get("filled_fields") or [])
            if required_fields:
                fields = fields & set(required_fields)
            filled_by_source.setdefault(source, set()).update(fields)
    return {
        "ready": bool(readiness.get("ready")),
        "coverage": readiness.get("coverage"),
        "missing_fields": readiness.get("missing_fields") or [],
        "excluded_tickers": readiness.get("excluded_tickers") or {},
        "exclusion_counts": readiness.get("exclusion_counts") or {},
        "field_coverage": readiness.get("field_coverage") or {},
        "filled_by_source": {
            source: sorted(fields)
            for source, fields in sorted(filled_by_source.items())
            if fields
        },
        "provenance_summary": summarize_feature_provenance(holdings),
    }


def _build_risk_profile(
    weights: dict[str, float] | None,
    turnover: float | None,
    estimated_cost: float | None,
    *,
    diagnostic_turnover: float | None = None,
    turnover_status: str = "actionable",
) -> dict[str, Any]:
    weights = weights or {}
    non_cash = {ticker: float(weight) for ticker, weight in weights.items() if ticker != "CASH" and weight > 0}
    max_single = max(non_cash.values()) if non_cash else 0.0
    concentration = "low"
    if max_single >= 0.20 or len(non_cash) <= 3:
        concentration = "high"
    elif max_single >= 0.12 or len(non_cash) <= 6:
        concentration = "medium"
    return {
        "turnover": round(turnover, 6) if turnover is not None else None,
        "diagnostic_turnover": round(diagnostic_turnover, 6) if diagnostic_turnover is not None else None,
        "fallback_cash_turnover": None,
        "turnover_status": turnover_status,
        "estimated_cost": estimated_cost,
        "position_count": len(non_cash),
        "max_single_weight": round(max_single, 6),
        "cash_weight": round(float(weights.get("CASH", 0.0)), 6),
        "concentration": concentration,
    }


def _build_agent_interpretation(
    strategy,
    scored: list[ScoredTicker],
    weights: dict[str, float] | None,
    context: dict[str, Any],
    readiness: dict[str, Any],
    feature_contract: dict[str, Any] | None = None,
    memory_feedback: dict[str, Any] | None = None,
) -> dict[str, Any]:
    weights = weights or {}
    selected = [ticker for ticker, weight in weights.items() if ticker != "CASH" and weight > 0.01]
    top_scores = [item.ticker for item in scored[:3]]
    contract = feature_contract or {}
    if not readiness.get("ready") or not contract.get("can_influence_allocation", True):
        verdict = contract.get("verdict") or "not_data_ready"
        reason = readiness.get("not_scored_reason") or verdict
        what = f"The strategy was not scored ({reason}) and should not influence allocation."
    elif readiness.get("status") == "partially_scored" and selected:
        excluded_count = int(readiness.get("excluded_ticker_count") or 0)
        what = (
            f"The strategy favors {', '.join(selected[:5])} after isolating "
            f"{excluded_count} ticker(s) with input issues."
        )
    elif selected:
        what = f"The strategy favors {', '.join(selected[:5])} based on its {strategy.family} logic."
    else:
        what = "The strategy finds no sufficiently attractive non-cash allocation."

    invalidate = _strategy_invalidation_hint(strategy.family, context.get("regime"))
    memory = memory_feedback or _neutral_memory_feedback(strategy.name, context.get("regime", ""))
    discount = float(memory.get("discount_multiplier") or 1.0)
    return {
        "what_it_is_saying": what,
        "top_score_tickers": top_scores,
        "how_to_use": strategy.agent_guidance or "Use this as one advisory signal, not as an execution instruction.",
        "what_would_invalidate_it": invalidate,
        "agent_checks": [
            "Do not select a strategy solely because its replay Sharpe is highest.",
            "Check whether its regime assumptions match current market regime and sector rotation.",
            "Check data quality, especially fields filled by yfinance rather than QC snapshots.",
            "Check memory feedback; same-regime underperformance reduces advisory weight only.",
            "Check turnover and estimated cost before giving it decision weight.",
            "Compare top picks against macro, news, risk, and position-manager constraints.",
        ],
        "feature_contract_verdict": contract.get("verdict"),
        "can_influence_allocation": contract.get("can_influence_allocation"),
        "memory_discount_multiplier": discount,
        "memory_feedback_note": memory.get("advisory_note"),
    }


def _strategy_invalidation_hint(family: str, regime: str | None) -> str:
    if family == "mean_reversion":
        return "Invalidated if downside momentum persists, volatility expands, or macro/news risk keeps deteriorating."
    if family in ("trend_following", "dual_momentum"):
        return "Invalidated if leadership reverses, breadth weakens, or the market shifts into choppy mean reversion."
    if family == "defensive_factor":
        return "Invalidated if breadth and cyclical leadership improve enough to make defensive assets opportunity-costly."
    if family == "carry_or_cash_proxy":
        return "Invalidated if risk-on breadth improves or rate/duration shocks dominate defensive carry."
    if family == "macro_rate":
        return "Invalidated if rate regime evidence changes, duration momentum breaks, or inflation shocks make bonds non-defensive."
    if family == "macro_cycle_rotation":
        return "Invalidated if macro-cycle evidence conflicts with price trend, rates shock cyclical assets, or growth risk overwhelms inflation/cyclical exposure."
    if family == "volatility_hedge":
        return "Invalidated if volatility spike has already mean-reverted or VIX futures decay overwhelms hedge value."
    if family == "event_risk_avoidance":
        return "Invalidated if breakdown evidence fades, the market returns to risk-on, or inverse ETF decay/whipsaw risk dominates."
    if family == "seasonality_flow":
        return "Invalidated if the signal is outside the turn-of-month window, volatility is stressed, or the flow effect fails live validation."
    if family == "sector_theme_rotation":
        return "Invalidated if sector leadership reverses, theme concentration becomes crowded, or high-volatility risk gates block expansion."
    if family == "risk_budgeting":
        return "Invalidated as an alpha view if expected-return evidence strongly favors a specific leadership theme."
    if family == "leveraged_rotation":
        return "Invalidated if leveraged ETF decay, ATR expansion, or regime whipsaw overwhelms the risk-on trend evidence."
    return f"Validate against current regime={regime or 'unknown'}, rotation, macro/news risk, and execution constraints."


def compute_weight_divergence(results: list[StrategyResult], top_n: int = 10) -> list[dict[str, Any]]:
    ready_results = [result for result in results if result.data_ready]
    tickers = sorted({ticker for result in ready_results for ticker in (result.weights or {}) if ticker != "CASH"})
    rows: list[dict[str, Any]] = []
    for ticker in tickers:
        weights = {
            result.strategy_name: float((result.weights or {}).get(ticker, 0.0))
            for result in ready_results
        }
        vals = list(weights.values())
        rows.append({
            "ticker": ticker,
            "min_weight": round(min(vals), 4),
            "max_weight": round(max(vals), 4),
            "spread": round(max(vals) - min(vals), 4),
            "weights": weights,
        })
    rows.sort(key=lambda item: item["spread"], reverse=True)
    return rows[:top_n]


def compute_consensus_weights(results: list[StrategyResult]) -> dict[str, float]:
    ready_results = [result for result in results if result.data_ready]
    if not ready_results:
        return {"CASH": 1.0}
    tickers = sorted({ticker for result in ready_results for ticker in (result.weights or {})})
    strategy_multipliers = {
        result.strategy_name: max(0.0, float((result.memory_feedback or {}).get("discount_multiplier") or 1.0))
        for result in ready_results
    }
    multiplier_total = sum(strategy_multipliers.values())
    if multiplier_total <= 0:
        strategy_multipliers = {result.strategy_name: 1.0 for result in ready_results}
        multiplier_total = float(len(ready_results))
    averaged = {
        ticker: sum(
            float((result.weights or {}).get(ticker, 0.0)) * strategy_multipliers[result.strategy_name]
            for result in ready_results
        ) / multiplier_total
        for ticker in tickers
    }
    total = sum(averaged.values())
    if total <= 0:
        return {"CASH": 1.0}
    out = {ticker: round(weight / total, 4) for ticker, weight in averaged.items() if ticker != "CASH"}
    out["CASH"] = round(max(1.0 - sum(out.values()), 0.0), 4)
    return out


def _compute_strategy_confidence(
    results: list[StrategyResult],
    live_metrics: dict[str, dict[str, Any]],
    historical_metrics: dict[str, dict[str, Any]],
    regime: str,
    consensus_weights: dict[str, float],
    walk_forward_validation: dict[str, Any] | None = None,
) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    consensus_conflict = bool(_detect_consensus_regime_conflicts(regime, consensus_weights))
    walk_forward_items = (walk_forward_validation or {}).get("items") or {}
    for result in results:
        name = result.strategy_name
        hist = historical_metrics.get(name) or {}
        live = live_metrics.get(name) or {}
        walk_forward = walk_forward_items.get(name) or {}
        historical_score = _historical_evidence_score(hist)
        live_fit_score = _live_fit_score(result, live, regime)
        walk_forward_score = _walk_forward_evidence_score(walk_forward)
        turnover_penalty = _turnover_penalty(result.expected_turnover_pct)
        data_penalty = 0.20 if not result.data_ready else 0.0
        strategy_conflict = bool(_detect_strategy_regime_conflicts(regime, result.weights or {}))
        if walk_forward:
            confidence_score = max(
                0.0,
                min(
                    1.0,
                    0.45 * historical_score
                    + 0.25 * live_fit_score
                    + 0.20 * walk_forward_score
                    - turnover_penalty
                    - data_penalty,
                ),
            )
        else:
            confidence_score = max(
                0.0,
                min(1.0, 0.70 * historical_score + 0.20 * live_fit_score - turnover_penalty - data_penalty),
            )
        suggested_use = _suggested_strategy_use(
            confidence_score=confidence_score,
            result=result,
            hist_metrics=hist,
            walk_forward=walk_forward,
            turnover_penalty=turnover_penalty,
            strategy_conflict=strategy_conflict,
        )
        out[name] = {
            "strategy_name": name,
            "historical_score": round(historical_score, 4),
            "live_fit_score": round(live_fit_score, 4),
            "walk_forward_score": round(walk_forward_score, 4),
            "turnover_penalty": round(turnover_penalty, 4),
            "data_penalty": round(data_penalty, 4),
            "confidence_score": round(confidence_score, 4),
            "suggested_use": suggested_use,
            "historical_reliability": (hist.get("metric_reliability") or {}).get("level", "unknown"),
            "walk_forward_level": walk_forward.get("level", "missing"),
            "walk_forward_valid_folds": walk_forward.get("valid_fold_count", 0),
            "walk_forward_pass_rate": walk_forward.get("pass_rate"),
            "historical_samples": hist.get("n_forward_return_samples", 0),
            "live_samples": live.get("n_forward_return_samples", 0),
            "execution_intel_status": _execution_intel_status_from_metrics(live),
            "regime_fit": result.regime_fit,
            "consensus_conflict": consensus_conflict,
            "strategy_regime_conflict": strategy_conflict,
            "defensive_weight": round(_defensive_exposure(result.weights or {})["weight"], 4),
            "reason_codes": _strategy_confidence_reason_codes(
                result,
                hist,
                live,
                walk_forward,
                strategy_conflict,
            ),
            "notes": _strategy_confidence_notes(result, hist, live, walk_forward, strategy_conflict),
        }
    return out


def _historical_evidence_score(metrics: dict[str, Any]) -> float:
    reliability = (metrics.get("metric_reliability") or {}).get("level")
    reliability_score = {"high": 1.0, "medium": 0.65, "insufficient": 0.25}.get(reliability, 0.0)
    sharpe = metrics.get("sharpe")
    hit_rate = metrics.get("hit_rate")
    sharpe_score = 0.5
    if sharpe is not None:
        sharpe_score = max(0.0, min(1.0, (float(sharpe) + 0.5) / 2.5))
    hit_score = 0.5
    if hit_rate is not None:
        hit_score = max(0.0, min(1.0, float(hit_rate)))
    return 0.50 * reliability_score + 0.30 * sharpe_score + 0.20 * hit_score


def _live_fit_score(result: StrategyResult, live_metrics: dict[str, Any], regime: str) -> float:
    fit_score = {"strong": 0.85, "medium": 0.60, "benchmark": 0.45, "unknown": 0.35}.get(result.regime_fit, 0.35)
    readiness_score = 1.0 if result.data_ready else 0.0
    return 0.65 * fit_score + 0.35 * readiness_score


def _execution_intel_status_from_metrics(live_metrics: dict[str, Any]) -> str:
    samples = int((live_metrics or {}).get("n_forward_return_samples") or 0)
    if samples <= 0:
        return "insufficient_data"
    return "live_available"


def _turnover_penalty(turnover: float | None) -> float:
    if turnover is None:
        return 0.0
    if turnover <= 0.20:
        return 0.0
    if turnover <= 0.50:
        return 0.08
    if turnover <= 0.80:
        return 0.16
    return 0.24


def _suggested_strategy_use(
    *,
    confidence_score: float,
    result: StrategyResult,
    hist_metrics: dict[str, Any],
    walk_forward: dict[str, Any],
    turnover_penalty: float,
    strategy_conflict: bool,
) -> str:
    reliability = (hist_metrics.get("metric_reliability") or {}).get("level")
    walk_forward_level = str(walk_forward.get("level") or "missing")
    if not result.data_ready:
        return "ignore"
    if confidence_score >= 0.72 and reliability == "high" and walk_forward_level in {"high", "missing"} and turnover_penalty <= 0.08 and not strategy_conflict:
        return "primary"
    if walk_forward_level in {"weak", "insufficient"} and confidence_score < 0.62:
        return "watch_only"
    if confidence_score >= 0.50:
        return "advisory"
    return "watch_only"


def _strategy_confidence_notes(
    result: StrategyResult,
    hist_metrics: dict[str, Any],
    live_metrics: dict[str, Any],
    walk_forward: dict[str, Any],
    strategy_conflict: bool,
) -> list[str]:
    notes: list[str] = []
    hist_level = (hist_metrics.get("metric_reliability") or {}).get("level", "unknown")
    live_level = (live_metrics.get("metric_reliability") or {}).get("level", "unknown")
    notes.append(f"historical_reliability={hist_level}")
    notes.append(f"live_qc_reliability={live_level}")
    if walk_forward:
        notes.append(f"walk_forward={walk_forward.get('level', 'unknown')}")
    if result.expected_turnover_pct is not None and result.expected_turnover_pct > 0.50:
        notes.append("high_turnover")
    elif result.expected_turnover_pct is None and result.turnover_status == "not_scored":
        notes.append("turnover_not_applicable_not_scored")
    if strategy_conflict:
        notes.append("strategy_weights_conflict_with_regime")
    return notes


def _strategy_confidence_reason_codes(
    result: StrategyResult,
    hist_metrics: dict[str, Any],
    live_metrics: dict[str, Any],
    walk_forward: dict[str, Any],
    strategy_conflict: bool,
) -> list[str]:
    codes: list[str] = []
    hist_level = (hist_metrics.get("metric_reliability") or {}).get("level")
    live_level = (live_metrics.get("metric_reliability") or {}).get("level")
    hist_samples = int(hist_metrics.get("n_forward_return_samples") or 0)
    live_samples = int(live_metrics.get("n_forward_return_samples") or 0)
    sharpe = hist_metrics.get("sharpe")

    if not result.data_ready:
        codes.append("data_not_ready")
    elif result.score_status == "partially_scored":
        codes.append("partial_universe_excluded")
    if hist_level == "high" and hist_samples >= MIN_REPLAY_SAMPLES_FOR_STRONG_EVIDENCE:
        codes.append("historical_strong")
    elif hist_level in {"medium", "insufficient"}:
        codes.append(f"historical_{hist_level}")
    else:
        codes.append("historical_missing")
    if sharpe is not None:
        try:
            codes.append("historical_positive_sharpe" if float(sharpe) > 0 else "historical_nonpositive_sharpe")
        except (TypeError, ValueError):
            pass
    if result.regime_fit == "strong":
        codes.append("regime_fit_strong")
    elif result.regime_fit in {"medium", "benchmark"}:
        codes.append(f"regime_fit_{result.regime_fit}")
    else:
        codes.append("regime_fit_weak")
    if live_level == "high":
        codes.append("live_qc_supported")
    elif live_samples > 0:
        codes.append("live_qc_limited")
    else:
        codes.append("live_qc_missing")
    if walk_forward:
        codes.extend(str(code) for code in walk_forward.get("reason_codes") or [])
    else:
        codes.append("walk_forward_missing")
    if result.expected_turnover_pct is None and result.turnover_status == "not_scored":
        codes.append("turnover_not_applicable_not_scored")
    elif result.expected_turnover_pct is not None and result.expected_turnover_pct > 0.50:
        codes.append("high_turnover")
    elif result.expected_turnover_pct is not None and result.expected_turnover_pct > 0.20:
        codes.append("moderate_turnover")
    else:
        codes.append("low_turnover")
    if strategy_conflict:
        codes.append("strategy_regime_conflict")
    return list(dict.fromkeys(codes))


def _walk_forward_evidence_score(row: dict[str, Any]) -> float:
    level = str(row.get("level") or "missing")
    if level == "high":
        return 1.0
    if level == "medium":
        return 0.70
    if level == "weak":
        return 0.30
    if level == "insufficient":
        return 0.20
    return 0.0


def _build_playground_evidence_summary(
    *,
    snapshot_count: int,
    historical_snapshot_count: int,
    replay_metrics: dict[str, dict[str, Any]],
    historical_replay_metrics: dict[str, dict[str, Any]],
    strategy_confidence: dict[str, dict[str, Any]],
    walk_forward_validation: dict[str, Any] | None = None,
    data_gaps: list[str],
) -> dict[str, Any]:
    historical = _historical_evidence_level(historical_snapshot_count, historical_replay_metrics)
    walk_forward = _walk_forward_evidence_level(walk_forward_validation or {})
    execution_intel = _execution_intel_level(snapshot_count, replay_metrics, strategy_confidence)
    permission = _execution_permission_level(historical, walk_forward, execution_intel, strategy_confidence)
    return {
        "historical_evidence": historical["level"],
        "historical_samples": historical["samples"],
        "historical_reliability": historical["reliability"],
        "walk_forward_validation": walk_forward["level"],
        "walk_forward_valid_folds": walk_forward["valid_folds"],
        "live_fit": execution_intel["legacy_live_fit"],
        "execution_intel_status": execution_intel["level"],
        "live_samples": execution_intel["samples"],
        "qc_snapshot_count": int(snapshot_count or 0),
        "execution_intel": {
            "qc_snapshot_count": int(snapshot_count or 0),
            "forward_return_samples": execution_intel["samples"],
            "status": execution_intel["level"],
            "reason": execution_intel["reasons"][0] if execution_intel["reasons"] else None,
        },
        "execution_permission": permission["level"],
        "best_strategy": _best_strategy_summary(strategy_confidence),
        "summary_reasons": list(dict.fromkeys(
            historical["reasons"]
            + walk_forward["reasons"]
            + execution_intel["reasons"]
            + permission["reasons"]
            + [str(gap) for gap in data_gaps[:3]]
        )),
    }


def _historical_evidence_level(
    historical_snapshot_count: int,
    historical_replay_metrics: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    samples = max(
        [int((row or {}).get("n_forward_return_samples") or 0) for row in historical_replay_metrics.values()]
        or [0]
    )
    reliabilities = [
        str(((row or {}).get("metric_reliability") or {}).get("level") or "unknown")
        for row in historical_replay_metrics.values()
    ]
    reliability_rank = {"high": 3, "medium": 2, "insufficient": 1, "unknown": 0}
    best_reliability = max(reliabilities or ["unknown"], key=lambda item: reliability_rank.get(item, 0))
    reasons: list[str] = []
    if samples >= MIN_REPLAY_SAMPLES_FOR_STRONG_EVIDENCE and best_reliability == "high":
        level = "strong"
        reasons.append(f"yfinance historical replay has {samples} forward samples")
    elif samples >= MIN_REPLAY_SAMPLES_FOR_PERFORMANCE and best_reliability in {"high", "medium"}:
        level = "medium"
        reasons.append(f"yfinance historical replay has {samples} forward samples")
    elif historical_snapshot_count > 0 or samples > 0:
        level = "weak"
        reasons.append(f"yfinance historical replay has limited samples ({samples})")
    else:
        level = "missing"
        reasons.append("no yfinance historical replay evidence")
    return {"level": level, "samples": samples, "reliability": best_reliability, "reasons": reasons}


def _walk_forward_evidence_level(validation: dict[str, Any]) -> dict[str, Any]:
    items = validation.get("items") or {}
    if not items:
        return {
            "level": "missing",
            "valid_folds": 0,
            "reasons": ["no walk-forward validation available"],
        }
    rows = [row for row in items.values() if isinstance(row, dict)]
    rank = {"high": 3, "medium": 2, "weak": 1, "insufficient": 0, "missing": 0}
    best = max(rows, key=lambda row: rank.get(str(row.get("level") or "missing"), 0))
    level = str(best.get("level") or "missing")
    valid_folds = max([int(row.get("valid_fold_count") or 0) for row in rows] or [0])
    return {
        "level": level,
        "valid_folds": valid_folds,
        "reasons": [
            f"walk-forward validation {level} across {valid_folds} valid folds"
        ],
    }


def _execution_intel_level(
    snapshot_count: int,
    replay_metrics: dict[str, dict[str, Any]],
    strategy_confidence: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    samples = max(
        [int((row or {}).get("n_forward_return_samples") or 0) for row in replay_metrics.values()]
        or [0]
    )
    if snapshot_count < 20 or samples < MIN_REPLAY_SAMPLES_FOR_PERFORMANCE:
        return {
            "level": "insufficient_data",
            "legacy_live_fit": "insufficient",
            "samples": samples,
            "reasons": [f"QC live replay has {snapshot_count} snapshots and {samples} forward samples"],
        }
    return {
        "level": "live_available",
        "legacy_live_fit": "aligned",
        "samples": samples,
        "reasons": ["QC live replay is available for execution monitoring"],
    }


def _live_fit_level(
    snapshot_count: int,
    replay_metrics: dict[str, dict[str, Any]],
    strategy_confidence: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    """Backward-compatible view over execution intel for older tests/tools."""
    execution_intel = _execution_intel_level(snapshot_count, replay_metrics, strategy_confidence)
    return {
        "level": execution_intel["legacy_live_fit"],
        "samples": execution_intel["samples"],
        "reasons": execution_intel["reasons"],
    }


def _execution_permission_level(
    historical: dict[str, Any],
    walk_forward: dict[str, Any],
    execution_intel: dict[str, Any],
    strategy_confidence: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    rows = [row for row in strategy_confidence.values() if isinstance(row, dict)]
    primary = [row for row in rows if row.get("suggested_use") == "primary"]
    advisory = [row for row in rows if row.get("suggested_use") == "advisory"]
    if not rows or (historical["level"] in {"missing", "weak"} and not primary and not advisory):
        return {
            "level": "blocked",
            "reasons": ["no actionable strategy confidence"],
        }
    if any(bool(row.get("consensus_conflict")) for row in rows):
        return {
            "level": "human_required",
            "reasons": ["strategy evidence conflicts with regime/consensus"],
        }
    if walk_forward["level"] == "weak":
        return {
            "level": "human_required",
            "reasons": ["walk-forward validation is weak across historical folds"],
        }
    if primary and historical["level"] == "strong" and walk_forward["level"] in {"high", "missing"}:
        return {
            "level": "allowed",
            "reasons": ["primary strategy has strong historical evidence"],
        }
    if primary or advisory:
        return {
            "level": "advisory",
            "reasons": ["strategy evidence is useful but not fully confirmed by live QC fit"],
        }
    return {
        "level": "human_required",
        "reasons": ["strategy confidence is watch-only"],
    }


def _best_strategy_summary(strategy_confidence: dict[str, dict[str, Any]]) -> dict[str, Any] | None:
    rows = [row for row in strategy_confidence.values() if isinstance(row, dict)]
    if not rows:
        return None
    use_rank = {"primary": 0, "advisory": 1, "watch_only": 2, "ignore": 3}
    best = sorted(
        rows,
        key=lambda row: (
            use_rank.get(str(row.get("suggested_use") or "watch_only"), 9),
            -float(row.get("confidence_score") or 0.0),
        ),
    )[0]
    return {
        "strategy_name": best.get("strategy_name"),
        "suggested_use": best.get("suggested_use"),
        "confidence_score": best.get("confidence_score"),
        "reason_codes": best.get("reason_codes") or [],
    }


async def _read_recent_snapshots(days: int) -> list[dict[str, Any]]:
    cutoff = datetime.utcnow() - timedelta(days=days)
    row_limit = _recent_snapshot_row_limit(days)
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(QCSnapshot)
            .where(QCSnapshot.received_at >= cutoff)
            .where(QCSnapshot.packet_type.in_(("daily_feature_snapshot", "heartbeat")))
            .order_by(desc(QCSnapshot.received_at))
            .limit(row_limit)
        )
        rows = result.scalars().all()
    snapshots = _dedupe_market_snapshots(rows)
    snapshots.reverse()
    return snapshots


def _recent_snapshot_row_limit(days: int) -> int:
    # Heartbeats arrive about 26 times per market day; leave headroom for
    # daily_feature_snapshot rows and scheduling jitter before daily dedupe.
    return max(180, int(days or 0) * 40)


async def _read_yfinance_feature_snapshots(days: int) -> list[dict[str, Any]]:
    cutoff = datetime.utcnow().date() - timedelta(days=days)
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(MarketDailyFeature)
            .where(MarketDailyFeature.source == "yfinance")
            .where(MarketDailyFeature.trading_date >= cutoff)
            .order_by(MarketDailyFeature.trading_date, MarketDailyFeature.ticker)
        )
        rows = result.scalars().all()
    return _feature_rows_to_snapshots(rows)


def _feature_rows_to_snapshots(rows: list[Any]) -> list[dict[str, Any]]:
    by_date: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        trading_date = row.trading_date.isoformat() if row.trading_date else None
        if not trading_date:
            continue
        holding = _feature_model_to_holding(row)
        by_date.setdefault(trading_date, []).append(holding)
    return [
        {
            "packet_type": "yfinance_historical",
            "trading_date": trading_date,
            "features": holdings,
            "holdings": holdings,
            "portfolio": {},
        }
        for trading_date, holdings in sorted(by_date.items())
        if holdings
    ]


def _feature_model_to_holding(row: Any) -> dict[str, Any]:
    holding = {
        "ticker": row.ticker,
        "universe_role": "research",
        "price": _float_or_none(row.close_price) or _float_or_none(row.adj_close_price),
        "close_price": _float_or_none(row.close_price) or _float_or_none(row.adj_close_price),
        "open_price": _float_or_none(row.open_price),
        "high_price": _float_or_none(row.high_price),
        "low_price": _float_or_none(row.low_price),
        "volume": int(row.volume) if row.volume is not None else None,
        "dollar_volume": _float_or_none(row.dollar_volume),
        "daily_return_pct": _float_or_none(row.return_1d),
        "return_1d": _float_or_none(row.return_1d),
        "return_5d": _float_or_none(row.return_5d),
        "mom_20d": _float_or_none(row.return_20d),
        "mom_60d": _float_or_none(row.return_60d),
        "mom_252d": _float_or_none(row.return_252d),
        "sma_20": _float_or_none(row.sma_20),
        "sma_50": _float_or_none(row.sma_50),
        "sma_200": _float_or_none(row.sma_200),
        "hist_vol_20d": _float_or_none(row.hist_vol_20d),
        "rsi_10": _float_or_none(getattr(row, "rsi_10", None)),
        "rsi_14": _float_or_none(getattr(row, "rsi_14", None)),
        "atr_pct": _float_or_none(getattr(row, "atr_pct", None)),
        "bb_position": _float_or_none(getattr(row, "bb_position", None)),
        "beta_vs_spy": _float_or_none(getattr(row, "beta_vs_spy", None)),
    }
    filled_fields = sorted(
        field for field, value in holding.items()
        if field not in {"ticker", "universe_role", "feature_sources"}
        and value is not None
    )
    holding["feature_sources"] = [{
        "source": "yfinance_historical",
        "filled_fields": filled_fields,
        "authority_by_field": {
            field: authority_for_field(field, "yfinance_historical").value
            for field in filled_fields
        },
        "canonical_aliases": {
            field: canonical_field_name(field)
            for field in filled_fields
            if canonical_field_name(field) != field
        },
        "trading_date": row.trading_date.isoformat() if row.trading_date else None,
    }]
    return holding


def _float_or_none(value: Any) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _compute_replay_metrics(
    snapshots: list[dict[str, Any]],
    strategy_names: list[str],
) -> dict[str, dict[str, Any]]:
    metrics: dict[str, dict[str, Any]] = {}
    previous_weights: dict[str, dict[str, float]] = {}
    for name in strategy_names:
        turnovers: list[float] = []
        position_counts: list[int] = []
        cash_weights: list[float] = []
        score_leaders: list[str] = []
        strategy_returns: list[float] = []
        ic_values: list[float] = []

        for idx, snapshot in enumerate(snapshots):
            snapshot_date = _snapshot_as_of_date(snapshot)
            brief = _brief_from_snapshot(snapshot)
            holdings = _rows_for_strategy_snapshot(snapshot, name)
            if not holdings:
                continue
            portfolio = brief.get("portfolio") or {}
            spy = next((h for h in holdings if (h.get("ticker") or "").upper() == "SPY"), {})
            regime = classify_market_regime(portfolio, spy, holdings=holdings)
            macro_regime_context = brief.get("macro_regime_context") or build_deterministic_macro_regime(holdings)
            context = {
                "regime": regime.regime.value,
                "confidence": _confidence_to_float(regime.confidence),
                "uncertainty_flag": regime.confidence == "low",
                "stance": _stance_for_regime(regime.regime.value),
                "direction_bias": _direction_bias_for_regime(regime.regime.value),
                "risk_params": {},
                "current_weights": brief.get("current_weights") or {},
                "sector_rotation": brief.get("sector_rotation") or {},
                "macro_context": macro_regime_context,
                "rate_regime_label": macro_regime_context.get("rate_regime_label"),
                "inflation_regime_label": macro_regime_context.get("inflation_regime_label"),
                "growth_regime_label": macro_regime_context.get("growth_regime_label"),
            }
            result = _run_one_strategy(
                name,
                holdings,
                context,
                previous_weights.get(name, {}),
                as_of_date=snapshot_date,
            )
            weights = result.weights or {}
            if not weights:
                continue
            prev = previous_weights.get(name)
            if prev is not None:
                turnovers.append(_turnover(weights, prev))
            previous_weights[name] = weights
            position_counts.append(sum(1 for ticker, weight in weights.items() if ticker != "CASH" and weight > 0.01))
            cash_weights.append(float(weights.get("CASH", 0.0)))
            if result.score_breakdown:
                score_leaders.append(result.score_breakdown[0]["ticker"])

            if idx + 1 < len(snapshots):
                next_returns = _extract_daily_returns(_raw_snapshot_rows(snapshots[idx + 1]))
                if next_returns:
                    strategy_returns.append(
                        sum(float(weights.get(ticker, 0.0)) * ret for ticker, ret in next_returns.items())
                    )
                    score_by_ticker = {
                        item["ticker"]: float(item["score"])
                        for item in result.score_breakdown
                    }
                    common = [ticker for ticker in score_by_ticker if ticker in next_returns]
                    if len(common) >= 3:
                        ic = _correlation(
                            [score_by_ticker[ticker] for ticker in common],
                            [next_returns[ticker] for ticker in common],
                        )
                        if ic is not None:
                            ic_values.append(ic)

        sample_count = len(strategy_returns)
        enough_samples = sample_count >= MIN_REPLAY_SAMPLES_FOR_PERFORMANCE
        reliability = _replay_metric_reliability(
            sample_count=sample_count,
            ic_sample_count=len(ic_values),
            strategy_ready_samples=len(position_counts),
        )
        metrics[name] = {
            "avg_turnover": round(_avg(turnovers), 6) if turnovers else None,
            "max_turnover": round(max(turnovers), 6) if turnovers else None,
            "avg_position_count": round(_avg(position_counts), 2) if position_counts else None,
            "avg_cash_weight": round(_avg(cash_weights), 4) if cash_weights else None,
            "max_drawdown_pct": _max_drawdown(strategy_returns) if enough_samples else None,
            "top_signal_leaders": _top_counts(score_leaders, limit=5),
            "sharpe": _annualized_sharpe(strategy_returns) if enough_samples else None,
            "ic": round(_avg(ic_values), 4) if enough_samples and ic_values else None,
            "hit_rate": round(
                sum(1 for value in strategy_returns if value > 0) / len(strategy_returns),
                4,
            ) if enough_samples else None,
            "n_forward_return_samples": sample_count,
            "n_ic_samples": len(ic_values),
            "metric_reliability": reliability,
            "metric_notes": (
                "Replay metrics are high-reliability directional evidence, but still research-only."
                if reliability["level"] == "high"
                else "Replay metrics are usable but not decisive; require regime/data/risk confirmation."
                if reliability["level"] == "medium"
                else f"Sharpe/IC/hit-rate suppressed until >={MIN_REPLAY_SAMPLES_FOR_PERFORMANCE} forward-return samples; current={sample_count}."
                if strategy_returns
                else "Sharpe/IC require per-ticker daily_return_pct or return_1d from daily market snapshots."
            ),
            "selection_guardrail": (
                "May be considered as one input, never as execution authority."
                if enough_samples
                else "Do not select this strategy based on replay performance; sample size is insufficient."
            ),
        }
    return metrics


def _replay_metric_reliability(
    *,
    sample_count: int,
    ic_sample_count: int,
    strategy_ready_samples: int,
) -> dict[str, Any]:
    reasons: list[str] = []
    if sample_count < MIN_REPLAY_SAMPLES_FOR_PERFORMANCE:
        reasons.append(
            f"forward_return_samples {sample_count} < minimum {MIN_REPLAY_SAMPLES_FOR_PERFORMANCE}"
        )
    if strategy_ready_samples < MIN_REPLAY_SAMPLES_FOR_PERFORMANCE:
        reasons.append(
            f"strategy_ready_samples {strategy_ready_samples} < minimum {MIN_REPLAY_SAMPLES_FOR_PERFORMANCE}"
        )
    if sample_count >= MIN_REPLAY_SAMPLES_FOR_STRONG_EVIDENCE and ic_sample_count >= MIN_REPLAY_SAMPLES_FOR_PERFORMANCE:
        level = "high"
    elif sample_count >= MIN_REPLAY_SAMPLES_FOR_PERFORMANCE:
        level = "medium"
        if ic_sample_count < MIN_REPLAY_SAMPLES_FOR_PERFORMANCE:
            reasons.append(
                f"ic_samples {ic_sample_count} < minimum {MIN_REPLAY_SAMPLES_FOR_PERFORMANCE}"
            )
    else:
        level = "insufficient"

    return {
        "level": level,
        "sample_count": sample_count,
        "ic_sample_count": ic_sample_count,
        "strategy_ready_samples": strategy_ready_samples,
        "min_samples_for_metrics": MIN_REPLAY_SAMPLES_FOR_PERFORMANCE,
        "min_samples_for_strong_evidence": MIN_REPLAY_SAMPLES_FOR_STRONG_EVIDENCE,
        "reasons": reasons,
    }


def _compute_walk_forward_validation(
    snapshots: list[dict[str, Any]],
    strategy_names: list[str],
    *,
    fold_count: int = 4,
) -> dict[str, Any]:
    if len(snapshots) < 2 or not strategy_names:
        return {}
    clean_snapshots = [snapshot for snapshot in snapshots if _snapshot_rows(snapshot)]
    if len(clean_snapshots) < 2:
        return {}
    folds = _walk_forward_snapshot_folds(clean_snapshots, fold_count=fold_count)
    returns_by_strategy: dict[str, list[list[float]]] = {name: [] for name in strategy_names}
    for fold in folds:
        fold_returns = _strategy_forward_returns_for_snapshots(fold, strategy_names)
        for name in strategy_names:
            returns_by_strategy.setdefault(name, []).append(fold_returns.get(name, []))
    return validate_walk_forward(returns_by_strategy)


def _walk_forward_snapshot_folds(
    snapshots: list[dict[str, Any]],
    *,
    fold_count: int,
) -> list[list[dict[str, Any]]]:
    if fold_count <= 1:
        return [snapshots]
    min_fold_size = 2
    fold_count = max(1, min(int(fold_count), max(1, len(snapshots) // min_fold_size)))
    base_size = len(snapshots) // fold_count
    remainder = len(snapshots) % fold_count
    folds: list[list[dict[str, Any]]] = []
    start = 0
    for idx in range(fold_count):
        size = base_size + (1 if idx < remainder else 0)
        end = start + size
        fold = snapshots[start:end]
        if len(fold) >= min_fold_size:
            folds.append(fold)
        start = end
    return folds


def _strategy_forward_returns_for_snapshots(
    snapshots: list[dict[str, Any]],
    strategy_names: list[str],
) -> dict[str, list[float]]:
    returns_by_strategy: dict[str, list[float]] = {name: [] for name in strategy_names}
    previous_weights: dict[str, dict[str, float]] = {}
    for idx, snapshot in enumerate(snapshots[:-1]):
        snapshot_date = _snapshot_as_of_date(snapshot)
        brief = _brief_from_snapshot(snapshot)
        holdings = brief.get("holdings") or []
        if not holdings:
            continue
        portfolio = brief.get("portfolio") or {}
        spy = next((h for h in holdings if (h.get("ticker") or "").upper() == "SPY"), {})
        regime = classify_market_regime(portfolio, spy, holdings=holdings)
        macro_regime_context = brief.get("macro_regime_context") or build_deterministic_macro_regime(holdings)
        context = {
            "regime": regime.regime.value,
            "confidence": _confidence_to_float(regime.confidence),
            "uncertainty_flag": regime.confidence == "low",
            "stance": _stance_for_regime(regime.regime.value),
            "direction_bias": _direction_bias_for_regime(regime.regime.value),
            "risk_params": {},
            "current_weights": brief.get("current_weights") or {},
            "sector_rotation": brief.get("sector_rotation") or {},
            "macro_context": macro_regime_context,
            "rate_regime_label": macro_regime_context.get("rate_regime_label"),
            "inflation_regime_label": macro_regime_context.get("inflation_regime_label"),
            "growth_regime_label": macro_regime_context.get("growth_regime_label"),
        }
        next_returns = _extract_daily_returns(_raw_snapshot_rows(snapshots[idx + 1]))
        if not next_returns:
            continue
        for name in strategy_names:
            result = _run_one_strategy(
                name,
                _rows_for_strategy_snapshot(snapshot, name),
                context,
                previous_weights.get(name, {}),
                as_of_date=snapshot_date,
            )
            weights = result.weights or {}
            if not weights:
                continue
            previous_weights[name] = weights
            returns_by_strategy.setdefault(name, []).append(
                sum(float(weights.get(ticker, 0.0)) * ret for ticker, ret in next_returns.items())
            )
    return returns_by_strategy


def _brief_from_snapshot(snapshot: dict[str, Any]) -> dict[str, Any]:
    holdings = _snapshot_rows(snapshot)
    return {
        "holdings": holdings,
        "portfolio": snapshot.get("portfolio") or {},
        "current_weights": _extract_current_weights(holdings),
        "risk_params": {},
        "sector_rotation": detect_sector_rotation(holdings),
        "macro_regime_context": build_deterministic_macro_regime(holdings),
    }


def _snapshot_as_of_date(snapshot: dict[str, Any]) -> date:
    for key in ("trading_date", "date", "timestamp_utc", "timestamp", "received_at"):
        parsed = _date_from_value(snapshot.get(key))
        if parsed:
            return parsed
    for row in _raw_snapshot_rows(snapshot):
        for source_info in row.get("feature_sources") or []:
            parsed = _date_from_value(source_info.get("trading_date"))
            if parsed:
                return parsed
    return datetime.utcnow().date()


def _date_from_value(value: Any) -> date | None:
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


def _snapshot_rows(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    return filter_tradable_research_rows(snapshot.get("holdings") or snapshot.get("features") or [])


def _raw_snapshot_rows(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    return list(snapshot.get("holdings") or snapshot.get("features") or [])


def _rows_for_strategy_snapshot(snapshot: dict[str, Any], strategy_name: str) -> list[dict[str, Any]]:
    try:
        strategy = get_strategy(strategy_name)
        return strategy.eligible_rows(_raw_snapshot_rows(snapshot))
    except Exception:
        return _snapshot_rows(snapshot)


def _dedupe_market_snapshots(rows: list[QCSnapshot]) -> list[dict[str, Any]]:
    """
    Keep one market snapshot per trading day, preferring daily_feature_snapshot
    over heartbeat because it has richer raw features for replay.
    """
    by_date: dict[str, tuple[int, dict[str, Any]]] = {}
    priority = {"heartbeat": 1, "daily_feature_snapshot": 2}
    for row in rows:
        payload = row.raw_payload or {}
        key = str(row.trading_date or payload.get("trading_date") or row.received_at.date())
        score = priority.get(row.packet_type, 0)
        existing = by_date.get(key)
        if existing is None or score > existing[0]:
            by_date[key] = (score, payload)
    return [payload for _, payload in by_date.values()]


def _extract_current_weights(holdings: list[dict]) -> dict[str, float]:
    out: dict[str, float] = {}
    for h in holdings:
        ticker = (h.get("ticker") or "").upper().strip()
        if not ticker:
            continue
        try:
            out[ticker] = float(h.get("weight_current") or 0.0)
        except (TypeError, ValueError):
            out[ticker] = 0.0
    return out


def _extract_daily_returns(holdings: list[dict]) -> dict[str, float]:
    out: dict[str, float] = {}
    for h in holdings:
        ticker = (h.get("ticker") or "").upper().strip()
        if not ticker:
            continue
        value = h.get("daily_return_pct")
        if value is None:
            value = h.get("return_1d")
        try:
            out[ticker] = float(value)
        except (TypeError, ValueError):
            continue
    return out


def _detect_data_gaps(snapshots: list[dict[str, Any]]) -> list[str]:
    gaps: list[str] = []
    if len(snapshots) < 10:
        gaps.append(f"only {len(snapshots)} daily market snapshots in lookback window")
    sample_holdings = [h for s in snapshots for h in _snapshot_rows(s)]
    if sample_holdings and not any("daily_return_pct" in h or "return_1d" in h for h in sample_holdings):
        gaps.append("per-ticker forward returns missing; Sharpe/IC not computed")
    if any(s.get("packet_type") == "heartbeat" for s in snapshots):
        gaps.append("some replay rows use heartbeat fallback; prefer daily_feature_snapshot for cleaner daily metrics")
    return gaps


def _detect_historical_data_gaps(snapshots: list[dict[str, Any]]) -> list[str]:
    if not snapshots:
        return ["no yfinance historical replay rows available"]
    if len(snapshots) < MIN_REPLAY_SAMPLES_FOR_STRONG_EVIDENCE:
        return [f"only {len(snapshots)} yfinance historical replay rows available"]
    return []


def _detect_consensus_regime_conflicts(
    regime: str,
    consensus_weights: dict[str, float],
) -> list[str]:
    exposure = _defensive_exposure(consensus_weights)
    top3 = [
        ticker for ticker, _ in sorted(
            ((ticker, weight) for ticker, weight in consensus_weights.items() if ticker != "CASH"),
            key=lambda item: item[1],
            reverse=True,
        )[:3]
    ]
    if regime == "trending_bull" and top3 and all(ticker in DEFENSIVE_ASSETS for ticker in top3):
        return [
            "live consensus conflicts with trending_bull regime: "
            f"top3={','.join(top3)}, defensive_weight={exposure['weight']:.1%}"
        ]
    return []


DEFENSIVE_ASSETS = {"BND", "IEF", "TLT", "SGOV", "GLD"}


def _detect_strategy_regime_conflicts(
    regime: str,
    weights: dict[str, float],
) -> list[str]:
    exposure = _defensive_exposure(weights)
    if regime == "trending_bull" and exposure["top_non_cash"] and exposure["weight"] >= 0.50:
        return [
            "strategy weights conflict with trending_bull regime: "
            f"defensive_weight={exposure['weight']:.1%}"
        ]
    return []


def _defensive_exposure(weights: dict[str, float]) -> dict[str, Any]:
    non_cash = {
        str(ticker).upper(): float(weight or 0.0)
        for ticker, weight in (weights or {}).items()
        if str(ticker).upper() != "CASH"
    }
    return {
        "weight": sum(weight for ticker, weight in non_cash.items() if ticker in DEFENSIVE_ASSETS),
        "top_non_cash": [
            ticker for ticker, _ in sorted(non_cash.items(), key=lambda item: item[1], reverse=True)[:3]
        ],
    }


def _format_report_for_telegram(text: str, bundle: PlaygroundBundle) -> str:
    top = _top_weights(bundle.consensus_weights, n=5)
    evidence_summary = _format_evidence_summary(bundle.evidence_summary)
    confidence_summary = _format_strategy_confidence_summary(bundle)
    validation_summary = _format_validation_summary(bundle.validation_summary)
    structured_sections = "\n\n".join(
        section for section in (evidence_summary, confidence_summary, validation_summary) if section
    )
    body = f"{structured_sections}\n\n{text}" if structured_sections else text
    return (
        "🧪 <b>Playground Sandbox</b>\n"
        f"Regime: {bundle.regime_label} ({bundle.regime_confidence}) | "
        f"QC snapshots={bundle.snapshot_count} | yfinance history={bundle.historical_snapshot_count}\n"
        f"Consensus top5: {top}\n\n"
        f"{body}"
    )


def _fallback_report(bundle: PlaygroundBundle, error: str | None = None) -> str:
    lines = [
        "🧪 <b>Playground Sandbox</b>",
        f"Regime: {bundle.regime_label} ({bundle.regime_confidence}) | "
        f"QC snapshots={bundle.snapshot_count} | yfinance history={bundle.historical_snapshot_count}",
        f"Consensus top5: {_top_weights(bundle.consensus_weights, n=5)}",
    ]
    evidence_summary = _format_evidence_summary(bundle.evidence_summary)
    if evidence_summary:
        lines.extend(["", evidence_summary])
    confidence_summary = _format_strategy_confidence_summary(bundle)
    if confidence_summary:
        lines.extend(["", confidence_summary])
    validation_summary = _format_validation_summary(bundle.validation_summary)
    if validation_summary:
        lines.extend(["", validation_summary])
    lines.extend(["", "<b>QC Live Replay</b>"])
    for name, metrics in bundle.replay_metrics.items():
        reliability = metrics.get("metric_reliability") or {}
        lines.append(
            f"- {name}: avg_turnover={metrics.get('avg_turnover')}, "
            f"avg_cash={metrics.get('avg_cash_weight')}, "
            f"reliability={reliability.get('level', 'unknown')}"
        )
    if bundle.historical_replay_metrics:
        lines.append("")
        lines.append("<b>YFinance Historical Replay</b>")
        for name, metrics in bundle.historical_replay_metrics.items():
            reliability = metrics.get("metric_reliability") or {}
            confidence = (bundle.strategy_confidence or {}).get(name) or {}
            lines.append(
                f"- {name}: sharpe={metrics.get('sharpe')}, "
                f"hit_rate={metrics.get('hit_rate')}, "
                f"avg_turnover={metrics.get('avg_turnover')}, "
                f"reliability={reliability.get('level', 'unknown')}, "
                f"use={confidence.get('suggested_use', 'unknown')}"
            )
    if bundle.divergence_map:
        lines.append("")
        lines.append("<b>Largest divergences</b>")
        for row in bundle.divergence_map[:5]:
            lines.append(f"- {row['ticker']}: spread={row['spread']:.1%}")
    if bundle.data_gaps:
        lines.append("")
        lines.append("<b>Data gaps</b>")
        lines.extend(f"- {gap}" for gap in bundle.data_gaps)
    if error:
        lines.append(f"\nLLM report fallback: {error[:160]}")
    return "\n".join(lines)


def _format_evidence_summary(summary: dict[str, Any] | None) -> str:
    if not summary:
        return ""
    best = summary.get("best_strategy") or {}
    best_text = ""
    if best:
        best_text = (
            f"\nBest: {escape(str(best.get('strategy_name') or 'unknown'))} "
            f"({escape(str(best.get('suggested_use') or 'unknown'))}, "
            f"confidence={_format_pct(best.get('confidence_score'))})"
        )
    reasons = ", ".join(str(item) for item in (summary.get("summary_reasons") or [])[:3])
    reason_text = f"\nWhy: {escape(reasons)}" if reasons else ""
    return (
        "<b>Evidence Summary</b>\n"
        "<b>Strategy Analysis (yfinance)</b>\n"
        f"Historical evidence: {escape(str(summary.get('historical_evidence') or 'unknown'))} "
        f"({int(summary.get('historical_samples') or 0)} samples, "
        f"{escape(str(summary.get('historical_reliability') or 'unknown'))})\n"
        f"<b>Execution Intel (QC Live)</b>\n"
        f"Status: {escape(str(summary.get('execution_intel_status') or 'unknown'))} "
        f"(QC snapshots={int(summary.get('qc_snapshot_count') or 0)}, "
        f"forward={int(summary.get('live_samples') or 0)})\n"
        f"Execution permission: {escape(str(summary.get('execution_permission') or 'unknown'))}"
        f"{best_text}{reason_text}"
    )


def _format_validation_summary(summary: dict[str, Any] | None) -> str:
    if not summary or summary.get("status") == "unavailable":
        return ""
    pending = summary.get("pending_outcomes") or {}
    combined = summary.get("combined_profiles") or []
    lines = [
        "<b>Signal Validation</b>",
        f"Signals today: {int(summary.get('signals_recorded_today') or 0)} | "
        f"Outcomes today: {int(summary.get('outcomes_labeled_today') or 0)} | "
        f"Pending mature: {int(pending.get('mature') or 0)}",
        f"Profiles: hist={len(summary.get('historical_prior_profiles') or [])}, "
        f"live={len(summary.get('live_paper_profiles') or [])}, "
        f"combined={len(combined)} | "
        f"live confirmation={int(summary.get('requires_live_confirmation_count') or 0)}",
    ]
    if combined:
        lines.append("Top combined:")
        for row in combined[:3]:
            conviction = row.get("conviction_display") or "-"
            status = escape(str(row.get("status") or "unknown"))
            lines.append(
                f"- {escape(str(row.get('strategy') or 'unknown'))}/"
                f"{escape(str(row.get('ticker') or 'unknown'))}: "
                f"{conviction}, n={int(row.get('n') or 0)}, {status}"
            )
    return "\n".join(lines)


def _format_strategy_confidence_summary(bundle: PlaygroundBundle, limit: int = 4) -> str:
    confidence = bundle.strategy_confidence or {}
    if not confidence:
        return ""
    by_name = {result.strategy_name: result for result in bundle.strategies}
    use_rank = {"primary": 0, "advisory": 1, "watch_only": 2, "ignore": 3}
    rows = sorted(
        confidence.values(),
        key=lambda row: (
            use_rank.get(str(row.get("suggested_use") or "watch_only"), 9),
            -float(row.get("confidence_score") or 0.0),
        ),
    )
    lines = ["<b>Strategy Confidence</b>"]
    for row in rows[:limit]:
        name = str(row.get("strategy_name") or "")
        result = by_name.get(name)
        metrics = (bundle.historical_replay_metrics or {}).get(name) or {}
        confidence_score = _format_pct(row.get("confidence_score"))
        historical = row.get("historical_reliability") or "unknown"
        live_samples = int(row.get("live_samples") or 0)
        hist_samples = int(row.get("historical_samples") or 0)
        current_turnover = _format_pct(result.expected_turnover_pct if result else None)
        input_scope = ""
        if result and result.score_status == "partially_scored":
            input_scope = f", scorable={result.scorable_ticker_count}, excluded={len(result.excluded_tickers)}"
        elif result and result.score_status == "not_scored":
            input_scope = f", not_scored={escape(str(result.not_scored_reason or 'unknown'))}"
        hist_turnover = _format_pct(metrics.get("avg_turnover"))
        sharpe = metrics.get("sharpe")
        sharpe_text = f"{float(sharpe):.2f}" if sharpe is not None else "n/a"
        codes = ", ".join(_prioritize_reason_codes(row.get("reason_codes") or row.get("notes") or [])[:5])
        lines.append(
            f"- {escape(name)}: use={escape(str(row.get('suggested_use') or 'unknown'))}, "
            f"confidence={confidence_score}, hist={escape(str(historical))}/{hist_samples}, "
            f"live_samples={live_samples}, sharpe={sharpe_text}, "
            f"current_turnover={current_turnover}, hist_avg_turnover={hist_turnover}{input_scope}"
        )
        if codes:
            lines.append(f"  reasons={escape(codes)}")
    return "\n".join(lines)


def _prioritize_reason_codes(codes: list[Any]) -> list[str]:
    priority = {
        "consensus_regime_conflict": 0,
        "strategy_regime_conflict": 0,
        "high_turnover": 1,
        "moderate_turnover": 2,
        "data_not_ready": 3,
        "live_qc_missing": 4,
        "live_qc_limited": 5,
        "regime_fit_weak": 6,
        "historical_missing": 7,
        "historical_insufficient": 8,
        "historical_strong": 20,
        "historical_positive_sharpe": 21,
        "regime_fit_strong": 22,
        "regime_fit_medium": 23,
        "low_turnover": 24,
    }
    cleaned = [str(code) for code in codes if str(code)]
    return sorted(dict.fromkeys(cleaned), key=lambda code: (priority.get(code, 10), code))


def _format_pct(value: Any) -> str:
    if value is None:
        return "n/a"
    try:
        return f"{float(value):.1%}"
    except (TypeError, ValueError):
        return "n/a"


def _score_to_dict(item: ScoredTicker) -> dict[str, Any]:
    return {
        "ticker": item.ticker,
        "score": round(float(item.score), 6),
        "factor_breakdown": item.factor_breakdown,
        "raw_factors": item.raw_factors,
    }


def _strategy_regime_fit(name: str, regime: str) -> str:
    if name == "momentum_lite_v1":
        return "strong" if regime in ("trending_bull", "trending_bear") else "medium"
    if name == "mean_reversion_lite":
        return "strong" if regime in ("mean_reverting", "high_vol") else "medium"
    if name == "low_vol_factor":
        return "strong" if regime in ("defensive", "high_vol", "trending_bear") else "medium"
    if name == "dual_momentum_rotation":
        return "strong" if regime in ("trending_bull", "trending_bear") else "medium"
    if name == "risk_parity_lite":
        return "strong" if regime in ("high_vol", "defensive") else "medium"
    if name == "equal_weight_benchmark":
        return "benchmark"
    if name == "leveraged_etf_momentum_allocator":
        return "medium" if regime in ("trending_bull", "trending_bear", "high_vol") else "unknown"
    return "unknown"


def _direction_bias_for_regime(regime: str) -> str:
    if regime == "trending_bull":
        return "bullish"
    if regime in ("trending_bear", "defensive"):
        return "bearish"
    return "neutral"


def _stance_for_regime(regime: str) -> str:
    if regime == "trending_bull":
        return "increase"
    if regime in ("trending_bear", "defensive", "high_vol"):
        return "defensive"
    return "maintain"


def _confidence_to_float(confidence: str) -> float:
    return {"high": 0.8, "medium": 0.6, "low": 0.4}.get(confidence, 0.5)


def _turnover(left: dict[str, float], right: dict[str, float]) -> float:
    tickers = set(left) | set(right)
    return sum(abs(float(left.get(t, 0.0)) - float(right.get(t, 0.0))) for t in tickers) / 2.0


def _avg(values: list[float | int]) -> float:
    clean = [float(v) for v in values if v is not None and not math.isnan(float(v))]
    return sum(clean) / len(clean) if clean else 0.0


def _max_drawdown(returns: list[float]) -> float | None:
    clean = [float(v) for v in returns if v is not None and not math.isnan(float(v))]
    if not clean:
        return None
    equity = 1.0
    peak = 1.0
    max_dd = 0.0
    for ret in clean:
        equity *= 1.0 + ret
        peak = max(peak, equity)
        if peak > 0:
            max_dd = max(max_dd, (peak - equity) / peak)
    return round(max_dd, 6)


def _annualized_sharpe(returns: list[float]) -> float | None:
    clean = [float(v) for v in returns if v is not None and not math.isnan(float(v))]
    if len(clean) < 2:
        return None
    mean = sum(clean) / len(clean)
    variance = sum((value - mean) ** 2 for value in clean) / (len(clean) - 1)
    std = math.sqrt(variance)
    if std <= 1e-12:
        return None
    return round((mean / std) * math.sqrt(252), 4)


def _correlation(left: list[float], right: list[float]) -> float | None:
    if len(left) != len(right) or len(left) < 2:
        return None
    mean_left = sum(left) / len(left)
    mean_right = sum(right) / len(right)
    num = sum((l - mean_left) * (r - mean_right) for l, r in zip(left, right))
    den_left = math.sqrt(sum((l - mean_left) ** 2 for l in left))
    den_right = math.sqrt(sum((r - mean_right) ** 2 for r in right))
    denom = den_left * den_right
    if denom <= 1e-12:
        return None
    return num / denom


def _top_counts(values: list[str], limit: int) -> list[dict[str, Any]]:
    counts: dict[str, int] = {}
    for value in values:
        counts[value] = counts.get(value, 0) + 1
    return [
        {"ticker": ticker, "count": count}
        for ticker, count in sorted(counts.items(), key=lambda item: item[1], reverse=True)[:limit]
    ]


def _top_weights(weights: dict[str, float], n: int) -> str:
    rows = sorted(
        [(ticker, weight) for ticker, weight in weights.items() if ticker != "CASH"],
        key=lambda item: item[1],
        reverse=True,
    )[:n]
    return ", ".join(f"{ticker} {weight:.1%}" for ticker, weight in rows) or "N/A"
