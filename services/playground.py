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
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from typing import Any

from openai import AsyncOpenAI
from sqlalchemy import desc, select

from config import get_settings
from db.models import QCSnapshot
from db.session import AsyncSessionLocal
from services.quant_baseline import classify_market_regime
from services.sector_rotation import detect_sector_rotation
from services.strategy_feature_contract import build_strategy_feature_contract
from services.universe_policy import filter_tradable_research_rows
from services.feature_provenance import summarize_feature_provenance
from strategies import ScoredTicker, compute_rebalance_actions, estimate_cost_pct, get_strategy

logger = logging.getLogger("qc_fastapi_2.playground")
settings = get_settings()

DEFAULT_PLAYGROUND_STRATEGIES = [
    "momentum_lite_v1",
    "dual_momentum_rotation",
    "mean_reversion_lite",
    "low_vol_factor",
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
    weights: dict[str, float]
    score_breakdown: list[dict[str, Any]]
    selected_tickers: list[str]
    expected_turnover_pct: float
    estimated_cost_pct: float
    regime_fit: str
    data_ready: bool
    data_readiness: dict[str, Any]
    feature_contract: dict[str, Any]
    data_quality: dict[str, Any]
    risk_profile: dict[str, Any]
    memory_feedback: dict[str, Any]
    agent_interpretation: dict[str, Any]


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
    data_gaps: list[str]

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["strategies"] = [asdict(item) for item in self.strategies]
        return data


async def run_playground(
    brief: dict[str, Any],
    strategy_names: list[str] | None = None,
) -> PlaygroundBundle:
    holdings = brief.get("holdings") or []
    holdings, enrichment = await _ensure_playground_features(holdings, strategy_names)
    portfolio = brief.get("portfolio") or {}
    current_weights = brief.get("current_weights") or _extract_current_weights(holdings)
    sector_rotation = brief.get("sector_rotation") or detect_sector_rotation(holdings)
    spy_holding = next((h for h in holdings if (h.get("ticker") or "").upper() == "SPY"), {})
    regime = classify_market_regime(portfolio, spy_holding)
    context = {
        "regime": regime.regime.value,
        "confidence": _confidence_to_float(regime.confidence),
        "uncertainty_flag": regime.confidence == "low",
        "stance": _stance_for_regime(regime.regime.value),
        "direction_bias": _direction_bias_for_regime(regime.regime.value),
        "risk_params": brief.get("risk_params") or {},
        "current_weights": current_weights,
        "sector_rotation": sector_rotation,
    }

    names = strategy_names or DEFAULT_PLAYGROUND_STRATEGIES
    memory_feedback = await _load_strategy_memory_feedback(regime.regime.value, names)
    results = [
        _run_one_strategy(
            name,
            holdings,
            context,
            current_weights,
            memory_feedback=memory_feedback.get(name),
        )
        for name in names
    ]

    return PlaygroundBundle(
        generated_at=datetime.utcnow().isoformat(),
        regime_label=regime.regime.value,
        regime_confidence=regime.confidence,
        snapshot_count=1,
        strategies=results,
        divergence_map=compute_weight_divergence(results),
        consensus_weights=compute_consensus_weights(results),
        replay_metrics={},
        data_gaps=enrichment.get("data_gaps", []),
    )


async def run_playground_analysis(
    days: int = 30,
    strategy_names: list[str] | None = None,
) -> PlaygroundBundle:
    snapshots = await _read_recent_snapshots(days=days)
    if not snapshots:
        return PlaygroundBundle(
            generated_at=datetime.utcnow().isoformat(),
            regime_label="unknown",
            regime_confidence="low",
            snapshot_count=0,
            strategies=[],
            divergence_map=[],
            consensus_weights={"CASH": 1.0},
            replay_metrics={},
            data_gaps=["no QC snapshots available"],
        )

    latest_brief = _brief_from_snapshot(snapshots[-1])
    bundle = await run_playground(latest_brief, strategy_names=strategy_names)
    bundle.snapshot_count = len(snapshots)
    bundle.replay_metrics = _compute_replay_metrics(snapshots, strategy_names or DEFAULT_PLAYGROUND_STRATEGIES)
    bundle.data_gaps = list(dict.fromkeys((bundle.data_gaps or []) + _detect_data_gaps(snapshots)))
    return bundle


async def _ensure_playground_features(
    holdings: list[dict[str, Any]],
    strategy_names: list[str] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """
    Ensure strategy-required fields exist before comparison.

    1. Enrich missing fields from market_daily_features, preferring yfinance.
    2. If still missing, fetch yfinance immediately and persist it.
    3. Return enriched holdings plus transparent data-gap/source notes.
    """
    clean_holdings = filter_tradable_research_rows(holdings)
    tickers = sorted({(row.get("ticker") or "").upper().strip() for row in clean_holdings if row.get("ticker")})
    if not tickers:
        return clean_holdings, {"data_gaps": ["no tradable research tickers after universe filtering"]}

    required_fields = _required_fields_for_strategies(strategy_names or DEFAULT_PLAYGROUND_STRATEGIES)
    missing_before = _missing_required_by_ticker(clean_holdings, required_fields)
    if not any(missing_before.values()):
        return clean_holdings, {"data_gaps": []}

    data_gaps: list[str] = []
    enriched = clean_holdings

    try:
        from services.market_feature_store import latest_feature_map
        async with AsyncSessionLocal() as db:
            feature_map = await latest_feature_map(db, tickers=tickers, source="yfinance", max_age_days=14)
        enriched = _merge_feature_map(enriched, feature_map)
    except Exception as exc:
        logger.warning("[playground] yfinance feature-store enrichment failed: %s", exc)
        data_gaps.append(f"yfinance feature-store enrichment failed: {type(exc).__name__}")

    missing_after_store = _missing_required_by_ticker(enriched, required_fields)
    tickers_to_fetch = [ticker for ticker, fields in missing_after_store.items() if fields]
    if tickers_to_fetch:
        try:
            from services.market_feature_store import upsert_market_daily_features
            from services.yfinance_backfill import fetch_yfinance_feature_rows
            fetched_rows = fetch_yfinance_feature_rows(tickers_to_fetch, lookback_days=420)
            latest_rows = _latest_rows_by_ticker(fetched_rows)
            if latest_rows:
                async with AsyncSessionLocal() as db:
                    await upsert_market_daily_features(db, fetched_rows, source="yfinance")
                enriched = _merge_feature_map(enriched, latest_rows)
        except Exception as exc:
            logger.warning("[playground] immediate yfinance enrichment failed: %s", exc)
            data_gaps.append(f"immediate yfinance enrichment failed: {type(exc).__name__}")

    missing_after = _missing_required_by_ticker(enriched, required_fields)
    still_missing = {ticker: fields for ticker, fields in missing_after.items() if fields}
    if still_missing:
        data_gaps.append(f"strategy-required fields still missing after yfinance enrichment: {still_missing}")
    else:
        data_gaps.append("missing strategy fields filled from yfinance research feature layer")

    return enriched, {"data_gaps": data_gaps}


async def generate_playground_report(bundle: PlaygroundBundle) -> str:
    data = bundle.to_dict()
    if not bundle.strategies:
        return "🧪 <b>Playground Sandbox</b>\nNo QC snapshots available, skipped."

    prompt = {
        "task": (
            "Analyze this strategy comparison bundle for a research-only trading sandbox. "
            "No execution is allowed. Use the embedded English strategy_card and "
            "agent_interpretation fields to understand each strategy's meaning, regime fit, "
            "failure modes, and how downstream agents should use it."
        ),
        "output_language": "English",
        "required_sections": [
            "best_strategy_or_blend",
            "key_divergences",
            "turnover_and_cost_risk",
            "data_gaps",
            "next_research_actions",
        ],
        "review_rules": [
            "Do not choose a strategy solely because it has the highest replay Sharpe.",
            "Treat any replay metric with metric_reliability.level != high as weak evidence.",
            "If n_forward_return_samples is below the stated minimum, explicitly say performance metrics are not reliable yet.",
            "Check regime compatibility, data quality, yfinance-filled fields, turnover, and macro/news consistency.",
            "Discount strategies with weak memory_feedback in the same regime; this is advisory and cannot bypass Risk Manager.",
            "Explicitly mention if a strategy should be discounted due to its failure modes.",
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


def _run_one_strategy(
    name: str,
    holdings: list[dict],
    context: dict[str, Any],
    current_weights: dict[str, float],
    memory_feedback: dict[str, Any] | None = None,
) -> StrategyResult:
    strategy = get_strategy(name)
    readiness = strategy.data_readiness(holdings)
    feature_contract = build_strategy_feature_contract(strategy, holdings)
    if readiness.get("ready") and feature_contract.get("can_influence_allocation"):
        scored = strategy.score(holdings, context)
        weights = strategy.optimize(scored, context)
    else:
        scored = []
        weights = {"CASH": 1.0}
    actions = compute_rebalance_actions(weights, current_weights, threshold=1e-9)
    turnover = sum(abs(float(a["weight_delta"])) for a in actions) / 2.0
    return StrategyResult(
        strategy_name=name,
        strategy_version=strategy.version,
        description=strategy.description,
        strategy_card=strategy.strategy_card(),
        weights=weights,
        score_breakdown=[_score_to_dict(item) for item in scored],
        selected_tickers=[ticker for ticker, weight in weights.items() if ticker != "CASH" and weight > 0.01],
        expected_turnover_pct=round(turnover, 6),
        estimated_cost_pct=estimate_cost_pct(actions),
        regime_fit=_strategy_regime_fit(name, context.get("regime", "")),
        data_ready=bool(readiness.get("ready")),
        data_readiness={
            **readiness,
            "requirements": strategy.data_requirements(),
        },
        feature_contract=feature_contract,
        data_quality=_build_data_quality(readiness, holdings, strategy.required_fields),
        risk_profile=_build_risk_profile(weights, turnover, estimate_cost_pct(actions)),
        memory_feedback=memory_feedback or _neutral_memory_feedback(name, context.get("regime", "")),
        agent_interpretation=_build_agent_interpretation(
            strategy, scored, weights, context, readiness, feature_contract, memory_feedback
        ),
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


def _missing_required_by_ticker(
    holdings: list[dict[str, Any]],
    required_fields: set[str],
) -> dict[str, list[str]]:
    if not required_fields:
        return {}
    out: dict[str, list[str]] = {}
    for row in holdings:
        ticker = (row.get("ticker") or "").upper().strip()
        if not ticker:
            continue
        missing = [field for field in sorted(required_fields) if row.get(field) is None]
        if missing:
            out[ticker] = missing
    return out


def _merge_feature_map(
    holdings: list[dict[str, Any]],
    feature_map: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    if not feature_map:
        return holdings
    enriched: list[dict[str, Any]] = []
    for row in holdings:
        ticker = (row.get("ticker") or "").upper().strip()
        feature = feature_map.get(ticker) or {}
        mapped = _feature_row_to_holding_fields(feature)
        merged = dict(row)
        filled_fields: list[str] = []
        for key, value in mapped.items():
            if value is not None and merged.get(key) is None:
                merged[key] = value
                filled_fields.append(key)
        if filled_fields:
            sources = list(merged.get("feature_sources") or [])
            sources.append({
                "source": feature.get("source", "yfinance"),
                "filled_fields": sorted(filled_fields),
                "trading_date": feature.get("trading_date"),
            })
            merged["feature_sources"] = sources
        enriched.append(merged)
    return enriched


def _feature_row_to_holding_fields(feature: dict[str, Any]) -> dict[str, Any]:
    return {
        "price": feature.get("close_price") or feature.get("adj_close_price"),
        "close_price": feature.get("close_price") or feature.get("adj_close_price"),
        "open_price": feature.get("open_price"),
        "high_price": feature.get("high_price"),
        "low_price": feature.get("low_price"),
        "volume": feature.get("volume"),
        "dollar_volume": feature.get("dollar_volume"),
        "daily_return_pct": feature.get("return_1d"),
        "return_1d": feature.get("return_1d"),
        "return_5d": feature.get("return_5d"),
        "mom_20d": feature.get("return_20d"),
        "mom_60d": feature.get("return_60d"),
        "mom_252d": feature.get("return_252d"),
        "sma_20": feature.get("sma_20"),
        "sma_50": feature.get("sma_50"),
        "sma_200": feature.get("sma_200"),
        "hist_vol_20d": feature.get("hist_vol_20d"),
    }


def _latest_rows_by_ticker(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    for row in rows:
        ticker = (row.get("ticker") or "").upper().strip()
        if not ticker:
            continue
        current = latest.get(ticker)
        if current is None or str(row.get("trading_date")) > str(current.get("trading_date")):
            latest[ticker] = row
    return latest


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
        "field_coverage": readiness.get("field_coverage") or {},
        "filled_by_source": {
            source: sorted(fields)
            for source, fields in sorted(filled_by_source.items())
            if fields
        },
        "provenance_summary": summarize_feature_provenance(holdings),
    }


def _build_risk_profile(
    weights: dict[str, float],
    turnover: float,
    estimated_cost: float,
) -> dict[str, Any]:
    non_cash = {ticker: float(weight) for ticker, weight in weights.items() if ticker != "CASH" and weight > 0}
    max_single = max(non_cash.values()) if non_cash else 0.0
    concentration = "low"
    if max_single >= 0.20 or len(non_cash) <= 3:
        concentration = "high"
    elif max_single >= 0.12 or len(non_cash) <= 6:
        concentration = "medium"
    return {
        "turnover": round(turnover, 6),
        "estimated_cost": estimated_cost,
        "position_count": len(non_cash),
        "max_single_weight": round(max_single, 6),
        "cash_weight": round(float(weights.get("CASH", 0.0)), 6),
        "concentration": concentration,
    }


def _build_agent_interpretation(
    strategy,
    scored: list[ScoredTicker],
    weights: dict[str, float],
    context: dict[str, Any],
    readiness: dict[str, Any],
    feature_contract: dict[str, Any] | None = None,
    memory_feedback: dict[str, Any] | None = None,
) -> dict[str, Any]:
    selected = [ticker for ticker, weight in weights.items() if ticker != "CASH" and weight > 0.01]
    top_scores = [item.ticker for item in scored[:3]]
    contract = feature_contract or {}
    if not readiness.get("ready") or not contract.get("can_influence_allocation", True):
        verdict = contract.get("verdict") or "not_data_ready"
        what = f"The strategy is not data-ready ({verdict}) and should not influence allocation."
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
    if family == "risk_budgeting":
        return "Invalidated as an alpha view if expected-return evidence strongly favors a specific leadership theme."
    return f"Validate against current regime={regime or 'unknown'}, rotation, macro/news risk, and execution constraints."


def compute_weight_divergence(results: list[StrategyResult], top_n: int = 10) -> list[dict[str, Any]]:
    ready_results = [result for result in results if result.data_ready]
    tickers = sorted({ticker for result in ready_results for ticker in result.weights if ticker != "CASH"})
    rows: list[dict[str, Any]] = []
    for ticker in tickers:
        weights = {
            result.strategy_name: float(result.weights.get(ticker, 0.0))
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
    tickers = sorted({ticker for result in ready_results for ticker in result.weights})
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
            float(result.weights.get(ticker, 0.0)) * strategy_multipliers[result.strategy_name]
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


async def _read_recent_snapshots(days: int) -> list[dict[str, Any]]:
    cutoff = datetime.utcnow() - timedelta(days=days)
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(QCSnapshot)
            .where(QCSnapshot.received_at >= cutoff)
            .where(QCSnapshot.packet_type.in_(("daily_feature_snapshot", "heartbeat")))
            .order_by(desc(QCSnapshot.received_at))
            .limit(180)
        )
        rows = result.scalars().all()
    snapshots = _dedupe_market_snapshots(rows)
    snapshots.reverse()
    return snapshots


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
            brief = _brief_from_snapshot(snapshot)
            holdings = brief.get("holdings") or []
            if not holdings:
                continue
            portfolio = brief.get("portfolio") or {}
            spy = next((h for h in holdings if (h.get("ticker") or "").upper() == "SPY"), {})
            regime = classify_market_regime(portfolio, spy)
            context = {
                "regime": regime.regime.value,
                "confidence": _confidence_to_float(regime.confidence),
                "uncertainty_flag": regime.confidence == "low",
                "stance": _stance_for_regime(regime.regime.value),
                "direction_bias": _direction_bias_for_regime(regime.regime.value),
                "risk_params": {},
                "current_weights": brief.get("current_weights") or {},
                "sector_rotation": brief.get("sector_rotation") or {},
            }
            result = _run_one_strategy(name, holdings, context, previous_weights.get(name, {}))
            weights = result.weights
            prev = previous_weights.get(name)
            if prev is not None:
                turnovers.append(_turnover(weights, prev))
            previous_weights[name] = weights
            position_counts.append(sum(1 for ticker, weight in weights.items() if ticker != "CASH" and weight > 0.01))
            cash_weights.append(float(weights.get("CASH", 0.0)))
            if result.score_breakdown:
                score_leaders.append(result.score_breakdown[0]["ticker"])

            if idx + 1 < len(snapshots):
                next_returns = _extract_daily_returns(_snapshot_rows(snapshots[idx + 1]))
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


def _brief_from_snapshot(snapshot: dict[str, Any]) -> dict[str, Any]:
    holdings = _snapshot_rows(snapshot)
    return {
        "holdings": holdings,
        "portfolio": snapshot.get("portfolio") or {},
        "current_weights": _extract_current_weights(holdings),
        "risk_params": {},
        "sector_rotation": detect_sector_rotation(holdings),
    }


def _snapshot_rows(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    return filter_tradable_research_rows(snapshot.get("holdings") or snapshot.get("features") or [])


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


def _format_report_for_telegram(text: str, bundle: PlaygroundBundle) -> str:
    top = _top_weights(bundle.consensus_weights, n=5)
    return (
        "🧪 <b>Playground Sandbox</b>\n"
        f"Regime: {bundle.regime_label} ({bundle.regime_confidence}) | snapshots={bundle.snapshot_count}\n"
        f"Consensus top5: {top}\n\n"
        f"{text}"
    )


def _fallback_report(bundle: PlaygroundBundle, error: str | None = None) -> str:
    lines = [
        "🧪 <b>Playground Sandbox</b>",
        f"Regime: {bundle.regime_label} ({bundle.regime_confidence}) | snapshots={bundle.snapshot_count}",
        f"Consensus top5: {_top_weights(bundle.consensus_weights, n=5)}",
        "",
        "<b>Strategy turnover</b>",
    ]
    for name, metrics in bundle.replay_metrics.items():
        reliability = metrics.get("metric_reliability") or {}
        lines.append(
            f"- {name}: avg_turnover={metrics.get('avg_turnover')}, "
            f"avg_cash={metrics.get('avg_cash_weight')}, "
            f"reliability={reliability.get('level', 'unknown')}"
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
