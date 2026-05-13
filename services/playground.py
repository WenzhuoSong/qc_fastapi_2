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
from strategies import ScoredTicker, compute_rebalance_actions, estimate_cost_pct, get_strategy

logger = logging.getLogger("qc_fastapi_2.playground")
settings = get_settings()

DEFAULT_PLAYGROUND_STRATEGIES = [
    "momentum_lite_v1",
    "mean_reversion_lite",
    "low_vol_factor",
]


@dataclass
class StrategyResult:
    strategy_name: str
    strategy_version: str
    description: str
    weights: dict[str, float]
    score_breakdown: list[dict[str, Any]]
    selected_tickers: list[str]
    expected_turnover_pct: float
    estimated_cost_pct: float
    regime_fit: str


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
    portfolio = brief.get("portfolio") or {}
    current_weights = brief.get("current_weights") or _extract_current_weights(holdings)
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
    }

    names = strategy_names or DEFAULT_PLAYGROUND_STRATEGIES
    results = [
        _run_one_strategy(name, holdings, context, current_weights)
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
        data_gaps=[],
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
    bundle.data_gaps = _detect_data_gaps(snapshots)
    return bundle


async def generate_playground_report(bundle: PlaygroundBundle) -> str:
    data = bundle.to_dict()
    if not bundle.strategies:
        return "🧪 <b>Playground Sandbox</b>\nNo QC snapshots available, skipped."

    prompt = {
        "task": "Analyze this strategy comparison bundle for a research-only trading sandbox. No execution is allowed.",
        "output_language": "Chinese",
        "required_sections": [
            "best_strategy_or_blend",
            "key_divergences",
            "turnover_and_cost_risk",
            "data_gaps",
            "next_research_actions",
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
) -> StrategyResult:
    strategy = get_strategy(name)
    scored = strategy.score(holdings, context)
    weights = strategy.optimize(scored, context)
    actions = compute_rebalance_actions(weights, current_weights, threshold=1e-9)
    turnover = sum(abs(float(a["weight_delta"])) for a in actions) / 2.0
    return StrategyResult(
        strategy_name=name,
        strategy_version=strategy.version,
        description=strategy.description,
        weights=weights,
        score_breakdown=[_score_to_dict(item) for item in scored[:10]],
        selected_tickers=[ticker for ticker, weight in weights.items() if ticker != "CASH" and weight > 0.01],
        expected_turnover_pct=round(turnover, 6),
        estimated_cost_pct=estimate_cost_pct(actions),
        regime_fit=_strategy_regime_fit(name, context.get("regime", "")),
    )


def compute_weight_divergence(results: list[StrategyResult], top_n: int = 10) -> list[dict[str, Any]]:
    tickers = sorted({ticker for result in results for ticker in result.weights if ticker != "CASH"})
    rows: list[dict[str, Any]] = []
    for ticker in tickers:
        weights = {
            result.strategy_name: float(result.weights.get(ticker, 0.0))
            for result in results
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
    if not results:
        return {"CASH": 1.0}
    tickers = sorted({ticker for result in results for ticker in result.weights})
    averaged = {
        ticker: sum(float(result.weights.get(ticker, 0.0)) for result in results) / len(results)
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
            .where(QCSnapshot.packet_type == "heartbeat")
            .order_by(desc(QCSnapshot.received_at))
            .limit(90)
        )
        rows = result.scalars().all()
    snapshots = [row.raw_payload or {} for row in rows]
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

        for snapshot in snapshots:
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

        metrics[name] = {
            "avg_turnover": round(_avg(turnovers), 6) if turnovers else None,
            "max_turnover": round(max(turnovers), 6) if turnovers else None,
            "avg_position_count": round(_avg(position_counts), 2) if position_counts else None,
            "avg_cash_weight": round(_avg(cash_weights), 4) if cash_weights else None,
            "top_signal_leaders": _top_counts(score_leaders, limit=5),
            "sharpe": None,
            "ic": None,
            "metric_notes": "Sharpe/IC require per-ticker forward returns; current QC snapshots expose portfolio PnL only.",
        }
    return metrics


def _brief_from_snapshot(snapshot: dict[str, Any]) -> dict[str, Any]:
    holdings = snapshot.get("holdings") or []
    return {
        "holdings": holdings,
        "portfolio": snapshot.get("portfolio") or {},
        "current_weights": _extract_current_weights(holdings),
        "risk_params": {},
    }


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


def _detect_data_gaps(snapshots: list[dict[str, Any]]) -> list[str]:
    gaps: list[str] = []
    if len(snapshots) < 10:
        gaps.append(f"only {len(snapshots)} heartbeat snapshots in lookback window")
    sample_holdings = [h for s in snapshots for h in (s.get("holdings") or [])]
    if sample_holdings and not any("daily_return_pct" in h or "return_1d" in h for h in sample_holdings):
        gaps.append("per-ticker forward returns missing; Sharpe/IC not computed")
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
        lines.append(
            f"- {name}: avg_turnover={metrics.get('avg_turnover')}, "
            f"avg_cash={metrics.get('avg_cash_weight')}"
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
