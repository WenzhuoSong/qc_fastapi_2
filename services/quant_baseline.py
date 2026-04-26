# services/quant_baseline.py
"""
Stage 2: QUANT BASELINE — layered Python signal decomposition.

Responsibilities (no LLM):
    1. Take holdings from brief
    2. Classify market regime (hard-math, no LLM)
    3. compute_layered_signals() -> LayeredSignal[] (short/medium/long Z-score layers)
    4. scores_to_weights() -> base_weights (score-weighted + vol-adjusted + cap + cash floor)
    5. Pack baseline for downstream RESEARCHER

Output contract (downstream consumer):
    {
        "base_weights":        dict[ticker, float]   # sum ~= 1.0 (incl. CASH)
        "scoring_breakdown":   list[dict]             # [{ticker, score, factors}, ...]
        "ranking_summary":     {"top_5": [...], "bottom_3": [...]}
        "selected_tickers":    list[str]              # non-CASH tickers with w > 0
        "regime_result":       dict                   # from classify_market_regime
        "metadata":            {strategy_used, strategy_version, params_used}
    }
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Any

logger = logging.getLogger("qc_fastapi_2.quant_baseline")


# Regime -> time-layer blend weights mapping
REGIME_BLEND_WEIGHTS = {
    "trending_bull": {
        "short":  0.20,
        "medium": 0.55,
        "long":   0.25,
    },
    "trending_bear": {
        "short":  0.10,
        "medium": 0.35,
        "long":   0.55,
    },
    "high_vol": {
        "short":  0.10,
        "medium": 0.30,
        "long":   0.60,
    },
    "mean_reverting": {
        "short":  0.40,
        "medium": 0.40,
        "long":   0.20,
    },
    "defensive": {
        "short":  0.05,
        "medium": 0.25,
        "long":   0.70,
    },
}

DEFAULT_BLEND_WEIGHTS = {"short": 0.25, "medium": 0.45, "long": 0.30}


@dataclass
class LayeredSignal:
    """Three-layer signal decomposition result."""
    ticker: str

    # Short-term signal (1-5d perspective)
    signal_short: float
    signal_short_components: dict

    # Medium-term signal (20-60d perspective)
    signal_medium: float
    signal_medium_components: dict

    # Long-term signal (60-252d perspective)
    signal_long: float
    signal_long_components: dict

    # Final composite score (blended by regime)
    composite_score: float
    blend_weights_used: dict


# ─────────────────────────────── Layered Signal Helpers ───────────────────────────────


def _safe_zscore(values: list[float]) -> list[float]:
    """Cross-sectional Z-score, handling constant columns and NaN."""
    arr = [v if v is not None else 0.0 for v in values]
    mean = sum(arr) / len(arr) if arr else 0
    std = (sum((x - mean) ** 2 for x in arr) / len(arr)) ** 0.5
    if std < 1e-8:
        return [0.0] * len(arr)
    return [(x - mean) / std for x in arr]


def _clip_signal(values: list[float], clip: float = 2.5) -> list[float]:
    """Winsorize: clip extreme values to prevent single ticker domination."""
    return [max(-clip, min(clip, v)) for v in values]


def compute_layered_signals(
    holdings: list[dict],
    regime: str,
) -> list[LayeredSignal]:
    """
    Compute three-layer (short/medium/long) separated signals per ticker,
    blended by regime.

    Short-term factors:   mom_20d, RSI (direction varies by regime), BB position (mean-reversion)
    Medium-term factors: mom_60d, hist_vol (low-vol premium), RSI confirmation
    Long-term factors:   mom_252d, ATR (low-ATR stability)
    """
    blend = REGIME_BLEND_WEIGHTS.get(regime, DEFAULT_BLEND_WEIGHTS)

    # Filter to non-CASH tickers
    valid = [h for h in holdings if h.get("ticker") and h.get("ticker") != "CASH"]
    tickers = [h["ticker"] for h in valid]

    # Extract raw series
    mom_20d  = [float(h.get("mom_20d", 0) or 0) for h in valid]
    mom_60d  = [float(h.get("mom_60d", 0) or 0) for h in valid]
    mom_252d = [float(h.get("mom_252d", 0) or 0) for h in valid]
    rsi      = [float(h.get("rsi_14", 50) or 50) for h in valid]
    bb_pos   = [float(h.get("bb_position", 0.5) or 0.5) for h in valid]
    hist_vol = [float(h.get("hist_vol_20d", 0.15) or 0.15) for h in valid]
    atr_pct  = [float(h.get("atr_pct", 0.01) or 0.01) for h in valid]

    # Cross-sectional Z-scores
    z_mom20  = _clip_signal(_safe_zscore(mom_20d))
    z_mom60  = _clip_signal(_safe_zscore(mom_60d))
    z_mom252 = _clip_signal(_safe_zscore(mom_252d))
    z_rsi    = _clip_signal(_safe_zscore(rsi))
    z_bb     = _clip_signal(_safe_zscore(bb_pos))
    z_vol    = _clip_signal(_safe_zscore([-v for v in hist_vol]))  # low vol = good
    z_atr    = _clip_signal(_safe_zscore([-v for v in atr_pct]))   # low ATR = good

    results = []
    for i, ticker in enumerate(tickers):

        # Short-term signal
        if regime in ("trending_bull", "trending_bear"):
            # Trending: RSI follows trend (high RSI = strength)
            short_raw = (
                0.60 * z_mom20[i]
                + 0.25 * z_rsi[i]
                + 0.15 * (-z_bb[i])
            )
        else:
            # Volatile/defensive: RSI mean-reverts (high RSI = overbought = bearish)
            short_raw = (
                0.50 * z_mom20[i]
                + 0.30 * (-z_rsi[i])
                + 0.20 * (-z_bb[i])
            )

        # Medium-term signal
        medium_raw = (
            0.65 * z_mom60[i]
            + 0.25 * z_vol[i]
            + 0.10 * z_rsi[i]
        )

        # Long-term signal
        long_raw = (
            0.80 * z_mom252[i]
            + 0.20 * z_atr[i]
        )

        # Regime blend
        composite = (
            blend["short"]  * short_raw
            + blend["medium"] * medium_raw
            + blend["long"]   * long_raw
        )

        results.append(LayeredSignal(
            ticker=ticker,
            signal_short=round(short_raw, 4),
            signal_short_components={
                "mom_20d_z": round(z_mom20[i], 3),
                "rsi_z":    round(z_rsi[i],  3),
                "bb_z":     round(z_bb[i],   3),
            },
            signal_medium=round(medium_raw, 4),
            signal_medium_components={
                "mom_60d_z":     round(z_mom60[i], 3),
                "hist_vol_z":    round(z_vol[i],  3),
                "rsi_confirm_z": round(z_rsi[i],  3),
            },
            signal_long=round(long_raw, 4),
            signal_long_components={
                "mom_252d_z": round(z_mom252[i], 3),
                "atr_z":     round(z_atr[i],    3),
            },
            composite_score=round(composite, 4),
            blend_weights_used=blend,
        ))

    return results


def scores_to_weights(
    signals: list[LayeredSignal],
    min_cash: float = 0.05,
    max_single: float = 0.20,
    vol_adjustment: bool = True,
    hist_vol_map: dict[str, float] | None = None,
) -> dict[str, float]:
    """
    Convert composite_score to portfolio weights.
    vol_adjustment=True: 70% score weight + 30% inverse-vol weight.
    """
    if not signals:
        return {"CASH": 1.0}

    positive = [s for s in signals if s.composite_score > 0]
    if not positive:
        return {"CASH": 1.0}

    # Score-weighted allocation
    total_score = sum(s.composite_score for s in positive)
    score_weights = {
        s.ticker: s.composite_score / total_score
        for s in positive
    }

    # Volatility adjustment (optional)
    if vol_adjustment and hist_vol_map:
        inv_vol = {
            t: 1.0 / max(hist_vol_map.get(t, 0.15), 0.05)
            for t in score_weights
        }
        total_inv_vol = sum(inv_vol.values())
        vol_weights = {t: v / total_inv_vol for t, v in inv_vol.items()}
        raw_weights = {
            t: 0.70 * score_weights[t] + 0.30 * vol_weights[t]
            for t in score_weights
        }
    else:
        raw_weights = score_weights

    # Single-position cap
    capped = {t: min(w, max_single) for t, w in raw_weights.items()}

    # Cash floor enforcement
    equity_total = min(sum(capped.values()), 1.0 - min_cash)
    scale = equity_total / sum(capped.values()) if sum(capped.values()) > 0 else 1.0
    scaled = {t: w * scale for t, w in capped.items()}
    scaled["CASH"] = round(1.0 - sum(scaled.values()), 6)

    return {k: round(v, 6) for k, v in scaled.items()}


# ─────────────────────────────── Main Entry ───────────────────────────────


async def run_quant_baseline_async(
    pipeline_context: dict,
    brief: dict,
) -> dict:
    """
    Stage 2 entry: layered signal computation -> base_weights.
    Async only for interface alignment with other stages.
    """
    holdings = brief.get("holdings") or []

    if not holdings:
        logger.error("QuantBaseline: no holdings in brief")
        return _empty_output("no_holdings")

    risk_params = pipeline_context.get("risk_params") or {}

    # ── Regime classification (must precede layered signal computation) ──
    portfolio = brief.get("portfolio") or {}
    spy_holding = next(
        (h for h in holdings if (h.get("ticker") or "").upper() == "SPY"),
        {},
    )
    regime_result = classify_market_regime(portfolio, spy_holding)
    logger.info(
        f"[Stage2] Regime: {regime_result.regime.value} "
        f"(confidence={regime_result.confidence}) | {regime_result.reasoning}"
    )

    # ── Layered signals ──
    layered_signals = compute_layered_signals(
        holdings=holdings,
        regime=regime_result.regime.value,
    )

    # ── Build hist_vol_map for volatility adjustment ──
    hist_vol_map = {
        h["ticker"]: float(h.get("hist_vol_20d", 0.15) or 0.15)
        for h in holdings
        if h.get("ticker") and h.get("ticker") != "CASH"
    }

    # ── Convert to weights ──
    base_weights = scores_to_weights(
        signals=layered_signals,
        min_cash=float(risk_params.get("min_cash_pct", 0.05)),
        max_single=float(risk_params.get("max_single_position", 0.20)),
        vol_adjustment=True,
        hist_vol_map=hist_vol_map,
    )

    selected_tickers = [
        t for t, w in base_weights.items()
        if t != "CASH" and w > 0
    ]

    # Sort by composite_score for ranking
    sorted_signals = sorted(layered_signals, key=lambda s: s.composite_score, reverse=True)

    logger.info(
        f"QuantBaseline done | regime={regime_result.regime.value} | "
        f"n_signals={len(layered_signals)} | n_selected={len(selected_tickers)} | "
        f"top5={[s.ticker for s in sorted_signals[:5]]}"
    )

    return {
        "base_weights": base_weights,
        "scoring_breakdown": [
            {
                "ticker":          s.ticker,
                "score":            s.composite_score,
                "factors": {
                    "composite_score":  s.composite_score,
                    "signal_short":     s.signal_short,
                    "signal_medium":    s.signal_medium,
                    "signal_long":      s.signal_long,
                    "blend_weights":    s.blend_weights_used,
                    "short_components": s.signal_short_components,
                    "medium_components": s.signal_medium_components,
                    "long_components":  s.signal_long_components,
                },
            }
            for s in layered_signals
        ],
        "ranking_summary": {
            "top_5":    [s.ticker for s in sorted_signals[:5]],
            "bottom_3": [s.ticker for s in sorted_signals[-3:]],
        },
        "selected_tickers": selected_tickers,
        "regime_result": {
            "regime":      regime_result.regime.value,
            "confidence":  regime_result.confidence,
            "signals":     regime_result.signals,
            "constraints": regime_result.constraints,
            "reasoning":   regime_result.reasoning,
        },
        "metadata": {
            "strategy_used":    "layered_momentum_v1",
            "strategy_version": "1.0",
            "params_used":      {},
        },
    }


# ─────────────────────────────── 工具 ───────────────────────────────


def _empty_output(reason: str) -> dict:
    return {
        "base_weights":      {"CASH": 1.0},
        "scoring_breakdown": [],
        "ranking_summary":    {"top_5": [], "bottom_3": []},
        "selected_tickers":  [],
        "regime_result":      None,
        "metadata":          {"strategy_used": None, "error": reason},
    }


# ─────────────────────────────── Regime 硬分类 ───────────────────────────────


class MarketRegime(str, Enum):
    TRENDING_BULL  = "trending_bull"
    TRENDING_BEAR = "trending_bear"
    HIGH_VOL      = "high_vol"
    MEAN_REVERTING = "mean_reverting"
    DEFENSIVE     = "defensive"


@dataclass
class RegimeResult:
    regime: MarketRegime
    confidence: str  # "high" / "medium" / "low"
    signals: dict[str, Any]
    constraints: dict[str, Any]
    reasoning: str


def classify_market_regime(
    portfolio_data: dict[str, Any],
    spy_holding: dict[str, Any],
) -> RegimeResult:
    """
    纯数学 regime 分类，无 LLM 依赖。

    分类逻辑（优先级从高到低）：
    1. drawdown > 10%  → DEFENSIVE
    2. VIX > 30 or atr_pct > 0.025  → HIGH_VOL
    3. SPY mom20d>0 & mom60d>0 & RSI<75  → TRENDING_BULL
    4. SPY mom20d<0 & mom60d<0  → TRENDING_BEAR
    5. 其他  → MEAN_REVERTING
    """
    drawdown = abs(float(portfolio_data.get("current_drawdown_pct") or 0.0))
    vix = portfolio_data.get("vix")
    if vix is not None:
        vix = float(vix)

    spy_mom_20d  = _safe_float(spy_holding.get("mom_20d"))
    spy_mom_60d  = _safe_float(spy_holding.get("mom_60d"))
    spy_mom_252d = _safe_float(spy_holding.get("mom_252d"))
    spy_rsi      = _safe_float(spy_holding.get("rsi_14"), 50.0)
    spy_atr_pct  = _safe_float(spy_holding.get("atr_pct"), 0.01)

    signals = {
        "drawdown":    drawdown,
        "vix":         vix,
        "spy_mom_20d":  spy_mom_20d,
        "spy_mom_60d":  spy_mom_60d,
        "spy_mom_252d": spy_mom_252d,
        "spy_rsi":      spy_rsi,
        "spy_atr_pct":  spy_atr_pct,
    }

    # 1. 防御模式
    if drawdown > 0.10:
        return RegimeResult(
            regime=MarketRegime.DEFENSIVE,
            confidence="high",
            signals=signals,
            constraints={
                "max_equity_weight":    0.50,
                "min_cash_weight":      0.20,
                "allow_new_positions":  False,
                "max_single_position":  0.12,
                "llm_instruction": (
                    "当前组合回撤超过10%，系统处于防御模式。"
                    "你的分析必须以降低风险为第一优先级，不得建议增加权益持仓。"
                    "任何新开仓建议将被系统自动拒绝。"
                ),
            },
            reasoning=f"Drawdown {drawdown:.1%} > 10%, defensive mode activated",
        )

    # 2. 高波动
    high_vol_trigger = (vix is not None and vix > 30) or (spy_atr_pct > 0.025)
    if high_vol_trigger:
        return RegimeResult(
            regime=MarketRegime.HIGH_VOL,
            confidence="high" if (vix is not None and vix > 35) else "medium",
            signals=signals,
            constraints={
                "max_equity_weight":    0.65,
                "min_cash_weight":      0.15,
                "allow_new_positions":  True,
                "max_single_position":  0.15,
                "prefer_hedges":        True,
                "llm_instruction": (
                    f"市场处于高波动状态（VIX={vix}, SPY ATR={spy_atr_pct:.2%}）。"
                    "分析时需优先考虑对冲资产（GLD/TLT/BND/IEF），降低进攻性持仓。"
                    "动量信号的可靠性在高波动期显著降低，请相应调低置信度。"
                ),
            },
            reasoning=f"High vol: VIX={vix}, SPY ATR={spy_atr_pct:.2%}",
        )

    # 3. 趋势上涨
    bull_score = sum([
        spy_mom_20d > 0.01,
        spy_mom_60d > 0.02,
        spy_mom_252d > 0.05,
        spy_rsi > 50,
        spy_rsi < 72,
    ])
    if bull_score >= 4:
        return RegimeResult(
            regime=MarketRegime.TRENDING_BULL,
            confidence="high" if bull_score == 5 else "medium",
            signals=signals,
            constraints={
                "max_equity_weight":    0.90,
                "min_cash_weight":      0.05,
                "allow_new_positions":  True,
                "max_single_position":  0.20,
                "llm_instruction": (
                    "市场处于趋势上涨状态，动量信号可信度高。"
                    "可适度增加进攻性持仓（XLK/XLY/QQQ/IWM），"
                    "但需警惕 RSI 超买风险。"
                ),
            },
            reasoning=f"Bull trend: mom20={spy_mom_20d:.2%}, mom60={spy_mom_60d:.2%}, RSI={spy_rsi:.1f}",
        )

    # 4. 趋势下跌
    bear_score = sum([
        spy_mom_20d < -0.01,
        spy_mom_60d < -0.02,
        spy_mom_252d < 0,
        spy_rsi < 45,
    ])
    if bear_score >= 3:
        return RegimeResult(
            regime=MarketRegime.TRENDING_BEAR,
            confidence="high" if bear_score == 4 else "medium",
            signals=signals,
            constraints={
                "max_equity_weight":    0.55,
                "min_cash_weight":      0.15,
                "allow_new_positions":  False,
                "max_single_position":  0.15,
                "prefer_hedges":        True,
                "llm_instruction": (
                    "市场处于趋势下跌状态。不得建议增加权益持仓，"
                    "应优先考虑减仓至防御性资产（GLD/TLT/BND）。"
                    "动量追涨策略在此环境下失效，请忽略短期反弹信号。"
                ),
            },
            reasoning=f"Bear trend: mom20={spy_mom_20d:.2%}, mom60={spy_mom_60d:.2%}, RSI={spy_rsi:.1f}",
        )

    # 5. 默认：震荡/均值回归
    return RegimeResult(
        regime=MarketRegime.MEAN_REVERTING,
        confidence="low",
        signals=signals,
        constraints={
            "max_equity_weight":    0.75,
            "min_cash_weight":      0.10,
            "allow_new_positions":  True,
            "max_single_position":  0.18,
            "llm_instruction": (
                "市场处于震荡均值回归状态，趋势信号可靠性低。"
                "建议保持接近 base_weights 的保守配置，"
                "避免根据短期动量做大幅调整。"
            ),
        },
        reasoning="No clear trend, mean-reverting regime",
    )


def _safe_float(val: Any, default: float = 0.0) -> float:
    try:
        return float(val) if val is not None else default
    except (TypeError, ValueError):
        return default
