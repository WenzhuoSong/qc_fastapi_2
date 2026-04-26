# services/quant_baseline.py
"""
Stage 2: QUANT BASELINE —— 纯 Python 量化基准打分层

职责（无 LLM）：
    1. 从 brief 拿 holdings
    2. 读 pipeline_context 里的策略名 / 参数 / risk_params
    3. strategy.score() → scored[]       （5 因子复合 Z-score 排名）
    4. strategy.optimize() → base_weights（分数加权 + 波动率混合 + 单仓 cap + 现金底线）
    5. 打包 baseline 给下游 RESEARCHER 用

严格无状态、无副作用、无任何 overlay。Overlay (transmission / defensive /
hard_risk) 全部下沉到 RISK MGR 负责。

输出约定（下游消费者合同）：
    {
        "base_weights":       dict[ticker, float]  # 总和 ≈ 1.0 (含 CASH)
        "scoring_breakdown":  list[dict]            # 全 universe 的 ticker + score + factors
        "ranking_summary":    {"top_5": [...], "bottom_3": [...]}
        "selected_tickers":   list[str]             # optimize 选中的非 CASH 头部
        "metadata":           {strategy_used, strategy_version, params_used}
    }
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from strategies import get_strategy

logger = logging.getLogger("qc_fastapi_2.quant_baseline")


# regime 映射（Stage 2 的 optimize 需要 direction_bias 决定 N）
# 注意：Stage 2 发生在 RESEARCHER 之前，此时还没有 regime 判断。
# 为此我们用一个"中性"占位上下文 —— baseline 只负责做"纯数学"的最优分配，
# 让 LLM 稍后在 Stage 3 基于 baseline + 新闻 + 自己的 regime 判断做定性调整。
NEUTRAL_STRATEGY_CONTEXT = {
    "regime":           "neutral",
    "confidence":       0.5,
    "uncertainty_flag": False,
    "stance":           "maintain",
    "direction_bias":   "neutral",
}


# ─────────────────────────────── 主入口 ───────────────────────────────


async def run_quant_baseline_async(
    pipeline_context: dict,
    brief: dict,
) -> dict:
    """
    Stage 2 入口。同步逻辑，async 只是为了和其他 stage 对齐。
    """
    holdings = brief.get("holdings") or []
    current_weights = brief.get("current_weights") or {}

    if not holdings:
        logger.error("QuantBaseline: no holdings in brief")
        return _empty_output("no_holdings")

    risk_params     = pipeline_context.get("risk_params") or {}
    active_name     = pipeline_context.get("active_strategy") or "momentum_lite_v1"
    strategy_params = pipeline_context.get("strategy_params") or {}

    strategy = get_strategy(active_name, strategy_params)

    context = {
        **NEUTRAL_STRATEGY_CONTEXT,
        "risk_params":     risk_params,
        "current_weights": current_weights,
    }

    scored = strategy.score(holdings, context)
    if not scored:
        logger.error("QuantBaseline: strategy produced empty scoring")
        return _empty_output("empty_scoring")

    base_weights = strategy.optimize(scored, context)

    selected_tickers = [
        t for t, w in base_weights.items()
        if t != "CASH" and w > 0
    ]

    # ── Regime 硬分类 ─────────────────────────────────────────────
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

    logger.info(
        f"QuantBaseline done | strategy={active_name} | "
        f"n_scored={len(scored)} | n_selected={len(selected_tickers)} | "
        f"top5={[s.ticker for s in scored[:5]]}"
    )

    return {
        "base_weights": base_weights,
        "scoring_breakdown": [
            {
                "ticker":  s.ticker,
                "score":   round(s.score, 4),
                "factors": s.factor_breakdown,
            }
            for s in scored
        ],
        "ranking_summary": {
            "top_5":    [s.ticker for s in scored[:5]],
            "bottom_3": [s.ticker for s in scored[-3:]],
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
            "strategy_used":    active_name,
            "strategy_version": strategy.version,
            "params_used":      dict(strategy.params),
        },
    }


# ─────────────────────────────── 工具 ───────────────────────────────


def _empty_output(reason: str) -> dict:
    return {
        "base_weights":      {"CASH": 1.0},
        "scoring_breakdown": [],
        "ranking_summary":   {"top_5": [], "bottom_3": []},
        "selected_tickers":  [],
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
