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
