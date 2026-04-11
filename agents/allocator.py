# agents/allocator.py
"""
ALLOCATOR = 策略执行器 + LLM 决策层

流程：
  1. 读最新持仓快照 + risk_params + 活跃策略配置
  2. 策略打分 → Plan A（理想组合）
  3. defensive_adjust → Plan B（保守版）
  4. 计算 rebalance_actions + estimated_cost
  5. LLM 从 A/B 选一个并写 reasoning（LLM 失败自动降级为规则）
"""
import json
import logging
from typing import Any

from openai import AsyncOpenAI

from config import get_settings
from db.session import AsyncSessionLocal
from db.queries import get_system_config
from tools.db_tools import tool_read_latest_snapshots
from strategies import (
    get_strategy,
    defensive_adjust,
    compute_rebalance_actions,
    estimate_cost_pct,
)

logger = logging.getLogger("qc_fastapi_2.allocator")
settings = get_settings()
_openai = AsyncOpenAI(api_key=settings.openai_api_key)


# regime → direction_bias 映射（用于策略的 N 选择）
REGIME_TO_BIAS = {
    "bull_trend": "bullish",
    "bull_weak":  "bullish",
    "neutral":    "neutral",
    "bear_weak":  "bearish",
    "bear_trend": "bearish",
    "high_vol":   "bearish",
}


DECIDE_SYSTEM_PROMPT = """你是 ALLOCATOR 的决策层。
你不做任何算术，只基于给定的两套方案和市场上下文选择 Plan A 或 Plan B。

决策原则：
- stance=defensive 或 regime ∈ {bear_trend, high_vol} → 倾向 B
- 当前回撤接近 max_drawdown → 倾向 B
- regime=bull_trend 且 confidence>0.65 → 倾向 A
- Plan A 成本 > max_trade_cost_pct → 倾向 B
- 其它情况默认 A

回答必须为纯 JSON，不包含 markdown：
{"recommended_plan": "A" | "B", "reasoning": "不超过 100 字的理由"}
"""


# ─────────────────────────────── 主入口 ───────────────────────────────


async def run_allocator_async(plan: dict, researcher_output: dict) -> dict:
    """Pipeline 入口。返回结构兼容 executor.py 和 pipeline.py。"""
    # 1. 读快照 + 当前持仓
    snapshots = await tool_read_latest_snapshots({"n": 1})
    if not snapshots:
        logger.error("Allocator: no snapshots available")
        return _empty_output("no_snapshots")

    latest     = snapshots[0] or {}
    holdings   = latest.get("holdings", []) or []
    portfolio  = latest.get("portfolio", {}) or {}
    current_weights = {
        h["ticker"]: float(h.get("weight_current") or 0)
        for h in holdings if h.get("ticker")
    }
    drawdown_pct = float(portfolio.get("current_drawdown_pct") or 0)

    # 2. 读配置
    risk_params = await _load_config("risk_params", default={})
    active_name, strategy_params = await _load_active_strategy()
    strategy = get_strategy(active_name, strategy_params)

    # 3. 构建 context
    mj = researcher_output.get("market_judgment", {}) or {}
    regime = mj.get("regime", "neutral")
    context = {
        "regime":           regime,
        "confidence":       float(mj.get("adjusted_confidence", 0.5) or 0.5),
        "uncertainty_flag": bool(mj.get("uncertainty_flag", False)),
        "stance":           researcher_output.get("recommended_stance", "maintain"),
        "direction_bias":   REGIME_TO_BIAS.get(regime, "neutral"),
        "risk_params":      risk_params,
        "current_weights":  current_weights,
    }

    # 4. 策略打分 → Plan A
    scored = strategy.score(holdings, context)
    if not scored:
        logger.error("Allocator: strategy produced empty scoring")
        return _empty_output("empty_scoring")

    plan_a_weights = strategy.optimize(scored, context)

    # 5. Plan B（通用防御调整）
    plan_b_weights = defensive_adjust(plan_a_weights, context)

    # 6. rebalance_actions + cost
    threshold = float(risk_params.get("rebalance_threshold", 0.02))
    plan_a_actions = compute_rebalance_actions(plan_a_weights, current_weights, threshold)
    plan_b_actions = compute_rebalance_actions(plan_b_weights, current_weights, threshold)
    plan_a_cost    = estimate_cost_pct(plan_a_actions)
    plan_b_cost    = estimate_cost_pct(plan_b_actions)

    max_trade_cost = float(risk_params.get("max_trade_cost_pct", 0.005))

    plan_a = {
        "label":               "standard",
        "target_weights":      plan_a_weights,
        "rebalance_actions":   plan_a_actions,
        "estimated_cost_pct":  plan_a_cost,
        "n_holdings":          _count_non_cash(plan_a_weights),
        "high_cost_warning":   plan_a_cost > max_trade_cost,
    }
    plan_b = {
        "label":               "conservative",
        "target_weights":      plan_b_weights,
        "rebalance_actions":   plan_b_actions,
        "estimated_cost_pct":  plan_b_cost,
        "n_holdings":          _count_non_cash(plan_b_weights),
        "high_cost_warning":   plan_b_cost > max_trade_cost,
    }

    # 7. LLM 决策（失败自动降级规则）
    decision = await _choose_plan(
        plan_a, plan_b, researcher_output, drawdown_pct,
        max_trade_cost, risk_params,
    )

    logger.info(
        f"Allocator done | strategy={active_name} | regime={regime} | "
        f"plan={decision['recommended_plan']} | "
        f"n_actions_a={len(plan_a_actions)} | cost_a={plan_a_cost:.4%}"
    )

    return {
        "recommended_plan": decision["recommended_plan"],
        "plan_a":           plan_a,
        "plan_b":           plan_b,
        "reasoning":        decision["reasoning"],
        "ranking_summary": {
            "top_5":    [s.ticker for s in scored[:5]],
            "bottom_3": [s.ticker for s in scored[-3:]],
        },
        "metadata": {
            "strategy_used":    active_name,
            "strategy_version": strategy.version,
            "regime":           regime,
            "direction_bias":   context["direction_bias"],
            "confidence":       context["confidence"],
        },
        "scoring_breakdown": [
            {
                "ticker":  s.ticker,
                "score":   round(s.score, 4),
                "factors": s.factor_breakdown,
            }
            for s in scored
        ],
    }


# ─────────────────────────────── 辅助函数 ───────────────────────────────


async def _load_config(key: str, default: Any = None) -> Any:
    async with AsyncSessionLocal() as db:
        cfg = await get_system_config(db, key)
        return cfg.value if cfg else default


async def _load_active_strategy() -> tuple[str, dict]:
    """从 system_config 读取活跃策略名和对应参数。"""
    active_cfg = await _load_config("active_strategy", default={"value": "momentum_lite_v1"})
    active_name = active_cfg.get("value", "momentum_lite_v1")

    params_key = f"strategy_{active_name}_params"
    params = await _load_config(params_key, default={}) or {}
    return active_name, params


def _count_non_cash(weights: dict[str, float]) -> int:
    return sum(1 for t, w in weights.items() if t != "CASH" and w > 0)


async def _choose_plan(
    plan_a: dict,
    plan_b: dict,
    researcher_output: dict,
    drawdown_pct: float,
    max_trade_cost: float,
    risk_params: dict,
) -> dict:
    """LLM 选 A/B；失败自动降级到规则。"""
    input_data = {
        "plan_a_summary": {
            "target_weights":     plan_a["target_weights"],
            "estimated_cost_pct": plan_a["estimated_cost_pct"],
            "n_holdings":         plan_a["n_holdings"],
            "high_cost_warning":  plan_a["high_cost_warning"],
        },
        "plan_b_summary": {
            "target_weights":     plan_b["target_weights"],
            "estimated_cost_pct": plan_b["estimated_cost_pct"],
            "n_holdings":         plan_b["n_holdings"],
        },
        "market_judgment":      researcher_output.get("market_judgment", {}),
        "recommended_stance":   researcher_output.get("recommended_stance"),
        "current_drawdown_pct": round(drawdown_pct, 4),
        "max_trade_cost_pct":   max_trade_cost,
        "max_drawdown":         risk_params.get("max_drawdown", 0.15),
    }

    try:
        resp = await _openai.chat.completions.create(
            model=settings.openai_model_heavy,
            messages=[
                {"role": "system", "content": DECIDE_SYSTEM_PROMPT},
                {"role": "user",   "content": json.dumps(input_data, ensure_ascii=False)},
            ],
            temperature=0.0,
            max_tokens=400,
        )
        raw = (resp.choices[0].message.content or "").strip()
        if raw.startswith("```"):
            lines = raw.split("\n")
            raw = "\n".join(lines[1:-1] if lines[-1] == "```" else lines[1:])
        parsed = json.loads(raw)
        chosen = str(parsed.get("recommended_plan", "A")).upper()
        if chosen not in ("A", "B"):
            chosen = "A"
        return {
            "recommended_plan": chosen,
            "reasoning":        str(parsed.get("reasoning", "")).strip() or "(LLM 未提供理由)",
        }
    except Exception as e:
        logger.warning(f"LLM plan selection failed, using rule fallback: {e}")
        return _rule_fallback(researcher_output, plan_a, drawdown_pct, risk_params)


def _rule_fallback(
    researcher_output: dict,
    plan_a: dict,
    drawdown_pct: float,
    risk_params: dict,
) -> dict:
    regime = (researcher_output.get("market_judgment") or {}).get("regime", "neutral")
    stance = researcher_output.get("recommended_stance", "maintain")
    cost   = plan_a.get("estimated_cost_pct", 0)
    max_dd = float(risk_params.get("max_drawdown", 0.15))
    max_cost = float(risk_params.get("max_trade_cost_pct", 0.005))

    if stance == "defensive" or regime in ("bear_trend", "high_vol"):
        return {"recommended_plan": "B", "reasoning": f"规则降级: stance={stance} regime={regime}"}
    if drawdown_pct >= max_dd * 0.75:
        return {"recommended_plan": "B", "reasoning": f"规则降级: 回撤 {drawdown_pct:.2%} 接近上限 {max_dd:.2%}"}
    if cost > max_cost:
        return {"recommended_plan": "B", "reasoning": f"规则降级: Plan A 成本 {cost:.4%} > {max_cost:.4%}"}
    return {"recommended_plan": "A", "reasoning": "规则降级: 默认 A"}


def _empty_output(reason: str) -> dict:
    empty_plan = {
        "label":              "empty",
        "target_weights":     {"CASH": 1.0},
        "rebalance_actions":  [],
        "estimated_cost_pct": 0.0,
        "n_holdings":         0,
        "high_cost_warning":  False,
    }
    return {
        "recommended_plan":  "A",
        "plan_a":            empty_plan,
        "plan_b":            empty_plan,
        "reasoning":         f"ALLOCATOR 空输出: {reason}",
        "ranking_summary":   {"top_5": [], "bottom_3": []},
        "metadata":          {"strategy_used": None, "error": reason},
        "scoring_breakdown": [],
    }
