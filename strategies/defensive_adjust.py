# strategies/defensive_adjust.py
"""
防御调整 + 调仓动作 + 成本估算

这些函数独立于具体策略，适用于任何 Strategy 产出的 Plan A。
"""
from typing import Any


DEFAULT_DEFENSE_MATRIX: dict[str, float] = {
    "bull_trend":  0.00,   # 不调整
    "bull_weak":   0.15,   # 股票仓降 15%
    "neutral":     0.25,   # 股票仓降 25%
    "bear_weak":   0.40,   # 股票仓降 40%
    "bear_trend":  0.55,   # 股票仓降 55%
    "high_vol":    0.50,   # 高波动降 50%
}

UNCERTAINTY_BONUS = 0.10   # uncertainty_flag=True 时额外 +10%
DEFENSE_CAP       = 0.60   # 防御力度上限


def defensive_adjust(
    plan_a_weights: dict[str, float],
    context: dict[str, Any],
    defense_matrix: dict[str, float] | None = None,
) -> dict[str, float]:
    """
    从 Plan A 按 regime 等比缩减股票仓，多余归现金。

    context 字段：
        regime           : str
        uncertainty_flag : bool
    """
    regime      = context.get("regime", "neutral")
    uncertainty = bool(context.get("uncertainty_flag", False))

    matrix = defense_matrix or DEFAULT_DEFENSE_MATRIX
    base_defense = float(matrix.get(regime, matrix.get("neutral", 0.25)))

    defense = base_defense + UNCERTAINTY_BONUS if uncertainty else base_defense
    defense = min(defense, DEFENSE_CAP)

    plan_b: dict[str, float] = {}
    for ticker, weight in plan_a_weights.items():
        if ticker == "CASH":
            continue
        plan_b[ticker] = round(weight * (1.0 - defense), 4)

    non_cash_sum = sum(plan_b.values())
    plan_b["CASH"] = round(max(1.0 - non_cash_sum, 0.0), 4)
    return plan_b


def compute_rebalance_actions(
    target: dict[str, float],
    current: dict[str, float],
    threshold: float = 0.02,
) -> list[dict]:
    """
    对比 target vs current，过滤小于 threshold 的调整。
    卖出操作优先排前，便于后续 EXECUTOR 先释放现金。
    """
    actions: list[dict] = []
    all_tickers = set(target.keys()) | set(current.keys())

    for ticker in all_tickers:
        if ticker == "CASH":
            continue
        tgt = float(target.get(ticker, 0.0) or 0.0)
        cur = float(current.get(ticker, 0.0) or 0.0)
        delta = tgt - cur
        if abs(delta) < threshold:
            continue
        actions.append({
            "ticker":         ticker,
            "action":         "buy" if delta > 0 else "sell",
            "weight_current": round(cur, 4),
            "weight_target":  round(tgt, 4),
            "weight_delta":   round(delta, 4),
        })

    # 卖出优先，内部按 |delta| 降序
    actions.sort(key=lambda a: (a["action"] != "sell", -abs(a["weight_delta"])))
    return actions


def estimate_cost_pct(actions: list[dict], commission_rate: float = 0.001) -> float:
    """
    交易成本 ≈ 总换手率 × 佣金率（默认 0.1%）
    仅粗略估算，供方案对比使用。
    """
    turnover = sum(abs(a["weight_delta"]) for a in actions)
    return round(turnover * commission_rate, 6)
