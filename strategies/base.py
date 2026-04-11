# strategies/base.py
"""
Strategy 抽象基类。所有评分策略必须继承 Strategy 并实现 score() / optimize()。
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any


@dataclass
class ScoredTicker:
    ticker: str
    score: float
    factor_breakdown: dict[str, float] = field(default_factory=dict)
    raw_factors: dict[str, float | None] = field(default_factory=dict)


class Strategy(ABC):
    """
    所有策略的统一接口。

    Context 约定字段：
        regime           : str   — 6 值枚举之一
        confidence       : float — 0.0–1.0
        uncertainty_flag : bool
        stance           : str   — maintain|increase|reduce|defensive
        direction_bias   : str   — bullish|neutral|bearish
        risk_params      : dict  — max_single_position / min_cash_pct / rebalance_threshold ...
        current_weights  : dict  — {ticker: weight} 当前持仓
    """

    name: str = ""
    version: str = "1.0"
    description: str = ""

    def __init__(self, params: dict[str, Any] | None = None):
        self.params = params or {}

    @abstractmethod
    def score(
        self,
        holdings: list[dict],
        context: dict[str, Any],
    ) -> list[ScoredTicker]:
        """对 universe 所有标的打分，返回按 score 降序排列的 ScoredTicker 列表。"""

    @abstractmethod
    def optimize(
        self,
        scored: list[ScoredTicker],
        context: dict[str, Any],
    ) -> dict[str, float]:
        """
        从评分结果生成目标权重字典（Plan A），必须包含 'CASH' key。
        总和应该 = 1.0。
        """
