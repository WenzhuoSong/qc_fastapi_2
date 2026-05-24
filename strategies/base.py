# strategies/base.py
"""
Strategy 抽象基类。所有评分策略必须继承 Strategy 并实现 score() / optimize()。
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

from services.universe_policy import is_tradable_research_row


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
    required_fields: tuple[str, ...] = ()
    optional_fields: tuple[str, ...] = ()
    min_required_coverage: float = 0.70
    family: str = "benchmark"
    core_idea: str = ""
    best_regimes: tuple[str, ...] = ()
    bad_regimes: tuple[str, ...] = ()
    signals_used: tuple[str, ...] = ()
    failure_modes: tuple[str, ...] = ()
    agent_guidance: str = ""
    universe_tickers: tuple[str, ...] = ()
    allow_hedge_research_tickers: bool = False

    def __init__(self, params: dict[str, Any] | None = None):
        self.params = params or {}

    def eligible_rows(self, holdings: list[dict]) -> list[dict]:
        if self.universe_tickers:
            allowed = {ticker.upper() for ticker in self.universe_tickers}
            return [
                h for h in holdings
                if (h.get("ticker") or "").upper().strip() in allowed
            ]
        if self.allow_hedge_research_tickers:
            return [
                h for h in holdings
                if (h.get("ticker") or "").upper().strip()
                and (h.get("ticker") or "").upper().strip() != "CASH"
            ]
        return [
            h for h in holdings
            if is_tradable_research_row(h)
        ]

    def data_requirements(self) -> dict[str, Any]:
        return {
            "required_fields": list(self.required_fields),
            "optional_fields": list(self.optional_fields),
            "min_required_coverage": self.min_required_coverage,
        }

    def strategy_card(self) -> dict[str, Any]:
        """Compact English description for downstream agents."""
        return {
            "name": self.name,
            "version": self.version,
            "family": self.family,
            "description": self.description,
            "core_idea": self.core_idea,
            "best_regimes": list(self.best_regimes),
            "bad_regimes": list(self.bad_regimes),
            "signals_used": list(self.signals_used),
            "failure_modes": list(self.failure_modes),
            "agent_guidance": self.agent_guidance,
            "data_requirements": self.data_requirements(),
        }

    def data_readiness(self, holdings: list[dict]) -> dict[str, Any]:
        valid_holdings = self.eligible_rows(holdings)
        if not self.required_fields:
            return {
                "ready": True,
                "coverage": 1.0,
                "missing_fields": [],
                "field_coverage": {},
                "eligible_tickers": [h.get("ticker") for h in valid_holdings],
            }
        if not valid_holdings:
            return {
                "ready": False,
                "coverage": 0.0,
                "missing_fields": list(self.required_fields),
                "field_coverage": {},
                "eligible_tickers": [],
            }

        field_coverage: dict[str, float] = {}
        missing_fields: list[str] = []
        for field in self.required_fields:
            covered = sum(1 for h in valid_holdings if h.get(field) is not None)
            coverage = covered / len(valid_holdings)
            field_coverage[field] = round(coverage, 4)
            if coverage < self.min_required_coverage:
                missing_fields.append(field)

        eligible = [
            h.get("ticker") for h in valid_holdings
            if all(h.get(field) is not None for field in self.required_fields)
        ]
        aggregate = min(field_coverage.values()) if field_coverage else 1.0
        return {
            "ready": aggregate >= self.min_required_coverage,
            "coverage": round(aggregate, 4),
            "missing_fields": missing_fields,
            "field_coverage": field_coverage,
            "eligible_tickers": eligible,
        }

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
