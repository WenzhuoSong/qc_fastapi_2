# strategies/__init__.py
"""
策略注册表。新增策略只需在 STRATEGY_REGISTRY 里加一行。
"""
from typing import Any

from strategies.base import Strategy, ScoredTicker
from strategies.momentum_lite import MomentumLiteV1
from strategies.defensive_adjust import (
    defensive_adjust,
    compute_rebalance_actions,
    estimate_cost_pct,
    DEFAULT_DEFENSE_MATRIX,
)


STRATEGY_REGISTRY: dict[str, type[Strategy]] = {
    "momentum_lite_v1": MomentumLiteV1,
}


def get_strategy(name: str, params: dict[str, Any] | None = None) -> Strategy:
    cls = STRATEGY_REGISTRY.get(name)
    if cls is None:
        raise ValueError(
            f"Unknown strategy: {name}. Available: {list(STRATEGY_REGISTRY.keys())}"
        )
    return cls(params=params)


__all__ = [
    "Strategy",
    "ScoredTicker",
    "STRATEGY_REGISTRY",
    "get_strategy",
    "defensive_adjust",
    "compute_rebalance_actions",
    "estimate_cost_pct",
    "DEFAULT_DEFENSE_MATRIX",
]
