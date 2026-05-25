# strategies/__init__.py
"""
策略注册表。新增策略只需在 STRATEGY_REGISTRY 里加一行。
"""
from typing import Any

from strategies.base import Strategy, ScoredTicker
from strategies.momentum_lite import MomentumLiteV1
from strategies.absolute_trend_following_lite import AbsoluteTrendFollowingLite
from strategies.seasonality_month_end_lite import SeasonalityMonthEndLite
from strategies.sector_theme_relative_strength_lite import SectorThemeRelativeStrengthLite
from strategies.mean_reversion_lite import MeanReversionLite
from strategies.relative_value_reversion_lite import RelativeValueReversionLite
from strategies.low_vol_factor import LowVolFactor
from strategies.defensive_quality_rotation_lite import DefensiveQualityRotationLite
from strategies.macro_rate_duration_lite import MacroRateDurationLite
from strategies.equal_weight import EqualWeightBenchmark
from strategies.risk_parity_lite import RiskParityLite
from strategies.dual_momentum import DualMomentumRotation
from strategies.leveraged_etf_momentum_allocator import LeveragedETFMomentumAllocator
from strategies.carry_cash_proxy_lite import CarryCashProxyLite
from strategies.volatility_hedge_lite import VolatilityHedgeLite
from strategies.inverse_equity_hedge_lite import InverseEquityHedgeLite
from strategies.leveraged_long_amplifier_lite import LeveragedLongAmplifierLite
from strategies.defensive_adjust import (
    defensive_adjust,
    compute_rebalance_actions,
    estimate_cost_pct,
    DEFAULT_DEFENSE_MATRIX,
)


STRATEGY_REGISTRY: dict[str, type[Strategy]] = {
    "momentum_lite_v1": MomentumLiteV1,
    "absolute_trend_following_lite": AbsoluteTrendFollowingLite,
    "seasonality_month_end_lite": SeasonalityMonthEndLite,
    "sector_theme_relative_strength_lite": SectorThemeRelativeStrengthLite,
    "mean_reversion_lite": MeanReversionLite,
    "relative_value_reversion_lite": RelativeValueReversionLite,
    "low_vol_factor": LowVolFactor,
    "defensive_quality_rotation_lite": DefensiveQualityRotationLite,
    "macro_rate_duration_lite": MacroRateDurationLite,
    "dual_momentum_rotation": DualMomentumRotation,
    "risk_parity_lite": RiskParityLite,
    "equal_weight_benchmark": EqualWeightBenchmark,
    "leveraged_etf_momentum_allocator": LeveragedETFMomentumAllocator,
    "carry_cash_proxy_lite": CarryCashProxyLite,
    "volatility_hedge_lite": VolatilityHedgeLite,
    "inverse_equity_hedge_lite": InverseEquityHedgeLite,
    "leveraged_long_amplifier_lite": LeveragedLongAmplifierLite,
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
