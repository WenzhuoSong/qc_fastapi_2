# db/seed.py
"""
写入 system_config 默认值（仅在 key 不存在时插入，不覆盖已有配置）。
"""
import logging
from db.session import AsyncSessionLocal
from db.queries import get_system_config, upsert_system_config

logger = logging.getLogger(__name__)

_DEFAULTS = {
    "risk_params": {
        "max_drawdown":            0.15,
        "max_single_position":     0.20,
        "min_cash_pct":            0.05,
        "max_sector_concentration": 0.60,
        "rebalance_threshold":     0.02,
        "max_trade_cost_pct":      0.005,
        "max_hist_vol":            0.35,
        "max_broad_market":        0.40,
    },
    "authorization_mode": {"value": "SEMI_AUTO"},
    "circuit_state":      {"value": "CLOSED"},
    "feature_authority_mode": {"value": "audit_only"},

    # 活跃策略指针
    "active_strategy": {"value": "momentum_lite_v1"},

    # MomentumLite v1.0 策略参数（与 strategies/momentum_lite.py 的 DEFAULT_PARAMS 一致）
    "strategy_momentum_lite_v1_params": {
        "w_mom_20d":       0.30,
        "w_mom_60d":       0.35,
        "w_mom_252d":      0.20,
        "w_rsi":           0.10,
        "w_atr":           0.05,
        "zscore_clip":     3.0,
        "max_holdings":    8,
        "vol_blend_alpha": 0.70,
    },

    # Stage 6.5 Position Manager: quantity/frequency controls after Risk Manager
    "position_manager_config": {
        "max_new_buys_per_cycle": 3,
        "max_positions": 12,
        "max_single_trade_pct": 0.08,
        "max_turnover_per_cycle": 0.30,
        "max_daily_trades": 5,
        "min_hold_days": 2,
    },

    # Research-only strategy playground. No execution authority.
    "playground_config": {
        "enabled": True,
        "lookback_days": 30,
        "strategies": [
            "momentum_lite_v1",
            "dual_momentum_rotation",
            "mean_reversion_lite",
            "low_vol_factor",
            "risk_parity_lite",
            "equal_weight_benchmark",
        ],
    },

    # Observe-only daily EvidenceCard freeze. Writes signal ledger rows only.
    "daily_signal_freeze_config": {
        "enabled": True,
    },

    # Observe-only outcome labeling + conviction profile refresh.
    "daily_signal_validation_config": {
        "enabled": True,
        "horizons": [1, 5, 20],
        "signal_row_limit": 5000,
        "feature_source": "yfinance",
    },
}


async def seed_system_config():
    async with AsyncSessionLocal() as db:
        for key, default_value in _DEFAULTS.items():
            existing = await get_system_config(db, key)
            if existing is None:
                await upsert_system_config(db, key, default_value, "seed")
                logger.info(f"seed_system_config: inserted default for '{key}'")
            else:
                logger.debug(f"seed_system_config: '{key}' already exists, skipped")
