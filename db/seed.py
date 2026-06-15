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
    "operator_halt_state": {
        "halted": False,
        "reason": "",
        "updated_at": None,
        "updated_by": "seed",
    },
    "feature_authority_mode": {"value": "audit_only"},
    "reconciliation_guard_config": {
        "enabled": True,
        "mode": "blocking",
        "relative_weight_tolerance": 0.0025,
        "absolute_notional_tolerance_usd": 100.0,
        "ignore_cash": True,
        "cash_tolerance_mode": "residual",
        "market_closed_behavior": "skip",
        "auto_set_reconciliation_halt": False,
        "max_pending_ack_age_seconds": 300,
        "max_in_flight_age_seconds": 900,
    },

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

    # Execution-grade strategy evidence gate. This does not create a new
    # execution path; it controls whether certified strategy evidence may
    # release the scorecard strategy-evidence no-add boundary.
    "strategy_execution_evidence_config": {
        "enabled": True,
        "force_advisory_only": False,
        "min_live_samples_for_execution": 5,
        "state_scope": "strategy_level",
        "paper_live_outcome_evidence_enabled": True,
        "paper_live_signal_source": "fastapi_live_freeze",
        "paper_live_outcome_horizon_days": 1,
        "paper_live_actions": ["increase"],
    },

    # Portfolio Construction PR4 candidate diagnostics. Shadow is diagnostic-only.
    "portfolio_construction_promotion_config": {
        "portfolio_construction_mode": "shadow",
        "enabled": False,
        "require_manual_approval": True,
        "min_shadow_cycles": 20,
        "min_pass_rate": 0.90,
        "max_material_diff": 0.015,
        "max_turnover_diff": 0.02,
        "require_semi_auto_gated_before_full_auto": True,
        "min_gated_semi_auto_confirmed_cycles": 5,
        "allow_full_auto_gated": False,
    },

    # Final post-risk validation. Blocking mode should be enabled only after
    # the operator reviews observe-mode drift distribution.
    "final_risk_validation_config": {
        "mode": "observe",
        "material_drift_threshold": 0.015,
        "threshold_basis": "operator_default_pending_observe_mode_distribution",
        "require_human_confirmation_for_conditional_material_drift": True,
    },

    # Post-risk executable target contract. Active mode makes TargetEnvelope
    # the authority for final validation and execution while legacy dict
    # outputs remain available as diagnostics during migration.
    "target_envelope_config": {
        "enabled": True,
        "mode": "active",
        "shadow_compare_enabled": True,
        "block_on_accounting_failure": True,
        "block_on_safety_failure": True,
    },

    # Command-level execution preflight. These caps sit after final risk
    # validation and before any SetWeights command can be submitted to QC.
    "execution_command_config": {
        "max_daily_commands": 12,
        "max_gross_turnover_per_day": 1.50,
        "risk_reduce_reserved_commands": 4,
        "risk_reduce_gross_turnover_per_day": 0.25,
        "max_buy_delta": 0.15,
        "max_sell_delta": 0.20,
    },

    # Control-plane recovery for FastAPI/QC policy drift. This may send
    # PolicySync only; it never authorizes SetWeights in the same cycle.
    "policy_sync_recovery_config": {
        "enabled": True,
        "max_recovery_attempts": 3,
        "max_consecutive_mismatch_cycles": 5,
        "fire_and_forget": True,
        "expected_policy_version_source": "execution_policy",
    },

    # Alpha decision diagnostics consumption policy. This never authorizes
    # direct execution; gated allocation impact requires explicit operator
    # review and still flows through target_builder and all risk gates.
    "alpha_decision_policy_config": {
        "mode": "observe",
        "min_status_for_promotion": "indicative",
        "min_status_for_allocation_full_credit": "statistically_meaningful",
        "require_positive_residual_alpha": True,
        "require_cost_adjusted_edge_positive": True,
        "max_full_credit_correlation": 0.4,
        "max_allowed_duplicate_correlation": 0.8,
        "cost_model": "ibkr_proxy",
        "min_observe_cycles_before_gated": 20,
        "operator_approval_required_for_gated": True,
        "operator_gated_approved": False,
        "raw_adjusted_diagnostics_reviewed": False,
        "dry_run_report_reviewed": False,
        "unexpected_mature_degradation_false_positive_count": 0,
        "evidence_cap_calibration_fresh": False,
        "dashboard_naked_conviction_blocked": True,
        "observe_cycles": 0,
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
