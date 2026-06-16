-- Step 3 paper-live calibration: widen concurrent small-add exploration.
--
-- This does not loosen certification, scorecard no-add semantics, sizing,
-- single-trade caps, turnover caps, or daily buy-delta caps. It only allows
-- the already-approved small-add lane to carry up to four new buys per cycle.

INSERT INTO system_config (key, value, updated_at, updated_by)
VALUES (
    'position_manager_config',
    '{
      "max_new_buys_per_cycle": 4,
      "max_positions": 12,
      "max_single_trade_pct": 0.08,
      "max_turnover_per_cycle": 0.30,
      "max_daily_trades": 5,
      "min_hold_days": 2
    }'::jsonb,
    now(),
    'migration_20260616_step3_small_add_width'
)
ON CONFLICT (key) DO UPDATE
SET
    value = system_config.value
        || '{"max_new_buys_per_cycle": 4}'::jsonb,
    updated_at = now(),
    updated_by = 'migration_20260616_step3_small_add_width';
