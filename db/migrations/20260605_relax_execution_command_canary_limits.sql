-- Relax command-level execution throttle for the paper-live calibration phase.
--
-- This intentionally does not loosen:
--   - execution policy single/security caps
--   - final risk validation
--   - account state guard
--   - active execution serialization
--   - recent same-target dedupe
--
-- It only widens daily command / turnover budgets so the system can collect
-- execution feedback during the first hours of market activity.

INSERT INTO system_config (key, value, updated_at, updated_by)
VALUES (
    'execution_command_config',
    '{
      "max_daily_commands": 12,
      "max_gross_turnover_per_day": 1.50,
      "risk_reduce_reserved_commands": 4,
      "risk_reduce_gross_turnover_per_day": 0.25,
      "max_buy_delta": 0.15,
      "max_sell_delta": 0.20,
      "recent_same_target_dedupe_minutes": 5,
      "recent_same_target_dedupe_tolerance": 0.005
    }'::jsonb,
    now(),
    'migration_20260605_relax_execution_command_canary_limits'
)
ON CONFLICT (key) DO UPDATE
SET
    value = system_config.value
        || '{
          "max_daily_commands": 12,
          "max_gross_turnover_per_day": 1.50,
          "risk_reduce_reserved_commands": 4,
          "risk_reduce_gross_turnover_per_day": 0.25,
          "recent_same_target_dedupe_minutes": 5,
          "recent_same_target_dedupe_tolerance": 0.005
        }'::jsonb,
    updated_at = now(),
    updated_by = 'migration_20260605_relax_execution_command_canary_limits';
