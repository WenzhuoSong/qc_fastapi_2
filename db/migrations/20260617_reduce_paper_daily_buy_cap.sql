-- Reduce paper-live daily buy cap after Step 3 pressure-test validation.
--
-- Step 3 proved single-command multi-leg SetWeights, daily cap accounting,
-- QC ownership/fill, and reconciliation can close cleanly. Keep the paper
-- path close to real-money cadence so future portfolio trajectory data is
-- transferable while preserving the 3% shadow real-money diagnostic cap.

INSERT INTO system_config (key, value, updated_at, updated_by)
VALUES (
    'execution_command_config',
    '{
      "max_buy_delta_per_day": 0.04,
      "shadow_real_money_max_buy_delta_per_day": 0.03
    }'::jsonb,
    now(),
    'migration_20260617_reduce_paper_daily_buy_cap'
)
ON CONFLICT (key) DO UPDATE
SET
    value = system_config.value
        || '{
          "max_buy_delta_per_day": 0.04,
          "shadow_real_money_max_buy_delta_per_day": 0.03
        }'::jsonb,
    updated_at = now(),
    updated_by = 'migration_20260617_reduce_paper_daily_buy_cap';
