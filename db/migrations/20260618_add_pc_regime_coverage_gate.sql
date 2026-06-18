-- Require point-in-time non-bull regime coverage before Portfolio
-- Construction can be promoted out of shadow/candidate evaluation.
--
-- This is a conservative promotion blocker only: if recent PC shadow cycles
-- are all from one bull regime, the gated PC path stays blocked. The flag is
-- configurable so operators can explicitly roll back to the previous behavior.

INSERT INTO system_config (key, value, updated_at, updated_by)
VALUES (
    'portfolio_construction_promotion_config',
    '{
      "require_regime_coverage": true,
      "min_non_bull_regime_cycles": 2,
      "min_regime_confidence_for_coverage": 0.60
    }'::jsonb,
    now(),
    'migration_20260618_add_pc_regime_coverage_gate'
)
ON CONFLICT (key) DO UPDATE
SET
    value = system_config.value
        || '{
          "require_regime_coverage": true,
          "min_non_bull_regime_cycles": 2,
          "min_regime_confidence_for_coverage": 0.60
        }'::jsonb,
    updated_at = now(),
    updated_by = 'migration_20260618_add_pc_regime_coverage_gate';
