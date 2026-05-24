-- PR7 conviction profiles.
-- Profiles are derived daily snapshots and may be recomputed from frozen
-- signals plus append-only outcomes. They do not authorize execution.

CREATE TABLE IF NOT EXISTS strategy_conviction_profiles (
    id BIGSERIAL PRIMARY KEY,
    profile_id VARCHAR(64) NOT NULL,
    as_of_date DATE NOT NULL,
    strategy_id VARCHAR(100) NOT NULL,
    ticker VARCHAR(20) NOT NULL,
    branch VARCHAR(120),
    action VARCHAR(30) NOT NULL,
    regime_at_signal VARCHAR(50),
    horizon_days INTEGER NOT NULL,
    source_bucket VARCHAR(40) NOT NULL,
    conviction DOUBLE PRECISION,
    status VARCHAR(60) NOT NULL,
    n INTEGER NOT NULL DEFAULT 0,
    required_samples INTEGER NOT NULL DEFAULT 30,
    hit_rate DOUBLE PRECISION,
    avg_forward_return DOUBLE PRECISION,
    avg_excess_vs_spy DOUBLE PRECISION,
    ic DOUBLE PRECISION,
    max_adverse_drawdown DOUBLE PRECISION,
    data_lag_filtered INTEGER NOT NULL DEFAULT 0,
    requires_live_confirmation BOOLEAN NOT NULL DEFAULT false,
    hist_n INTEGER NOT NULL DEFAULT 0,
    live_n INTEGER NOT NULL DEFAULT 0,
    hist_weight DOUBLE PRECISION,
    live_weight DOUBLE PRECISION,
    source_counts JSONB,
    diagnostics JSONB,
    content_hash VARCHAR(64) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    CONSTRAINT uq_strategy_conviction_profile_id UNIQUE (profile_id)
);

CREATE INDEX IF NOT EXISTS idx_strategy_conviction_profiles_as_of_date
    ON strategy_conviction_profiles (as_of_date);

CREATE INDEX IF NOT EXISTS idx_strategy_conviction_profiles_strategy
    ON strategy_conviction_profiles (strategy_id);

CREATE INDEX IF NOT EXISTS idx_strategy_conviction_profiles_ticker
    ON strategy_conviction_profiles (ticker);

CREATE INDEX IF NOT EXISTS idx_strategy_conviction_profiles_source_bucket
    ON strategy_conviction_profiles (source_bucket);
