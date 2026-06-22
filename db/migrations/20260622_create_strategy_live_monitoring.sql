-- newBase/QC live monitoring support.
--
-- These tables are observer-only. They do not authorize execution, mutate
-- target weights, or participate in broker/QC command submission.

CREATE TABLE IF NOT EXISTS strategy_registry_entries (
    strategy_id VARCHAR(100) PRIMARY KEY,
    source VARCHAR(40) NOT NULL,
    display_name VARCHAR(120),
    benchmark_primary VARCHAR(20) NOT NULL,
    benchmark_secondary VARCHAR(20),
    expected_profile JSONB NOT NULL,
    execution_authority VARCHAR(40) NOT NULL DEFAULT 'none',
    target_weight_mutation VARCHAR(40) NOT NULL DEFAULT 'none',
    review_only BOOLEAN NOT NULL DEFAULT true,
    notes TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    updated_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS strategy_live_snapshots (
    id BIGSERIAL PRIMARY KEY,
    snapshot_uid VARCHAR(140) NOT NULL,
    strategy_id VARCHAR(100) NOT NULL,
    qc_snapshot_id BIGINT REFERENCES qc_snapshots(id),
    recorded_at TIMESTAMP NOT NULL,
    trading_date DATE NOT NULL,
    source VARCHAR(40) NOT NULL DEFAULT 'quantconnect',
    mode VARCHAR(40),
    algorithm_version VARCHAR(80),
    total_value DOUBLE PRECISION,
    cash DOUBLE PRECISION,
    cash_pct DOUBLE PRECISION,
    daily_return DOUBLE PRECISION,
    cumulative_return DOUBLE PRECISION,
    current_drawdown DOUBLE PRECISION,
    turnover DOUBLE PRECISION,
    fees DOUBLE PRECISION,
    benchmark_primary VARCHAR(20) NOT NULL DEFAULT 'QQQ',
    benchmark_primary_return DOUBLE PRECISION,
    benchmark_primary_cumulative_return DOUBLE PRECISION,
    benchmark_secondary VARCHAR(20) NOT NULL DEFAULT 'SPY',
    benchmark_secondary_return DOUBLE PRECISION,
    benchmark_secondary_cumulative_return DOUBLE PRECISION,
    rolling_beta_primary DOUBLE PRECISION,
    rolling_excess_primary DOUBLE PRECISION,
    holdings JSONB,
    orders JSONB,
    fills JSONB,
    diagnostics JSONB,
    raw_payload JSONB NOT NULL,
    content_hash VARCHAR(64) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    CONSTRAINT uq_strategy_live_snapshot_uid UNIQUE (snapshot_uid)
);

CREATE INDEX IF NOT EXISTS idx_strategy_live_snapshots_strategy_recorded
    ON strategy_live_snapshots (strategy_id, recorded_at DESC);

CREATE INDEX IF NOT EXISTS idx_strategy_live_snapshots_trading_date
    ON strategy_live_snapshots (trading_date DESC);

CREATE INDEX IF NOT EXISTS idx_strategy_live_snapshots_qc_snapshot_id
    ON strategy_live_snapshots (qc_snapshot_id);
