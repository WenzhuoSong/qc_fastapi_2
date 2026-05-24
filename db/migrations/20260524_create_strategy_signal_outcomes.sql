-- PR6 signal outcome labels.
-- Outcomes are append-only labels for immutable strategy_frozen_signals.

CREATE TABLE IF NOT EXISTS strategy_signal_outcomes (
    id BIGSERIAL PRIMARY KEY,
    outcome_id VARCHAR(64) NOT NULL,
    signal_id VARCHAR(64) NOT NULL,
    signal_source VARCHAR(40) NOT NULL,
    signal_date DATE NOT NULL,
    label_date DATE NOT NULL,
    strategy_id VARCHAR(100) NOT NULL,
    ticker VARCHAR(20) NOT NULL,
    branch VARCHAR(120),
    action VARCHAR(30) NOT NULL,
    horizon_days INTEGER NOT NULL,
    forward_return DOUBLE PRECISION,
    spy_forward_return DOUBLE PRECISION,
    excess_vs_spy DOUBLE PRECISION,
    drawdown_during_horizon DOUBLE PRECISION,
    spy_drawdown_during_horizon DOUBLE PRECISION,
    target_pool_drawdown DOUBLE PRECISION,
    hit BOOLEAN,
    hit_definition VARCHAR(160) NOT NULL,
    excess_calculation_method VARCHAR(30) NOT NULL,
    outcome_source VARCHAR(40) NOT NULL,
    data_quality VARCHAR(40) NOT NULL,
    content_hash VARCHAR(64) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    CONSTRAINT uq_strategy_signal_outcome_id UNIQUE (outcome_id),
    CONSTRAINT uq_strategy_signal_outcome_signal_horizon_source UNIQUE (
        signal_id,
        horizon_days,
        outcome_source
    )
);

CREATE INDEX IF NOT EXISTS idx_strategy_signal_outcomes_signal_id
    ON strategy_signal_outcomes (signal_id);

CREATE INDEX IF NOT EXISTS idx_strategy_signal_outcomes_label_date
    ON strategy_signal_outcomes (label_date);

CREATE INDEX IF NOT EXISTS idx_strategy_signal_outcomes_strategy
    ON strategy_signal_outcomes (strategy_id);

CREATE INDEX IF NOT EXISTS idx_strategy_signal_outcomes_ticker
    ON strategy_signal_outcomes (ticker);
