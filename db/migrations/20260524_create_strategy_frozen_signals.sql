CREATE TABLE IF NOT EXISTS strategy_frozen_signals (
    id BIGSERIAL PRIMARY KEY,
    signal_id VARCHAR(64) NOT NULL,
    signal_source VARCHAR(40) NOT NULL,
    signal_date DATE NOT NULL,
    generated_at TIMESTAMP NOT NULL,
    tradable_from_date DATE NOT NULL,
    strategy_id VARCHAR(100) NOT NULL,
    strategy_version VARCHAR(20),
    ticker VARCHAR(20) NOT NULL,
    role VARCHAR(50),
    branch VARCHAR(120),
    action VARCHAR(30) NOT NULL,
    signal_type VARCHAR(80),
    confidence DOUBLE PRECISION,
    raw_score DOUBLE PRECISION,
    normalized_score DOUBLE PRECISION,
    max_reasonable_weight DOUBLE PRECISION,
    risk_budget_cost DOUBLE PRECISION,
    feature_data_date DATE,
    data_lag_days INTEGER,
    feature_source VARCHAR(40),
    feature_authority VARCHAR(40),
    regime_at_signal VARCHAR(50),
    vix_at_signal DOUBLE PRECISION,
    evidence_contract_version VARCHAR(20),
    diagnostics JSONB,
    content_hash VARCHAR(64) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    CONSTRAINT uq_strategy_frozen_signal_id UNIQUE (signal_id)
);

CREATE INDEX IF NOT EXISTS idx_strategy_frozen_signals_date
ON strategy_frozen_signals (signal_date);

CREATE INDEX IF NOT EXISTS idx_strategy_frozen_signals_strategy
ON strategy_frozen_signals (strategy_id);

CREATE INDEX IF NOT EXISTS idx_strategy_frozen_signals_ticker
ON strategy_frozen_signals (ticker);
