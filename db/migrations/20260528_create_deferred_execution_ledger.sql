-- Execution throttle carryover ledger.

CREATE TABLE IF NOT EXISTS deferred_execution_ledger (
    id BIGSERIAL PRIMARY KEY,
    deferred_id VARCHAR(96) NOT NULL,
    analysis_id BIGINT REFERENCES agent_analysis(id),
    command_id VARCHAR(64),
    source VARCHAR(40) NOT NULL DEFAULT 'execution_throttle',
    status VARCHAR(40) NOT NULL DEFAULT 'open',
    side VARCHAR(10) NOT NULL,
    ticker VARCHAR(20) NOT NULL,
    original_delta DOUBLE PRECISION NOT NULL,
    remaining_delta DOUBLE PRECISION NOT NULL,
    current_weight DOUBLE PRECISION,
    desired_weight DOUBLE PRECISION,
    staged_weight DOUBLE PRECISION,
    latest_current_weight DOUBLE PRECISION,
    latest_desired_weight DOUBLE PRECISION,
    latest_staged_weight DOUBLE PRECISION,
    reason TEXT,
    resolution_reason TEXT,
    review_count INTEGER NOT NULL DEFAULT 0,
    raw_payload JSONB,
    review_payload JSONB,
    resolved_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    updated_at TIMESTAMP NOT NULL DEFAULT now(),
    CONSTRAINT uq_deferred_execution_ledger_id UNIQUE (deferred_id)
);

CREATE INDEX IF NOT EXISTS idx_deferred_execution_ledger_status
    ON deferred_execution_ledger (status);

CREATE INDEX IF NOT EXISTS idx_deferred_execution_ledger_ticker
    ON deferred_execution_ledger (ticker);

CREATE INDEX IF NOT EXISTS idx_deferred_execution_ledger_command_id
    ON deferred_execution_ledger (command_id);

CREATE INDEX IF NOT EXISTS idx_deferred_execution_ledger_analysis_id
    ON deferred_execution_ledger (analysis_id);
