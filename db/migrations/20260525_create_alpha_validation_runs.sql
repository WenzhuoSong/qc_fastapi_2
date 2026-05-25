-- Persistent alpha validation snapshots.

CREATE TABLE IF NOT EXISTS alpha_validation_runs (
    id BIGSERIAL PRIMARY KEY,
    analysis_id BIGINT REFERENCES agent_analysis(id),
    generated_at TIMESTAMP NOT NULL DEFAULT now(),
    analyzed_at TIMESTAMP,
    trigger_type VARCHAR(30),
    risk_approved BOOLEAN,
    execution_status VARCHAR(40),
    status VARCHAR(40) NOT NULL,
    data_quality VARCHAR(40) NOT NULL,
    cost_gate_status VARCHAR(40),
    low_edge_trade_count INTEGER NOT NULL DEFAULT 0,
    min_edge_to_cost_ratio DOUBLE PRECISION,
    avg_edge_to_cost_ratio DOUBLE PRECISION,
    var_95_loss DOUBLE PRECISION,
    cvar_95_loss DOUBLE PRECISION,
    max_scenario_loss DOUBLE PRECISION,
    signal_weighted_effective_n DOUBLE PRECISION,
    signal_alignment_score DOUBLE PRECISION,
    signal_objective_warning_count INTEGER NOT NULL DEFAULT 0,
    independent_alpha_family_count INTEGER NOT NULL DEFAULT 0,
    actionable_alpha_strategy_count INTEGER NOT NULL DEFAULT 0,
    calibrated_conviction_count INTEGER NOT NULL DEFAULT 0,
    early_conviction_count INTEGER NOT NULL DEFAULT 0,
    insufficient_conviction_count INTEGER NOT NULL DEFAULT 0,
    warnings JSONB,
    diagnostic_payload JSONB,
    content_hash VARCHAR(64) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    CONSTRAINT uq_alpha_validation_runs_analysis_id UNIQUE (analysis_id)
);

CREATE INDEX IF NOT EXISTS idx_alpha_validation_runs_generated_at
    ON alpha_validation_runs (generated_at DESC);

CREATE INDEX IF NOT EXISTS idx_alpha_validation_runs_status_generated_at
    ON alpha_validation_runs (status, generated_at DESC);

CREATE INDEX IF NOT EXISTS idx_alpha_validation_runs_analysis_id
    ON alpha_validation_runs (analysis_id);
