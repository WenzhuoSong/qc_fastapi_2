-- Persistent observe-only validation observations.
--
-- This table is the durable data loop for strategy/basket/hedge/execution
-- calibration. Rows are recommendation-only and never grant execution
-- authority.

CREATE TABLE IF NOT EXISTS validation_observations (
    id BIGSERIAL PRIMARY KEY,
    observation_id VARCHAR(120) NOT NULL,
    observation_type VARCHAR(40) NOT NULL,
    analysis_id BIGINT REFERENCES agent_analysis(id),
    command_id VARCHAR(64),
    observed_at TIMESTAMP NOT NULL,
    observation_date DATE NOT NULL,
    horizon_days INTEGER,
    maturity_date DATE,
    status VARCHAR(40) NOT NULL,
    execution_authority VARCHAR(40) NOT NULL DEFAULT 'none',
    target_weight_mutation VARCHAR(40) NOT NULL DEFAULT 'none',
    observation_payload JSONB NOT NULL,
    outcome_payload JSONB,
    metrics JSONB,
    recommendation JSONB,
    content_hash VARCHAR(64) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    updated_at TIMESTAMP NOT NULL DEFAULT now(),
    CONSTRAINT uq_validation_observation_id UNIQUE (observation_id)
);

CREATE INDEX IF NOT EXISTS idx_validation_observations_type_status
    ON validation_observations (observation_type, status);

CREATE INDEX IF NOT EXISTS idx_validation_observations_observed_at
    ON validation_observations (observed_at DESC);

CREATE INDEX IF NOT EXISTS idx_validation_observations_maturity
    ON validation_observations (maturity_date, status);

CREATE INDEX IF NOT EXISTS idx_validation_observations_analysis_id
    ON validation_observations (analysis_id);

CREATE INDEX IF NOT EXISTS idx_validation_observations_command_id
    ON validation_observations (command_id);
