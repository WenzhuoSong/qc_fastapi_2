-- Ordered command lifecycle event trail.

CREATE TABLE IF NOT EXISTS command_lifecycle_events (
    id BIGSERIAL PRIMARY KEY,
    command_id VARCHAR(64) NOT NULL,
    analysis_id BIGINT REFERENCES agent_analysis(id),
    event_type VARCHAR(40) NOT NULL,
    event_status VARCHAR(40),
    event_time TIMESTAMP NOT NULL DEFAULT now(),
    source VARCHAR(40) NOT NULL,
    reason TEXT,
    payload JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_command_lifecycle_events_command_id
    ON command_lifecycle_events (command_id);

CREATE INDEX IF NOT EXISTS idx_command_lifecycle_events_event_type
    ON command_lifecycle_events (event_type);

CREATE INDEX IF NOT EXISTS idx_command_lifecycle_events_event_time
    ON command_lifecycle_events (event_time);
