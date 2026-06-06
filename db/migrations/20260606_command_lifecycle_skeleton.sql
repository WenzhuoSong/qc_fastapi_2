-- PR2a-0: command lifecycle skeleton on execution_log.
-- execution_log is the single row per command; command_lifecycle_events remains
-- the append-only event stream.

ALTER TABLE execution_log
    ADD COLUMN IF NOT EXISTS correlation_id VARCHAR(96),
    ADD COLUMN IF NOT EXISTS source_analysis_id BIGINT,
    ADD COLUMN IF NOT EXISTS submitted_at TIMESTAMP,
    ADD COLUMN IF NOT EXISTS policy_version VARCHAR(50),
    ADD COLUMN IF NOT EXISTS target_fingerprint VARCHAR(64),
    ADD COLUMN IF NOT EXISTS lifecycle_state VARCHAR(40) DEFAULT 'created',
    ADD COLUMN IF NOT EXISTS latest_qc_ack_at TIMESTAMP,
    ADD COLUMN IF NOT EXISTS metadata JSONB;

CREATE INDEX IF NOT EXISTS idx_execution_log_correlation_id
    ON execution_log (correlation_id);

CREATE INDEX IF NOT EXISTS idx_execution_log_target_fingerprint
    ON execution_log (target_fingerprint);

CREATE INDEX IF NOT EXISTS idx_execution_log_lifecycle_state
    ON execution_log (lifecycle_state);
