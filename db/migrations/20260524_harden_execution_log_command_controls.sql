-- PR7 command-level execution controls.

ALTER TABLE execution_log
    ADD COLUMN IF NOT EXISTS command_id VARCHAR(64),
    ADD COLUMN IF NOT EXISTS qc_status VARCHAR(32) DEFAULT 'submitted',
    ADD COLUMN IF NOT EXISTS qc_ack_at TIMESTAMP,
    ADD COLUMN IF NOT EXISTS qc_rejection_reason TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS uq_execution_log_command_id
    ON execution_log (command_id)
    WHERE command_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_execution_log_analysis_id
    ON execution_log (analysis_id);

CREATE INDEX IF NOT EXISTS idx_execution_log_executed_at
    ON execution_log (executed_at);
