-- Unified audit log for Railway cron jobs.
-- This table lets health reports answer which jobs ran, failed, and wrote data.

CREATE TABLE IF NOT EXISTS cron_run_log (
    id BIGSERIAL PRIMARY KEY,
    job_name VARCHAR(80) NOT NULL,
    started_at TIMESTAMP NOT NULL DEFAULT now(),
    finished_at TIMESTAMP,
    status VARCHAR(20) NOT NULL DEFAULT 'running',
    duration_ms INTEGER,
    rows_written INTEGER DEFAULT 0,
    summary JSONB,
    error_message TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_cron_run_log_job_started
ON cron_run_log (job_name, started_at DESC);

CREATE INDEX IF NOT EXISTS ix_cron_run_log_status_started
ON cron_run_log (status, started_at DESC);
