-- Versioned, append-only calculations over rebuildable QC observer projections.

CREATE TABLE IF NOT EXISTS qc_audit_runs (
    id BIGSERIAL PRIMARY KEY,
    audit_name VARCHAR(100) NOT NULL,
    method_version VARCHAR(40) NOT NULL,
    projection_version VARCHAR(40) NOT NULL,
    projection_cutoff_raw_event_id BIGINT NOT NULL,
    input_hash VARCHAR(64) NOT NULL,
    status VARCHAR(32) NOT NULL,
    summary JSONB NOT NULL DEFAULT '{}',
    error TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    CONSTRAINT uq_qc_audit_runs_method_cutoff
        UNIQUE(audit_name, method_version, projection_cutoff_raw_event_id)
);
CREATE INDEX IF NOT EXISTS ix_qc_audit_runs_name ON qc_audit_runs(audit_name);
CREATE INDEX IF NOT EXISTS ix_qc_audit_runs_cutoff ON qc_audit_runs(projection_cutoff_raw_event_id);
CREATE INDEX IF NOT EXISTS ix_qc_audit_runs_status ON qc_audit_runs(status);

CREATE TABLE IF NOT EXISTS qc_decision_execution_audits (
    id BIGSERIAL PRIMARY KEY,
    audit_run_id BIGINT NOT NULL REFERENCES qc_audit_runs(id),
    decision_manifest_id BIGINT NOT NULL REFERENCES qc_decision_manifests(id),
    event_stream_id VARCHAR(128) NOT NULL,
    decision_id VARCHAR(128) NOT NULL,
    decision_time TIMESTAMPTZ NOT NULL,
    account_snapshot_id BIGINT REFERENCES qc_account_snapshots(id),
    status VARCHAR(40) NOT NULL,
    target_exposure NUMERIC(18,10) NOT NULL,
    actual_exposure NUMERIC(18,10),
    cash_weight NUMERIC(18,10),
    tracking_error_l1 NUMERIC(18,10),
    intent_count INTEGER NOT NULL DEFAULT 0,
    terminal_intent_count INTEGER NOT NULL DEFAULT 0,
    partial_fill_count INTEGER NOT NULL DEFAULT 0,
    rejected_intent_count INTEGER NOT NULL DEFAULT 0,
    clipped_intent_count INTEGER NOT NULL DEFAULT 0,
    aggregate_fill_ratio NUMERIC(18,10),
    total_fees NUMERIC(24,8) NOT NULL DEFAULT 0,
    first_submit_latency_seconds NUMERIC(24,6),
    terminal_latency_seconds NUMERIC(24,6),
    evidence_raw_event_ids JSONB NOT NULL DEFAULT '[]',
    details JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_qc_decision_audit_run_manifest UNIQUE(audit_run_id, decision_manifest_id)
);
CREATE INDEX IF NOT EXISTS ix_qc_decision_audits_stream ON qc_decision_execution_audits(event_stream_id);
CREATE INDEX IF NOT EXISTS ix_qc_decision_audits_decision ON qc_decision_execution_audits(decision_id);
CREATE INDEX IF NOT EXISTS ix_qc_decision_audits_status ON qc_decision_execution_audits(status);

CREATE TABLE IF NOT EXISTS qc_risk_exit_audits (
    id BIGSERIAL PRIMARY KEY,
    audit_run_id BIGINT NOT NULL REFERENCES qc_audit_runs(id),
    local_risk_event_id BIGINT NOT NULL REFERENCES qc_local_risk_events(id),
    event_stream_id VARCHAR(128) NOT NULL,
    risk_event_id VARCHAR(128) NOT NULL,
    security_id VARCHAR(128) NOT NULL,
    ticker VARCHAR(32),
    risk_type VARCHAR(60) NOT NULL,
    risk_time TIMESTAMPTZ NOT NULL,
    order_intent_id BIGINT REFERENCES qc_order_intents(id),
    status VARCHAR(40) NOT NULL,
    intent_latency_seconds NUMERIC(24,6),
    first_fill_latency_seconds NUMERIC(24,6),
    terminal_fill_latency_seconds NUMERIC(24,6),
    requested_quantity NUMERIC(24,8),
    filled_quantity NUMERIC(24,8),
    residual_quantity NUMERIC(24,8),
    residual_notional NUMERIC(24,8),
    evidence_raw_event_ids JSONB NOT NULL DEFAULT '[]',
    details JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_qc_risk_audit_run_event UNIQUE(audit_run_id, local_risk_event_id)
);
CREATE INDEX IF NOT EXISTS ix_qc_risk_audits_stream ON qc_risk_exit_audits(event_stream_id);
CREATE INDEX IF NOT EXISTS ix_qc_risk_audits_event ON qc_risk_exit_audits(risk_event_id);
CREATE INDEX IF NOT EXISTS ix_qc_risk_audits_status ON qc_risk_exit_audits(status);
