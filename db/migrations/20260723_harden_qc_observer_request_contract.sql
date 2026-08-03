-- Request-level replay protection and per-event quarantine for OBS-25/SEC-02.
-- Additive only; raw observer facts and news tables remain unchanged.

CREATE TABLE IF NOT EXISTS qc_observer_request_nonces (
    nonce VARCHAR(128) PRIMARY KEY,
    account_id VARCHAR(128) NOT NULL,
    deployment_id VARCHAR(128) NOT NULL,
    request_timestamp TIMESTAMPTZ NOT NULL,
    body_hash VARCHAR(64) NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_qc_observer_request_nonces_account
    ON qc_observer_request_nonces (account_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_qc_observer_request_nonces_deployment
    ON qc_observer_request_nonces (deployment_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_qc_observer_request_nonces_expiry
    ON qc_observer_request_nonces (expires_at);

CREATE TABLE IF NOT EXISTS qc_observer_rejected_events (
    id BIGSERIAL PRIMARY KEY,
    request_nonce VARCHAR(128),
    event_index INTEGER NOT NULL,
    event_id VARCHAR(128),
    event_stream_id VARCHAR(128),
    event_channel VARCHAR(16),
    event_sequence BIGINT,
    account_id VARCHAR(128),
    deployment_id VARCHAR(128),
    rejection_code VARCHAR(80) NOT NULL,
    rejection_message TEXT NOT NULL,
    raw_content_hash VARCHAR(64) NOT NULL,
    raw_event JSONB NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_qc_observer_rejected_events_request
    ON qc_observer_rejected_events (request_nonce, event_index);
CREATE INDEX IF NOT EXISTS idx_qc_observer_rejected_events_identity
    ON qc_observer_rejected_events (event_id, received_at DESC);
CREATE INDEX IF NOT EXISTS idx_qc_observer_rejected_events_reason
    ON qc_observer_rejected_events (rejection_code, received_at DESC);
