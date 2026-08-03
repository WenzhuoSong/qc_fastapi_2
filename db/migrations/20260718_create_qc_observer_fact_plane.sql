-- Observer-first QC fact plane. Additive only: no legacy or news working-set
-- tables are altered. Raw facts are immutable; corrections are new events.

CREATE TABLE IF NOT EXISTS qc_observer_events (
    id BIGSERIAL PRIMARY KEY,
    event_id VARCHAR(128) NOT NULL,
    event_stream_id VARCHAR(128) NOT NULL,
    event_channel VARCHAR(16) NOT NULL,
    event_sequence BIGINT NOT NULL,
    schema_version VARCHAR(64) NOT NULL,
    event_type VARCHAR(80) NOT NULL,
    account_id VARCHAR(128) NOT NULL,
    deployment_id VARCHAR(128) NOT NULL,
    strategy_id VARCHAR(100),
    event_time TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    source_fingerprint VARCHAR(128),
    policy_fingerprint VARCHAR(128),
    content_hash VARCHAR(64) NOT NULL,
    validation_status VARCHAR(32) NOT NULL DEFAULT 'valid',
    validation_errors JSONB,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_qc_observer_events_event_id UNIQUE (event_id),
    CONSTRAINT uq_qc_observer_events_stream_channel_sequence
        UNIQUE (event_stream_id, event_channel, event_sequence),
    CONSTRAINT ck_qc_observer_events_sequence_positive CHECK (event_sequence > 0)
);

CREATE INDEX IF NOT EXISTS idx_qc_observer_events_stream_channel
    ON qc_observer_events (event_stream_id, event_channel, event_sequence);
CREATE INDEX IF NOT EXISTS idx_qc_observer_events_type_time
    ON qc_observer_events (event_type, event_time DESC);
CREATE INDEX IF NOT EXISTS idx_qc_observer_events_account_time
    ON qc_observer_events (account_id, event_time DESC);
CREATE INDEX IF NOT EXISTS idx_qc_observer_events_deployment
    ON qc_observer_events (deployment_id);
CREATE INDEX IF NOT EXISTS idx_qc_observer_events_strategy_time
    ON qc_observer_events (strategy_id, event_time DESC);
CREATE INDEX IF NOT EXISTS idx_qc_observer_events_validation
    ON qc_observer_events (validation_status, received_at DESC);

CREATE TABLE IF NOT EXISTS qc_observer_stream_receipts (
    id BIGSERIAL PRIMARY KEY,
    event_stream_id VARCHAR(128) NOT NULL,
    event_channel VARCHAR(16) NOT NULL,
    highest_contiguous_sequence BIGINT NOT NULL DEFAULT 0,
    highest_seen_sequence BIGINT NOT NULL DEFAULT 0,
    missing_sequences JSONB NOT NULL DEFAULT '[]'::jsonb,
    last_event_id VARCHAR(128),
    last_event_time TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_qc_observer_stream_receipts_stream_channel
        UNIQUE (event_stream_id, event_channel)
);

CREATE TABLE IF NOT EXISTS qc_projection_checkpoints (
    id BIGSERIAL PRIMARY KEY,
    projection_name VARCHAR(100) NOT NULL,
    projection_version VARCHAR(40) NOT NULL,
    event_stream_id VARCHAR(128) NOT NULL,
    event_channel VARCHAR(16) NOT NULL,
    last_processed_sequence BIGINT NOT NULL DEFAULT 0,
    status VARCHAR(32) NOT NULL DEFAULT 'ready',
    last_error TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_qc_projection_checkpoints_projection_stream_channel
        UNIQUE (projection_name, event_stream_id, event_channel)
);

CREATE TABLE IF NOT EXISTS news_evidence_snapshots (
    id BIGSERIAL PRIMARY KEY,
    snapshot_id VARCHAR(128) NOT NULL,
    raw_event_id BIGINT REFERENCES qc_observer_events(id),
    reference_type VARCHAR(40) NOT NULL,
    reference_id VARCHAR(128) NOT NULL,
    cited_at TIMESTAMPTZ NOT NULL,
    ticker VARCHAR(20),
    article_url TEXT NOT NULL,
    article_title TEXT,
    published_at TIMESTAMPTZ,
    source_name VARCHAR(100),
    credibility NUMERIC(8,6),
    relevance NUMERIC(8,6),
    freshness NUMERIC(8,6),
    content_hash VARCHAR(64) NOT NULL,
    evidence_payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_news_evidence_snapshots_snapshot_id UNIQUE (snapshot_id)
);

CREATE INDEX IF NOT EXISTS idx_news_evidence_snapshots_reference
    ON news_evidence_snapshots (reference_type, reference_id);
CREATE INDEX IF NOT EXISTS idx_news_evidence_snapshots_raw_event
    ON news_evidence_snapshots (raw_event_id);
CREATE INDEX IF NOT EXISTS idx_news_evidence_snapshots_ticker_time
    ON news_evidence_snapshots (ticker, cited_at DESC);

CREATE OR REPLACE FUNCTION reject_qc_observer_event_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'qc_observer_events is append-only; write a correction event instead';
END;
$$;

DROP TRIGGER IF EXISTS trg_qc_observer_events_append_only ON qc_observer_events;
CREATE TRIGGER trg_qc_observer_events_append_only
BEFORE UPDATE OR DELETE ON qc_observer_events
FOR EACH ROW EXECUTE FUNCTION reject_qc_observer_event_mutation();
