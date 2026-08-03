-- Rebuildable observer projections. Raw facts remain the authority.

CREATE TABLE IF NOT EXISTS qc_decision_manifests (
    id BIGSERIAL PRIMARY KEY, raw_event_id BIGINT NOT NULL REFERENCES qc_observer_events(id),
    event_id VARCHAR(128) NOT NULL, event_stream_id VARCHAR(128) NOT NULL,
    account_id VARCHAR(128) NOT NULL, deployment_id VARCHAR(128) NOT NULL,
    strategy_id VARCHAR(100), event_sequence BIGINT NOT NULL, event_time TIMESTAMPTZ NOT NULL,
    projection_version VARCHAR(40) NOT NULL, decision_id VARCHAR(128) NOT NULL,
    decision_kind VARCHAR(80) NOT NULL, as_of TIMESTAMPTZ NOT NULL,
    basis_account_snapshot_id VARCHAR(128), market_data_cutoff TIMESTAMPTZ,
    strategy_version VARCHAR(100), execution_style VARCHAR(40),
    execution_authority VARCHAR(40) NOT NULL, raw_target_weights JSONB NOT NULL DEFAULT '{}',
    risk_adjusted_weights JSONB NOT NULL DEFAULT '{}', final_target_weights JSONB NOT NULL DEFAULT '{}',
    suppressed_deltas JSONB NOT NULL DEFAULT '{}', payload_extra JSONB NOT NULL DEFAULT '{}',
    CONSTRAINT uq_qc_decision_manifests_raw_event UNIQUE(raw_event_id),
    CONSTRAINT uq_qc_decision_manifests_stream_decision UNIQUE(event_stream_id, decision_id)
);
CREATE INDEX IF NOT EXISTS ix_qc_decision_manifests_time ON qc_decision_manifests(event_time);
CREATE INDEX IF NOT EXISTS ix_qc_decision_manifests_decision ON qc_decision_manifests(decision_id);

CREATE TABLE IF NOT EXISTS qc_decision_rankings (
    id BIGSERIAL PRIMARY KEY, decision_manifest_id BIGINT NOT NULL REFERENCES qc_decision_manifests(id),
    raw_event_id BIGINT NOT NULL REFERENCES qc_observer_events(id), security_id VARCHAR(128) NOT NULL,
    ticker VARCHAR(32), rank INTEGER NOT NULL, score NUMERIC(24,12), history_hash VARCHAR(128),
    payload_extra JSONB NOT NULL DEFAULT '{}',
    CONSTRAINT uq_qc_decision_rankings_manifest_security UNIQUE(decision_manifest_id, security_id)
);

CREATE TABLE IF NOT EXISTS qc_order_intents (
    id BIGSERIAL PRIMARY KEY, raw_event_id BIGINT NOT NULL REFERENCES qc_observer_events(id),
    event_stream_id VARCHAR(128) NOT NULL, account_id VARCHAR(128) NOT NULL,
    deployment_id VARCHAR(128) NOT NULL, event_time TIMESTAMPTZ NOT NULL,
    projection_version VARCHAR(40) NOT NULL, order_intent_id VARCHAR(128) NOT NULL,
    decision_id VARCHAR(128), security_id VARCHAR(128) NOT NULL, ticker VARCHAR(32),
    requested_quantity NUMERIC(24,8) NOT NULL, submitted_quantity NUMERIC(24,8),
    target_weight NUMERIC(18,10), reference_price NUMERIC(24,8), reference_time TIMESTAMPTZ,
    order_type VARCHAR(40) NOT NULL, reason VARCHAR(100) NOT NULL, risk_class VARCHAR(40) NOT NULL,
    clipped BOOLEAN, payload_extra JSONB NOT NULL DEFAULT '{}',
    CONSTRAINT uq_qc_order_intents_raw_event UNIQUE(raw_event_id),
    CONSTRAINT uq_qc_order_intents_stream_intent UNIQUE(event_stream_id, order_intent_id)
);

CREATE TABLE IF NOT EXISTS qc_order_lifecycle_events (
    id BIGSERIAL PRIMARY KEY, raw_event_id BIGINT NOT NULL REFERENCES qc_observer_events(id) UNIQUE,
    event_stream_id VARCHAR(128) NOT NULL, account_id VARCHAR(128) NOT NULL,
    deployment_id VARCHAR(128) NOT NULL, event_time TIMESTAMPTZ NOT NULL,
    projection_version VARCHAR(40) NOT NULL, broker_order_id VARCHAR(128) NOT NULL,
    order_intent_id VARCHAR(128), security_id VARCHAR(128) NOT NULL, ticker VARCHAR(32),
    status VARCHAR(60) NOT NULL, quantity NUMERIC(24,8), fill_quantity NUMERIC(24,8),
    fill_quantity_semantics VARCHAR(40) NOT NULL, fill_price NUMERIC(24,8), fee NUMERIC(24,8),
    message TEXT, reconstructed BOOLEAN NOT NULL DEFAULT FALSE, reconstruction_source VARCHAR(80),
    remaining_quantity NUMERIC(24,8), payload_extra JSONB NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS qc_local_risk_events (
    id BIGSERIAL PRIMARY KEY, raw_event_id BIGINT NOT NULL REFERENCES qc_observer_events(id) UNIQUE,
    event_stream_id VARCHAR(128) NOT NULL, account_id VARCHAR(128) NOT NULL,
    deployment_id VARCHAR(128) NOT NULL, event_time TIMESTAMPTZ NOT NULL,
    projection_version VARCHAR(40) NOT NULL, risk_event_id VARCHAR(128) NOT NULL,
    security_id VARCHAR(128) NOT NULL, ticker VARCHAR(32), risk_type VARCHAR(60) NOT NULL,
    stage VARCHAR(60) NOT NULL, price NUMERIC(24,8), entry_price NUMERIC(24,8),
    anchor_price NUMERIC(24,8), cooldown_until TIMESTAMPTZ, details JSONB NOT NULL DEFAULT '{}',
    CONSTRAINT uq_qc_local_risk_stream_event UNIQUE(event_stream_id, risk_event_id)
);

CREATE TABLE IF NOT EXISTS qc_account_snapshots (
    id BIGSERIAL PRIMARY KEY, raw_event_id BIGINT NOT NULL REFERENCES qc_observer_events(id) UNIQUE,
    event_stream_id VARCHAR(128) NOT NULL, account_id VARCHAR(128) NOT NULL,
    deployment_id VARCHAR(128) NOT NULL, event_time TIMESTAMPTZ NOT NULL,
    projection_version VARCHAR(40) NOT NULL, account_snapshot_id VARCHAR(128) NOT NULL,
    equity NUMERIC(24,8) NOT NULL, cash NUMERIC(24,8) NOT NULL, buying_power NUMERIC(24,8),
    state_generation BIGINT NOT NULL, state_active BOOLEAN, broker_reconciled BOOLEAN NOT NULL,
    pending_order_count INTEGER NOT NULL DEFAULT 0, deferred_target_count INTEGER NOT NULL DEFAULT 0,
    payload_extra JSONB NOT NULL DEFAULT '{}',
    CONSTRAINT uq_qc_account_snapshots_stream_snapshot UNIQUE(event_stream_id, account_snapshot_id)
);

CREATE TABLE IF NOT EXISTS qc_account_snapshot_holdings (
    id BIGSERIAL PRIMARY KEY, account_snapshot_id BIGINT NOT NULL REFERENCES qc_account_snapshots(id),
    raw_event_id BIGINT NOT NULL REFERENCES qc_observer_events(id), security_id VARCHAR(128) NOT NULL,
    ticker VARCHAR(32), quantity NUMERIC(24,8) NOT NULL, average_price NUMERIC(24,8),
    holdings_value NUMERIC(24,8), payload_extra JSONB NOT NULL DEFAULT '{}',
    CONSTRAINT uq_qc_snapshot_holdings_snapshot_security UNIQUE(account_snapshot_id, security_id)
);

CREATE TABLE IF NOT EXISTS qc_account_snapshot_open_orders (
    id BIGSERIAL PRIMARY KEY, account_snapshot_id BIGINT NOT NULL REFERENCES qc_account_snapshots(id),
    raw_event_id BIGINT NOT NULL REFERENCES qc_observer_events(id), broker_order_id VARCHAR(128) NOT NULL,
    ticker VARCHAR(32), quantity NUMERIC(24,8), quantity_filled NUMERIC(24,8),
    payload_extra JSONB NOT NULL DEFAULT '{}',
    CONSTRAINT uq_qc_snapshot_orders_snapshot_order UNIQUE(account_snapshot_id, broker_order_id)
);
