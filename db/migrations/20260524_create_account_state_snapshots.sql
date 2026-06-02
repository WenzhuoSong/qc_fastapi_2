-- Store normalized QC account-state facts for execution guard development.

CREATE TABLE IF NOT EXISTS account_state_snapshots (
    id BIGSERIAL PRIMARY KEY,
    qc_snapshot_id BIGINT REFERENCES qc_snapshots(id),
    recorded_at TIMESTAMP NOT NULL,
    account_timestamp TIMESTAMP,
    source_packet_type VARCHAR(40) NOT NULL,
    contract_version VARCHAR(20) NOT NULL,
    account_status VARCHAR(40),
    data_status VARCHAR(40),
    policy_version VARCHAR(50),
    total_value NUMERIC(15,2),
    cash NUMERIC(15,2),
    cash_pct NUMERIC(8,6),
    buying_power NUMERIC(15,2),
    open_order_count INTEGER,
    has_open_orders BOOLEAN,
    is_market_open BOOLEAN,
    last_command_id VARCHAR(64),
    active_command_id VARCHAR(64),
    active_execution_status VARCHAR(32),
    processed_command_count INTEGER DEFAULT 0,
    holdings_weights JSONB,
    target_weights JSONB,
    raw_snapshot JSONB NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_account_state_snapshots_qc_snapshot_id
    ON account_state_snapshots (qc_snapshot_id);

CREATE INDEX IF NOT EXISTS idx_account_state_snapshots_recorded_at
    ON account_state_snapshots (recorded_at);

ALTER TABLE account_state_snapshots
    ADD COLUMN IF NOT EXISTS last_command_id VARCHAR(64);

ALTER TABLE account_state_snapshots
    ADD COLUMN IF NOT EXISTS active_command_id VARCHAR(64);

ALTER TABLE account_state_snapshots
    ADD COLUMN IF NOT EXISTS active_execution_status VARCHAR(32);

ALTER TABLE account_state_snapshots
    ADD COLUMN IF NOT EXISTS processed_command_count INTEGER DEFAULT 0;
