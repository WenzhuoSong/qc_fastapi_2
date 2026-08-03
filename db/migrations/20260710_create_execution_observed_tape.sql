-- Observer-only execution tape for QC-owned strategies.
-- FastAPI stores observed orders/fills for audit/TCA; it does not submit,
-- cancel, or mutate orders from these tables.

CREATE TABLE IF NOT EXISTS execution_observed_orders (
    id BIGSERIAL PRIMARY KEY,
    event_uid VARCHAR(180) NOT NULL,
    strategy_live_snapshot_id BIGINT REFERENCES strategy_live_snapshots(id),
    qc_snapshot_id BIGINT REFERENCES qc_snapshots(id),
    snapshot_uid VARCHAR(140),
    strategy_id VARCHAR(100) NOT NULL,
    algorithm_version VARCHAR(80),
    trading_date DATE NOT NULL,
    observed_at TIMESTAMP NOT NULL,
    event_time TIMESTAMP,
    order_id VARCHAR(80),
    ticker VARCHAR(20) NOT NULL,
    direction VARCHAR(12),
    quantity DOUBLE PRECISION,
    status VARCHAR(40),
    order_type VARCHAR(40),
    limit_price DOUBLE PRECISION,
    stop_price DOUBLE PRECISION,
    raw_order JSONB NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    CONSTRAINT uq_execution_observed_order_event_uid UNIQUE (event_uid)
);

CREATE INDEX IF NOT EXISTS idx_execution_observed_orders_strategy ON execution_observed_orders (strategy_id);
CREATE INDEX IF NOT EXISTS idx_execution_observed_orders_trading_date ON execution_observed_orders (trading_date);
CREATE INDEX IF NOT EXISTS idx_execution_observed_orders_event_time ON execution_observed_orders (event_time DESC);
CREATE INDEX IF NOT EXISTS idx_execution_observed_orders_ticker ON execution_observed_orders (ticker);
CREATE INDEX IF NOT EXISTS idx_execution_observed_orders_order_id ON execution_observed_orders (order_id);

CREATE TABLE IF NOT EXISTS execution_observed_fills (
    id BIGSERIAL PRIMARY KEY,
    fill_uid VARCHAR(180) NOT NULL,
    strategy_live_snapshot_id BIGINT REFERENCES strategy_live_snapshots(id),
    qc_snapshot_id BIGINT REFERENCES qc_snapshots(id),
    snapshot_uid VARCHAR(140),
    strategy_id VARCHAR(100) NOT NULL,
    algorithm_version VARCHAR(80),
    trading_date DATE NOT NULL,
    observed_at TIMESTAMP NOT NULL,
    event_time TIMESTAMP,
    order_id VARCHAR(80),
    ticker VARCHAR(20) NOT NULL,
    direction VARCHAR(12),
    quantity DOUBLE PRECISION,
    fill_price DOUBLE PRECISION,
    fee DOUBLE PRECISION,
    notional DOUBLE PRECISION,
    signed_notional DOUBLE PRECISION,
    fee_bps DOUBLE PRECISION,
    raw_fill JSONB NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    CONSTRAINT uq_execution_observed_fill_uid UNIQUE (fill_uid)
);

CREATE INDEX IF NOT EXISTS idx_execution_observed_fills_strategy ON execution_observed_fills (strategy_id);
CREATE INDEX IF NOT EXISTS idx_execution_observed_fills_trading_date ON execution_observed_fills (trading_date);
CREATE INDEX IF NOT EXISTS idx_execution_observed_fills_event_time ON execution_observed_fills (event_time DESC);
CREATE INDEX IF NOT EXISTS idx_execution_observed_fills_ticker ON execution_observed_fills (ticker);
CREATE INDEX IF NOT EXISTS idx_execution_observed_fills_order_id ON execution_observed_fills (order_id);
