-- The user's own trades (journal) and account settings used for position sizing.
CREATE TABLE IF NOT EXISTS scout.user_settings (
    key         VARCHAR(48) PRIMARY KEY,
    value       JSONB       NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS scout.trades (
    id              BIGSERIAL PRIMARY KEY,
    ticker          VARCHAR(16) NOT NULL,
    opportunity_id  VARCHAR(64) REFERENCES scout.opportunities(id) ON DELETE SET NULL,
    opened_on       DATE        NOT NULL,
    entry_price     DOUBLE PRECISION NOT NULL,
    shares          DOUBLE PRECISION NOT NULL,
    stop            DOUBLE PRECISION,
    target          DOUBLE PRECISION,
    closed_on       DATE,
    exit_price      DOUBLE PRECISION,
    note            TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS trades_open_idx ON scout.trades (closed_on) WHERE closed_on IS NULL;
