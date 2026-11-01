-- Trades described in a Telegram message: parsed, checked, and written to the journal only after the user confirms.
CREATE TABLE IF NOT EXISTS scout.trade_drafts (
    id          BIGSERIAL PRIMARY KEY,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    raw         TEXT        NOT NULL,
    parsed      JSONB       NOT NULL,
    checks      JSONB,
    status      VARCHAR(12) NOT NULL DEFAULT 'pending',   -- pending | confirmed | cancelled | expired
    trade_id    BIGINT,
    decided_at  TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS trade_drafts_status_idx ON scout.trade_drafts (status, created_at DESC);
