-- Intraday alerts already sent (one row per event, so a price level or an article is pushed once).
CREATE TABLE IF NOT EXISTS scout.alerts (
    key         VARCHAR(256) PRIMARY KEY,
    kind        VARCHAR(32)  NOT NULL,
    ticker      VARCHAR(16),
    payload     JSONB,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS alerts_created_idx ON scout.alerts (created_at);
