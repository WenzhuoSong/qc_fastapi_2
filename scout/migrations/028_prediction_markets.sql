-- Prediction markets (Polymarket, Kalshi; read-only public data): the events Scout follows, why (a calendar event,
-- a macro topic, a holding, or found by the daily scan), and point-in-time snapshots of every outcome's price.
-- Append-only snapshots; an event stops being refreshed when it closes, nothing is deleted.
CREATE TABLE IF NOT EXISTS scout.prediction_events (
    key          VARCHAR(80)  PRIMARY KEY,         -- polymarket:<id> | kalshi:<event ticker>
    source       VARCHAR(12)  NOT NULL,
    title        TEXT         NOT NULL,
    url          TEXT,
    end_date     DATE,
    topic        VARCHAR(12)  NOT NULL,            -- calendar | macro | holding | discovery
    link         VARCHAR(160),                     -- calendar event key, macro topic id, or ticker
    sectors      TEXT[]       NOT NULL DEFAULT '{}',
    tickers      TEXT[]       NOT NULL DEFAULT '{}',
    rules        TEXT,
    relevance    JSONB,                            -- the relevance check (why it matters), once per event
    active       BOOLEAN      NOT NULL DEFAULT true,
    first_seen   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    last_seen    TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_scout_prediction_events_link ON scout.prediction_events (link);

CREATE TABLE IF NOT EXISTS scout.prediction_snapshots (
    id           BIGSERIAL PRIMARY KEY,
    event_key    VARCHAR(80)  NOT NULL,
    outcome_id   VARCHAR(80)  NOT NULL,
    label        TEXT         NOT NULL,
    prob         DOUBLE PRECISION,
    volume       DOUBLE PRECISION,
    volume_24h   DOUBLE PRECISION,
    event_volume DOUBLE PRECISION,
    captured_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_scout_prediction_snapshots_key ON scout.prediction_snapshots (event_key, outcome_id, captured_at);
