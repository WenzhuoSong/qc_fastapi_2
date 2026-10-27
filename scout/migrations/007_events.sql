-- Abnormal moves with their researched cause, forward-return base rates after
-- such moves, and cached fundamentals (Finnhub free tier) per ticker.
CREATE TABLE IF NOT EXISTS scout.stock_events (
    ticker      VARCHAR(16) NOT NULL,
    event_date  DATE        NOT NULL,
    metrics     JSONB       NOT NULL,
    cause       JSONB,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (ticker, event_date)
);

CREATE TABLE IF NOT EXISTS scout.event_base_rates (
    bucket       VARCHAR(64) PRIMARY KEY,
    stats        JSONB       NOT NULL,
    computed_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS scout.fundamentals (
    ticker      VARCHAR(16) PRIMARY KEY,
    asof        DATE        NOT NULL,
    payload     JSONB       NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
