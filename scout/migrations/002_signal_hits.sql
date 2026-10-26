-- Every rule hit of every scan (live and candidate rules), for the strategy
-- screener. Opportunities keep only the curated Top N; this keeps them all.
CREATE TABLE IF NOT EXISTS scout.signal_hits (
    asof      DATE        NOT NULL,
    detector  VARCHAR(48) NOT NULL,
    ticker    VARCHAR(16) NOT NULL,
    score     DOUBLE PRECISION,
    payload   JSONB       NOT NULL,
    PRIMARY KEY (asof, detector, ticker)
);
CREATE INDEX IF NOT EXISTS idx_scout_signal_hits_asof ON scout.signal_hits (asof DESC);
