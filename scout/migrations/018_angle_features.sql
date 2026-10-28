-- v3 multi-angle features per stock per data date (point in time, appended daily, never pruned).
CREATE TABLE IF NOT EXISTS scout.angle_features (
    ticker   VARCHAR(16) NOT NULL,
    asof     DATE        NOT NULL,
    payload  JSONB       NOT NULL,
    PRIMARY KEY (ticker, asof)
);
CREATE INDEX IF NOT EXISTS angle_features_asof_idx ON scout.angle_features (asof DESC);
