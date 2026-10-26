-- Daily sector / theme overview (rotation, breadth, participation, heat).
CREATE TABLE IF NOT EXISTS scout.sector_daily (
    asof     DATE        NOT NULL,
    key      VARCHAR(16) NOT NULL,          -- ETF ticker
    kind     VARCHAR(8)  NOT NULL,          -- sector | theme
    payload  JSONB       NOT NULL,
    PRIMARY KEY (asof, key)
);
CREATE INDEX IF NOT EXISTS idx_scout_sector_daily_asof ON scout.sector_daily (asof DESC);
