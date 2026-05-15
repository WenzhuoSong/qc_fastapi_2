-- Research/backfill feature store for non-authoritative daily market data.
-- yfinance rows must never overwrite QC snapshots or execution-state records.

CREATE TABLE IF NOT EXISTS market_daily_features (
    id BIGSERIAL PRIMARY KEY,
    trading_date DATE NOT NULL,
    ticker VARCHAR(20) NOT NULL,
    source VARCHAR(30) NOT NULL DEFAULT 'yfinance',
    open_price NUMERIC(15,4),
    high_price NUMERIC(15,4),
    low_price NUMERIC(15,4),
    close_price NUMERIC(15,4),
    adj_close_price NUMERIC(15,4),
    volume BIGINT,
    dollar_volume NUMERIC(20,2),
    return_1d NUMERIC(8,6),
    return_5d NUMERIC(8,6),
    return_20d NUMERIC(8,6),
    return_60d NUMERIC(8,6),
    return_252d NUMERIC(8,6),
    sma_20 NUMERIC(15,4),
    sma_50 NUMERIC(15,4),
    sma_200 NUMERIC(15,4),
    hist_vol_20d NUMERIC(8,6),
    rsi_14 NUMERIC(6,2),
    atr_pct NUMERIC(8,6),
    bb_position NUMERIC(6,4),
    data_quality_flag VARCHAR(40) DEFAULT 'ok',
    raw_payload JSONB,
    created_at TIMESTAMP DEFAULT now() NOT NULL,
    updated_at TIMESTAMP DEFAULT now() NOT NULL,
    CONSTRAINT uq_market_daily_feature_date_ticker_source
        UNIQUE (trading_date, ticker, source)
);

CREATE INDEX IF NOT EXISTS ix_market_daily_features_ticker_date
ON market_daily_features (ticker, trading_date DESC);

CREATE INDEX IF NOT EXISTS ix_market_daily_features_date_source
ON market_daily_features (trading_date DESC, source);

ALTER TABLE market_daily_features
ADD COLUMN IF NOT EXISTS rsi_14 NUMERIC(6,2),
ADD COLUMN IF NOT EXISTS atr_pct NUMERIC(8,6),
ADD COLUMN IF NOT EXISTS bb_position NUMERIC(6,4);
