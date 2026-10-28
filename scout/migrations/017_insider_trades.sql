-- Open-market insider trades (Form 4 codes P = purchase, S = sale) from the SEC's quarterly
-- Insider Transactions Data Sets, for the companies in the universe. Kept as filed; the filing
-- date is when the market could know.
CREATE TABLE IF NOT EXISTS scout.insider_trades (
    accession     VARCHAR(32)  NOT NULL,
    trans_sk      BIGINT       NOT NULL,          -- the data set's row key within the filing
    ticker        VARCHAR(16)  NOT NULL,
    issuer_cik    VARCHAR(10)  NOT NULL,
    filed         DATE         NOT NULL,
    trans_date    DATE,
    code          CHAR(1)      NOT NULL,          -- P | S
    security      TEXT,                           -- as filed, e.g. Common Stock, Class A Common Stock, Series A Preferred
    common        BOOLEAN      NOT NULL,          -- common / ordinary shares (preferred, notes and units are not)
    shares        DOUBLE PRECISION,
    price         DOUBLE PRECISION,
    value         DOUBLE PRECISION,
    owner_cik     VARCHAR(10),
    owner_name    TEXT,
    relationship  TEXT,                           -- Director, Officer, TenPercentOwner, Other (as filed)
    title         TEXT,
    plan_10b5_1   BOOLEAN,                        -- flagged on the filing (SEC field from 2023; null before)
    PRIMARY KEY (accession, trans_sk)
);
CREATE INDEX IF NOT EXISTS insider_trades_ticker_idx ON scout.insider_trades (ticker, filed);

-- Which bulk files were loaded, so reruns fetch only what is missing or may have changed.
CREATE TABLE IF NOT EXISTS scout.data_loads (
    source     VARCHAR(32) NOT NULL,
    period     VARCHAR(16) NOT NULL,
    rows       INTEGER     NOT NULL,
    loaded_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (source, period)
);
