-- Scout v2 phase 1: research data, stored point in time (append, never overwrite
-- history) so later evaluation sees what the system knew on each day.

-- SEC identity and bookkeeping for each company in the research pool.
CREATE TABLE IF NOT EXISTS scout.sec_companies (
    ticker          VARCHAR(16) PRIMARY KEY,
    cik             VARCHAR(10) NOT NULL,
    name            TEXT,
    facts_accession VARCHAR(32),             -- last 10-Q/10-K whose XBRL facts were loaded
    facts_fetched_at TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Quarterly financials from EDGAR XBRL, one row per value as filed; a restatement
-- adds a row with a later `filed` date instead of replacing the earlier one.
CREATE TABLE IF NOT EXISTS scout.financials (
    ticker        VARCHAR(16) NOT NULL,
    metric        VARCHAR(48) NOT NULL,
    period_end    DATE        NOT NULL,
    period_start  DATE,
    value         DOUBLE PRECISION NOT NULL,
    fy            INTEGER,
    fp            VARCHAR(4),
    form          VARCHAR(16),
    filed         DATE        NOT NULL,
    derived       BOOLEAN     NOT NULL DEFAULT false,   -- e.g. Q4 = FY − 9M, cash flow from YTD differences
    concept       VARCHAR(128),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (ticker, metric, period_end, filed)
);

CREATE TABLE IF NOT EXISTS scout.sec_filings (
    accession     VARCHAR(32) PRIMARY KEY,
    ticker        VARCHAR(16) NOT NULL,
    cik           VARCHAR(10) NOT NULL,
    form          VARCHAR(16) NOT NULL,
    filed         DATE        NOT NULL,
    report_date   DATE,
    items         TEXT,                       -- 8-K items, e.g. "2.02,9.01" (2.02 = results of operations)
    url           TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS sec_filings_ticker_idx ON scout.sec_filings (ticker, filed DESC);

-- Daily research snapshots per ticker: estimates, ownership, options (kind).
CREATE TABLE IF NOT EXISTS scout.research_snapshots (
    ticker      VARCHAR(16) NOT NULL,
    asof        DATE        NOT NULL,
    kind        VARCHAR(24) NOT NULL,
    source      VARCHAR(24) NOT NULL,
    payload     JSONB       NOT NULL,
    fetched_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (ticker, asof, kind)
);

-- Rating and price-target changes, one row per firm action.
CREATE TABLE IF NOT EXISTS scout.analyst_actions (
    ticker        VARCHAR(16) NOT NULL,
    action_at     TIMESTAMPTZ NOT NULL,
    firm          TEXT        NOT NULL,
    to_grade      TEXT,
    from_grade    TEXT,
    action        VARCHAR(16),
    pt_action     VARCHAR(24),
    price_target  DOUBLE PRECISION,
    prior_target  DOUBLE PRECISION,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (ticker, action_at, firm)
);

CREATE TABLE IF NOT EXISTS scout.macro_series (
    series  VARCHAR(32) NOT NULL,
    date    DATE        NOT NULL,
    value   DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (series, date)
);

-- Data-quality findings (cross-checks, stale or failing sources).
CREATE TABLE IF NOT EXISTS scout.data_quality (
    id          BIGSERIAL PRIMARY KEY,
    asof        DATE        NOT NULL,
    check_name  VARCHAR(48) NOT NULL,
    ticker      VARCHAR(16),
    detail      JSONB,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS data_quality_asof_idx ON scout.data_quality (asof DESC);
