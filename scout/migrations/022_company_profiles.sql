-- Company profiles from the annual report's business section (10-K Item 1 / 20-F Item 4), point in time:
-- the source text per filing, every profile version, and the relations it names (customers, suppliers,
-- competitors, partners). A new annual report adds rows; nothing is overwritten.
CREATE TABLE IF NOT EXISTS scout.company_docs (
    accession     VARCHAR(32) PRIMARY KEY,
    ticker        VARCHAR(16) NOT NULL,
    cik           VARCHAR(10) NOT NULL,
    form          VARCHAR(16) NOT NULL,
    filed         DATE        NOT NULL,
    report_date   DATE,
    url           TEXT,
    section       TEXT,                        -- the business section as plain text
    snippets      JSONB,                       -- customer-concentration sentences found elsewhere in the filing
    chars         INTEGER,
    status        VARCHAR(16) NOT NULL,        -- ok | no_section | fetch_failed
    fetched_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS company_docs_ticker_idx ON scout.company_docs (ticker, filed DESC);

CREATE TABLE IF NOT EXISTS scout.company_profiles (
    id            BIGSERIAL PRIMARY KEY,
    ticker        VARCHAR(16) NOT NULL,
    accession     VARCHAR(32) NOT NULL,
    filed         DATE        NOT NULL,
    profile       JSONB       NOT NULL,        -- validated items only, each with its quote from the filing
    validation    JSONB,                       -- kept / dropped counts and the reasons
    model         VARCHAR(48),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (accession)
);
CREATE INDEX IF NOT EXISTS company_profiles_ticker_idx ON scout.company_profiles (ticker, filed DESC);

CREATE TABLE IF NOT EXISTS scout.company_relations (
    id             BIGSERIAL PRIMARY KEY,
    ticker         VARCHAR(16) NOT NULL,       -- the company whose filing names the other
    accession      VARCHAR(32) NOT NULL,
    filed          DATE        NOT NULL,
    relation       VARCHAR(16) NOT NULL,       -- customer | supplier | competitor | partner
    name           TEXT        NOT NULL,       -- as named in the filing
    related_ticker VARCHAR(16),                -- matched to a Scout instrument, else NULL
    detail         TEXT,                       -- share of revenue / what is supplied, as disclosed
    quote          TEXT
);
CREATE INDEX IF NOT EXISTS company_relations_ticker_idx ON scout.company_relations (ticker, filed DESC);
CREATE INDEX IF NOT EXISTS company_relations_related_idx ON scout.company_relations (related_ticker);
