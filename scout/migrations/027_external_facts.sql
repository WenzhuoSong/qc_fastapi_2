-- External facts: what web searches found that Scout's own data does not hold (analyst actions, guidance, contracts,
-- regulation, policy …), one row per fact with its source, a verbatim quote and whether Scout found that quote on the
-- page itself. Point in time and append-only: a fact seen again only moves last_seen; a later change is a new row.
CREATE TABLE IF NOT EXISTS scout.external_facts (
    id           BIGSERIAL PRIMARY KEY,
    fingerprint  VARCHAR(64)  NOT NULL UNIQUE,     -- subject + url + quote
    subject      VARCHAR(32)  NOT NULL,            -- ticker, sector ETF, or MACRO
    subject_kind VARCHAR(8)   NOT NULL,            -- stock | sector | macro
    fact_type    VARCHAR(24)  NOT NULL,
    claim        TEXT         NOT NULL,            -- Scout's own one-sentence summary (Chinese)
    value        DOUBLE PRECISION,
    unit         VARCHAR(24),
    quote        TEXT         NOT NULL,            -- one verbatim sentence from the source, nothing more
    url          TEXT         NOT NULL,
    outlet       VARCHAR(120),
    published    DATE,
    status       VARCHAR(12)  NOT NULL,            -- verified | unreachable | mismatch
    match        VARCHAR(8),                       -- exact | close (when verified)
    purpose      VARCHAR(48),                      -- the workflow whose search found it
    first_seen   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    last_seen    TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_scout_external_facts_subject ON scout.external_facts (subject, published DESC);
CREATE INDEX IF NOT EXISTS idx_scout_external_facts_seen ON scout.external_facts (first_seen);
