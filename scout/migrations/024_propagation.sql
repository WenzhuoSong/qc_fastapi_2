-- News passed along the supply chain: a material article about one company, and what it may mean for the
-- companies its annual report (or theirs) names as customers, suppliers, competitors, partners. An inference,
-- recorded with the target's move that day and scored in the judgment ledger (kind 'propagation').
CREATE TABLE IF NOT EXISTS scout.propagation_signals (
    id             BIGSERIAL PRIMARY KEY,
    asof           DATE        NOT NULL,           -- the session whose close is the entry price
    source_ticker  VARCHAR(16) NOT NULL,
    news_url       TEXT        NOT NULL,
    headline       TEXT        NOT NULL,
    target_ticker  VARCHAR(16) NOT NULL,
    relation       VARCHAR(32) NOT NULL,           -- the target seen from the source: supplier, customer, competitor, partner
    impact         SMALLINT    NOT NULL,           -- +1 good for the target, −1 bad
    mechanism      TEXT        NOT NULL,
    relation_quote TEXT,
    confidence     VARCHAR(8),
    reaction       JSONB,                          -- the target's ret_1d / own_ret_1d on asof: priced already?
    model          VARCHAR(48),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (news_url, target_ticker)
);
CREATE INDEX IF NOT EXISTS propagation_target_idx ON scout.propagation_signals (target_ticker, asof DESC);
CREATE INDEX IF NOT EXISTS propagation_asof_idx ON scout.propagation_signals (asof DESC);
