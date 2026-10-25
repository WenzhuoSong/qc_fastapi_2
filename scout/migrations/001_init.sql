-- Scout schema v1. Idempotent; applied by `python -m scout.jobs.migrate`.
CREATE SCHEMA IF NOT EXISTS scout;

CREATE TABLE IF NOT EXISTS scout.instruments (
    ticker      VARCHAR(16) PRIMARY KEY,
    name        TEXT,
    sector      VARCHAR(64),
    industry    VARCHAR(128),
    kind        VARCHAR(8)  NOT NULL DEFAULT 'stock',   -- stock | etf
    in_scan     BOOLEAN     NOT NULL DEFAULT true,
    sources     JSONB,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS scout.watchlist (
    ticker    VARCHAR(16) PRIMARY KEY,
    theme     VARCHAR(64),
    note      TEXT,
    active    BOOLEAN     NOT NULL DEFAULT true,
    added_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS scout.bars_daily (
    ticker  VARCHAR(16) NOT NULL,
    date    DATE        NOT NULL,
    open    DOUBLE PRECISION NOT NULL,
    high    DOUBLE PRECISION NOT NULL,
    low     DOUBLE PRECISION NOT NULL,
    close   DOUBLE PRECISION NOT NULL,
    volume  DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (ticker, date)
);
CREATE INDEX IF NOT EXISTS idx_scout_bars_daily_date ON scout.bars_daily (date);

CREATE TABLE IF NOT EXISTS scout.features_daily (
    ticker   VARCHAR(16) NOT NULL,
    date     DATE        NOT NULL,
    payload  JSONB       NOT NULL,
    PRIMARY KEY (ticker, date)
);
CREATE INDEX IF NOT EXISTS idx_scout_features_daily_date ON scout.features_daily (date);

CREATE TABLE IF NOT EXISTS scout.market_events (
    id          BIGSERIAL PRIMARY KEY,
    ticker      VARCHAR(16) NOT NULL,
    event_type  VARCHAR(32) NOT NULL,          -- earnings
    event_date  DATE        NOT NULL,
    source      VARCHAR(32) NOT NULL,
    payload     JSONB,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_scout_market_events UNIQUE (ticker, event_type, event_date)
);
CREATE INDEX IF NOT EXISTS idx_scout_market_events_date ON scout.market_events (event_date);

CREATE TABLE IF NOT EXISTS scout.detector_base_rates (
    detector     VARCHAR(48) NOT NULL,
    regime       VARCHAR(16) NOT NULL,         -- all | uptrend | mixed | downtrend
    n            INTEGER     NOT NULL,
    stats        JSONB       NOT NULL,
    computed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (detector, regime)
);

CREATE TABLE IF NOT EXISTS scout.opportunities (
    id               VARCHAR(64) PRIMARY KEY,
    ticker           VARCHAR(16) NOT NULL,
    asof             DATE        NOT NULL,
    primary_detector VARCHAR(48) NOT NULL,
    detectors        JSONB       NOT NULL,
    direction        VARCHAR(8)  NOT NULL DEFAULT 'long',
    score            DOUBLE PRECISION NOT NULL,
    status           VARCHAR(24) NOT NULL DEFAULT 'new',
    entry_lo         DOUBLE PRECISION NOT NULL,
    entry_hi         DOUBLE PRECISION NOT NULL,
    stop             DOUBLE PRECISION NOT NULL,
    target1          DOUBLE PRECISION NOT NULL,
    target2          DOUBLE PRECISION,
    horizon_days     INTEGER     NOT NULL,
    risk_reward      DOUBLE PRECISION,
    regime           VARCHAR(16),
    base_rate        JSONB,
    evidence         JSONB       NOT NULL,
    thesis           JSONB,
    thesis_source    VARCHAR(16),
    pushed_at        TIMESTAMPTZ,
    tg_message_id    BIGINT,
    triggered_on     DATE,
    closed_on        DATE,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_scout_opportunities_asof ON scout.opportunities (asof DESC);
CREATE INDEX IF NOT EXISTS idx_scout_opportunities_status ON scout.opportunities (status);
CREATE INDEX IF NOT EXISTS idx_scout_opportunities_ticker ON scout.opportunities (ticker);

CREATE TABLE IF NOT EXISTS scout.opportunity_events (
    id              BIGSERIAL PRIMARY KEY,
    opportunity_id  VARCHAR(64) NOT NULL REFERENCES scout.opportunities(id) ON DELETE CASCADE,
    event_type      VARCHAR(24) NOT NULL,
    event_date      DATE        NOT NULL,
    price           DOUBLE PRECISION,
    payload         JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_scout_opportunity_events UNIQUE (opportunity_id, event_type)
);

CREATE TABLE IF NOT EXISTS scout.opportunity_outcomes (
    opportunity_id  VARCHAR(64) PRIMARY KEY REFERENCES scout.opportunities(id) ON DELETE CASCADE,
    outcome         VARCHAR(24) NOT NULL,      -- hit_target | stopped | expired | expired_untriggered
    entry_price     DOUBLE PRECISION,
    exit_price      DOUBLE PRECISION,
    return_pct      DOUBLE PRECISION,
    spy_return_pct  DOUBLE PRECISION,
    excess_pct      DOUBLE PRECISION,
    mfe_pct         DOUBLE PRECISION,
    mae_pct         DOUBLE PRECISION,
    days_held       INTEGER,
    closed_on       DATE        NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS scout.feedback (
    id              BIGSERIAL PRIMARY KEY,
    opportunity_id  VARCHAR(64) NOT NULL,
    action          VARCHAR(16) NOT NULL,      -- up | down | star | dismiss
    source          VARCHAR(16) NOT NULL DEFAULT 'telegram',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_scout_feedback_opportunity ON scout.feedback (opportunity_id);

CREATE TABLE IF NOT EXISTS scout.briefs (
    id          BIGSERIAL PRIMARY KEY,
    kind        VARCHAR(24) NOT NULL,          -- eod | weekly
    asof        DATE        NOT NULL,
    content     TEXT        NOT NULL,
    payload     JSONB,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS scout.chat_messages (
    id          BIGSERIAL PRIMARY KEY,
    channel     VARCHAR(16) NOT NULL,          -- telegram | dashboard
    role        VARCHAR(16) NOT NULL,          -- user | assistant
    content     TEXT        NOT NULL,
    meta        JSONB,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS scout.job_runs (
    id           BIGSERIAL PRIMARY KEY,
    job          VARCHAR(48) NOT NULL,
    started_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at  TIMESTAMPTZ,
    status       VARCHAR(16) NOT NULL DEFAULT 'running',
    summary      JSONB,
    error        TEXT
);
CREATE INDEX IF NOT EXISTS idx_scout_job_runs_job ON scout.job_runs (job, started_at DESC);
