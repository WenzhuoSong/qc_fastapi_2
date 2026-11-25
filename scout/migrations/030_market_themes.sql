-- Market themes (user's request 2026-11-26: the market read should remember): each driver of the daily read belongs to a
-- theme; a theme keeps when it started, the days it was a driver, its anchors (the symbols its evidence named) and its
-- daily entries. Not seen for 10 sessions = faded; seen again = active again. Nothing is deleted.
CREATE TABLE IF NOT EXISTS scout.market_themes (
    id          BIGSERIAL PRIMARY KEY,
    title       VARCHAR(40) NOT NULL UNIQUE,
    first_seen  DATE        NOT NULL,
    last_seen   DATE        NOT NULL,
    days        INTEGER     NOT NULL DEFAULT 1,
    status      VARCHAR(10) NOT NULL DEFAULT 'active',     -- active | faded
    anchors     JSONB       NOT NULL DEFAULT '[]',
    entries     JSONB       NOT NULL DEFAULT '[]',          -- [{date, strength, explanation}]
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
