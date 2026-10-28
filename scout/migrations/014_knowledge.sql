-- v3 P1: the knowledge base. One row per version of an entry; every status
-- change or evidence update adds a version, nothing is overwritten.
CREATE TABLE IF NOT EXISTS scout.knowledge (
    id            BIGSERIAL PRIMARY KEY,
    key           VARCHAR(64)  NOT NULL,
    version       INTEGER      NOT NULL,
    kind          VARCHAR(16)  NOT NULL,           -- rule | feature | regularity | method | judgment
    polarity      VARCHAR(8)   NOT NULL,           -- positive (it works) | negative (it does not)
    status        VARCHAR(16)  NOT NULL,           -- hypothesis | backtested | forward | confirmed | rejected | retired
    title         VARCHAR(120) NOT NULL,
    claim         TEXT         NOT NULL,
    mechanism     TEXT,
    context       JSONB        NOT NULL DEFAULT '{}'::jsonb,   -- conditions it holds under (regime, kind, horizon)
    applies       JSONB        NOT NULL DEFAULT '{}'::jsonb,   -- what it is about: detectors, lenses, topics
    evidence      JSONB        NOT NULL DEFAULT '[]'::jsonb,   -- [{source, date, summary, stats}]
    invalidation  TEXT,
    recheck_on    DATE,
    supersedes    VARCHAR(64),
    changed_by    VARCHAR(16)  NOT NULL,           -- seed | system | user
    note          TEXT,                            -- why this version exists
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    UNIQUE (key, version)
);
CREATE INDEX IF NOT EXISTS knowledge_key_idx ON scout.knowledge (key, version DESC);
