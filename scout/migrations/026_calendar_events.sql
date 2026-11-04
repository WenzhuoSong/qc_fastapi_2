-- Event calendar: scheduled events that can move the market, sectors or holdings (rule dates, the Fed's own
-- schedule, Forex Factory's week, a weekly web search), and the notes written about them (preview before,
-- review after). Point in time: events are upserted (last_seen moves), notes are appended, nothing is deleted.
CREATE TABLE IF NOT EXISTS scout.calendar_events (
    key          VARCHAR(160) PRIMARY KEY,         -- source:kind:date[:slug]
    event_date   DATE         NOT NULL,
    time_et      VARCHAR(5),
    category     VARCHAR(16)  NOT NULL,            -- 宏观数据 | 美联储 | 政治与政策 | 行业与公司 | 市场结构 | 地缘
    kind         VARCHAR(32)  NOT NULL,            -- fomc | election | triple_witching | holiday | ff | web
    title        TEXT         NOT NULL,
    importance   VARCHAR(4)   NOT NULL,            -- 高 | 中
    source       VARCHAR(16)  NOT NULL,            -- rule | fed | ff | web
    detail       JSONB,
    first_seen   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    last_seen    TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_scout_calendar_events_date ON scout.calendar_events (event_date);

CREATE TABLE IF NOT EXISTS scout.event_notes (
    id           BIGSERIAL PRIMARY KEY,
    event_key    VARCHAR(160) NOT NULL,
    note         VARCHAR(16)  NOT NULL,            -- preview | review
    payload      JSONB        NOT NULL,
    pushed       BOOLEAN      NOT NULL DEFAULT false,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_scout_event_notes_key ON scout.event_notes (event_key, note, created_at);
