CREATE TABLE IF NOT EXISTS ticker_news_archive (
    id BIGSERIAL PRIMARY KEY,
    source_library_id BIGINT NOT NULL,
    ticker VARCHAR(10) NOT NULL,
    url TEXT NOT NULL,
    headline TEXT NOT NULL,
    source VARCHAR(100),
    source_api VARCHAR(20),
    summary TEXT,
    llm_summary TEXT,
    sentiment VARCHAR(10),
    relevance VARCHAR(15),
    is_hard_event BOOLEAN DEFAULT FALSE,
    hard_risks JSONB,
    category VARCHAR(50),
    related JSONB,
    datetime_utc BIGINT NOT NULL,
    credibility INTEGER,
    source_created_at TIMESTAMP,
    archived_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_ticker_news_archive_url UNIQUE (ticker, url)
);

CREATE INDEX IF NOT EXISTS idx_ticker_news_archive_source_library
    ON ticker_news_archive (source_library_id);
CREATE INDEX IF NOT EXISTS idx_ticker_news_archive_ticker_time
    ON ticker_news_archive (ticker, datetime_utc);
CREATE INDEX IF NOT EXISTS idx_ticker_news_archive_source_api
    ON ticker_news_archive (source_api);
