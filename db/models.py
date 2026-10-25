# db/models.py — tables shared by the news cron (Scout reads them).
from sqlalchemy import (
    BigInteger, Boolean, Column, DateTime, Integer, String, Text, UniqueConstraint, func,
)
from sqlalchemy.dialects.postgresql import JSONB
from db.session import Base


class CronRunLog(Base):
    __tablename__ = "cron_run_log"

    id              = Column(BigInteger, primary_key=True, autoincrement=True)
    job_name        = Column(String(80), nullable=False, index=True)
    started_at      = Column(DateTime, nullable=False, default=func.now())
    finished_at     = Column(DateTime)
    status          = Column(String(20), nullable=False, default="running")  # running|success|failed|skipped
    duration_ms     = Column(Integer)
    rows_written    = Column(Integer, default=0)
    summary         = Column(JSONB)
    error_message   = Column(Text)
    created_at      = Column(DateTime, nullable=False, default=func.now())


# ─────────────────────────────── Agent Step Log ───────────────────────────────


# ─────────────────────────────── News layer ───────────────────────────────


class TickerNewsLibrary(Base):
    """
    Multi-source news + LLM summary + hard risk tagging.
    Written every 2h by cron/pre_fetch_news.py (Finnhub / Alpha Vantage / RSS), auto-cleaned after 48h.
    Read by market_brief and risk_manager.hard_risk_filter.
    """
    __tablename__ = "ticker_news_library"

    id            = Column(BigInteger, primary_key=True, autoincrement=True)
    ticker        = Column(String(10), nullable=False, index=True)
    url           = Column(Text, nullable=False)
    headline      = Column(Text, nullable=False)
    source        = Column(String(100))
    source_api    = Column(String(20), default="finnhub")  # finnhub|alphavantage|rss
    summary       = Column(Text)               # raw summary
    llm_summary   = Column(Text)               # gpt-4o-mini one-sentence impact summary
    sentiment     = Column(String(10))         # positive|negative|neutral
    relevance     = Column(String(15))         # direct|indirect|not_relevant
    is_hard_event = Column(Boolean, default=False)
    hard_risks    = Column(JSONB)              # {risk_type: reason}, output of scan_hard_risks
    category      = Column(String(50))
    related       = Column(JSONB)              # Finnhub related tickers
    datetime_utc  = Column(BigInteger, index=True)  # Unix seconds, used for 48h TTL filtering
    credibility   = Column(Integer)            # 0-100 source credibility
    created_at    = Column(DateTime, nullable=False, default=func.now())

    __table_args__ = (
        UniqueConstraint("ticker", "url", name="uq_ticker_news_url"),
    )


class TickerNewsArchive(Base):
    """Append-only research copy of news removed from the 48-hour live cache."""

    __tablename__ = "ticker_news_archive"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    source_library_id = Column(BigInteger, nullable=False, index=True)
    ticker = Column(String(10), nullable=False, index=True)
    url = Column(Text, nullable=False)
    headline = Column(Text, nullable=False)
    source = Column(String(100))
    source_api = Column(String(20))
    summary = Column(Text)
    llm_summary = Column(Text)
    sentiment = Column(String(10))
    relevance = Column(String(15))
    is_hard_event = Column(Boolean, default=False)
    hard_risks = Column(JSONB)
    category = Column(String(50))
    related = Column(JSONB)
    datetime_utc = Column(BigInteger, nullable=False, index=True)
    credibility = Column(Integer)
    source_created_at = Column(DateTime)
    archived_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    __table_args__ = (
        UniqueConstraint("ticker", "url", name="uq_ticker_news_archive_url"),
    )


class MacroNewsCache(Base):
    """
    Single-row rolling cache: macro news + economic calendar + pre-assembled prose summary.
    Upserted every 2h by cron/pre_fetch_news.py, always keeps only the latest 1 row (key=1).

    Phase 2 new fields:
    - raw_payload: raw news list (all news before structurization)
    - structured_payload: gpt-4o-mini structurized output (macro_signals + ticker_signals)
    """
    __tablename__ = "macro_news_cache"

    id                = Column(Integer, primary_key=True, default=1)
    as_of             = Column(DateTime, nullable=False, default=func.now())
    macro_news        = Column(JSONB)          # list[dict] (from fetch_macro_news)
    economic_calendar = Column(JSONB)          # list[dict] (from fetch_economic_calendar)
    prose_summary     = Column(Text)           # pre-assembled prose for market_brief to read directly
    updated_at        = Column(DateTime, default=func.now(), onupdate=func.now())
    # Phase 2: structurized news preprocessing
    raw_payload        = Column(JSONB, nullable=True)   # raw news list
    structured_payload = Column(JSONB, nullable=True)   # LLM structurized output


# ─────────────────────────────── Memory Layer ───────────────────────────────


# ─────────────────────────────── Earnings Calendar ───────────────────────────────


# ─────────────────────────────── Macro Events Cache ───────────────────────────────


# ─────────────────────────────── Memory Monthly ───────────────────────────────


# ─────────────────────────────── Scenario Analysis ───────────────────────────────


