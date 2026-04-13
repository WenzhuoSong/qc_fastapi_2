# db/models.py
import uuid
from datetime import datetime, date
from sqlalchemy import (
    BigInteger, Boolean, Column, Date, DateTime,
    Integer, Numeric, String, Text, ForeignKey, UniqueConstraint, func
)
from sqlalchemy.dialects.postgresql import JSONB
from db.session import Base


class SystemConfig(Base):
    __tablename__ = "system_config"
    key        = Column(String(100), primary_key=True)
    value      = Column(JSONB, nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    updated_by = Column(String(50), default="user")


class QCSnapshot(Base):
    __tablename__ = "qc_snapshots"
    id             = Column(BigInteger, primary_key=True, autoincrement=True)
    received_at    = Column(DateTime, nullable=False, default=func.now())
    trading_date   = Column(Date, nullable=False)
    packet_type    = Column(String(20), nullable=False)  # heartbeat|alert|emergency
    trading_session = Column(String(20))
    schema_version = Column(String(10))
    checksum       = Column(String(32))
    raw_payload    = Column(JSONB, nullable=False)
    is_processed   = Column(Boolean, default=False)
    processed_at   = Column(DateTime)


class PortfolioTimeseries(Base):
    __tablename__ = "portfolio_timeseries"
    id                   = Column(BigInteger, primary_key=True, autoincrement=True)
    snapshot_id          = Column(BigInteger, ForeignKey("qc_snapshots.id"))
    recorded_at          = Column(DateTime, nullable=False)
    total_value          = Column(Numeric(15, 2))
    cash_pct             = Column(Numeric(6, 4))
    daily_pnl_pct        = Column(Numeric(8, 6))
    current_drawdown_pct = Column(Numeric(8, 6))
    sharpe_7d            = Column(Numeric(8, 4))
    sharpe_30d           = Column(Numeric(8, 4))
    regime_label         = Column(String(30))
    vix                  = Column(Numeric(8, 4))
    regime_changed       = Column(Boolean, default=False)


class HoldingsFactor(Base):
    __tablename__ = "holdings_factors"
    id             = Column(BigInteger, primary_key=True, autoincrement=True)
    snapshot_id    = Column(BigInteger, ForeignKey("qc_snapshots.id"))
    recorded_at    = Column(DateTime, nullable=False)
    ticker         = Column(String(10), nullable=False)
    weight_current = Column(Numeric(6, 4))
    weight_target  = Column(Numeric(6, 4))
    weight_drift   = Column(Numeric(6, 4))
    mom_20d        = Column(Numeric(8, 6))
    mom_60d        = Column(Numeric(8, 6))
    mom_252d       = Column(Numeric(8, 6))
    rsi_14         = Column(Numeric(6, 2))
    atr_pct        = Column(Numeric(8, 6))
    bb_position    = Column(Numeric(6, 4))
    hist_vol_20d   = Column(Numeric(8, 6))
    beta_vs_spy    = Column(Numeric(6, 4))
    unrealized_pnl_pct = Column(Numeric(8, 6))
    holding_days   = Column(Integer)


class AlertLog(Base):
    __tablename__ = "alerts_log"
    id           = Column(BigInteger, primary_key=True, autoincrement=True)
    snapshot_id  = Column(BigInteger, ForeignKey("qc_snapshots.id"))
    alert_id     = Column(String(20))
    level        = Column(String(10), nullable=False)  # info|warning|critical
    type         = Column(String(30), nullable=False)
    message      = Column(Text)
    ticker       = Column(String(10))
    value        = Column(Numeric(15, 6))
    threshold    = Column(Numeric(15, 6))
    triggered_at = Column(DateTime, nullable=False)
    is_handled   = Column(Boolean, default=False)
    handled_by   = Column(String(50))
    handled_at   = Column(DateTime)


class AgentAnalysis(Base):
    __tablename__ = "agent_analysis"
    id                 = Column(BigInteger, primary_key=True, autoincrement=True)
    analyzed_at        = Column(DateTime, nullable=False, default=func.now())
    trigger_type       = Column(String(30))  # scheduled|alert|emergency|user
    snapshot_ids       = Column(JSONB)
    planner_output     = Column(JSONB)
    researcher_output  = Column(JSONB)
    allocator_output   = Column(JSONB)
    risk_output        = Column(JSONB)
    risk_approved      = Column(Boolean)
    decision           = Column(JSONB)
    execution_status   = Column(String(40), default="pending")
    executed_at        = Column(DateTime)
    notes              = Column(Text)


class ExecutionLog(Base):
    __tablename__ = "execution_log"
    id              = Column(BigInteger, primary_key=True, autoincrement=True)
    analysis_id     = Column(BigInteger, ForeignKey("agent_analysis.id"))
    executed_at     = Column(DateTime, nullable=False, default=func.now())
    command_type    = Column(String(30))
    command_payload = Column(JSONB, nullable=False)
    qc_response     = Column(JSONB)
    status          = Column(String(20))  # success|failed|timeout
    retry_count     = Column(Integer, default=0)


# ─────────────────────────────── News layer ───────────────────────────────


class TickerNewsLibrary(Base):
    """
    多源新闻 + LLM 摘要 + 硬风险标记。
    由 cron/pre_fetch_news.py 每 2h 写入（Finnhub / Alpha Vantage / RSS），48h 自动清理。
    由 market_brief 和 risk_manager.hard_risk_filter 读取。
    """
    __tablename__ = "ticker_news_library"

    id            = Column(BigInteger, primary_key=True, autoincrement=True)
    ticker        = Column(String(10), nullable=False, index=True)
    url           = Column(Text, nullable=False)
    headline      = Column(Text, nullable=False)
    source        = Column(String(100))
    source_api    = Column(String(20), default="finnhub")  # finnhub|alphavantage|rss
    summary       = Column(Text)               # 原始摘要
    llm_summary   = Column(Text)               # gpt-4o-mini 一句话影响摘要
    sentiment     = Column(String(10))         # positive|negative|neutral
    relevance     = Column(String(15))         # direct|indirect|not_relevant
    is_hard_event = Column(Boolean, default=False)
    hard_risks    = Column(JSONB)              # {risk_type: reason}，scan_hard_risks 输出
    category      = Column(String(50))
    related       = Column(JSONB)              # Finnhub related tickers
    datetime_utc  = Column(BigInteger, index=True)  # Unix 秒，用于 48h TTL 过滤
    credibility   = Column(Integer)            # 0−100 source 可信度
    created_at    = Column(DateTime, nullable=False, default=func.now())

    __table_args__ = (
        UniqueConstraint("ticker", "url", name="uq_ticker_news_url"),
    )


class MacroNewsCache(Base):
    """
    单行滚动缓存：宏观新闻 + 经济日历 + 拼好的 prose 摘要。
    由 cron/pre_fetch_news.py 每 2h upsert，始终只保留最新 1 行（key=1）。
    """
    __tablename__ = "macro_news_cache"

    id                = Column(Integer, primary_key=True, default=1)
    as_of             = Column(DateTime, nullable=False, default=func.now())
    macro_news        = Column(JSONB)          # list[dict] (from fetch_macro_news)
    economic_calendar = Column(JSONB)          # list[dict] (from fetch_economic_calendar)
    prose_summary     = Column(Text)           # 预拼的散文，供 market_brief 直接读
    updated_at        = Column(DateTime, default=func.now(), onupdate=func.now())
