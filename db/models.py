# db/models.py
import uuid
from datetime import datetime, date
from sqlalchemy import (
    BigInteger, Boolean, Column, Date, DateTime,
    Float, Integer, Numeric, String, Text, ForeignKey, UniqueConstraint, func
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
    alert_id     = Column(String(60))
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


# ─────────────────────────────── Agent Step Log ───────────────────────────────


class AgentStepLog(Base):
    """
    Input/output log for each pipeline stage.
    Each pipeline run produces 7-8 records (brief -> researcher -> bull -> bear -> synthesizer -> risk -> communicator).
    Used for post-hoc debugging and analyzing decision chains.
    """
    __tablename__ = "agent_step_log"

    id            = Column(BigInteger, primary_key=True, autoincrement=True)
    analysis_id   = Column(BigInteger, ForeignKey("agent_analysis.id"), nullable=False, index=True)
    stage         = Column(String(30), nullable=False)   # e.g. "1_brief", "3_researcher", "4a_bull"
    agent_name    = Column(String(30), nullable=False)   # e.g. "market_brief", "researcher", "bull"
    input_data    = Column(JSONB)                        # this agent's input (can be large: brief/research_report)
    output_data   = Column(JSONB)                        # this agent's output
    duration_ms   = Column(Integer)                      # duration in milliseconds
    model         = Column(String(40))                   # LLM model name, null for Python stages
    token_usage   = Column(JSONB)                        # {"prompt_tokens": N, "completion_tokens": N}
    failed        = Column(Boolean, default=False)       # whether degraded
    created_at    = Column(DateTime, nullable=False, default=func.now())


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


class MemoryDaily(Base):
    """
    Daily market memory distilled from the day's last pipeline run.
    Written by cron/daily_analyst.py each trading day after market close.

    Captures: regime snapshot, portfolio decisions, macro narrative, and key events.
    Portfolio performance fields (portfolio_return_pct, decision_quality_score)
    are backfilled the following day.
    """
    __tablename__ = "memory_daily"

    id                   = Column(BigInteger, primary_key=True, autoincrement=True)
    trading_date         = Column(Date, nullable=False, unique=True)
    created_at           = Column(DateTime(timezone=True), server_default=func.now())
    updated_at           = Column(DateTime(timezone=True), onupdate=func.now())

    # Market state snapshot (from day's last pipeline result)
    regime_label         = Column(String(50), nullable=False)   # trending_bull / high_vol / mean_reverting / etc.
    regime_confidence    = Column(Float, nullable=True)
    vix_close            = Column(Float, nullable=True)
    spy_return_pct       = Column(Float, nullable=True)         # SPY day return %

    # Pipeline decision summary
    recommended_stance   = Column(String(50), nullable=True)    # buy / overweight / maintain / underweight / sell
    risk_approved        = Column(Boolean, nullable=False, default=False)
    execution_happened   = Column(Boolean, nullable=False, default=False)
    top3_overweight      = Column(JSONB, nullable=True)         # [{"ticker": "XLK", "weight": 0.18}]
    top3_underweight     = Column(JSONB, nullable=True)

    # Macro narrative (LLM distilled, ≤200 chars)
    macro_narrative      = Column(Text, nullable=True)
    key_events           = Column(JSONB, nullable=True)          # ["Fed hawkish", "CPI beat"]
    hard_risks_detected  = Column(JSONB, nullable=True)          # ["XLE: earnings_soon"]

    # Strategy performance (backfilled next day, null today)
    portfolio_return_pct  = Column(Float, nullable=True)         # actual portfolio day return %
    decision_quality_score = Column(Float, nullable=True)         # 0-1, post-hoc evaluation

    # Source reference
    agent_analysis_id     = Column(BigInteger, ForeignKey("agent_analysis.id"), nullable=True)
    raw_researcher_output = Column(JSONB, nullable=True)          # full researcher output snapshot


class MemoryWeekly(Base):
    """
    Weekly market memory distilled from the week's MemoryDaily records.
    Written by cron/weekly_analyst.py each Friday after market close.

    Captures: dominant regime, regime shifts, macro themes, sector rotation signals,
    strategy effectiveness review, and next-week outlook.
    """
    __tablename__ = "memory_weekly"

    id                   = Column(BigInteger, primary_key=True, autoincrement=True)
    week_start           = Column(Date, nullable=False, unique=True)   # Monday date
    week_end             = Column(Date, nullable=False)                # Friday date
    created_at           = Column(DateTime(timezone=True), server_default=func.now())

    # Weekly market summary (LLM distilled)
    dominant_regime      = Column(String(50), nullable=False)          # dominant regime this week
    regime_shift         = Column(Boolean, default=False)               # whether regime switched this week
    regime_shift_detail  = Column(Text, nullable=True)                  # how it switched

    # Key macro themes (LLM distilled, 3-5 items)
    macro_themes         = Column(JSONB, nullable=True)                 # ["Fed pivot narrative", "Tech earnings beat"]
    sector_rotation_signal = Column(Text, nullable=True)                # sector rotation summary

    # Strategy signal effectiveness review
    momentum_effectiveness = Column(String(20), nullable=True)          # strong / moderate / weak / failed
    signal_conflicts      = Column(JSONB, nullable=True)                # tickers with biggest Bull/Bear disagreements
    best_calls            = Column(JSONB, nullable=True)                # most accurate calls this week
    worst_calls           = Column(JSONB, nullable=True)                # worst calls this week

    # Portfolio performance
    weekly_return_pct    = Column(Float, nullable=True)
    weekly_sharpe        = Column(Float, nullable=True)
    max_drawdown_pct     = Column(Float, nullable=True)
    execution_count      = Column(Integer, default=0)

    # Next-week outlook (LLM generated, ≤150 chars)
    next_week_watch      = Column(Text, nullable=True)                  # risks/opportunities to watch next week
    calendar_events      = Column(JSONB, nullable=True)                 # important economic events next week

    # Source records
    daily_count          = Column(Integer, default=0)                   # how many days have memory_daily this week
    source_daily_ids     = Column(JSONB, nullable=True)                 # memory_daily id list


# ─────────────────────────────── Earnings Calendar ───────────────────────────────


class EarningsCalendar(Base):
    """
    Tracks upcoming earnings release dates for ETF constituent stocks.
    Written by services/earnings_tracker.py daily (morning_health after).
    Used by RISK MGR hard_risk_filter and context_assembler.
    """
    __tablename__ = "earnings_calendar"

    id              = Column(BigInteger, primary_key=True, autoincrement=True)
    ticker          = Column(String(10), nullable=False, index=True)
    company_name    = Column(String(100), nullable=True)
    earnings_date   = Column(Date, nullable=False)
    eps_estimate    = Column(Float, nullable=True)
    eps_actual      = Column(Float, nullable=True)
    revenue_estimate = Column(Float, nullable=True)
    revenue_actual  = Column(Float, nullable=True)
    is_confirmed    = Column(Boolean, default=False)    # confirmed by exchange
    updated_at      = Column(DateTime, default=func.now(), onupdate=func.now())
    created_at      = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        UniqueConstraint("ticker", "earnings_date", name="uq_earnings_ticker_date"),
    )


# ─────────────────────────────── Macro Events Cache ───────────────────────────────


class MacroEventsCache(Base):
    """
    Single-row rolling cache for economic calendar + Fed schedule.
    Written daily by services/macro_watcher.py (morning_health after).
    Read by context_assembler for injection into RESEARCHER prompt.
    """
    __tablename__ = "macro_events_cache"

    id                  = Column(Integer, primary_key=True, default=1)   # single-row
    economic_calendar   = Column(JSONB, nullable=True)    # Finnhub high-impact events
    fed_schedule        = Column(JSONB, nullable=True)    # Fed meeting dates
    cpi_schedule        = Column(JSONB, nullable=True)    # CPI release dates
    nfp_schedule        = Column(JSONB, nullable=True)    # NFP release dates
    pmi_schedule        = Column(JSONB, nullable=True)    # PMI release dates
    next_fomc           = Column(Date, nullable=True)     # next Fed meeting date
    next_cpi            = Column(Date, nullable=True)     # next CPI release date
    market_watch        = Column(Text, nullable=True)     # LLM summary: key events this week
    updated_at          = Column(DateTime, default=func.now(), onupdate=func.now())


# ─────────────────────────────── Memory Monthly ───────────────────────────────


class MemoryMonthly(Base):
    """
    Monthly market memory distilled from the month's MemoryWeekly records.
    Written by cron/monthly_analyst.py on the last trading day of each month.

    Captures: dominant regime, regime stability, macro themes, momentum effectiveness,
    key lessons, and next-month outlook.
    """
    __tablename__ = "memory_monthly"

    id                   = Column(BigInteger, primary_key=True, autoincrement=True)
    month_start          = Column(Date, nullable=False, unique=True)   # first trading day of month
    month_end            = Column(Date, nullable=False)              # last trading day of month
    created_at           = Column(DateTime(timezone=True), server_default=func.now())

    # Monthly market summary (LLM distilled)
    dominant_regime      = Column(String(50), nullable=False)
    regime_stability     = Column(String(20))                         # stable / shifting / volatile
    macro_themes          = Column(JSONB, nullable=True)                # ["Fed pivot", "Tech earnings"]
    sector_rotation_summary = Column(Text, nullable=True)

    # Strategy effectiveness
    momentum_effectiveness = Column(String(20))                        # strong / moderate / weak / failed
    key_lessons           = Column(JSONB, nullable=True)               # LLM-distilled lessons
    signal_conflicts      = Column(JSONB, nullable=True)               # tickers with biggest disagreements

    # Best/worst calls this month
    best_calls            = Column(JSONB, nullable=True)
    worst_calls           = Column(JSONB, nullable=True)

    # Portfolio performance
    monthly_return_pct    = Column(Float, nullable=True)
    monthly_sharpe        = Column(Float, nullable=True)
    max_drawdown_pct      = Column(Float, nullable=True)
    execution_count       = Column(Integer, default=0)

    # Next month outlook
    next_month_watch      = Column(Text, nullable=True)
    calendar_events       = Column(JSONB, nullable=True)             # important events next month

    # Source records
    weekly_count          = Column(Integer, default=0)                 # how many weeks this month
    source_weekly_ids      = Column(JSONB, nullable=True)              # memory_weekly id list


# ─────────────────────────────── Scenario Analysis ───────────────────────────────


class ScenarioAnalysis(Base):
    """
    Stores scenario stress-test results for each pipeline run.
    Written by services/scenario_analyst.py after Stage 1 (optional).
    Read by RISK MGR overlay or RESEARCHER prompt injection.

    P2-2: SCENARIO_ANALYST
    """
    __tablename__ = "scenario_analysis"

    id                   = Column(BigInteger, primary_key=True, autoincrement=True)
    analysis_id          = Column(BigInteger, ForeignKey("agent_analysis.id"), nullable=True, index=True)
    created_at           = Column(DateTime(timezone=True), server_default=func.now())

    # Which scenario was analyzed
    scenario_name        = Column(String(50), nullable=False)   # e.g. "supply_shock_oil" or "all"

    # Scenario impact estimates
    estimated_impact_pct = Column(Float, nullable=True)         # scenario下的组合预期变化
    affected_tickers     = Column(JSONB, nullable=True)         # [ticker, ...] impacted
    tilt_vector          = Column(JSONB, nullable=True)         # {ticker: strength} from transmission

    # Confidence and source
    confidence           = Column(String(20))                   # high/medium/low
    source               = Column(String(30))                   # "transmission_pattern" / "llm_analysis"
    notes                = Column(Text, nullable=True)
