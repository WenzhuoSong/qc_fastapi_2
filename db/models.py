# db/models.py
import uuid
from datetime import datetime, date
from sqlalchemy import (
    BigInteger, Boolean, Column, Date, DateTime,
    Integer, Numeric, String, Text, ForeignKey, func
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
    execution_status   = Column(String(20), default="pending")
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
