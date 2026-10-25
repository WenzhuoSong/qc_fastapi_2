# db/models.py
from sqlalchemy import (
    BigInteger, Boolean, Column, DateTime,
    CheckConstraint, Integer, Numeric, String, Text, ForeignKey,
    UniqueConstraint, func
)
from sqlalchemy.dialects.postgresql import JSONB
from db.session import Base


class QCObserverEvent(Base):
    """Immutable FastAPI-side copy of a QC observer fact."""

    __tablename__ = "qc_observer_events"
    __table_args__ = (
        UniqueConstraint("event_id", name="uq_qc_observer_events_event_id"),
        UniqueConstraint(
            "event_stream_id",
            "event_channel",
            "event_sequence",
            name="uq_qc_observer_events_stream_channel_sequence",
        ),
        CheckConstraint("event_sequence > 0", name="ck_qc_observer_events_sequence_positive"),
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    event_id = Column(String(128), nullable=False, index=True)
    event_stream_id = Column(String(128), nullable=False, index=True)
    event_channel = Column(String(16), nullable=False, index=True)
    event_sequence = Column(BigInteger, nullable=False)
    schema_version = Column(String(64), nullable=False)
    event_type = Column(String(80), nullable=False, index=True)
    account_id = Column(String(128), nullable=False, index=True)
    deployment_id = Column(String(128), nullable=False, index=True)
    strategy_id = Column(String(100), index=True)
    event_time = Column(DateTime(timezone=True), nullable=False, index=True)
    received_at = Column(DateTime(timezone=True), nullable=False, default=func.now(), index=True)
    source_fingerprint = Column(String(128))
    policy_fingerprint = Column(String(128))
    content_hash = Column(String(64), nullable=False)
    validation_status = Column(String(32), nullable=False, default="valid", index=True)
    validation_errors = Column(JSONB)
    payload = Column(JSONB, nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False, default=func.now())


class QCObserverStreamReceipt(Base):
    """Committed receive high-water mark for one independent event channel."""

    __tablename__ = "qc_observer_stream_receipts"
    __table_args__ = (
        UniqueConstraint(
            "event_stream_id",
            "event_channel",
            name="uq_qc_observer_stream_receipts_stream_channel",
        ),
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    event_stream_id = Column(String(128), nullable=False, index=True)
    event_channel = Column(String(16), nullable=False)
    highest_contiguous_sequence = Column(BigInteger, nullable=False, default=0)
    highest_seen_sequence = Column(BigInteger, nullable=False, default=0)
    missing_sequences = Column(JSONB, nullable=False, default=list)
    last_event_id = Column(String(128))
    last_event_time = Column(DateTime(timezone=True))
    updated_at = Column(DateTime(timezone=True), nullable=False, default=func.now(), onupdate=func.now())


class QCObserverEventAnnotation(Base):
    """Append-only evidence classification for an immutable observer fact."""

    __tablename__ = "qc_observer_event_annotations"
    __table_args__ = (
        UniqueConstraint(
            "raw_event_id",
            "annotation_type",
            name="uq_qc_observer_event_annotations_event_type",
        ),
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    raw_event_id = Column(BigInteger, ForeignKey("qc_observer_events.id"), nullable=False, index=True)
    annotation_type = Column(String(40), nullable=False, index=True)
    reason_code = Column(String(80), nullable=False, index=True)
    reason = Column(Text, nullable=False)
    excludes_from_evidence = Column(Boolean, nullable=False, default=False, index=True)
    annotation_payload = Column(JSONB, nullable=False, default=dict)
    created_by = Column(String(80), nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False, default=func.now())


class QCObserverRequestNonce(Base):
    """One accepted request nonce used to reject signed payload replay."""

    __tablename__ = "qc_observer_request_nonces"

    nonce = Column(String(128), primary_key=True)
    account_id = Column(String(128), nullable=False, index=True)
    deployment_id = Column(String(128), nullable=False, index=True)
    request_timestamp = Column(DateTime(timezone=True), nullable=False)
    body_hash = Column(String(64), nullable=False)
    expires_at = Column(DateTime(timezone=True), nullable=False, index=True)
    created_at = Column(DateTime(timezone=True), nullable=False, default=func.now())


class QCObserverRejectedEvent(Base):
    """Quarantined event rejected without rolling back valid batch siblings."""

    __tablename__ = "qc_observer_rejected_events"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    request_nonce = Column(String(128), index=True)
    event_index = Column(Integer, nullable=False)
    event_id = Column(String(128), index=True)
    event_stream_id = Column(String(128), index=True)
    event_channel = Column(String(16))
    event_sequence = Column(BigInteger)
    account_id = Column(String(128), index=True)
    deployment_id = Column(String(128), index=True)
    rejection_code = Column(String(80), nullable=False, index=True)
    rejection_message = Column(Text, nullable=False)
    raw_content_hash = Column(String(64), nullable=False)
    raw_event = Column(JSONB, nullable=False)
    received_at = Column(DateTime(timezone=True), nullable=False, default=func.now())


class QCProjectionCheckpoint(Base):
    """Rebuildable projection cursor, isolated by stream and channel."""

    __tablename__ = "qc_projection_checkpoints"
    __table_args__ = (
        UniqueConstraint(
            "projection_name",
            "event_stream_id",
            "event_channel",
            name="uq_qc_projection_checkpoints_projection_stream_channel",
        ),
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    projection_name = Column(String(100), nullable=False, index=True)
    projection_version = Column(String(40), nullable=False)
    event_stream_id = Column(String(128), nullable=False)
    event_channel = Column(String(16), nullable=False)
    last_processed_sequence = Column(BigInteger, nullable=False, default=0)
    status = Column(String(32), nullable=False, default="ready")
    last_error = Column(Text)
    updated_at = Column(DateTime(timezone=True), nullable=False, default=func.now(), onupdate=func.now())


class QCDecisionManifestProjection(Base):
    __tablename__ = "qc_decision_manifests"
    __table_args__ = (
        UniqueConstraint("raw_event_id", name="uq_qc_decision_manifests_raw_event"),
        UniqueConstraint("event_stream_id", "decision_id", name="uq_qc_decision_manifests_stream_decision"),
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    raw_event_id = Column(BigInteger, ForeignKey("qc_observer_events.id"), nullable=False, index=True)
    event_id = Column(String(128), nullable=False)
    event_stream_id = Column(String(128), nullable=False, index=True)
    account_id = Column(String(128), nullable=False, index=True)
    deployment_id = Column(String(128), nullable=False, index=True)
    strategy_id = Column(String(100), index=True)
    event_sequence = Column(BigInteger, nullable=False)
    event_time = Column(DateTime(timezone=True), nullable=False, index=True)
    projection_version = Column(String(40), nullable=False)
    decision_id = Column(String(128), nullable=False, index=True)
    decision_kind = Column(String(80), nullable=False)
    as_of = Column(DateTime(timezone=True), nullable=False)
    basis_account_snapshot_id = Column(String(128))
    market_data_cutoff = Column(DateTime(timezone=True))
    strategy_version = Column(String(100))
    execution_style = Column(String(40))
    execution_authority = Column(String(40), nullable=False)
    raw_target_weights = Column(JSONB, nullable=False, default=dict)
    risk_adjusted_weights = Column(JSONB, nullable=False, default=dict)
    final_target_weights = Column(JSONB, nullable=False, default=dict)
    suppressed_deltas = Column(JSONB, nullable=False, default=dict)
    payload_extra = Column(JSONB, nullable=False, default=dict)


class QCDecisionRankingProjection(Base):
    __tablename__ = "qc_decision_rankings"
    __table_args__ = (
        UniqueConstraint("decision_manifest_id", "security_id", name="uq_qc_decision_rankings_manifest_security"),
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    decision_manifest_id = Column(BigInteger, ForeignKey("qc_decision_manifests.id"), nullable=False, index=True)
    raw_event_id = Column(BigInteger, ForeignKey("qc_observer_events.id"), nullable=False, index=True)
    security_id = Column(String(128), nullable=False, index=True)
    ticker = Column(String(32), index=True)
    rank = Column(Integer, nullable=False)
    score = Column(Numeric(24, 12))
    history_hash = Column(String(128))
    payload_extra = Column(JSONB, nullable=False, default=dict)


class QCOrderIntentProjection(Base):
    __tablename__ = "qc_order_intents"
    __table_args__ = (
        UniqueConstraint("raw_event_id", name="uq_qc_order_intents_raw_event"),
        UniqueConstraint("event_stream_id", "order_intent_id", name="uq_qc_order_intents_stream_intent"),
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    raw_event_id = Column(BigInteger, ForeignKey("qc_observer_events.id"), nullable=False, index=True)
    event_stream_id = Column(String(128), nullable=False, index=True)
    account_id = Column(String(128), nullable=False)
    deployment_id = Column(String(128), nullable=False)
    event_time = Column(DateTime(timezone=True), nullable=False, index=True)
    projection_version = Column(String(40), nullable=False)
    order_intent_id = Column(String(128), nullable=False, index=True)
    decision_id = Column(String(128), index=True)
    security_id = Column(String(128), nullable=False, index=True)
    ticker = Column(String(32), index=True)
    requested_quantity = Column(Numeric(24, 8), nullable=False)
    submitted_quantity = Column(Numeric(24, 8))
    target_weight = Column(Numeric(18, 10))
    reference_price = Column(Numeric(24, 8))
    reference_time = Column(DateTime(timezone=True))
    order_type = Column(String(40), nullable=False)
    reason = Column(String(100), nullable=False)
    risk_class = Column(String(40), nullable=False)
    clipped = Column(Boolean)
    payload_extra = Column(JSONB, nullable=False, default=dict)


class QCOrderLifecycleProjection(Base):
    __tablename__ = "qc_order_lifecycle_events"
    __table_args__ = (UniqueConstraint("raw_event_id", name="uq_qc_order_lifecycle_raw_event"),)

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    raw_event_id = Column(BigInteger, ForeignKey("qc_observer_events.id"), nullable=False, index=True)
    event_stream_id = Column(String(128), nullable=False, index=True)
    account_id = Column(String(128), nullable=False)
    deployment_id = Column(String(128), nullable=False)
    event_time = Column(DateTime(timezone=True), nullable=False, index=True)
    projection_version = Column(String(40), nullable=False)
    broker_order_id = Column(String(128), nullable=False, index=True)
    order_intent_id = Column(String(128), index=True)
    security_id = Column(String(128), nullable=False, index=True)
    ticker = Column(String(32), index=True)
    status = Column(String(60), nullable=False, index=True)
    quantity = Column(Numeric(24, 8))
    fill_quantity = Column(Numeric(24, 8))
    fill_quantity_semantics = Column(String(40), nullable=False)
    fill_price = Column(Numeric(24, 8))
    fee = Column(Numeric(24, 8))
    message = Column(Text)
    reconstructed = Column(Boolean, nullable=False, default=False)
    reconstruction_source = Column(String(80))
    remaining_quantity = Column(Numeric(24, 8))
    payload_extra = Column(JSONB, nullable=False, default=dict)


class QCLocalRiskProjection(Base):
    __tablename__ = "qc_local_risk_events"
    __table_args__ = (
        UniqueConstraint("raw_event_id", name="uq_qc_local_risk_raw_event"),
        UniqueConstraint("event_stream_id", "risk_event_id", name="uq_qc_local_risk_stream_event"),
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    raw_event_id = Column(BigInteger, ForeignKey("qc_observer_events.id"), nullable=False, index=True)
    event_stream_id = Column(String(128), nullable=False, index=True)
    account_id = Column(String(128), nullable=False)
    deployment_id = Column(String(128), nullable=False)
    event_time = Column(DateTime(timezone=True), nullable=False, index=True)
    projection_version = Column(String(40), nullable=False)
    risk_event_id = Column(String(128), nullable=False, index=True)
    security_id = Column(String(128), nullable=False, index=True)
    ticker = Column(String(32), index=True)
    risk_type = Column(String(60), nullable=False, index=True)
    stage = Column(String(60), nullable=False, index=True)
    price = Column(Numeric(24, 8))
    entry_price = Column(Numeric(24, 8))
    anchor_price = Column(Numeric(24, 8))
    cooldown_until = Column(DateTime(timezone=True))
    details = Column(JSONB, nullable=False, default=dict)


class QCAccountSnapshotProjection(Base):
    __tablename__ = "qc_account_snapshots"
    __table_args__ = (
        UniqueConstraint("raw_event_id", name="uq_qc_account_snapshots_raw_event"),
        UniqueConstraint("event_stream_id", "account_snapshot_id", name="uq_qc_account_snapshots_stream_snapshot"),
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    raw_event_id = Column(BigInteger, ForeignKey("qc_observer_events.id"), nullable=False, index=True)
    event_stream_id = Column(String(128), nullable=False, index=True)
    account_id = Column(String(128), nullable=False, index=True)
    deployment_id = Column(String(128), nullable=False)
    event_time = Column(DateTime(timezone=True), nullable=False, index=True)
    projection_version = Column(String(40), nullable=False)
    account_snapshot_id = Column(String(128), nullable=False, index=True)
    equity = Column(Numeric(24, 8), nullable=False)
    cash = Column(Numeric(24, 8), nullable=False)
    buying_power = Column(Numeric(24, 8))
    state_generation = Column(BigInteger, nullable=False)
    state_active = Column(Boolean)
    broker_reconciled = Column(Boolean, nullable=False)
    pending_order_count = Column(Integer, nullable=False, default=0)
    deferred_target_count = Column(Integer, nullable=False, default=0)
    payload_extra = Column(JSONB, nullable=False, default=dict)


class QCAccountSnapshotHoldingProjection(Base):
    __tablename__ = "qc_account_snapshot_holdings"
    __table_args__ = (
        UniqueConstraint("account_snapshot_id", "security_id", name="uq_qc_snapshot_holdings_snapshot_security"),
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    account_snapshot_id = Column(BigInteger, ForeignKey("qc_account_snapshots.id"), nullable=False, index=True)
    raw_event_id = Column(BigInteger, ForeignKey("qc_observer_events.id"), nullable=False, index=True)
    security_id = Column(String(128), nullable=False, index=True)
    ticker = Column(String(32), index=True)
    quantity = Column(Numeric(24, 8), nullable=False)
    average_price = Column(Numeric(24, 8))
    holdings_value = Column(Numeric(24, 8))
    payload_extra = Column(JSONB, nullable=False, default=dict)


class QCAccountSnapshotOpenOrderProjection(Base):
    __tablename__ = "qc_account_snapshot_open_orders"
    __table_args__ = (
        UniqueConstraint("account_snapshot_id", "broker_order_id", name="uq_qc_snapshot_orders_snapshot_order"),
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    account_snapshot_id = Column(BigInteger, ForeignKey("qc_account_snapshots.id"), nullable=False, index=True)
    raw_event_id = Column(BigInteger, ForeignKey("qc_observer_events.id"), nullable=False, index=True)
    broker_order_id = Column(String(128), nullable=False, index=True)
    ticker = Column(String(32), index=True)
    quantity = Column(Numeric(24, 8))
    quantity_filled = Column(Numeric(24, 8))
    payload_extra = Column(JSONB, nullable=False, default=dict)


class QCAuditRun(Base):
    """Immutable, versioned computation over observer projections."""

    __tablename__ = "qc_audit_runs"
    __table_args__ = (
        UniqueConstraint(
            "audit_name",
            "method_version",
            "projection_cutoff_raw_event_id",
            name="uq_qc_audit_runs_method_cutoff",
        ),
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    audit_name = Column(String(100), nullable=False, index=True)
    method_version = Column(String(40), nullable=False)
    projection_version = Column(String(40), nullable=False)
    projection_cutoff_raw_event_id = Column(BigInteger, nullable=False, index=True)
    input_hash = Column(String(64), nullable=False)
    status = Column(String(32), nullable=False, index=True)
    summary = Column(JSONB, nullable=False, default=dict)
    error = Column(Text)
    started_at = Column(DateTime(timezone=True), nullable=False, default=func.now())
    completed_at = Column(DateTime(timezone=True))


class QCDecisionExecutionAudit(Base):
    """Decision-to-order-to-fill and target-to-actual reconciliation result."""

    __tablename__ = "qc_decision_execution_audits"
    __table_args__ = (
        UniqueConstraint("audit_run_id", "decision_manifest_id", name="uq_qc_decision_audit_run_manifest"),
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    audit_run_id = Column(BigInteger, ForeignKey("qc_audit_runs.id"), nullable=False, index=True)
    decision_manifest_id = Column(
        BigInteger, ForeignKey("qc_decision_manifests.id"), nullable=False, index=True
    )
    event_stream_id = Column(String(128), nullable=False, index=True)
    decision_id = Column(String(128), nullable=False, index=True)
    decision_time = Column(DateTime(timezone=True), nullable=False)
    account_snapshot_id = Column(BigInteger, ForeignKey("qc_account_snapshots.id"), index=True)
    status = Column(String(40), nullable=False, index=True)
    target_exposure = Column(Numeric(18, 10), nullable=False)
    actual_exposure = Column(Numeric(18, 10))
    cash_weight = Column(Numeric(18, 10))
    tracking_error_l1 = Column(Numeric(18, 10))
    intent_count = Column(Integer, nullable=False, default=0)
    terminal_intent_count = Column(Integer, nullable=False, default=0)
    partial_fill_count = Column(Integer, nullable=False, default=0)
    rejected_intent_count = Column(Integer, nullable=False, default=0)
    clipped_intent_count = Column(Integer, nullable=False, default=0)
    aggregate_fill_ratio = Column(Numeric(18, 10))
    total_fees = Column(Numeric(24, 8), nullable=False, default=0)
    first_submit_latency_seconds = Column(Numeric(24, 6))
    terminal_latency_seconds = Column(Numeric(24, 6))
    evidence_raw_event_ids = Column(JSONB, nullable=False, default=list)
    details = Column(JSONB, nullable=False, default=dict)
    created_at = Column(DateTime(timezone=True), nullable=False, default=func.now())


class QCRiskExitAudit(Base):
    """Observed local-risk event through the corresponding risk-reducing order path."""

    __tablename__ = "qc_risk_exit_audits"
    __table_args__ = (
        UniqueConstraint("audit_run_id", "local_risk_event_id", name="uq_qc_risk_audit_run_event"),
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    audit_run_id = Column(BigInteger, ForeignKey("qc_audit_runs.id"), nullable=False, index=True)
    local_risk_event_id = Column(
        BigInteger, ForeignKey("qc_local_risk_events.id"), nullable=False, index=True
    )
    event_stream_id = Column(String(128), nullable=False, index=True)
    risk_event_id = Column(String(128), nullable=False, index=True)
    security_id = Column(String(128), nullable=False, index=True)
    ticker = Column(String(32), index=True)
    risk_type = Column(String(60), nullable=False)
    risk_time = Column(DateTime(timezone=True), nullable=False)
    order_intent_id = Column(BigInteger, ForeignKey("qc_order_intents.id"), index=True)
    status = Column(String(40), nullable=False, index=True)
    intent_latency_seconds = Column(Numeric(24, 6))
    first_fill_latency_seconds = Column(Numeric(24, 6))
    terminal_fill_latency_seconds = Column(Numeric(24, 6))
    requested_quantity = Column(Numeric(24, 8))
    filled_quantity = Column(Numeric(24, 8))
    residual_quantity = Column(Numeric(24, 8))
    residual_notional = Column(Numeric(24, 8))
    evidence_raw_event_ids = Column(JSONB, nullable=False, default=list)
    details = Column(JSONB, nullable=False, default=dict)
    created_at = Column(DateTime(timezone=True), nullable=False, default=func.now())


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


