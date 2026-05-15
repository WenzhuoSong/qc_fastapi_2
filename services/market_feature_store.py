"""
Research/backfill market feature store.

The rows here are not authoritative for execution. They are used for
playground replay, cold-start history, data-quality checks, and memory
calibration when QC snapshots are sparse.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Any

logger = logging.getLogger("qc_fastapi_2.market_feature_store")


FEATURE_COLUMNS = [
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "adj_close_price",
    "volume",
    "dollar_volume",
    "return_1d",
    "return_5d",
    "return_20d",
    "return_60d",
    "return_252d",
    "sma_20",
    "sma_50",
    "sma_200",
    "hist_vol_20d",
    "rsi_14",
    "atr_pct",
    "bb_position",
    "data_quality_flag",
    "raw_payload",
]


async def ensure_market_daily_feature_schema(db: Any) -> None:
    """Apply lightweight feature-store columns needed by standalone cron jobs."""
    from sqlalchemy import text

    migrations = (
        "ALTER TABLE market_daily_features ADD COLUMN IF NOT EXISTS rsi_14 NUMERIC(6,2)",
        "ALTER TABLE market_daily_features ADD COLUMN IF NOT EXISTS atr_pct NUMERIC(8,6)",
        "ALTER TABLE market_daily_features ADD COLUMN IF NOT EXISTS bb_position NUMERIC(6,4)",
    )
    for sql in migrations:
        await db.execute(text(sql))
    await db.commit()


async def upsert_market_daily_features(
    db: Any,
    rows: list[dict[str, Any]],
    source: str = "yfinance",
) -> int:
    """Upsert daily feature rows by (trading_date, ticker, source)."""
    from sqlalchemy.dialects.postgresql import insert

    from db.models import MarketDailyFeature

    if not rows:
        return 0

    now = datetime.utcnow()
    payloads: list[dict[str, Any]] = []
    for row in rows:
        ticker = (row.get("ticker") or "").upper().strip()
        trading_date = row.get("trading_date")
        if not ticker or not trading_date:
            continue
        payload = {
            "trading_date": trading_date,
            "ticker": ticker,
            "source": row.get("source") or source,
            "created_at": now,
            "updated_at": now,
        }
        for col in FEATURE_COLUMNS:
            if col in row:
                payload[col] = row.get(col)
        payloads.append(payload)

    if not payloads:
        return 0

    stmt = insert(MarketDailyFeature).values(payloads)
    update_cols = {
        col: getattr(stmt.excluded, col)
        for col in FEATURE_COLUMNS
        if col != "raw_payload"
    }
    update_cols["raw_payload"] = stmt.excluded.raw_payload
    update_cols["updated_at"] = now
    stmt = stmt.on_conflict_do_update(
        constraint="uq_market_daily_feature_date_ticker_source",
        set_=update_cols,
    )
    await db.execute(stmt)
    await db.commit()
    return len(payloads)


async def get_latest_feature_date(
    db: Any,
    ticker: str,
    source: str = "yfinance",
) -> date | None:
    """Return the latest stored trading date for one ticker/source."""
    from sqlalchemy import desc, select

    from db.models import MarketDailyFeature

    clean_ticker = (ticker or "").upper().strip()
    if not clean_ticker:
        return None

    result = await db.execute(
        select(MarketDailyFeature.trading_date)
        .where(MarketDailyFeature.ticker == clean_ticker)
        .where(MarketDailyFeature.source == source)
        .order_by(desc(MarketDailyFeature.trading_date))
        .limit(1)
    )
    return result.scalar_one_or_none()


async def get_latest_feature_state(
    db: Any,
    ticker: str,
    source: str = "yfinance",
) -> dict[str, Any]:
    """Return latest stored row status for one ticker/source."""
    from sqlalchemy import desc, select

    from db.models import MarketDailyFeature

    clean_ticker = (ticker or "").upper().strip()
    if not clean_ticker:
        return {"trading_date": None, "missing_fields": []}

    row = (
        await db.execute(
            select(MarketDailyFeature)
            .where(MarketDailyFeature.ticker == clean_ticker)
            .where(MarketDailyFeature.source == source)
            .order_by(desc(MarketDailyFeature.trading_date))
            .limit(1)
        )
    ).scalar_one_or_none()
    if not row:
        return {"trading_date": None, "missing_fields": []}

    required_history_fields = ("rsi_14", "atr_pct", "bb_position")
    return {
        "trading_date": row.trading_date,
        "missing_fields": [
            field for field in required_history_fields
            if getattr(row, field, None) is None
        ],
    }


async def get_market_daily_feature_rows(
    db: Any,
    tickers: list[str],
    start_date: date | None = None,
    end_date: date | None = None,
    source: str | None = None,
    limit: int | None = None,
) -> list[Any]:
    """Read feature rows for research/replay consumers."""
    from sqlalchemy import desc, select

    from db.models import MarketDailyFeature

    clean_tickers = sorted({ticker.upper().strip() for ticker in tickers if ticker})
    if not clean_tickers:
        return []

    stmt = select(MarketDailyFeature).where(MarketDailyFeature.ticker.in_(clean_tickers))
    if start_date:
        stmt = stmt.where(MarketDailyFeature.trading_date >= start_date)
    if end_date:
        stmt = stmt.where(MarketDailyFeature.trading_date <= end_date)
    if source:
        stmt = stmt.where(MarketDailyFeature.source == source)
    stmt = stmt.order_by(desc(MarketDailyFeature.trading_date), MarketDailyFeature.ticker)
    if limit:
        stmt = stmt.limit(limit)

    result = await db.execute(stmt)
    return list(result.scalars().all())


async def latest_feature_map(
    db: Any,
    tickers: list[str],
    source: str | None = None,
    max_age_days: int = 10,
) -> dict[str, dict[str, Any]]:
    """Return latest available daily feature row by ticker."""
    cutoff = date.today() - timedelta(days=max_age_days)
    rows = await get_market_daily_feature_rows(
        db,
        tickers=tickers,
        start_date=cutoff,
        source=source,
        limit=max(len(tickers) * max_age_days, 50),
    )
    latest: dict[str, dict[str, Any]] = {}
    for row in rows:
        if row.ticker in latest:
            continue
        latest[row.ticker] = model_to_feature_dict(row)
    return latest


def model_to_feature_dict(row: Any) -> dict[str, Any]:
    return {
        "ticker": row.ticker,
        "trading_date": row.trading_date.isoformat() if row.trading_date else None,
        "source": row.source,
        "open_price": _float_or_none(row.open_price),
        "high_price": _float_or_none(row.high_price),
        "low_price": _float_or_none(row.low_price),
        "close_price": _float_or_none(row.close_price),
        "adj_close_price": _float_or_none(row.adj_close_price),
        "volume": int(row.volume) if row.volume is not None else None,
        "dollar_volume": _float_or_none(row.dollar_volume),
        "return_1d": _float_or_none(row.return_1d),
        "return_5d": _float_or_none(row.return_5d),
        "return_20d": _float_or_none(row.return_20d),
        "return_60d": _float_or_none(row.return_60d),
        "return_252d": _float_or_none(row.return_252d),
        "sma_20": _float_or_none(row.sma_20),
        "sma_50": _float_or_none(row.sma_50),
        "sma_200": _float_or_none(row.sma_200),
        "hist_vol_20d": _float_or_none(row.hist_vol_20d),
        "rsi_14": _float_or_none(row.rsi_14),
        "atr_pct": _float_or_none(row.atr_pct),
        "bb_position": _float_or_none(row.bb_position),
        "data_quality_flag": row.data_quality_flag,
    }


def _float_or_none(value: Any) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None
