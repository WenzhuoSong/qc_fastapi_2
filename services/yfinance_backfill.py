"""
yfinance daily feature backfill.

This module is intentionally research-only. It should run from cron and write
to market_daily_features; it must not drive execution decisions directly.
"""
from __future__ import annotations

import logging
import math
from datetime import date
from typing import Any

logger = logging.getLogger("qc_fastapi_2.yfinance_backfill")


LOOKBACK_DAYS_DEFAULT = 420
YFINANCE_SOURCE = "yfinance"


async def run_yfinance_backfill(
    tickers: list[str],
    lookback_days: int = LOOKBACK_DAYS_DEFAULT,
    batch_size: int = 30,
) -> dict[str, Any]:
    """Download and store yfinance daily features for the requested tickers."""
    clean_tickers = sorted({ticker.upper().strip() for ticker in tickers if ticker})
    if not clean_tickers:
        return {"status": "skipped", "reason": "no_tickers", "rows_upserted": 0}

    total_rows = 0
    failures: dict[str, str] = {}
    for idx in range(0, len(clean_tickers), batch_size):
        batch = clean_tickers[idx: idx + batch_size]
        try:
            rows = fetch_yfinance_feature_rows(batch, lookback_days=lookback_days)
            from db.session import AsyncSessionLocal
            from services.market_feature_store import upsert_market_daily_features
            async with AsyncSessionLocal() as db:
                total_rows += await upsert_market_daily_features(db, rows, source=YFINANCE_SOURCE)
            logger.info("[yfinance_backfill] batch=%s rows=%s", ",".join(batch), len(rows))
        except Exception as exc:
            logger.warning("[yfinance_backfill] batch failed tickers=%s error=%s", batch, exc)
            for ticker in batch:
                failures[ticker] = str(exc)

    return {
        "status": "ok" if not failures else "partial",
        "tickers": len(clean_tickers),
        "rows_upserted": total_rows,
        "failures": failures,
        "source": YFINANCE_SOURCE,
    }


def fetch_yfinance_feature_rows(
    tickers: list[str],
    lookback_days: int = LOOKBACK_DAYS_DEFAULT,
) -> list[dict[str, Any]]:
    """
    Fetch OHLCV from yfinance and compute daily research features.

    Kept sync because yfinance is sync; caller decides whether to run it from
    cron. Tests can exercise compute_feature_rows_from_frame without yfinance.
    """
    try:
        import yfinance as yf
    except ImportError as exc:
        raise RuntimeError("yfinance is not installed. Add yfinance to requirements and redeploy.") from exc

    if not tickers:
        return []

    import pandas as pd

    data = yf.download(
        tickers=" ".join(tickers),
        period=f"{max(int(lookback_days), 30)}d",
        interval="1d",
        group_by="ticker",
        auto_adjust=False,
        threads=True,
        progress=False,
    )
    if data is None or data.empty:
        return []

    rows: list[dict[str, Any]] = []
    multi_ticker = isinstance(data.columns, pd.MultiIndex)
    for ticker in tickers:
        if multi_ticker:
            if ticker not in data.columns.get_level_values(0):
                continue
            frame = data[ticker].copy()
        else:
            frame = data.copy()
        rows.extend(compute_feature_rows_from_frame(ticker, frame))
    return rows


def compute_feature_rows_from_frame(ticker: str, frame) -> list[dict[str, Any]]:
    """Convert a yfinance OHLCV DataFrame into normalized feature rows."""
    if frame is None or frame.empty:
        return []

    df = frame.copy()
    df = df.rename(columns={col: _normalize_col(col) for col in df.columns})
    required = {"open", "high", "low", "close", "volume"}
    if not required.issubset(set(df.columns)):
        return []

    df = df.sort_index()
    df = df.dropna(subset=["close"])
    if df.empty:
        return []

    adj_col = "adj_close" if "adj_close" in df.columns else "close"
    close = df[adj_col]
    df["return_1d"] = close.pct_change(1)
    df["return_5d"] = close.pct_change(5)
    df["return_20d"] = close.pct_change(20)
    df["return_60d"] = close.pct_change(60)
    df["return_252d"] = close.pct_change(252)
    df["sma_20"] = close.rolling(20).mean()
    df["sma_50"] = close.rolling(50).mean()
    df["sma_200"] = close.rolling(200).mean()
    df["hist_vol_20d"] = df["return_1d"].rolling(20).std()

    rows: list[dict[str, Any]] = []
    for idx, row in df.iterrows():
        trading_date = _date_from_index(idx)
        close_price = _num(row.get("close"))
        volume = _int_or_none(row.get("volume"))
        quality = _quality_flag(row, close_price, volume)
        rows.append({
            "trading_date": trading_date,
            "ticker": ticker.upper(),
            "source": YFINANCE_SOURCE,
            "open_price": _num(row.get("open")),
            "high_price": _num(row.get("high")),
            "low_price": _num(row.get("low")),
            "close_price": close_price,
            "adj_close_price": _num(row.get(adj_col)),
            "volume": volume,
            "dollar_volume": round(close_price * volume, 2) if close_price is not None and volume is not None else None,
            "return_1d": _num(row.get("return_1d")),
            "return_5d": _num(row.get("return_5d")),
            "return_20d": _num(row.get("return_20d")),
            "return_60d": _num(row.get("return_60d")),
            "return_252d": _num(row.get("return_252d")),
            "sma_20": _num(row.get("sma_20")),
            "sma_50": _num(row.get("sma_50")),
            "sma_200": _num(row.get("sma_200")),
            "hist_vol_20d": _num(row.get("hist_vol_20d")),
            "data_quality_flag": quality,
            "raw_payload": {
                "provider": YFINANCE_SOURCE,
                "ticker": ticker.upper(),
                "date": trading_date.isoformat(),
            },
        })
    return rows


def _normalize_col(col: Any) -> str:
    return str(col).strip().lower().replace(" ", "_")


def _date_from_index(idx: Any) -> date:
    if hasattr(idx, "date"):
        return idx.date()
    if isinstance(idx, date):
        return idx
    return date.fromisoformat(str(idx)[:10])


def _num(value: Any, digits: int = 6) -> float | None:
    try:
        value = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(value) or math.isinf(value):
        return None
    return round(value, digits)


def _int_or_none(value: Any) -> int | None:
    try:
        value = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(value) or math.isinf(value):
        return None
    return int(value)


def _quality_flag(row: Any, close_price: float | None, volume: int | None) -> str:
    if close_price is None or close_price <= 0:
        return "bad_price"
    if volume is None:
        return "missing_volume"
    if volume == 0:
        return "zero_volume"
    return "ok"
