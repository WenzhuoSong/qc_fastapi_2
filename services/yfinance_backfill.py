"""
yfinance daily feature backfill.

This module is intentionally research-only. It should run from cron and write
to market_daily_features; it must not drive execution decisions directly.
"""
from __future__ import annotations

import logging
import math
from datetime import date, timedelta
from typing import Any

logger = logging.getLogger("qc_fastapi_2.yfinance_backfill")


LOOKBACK_DAYS_DEFAULT = 420
YFINANCE_SOURCE = "yfinance"


async def run_yfinance_backfill(
    tickers: list[str],
    lookback_days: int = LOOKBACK_DAYS_DEFAULT,
    batch_size: int = 30,
) -> dict[str, Any]:
    """Download and store yfinance features one ticker at a time.

    New tickers receive a full lookback. Existing tickers fetch enough recent
    history to recompute rolling features, then only upsert the latest gap.
    """
    clean_tickers = sorted({ticker.upper().strip() for ticker in tickers if ticker})
    if not clean_tickers:
        return {"status": "skipped", "reason": "no_tickers", "rows_upserted": 0}

    total_rows = 0
    full_backfills = 0
    incremental_updates = 0
    failures: dict[str, str] = {}
    empty_results: list[str] = []

    from db.session import AsyncSessionLocal
    from services.market_feature_store import (
        ensure_market_daily_feature_schema,
        get_latest_feature_state,
        upsert_market_daily_features,
    )

    async with AsyncSessionLocal() as db:
        await ensure_market_daily_feature_schema(db)

    for ticker in clean_tickers:
        try:
            async with AsyncSessionLocal() as db:
                latest_state = await get_latest_feature_state(db, ticker, source=YFINANCE_SOURCE)
            latest_date = latest_state.get("trading_date")
            missing_history_fields = latest_state.get("missing_fields") or []
            if missing_history_fields:
                latest_date = None

            fetch_days = _lookback_days_for_ticker(latest_date, lookback_days)
            rows = fetch_yfinance_feature_rows([ticker], lookback_days=fetch_days)
            rows_to_write = _rows_needed_for_ticker(rows, latest_date)
            if not rows_to_write:
                empty_results.append(ticker)
                logger.info(
                    "[yfinance_backfill] ticker=%s latest=%s fetch_days=%s rows=0",
                    ticker,
                    latest_state.get("trading_date"),
                    fetch_days,
                )
                continue

            async with AsyncSessionLocal() as db:
                written = await upsert_market_daily_features(db, rows_to_write, source=YFINANCE_SOURCE)
            total_rows += written
            if latest_date is None:
                full_backfills += 1
                mode = "full" if not missing_history_fields else "full_refresh_missing_fields"
            else:
                incremental_updates += 1
                mode = "incremental"
            logger.info(
                "[yfinance_backfill] ticker=%s mode=%s latest=%s fetch_days=%s rows=%s",
                ticker,
                mode,
                latest_date,
                fetch_days,
                written,
            )
        except Exception as exc:
            logger.warning("[yfinance_backfill] ticker failed ticker=%s error=%s", ticker, exc)
            failures[ticker] = _short_error(exc)

    return {
        "status": "ok" if not failures else "partial",
        "tickers": len(clean_tickers),
        "rows_upserted": total_rows,
        "full_backfills": full_backfills,
        "incremental_updates": incremental_updates,
        "empty_results": empty_results,
        "failures": failures,
        "source": YFINANCE_SOURCE,
    }


def _short_error(exc: Exception) -> str:
    text = str(exc)
    first_line = text.splitlines()[0] if text else type(exc).__name__
    return first_line[:240]


def _lookback_days_for_ticker(
    latest_date: date | None,
    full_lookback_days: int = LOOKBACK_DAYS_DEFAULT,
) -> int:
    if latest_date is None:
        return max(int(full_lookback_days), 30)

    days_since = max((date.today() - latest_date).days, 0)
    rolling_context_days = 280
    refresh_buffer_days = 7
    return max(days_since + refresh_buffer_days + rolling_context_days, 30)


def _rows_needed_for_ticker(
    rows: list[dict[str, Any]],
    latest_date: date | None,
) -> list[dict[str, Any]]:
    if latest_date is None:
        return rows
    cutoff = latest_date - timedelta(days=3)
    return [
        row for row in rows
        if row.get("trading_date") and row["trading_date"] >= cutoff
    ]


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

    clean_tickers = sorted({ticker.upper().strip() for ticker in tickers if ticker})
    if not clean_tickers:
        return []

    import pandas as pd

    download_tickers = sorted(set(clean_tickers) | {"SPY"})
    data = yf.download(
        tickers=" ".join(download_tickers),
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
    frames = {
        ticker: _frame_for_ticker(data, ticker, multi_ticker)
        for ticker in download_tickers
    }
    benchmark_frame = frames.get("SPY")
    for ticker in clean_tickers:
        frame = frames.get(ticker)
        if frame is None:
            continue
        rows.extend(compute_feature_rows_from_frame(ticker, frame, benchmark_frame=benchmark_frame))
    return rows


def _frame_for_ticker(data: Any, ticker: str, multi_ticker: bool):
    if data is None or data.empty:
        return None
    if multi_ticker:
        if ticker not in data.columns.get_level_values(0):
            return None
        return data[ticker].copy()
    return data.copy()


def compute_feature_rows_from_frame(
    ticker: str,
    frame,
    benchmark_frame=None,
) -> list[dict[str, Any]]:
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
    df["rsi_10"] = _rsi(close, 10)
    df["rsi_14"] = _rsi(close, 14)
    true_range = _true_range(df)
    df["atr_pct"] = true_range.rolling(14).mean() / df["close"]
    bb_mid = close.rolling(20).mean()
    bb_std = close.rolling(20).std()
    bb_upper = bb_mid + 2 * bb_std
    bb_lower = bb_mid - 2 * bb_std
    bb_width = bb_upper - bb_lower
    df["bb_position"] = (close - bb_lower) / bb_width.replace(0, math.nan)
    if ticker.upper() == "SPY":
        df["beta_vs_spy"] = 1.0
    else:
        df["beta_vs_spy"] = _rolling_beta_vs_spy(close, benchmark_frame)

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
            "rsi_10": _num(row.get("rsi_10"), digits=2),
            "rsi_14": _num(row.get("rsi_14"), digits=2),
            "atr_pct": _num(row.get("atr_pct")),
            "bb_position": _num(row.get("bb_position"), digits=4),
            "beta_vs_spy": _num(row.get("beta_vs_spy"), digits=4),
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


def _rsi(close, period: int):
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    rs = avg_gain / avg_loss.replace(0, math.nan)
    return 100 - (100 / (1 + rs))


def _true_range(df):
    high = df["high"]
    low = df["low"]
    prev_close = df["close"].shift(1)
    ranges = [
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    ]
    import pandas as pd

    return pd.concat(ranges, axis=1).max(axis=1)


def _rolling_beta_vs_spy(close, benchmark_frame):
    if benchmark_frame is None or getattr(benchmark_frame, "empty", True):
        return close * math.nan

    spy = benchmark_frame.copy()
    spy = spy.rename(columns={col: _normalize_col(col) for col in spy.columns})
    if "close" not in spy.columns:
        return close * math.nan

    spy_adj_col = "adj_close" if "adj_close" in spy.columns else "close"
    spy_close = spy[spy_adj_col].sort_index()
    asset_returns = close.pct_change(1)
    spy_returns = spy_close.pct_change(1).reindex(asset_returns.index)
    spy_var = spy_returns.rolling(60, min_periods=40).var()
    beta = asset_returns.rolling(60, min_periods=40).cov(spy_returns) / spy_var.replace(0, math.nan)
    return beta


def _quality_flag(row: Any, close_price: float | None, volume: int | None) -> str:
    if close_price is None or close_price <= 0:
        return "bad_price"
    if volume is None:
        return "missing_volume"
    if volume == 0:
        return "zero_volume"
    return "ok"
