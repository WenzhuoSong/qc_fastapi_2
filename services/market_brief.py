"""
Stage 1: market_brief -- pure Python market summary generation.

Responsibilities (no LLM calls):
    1. Read latest QCSnapshot -> holdings[] + portfolio
    2. Read MacroNewsCache (latest 1 row) -> macro_news + calendar + pre-assembled prose
    3. Read TickerNewsLibrary (within 48h) -> per_ticker_news + hard_risks_map
    4. Python compute quant metrics: breadth_pct / spy_mom_60d / avg_atr_pct / risk_on_score /
                          drawdown_pct / top5 / bottom5
    5. Translate to prose + package as brief dict for downstream RESEARCHER / STRATEGY ENGINE

Downstream consumers:
    - RESEARCHER: uses prose_summary + macro_news_section + calendar_section as user_message
    - STRATEGY ENGINE: uses hard_risks_map for hard_risk_filter; uses current_weights / holdings
"""
from __future__ import annotations

import json
import logging
import time
from datetime import datetime as dt_cls
from typing import Any

from sqlalchemy import desc, select

from constants import RISK_ON_SECTORS, RISK_OFF_SECTORS
from db.models import MacroNewsCache, QCSnapshot, TickerNewsLibrary
from db.session import AsyncSessionLocal

logger = logging.getLogger("qc_fastapi_2.market_brief")

NEWS_LOOKBACK_SECONDS = 48 * 3600


# ═══════════════════════════════════════════════════════════════
# Entry
# ═══════════════════════════════════════════════════════════════

async def build_market_brief(pipeline_context: dict) -> dict[str, Any]:
    """
    Returns brief = {
        prose_summary:       str,
        key_facts:           dict,
        macro_news_section:  str,
        calendar_section:    str,
        per_ticker_news:     dict[ticker, list[news_dict]],
        hard_risks_map:      dict[ticker, dict[risk_type, reason]],
        holdings:            list[dict]    # original snapshot.holdings
        current_weights:     dict[ticker, float],
        portfolio:            dict,
        news_context:        dict,         # structured news (Phase 2 addition)
    }
    """
    snapshot = await _read_latest_snapshot()
    holdings = (snapshot.get("holdings") or []) if snapshot else []
    portfolio = (snapshot.get("portfolio") or {}) if snapshot else {}

    current_weights = _extract_current_weights(holdings)

    tickers_of_interest = sorted(set(current_weights.keys()) | _candidate_tickers(holdings))

    macro = await _read_macro_cache()
    news_context = await get_news_context()
    per_ticker_news, hard_risks_map = await _read_ticker_news(tickers_of_interest)

    key_facts = _compute_key_facts(holdings, portfolio)

    prose = _build_prose(key_facts, holdings)

    brief = {
        "prose_summary":      prose,
        "key_facts":          key_facts,
        "macro_news_section": macro.get("prose_summary") or "",
        "calendar_section":   _format_calendar(macro.get("economic_calendar") or []),
        "per_ticker_news":    per_ticker_news,
        "hard_risks_map":     hard_risks_map,
        "holdings":           holdings,
        "current_weights":    current_weights,
        "portfolio":          portfolio,
        "news_context":       news_context,
    }

    logger.info(
        f"market_brief built | breadth={key_facts.get('breadth_pct')} "
        f"| spy_mom_60d={key_facts.get('spy_mom_60d')} "
        f"| drawdown={key_facts.get('drawdown_pct')} "
        f"| per_ticker_news={sum(len(v) for v in per_ticker_news.values())} items "
        f"| hard_risks={len(hard_risks_map)}"
    )
    return brief


# ═══════════════════════════════════════════════════════════════
# Snapshot
# ═══════════════════════════════════════════════════════════════

async def _read_latest_snapshot() -> dict | None:
    """Read latest heartbeat snapshot's raw_payload."""
    async with AsyncSessionLocal() as db:
        stmt = (
            select(QCSnapshot)
            .order_by(desc(QCSnapshot.received_at))
            .limit(1)
        )
        row = (await db.execute(stmt)).scalar_one_or_none()
    if not row:
        return None
    return row.raw_payload or {}


def _extract_current_weights(holdings: list[dict]) -> dict[str, float]:
    out: dict[str, float] = {}
    for h in holdings:
        t = (h.get("ticker") or "").upper().strip()
        if not t:
            continue
        try:
            w = float(h.get("weight_current") or 0)
        except (TypeError, ValueError):
            w = 0.0
        out[t] = w
    return out


def _candidate_tickers(holdings: list[dict]) -> set[str]:
    """Also collect tickers appearing in target/drift from holdings as news query scope."""
    return {(h.get("ticker") or "").upper() for h in holdings if h.get("ticker")}


# ═══════════════════════════════════════════════════════════════
# Macro cache
# ═══════════════════════════════════════════════════════════════

async def _read_macro_cache() -> dict:
    async with AsyncSessionLocal() as db:
        row = (await db.execute(
            select(MacroNewsCache).where(MacroNewsCache.id == 1)
        )).scalar_one_or_none()
    if not row:
        return {}
    return {
        "as_of":             row.as_of,
        "macro_news":        row.macro_news or [],
        "economic_calendar": row.economic_calendar or [],
        "prose_summary":     row.prose_summary or "",
    }


# ═══════════════════════════════════════════════════════════════
# Structured news context (Phase 2)
# ═══════════════════════════════════════════════════════════════

async def get_news_context() -> dict:
    """
    Read structured news context.
    Prefer structured_payload, fall back to raw_payload (only contains raw news list).

    Return format:
    {
        "macro_signals": [...],   # LLM-structured macro signals
        "ticker_signals": {...},  # LLM-structured ticker signals
        "noise_filtered": int,
        "data_gaps": [...],
        "_stale_warning": str,    # optional, freshness warning
        "_fallback": bool,        # whether this is a fallback response
    }
    """
    async with AsyncSessionLocal() as db:
        row = (await db.execute(
            select(MacroNewsCache).where(MacroNewsCache.id == 1)
        )).scalar_one_or_none()

    if not row:
        return {
            "macro_signals": [],
            "ticker_signals": {},
            "noise_filtered": 0,
            "data_gaps": ["no news cache"],
            "_fallback": True,
        }

    # Prefer structured version
    if row.structured_payload:
        try:
            structured = row.structured_payload
            if isinstance(structured, str):
                structured = json.loads(structured)

            # Check freshness (older than 4 hours considered stale)
            processed_at = structured.get("processed_at")
            if processed_at:
                try:
                    processed_dt = dt_cls.fromisoformat(processed_at)
                    age_seconds = (dt_cls.utcnow() - processed_dt).total_seconds()
                    age_hours = age_seconds / 3600
                    if age_hours > 4:
                        structured["_stale_warning"] = f"News has not been updated in {age_hours:.1f} hours"
                except (ValueError, TypeError):
                    pass

            return structured
        except (json.JSONDecodeError, TypeError) as e:
            logger.warning(f"structured_payload parse failed: {e}, falling back to raw_payload")

    # Fallback: use raw_payload (raw news list, format: [{headline, summary, source, datetime, ...}, ...]）
    if row.raw_payload:
        try:
            raw_news = row.raw_payload
            if isinstance(raw_news, str):
                raw_news = json.loads(raw_news)
            return {
                "macro_signals": [],
                "ticker_signals": {},
                "noise_filtered": 0,
                "data_gaps": ["structured data unavailable, raw cache only"],
                "_fallback": True,
                "_raw_news_count": len(raw_news) if isinstance(raw_news, list) else 0,
            }
        except (json.JSONDecodeError, TypeError):
            pass

    # Complete fallback: return empty structure to let RESEARCHER know data quality is poor
    return {
        "macro_signals": [],
        "ticker_signals": {},
        "noise_filtered": 0,
        "data_gaps": ["structured data unavailable, raw cache only"],
        "_fallback": True,
    }


def _format_calendar(events: list[dict]) -> str:
    if not events:
        return "(No high-impact economic events this week)"
    lines = []
    for e in events[:8]:
        impact = e.get("impact", "")
        evt    = e.get("event", "")
        when   = e.get("time", "") or e.get("date", "")
        lines.append(f"- [{impact}] {when} {evt}")
    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════
# Ticker news
# ═══════════════════════════════════════════════════════════════

async def _read_ticker_news(
    tickers: list[str],
) -> tuple[dict[str, list[dict]], dict[str, dict]]:
    """
    Read TickerNewsLibrary from last 48h, grouped by ticker.
    Returns (per_ticker_news, hard_risks_map).
    """
    if not tickers:
        return {}, {}

    cutoff = int(time.time()) - NEWS_LOOKBACK_SECONDS

    async with AsyncSessionLocal() as db:
        stmt = (
            select(TickerNewsLibrary)
            .where(TickerNewsLibrary.ticker.in_(tickers))
            .where(TickerNewsLibrary.datetime_utc >= cutoff)
            .order_by(desc(TickerNewsLibrary.datetime_utc))
        )
        rows = (await db.execute(stmt)).scalars().all()

    per_ticker: dict[str, list[dict]] = {}
    hard_risks_map: dict[str, dict] = {}

    for r in rows:
        per_ticker.setdefault(r.ticker, []).append({
            "headline":    r.headline,
            "source":      r.source,
            "source_api":  r.source_api or "finnhub",
            "llm_summary": r.llm_summary,
            "sentiment":   r.sentiment,
            "relevance":   r.relevance,
            "credibility": r.credibility,
            "datetime":    r.datetime_utc,
            "is_hard_event": bool(r.is_hard_event),
        })
        # Keep hard_risks from latest ticker entry (rows already sorted by datetime_utc desc)
        if r.ticker not in hard_risks_map and r.hard_risks:
            hard_risks_map[r.ticker] = r.hard_risks

    # Keep max 5 items per ticker
    for t in per_ticker:
        per_ticker[t] = per_ticker[t][:5]

    return per_ticker, hard_risks_map


# ═══════════════════════════════════════════════════════════════
# Quant key facts
# ═══════════════════════════════════════════════════════════════

def _compute_key_facts(holdings: list[dict], portfolio: dict) -> dict[str, Any]:
    """
    Compute quant metrics:
      - breadth_pct      : positive momentum ETF ratio (mom_60d > 0)
      - spy_mom_60d      : SPY 60-day momentum
      - avg_atr_pct      : average ATR% across all ETFs
      - risk_on_score    : (XLK+XLY+XLC+XLI) − (XLP+XLU+XLV+XLRE) mom_60d spread
      - drawdown_pct     : portfolio.current_drawdown_pct
      - top5 / bottom5   : sorted by mom_60d
    """
    etf_rows = [
        h for h in holdings
        if (h.get("ticker") or "").upper() not in ("CASH", "")
    ]

    def _f(v) -> float | None:
        try:
            return float(v) if v is not None else None
        except (TypeError, ValueError):
            return None

    mom60 = {
        (h.get("ticker") or "").upper(): _f(h.get("mom_60d"))
        for h in etf_rows
    }
    mom60 = {k: v for k, v in mom60.items() if v is not None}

    breadth_pct = None
    if mom60:
        breadth_pct = round(sum(1 for v in mom60.values() if v > 0) / len(mom60), 4)

    spy_mom_60d = mom60.get("SPY")

    atrs = [_f(h.get("atr_pct")) for h in etf_rows]
    atrs = [a for a in atrs if a is not None]
    avg_atr_pct = round(sum(atrs) / len(atrs), 6) if atrs else None

    risk_on_val  = sum(v for k, v in mom60.items() if k in RISK_ON_SECTORS)
    risk_off_val = sum(v for k, v in mom60.items() if k in RISK_OFF_SECTORS)
    risk_on_score = round(risk_on_val - risk_off_val, 6) if mom60 else None

    drawdown_pct = None
    try:
        if portfolio and portfolio.get("current_drawdown_pct") is not None:
            drawdown_pct = round(float(portfolio["current_drawdown_pct"]), 6)
    except (TypeError, ValueError):
        pass

    ranked = sorted(mom60.items(), key=lambda kv: kv[1], reverse=True)
    top5    = [k for k, _ in ranked[:5]]
    bottom5 = [k for k, _ in ranked[-5:]]

    return {
        "breadth_pct":   breadth_pct,
        "spy_mom_60d":   round(spy_mom_60d, 6) if spy_mom_60d is not None else None,
        "avg_atr_pct":   avg_atr_pct,
        "risk_on_score": risk_on_score,
        "drawdown_pct":  drawdown_pct,
        "top5_momentum":    top5,
        "bottom5_momentum": bottom5,
        "n_etfs":        len(mom60),
    }


# ═══════════════════════════════════════════════════════════════
# Prose
# ═══════════════════════════════════════════════════════════════

def _build_prose(key_facts: dict, holdings: list[dict]) -> str:
    """Translate key_facts into human-readable prose."""
    parts: list[str] = []

    spy = key_facts.get("spy_mom_60d")
    if spy is not None:
        direction = "rising" if spy > 0.01 else ("falling" if spy < -0.01 else "sideways")
        parts.append(f"SPY 60d momentum {spy:+.2%}, trend {direction}.")

    breadth = key_facts.get("breadth_pct")
    n_etfs  = key_facts.get("n_etfs") or 0
    if breadth is not None:
        up = int(round(breadth * n_etfs))
        parts.append(f"Breadth {up}/{n_etfs} ({breadth:.0%}).")

    atr = key_facts.get("avg_atr_pct")
    if atr is not None:
        level = "low" if atr < 0.015 else ("mid" if atr < 0.025 else "high")
        parts.append(f"Avg ATR {atr:.2%} at {level} level.")

    risk_score = key_facts.get("risk_on_score")
    if risk_score is not None:
        bias = "risk-on" if risk_score > 0.005 else ("risk-off" if risk_score < -0.005 else "balanced")
        parts.append(f"Style rotation {bias} ({risk_score:+.2%}).")

    dd = key_facts.get("drawdown_pct")
    if dd is not None:
        safety = "safe zone" if dd > -0.05 else ("warning zone" if dd > -0.10 else "danger zone")
        parts.append(f"Account drawdown {dd:+.2%} {safety}.")

    top = key_facts.get("top5_momentum") or []
    bot = key_facts.get("bottom5_momentum") or []
    if top:
        parts.append(f"Momentum leaders: {' '.join(top)}.")
    if bot:
        parts.append(f"Momentum laggards: {' '.join(bot)}.")

    return " ".join(parts) if parts else "(Insufficient data to build brief)"
