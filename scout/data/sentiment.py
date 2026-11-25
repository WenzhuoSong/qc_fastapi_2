"""Free sentiment data, read-only public files: CBOE's daily options statistics (put/call ratios, JSON per session,
from 2019-10) and the AAII investor sentiment survey (weekly, an .xls with the history since 1987)."""
from __future__ import annotations

import io
import logging
from datetime import date

import httpx
import pandas as pd

logger = logging.getLogger("scout.data.sentiment")

UA = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/537.36 Chrome/120 Safari/537.36"}
CBOE = "https://cdn.cboe.com/data/us/options/market_statistics/daily/{d}_daily_options"
CBOE_START = date(2019, 10, 7)            # earlier sessions are refused (403)
CBOE_RATIOS = {"TOTAL PUT/CALL RATIO": "total", "EQUITY PUT/CALL RATIO": "equity", "INDEX PUT/CALL RATIO": "index"}
AAII = "https://www.aaii.com/files/surveys/sentiment.xls"


def parse_cboe(data: dict) -> dict[str, float]:
    out = {}
    for r in data.get("ratios") or []:
        key = CBOE_RATIOS.get((r.get("name") or "").upper())
        try:
            if key:
                out[key] = float(r["value"])
        except (KeyError, TypeError, ValueError):
            continue
    return out


async def cboe_day(client: httpx.AsyncClient, d: date) -> dict[str, float] | None:
    """{total, equity, index} put/call ratios of one session; None when CBOE has no file for it."""
    try:
        resp = await client.get(CBOE.format(d=d.isoformat()), headers=UA)
    except Exception as exc:
        logger.warning("cboe %s failed: %s", d, exc)
        return None
    if resp.status_code != 200:
        return None
    try:
        return parse_cboe(resp.json()) or None
    except ValueError:
        return None


def parse_aaii(raw: bytes) -> list[tuple[date, float, float, float]]:
    """(survey date, bullish, neutral, bearish) as shares; rows without all three are skipped."""
    df = pd.read_excel(io.BytesIO(raw), "SENTIMENT", header=None, engine="xlrd")
    when = pd.to_datetime(df[0], errors="coerce", format="mixed")
    out = []
    for d, b, n, s in zip(when, *(pd.to_numeric(df[i], errors="coerce") for i in (1, 2, 3))):
        if pd.notna(d) and pd.notna(b) and pd.notna(n) and pd.notna(s):
            out.append((d.date(), float(b), float(n), float(s)))
    return out


async def aaii() -> list[tuple[date, float, float, float]]:
    try:
        async with httpx.AsyncClient(timeout=60, follow_redirects=True) as client:
            resp = await client.get(AAII, headers=UA)
        resp.raise_for_status()
        return parse_aaii(resp.content)
    except Exception as exc:
        logger.warning("aaii failed: %s", exc)
        return []
