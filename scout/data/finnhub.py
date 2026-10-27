"""Finnhub free-tier endpoints Scout reads directly (fundamentals, filings,
insiders, dated company news). Sync httpx wrapped in to_thread; failures
return empty results so a research page never breaks on a data hiccup."""
from __future__ import annotations

import asyncio
import logging
from datetime import date

import httpx

from scout.config import get_scout_settings

logger = logging.getLogger("scout.data.finnhub")
BASE = "https://finnhub.io/api/v1"


def _get(path: str, params: dict):
    token = get_scout_settings().finnhub_api_key
    if not token:
        return None
    for attempt in (1, 2):                      # one retry: the free tier times out now and then
        try:
            resp = httpx.get(BASE + path, params={**params, "token": token}, timeout=20)
            if resp.status_code != 200:
                logger.info("finnhub %s: HTTP %s", path, resp.status_code)
                return None
            return resp.json()
        except Exception as exc:
            if attempt == 2:
                logger.warning("finnhub %s failed: %s", path, exc)
    return None


async def get(path: str, **params):
    return await asyncio.to_thread(_get, path, params)


async def company_news(ticker: str, start: date, end: date) -> list[dict]:
    items = await get("/company-news", symbol=ticker, **{"from": start.isoformat(), "to": end.isoformat()})
    return items if isinstance(items, list) else []


async def filings(ticker: str, start: date, end: date) -> list[dict]:
    items = await get("/stock/filings", symbol=ticker, **{"from": start.isoformat(), "to": end.isoformat()})
    return items if isinstance(items, list) else []


async def insider_transactions(ticker: str, start: date) -> list[dict]:
    data = await get("/stock/insider-transactions", symbol=ticker, **{"from": start.isoformat()})
    return (data or {}).get("data") or [] if isinstance(data, dict) else []


async def metrics(ticker: str) -> dict:
    data = await get("/stock/metric", symbol=ticker, metric="all")
    return (data or {}).get("metric") or {} if isinstance(data, dict) else {}


async def recommendations(ticker: str) -> list[dict]:
    items = await get("/stock/recommendation", symbol=ticker)
    return items if isinstance(items, list) else []


async def earnings_history(ticker: str) -> list[dict]:
    items = await get("/stock/earnings", symbol=ticker)
    return items if isinstance(items, list) else []
