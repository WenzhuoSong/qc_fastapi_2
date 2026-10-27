"""FRED (St. Louis Fed) daily and monthly series via the keyless CSV download."""
from __future__ import annotations

import csv
import io
import logging
from datetime import date

import httpx

logger = logging.getLogger("scout.data.fred")

# series id → Chinese name (shown wherever the value is shown)
SERIES = {
    "DGS3MO": "3 个月美债收益率", "DGS2": "2 年期美债收益率", "DGS10": "10 年期美债收益率",
    "T10Y2Y": "10 年 − 2 年期利差", "DFF": "联邦基金有效利率", "DTWEXBGS": "美元指数（广义）",
    "DCOILWTICO": "WTI 原油", "VIXCLS": "VIX", "BAMLH0A0HYM2": "高收益债利差",
    "CPIAUCSL": "CPI（月）", "UNRATE": "失业率（月）",
}


async def series(series_id: str, start: date | None = None) -> list[tuple[date, float]]:
    url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}"
    if start:
        url += f"&cosd={start.isoformat()}"
    try:
        async with httpx.AsyncClient(timeout=60, follow_redirects=True) as client:
            resp = await client.get(url)
        resp.raise_for_status()
    except Exception as exc:
        logger.warning("FRED %s failed: %s", series_id, exc)
        return []
    return parse(resp.text)


def parse(text: str) -> list[tuple[date, float]]:
    rows = list(csv.reader(io.StringIO(text)))
    out = []
    for r in rows[1:]:
        if len(r) >= 2 and r[1] not in ("", "."):
            try:
                out.append((date.fromisoformat(r[0]), float(r[1])))
            except ValueError:
                continue
    return out
