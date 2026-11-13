"""BLS public API v2, keyless (25 queries a day; the pre-market brief makes one): official numbers on release day.
Forex Factory's feed carries forecast and previous but never the actual, so BLS releases are read here."""
from __future__ import annotations

import logging
from datetime import date

import httpx

logger = logging.getLogger("scout.data.bls")

URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"

# Forex Factory title → (BLS series, measure). mom / yoy: % change on the month / the same month a year before (the
# headline y/y uses the unadjusted index); diff_k: monthly change in thousands; level: the value in %.
RELEASES = {
    "CPI m/m": ("CUSR0000SA0", "mom"), "Core CPI m/m": ("CUSR0000SA0L1E", "mom"), "CPI y/y": ("CUUR0000SA0", "yoy"),
    "PPI m/m": ("WPSFD4", "mom"), "Core PPI m/m": ("WPSFD49104", "mom"), "PPI y/y": ("WPUFD4", "yoy"),
    "Non-Farm Employment Change": ("CES0000000001", "diff_k"), "Unemployment Rate": ("LNS14000000", "level"),
    "Average Hourly Earnings m/m": ("CES0500000003", "mom"),
}


async def monthly(series_ids: list[str], start_year: int, end_year: int) -> dict[str, dict[tuple[int, int], float]]:
    """{series: {(year, month): value}}; {} on failure."""
    body = {"seriesid": series_ids, "startyear": str(start_year), "endyear": str(end_year)}
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(URL, json=body)
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        logger.warning("BLS failed: %s", exc)
        return {}
    if data.get("status") != "REQUEST_SUCCEEDED":
        logger.warning("BLS refused: %s", data.get("message"))
        return {}
    return parse(data)


def parse(data: dict) -> dict[str, dict[tuple[int, int], float]]:
    out: dict[str, dict[tuple[int, int], float]] = {}
    for s in (data.get("Results") or {}).get("series") or []:
        rows = out.setdefault(s.get("seriesID"), {})
        for x in s.get("data") or []:
            p = x.get("period") or ""
            if not p.startswith("M") or p == "M13":
                continue
            try:
                rows[(int(x["year"]), int(p[1:]))] = float(x["value"])
            except (KeyError, ValueError):
                continue
    return out


def reference_month(release: date) -> tuple[int, int]:
    """The month a monthly BLS release reports: the one before the release month."""
    return (release.year - 1, 12) if release.month == 1 else (release.year, release.month - 1)


def actual(title: str, data: dict[str, dict[tuple[int, int], float]], release: date) -> str | None:
    """The released value formatted like Forex Factory's forecast ("0.3%", "119K"); None when the release month's
    observation is not in BLS yet (never last month's number in its place)."""
    if title not in RELEASES:
        return None
    sid, how = RELEASES[title]
    rows = data.get(sid) or {}
    y, m = reference_month(release)
    now = rows.get((y, m))
    if now is None:
        return None
    if how == "level":
        return f"{now:.1f}%"
    before = rows.get((y - 1, m) if how == "yoy" else ((y - 1, 12) if m == 1 else (y, m - 1)))
    if before is None or (how != "diff_k" and before <= 0):
        return None
    if how == "diff_k":
        return f"{now - before:.0f}K"
    x = round((now / before - 1) * 100, 1)
    return f"{x + 0.0:.1f}%"                                          # + 0.0: no "-0.0%"


def versus(actual_text: str | None, forecast: str | None) -> str | None:
    """高于预期 / 符合预期 / 低于预期 on the numbers as printed."""
    def num(s):
        try:
            return float(str(s).strip().rstrip("%KkMB"))
        except (TypeError, ValueError):
            return None
    a, f = num(actual_text), num(forecast)
    if a is None or f is None:
        return None
    return "符合预期" if a == f else ("高于预期" if a > f else "低于预期")
