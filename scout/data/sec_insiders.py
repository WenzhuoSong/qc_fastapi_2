"""SEC Insider Transactions Data Sets (Forms 3/4/5, quarterly zips from 2006 Q1).

https://www.sec.gov/data-research/sec-markets-data/insider-transactions-data-sets
Only non-derivative open-market trades (transaction code P or S) of the issuers we follow are
kept. A zip is streamed to a temporary file and parsed in place; nothing is executed from it.
"""
from __future__ import annotations

import csv
import io
import logging
import tempfile
import zipfile
from datetime import date, datetime

import httpx

from scout.data.sec import USER_AGENT

logger = logging.getLogger("scout.data.sec_insiders")

URLS = ("https://www.sec.gov/files/structureddata/data/insider-transactions-data-sets/{p}_form345.zip",
        "https://www.sec.gov/files/datastandardsinnovation/data/insider-transactions-data-sets/{p}_form345.zip")
CODES = {"P", "S"}
_NOT_COMMON = ("preferred", "note", "debenture", "warrant", "unit", "right", "depositary", "bond")


def is_common(title: str | None) -> bool:
    t = (title or "").lower()
    return ("common" in t or "ordinary" in t or t.startswith("class ") or t in ("", "shares")) and not any(x in t for x in _NOT_COMMON)
csv.field_size_limit(10_000_000)


def period(d: date) -> str:
    return f"{d.year}q{(d.month - 1) // 3 + 1}"


def periods(start: date, end: date) -> list[str]:
    out, y, q = [], start.year, (start.month - 1) // 3 + 1
    while (y, q) <= (end.year, (end.month - 1) // 3 + 1):
        out.append(f"{y}q{q}")
        y, q = (y + 1, 1) if q == 4 else (y, q + 1)
    return out


def _date(s: str) -> date | None:
    s = (s or "").strip()
    for fmt in ("%d-%b-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _num(s: str) -> float | None:
    try:
        return float(s) if s not in (None, "") else None
    except ValueError:
        return None


def _rows(z: zipfile.ZipFile, name: str):
    with z.open(name) as f:
        yield from csv.DictReader(io.TextIOWrapper(f, encoding="utf-8", errors="replace"), delimiter="\t")


def parse(z: zipfile.ZipFile, cik_to_ticker: dict[str, str]) -> list[dict]:
    """Open-market trades of the issuers in `cik_to_ticker` (10-digit CIK → ticker)."""
    subs = {}
    for r in _rows(z, "SUBMISSION.tsv"):
        cik = (r.get("ISSUERCIK") or "").strip().zfill(10)
        if cik in cik_to_ticker and (r.get("DOCUMENT_TYPE") or "").startswith("4"):
            subs[r["ACCESSION_NUMBER"]] = {"cik": cik, "filed": _date(r.get("FILING_DATE")),
                                           "plan": {"1": True, "0": False}.get((r.get("AFF10B5ONE") or "").strip())}
    if not subs:
        return []
    owners = {}
    for r in _rows(z, "REPORTINGOWNER.tsv"):
        acc = r["ACCESSION_NUMBER"]
        if acc in subs and acc not in owners:          # the first reporting owner of the filing
            owners[acc] = {"cik": (r.get("RPTOWNERCIK") or "").strip().zfill(10), "name": r.get("RPTOWNERNAME"),
                           "rel": r.get("RPTOWNER_RELATIONSHIP"), "title": r.get("RPTOWNER_TITLE")}
    out = []
    for r in _rows(z, "NONDERIV_TRANS.tsv"):
        acc, code = r["ACCESSION_NUMBER"], (r.get("TRANS_CODE") or "").strip()
        s = subs.get(acc)
        if not s or code not in CODES or not s["filed"]:
            continue
        shares, price = _num(r.get("TRANS_SHARES")), _num(r.get("TRANS_PRICEPERSHARE"))
        o = owners.get(acc) or {}
        out.append({"accession": acc, "trans_sk": int(r["NONDERIV_TRANS_SK"]), "ticker": cik_to_ticker[s["cik"]],
                    "issuer_cik": s["cik"], "filed": s["filed"], "trans_date": _date(r.get("TRANS_DATE")), "code": code,
                    "security": r.get("SECURITY_TITLE"), "common": is_common(r.get("SECURITY_TITLE")),
                    "shares": shares, "price": price, "value": shares * price if shares and price else None,
                    "owner_cik": o.get("cik"), "owner_name": o.get("name"), "relationship": o.get("rel"),
                    "title": o.get("title"), "plan_10b5_1": s["plan"]})
    return out


async def fetch(p: str, cik_to_ticker: dict[str, str]) -> list[dict] | None:
    """Trades of one quarter, or None when the quarter is not published (yet)."""
    async with httpx.AsyncClient(timeout=180, headers={"User-Agent": USER_AGENT}, follow_redirects=True) as client:
        for url in URLS:
            with tempfile.TemporaryFile() as tmp:
                async with client.stream("GET", url.format(p=p)) as resp:
                    if resp.status_code in (403, 404):          # not published (yet) at this path
                        continue
                    resp.raise_for_status()
                    async for chunk in resp.aiter_bytes():
                        tmp.write(chunk)
                tmp.seek(0)
                with zipfile.ZipFile(tmp) as z:
                    return parse(z, cik_to_ticker)
    return None
