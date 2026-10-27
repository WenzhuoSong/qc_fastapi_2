"""SEC EDGAR (official, free): ticker → CIK, filings index, XBRL financials.

EDGAR asks for a descriptive User-Agent with a contact and at most 10
requests per second. XBRL company facts mix single quarters with year-to-date
and full-year values (cash flow statements are year-to-date only), so
`quarterly` derives single quarters: direct 3-month values first, otherwise
the difference of consecutive cumulative values sharing a start date
(Q2 = 6M − Q1, Q4 = FY − 9M).
"""
from __future__ import annotations

import asyncio
import logging
import time
from datetime import date

import httpx

logger = logging.getLogger("scout.data.sec")

USER_AGENT = "Scout research (single user) wenzhuosong1996@outlook.com"
MIN_INTERVAL_S = 0.12            # ≤ 10 requests per second
QUARTER_DAYS = (80, 100)
_lock = asyncio.Lock()
_last = 0.0

# metric → (XBRL concepts in priority order, kind). Flow metrics are additive over
# time (quarters can be derived from year-to-date values); per-share and share
# counts are not, so they are taken only as reported for the quarter.
METRICS: dict[str, tuple[tuple[str, ...], str]] = {
    "revenue": (("RevenueFromContractWithCustomerExcludingAssessedTax", "Revenues", "SalesRevenueNet",
                 "RevenueFromContractWithCustomerIncludingAssessedTax"), "flow"),
    "cost_of_revenue": (("CostOfRevenue", "CostOfGoodsAndServicesSold", "CostOfGoodsSold"), "flow"),
    "gross_profit": (("GrossProfit",), "flow"),
    "rnd": (("ResearchAndDevelopmentExpense",), "flow"),
    "sga": (("SellingGeneralAndAdministrativeExpense",), "flow"),
    "operating_income": (("OperatingIncomeLoss",), "flow"),
    "net_income": (("NetIncomeLoss", "ProfitLoss"), "flow"),
    "operating_cash_flow": (("NetCashProvidedByUsedInOperatingActivities",
                             "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations"), "flow"),
    "capex": (("PaymentsToAcquirePropertyPlantAndEquipment", "PaymentsToAcquireProductiveAssets"), "flow"),
    "buybacks": (("PaymentsForRepurchaseOfCommonStock",), "flow"),
    "eps_diluted": (("EarningsPerShareDiluted",), "per_share"),
    "shares_diluted": (("WeightedAverageNumberOfDilutedSharesOutstanding",), "per_share"),
    "cash": (("CashAndCashEquivalentsAtCarryingValue",
              "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents"), "instant"),
    "short_term_investments": (("ShortTermInvestments", "MarketableSecuritiesCurrent"), "instant"),
    "total_assets": (("Assets",), "instant"),
    "total_liabilities": (("Liabilities",), "instant"),
    "long_term_debt": (("LongTermDebtNoncurrent", "LongTermDebt"), "instant"),
    "current_debt": (("LongTermDebtCurrent", "DebtCurrent"), "instant"),
    "equity": (("StockholdersEquity",
                "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"), "instant"),
    "inventory": (("InventoryNet",), "instant"),
}
# IFRS filers (TSMC, many ADRs) use the ifrs-full taxonomy, often annual only (20-F).
IFRS_METRICS: dict[str, tuple[str, ...]] = {
    "revenue": ("Revenue", "RevenueFromContractsWithCustomers"),
    "cost_of_revenue": ("CostOfSales",),
    "gross_profit": ("GrossProfit",),
    "rnd": ("ResearchAndDevelopmentExpense",),
    "operating_income": ("ProfitLossFromOperatingActivities",),
    "net_income": ("ProfitLossAttributableToOwnersOfParent", "ProfitLoss"),
    "operating_cash_flow": ("CashFlowsFromUsedInOperatingActivities",),
    "capex": ("PurchaseOfPropertyPlantAndEquipmentClassifiedAsInvestingActivities", "PurchaseOfPropertyPlantAndEquipment"),
    "eps_diluted": ("DilutedEarningsLossPerShare",),
    "cash": ("CashAndCashEquivalents",),
    "total_assets": ("Assets",),
    "total_liabilities": ("Liabilities",),
    "equity": ("EquityAttributableToOwnersOfParent", "Equity"),
    "inventory": ("Inventories",),
}
FILING_FORMS = {"8-K", "8-K/A", "10-K", "10-K/A", "10-Q", "10-Q/A", "4", "S-1", "S-3", "424B5", "424B3",
                "SC 13D", "SC 13D/A", "SC 13G", "SC 13G/A", "DEF 14A", "6-K", "20-F"}


async def _get(url: str) -> dict | None:
    global _last
    async with _lock:                                    # global pacing across all callers
        wait = MIN_INTERVAL_S - (time.monotonic() - _last)
        if wait > 0:
            await asyncio.sleep(wait)
        _last = time.monotonic()
    for attempt in (1, 2):
        try:
            async with httpx.AsyncClient(timeout=60, headers={"User-Agent": USER_AGENT}) as client:
                resp = await client.get(url)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            if attempt == 2:
                logger.warning("EDGAR %s failed: %s", url, exc)
            else:
                await asyncio.sleep(2)
    return None


async def cik_map() -> dict[str, tuple[str, str]]:
    """ticker → (10-digit CIK, company name). Class shares map BRK.B → BRK-B."""
    data = await _get("https://www.sec.gov/files/company_tickers.json") or {}
    return {v["ticker"].upper().replace(".", "-"): (f"{int(v['cik_str']):010d}", v["title"]) for v in data.values()}


async def submissions(cik: str) -> dict:
    return await _get(f"https://data.sec.gov/submissions/CIK{cik}.json") or {}


async def company_facts(cik: str) -> dict:
    return await _get(f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json") or {}


def filings(sub: dict, ticker: str, cik: str, forms: set[str] = FILING_FORMS) -> list[dict]:
    """Recent filings of interest from a submissions document."""
    r = (sub.get("filings") or {}).get("recent") or {}
    out = []
    for i, form in enumerate(r.get("form") or []):
        if form not in forms:
            continue
        acc = r["accessionNumber"][i]
        doc = (r.get("primaryDocument") or [""] * (i + 1))[i]
        out.append({
            "accession": acc, "ticker": ticker, "cik": cik, "form": form,
            "filed": date.fromisoformat(r["filingDate"][i]),
            "report_date": date.fromisoformat(r["reportDate"][i]) if (r.get("reportDate") or [""])[i] else None,
            "items": (r.get("items") or [""] * (i + 1))[i] or None,
            "url": f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc.replace('-', '')}/{doc}" if doc else None,
        })
    return out


# ── XBRL → quarters ──

def _days(start: str, end: str) -> int:
    return (date.fromisoformat(end) - date.fromisoformat(start)).days


def _latest(entries: list[dict]) -> dict[tuple, dict]:
    """One value per (start, end): the most recently filed."""
    out: dict[tuple, dict] = {}
    for e in entries:
        key = (e.get("start"), e["end"])
        if key not in out or e["filed"] > out[key]["filed"]:
            out[key] = e
    return out


def quarterly(entries: list[dict], kind: str) -> dict[str, dict]:
    """period_end → {start, value, fy, fp, form, filed, derived} for one concept."""
    latest = _latest(entries)
    if kind == "instant":
        return {end: {"start": None, "value": e["val"], "fy": e.get("fy"), "fp": e.get("fp"), "form": e.get("form"),
                      "filed": e["filed"], "derived": False} for (start, end), e in latest.items() if start is None}
    out: dict[str, dict] = {}
    for (start, end), e in latest.items():
        if start and QUARTER_DAYS[0] <= _days(start, end) <= QUARTER_DAYS[1]:
            out[end] = {"start": start, "value": e["val"], "fy": e.get("fy"), "fp": e.get("fp"), "form": e.get("form"),
                        "filed": e["filed"], "derived": False}
    if kind != "flow":
        return out
    by_start: dict[str, list[dict]] = {}
    for (start, end), e in latest.items():
        if start:
            by_start.setdefault(start, []).append(e)
    for start, group in by_start.items():
        group.sort(key=lambda e: e["end"])
        for prev, cur in zip(group, group[1:]):
            gap = _days(prev["end"], cur["end"])
            if cur["end"] in out or not (QUARTER_DAYS[0] <= gap <= QUARTER_DAYS[1]):
                continue
            out[cur["end"]] = {"start": prev["end"], "value": cur["val"] - prev["val"], "fy": cur.get("fy"),
                               "fp": "Q4" if cur.get("fp") == "FY" else cur.get("fp"), "form": cur.get("form"),
                               "filed": max(cur["filed"], prev["filed"]), "derived": True}
    return out


def pick_unit(units: dict, kind: str) -> str | None:
    """USD when reported in USD, else the reporting currency; per-share units for EPS / shares."""
    names = list(units)
    if kind == "per_share":
        return next((u for u in names if "/shares" in u and u.startswith("USD")), None) or \
            next((u for u in names if "/shares" in u or u == "shares"), None)
    if "USD" in names:
        return "USD"
    return next((u for u in names if len(u) == 3 and u.isalpha() and u.isupper()), None)


def financial_rows(facts: dict, ticker: str) -> list[dict]:
    """All METRICS as quarterly (or, for annual-only filers, yearly) rows; concepts merged by priority per period."""
    ns = (facts or {}).get("facts") or {}
    gaap, ifrs = ns.get("us-gaap") or {}, ns.get("ifrs-full") or {}
    rows = []
    for metric, (concepts, kind) in METRICS.items():
        merged: dict[str, tuple[str, str, dict]] = {}
        sources = [(gaap, c) for c in concepts] + [(ifrs, c) for c in IFRS_METRICS.get(metric, ())]
        for taxonomy, concept in sources:
            spec = taxonomy.get(concept)
            if not spec:
                continue
            unit = pick_unit(spec.get("units") or {}, kind)
            entries = (spec.get("units") or {}).get(unit) or []
            periods = quarterly(entries, kind)
            if not periods and kind != "instant":
                periods = annual(entries)
            for end, q in periods.items():
                merged.setdefault(end, (concept, unit, q))
        for end, (concept, unit, q) in merged.items():
            rows.append({"ticker": ticker, "metric": metric, "period_end": date.fromisoformat(end),
                         "period_start": date.fromisoformat(q["start"]) if q["start"] else None,
                         "value": float(q["value"]), "fy": q["fy"], "fp": q["fp"], "form": q["form"],
                         "filed": date.fromisoformat(q["filed"]), "derived": q["derived"], "concept": concept,
                         "unit": unit or "USD"})
    return rows


def annual(entries: list[dict]) -> dict[str, dict]:
    """Full-year values for filers without quarterly XBRL (20-F); fp = 'FY'."""
    out = {}
    for (start, end), e in _latest(entries).items():
        if start and 350 <= _days(start, end) <= 380:
            out[end] = {"start": start, "value": e["val"], "fy": e.get("fy"), "fp": "FY", "form": e.get("form"),
                        "filed": e["filed"], "derived": False}
    return out
