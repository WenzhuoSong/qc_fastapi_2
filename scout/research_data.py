"""v2 phase 1 data collection for the research pool, stored point in time.

`run_daily` (after the close): SEC filings and XBRL financials, estimates and
revisions, rating changes, ownership and short interest, FRED macro series,
and a price cross-check. `run_options` (last half hour of the session): one
options snapshot per ticker per day. Every source is isolated: a failure is
recorded in scout.data_quality and the rest continues.
"""
from __future__ import annotations

import asyncio
import logging
import time
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from scout import db, universe
from scout.data import finnhub, fred, options, sec, yahoo_research
from scout.market_calendar import us_equity_market_status

logger = logging.getLogger("scout.research_data")
ET = ZoneInfo("America/New_York")
YAHOO_CONCURRENCY = 4
PRICE_TOLERANCE = 0.005                  # 0.5 % between Yahoo's close and Finnhub's
FINNHUB_PACING_S = 1.1
CIK_REFRESH_DAYS = 7
FACT_FORMS = ("10-Q", "10-K", "10-Q/A", "10-K/A", "20-F", "40-F")
OPTIONS_WINDOW = ((15, 30), (16, 0))     # ET; Yahoo quotes are empty outside the session
DEFAULT_RATE = 0.04


async def _quality(asof: date, check: str, ticker: str | None, detail: dict) -> None:
    await db.execute("INSERT INTO scout.data_quality (asof, check_name, ticker, detail) VALUES (:d, :c, :t, CAST(:x AS JSONB))",
                     {"d": asof, "c": check, "t": ticker, "x": db.jsonb(detail)})


async def _snapshot(ticker: str, asof: date, kind: str, source: str, payload: dict) -> None:
    await db.execute(
        "INSERT INTO scout.research_snapshots (ticker, asof, kind, source, payload, fetched_at) "
        "VALUES (:t, :d, :k, :s, CAST(:p AS JSONB), now()) "
        "ON CONFLICT (ticker, asof, kind) DO UPDATE SET payload = EXCLUDED.payload, source = EXCLUDED.source, fetched_at = now()",
        {"t": ticker, "d": asof, "k": kind, "s": source, "p": db.jsonb(payload)})


# ── SEC ──

async def _ensure_ciks(tickers: list[str]) -> dict[str, str]:
    have = {r["ticker"]: r for r in await db.fetch_all("SELECT ticker, cik, updated_at FROM scout.sec_companies")}
    stale = [t for t in tickers if t not in have or (datetime.now(timezone.utc) - have[t]["updated_at"]).days >= CIK_REFRESH_DAYS]
    if stale:
        mapping = await sec.cik_map()
        rows = [{"t": t, "c": mapping[t][0], "n": mapping[t][1]} for t in stale if t in mapping]
        if rows:
            await db.execute_many(
                "INSERT INTO scout.sec_companies (ticker, cik, name, updated_at) VALUES (:t, :c, :n, now()) "
                "ON CONFLICT (ticker) DO UPDATE SET cik = EXCLUDED.cik, name = EXCLUDED.name, updated_at = now()", rows)
        have.update({r["t"]: {"cik": r["c"]} for r in rows})
    return {t: have[t]["cik"] for t in tickers if t in have}


async def sec_company(ticker: str, cik: str, asof: date) -> dict:
    sub = await sec.submissions(cik)
    if not sub:
        await _quality(asof, "sec_submissions_missing", ticker, {"cik": cik})
        return {"filings": 0, "facts": 0}
    found = sec.filings(sub, ticker, cik)
    if found:
        await db.execute_many(
            "INSERT INTO scout.sec_filings (accession, ticker, cik, form, filed, report_date, items, url) "
            "VALUES (:accession, :ticker, :cik, :form, :filed, :report_date, :items, :url) ON CONFLICT (accession) DO NOTHING",
            found)
    latest_fact_filing = next((f["accession"] for f in found if f["form"] in FACT_FORMS), None)
    row = await db.fetch_one("SELECT facts_accession FROM scout.sec_companies WHERE ticker = :t", {"t": ticker})
    n_facts = 0
    if latest_fact_filing and (row or {}).get("facts_accession") != latest_fact_filing:
        facts = await sec.company_facts(cik)
        rows = sec.financial_rows(facts, ticker)
        if rows:
            n_facts = await db.execute_many(
                "INSERT INTO scout.financials (ticker, metric, period_end, period_start, value, fy, fp, form, filed, derived, concept) "
                "VALUES (:ticker, :metric, :period_end, :period_start, :value, :fy, :fp, :form, :filed, :derived, :concept) "
                "ON CONFLICT (ticker, metric, period_end, filed) DO NOTHING", rows)
            await db.execute("UPDATE scout.sec_companies SET facts_accession = :a, facts_fetched_at = now() WHERE ticker = :t",
                             {"a": latest_fact_filing, "t": ticker})
        else:
            await _quality(asof, "sec_facts_empty", ticker, {"cik": cik})
    return {"filings": len(found), "facts": n_facts}


# ── Yahoo research ──

async def yahoo_company(ticker: str, asof: date, sem: asyncio.Semaphore) -> dict:
    async with sem:
        est = await asyncio.to_thread(yahoo_research.estimates, ticker)
        own = await asyncio.to_thread(yahoo_research.ownership, ticker)
        acts = await asyncio.to_thread(yahoo_research.analyst_actions, ticker)
    if est:
        await _snapshot(ticker, asof, "estimates", "yahoo", est)
    if own:
        await _snapshot(ticker, asof, "ownership", "yahoo", own)
    if acts:
        await db.execute_many(
            "INSERT INTO scout.analyst_actions (ticker, action_at, firm, to_grade, from_grade, action, pt_action, price_target, prior_target) "
            "VALUES (:ticker, :action_at, :firm, :to_grade, :from_grade, :action, :pt_action, :price_target, :prior_target) "
            "ON CONFLICT (ticker, action_at, firm) DO NOTHING", [{"ticker": ticker, **a} for a in acts])
    missing = [k for k, v in (("estimates", est), ("ownership", own)) if not v]
    if missing:
        await _quality(asof, "yahoo_missing", ticker, {"missing": missing})
    return {"estimates": bool(est), "ownership": bool(own), "actions": len(acts)}


# ── Macro and cross-checks ──

async def macro() -> int:
    total = 0
    for sid in fred.SERIES:
        last = await db.fetch_one("SELECT max(date) AS d FROM scout.macro_series WHERE series = :s", {"s": sid})
        start = (last or {}).get("d")
        rows = await fred.series(sid, start - timedelta(days=10) if start else date(2000, 1, 1))
        if rows:
            total += await db.execute_many(
                "INSERT INTO scout.macro_series (series, date, value) VALUES (:s, :d, :v) "
                "ON CONFLICT (series, date) DO UPDATE SET value = EXCLUDED.value",
                [{"s": sid, "d": d, "v": v} for d, v in rows])
    return total


async def price_crosscheck(tickers: list[str], asof: date) -> dict:
    """Yahoo daily close (stored bars) against Finnhub's quote for the same session."""
    bars = {r["ticker"]: r for r in await db.fetch_all(
        "SELECT DISTINCT ON (ticker) ticker, date, close FROM scout.bars_daily WHERE ticker = ANY(:t) ORDER BY ticker, date DESC",
        {"t": tickers})}
    checked, off = 0, []
    for t in tickers:
        b = bars.get(t)
        q = await finnhub.get("/quote", symbol=t)
        await asyncio.sleep(FINNHUB_PACING_S)
        if not b or not isinstance(q, dict) or not q.get("c") or not q.get("t"):
            continue
        q_day = datetime.fromtimestamp(q["t"], ET).date()
        ref = q["c"] if q_day == b["date"] else q.get("pc") if q_day > b["date"] else None
        if not ref:
            continue
        checked += 1
        diff = b["close"] / ref - 1
        if abs(diff) > PRICE_TOLERANCE:
            off.append({"ticker": t, "date": str(b["date"]), "yahoo": round(b["close"], 4), "finnhub": ref, "diff": round(diff, 5)})
    for o in off:
        await _quality(asof, "price_mismatch", o["ticker"], o)
    return {"checked": checked, "mismatched": len(off)}


async def run_daily(limit: int | None = None) -> dict:
    t0 = time.time()
    asof = date.fromisoformat(us_equity_market_status().market_date)
    pool = await universe.research_pool()
    if limit:
        pool = pool[:limit]
    summary: dict = {"asof": asof.isoformat(), "pool": len(pool)}
    ciks = await _ensure_ciks(pool)
    summary["no_cik"] = sorted(set(pool) - set(ciks))
    sec_stats = {"filings": 0, "facts": 0}
    for t, cik in ciks.items():
        try:
            r = await sec_company(t, cik, asof)
            sec_stats = {k: sec_stats[k] + r[k] for k in sec_stats}
        except Exception as exc:
            logger.warning("SEC %s failed: %s", t, exc)
            await _quality(asof, "sec_failed", t, {"error": repr(exc)[:300]})
    summary["sec"] = sec_stats
    sem = asyncio.Semaphore(YAHOO_CONCURRENCY)
    results = await asyncio.gather(*(yahoo_company(t, asof, sem) for t in pool), return_exceptions=True)
    summary["yahoo"] = {"estimates": sum(1 for r in results if isinstance(r, dict) and r["estimates"]),
                        "ownership": sum(1 for r in results if isinstance(r, dict) and r["ownership"]),
                        "actions": sum(r["actions"] for r in results if isinstance(r, dict)),
                        "failed": sum(1 for r in results if not isinstance(r, dict))}
    try:
        summary["macro_rows"] = await macro()
    except Exception as exc:
        summary["macro_rows"] = f"failed: {type(exc).__name__}"
        await _quality(asof, "fred_failed", None, {"error": repr(exc)[:300]})
    summary["price_check"] = await price_crosscheck(pool, asof)
    summary["storage"] = await storage_check(asof)
    summary["seconds"] = round(time.time() - t0)
    return summary


STORAGE_WARN = 0.8


async def storage_check(asof: date) -> dict:
    """Everything is kept by design; warn (once a day) when the volume needs expanding."""
    from scout.config import get_scout_settings

    row = await db.fetch_one("SELECT pg_database_size(current_database()) AS b")
    used_gb = row["b"] / 1e9
    cap = get_scout_settings().scout_db_volume_gb
    out = {"used_gb": round(used_gb, 2), "volume_gb": cap, "share": round(used_gb / cap, 3)}
    if used_gb / cap >= STORAGE_WARN:
        await _quality(asof, "storage_high", None, out)
        from scout import telegram

        await telegram.send(f"💾 Scout 数据库已用 {used_gb:.2f} GB / {cap:g} GB（{used_gb / cap:.0%}）。"
                            "按约定所有快照和分析都完整保留，请在 Railway 扩容 Postgres 卷，并更新 SCOUT_DB_VOLUME_GB。")
    return out


# ── Options ──

async def _rate() -> float:
    row = await db.fetch_one("SELECT value FROM scout.macro_series WHERE series = 'DGS3MO' ORDER BY date DESC LIMIT 1")
    return (row["value"] / 100) if row else DEFAULT_RATE


def in_options_window(now: datetime | None = None) -> bool:
    et = (now or datetime.now(ET)).astimezone(ET)
    return OPTIONS_WINDOW[0] <= (et.hour, et.minute) < OPTIONS_WINDOW[1]


async def options_company(ticker: str, today: date, r: float, sem: asyncio.Semaphore) -> str:
    async with sem:
        spot, chains = await asyncio.to_thread(yahoo_research.option_chains, ticker, today)
    if not spot or not chains:
        return "none"
    summary = options.summarize(chains, spot, today, r)
    summary["rate"] = r
    await _snapshot(ticker, today, "options", "yahoo", summary)
    if summary["quality"] != "ok":
        await _quality(today, "options_thin", ticker, {"valid_front": summary["expiries"].get("front", {}).get("valid_quotes")})
    return summary["quality"]


async def run_options(force: bool = False) -> dict:
    status = us_equity_market_status()
    if not force and (not status.is_open or not in_options_window()):
        return {"skipped": "outside the 15:30–16:00 ET window"}
    today = date.fromisoformat(status.market_date)
    done = await db.fetch_one("SELECT count(*) AS n FROM scout.research_snapshots WHERE kind = 'options' AND asof = :d", {"d": today})
    if done and done["n"] and not force:
        return {"skipped": "already captured", "rows": done["n"]}
    pool = list(universe.OPTIONS_MARKET_ETFS) + await universe.research_pool()
    r = await _rate()
    sem = asyncio.Semaphore(YAHOO_CONCURRENCY)
    res = await asyncio.gather(*(options_company(t, today, r, sem) for t in pool), return_exceptions=True)
    return {"asof": today.isoformat(), "pool": len(pool), "ok": res.count("ok"), "thin": res.count("thin"),
            "none": res.count("none"), "failed": sum(1 for x in res if not isinstance(x, str))}


# ── Reading back, with freshness ──
MAX_AGE_DAYS = {"estimates": 3, "ownership": 7, "options": 3}


async def latest(ticker: str, kind: str, asof: date | None = None) -> dict | None:
    """Newest snapshot on or before `asof` (point in time), flagged stale when too old."""
    row = await db.fetch_one(
        "SELECT asof, source, payload, fetched_at FROM scout.research_snapshots WHERE ticker = :t AND kind = :k "
        "AND asof <= :d ORDER BY asof DESC LIMIT 1", {"t": ticker, "k": kind, "d": asof or date.today()})
    if not row:
        return None
    age = ((asof or date.today()) - row["asof"]).days
    return {**row, "age_days": age, "stale": age > MAX_AGE_DAYS.get(kind, 3)}


async def status() -> dict:
    """Coverage and health of every phase-1 source, for the data page."""
    snaps = await db.fetch_all(
        "SELECT kind, max(asof) AS last, count(DISTINCT ticker) FILTER (WHERE asof = (SELECT max(asof) FROM scout.research_snapshots s2 "
        "WHERE s2.kind = s.kind)) AS tickers, count(*) AS rows FROM scout.research_snapshots s GROUP BY kind")
    fin = await db.fetch_one("SELECT count(DISTINCT ticker) AS tickers, count(*) AS rows, max(created_at) AS last FROM scout.financials")
    fil = await db.fetch_one("SELECT count(DISTINCT ticker) AS tickers, count(*) AS rows, max(filed) AS last FROM scout.sec_filings")
    act = await db.fetch_one("SELECT count(DISTINCT ticker) AS tickers, count(*) AS rows, max(action_at) AS last FROM scout.analyst_actions")
    mac = await db.fetch_all("SELECT series, max(date) AS last, count(*) AS rows FROM scout.macro_series GROUP BY series ORDER BY series")
    issues = await db.fetch_all(
        "SELECT asof, check_name, count(*) AS n, array_agg(DISTINCT ticker) FILTER (WHERE ticker IS NOT NULL) AS tickers "
        "FROM scout.data_quality WHERE asof >= current_date - 14 GROUP BY asof, check_name ORDER BY asof DESC, n DESC")
    runs = await db.fetch_all(
        "SELECT DISTINCT ON (job) job, status, started_at, finished_at, summary FROM scout.job_runs "
        "WHERE job IN ('research', 'options', 'eod', 'premarket', 'intraday') ORDER BY job, id DESC")
    size = await db.fetch_one("SELECT pg_database_size(current_database()) AS b")
    from scout.config import get_scout_settings

    return {"snapshots": snaps, "financials": fin, "filings": fil, "actions": act, "macro": mac, "issues": issues,
            "runs": runs, "pool": len(await universe.research_pool()),
            "storage": {"used_gb": round(size["b"] / 1e9, 2), "volume_gb": get_scout_settings().scout_db_volume_gb}}
