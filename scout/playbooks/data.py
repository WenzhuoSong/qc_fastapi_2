"""Load the playbooks' context from Scout's database (point in time: financials as filed by the as-of date).

Prices are the scan's stored bars (Yahoo, dividend-adjusted; the latest close equals the traded close, so today's
market value = close × today's diluted shares). Shares move onto today's split basis with Yahoo's split dates,
as in `scout.valuation`; a stock whose share count disagrees with its own EPS is left without market value.
"""
from __future__ import annotations

import logging
from datetime import date, timedelta

import numpy as np
import pandas as pd

from scout import db
from scout import valuation as val
from scout.playbooks.base import Ctx

logger = logging.getLogger("scout.playbooks")

LOOKBACK_DAYS = 520                     # ≥ 13 month ends (12-month momentum) and 290 sessions for the rankers
LONG_DAYS = 1160                        # closes only: 36 months + a margin for residual momentum
FLOWS = ("net_income", "operating_income", "revenue", "cost_of_revenue", "gross_profit", "operating_cash_flow",
         "eps_diluted", "buybacks", "capex", "rnd")
STOCKS = ("total_assets", "equity", "cash", "short_term_investments", "long_term_debt", "current_debt",
          "total_liabilities", "inventory")
# Fields the live SEC data lacks but the Sharadar history has (`full=True`): the playbooks then use the authors' formulas
FULL_FLOWS = ("ebitda", "dividends")
FULL_STOCKS = ("current_assets", "current_liabilities", "ppe_net")
YEAR = 365


# ── fundamentals (pure) ──

def _ttm_at(rows: list[tuple], cut: date) -> float | None:
    """Trailing four quarters (or the fiscal year) from periods ending on or before `cut`."""
    sub = [r for r in rows if r[0] <= cut]
    return val.trailing(sub, cut + timedelta(days=45)) if sub else None


def _level_at(rows: list[tuple], end: date, tol: int = 45) -> float | None:
    """The balance-sheet value at the period end closest to `end` (within tol days)."""
    best = min(rows, key=lambda r: abs((r[0] - end).days), default=None)
    return best[2] if best and abs((best[0] - end).days) <= tol else None


def fundamentals(rows: list[dict], close: pd.Series, splits: dict[str, list], asof: date,
                 caps: dict[str, float] | None = None, fin: dict | None = None, tickers: list[str] | None = None,
                 full: bool = False) -> pd.DataFrame:
    """ticker × fields from scout.financials rows (every filed version), as known on `asof`. A backtest passes the
    versions once (`fin`, as `valuation.versions` builds them) and the day's `tickers`. full (the Sharadar history):
    also FULL_FLOWS / FULL_STOCKS and ten years of trailing EPS, net income and dividends (Graham's rules)."""
    fin = fin if fin is not None else val.versions(rows)
    caps = caps or {}
    tickers = sorted({t for t, _ in fin}) if tickers is None else tickers
    out = {}
    for t in tickers:
        extra = (*FULL_FLOWS, *FULL_STOCKS) if full else ()
        get = {m: val.as_of(fin.get((t, m), []), asof) for m in (*FLOWS, *STOCKS, "shares_diluted", *extra)}
        quarters = [r for r in get["net_income"] if r[1] == "quarter"] or get["net_income"]
        if not quarters:
            continue
        last_end = quarters[-1][0]
        if (asof - last_end).days > val.FRESH["year"]:
            continue
        rec: dict[str, float | None] = {"last_end": last_end.toordinal()}
        for m in FLOWS:
            rec[m] = _ttm_at(get[m], asof)
            rec[m + "_prev"] = _ttm_at(get[m], last_end - timedelta(days=YEAR - 30))
        if rec["gross_profit"] is None and rec["revenue"] is not None and rec["cost_of_revenue"] is not None:
            rec["gross_profit"] = rec["revenue"] - rec["cost_of_revenue"]
            if rec["revenue_prev"] is not None and rec["cost_of_revenue_prev"] is not None:
                rec["gross_profit_prev"] = rec["revenue_prev"] - rec["cost_of_revenue_prev"]
        for m in STOCKS:
            rows_m = get[m]
            rec[m] = rows_m[-1][2] if rows_m and (asof - rows_m[-1][0]).days <= val.FRESH["year"] else None
            rec[m + "_prev"] = _level_at(rows_m, last_end - timedelta(days=YEAR))
        # trailing net income 0–4 years before the last period (trailing EPS is rarely complete: most filers give no
        # fourth-quarter EPS, so growth uses net income)
        rec["ni_years"] = [_ttm_at(get["net_income"], last_end - timedelta(days=k * YEAR - 30)) for k in range(5)]
        if full:
            for m in FULL_FLOWS:
                rec[m] = _ttm_at(get[m], asof)
            for m in FULL_STOCKS:
                rows_m = get[m]
                rec[m] = rows_m[-1][2] if rows_m and (asof - rows_m[-1][0]).days <= val.FRESH["year"] else None
                rec[m + "_prev"] = _level_at(rows_m, last_end - timedelta(days=YEAR))
            back = lambda m, n: [_ttm_at(get[m], last_end - timedelta(days=k * YEAR - 30)) for k in range(n)]  # noqa: E731
            rec["eps_years"], rec["ni_years10"], rec["div_years"] = back("eps_diluted", 11), back("net_income", 10), back("dividends", 10)
        eq = [r for r in get["eps_diluted"] if r[1] == "quarter"]
        if eq:
            prior = _level_at(eq, eq[-1][0] - timedelta(days=YEAR), tol=20)
            rec["eps_q"], rec["eps_q_prev_year"] = eq[-1][2], prior
        sh_rows = fin.get((t, "shares_diluted"), [])
        n = val.shares(sh_rows, splits.get(t, []), asof) if sh_rows else None
        rec["shares"] = n
        sh = val.as_of(sh_rows, asof)
        prev = _level_at(sh, last_end - timedelta(days=YEAR)) if sh else None
        if prev is not None:                                   # onto today's split basis, like the current count
            filed = next(r[3] for r in sh if abs((r[0] - (last_end - timedelta(days=YEAR))).days) <= 45)
            prev *= val.split_factor(splits.get(t, []), filed)
        rec["shares_prev"] = prev
        c = close.get(t)
        mcap = float(c) * n if n and c is not None and np.isfinite(c) else None
        if mcap is not None and not val.implied_ok(fin, t, n, asof):
            mcap = None                                        # share count disagrees with the company's own EPS
        if mcap is not None and caps.get(t) and not val.CHECK[0] <= mcap / caps[t] <= val.CHECK[1]:
            mcap = None
        rec["mcap"] = mcap
        if mcap is not None:
            debt = (rec["long_term_debt"] or 0.0) + (rec["current_debt"] or 0.0)
            cash = (rec["cash"] or 0.0) + (rec["short_term_investments"] or 0.0)
            rec["ev"] = mcap + debt - cash
        out[t] = rec
    return pd.DataFrame.from_dict(out, orient="index")


# ── loading ──

async def _angles(p, stocks: list[str], day) -> dict[str, dict[str, float]]:
    """The tested rankers' angle inputs today, as `scout.rankers.prepare` builds them for the gate."""
    from scout import angles

    out: dict[str, dict[str, float]] = {}
    try:
        pit = await angles.load_financials(stocks)
        pit.advance(day)
        keys = {"gross_profitability": "gross_profitability", "low_accruals": "accruals",
                "low_asset_growth": "asset_growth", "net_buyback": "net_issuance"}
        for name, need in keys.items():
            out[name] = {}
        for t in stocks:
            now = angles.fundamentals_now(pit, t, day)
            for name, need in keys.items():
                v = now.get(need)
                if v is not None and np.isfinite(v):
                    out[name][t] = float(v)
    except Exception as exc:                                   # one source failing leaves those playbooks empty
        logger.warning("playbooks: SEC angles failed: %s", exc)
    try:
        out["insider_cluster"] = {t: float(v["buyers"]) for t, v in
                                  angles.insider_buying(await angles.load_insider_buys(), day).items()}
    except Exception as exc:
        logger.warning("playbooks: insiders failed: %s", exc)
    try:
        i = len(p.dates) - 1
        out["ear_drift"] = {ev["ticker"]: float(ev["ear"]) for ev in angles.announcement_returns(await angles.load_earnings(), p)
                            if i - 21 < ev["known"] <= i}
    except Exception as exc:
        logger.warning("playbooks: earnings reactions failed: %s", exc)
    return out


async def load(asof: date | None = None, with_splits: bool = True) -> Ctx:
    from scout import pipeline, rankers, universe
    from scout.config import get_scout_settings
    from scout.features import build_panel
    from scout import bars

    settings = get_scout_settings()
    instruments = {r["ticker"]: r for r in await universe.load_scan_instruments()}
    since = (asof or date.today()) - timedelta(days=LOOKBACK_DAYS)
    frames = await bars.load(list(instruments), since=since)
    if asof:
        frames = {t: b[[d <= asof for d in b.index]] for t, b in frames.items()}
    p = build_panel(frames, instruments, settings.scout_min_price, settings.scout_min_dollar_volume)
    day = p.dates[-1]
    arrays = rankers.arrays(p)
    stocks_all = arrays[1]
    angle = await _angles(p, stocks_all, day)
    f = {}
    for k, frame in p.f.items():
        frame = frame.copy()
        frame.index = pd.to_datetime(frame.index)
        f[k] = frame
    liquid = f["liquid"].iloc[-1]
    stocks = [t for t in p.stocks if bool(liquid.get(t, False))]
    etfs = [t for t, r in instruments.items() if r["kind"] == "etf" and t in f["close"].columns]
    srcs = {r["ticker"]: r["sources"] or [] for r in await db.fetch_all(
        "SELECT ticker, sources FROM scout.instruments WHERE in_scan")}
    prior = {r["ticker"]: float(r["h"]) for r in await db.fetch_all(
        "SELECT ticker, max(high) AS h FROM scout.bars_daily WHERE date < :d GROUP BY ticker", {"d": since})}
    asof_d = pd.Timestamp(day).date()
    rows = await db.fetch_all(
        "SELECT ticker, metric, period_end, period_start, filed, value FROM scout.financials "
        "WHERE ticker = ANY(:t) AND metric = ANY(:m) AND period_end >= :s AND filed <= :d",
        {"t": stocks_all, "m": [*FLOWS, *STOCKS, "shares_diluted"], "s": asof_d - timedelta(days=7 * YEAR), "d": asof_d})
    snaps = {r["ticker"]: r["payload"] for r in await db.fetch_all(
        "SELECT DISTINCT ON (ticker) ticker, payload FROM scout.research_snapshots WHERE kind = 'ownership' "
        "AND asof BETWEEN :a AND :d ORDER BY ticker, asof DESC", {"a": asof_d - timedelta(days=10), "d": asof_d})}
    splits: dict[str, list] = {}
    if with_splits:
        try:
            from scout.data.yahoo import unadjusted_closes

            _, splits = await unadjusted_closes(stocks_all, asof_d - timedelta(days=2 * YEAR), "1mo")
        except Exception as exc:
            logger.warning("playbooks: split dates unavailable: %s", exc)
    caps = {t: s.get("marketCap") for t, s in snaps.items() if s.get("marketCap")}
    fund = fundamentals([dict(r) for r in rows], f["close"].iloc[-1], splits, asof_d, caps)
    long_rows = await db.fetch_all(
        "SELECT ticker, date, close FROM scout.bars_daily WHERE ticker = ANY(:t) AND date >= :d AND date <= :e",
        {"t": [*stocks_all, "SPY"], "d": asof_d - timedelta(days=LONG_DAYS), "e": asof_d})
    long_close = pd.DataFrame(long_rows).pivot(index="date", columns="ticker", values="close") if long_rows else pd.DataFrame()
    long_close.index = pd.to_datetime(long_close.index)
    macro = {}
    for r in await db.fetch_all("SELECT series, date, value FROM scout.macro_series WHERE series = ANY(:s) AND date >= :d "
                                "ORDER BY series, date", {"s": ["DFF", "DGS2", "DGS10", "VIXCLS", "DGS3MO"], "d": since}):
        macro.setdefault(r["series"], {})[pd.Timestamp(r["date"])] = float(r["value"])
    holdings = [dict(r) for r in await db.fetch_all(
        "SELECT ticker, opened_on, entry_price, shares FROM scout.trades WHERE closed_on IS NULL ORDER BY opened_on")]
    watch = [r["ticker"] for r in await db.fetch_all("SELECT ticker FROM scout.watchlist WHERE active ORDER BY ticker")]
    return Ctx(asof=asof_d, close=f["close"], high=f["high"], low=f["low"], open=f["open"], volume=f["volume"], f=f,
               stocks=stocks, etfs=etfs, sectors=p.sectors, sources=srcs,
               names={t: r.get("name") or t for t, r in instruments.items()}, prior_high=prior, fund=fund, angle=angle,
               snaps=snaps, macro={k: pd.Series(v) for k, v in macro.items()}, holdings=holdings, watchlist=watch,
               arrays=arrays, long_close=long_close)
