"""Point-in-time features from angles other than price (v3 multi-angle pipeline, step 2).

Every value is what could be known on the date: financials count from their SEC filing date
(restatements are later versions), an earnings reaction from the close after its 3-day window,
an insider trade from its Form 4 filing date.

- events: earnings announcements = 8-K filings with item 2.02 (results of operations); the
  announcement return (EAR) is the stock's close-to-close return from the session before the
  filing to the session after it, minus SPY's (Brandt, Kishore, Santa-Clara & Venkatachalam 2008).
  The filing time of day is not stored, so the 3-day window covers a release before the open and
  one after the close.
- financials (TTM = sum of the last four quarters): gross profitability = gross profit / total
  assets (Novy-Marx 2013; gross profit = revenue − cost of revenue where not reported); accruals
  = (net income − operating cash flow) / average total assets (Sloan 1996); asset growth = total
  assets / a year earlier − 1 (Cooper, Gulen & Schill 2008); net issuance = diluted shares / a year
  earlier − 1 (Pontiff & Woodgate 2008); revenue growth = latest quarter / the same quarter a
  year earlier − 1.
- insiders: distinct officers and directors with open-market purchases of common stock filed in
  the last 90 days, not flagged as 10b5-1 plan trades (Lakonishok & Lee 2001; Cohen, Malloy &
  Pomorski 2012: routine and planned trades carry little information).
"""
from __future__ import annotations

from bisect import bisect_right
from collections import defaultdict
from datetime import date, timedelta

import numpy as np

from scout import db

INSIDER_DAYS = 90
EARNINGS_GAP_DAYS = 45          # a second 2.02 filing within this many days is the same quarter (amendment, recast)
FUND_METRICS = ("revenue", "cost_of_revenue", "gross_profit", "net_income", "operating_cash_flow", "total_assets",
                "shares_diluted")
FLOW = {"revenue", "cost_of_revenue", "gross_profit", "net_income", "operating_cash_flow"}
YEAR_DAYS = 365


# ── events ──────────────────────────────────────────────────────────────

def earnings_dates(filings: list[tuple[str, date]]) -> dict[str, list[date]]:
    """ticker → announcement dates from (ticker, filed) of 8-Ks with item 2.02, one per quarter."""
    out: dict[str, list[date]] = defaultdict(list)
    for t, d in sorted(filings):
        if not out[t] or (d - out[t][-1]).days > EARNINGS_GAP_DAYS:
            out[t].append(d)
    return dict(out)


def announcement_returns(dates_by_ticker: dict[str, list[date]], p) -> list[dict]:
    """Per announcement: EAR and the index of the session after the window (when it is known)."""
    close = p["close"]
    spy = close["SPY"].to_numpy(dtype=float)
    idx = p.dates
    out = []
    for t, ds in dates_by_ticker.items():
        if t not in close.columns:
            continue
        c = close[t].to_numpy(dtype=float)
        for d in ds:
            k = idx.searchsorted(d)                    # the filing day, or the next session
            if k < 1 or k + 1 >= len(idx):
                continue
            a, b = k - 1, k + 1
            if not (np.isfinite(c[a]) and np.isfinite(c[b]) and c[a] > 0):
                continue
            out.append({"ticker": t, "filed": d, "known": b, "ear": float(c[b] / c[a] - 1 - (spy[b] / spy[a] - 1))})
    return out


async def load_earnings() -> dict[str, list[date]]:
    rows = await db.fetch_all("SELECT ticker, filed FROM scout.sec_filings WHERE form = '8-K' AND items LIKE '%2.02%'")
    return earnings_dates([(r["ticker"], r["filed"]) for r in rows])


# ── financials ──────────────────────────────────────────────────────────

class PointInTime:
    """Financial values as known on successive dates. Feed dates in ascending order; for each
    (ticker, metric) the latest filed version of every period is kept."""

    def __init__(self, rows: list[dict]):
        self.rows: dict[tuple[str, str], list[tuple[date, date, float]]] = defaultdict(list)
        for r in rows:
            self.rows[(r["ticker"], r["metric"])].append((r["filed"], r["period_end"], float(r["value"])))
        for v in self.rows.values():
            v.sort()
        self.pos = {k: 0 for k in self.rows}
        self.known: dict[tuple[str, str], dict[date, float]] = {k: {} for k in self.rows}

    def advance(self, d: date) -> None:
        for k, v in self.rows.items():
            i = self.pos[k]
            while i < len(v) and v[i][0] <= d:
                self.known[k][v[i][1]] = v[i][2]
                i += 1
            self.pos[k] = i

    def series(self, ticker: str, metric: str) -> list[tuple[date, float]]:
        return sorted(self.known.get((ticker, metric), {}).items())


def _near(s: list[tuple[date, float]], target: date, tol: int = 25) -> float | None:
    if not s:
        return None
    ends = [x[0] for x in s]
    j = bisect_right(ends, target)
    best = min((s[k] for k in (j - 1, j) if 0 <= k < len(s)), key=lambda x: abs((x[0] - target).days))
    return best[1] if abs((best[0] - target).days) <= tol else None


def _ttm(s: list[tuple[date, float]]) -> tuple[date, float] | None:
    """Sum of the last four quarters if they are consecutive (about a year apart end to end)."""
    if len(s) < 4:
        return None
    last = s[-4:]
    if not 250 <= (last[-1][0] - last[0][0]).days <= 300:
        return None
    return last[-1][0], sum(v for _, v in last)


def fundamentals_now(pit: PointInTime, ticker: str, d: date) -> dict:
    """The financial features of one company from what was filed by d (stale filings over a year old are ignored)."""
    out = {}
    get = lambda m: pit.series(ticker, m)  # noqa: E731
    assets = get("total_assets")
    if not assets or (d - assets[-1][0]).days > YEAR_DAYS + 120:
        return out
    a_end, a_now = assets[-1]
    a_prev = _near(assets, a_end - timedelta(days=YEAR_DAYS))
    if a_prev and a_prev > 0:
        out["asset_growth"] = a_now / a_prev - 1
    avg_assets = (a_now + a_prev) / 2 if a_prev else a_now
    rev, cost, gp = _ttm(get("revenue")), _ttm(get("cost_of_revenue")), _ttm(get("gross_profit"))
    gross = gp[1] if gp else (rev[1] - cost[1] if rev and cost else None)
    if gross is not None and a_now > 0:
        out["gross_profitability"] = gross / a_now
    ni, cfo = _ttm(get("net_income")), _ttm(get("operating_cash_flow"))
    if ni and cfo and avg_assets > 0:
        out["accruals"] = (ni[1] - cfo[1]) / avg_assets
    sh = get("shares_diluted")
    if sh:
        prev = _near(sh, sh[-1][0] - timedelta(days=YEAR_DAYS))
        if prev and prev > 0:
            out["net_issuance"] = sh[-1][1] / prev - 1
    r = get("revenue")
    if r:
        prev = _near(r, r[-1][0] - timedelta(days=YEAR_DAYS))
        if prev and prev > 0:
            out["revenue_growth"] = r[-1][1] / prev - 1
    return out


async def load_financials(tickers: list[str]) -> PointInTime:
    rows = await db.fetch_all(
        "SELECT ticker, metric, period_end, value, filed FROM scout.financials WHERE ticker = ANY(:t) AND metric = ANY(:m)",
        {"t": tickers, "m": list(FUND_METRICS)})
    return PointInTime(rows)


# ── insiders ────────────────────────────────────────────────────────────

def insider_buying(trades: list[dict], d: date) -> dict[str, dict]:
    """ticker → {buyers, value} of officers / directors buying common stock on the open market,
    filed in the INSIDER_DAYS before d (d included), 10b5-1 plan trades excluded."""
    lo = d - timedelta(days=INSIDER_DAYS)
    out: dict[str, dict] = {}
    for t in trades:
        if not (lo < t["filed"] <= d) or t["code"] != "P" or not t["common"] or t.get("plan_10b5_1"):
            continue
        rel = (t.get("relationship") or "")
        if "Officer" not in rel and "Director" not in rel:
            continue
        o = out.setdefault(t["ticker"], {"owners": set(), "value": 0.0})
        o["owners"].add(t.get("owner_cik") or t.get("owner_name"))
        o["value"] += t.get("value") or 0.0
    return {k: {"buyers": len(v["owners"]), "value": v["value"]} for k, v in out.items()}


async def load_insider_buys() -> list[dict]:
    return await db.fetch_all("SELECT ticker, filed, code, common, plan_10b5_1, relationship, owner_cik, owner_name, value "
                              "FROM scout.insider_trades WHERE code = 'P'")


# ── daily snapshot (live use) ───────────────────────────────────────────

async def snapshot(p, asof: date) -> dict:
    """Every stock's angle features for the data date, appended to scout.angle_features."""
    stocks = [t for t in p.stocks if t in p["close"].columns]
    pit = await load_financials(stocks)
    pit.advance(asof)
    last = len(p.dates) - 1
    latest_ear: dict[str, dict] = {}
    for ev in announcement_returns(await load_earnings(), p):
        if ev["known"] <= last and (ev["ticker"] not in latest_ear or ev["known"] > latest_ear[ev["ticker"]]["known"]):
            latest_ear[ev["ticker"]] = ev
    trades = await load_insider_buys()
    buying = insider_buying(trades, asof)
    through = await db.fetch_one("SELECT max(filed) AS d FROM scout.insider_trades")
    through = str((through or {}).get("d") or "")
    rows = []
    for t in stocks:
        f = {f"ang_{k}": v for k, v in fundamentals_now(pit, t, asof).items() if v is not None and np.isfinite(v)}
        ev = latest_ear.get(t)
        if ev:
            f["ang_ear"], f["ang_ear_sessions_ago"] = ev["ear"], last - ev["known"]
            f["ang_ear_filed"] = str(ev["filed"])
        if through:
            f["ang_insider_officer_buyers"] = (buying.get(t) or {}).get("buyers", 0)
            f["ang_insider_officer_value"] = (buying.get(t) or {}).get("value", 0.0)
            f["ang_insider_data_through"] = through
        if f:
            rows.append({"t": t, "d": asof, "p": db.jsonb(f)})
    n = await db.execute_many("INSERT INTO scout.angle_features (ticker, asof, payload) VALUES (:t, :d, CAST(:p AS JSONB)) "
                              "ON CONFLICT (ticker, asof) DO UPDATE SET payload = EXCLUDED.payload", rows)
    return {"stocks": n, "insider_data_through": through}


async def latest(ticker: str) -> dict:
    row = await db.fetch_one("SELECT payload FROM scout.angle_features WHERE ticker = :t ORDER BY asof DESC LIMIT 1", {"t": ticker})
    return (row or {}).get("payload") or {}


# ── the angle registry: where each angle comes from and how it is validated ──
# path "backtest": history exists point in time → the gate (G1 + G2), then the forward record.
# path "forward": no clean history (or an AI judgment) → the judgment ledger, scored from now on.

ANGLES = (
    {"id": "price", "name": "价格与成交量", "source": "Yahoo 日线", "pit": "收盘后可得，次日成交", "path": "backtest",
     "history": ("scout.bars_daily", "date"), "strategies": "rule", "lenses": ["trend_dow", "support_resistance", "momentum_rs", "mean_reversion_vol"]},
    {"id": "financials", "name": "财报（质量与资本配置）", "source": "SEC EDGAR XBRL", "pit": "按申报日期生效，重述另存版本", "path": "backtest",
     "history": ("scout.financials", "filed"), "strategies": ["rank_gross_profitability", "rank_low_accruals", "rank_low_asset_growth", "rank_net_buyback"],
     "lenses": ["fundamental_trend", "valuation"]},
    {"id": "events", "name": "财报事件", "source": "SEC 8-K（2.02 项）+ 日线", "pit": "公告窗口结束后的收盘可得", "path": "backtest",
     "history": ("scout.sec_filings", "filed"), "strategies": ["rank_ear_drift"], "lenses": ["earnings", "events", "catalysts"]},
    {"id": "insiders", "name": "内部人交易", "source": "SEC Form 4 季度数据集", "pit": "按 Form 4 申报日期", "path": "backtest",
     "history": ("scout.insider_trades", "filed"), "strategies": ["rank_insider_cluster"], "lenses": ["ownership"]},
    {"id": "expectations", "name": "分析师预期与评级", "source": "Yahoo、Finnhub", "pit": "每日快照", "path": "forward",
     "history": ("scout.research_snapshots", "asof"), "strategies": [], "lenses": ["expectations"]},
    {"id": "options", "name": "期权", "source": "Yahoo 期权链", "pit": "每日收盘前快照", "path": "forward",
     "history": ("scout.research_snapshots", "asof"), "strategies": [], "lenses": ["options"]},
    {"id": "news", "name": "新闻与异动原因", "source": "Finnhub、RSS、网络检索", "pit": "按发布时间", "path": "forward",
     "history": ("scout.stock_events", "event_date"), "strategies": [], "lenses": ["catalysts", "events"]},
    {"id": "market", "name": "大盘与宏观", "source": "日线、FRED", "pit": "收盘后可得", "path": "backtest",
     "history": ("scout.macro_series", "date"), "strategies": [], "lenses": ["market_context", "macro"]},
    {"id": "sectors", "name": "行业判断", "source": "日线 + 研究池财报 + AI", "pit": "每周记录", "path": "forward",
     "history": ("scout.judgments", "asof"), "strategies": [], "lenses": []},
)
