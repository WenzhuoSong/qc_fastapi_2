"""Twenty years of each playbook traded by its own rules (spec `scout/knowledge/external_specs/playbook_history_v1.yaml`).

Local only, on the Sharadar files (1998 on, delisted names included). At every week's last close the same
`scout.playbooks` functions run on a context rebuilt from what was known that day — the 1000 largest stocks by
market cap, dividend-adjusted bars, quarterly financials as first filed (`datekey`), market value from Sharadar's
daily table, S&P 500 membership, insider purchases, 8-K 2.02 earnings dates, FRED rates — and each playbook's
verdicts are traded at the next open:

  stock playbooks   at most SLOTS positions, 1/SLOTS of the account each, bought in the playbook's own order;
                    a holding is sold when its playbook says 卖出 or it leaves the liquid top 1000
  market timing     the index ETF the verdict is about (SPY first): in on 买入 / 持有, cash on 卖出
  allocation        the target weights the playbook gives (rest in cash), rebalanced when they change and monthly

Cash earns the T-bill rate (French), every trade pays COST one way (COST_HIGH reported too). Descriptive: no
playbook's status changes on this run (the whole sample is seen, ~45 strategies are compared at once).
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from datetime import date, timedelta

import numpy as np
import pandas as pd

from scout import cross_asset as ca
from scout import sharadar_exits as sx
from scout import sharadar_research as sr

logger = logging.getLogger("scout.playbook_history")

KEY = "playbook_history_v1"
FIRST = pd.Timestamp("2006-11-01")          # twenty years to the data's end (2026-10)
SPLIT = pd.Timestamp("2016-11-01")          # the two halves
WINDOW = 360                                # sessions in each context (≈ the live 520 calendar days)
LONG = 800                                  # sessions of closes for residual momentum (≈ 1160 calendar days)
SLOTS = 20
COST, COST_HIGH = 0.0005, 0.0020
SKIP_SCHOOLS = ("出场规则", "问题清单")      # exits need an entry; question lists give no trades
NO_HISTORY = {"valuation_tilt": "估值历史只从 2015 年起有（Scout 自己的篮子估值）",
              "short_squeeze": "没有做空比例的历史数据"}
SECTOR_MAP = {"Technology": "Information Technology", "Healthcare": "Health Care", "Financial Services": "Financials",
              "Consumer Cyclical": "Consumer Discretionary", "Consumer Defensive": "Consumer Staples",
              "Basic Materials": "Materials"}
FLOW_COLS = {"net_income": "netinc", "operating_income": "opinc", "revenue": "revenue", "cost_of_revenue": "cor",
             "gross_profit": "gp", "operating_cash_flow": "ncfo", "eps_diluted": "epsdil", "capex": "capex", "rnd": "rnd"}
STOCK_COLS = {"total_assets": "assets", "equity": "equity", "cash": "cashneq", "short_term_investments": "investmentsc",
              "long_term_debt": "debtnc", "current_debt": "debtc", "total_liabilities": "liabilities",
              "inventory": "inventory", "shares_diluted": "shareswadil"}
INSIDER_DAYS = 90


def etf_list() -> list[str]:
    from scout import universe
    from scout.playbooks import allocation, market

    ts = set(market.INDEXES) | {"IYT", "TLT", "GLD"} | set(universe.SECTOR_ETFS.values()) | set(universe.ALLOCATION_ETFS)
    for w in allocation.STATIC.values():
        ts |= set(w)
    return sorted(ts)


# ── data (all of it once, in the parent process) ──

@dataclass
class Hist:
    dates: pd.DatetimeIndex
    evals: list[int]                         # session index of each week's last close
    names: list[str]                         # stocks, then ETFs
    n_stocks: int
    o: np.ndarray
    h: np.ndarray
    l: np.ndarray
    c: np.ndarray
    v: np.ndarray
    members: dict[int, list[str]]            # eval index → the 1000 largest
    sp500: dict[int, set[str]]
    sectors: dict[str, str | None]
    caps: dict[int, dict[str, float]]        # eval index → market value (USD)
    fin: dict                                # (ticker, metric) → versions (valuation.versions format)
    insiders: pd.DataFrame
    earnings: dict[str, list[date]]
    macro: dict[str, pd.Series]
    rf: np.ndarray

    @property
    def col(self) -> dict[str, int]:
        return {t: j for j, t in enumerate(self.names)}


def _fin_versions(tickers: list[str]) -> dict:
    cols = sorted({*FLOW_COLS.values(), *STOCK_COLS.values(), "ncfcommon"})
    f = sr._read("fundamentals", ["ticker", "dimension", "date", "reportperiod", *cols],
                 filters=[("dimension", "=", "ARQ"), ("ticker", "in", tickers)]).rename(columns={"date": "datekey"})
    f = f.dropna(subset=["datekey", "reportperiod"])
    f["datekey"], f["reportperiod"] = pd.to_datetime(f["datekey"]).dt.date, pd.to_datetime(f["reportperiod"]).dt.date
    f = sr.first_filed(f)
    f["buybacks"] = (-f["ncfcommon"]).clip(lower=0)             # net repurchases (Sharadar gives the net equity flow)
    f["capex"] = -f["capex"]                                   # Sharadar's capex is a cash outflow (negative)
    out: dict = {}
    f = f.sort_values("datekey")
    metrics = {**FLOW_COLS, **STOCK_COLS, "buybacks": "buybacks"}
    for t, g in f.groupby("ticker", sort=False):
        filed, end = list(g["datekey"]), list(g["reportperiod"])
        for m, col in metrics.items():
            vals = g[col].to_numpy(dtype=float)
            rows = [(filed[k], end[k], "quarter", float(vals[k]), filed[k]) for k in range(len(vals)) if np.isfinite(vals[k])]
            if rows:
                out[(t, m)] = rows
    return out


def _insiders(tickers: list[str]) -> pd.DataFrame:
    """Open-market purchases of common stock (non-derivative rows: no `securityadcode`) by officers / directors, by
    filing date; Sharadar's file starts in 2008 and marks no 10b5-1 plans."""
    d = sr._read("insiders", ["ticker", "date", "ownername", "isdirector", "isofficer", "transactioncode", "securityadcode",
                              "securitytitle"], filters=[("ticker", "in", tickers), ("transactioncode", "=", "P")])
    d = d[d["securityadcode"].isna() & d["securitytitle"].isin(["Com", "ClA", "ClB"])
          & ((d["isdirector"] == "Y") | (d["isofficer"] == "Y"))]
    d["date"] = pd.to_datetime(d["date"])
    return d[["ticker", "date", "ownername"]]


def _earnings(tickers: list[str]) -> dict[str, list[date]]:
    from scout.angles import earnings_dates

    e = sr._read("events", ["ticker", "date", "eventcodes"], filters=[("ticker", "in", tickers)])
    e = e[e["eventcodes"].astype(str).str.split("|").apply(lambda x: "22" in x)]       # 8-K item 2.02
    return earnings_dates([(r.ticker, pd.Timestamp(r.date).date()) for r in e.itertuples()])


def _fred(series: str) -> pd.Series:
    import asyncio

    from scout.data import fred

    rows = asyncio.run(fred.series(series, date(1990, 1, 1)))
    return pd.Series({pd.Timestamp(d): v for d, v in rows}).sort_index()


def load() -> Hist:
    from scout.sharadar_b0 import sp500_members

    dates = sr.trading_days()
    weeks = pd.Series(np.arange(len(dates)), index=dates).groupby(dates.to_period("W-FRI")).max()
    evals = [int(i) for i in weeks if dates[i] >= FIRST]
    elig = sr.eligible()
    uni = sr.universe(dates[evals], elig)
    members = {i: uni.get(dates[i], {}).get("large", []) for i in evals}
    stocks = sorted({t for m in members.values() for t in m})
    logger.info("%d weekly dates, %d stocks", len(evals), len(stocks))
    p = sr.prices(stocks, dates)
    arr = [sx.settle(p, j) for j in range(len(stocks))]
    etfs = etf_list()
    fd = sr._read("funds", ["ticker", "date", "open", "high", "low", "close", "volume", "closeadj"], filters=[("ticker", "in", etfs)])
    fd["date"] = pd.to_datetime(fd["date"])
    etf_arr = {}
    for t, g in fd.groupby("ticker"):
        g = g.set_index("date").reindex(dates)
        k = (g["closeadj"] / g["close"]).to_numpy()
        etf_arr[t] = {x: g[x].to_numpy(dtype=float) * (k if x != "volume" else 1) for x in ("open", "high", "low", "close", "volume")}
    etfs = [t for t in etfs if t in etf_arr]
    names = stocks + etfs
    stack = lambda k: np.column_stack([a[k] for a in arr] + [etf_arr[t][k] for t in etfs])  # noqa: E731
    o, h, lo, c, v = (stack(k) for k in ("open", "high", "low", "close", "volume"))
    cap = sr._read("daily", ["ticker", "date", "marketcap"], filters=[("date", "in", [dates[i].date() for i in evals])])
    cap["date"] = pd.to_datetime(cap["date"])
    caps = {int(dates.get_loc(d)): dict(zip(g["ticker"], g["marketcap"] * 1e6)) for d, g in cap.groupby("date")}
    spx = sp500_members(dates[evals])
    rf_s = ca.french_daily_rf().reindex(dates).ffill()            # the last month of the data repeats September's rate
    return Hist(dates=dates, evals=evals, names=names, n_stocks=len(stocks), o=o, h=h, l=lo, c=c, v=v, members=members,
                sp500={i: spx.get(dates[i], set()) for i in evals},
                sectors={t: SECTOR_MAP.get(s, s) for t, s in elig["sector"].items() if t in set(stocks)},
                caps=caps, fin=_fin_versions(stocks), insiders=_insiders(stocks), earnings=_earnings(stocks),
                macro={s: _fred(s) for s in ("DFF", "DGS2")}, rf=rf_s.fillna(0.0).to_numpy())


# ── one week's context (pure, from Hist) ──

def _panel(H: Hist, a: int, b: int, cols: list[int]):
    """Scout's feature panel on sessions a..b (inclusive) for the given columns (SPY included)."""
    from scout.features import build_panel

    idx = H.dates[a:b + 1]
    bars, inst = {}, {}
    for j in cols:
        t = H.names[j]
        bars[t] = pd.DataFrame({"open": H.o[a:b + 1, j], "high": H.h[a:b + 1, j], "low": H.l[a:b + 1, j],
                                "close": H.c[a:b + 1, j], "volume": H.v[a:b + 1, j]}, index=idx)
        inst[t] = {"kind": "stock" if j < H.n_stocks else "etf", "sector": H.sectors.get(t)}
    return build_panel(bars, inst, 5.0, 20_000_000.0)


def _angles(H: Hist, fund: pd.DataFrame, i: int, cl: pd.DataFrame, stocks: list[str]) -> dict[str, dict[str, float]]:
    num = lambda k: pd.to_numeric(fund.get(k), errors="coerce") if k in fund else pd.Series(dtype=float)  # noqa: E731
    out = {}
    if not fund.empty:
        assets, prev = num("total_assets"), num("total_assets_prev")
        avg = ((assets + prev) / 2).where(prev.notna(), assets)
        series = {"gross_profitability": num("gross_profit") / assets.where(assets > 0),
                  "low_accruals": (num("net_income") - num("operating_cash_flow")) / avg.where(avg > 0),
                  "low_asset_growth": (assets / prev.where(prev > 0) - 1),
                  "net_buyback": (num("shares") / num("shares_prev").where(num("shares_prev") > 0) - 1)}
        for k, s in series.items():
            s = s.replace([np.inf, -np.inf], np.nan).dropna()
            out[k] = {t: float(x) for t, x in s.items() if t in stocks}
    d = H.dates[i]
    ins = H.insiders[(H.insiders["date"] > d - pd.Timedelta(days=INSIDER_DAYS)) & (H.insiders["date"] <= d)]
    out["insider_cluster"] = {t: float(n) for t, n in ins.groupby("ticker")["ownername"].nunique().items()}
    # 3-day announcement return vs SPY, known the session after the window (scout.angles.announcement_returns)
    ear, idx = {}, cl.index
    spy = cl["SPY"].to_numpy(dtype=float)
    last = len(idx) - 1
    for t in stocks:
        if t not in cl:
            continue
        x = cl[t].to_numpy(dtype=float)
        for e in H.earnings.get(t, []):
            k = idx.searchsorted(pd.Timestamp(e))
            if k < 1 or k + 1 > last or not (last - 21 < k + 1 <= last):
                continue
            if np.isfinite(x[k - 1]) and np.isfinite(x[k + 1]) and x[k - 1] > 0:
                ear[t] = float(x[k + 1] / x[k - 1] - 1 - (spy[k + 1] / spy[k - 1] - 1))
    out["ear_drift"] = ear
    return out


def context(H: Hist, panel, i: int, k: int):
    """The playbooks' Ctx on session i, from a panel whose row k is session i."""
    from scout import rankers
    from scout.features import FeaturePanel
    from scout.playbooks.base import Ctx
    from scout.playbooks.data import fundamentals

    a = max(0, k - WINDOW + 1)
    f = {name: frame.iloc[a:k + 1] for name, frame in panel.f.items()}
    etfs = [t for t in H.names[H.n_stocks:] if t in f["close"].columns]
    member = set(H.members[i])
    liquid = f["liquid"].iloc[-1]
    stocks = [t for t in H.members[i] if t in f["close"].columns and bool(liquid.get(t, False))]
    sliced = FeaturePanel(f=f, regime=panel.regime.iloc[a:k + 1], sectors=panel.sectors,
                          stocks=[t for t in panel.stocks if t in member])
    arrays = rankers.arrays(sliced)
    asof = H.dates[i].date()
    caps = H.caps.get(i, {})
    fund = fundamentals([], pd.Series(dtype=float), {}, asof, fin=H.fin, tickers=stocks)
    if not fund.empty:
        mcap = pd.Series({t: caps.get(t) for t in fund.index}, dtype=float)
        debt = pd.to_numeric(fund["long_term_debt"], errors="coerce").fillna(0) + pd.to_numeric(fund["current_debt"], errors="coerce").fillna(0)
        cash = pd.to_numeric(fund["cash"], errors="coerce").fillna(0) + pd.to_numeric(fund["short_term_investments"], errors="coerce").fillna(0)
        fund["mcap"], fund["ev"] = mcap, mcap + debt - cash
    cols = [H.col[t] for t in [*stocks, "SPY"] if t in H.col]
    lo = max(0, i - LONG + 1)
    long_close = pd.DataFrame(H.c[lo:i + 1][:, cols], index=H.dates[lo:i + 1], columns=[H.names[j] for j in cols])
    win0 = i - (k - a)
    prior = {t: float(np.nanmax(H.h[:win0, H.col[t]])) for t in stocks if win0 > 0 and np.isfinite(H.h[:win0, H.col[t]]).any()}
    macro = {s: x[x.index <= H.dates[i]] for s, x in H.macro.items()}
    every = [{"ticker": t, "opened_on": asof, "entry_price": math.nan, "shares": 0} for t in stocks]
    return Ctx(asof=asof, close=f["close"], high=f["high"], low=f["low"], open=f["open"], volume=f["volume"], f=f,
               stocks=stocks, etfs=etfs, sectors=panel.sectors, sources={t: ["sp500"] for t in stocks if t in H.sp500.get(i, set())},
               names={}, prior_high=prior, fund=fund, angle=_angles(H, fund, i, f["close"], stocks), macro=macro,
               holdings=every, watchlist=[], arrays=arrays, long_close=long_close)


def books():
    from scout import playbooks as pb

    return [p for p in pb.PLAYBOOKS if p.fn is not None and p.school not in SKIP_SCHOOLS and p.key not in NO_HISTORY]


def verdicts(ctx) -> dict[str, dict]:
    """Every playbook on one context → {key: {"buy": [ordered tickers], "sell": [...], "w": {etf: weight}, "etf": action}}.
    Holdings are set to every stock, so a playbook's sell rule is computed for all of them (only `ranked` and
    Connors' exit read the holdings)."""
    from scout import playbooks as pb
    from scout.playbooks.base import BUY, HOLD, SELL

    dv = ctx.f["dollar_vol20"].iloc[-1]
    out = {}
    for p in books():
        v = pb.apply(ctx, p)
        if v is None:
            out[p.key] = None
            continue
        if p.school == "资产配置":
            out[p.key] = {"w": {t: float(x.score or 0.0) for t, x in v.items() if x.action in (BUY, HOLD)}}
        elif p.school == "大盘择时":
            t = "SPY" if "SPY" in v else next(iter(v), None)
            out[p.key] = {"etf": t, "action": v[t].action if t else None}
        else:
            buys = [(t, x) for t, x in v.items() if x.action == BUY]
            buys.sort(key=lambda tx: (-(tx[1].score if tx[1].score is not None else -1e9), -float(dv.get(tx[0], 0) or 0)))
            out[p.key] = {"buy": [t for t, _ in buys], "sell": sorted(t for t, x in v.items() if x.action == SELL)}
    return out


def run_block(args: tuple) -> dict[int, dict]:
    """Verdicts on a run of consecutive weekly dates (one process): one feature panel for the block."""
    H, evals = args
    first, last = evals[0], evals[-1]
    a = max(0, first - WINDOW - 260)                   # 260 sessions of warm-up for the 252-session features
    cols = sorted({H.col[t] for i in evals for t in H.members[i] if t in H.col} | set(range(H.n_stocks, len(H.names))))
    panel = _panel(H, a, last, cols)
    out = {}
    for i in evals:
        try:
            ctx = context(H, panel, i, i - a)
            out[i] = {"stocks": ctx.stocks, "v": verdicts(ctx)}
        except Exception as exc:                       # a week that cannot be rebuilt is skipped: nothing traded
            logger.warning("week %s skipped: %s", H.dates[i].date(), exc, exc_info=True)
            out[i] = {"stocks": None, "v": {}}
    logger.info("block %s..%s done", H.dates[first].date(), H.dates[last].date())
    return out


def compute_verdicts(H: Hist, workers: int = 8, block: int = 26) -> dict[int, dict]:
    import multiprocessing as mp

    chunks = [H.evals[k:k + block] for k in range(0, len(H.evals), block)]
    global _H
    _H = H
    with mp.get_context("fork").Pool(workers) as pool:
        parts = pool.map(_run_chunk, chunks)
    return {i: x for part in parts for i, x in part.items()}


_H: Hist | None = None


def _run_chunk(evals: list[int]) -> dict[int, dict]:
    return run_block((_H, evals))


# ── trading the verdicts (pure) ──

def _price(H: Hist, arr: np.ndarray, i: int, j: int) -> float | None:
    x = arr[i, j] if i < len(H.dates) else np.nan
    return float(x) if np.isfinite(x) and x > 0 else None


def simulate_stocks(H: Hist, ver: dict[int, dict], key: str, cost: float) -> dict:
    """Weekly decisions, trades at the next open, marked at each close."""
    col = H.col
    first = H.evals[0]
    cash, pos = 1.0, {}                                 # pos: column → shares
    nav = np.full(len(H.dates), np.nan)
    trades = held_days = 0
    nav_held = []
    plan = {i + 1: i for i in H.evals}                  # the open after each decision date
    for d in range(first, len(H.dates)):
        if d in plan:
            x = ver[plan[d]]
            book = x["v"].get(key)
            if book is not None and x["stocks"] is not None:
                live = set(x["stocks"])
                sells = set(book["sell"])
                for j in list(pos):
                    t = H.names[j]
                    if t in sells or t not in live:
                        px = _price(H, H.o, d, j) or _price(H, H.c, d - 1, j)
                        cash += pos.pop(j) * px * (1 - cost)
                        trades += 1
                value = cash + sum(n * (_price(H, H.o, d, j) or _price(H, H.c, d - 1, j) or 0.0) for j, n in pos.items())
                slot = value / SLOTS
                for t in book["buy"]:
                    if len(pos) >= SLOTS or cash < 0.5 * slot:
                        break
                    j = col.get(t)
                    if j is None or j in pos:
                        continue
                    px = _price(H, H.o, d, j)
                    if px is None:
                        continue
                    spend = min(slot, cash / (1 + cost))
                    pos[j] = spend / px
                    cash -= spend * (1 + cost)
                    trades += 1
        cash *= 1 + H.rf[d]
        nav[d] = cash + sum(n * (_price(H, H.c, d, j) or _price(H, H.c, d - 1, j) or 0.0) for j, n in pos.items())
        nav_held.append(len(pos))
    return {"nav": nav, "trades": trades, "avg_positions": float(np.mean(nav_held))}


def simulate_weights(H: Hist, ver: dict[int, dict], key: str, cost: float, timing: bool) -> dict:
    """Target weights in ETFs (timing: 100 % in the verdict's ETF on 买入 / 持有, else cash), traded at the next open."""
    col = H.col
    first = H.evals[0]
    cash, pos, target = 1.0, {}, {}
    nav = np.full(len(H.dates), np.nan)
    trades, invested, started = 0, [], None
    month = None
    plan = {i + 1: i for i in H.evals}
    for d in range(first, len(H.dates)):
        if d in plan:
            book = ver[plan[d]]["v"].get(key)
            new = None
            if book is not None:
                if timing:
                    if book.get("etf") and book.get("action"):
                        new = {book["etf"]: 1.0} if book["action"] in ("买入", "持有") else {}
                else:
                    new = {t: w for t, w in book["w"].items() if t in col and w > 0}
            if new is not None and started is None:
                started = d
            m = H.dates[d].to_period("M")
            if new is not None and (new != target or (not timing and m != month)):
                target, month = new, m
                value = cash + sum(n * (_price(H, H.o, d, j) or _price(H, H.c, d - 1, j)) for j, n in pos.items())
                want = {col[t]: w * value for t, w in target.items()}
                for j in set(pos) | set(want):
                    px = _price(H, H.o, d, j) or _price(H, H.c, d - 1, j)
                    if px is None:
                        continue
                    delta = want.get(j, 0.0) - pos.get(j, 0.0) * px
                    if abs(delta) < 1e-4 * value:
                        continue
                    cash -= delta + abs(delta) * cost
                    pos[j] = pos.get(j, 0.0) + delta / px
                    if pos[j] <= 1e-12:
                        pos.pop(j)
                    trades += 1
        cash *= 1 + H.rf[d]
        nav[d] = cash + sum(n * (_price(H, H.c, d, j) or _price(H, H.c, d - 1, j) or 0.0) for j, n in pos.items())
        if started is not None:
            invested.append(1.0 - cash / nav[d] if nav[d] else 0.0)
    if started is not None:
        nav[:started] = np.nan                         # a playbook whose data starts later is measured from its start
    return {"nav": nav, "trades": trades, "invested": float(np.mean(invested)) if invested else None}


def benchmarks(H: Hist, ver: dict[int, dict]) -> dict[str, np.ndarray]:
    """SPY bought and held, and every liquid top-1000 stock in equal weight (rebalanced monthly, same cost)."""
    first = H.evals[0]
    spy = H.c[:, H.col["SPY"]]
    out = {"SPY": spy / spy[first]}
    nav = np.full(len(H.dates), np.nan)
    value, w, month = 1.0, {}, None
    plan = {i + 1: i for i in H.evals}
    for d in range(first, len(H.dates)):
        if d > first:
            r = sum(wt * ((_price(H, H.c, d, j) or _price(H, H.c, d - 1, j)) / (_price(H, H.c, d - 1, j) or 1.0) - 1)
                    for j, wt in w.items())
            value *= 1 + r
            tot = sum(wt * ((_price(H, H.c, d, j) or _price(H, H.c, d - 1, j)) / (_price(H, H.c, d - 1, j) or 1.0)) for j, wt in w.items())
            w = {j: wt * ((_price(H, H.c, d, j) or _price(H, H.c, d - 1, j)) / (_price(H, H.c, d - 1, j) or 1.0)) / tot
                 for j, wt in w.items()} if tot else w
        if d in plan and ver[plan[d]]["stocks"] and H.dates[d].to_period("M") != month:
            month = H.dates[d].to_period("M")
            live = [H.col[t] for t in ver[plan[d]]["stocks"] if t in H.col]
            new = {j: 1 / len(live) for j in live}
            value *= 1 - COST * sum(abs(new.get(j, 0) - w.get(j, 0)) for j in set(new) | set(w))
            w = new
        nav[d] = value
    out["等权股票池"] = nav
    return out


# ── metrics ──

def stats(nav: np.ndarray, dates: pd.DatetimeIndex, rf: np.ndarray) -> dict | None:
    s = pd.Series(nav, index=dates).dropna()
    if len(s) < 260:
        return None
    r = s.pct_change().dropna()
    ex = r - pd.Series(rf, index=dates).reindex(r.index)
    years = (s.index[-1] - s.index[0]).days / 365.25
    dd = float((s / s.cummax() - 1).min())
    cal = s.resample("YE").last().pct_change()
    cal.iloc[0] = s.resample("YE").last().iloc[0] / s.iloc[0] - 1
    return {"start": s.index[0].date().isoformat(), "end": s.index[-1].date().isoformat(),
            "cagr": float((s.iloc[-1] / s.iloc[0]) ** (1 / years) - 1), "total": float(s.iloc[-1] / s.iloc[0] - 1),
            "vol": float(r.std() * math.sqrt(252)), "sharpe": float(ex.mean() / ex.std() * math.sqrt(252)) if ex.std() else None,
            "max_dd": dd, "worst_year": float(cal.min()), "best_year": float(cal.max()),
            "years": {str(k.year): float(x) for k, x in cal.items()}}


def halves(nav: np.ndarray, dates: pd.DatetimeIndex, rf: np.ndarray) -> dict:
    cut = dates.searchsorted(SPLIT)
    a, b = nav.copy(), nav.copy()
    a[cut:], b[:cut] = np.nan, np.nan
    return {"first": stats(a, dates, rf), "second": stats(b, dates, rf)}


def excess_t(nav: np.ndarray, ref: np.ndarray, dates: pd.DatetimeIndex) -> dict | None:
    """Monthly return minus the reference's, mean and Newey-West t (lag 3), over the playbook's own months."""
    s, q = pd.Series(nav, index=dates), pd.Series(ref, index=dates)
    ok = s.notna() & q.notna()
    if ok.sum() < 260:
        return None
    m = (s[ok].resample("ME").last().pct_change() - q[ok].resample("ME").last().pct_change()).dropna()
    from scout.small_cap_rankers import nw_t

    t = nw_t(m, 3)
    return {"months": int(len(m)), "mean": float(m.mean()), "t": float(t) if t is not None else float("nan")}


def evaluate(H: Hist, ver: dict[int, dict]) -> dict:
    bench = benchmarks(H, ver)
    res = {"key": KEY, "period": [H.dates[H.evals[0]].date().isoformat(), H.dates[-1].date().isoformat()],
           "slots": SLOTS, "cost": COST, "cost_high": COST_HIGH, "benchmarks": {}, "playbooks": [],
           "not_run": {**NO_HISTORY}}
    for k, nav in bench.items():
        res["benchmarks"][k] = {"all": stats(nav, H.dates, H.rf), **halves(nav, H.dates, H.rf)}
    for p in books():
        timing, alloc = p.school == "大盘择时", p.school == "资产配置"
        runs = {}
        for label, cst in (("main", COST), ("high", COST_HIGH)):
            runs[label] = (simulate_weights(H, ver, p.key, cst, timing) if timing or alloc
                           else simulate_stocks(H, ver, p.key, cst))
        nav = runs["main"]["nav"]
        st = stats(nav, H.dates, H.rf)
        if st is None or not runs["main"]["trades"]:
            res["not_run"][p.key] = "没有足够的历史结果（数据缺失或从未给出交易）"
            continue
        ref = bench["SPY"] if timing or alloc else bench["等权股票池"]
        res["playbooks"].append({
            "key": p.key, "name": p.name, "school": p.school, "version": p.version,
            "all": st, **halves(nav, H.dates, H.rf), "high_cost": stats(runs["high"]["nav"], H.dates, H.rf),
            "vs_spy": excess_t(nav, bench["SPY"], H.dates), "vs_ref": excess_t(nav, ref, H.dates),
            "ref": "SPY" if timing or alloc else "等权股票池",
            "trades": runs["main"]["trades"], "avg_positions": runs["main"].get("avg_positions"),
            "invested": runs["main"].get("invested")})
    return res


# ── report ──

def _p(x, d: int = 1) -> str:
    return "—" if x is None else f"{x * 100:+.{d}f}%"


def report_text(res: dict, status: dict[str, str] | None = None) -> str:
    status = status or {}
    b = res["benchmarks"]
    lines = [f"打法二十年回测（{res['period'][0]} 至 {res['period'][1]}，Sharadar 前 1000 大、含退市股；每周收盘按打法判断、次日开盘成交；"
             f"个股打法最多 {res['slots']} 只等权，单边成本 {res['cost'] * 100:.2f}%）。描述历史，不改变任何打法的检验状态。",
             ""]
    for k, x in b.items():
        a = x["all"]
        lines.append(f"基准 {k}：年化 {_p(a['cagr'])}，最大回撤 {_p(a['max_dd'], 0)}，夏普 {a['sharpe']:.2f}；"
                     f"前十年 {_p(x['first']['cagr'])}，后十年 {_p(x['second']['cagr'])}")
    from scout.playbooks.base import SCHOOLS

    for school in SCHOOLS:
        rows = sorted((r for r in res["playbooks"] if r["school"] == school), key=lambda r: -r["all"]["cagr"])
        if not rows:
            continue
        lines += ["", f"【{school}】（对比 {rows[0]['ref']}）"]
        for r in rows:
            a, ref = r["all"], r["vs_ref"] or {}
            lines.append(f"· {r['name']}{'（' + status[r['key']] + '）' if r['key'] in status else ''}：年化 {_p(a['cagr'])}"
                         f"（高成本 {_p((r['high_cost'] or {}).get('cagr'))}），最大回撤 {_p(a['max_dd'], 0)}，夏普 "
                         f"{a['sharpe']:.2f}；前十年 {_p((r['first'] or {}).get('cagr'))}，后十年 {_p((r['second'] or {}).get('cagr'))}；"
                         f"每月超额 {_p(ref.get('mean'), 2)}（t {ref.get('t', float('nan')):.1f}）"
                         + (f"；起于 {a['start']}" if a["start"] > res["period"][0] else ""))
    if res["not_run"]:
        lines += ["", "没有回测：" + "；".join(f"{k}：{v}" for k, v in res["not_run"].items())]
    return "\n".join(lines)
