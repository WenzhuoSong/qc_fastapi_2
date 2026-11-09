"""Twenty years of each playbook traded by its own rules at its own pace (spec
`scout/knowledge/external_specs/playbook_history_v2.yaml`; v1, every playbook checked weekly, is commit fef699c).

Local only, on the Sharadar files (1998 on, delisted names included). On each of a playbook's decision days
(`CADENCE`: every session for the daily systems, the week's / month's / quarter's / year's last session for the
others) the same `scout.playbooks` functions run on a context rebuilt from what was known that day — the 1000
largest stocks by market cap (refreshed at each week's end), dividend-adjusted bars, quarterly financials as first
filed (`datekey`), market value from Sharadar's daily table, S&P 500 membership, insider purchases, 8-K 2.02
earnings dates, FRED rates — and the verdicts are traded at the next open:

  stock playbooks   at most SLOTS positions, 1/SLOTS of the account each, bought in the playbook's own order;
                    a holding is sold when its playbook says 卖出 or it left the liquid top 1000; screens rebuilt at
                    each decision (`REBALANCE`) also sell what is no longer on their list
  market timing     the index ETF the verdict is about (SPY first): in on 买入 / 持有, cash on 卖出
  allocation        the target weights the playbook gives (rest in cash), re-set on every decision day

A daily system acts on that session's signal only (`Ctx.fresh` = 1); the others see 5 sessions, as live. Cash
earns the T-bill rate (French), every trade pays COST one way (COST_HIGH reported too). Descriptive: no playbook's
status changes on this run (the whole sample is seen, ~45 strategies are compared at once).
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from datetime import date

import numpy as np
import pandas as pd

from scout import cross_asset as ca
from scout import sharadar_exits as sx
from scout import sharadar_research as sr

logger = logging.getLogger("scout.playbook_history")

KEY = "playbook_history_v2"
FIRST = pd.Timestamp("2006-11-01")          # twenty years to the data's end (2026-10)
SPLIT = pd.Timestamp("2016-11-01")          # the two halves
WINDOW = 360                                # sessions in each context (≈ the live 520 calendar days)
LONG = 800                                  # sessions of closes for residual momentum (≈ 1160 calendar days)
SLOTS = 20
COST, COST_HIGH = 0.0005, 0.0020
SKIP_SCHOOLS = ("出场规则", "问题清单")      # exits need an entry; question lists give no trades
NO_HISTORY = {"valuation_tilt": "估值历史只从 2015 年起有（Scout 自己的篮子估值）",
              "short_squeeze": "没有做空比例的历史数据"}
# How often each author checks the rule (catalogue docs/playbook_catalog_v1_zh.md; fixed before the run)
CADENCE = {
    **dict.fromkeys(("turtle_s1", "turtle_s2", "darvas", "connors_rsi2", "macd", "golden_cross", "chart_patterns",
                     "minervini", "can_slim", "livermore", "wyckoff", "wilcox", "elder", "pead", "insider",
                     "oneil_market", "dow_theory", "breadth_thrust", "golden_cross_etf"), "daily"),
    **dict.fromkeys(("weinstein", "zweig_4", "clenow"), "weekly"),
    **dict.fromkeys(("faber", "gem", "vaa", "gtaa", "liquidity", "high_52w", "residual_momentum", "frog_in_pan",
                     "short_reversal", "gross_profitability", "low_accruals", "low_asset_growth", "net_buyback"), "monthly"),
    "lynch": "quarterly",
    **dict.fromkeys(("magic_formula", "piotroski", "trending_value", "graham", "buffett_quality",
                     "all_weather", "permanent", "sixty_forty"), "annual"),
}
CADENCE_ZH = {"daily": "每日", "weekly": "每周", "monthly": "每月", "quarterly": "每季", "annual": "每年"}
REBALANCE = {"magic_formula", "piotroski", "trending_value"}      # Greenblatt / Piotroski / O'Shaughnessy: rebuild the list
SECTOR_MAP = {"Technology": "Information Technology", "Healthcare": "Health Care", "Financial Services": "Financials",
              "Consumer Cyclical": "Consumer Discretionary", "Consumer Defensive": "Consumer Staples",
              "Basic Materials": "Materials"}
FLOW_COLS = {"net_income": "netinc", "operating_income": "opinc", "revenue": "revenue", "cost_of_revenue": "cor",
             "gross_profit": "gp", "operating_cash_flow": "ncfo", "eps_diluted": "epsdil", "capex": "capex", "rnd": "rnd"}
STOCK_COLS = {"total_assets": "assets", "equity": "equity", "cash": "cashneq", "short_term_investments": "investmentsc",
              "long_term_debt": "debtnc", "current_debt": "debtc", "total_liabilities": "liabilities",
              "inventory": "inventory", "shares_diluted": "shareswadil"}
INSIDER_DAYS = 90
FUND_SCHOOLS = ("价值投资", "成长股")


def etf_list() -> list[str]:
    from scout import universe
    from scout.playbooks import allocation, market

    ts = set(market.INDEXES) | {"IYT", "TLT", "GLD"} | set(universe.SECTOR_ETFS.values()) | set(universe.ALLOCATION_ETFS)
    for w in allocation.STATIC.values():
        ts |= set(w)
    return sorted(ts)


def decision_days(dates: pd.DatetimeIndex, start: int) -> dict[str, list[int]]:
    """Session indices of each cadence's decisions from `start` on: every session, or the last of each period."""
    s = pd.Series(np.arange(len(dates)), index=dates)
    last = lambda freq: [int(i) for i in s.groupby(dates.to_period(freq)).max() if i >= start]  # noqa: E731
    quarters = [i for i in last("M") if dates[i].month in (3, 6, 9, 12)]
    return {"daily": list(range(start, len(dates))), "weekly": last("W-FRI"), "monthly": last("M"),
            "quarterly": quarters, "annual": [i for i in last("M") if dates[i].month == 12]}


# ── data (all of it once, in the parent process) ──

@dataclass
class Hist:
    dates: pd.DatetimeIndex
    start: int                               # first session on or after FIRST
    days: dict[str, list[int]]               # cadence → decision sessions
    names: list[str]                         # stocks, then ETFs
    n_stocks: int
    o: np.ndarray
    h: np.ndarray
    l: np.ndarray
    c: np.ndarray
    v: np.ndarray
    high_max: np.ndarray                     # running maximum of the highs (for all-time highs before a window)
    week_ends: np.ndarray                    # sessions whose membership is computed
    members: dict[int, list[str]]            # week end → the 1000 largest that day
    sp500: dict[int, set[str]]               # week end → S&P 500 members
    sectors: dict[str, str | None]
    caps: np.ndarray                         # session × stock market value (USD)
    fin: dict                                # (ticker, metric) → versions (valuation.versions format)
    insiders: pd.DataFrame
    earnings: dict[str, list[date]]
    macro: dict[str, pd.Series]
    rf: np.ndarray
    min_dv: float = 20_000_000.0             # liquid = 20-day dollar volume at least this (live Scout's floor)
    large: dict[int, set[str]] | None = None  # week end → the 1000 largest (set when members reach further)

    @property
    def col(self) -> dict[str, int]:
        return {t: j for j, t in enumerate(self.names)}

    def week_of(self, i: int) -> int:
        """The latest membership date on or before session i."""
        return int(self.week_ends[np.searchsorted(self.week_ends, i, side="right") - 1])


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


def load(top: int = 1000, min_dv: float = 20_000_000.0) -> Hist:
    """The data for the market-cap top `top` (1000, or 3000 = Sharadar's large + small lists)."""
    from scout.sharadar_b0 import sp500_members

    dates = sr.trading_days()
    start = int(dates.searchsorted(FIRST))
    days = decision_days(dates, start)
    weeks = [i for i in decision_days(dates, start - 10)["weekly"]]          # one membership at or before the first day
    elig = sr.eligible()
    uni = sr.universe(dates[weeks], elig)
    members = {i: uni.get(dates[i], {}).get("large", []) + (uni.get(dates[i], {}).get("small", []) if top > 1000 else [])
               for i in weeks}
    stocks = sorted({t for m in members.values() for t in m})
    logger.info("%d sessions, %d stocks", len(dates) - start, len(stocks))
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
    cap = sr._read("daily", ["ticker", "date", "marketcap"], filters=[("ticker", "in", stocks)])
    cap["date"] = pd.to_datetime(cap["date"])
    caps = (cap.pivot_table(index="date", columns="ticker", values="marketcap", aggfunc="last")
            .reindex(index=dates, columns=stocks).to_numpy(dtype=float) * 1e6)
    spx = sp500_members(dates[weeks])
    rf_s = ca.french_daily_rf().reindex(dates).ffill()            # the last month of the data repeats September's rate
    return Hist(dates=dates, start=start, days=days, names=names, n_stocks=len(stocks), o=o, h=h, l=lo, c=c, v=v,
                high_max=np.fmax.accumulate(np.nan_to_num(h, nan=-np.inf), axis=0), week_ends=np.array(weeks),
                members=members, sp500={i: spx.get(dates[i], set()) for i in weeks},
                sectors={t: SECTOR_MAP.get(s, s) for t, s in elig["sector"].items() if t in set(stocks)},
                caps=caps, fin=_fin_versions(stocks), insiders=_insiders(stocks), earnings=_earnings(stocks),
                macro={s: _fred(s) for s in ("DFF", "DGS2")}, rf=rf_s.fillna(0.0).to_numpy(), min_dv=min_dv,
                large={i: set(uni.get(dates[i], {}).get("large", [])) for i in weeks} if top > 1000 else None)


# ── one day's context (pure, from Hist) ──

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
    return build_panel(bars, inst, 5.0, H.min_dv)


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


def context(H: Hist, panel, i: int, k: int, with_fund: bool = True):
    """The playbooks' Ctx on session i, from a panel whose row k is session i."""
    from scout import rankers
    from scout.features import FeaturePanel
    from scout.playbooks.base import Ctx
    from scout.playbooks.data import fundamentals

    a = max(0, k - WINDOW + 1)
    f = {name: frame.iloc[a:k + 1] for name, frame in panel.f.items()}
    etfs = [t for t in H.names[H.n_stocks:] if t in f["close"].columns]
    wk = H.week_of(i)
    member = set(H.members[wk])
    liquid = f["liquid"].iloc[-1]
    stocks = [t for t in H.members[wk] if t in f["close"].columns and bool(liquid.get(t, False))]
    sliced = FeaturePanel(f=f, regime=panel.regime.iloc[a:k + 1], sectors=panel.sectors,
                          stocks=[t for t in panel.stocks if t in member])
    arrays = rankers.arrays(sliced)
    asof = H.dates[i].date()
    col = H.col
    fund = fundamentals([], pd.Series(dtype=float), {}, asof, fin=H.fin, tickers=stocks) if with_fund else pd.DataFrame()
    if not fund.empty:
        mcap = pd.Series({t: H.caps[i, col[t]] for t in fund.index}, dtype=float)
        debt = pd.to_numeric(fund["long_term_debt"], errors="coerce").fillna(0) + pd.to_numeric(fund["current_debt"], errors="coerce").fillna(0)
        cash = pd.to_numeric(fund["cash"], errors="coerce").fillna(0) + pd.to_numeric(fund["short_term_investments"], errors="coerce").fillna(0)
        fund["mcap"], fund["ev"] = mcap, mcap + debt - cash
    cols = [col[t] for t in [*stocks, "SPY"] if t in col]
    lo = max(0, i - LONG + 1)
    long_close = pd.DataFrame(H.c[lo:i + 1][:, cols], index=H.dates[lo:i + 1], columns=[H.names[j] for j in cols])
    win0 = i - (k - a)
    prior = {}
    if win0 > 0:
        for t in stocks:
            x = H.high_max[win0 - 1, col[t]]
            if np.isfinite(x):
                prior[t] = float(x)
    macro = {s: x[x.index <= H.dates[i]] for s, x in H.macro.items()}
    every = [{"ticker": t, "opened_on": asof, "entry_price": math.nan, "shares": 0} for t in stocks]
    return Ctx(asof=asof, close=f["close"], high=f["high"], low=f["low"], open=f["open"], volume=f["volume"], f=f,
               stocks=stocks, etfs=etfs, sectors=panel.sectors, sources={t: ["sp500"] for t in stocks if t in H.sp500.get(wk, set())},
               names={}, prior_high=prior, fund=fund, angle=_angles(H, fund, i, f["close"], stocks) if with_fund else {},
               macro=macro, holdings=every, watchlist=[], arrays=arrays, long_close=long_close)


def books():
    from scout import playbooks as pb

    return [p for p in pb.PLAYBOOKS if p.fn is not None and p.school not in SKIP_SCHOOLS and p.key not in NO_HISTORY]


def due(H: Hist) -> dict[int, list]:
    """Session → the playbooks deciding at its close."""
    out: dict[int, list] = {}
    for p in books():
        for i in H.days[CADENCE[p.key]]:
            out.setdefault(i, []).append(p)
    return out


def verdicts(ctx, playbooks: list, col: dict[str, int]) -> dict[str, dict | None]:
    """The given playbooks on one context → {key: {"buy": ordered columns, "sell": columns} | {"w": weights} |
    {"etf", "action"} | None}. Holdings are set to every stock, so a playbook's sell rule is computed for all of them
    (only `ranked` and Connors' exit read the holdings)."""
    from scout import playbooks as pb
    from scout.playbooks.base import BUY, HOLD, SELL

    dv = ctx.f["dollar_vol20"].iloc[-1]
    out: dict[str, dict | None] = {}
    for p in playbooks:
        ctx.fresh = 1 if CADENCE[p.key] == "daily" else 5
        ctx.cache.pop("missing", None)
        v = pb.apply(ctx, p)
        if v is None:
            out[p.key] = None
        elif p.school == "资产配置":
            # an ETF without enough history yet means no verdict (the playbook starts later), not cash
            out[p.key] = None if ctx.cache.get("missing", {}).get(p.key) else \
                {"w": {t: float(x.score or 0.0) for t, x in v.items() if x.action in (BUY, HOLD)}}
        elif p.school == "大盘择时":
            t = "SPY" if "SPY" in v else next(iter(v), None)
            out[p.key] = {"etf": t, "action": v[t].action} if t else None
        else:
            buys = [(t, x) for t, x in v.items() if x.action == BUY and t in col]
            buys.sort(key=lambda tx: (-(tx[1].score if tx[1].score is not None else -1e9), -float(dv.get(tx[0], 0) or 0)))
            out[p.key] = {"buy": np.array([col[t] for t, _ in buys], dtype=np.int16),
                          "sell": np.array(sorted(col[t] for t, x in v.items() if x.action == SELL and t in col), dtype=np.int16)}
    return out


def run_block(args: tuple) -> dict[int, dict]:
    """Verdicts on a run of consecutive sessions (one process): one feature panel for the block."""
    H, sessions, todo = args
    first, last = sessions[0], sessions[-1]
    a = max(0, first - WINDOW - 260)                   # 260 sessions of warm-up for the 252-session features
    weeks = {H.week_of(i) for i in sessions}
    cols = sorted({H.col[t] for w in weeks for t in H.members[w] if t in H.col} | set(range(H.n_stocks, len(H.names))))
    panel = _panel(H, a, last, cols)
    col = H.col
    out = {}
    if not any(todo.get(i) for i in sessions):
        return {}
    for i in sessions:
        books_i = todo.get(i)
        if not books_i:
            continue
        try:
            ctx = context(H, panel, i, i - a, with_fund=any(p.school in FUND_SCHOOLS or p.school == "事件与投机"
                                                            or p.key in ("gross_profitability", "low_accruals",
                                                                         "low_asset_growth", "net_buyback") for p in books_i))
            out[i] = {"stocks": np.array([col[t] for t in ctx.stocks], dtype=np.int16), "v": verdicts(ctx, books_i, col)}
        except Exception as exc:                       # a day that cannot be rebuilt is skipped: nothing traded
            logger.warning("session %s skipped: %s", H.dates[i].date(), exc, exc_info=True)
            out[i] = {"stocks": None, "v": {}}
    logger.info("block %s..%s done", H.dates[first].date(), H.dates[last].date())
    return out


_H: Hist | None = None
_TODO: dict | None = None


def _run_chunk(sessions: list[int]) -> dict[int, dict]:
    return run_block((_H, sessions, _TODO))


def compute_verdicts(H: Hist, workers: int = 8, block: int = 125, todo: dict[int, list] | None = None) -> dict[int, dict]:
    """`todo`: session → playbooks to decide (default: each playbook on its own cadence, `due`)."""
    import multiprocessing as mp

    global _H, _TODO
    _H, _TODO = H, todo if todo is not None else due(H)
    sessions = [i for i in range(H.start, len(H.dates)) if i >= min(_TODO, default=H.start)]
    chunks = [sessions[k:k + block] for k in range(0, len(sessions), block)]
    with mp.get_context("fork").Pool(workers) as pool:
        parts = pool.map(_run_chunk, chunks)
    return {i: x for part in parts for i, x in part.items()}


# ── trading the verdicts (pure) ──

def _px(H: Hist, arr: np.ndarray, d: int, j: int) -> float | None:
    """A price on session d, else the latest close before it (a halted stock is valued at its last close)."""
    x = arr[d, j]
    if np.isfinite(x) and x > 0:
        return float(x)
    back = H.c[max(0, d - 10):d, j]
    ok = back[np.isfinite(back) & (back > 0)]
    return float(ok[-1]) if ok.size else None


def simulate_stocks(H: Hist, ver: dict[int, dict], key: str, cost: float, rebalance: bool) -> dict:
    """Decisions at a close, trades at the next open, marked at each close; measured from the first decision."""
    decisions = set(H.days[CADENCE[key]])
    cash, pos = 1.0, {}                                 # pos: column → shares
    nav = np.full(len(H.dates), np.nan)
    trades, held, started = 0, [], None
    for d in range(H.start, len(H.dates)):
        x = ver.get(d - 1) if d - 1 in decisions else None
        book = x["v"].get(key) if x and x["stocks"] is not None else None
        if book is not None:
            started = started if started is not None else d - 1
            live = set(x["stocks"].tolist())
            out = set(book["sell"].tolist())
            keep = set(book["buy"].tolist()) if rebalance else None
            for j in list(pos):
                if j in out or j not in live or (keep is not None and j not in keep):
                    cash += pos.pop(j) * (_px(H, H.o, d, j) or 0.0) * (1 - cost)
                    trades += 1
            value = cash + sum(n * (_px(H, H.o, d, j) or 0.0) for j, n in pos.items())
            slot = value / SLOTS
            for j in book["buy"].tolist():
                if len(pos) >= SLOTS or cash < 0.5 * slot:
                    break
                if j in pos or not (np.isfinite(H.o[d, j]) and H.o[d, j] > 0):
                    continue
                spend = min(slot, cash / (1 + cost))
                pos[j] = spend / H.o[d, j]
                cash -= spend * (1 + cost)
                trades += 1
        cash *= 1 + H.rf[d]
        if started is not None:
            nav[d] = cash + sum(n * (_px(H, H.c, d, j) or 0.0) for j, n in pos.items())
            held.append(len(pos))
    if started is not None:
        nav[started] = 1.0
    return {"nav": nav, "trades": trades, "avg_positions": float(np.mean(held)) if held else None}


def simulate_weights(H: Hist, ver: dict[int, dict], key: str, cost: float, timing: bool) -> dict:
    """Target weights in ETFs (timing: 100 % in the verdict's ETF on 买入 / 持有, else cash), re-set at the next open
    after each decision (timing: when the state changes); measured from the first verdict."""
    col = H.col
    decisions = set(H.days[CADENCE[key]])
    cash, pos, target = 1.0, {}, None
    nav = np.full(len(H.dates), np.nan)
    trades, invested, started = 0, [], None
    for d in range(H.start, len(H.dates)):
        x = ver.get(d - 1) if d - 1 in decisions else None
        book = x["v"].get(key) if x and x["stocks"] is not None else None
        new = None
        if book is not None:
            if timing:
                new = {book["etf"]: 1.0} if book["action"] in ("买入", "持有") else {}
            else:
                new = {t: w for t, w in book["w"].items() if t in col and w > 0}
        if new is not None and (not timing or new != target):
            started = started if started is not None else d - 1
            target = new
            value = cash + sum(n * (_px(H, H.o, d, j) or 0.0) for j, n in pos.items())
            want = {col[t]: w * value for t, w in target.items()}
            for j in set(pos) | set(want):
                px = _px(H, H.o, d, j)
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
        if started is not None:
            nav[d] = cash + sum(n * (_px(H, H.c, d, j) or 0.0) for j, n in pos.items())
            invested.append(1.0 - cash / nav[d] if nav[d] else 0.0)
    if started is not None:
        nav[started] = 1.0
    return {"nav": nav, "trades": trades, "invested": float(np.mean(invested)) if invested else None}


def benchmarks(H: Hist, ver: dict[int, dict]) -> dict[str, np.ndarray]:
    """SPY bought and held, and every liquid top-1000 stock in equal weight (re-set each month, same cost), both from
    the first session's close."""
    s0 = H.start
    spy = H.c[:, H.col["SPY"]] / H.c[s0, H.col["SPY"]]
    spy[:s0] = np.nan
    nav = np.full(len(H.dates), np.nan)
    value, w = 1.0, {}
    months = set(H.days["monthly"])
    for d in range(s0, len(H.dates)):
        if w:
            growth = {j: (_px(H, H.c, d, j) or 0.0) / (_px(H, H.c, d - 1, j) or 1.0) for j in w}
            g = sum(wt * growth[j] for j, wt in w.items())
            value *= g
            w = {j: wt * growth[j] / g for j, wt in w.items()} if g else w
        x = ver.get(d)
        if (d in months or not w) and x and x["stocks"] is not None and len(x["stocks"]):
            live = x["stocks"].tolist()
            new = {j: 1 / len(live) for j in live}
            value *= 1 - COST * sum(abs(new.get(j, 0) - w.get(j, 0)) for j in set(new) | set(w))
            w = new
        nav[d] = value
    return {"SPY": spy, "等权股票池": nav}


# ── metrics ──

def stats(nav: np.ndarray, dates: pd.DatetimeIndex, rf: np.ndarray) -> dict | None:
    s = pd.Series(nav, index=dates).dropna()
    if len(s) < 260:
        return None
    r = s.pct_change().dropna()
    ex = r - pd.Series(rf, index=dates).reindex(r.index)
    years = (s.index[-1] - s.index[0]).days / 365.25
    ye = s.resample("YE").last()
    cal = ye.pct_change()
    cal.iloc[0] = ye.iloc[0] / s.iloc[0] - 1
    return {"start": s.index[0].date().isoformat(), "end": s.index[-1].date().isoformat(),
            "cagr": float((s.iloc[-1] / s.iloc[0]) ** (1 / years) - 1), "total": float(s.iloc[-1] / s.iloc[0] - 1),
            "vol": float(r.std() * math.sqrt(252)), "sharpe": float(ex.mean() / ex.std() * math.sqrt(252)) if ex.std() else None,
            "max_dd": float((s / s.cummax() - 1).min()), "worst_year": float(cal.min()), "best_year": float(cal.max()),
            "years": {str(k.year): float(x) for k, x in cal.items()}}


def halves(nav: np.ndarray, dates: pd.DatetimeIndex, rf: np.ndarray) -> dict:
    cut = dates.searchsorted(SPLIT)
    a, b = nav.copy(), nav.copy()
    a[cut:], b[:cut] = np.nan, np.nan
    return {"first": stats(a, dates, rf), "second": stats(b, dates, rf)}


def excess_t(nav: np.ndarray, ref: np.ndarray, dates: pd.DatetimeIndex) -> dict | None:
    """Monthly return minus the reference's, mean and Newey-West t (lag 3), over the playbook's own months."""
    from scout.small_cap_rankers import nw_t

    s, q = pd.Series(nav, index=dates), pd.Series(ref, index=dates)
    ok = s.notna() & q.notna()
    if ok.sum() < 260:
        return None
    m = (s[ok].resample("ME").last().pct_change() - q[ok].resample("ME").last().pct_change()).dropna()
    t = nw_t(m, 3)
    return {"months": int(len(m)), "mean": float(m.mean()), "t": float(t) if t is not None else float("nan")}


def same_period(bench: dict[str, np.ndarray], start: str, dates: pd.DatetimeIndex, rf: np.ndarray) -> dict:
    """Each benchmark over the playbook's own span (from its first decision), so a late starter is compared fairly."""
    out = {}
    for k, nav in bench.items():
        a = nav.copy()
        a[dates < pd.Timestamp(start)] = np.nan
        st = stats(a, dates, rf)
        out[k] = {"cagr": st["cagr"], "max_dd": st["max_dd"], "sharpe": st["sharpe"]} if st else None
    return out


def evaluate(H: Hist, ver: dict[int, dict]) -> dict:
    bench = benchmarks(H, ver)
    res = {"key": KEY, "period": [H.dates[H.start].date().isoformat(), H.dates[-1].date().isoformat()],
           "slots": SLOTS, "cost": COST, "cost_high": COST_HIGH, "benchmarks": {}, "playbooks": [],
           "not_run": {**NO_HISTORY}}
    for k, nav in bench.items():
        res["benchmarks"][k] = {"all": stats(nav, H.dates, H.rf), **halves(nav, H.dates, H.rf)}
    for p in books():
        timing, alloc = p.school == "大盘择时", p.school == "资产配置"
        runs = {}
        for label, cst in (("main", COST), ("high", COST_HIGH)):
            runs[label] = (simulate_weights(H, ver, p.key, cst, timing) if timing or alloc
                           else simulate_stocks(H, ver, p.key, cst, p.key in REBALANCE))
        nav = runs["main"]["nav"]
        st = stats(nav, H.dates, H.rf)
        if st is None or not runs["main"]["trades"]:
            res["not_run"][p.key] = "没有足够的历史结果（数据缺失或从未给出交易）"
            continue
        ref = bench["SPY"] if timing or alloc else bench["等权股票池"]
        res["playbooks"].append({
            "key": p.key, "name": p.name, "school": p.school, "version": p.version, "cadence": CADENCE[p.key],
            "rebalance": p.key in REBALANCE, "all": st, **halves(nav, H.dates, H.rf),
            "high_cost": stats(runs["high"]["nav"], H.dates, H.rf),
            "vs_spy": excess_t(nav, bench["SPY"], H.dates), "vs_ref": excess_t(nav, ref, H.dates),
            "ref": "SPY" if timing or alloc else "等权股票池", "same_period": same_period(bench, st["start"], H.dates, H.rf),
            "trades": runs["main"]["trades"], "avg_positions": runs["main"].get("avg_positions"),
            "invested": runs["main"].get("invested")})
    return res


# ── report ──

def _p(x, d: int = 1) -> str:
    return "—" if x is None else f"{x * 100:+.{d}f}%"


def report_text(res: dict, status: dict[str, str] | None = None) -> str:
    status = status or {}
    b = res["benchmarks"]
    lines = [f"打法二十年回测（{res['period'][0]} 至 {res['period'][1]}，Sharadar 前 1000 大、含退市股；每个打法按自己的节奏判断"
             f"（每日 / 每周 / 每月 / 每季 / 每年），收盘判断、次日开盘成交；个股打法最多 {res['slots']} 只等权，单边成本 "
             f"{res['cost'] * 100:.2f}%）。描述历史，不改变任何打法的检验状态。", ""]
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
            lines.append(f"· {r['name']}（{CADENCE_ZH[r['cadence']]}{'调仓' if r['rebalance'] else '判断'}"
                         f"{'；' + status[r['key']] if r['key'] in status else ''}）：年化 {_p(a['cagr'])}"
                         f"（高成本 {_p((r['high_cost'] or {}).get('cagr'))}），最大回撤 {_p(a['max_dd'], 0)}，夏普 "
                         f"{a['sharpe']:.2f}；前十年 {_p((r['first'] or {}).get('cagr'))}，后十年 {_p((r['second'] or {}).get('cagr'))}；"
                         f"每月超额 {_p(ref.get('mean'), 2)}（t {ref.get('t', float('nan')):.1f}）；交易 {r['trades']} 笔"
                         + (f"；起于 {a['start']}" if a["start"] > res["period"][0] else "")
                         + (f"；同期 {r['ref']} {_p(sp['cagr'])}" if (sp := (r.get("same_period") or {}).get(r["ref"])) else ""))
    if res["not_run"]:
        lines += ["", "没有回测：" + "；".join(f"{k}：{v}" for k, v in res["not_run"].items())]
    return "\n".join(lines)
