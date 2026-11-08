"""Basket valuation for the weekly report (user's request 2026-11-08, after Baird's "AI valuations sharply compressed"
could not be checked): trailing P/E of the S&P 500, Nasdaq-100, S&P 500 information technology and the large-cap
semiconductors, now, 21 and 63 sessions ago, against each basket's own month-end history since 2015, with the change
split into market value and trailing earnings; plus today's forward P/E where Yahoo snapshots cover the members.

P/E = Σ market value ÷ Σ trailing four-quarter net income (losses included, as index P/Es are computed). Market value =
close adjusted for splits only (not dividends) × the latest diluted share count, moved onto today's split basis.
History: today's members (survivorship: companies that left the index are missing), and every filed version of
each quarter from its own filing date (`sec.financial_rows` keeps the first filing and each change since the
2026-11-08 fix and re-import). Descriptive; not a valuation signal.
"""
from __future__ import annotations

import logging
from bisect import bisect_right
from datetime import date, timedelta

import pandas as pd

from scout import db

logger = logging.getLogger("scout.valuation")

BASKETS = (("sp500", "标普 500 成分"), ("ndx", "纳指 100 成分"), ("it", "标普 500 信息技术"), ("semis", "大盘半导体"))
SEMIS = ("Semiconductors", "Semiconductor Materials & Equipment")
HISTORY_START = date(2015, 1, 1)
FRESH = {"quarter": 200, "year": 460}        # a trailing figure older than this is stale, the stock is left out
CHECK = (0.7, 1 / 0.7)                       # computed vs Yahoo market value today; outside = bad share data, dropped
SESSIONS = (21, 63)


def span(start: date | None, end: date) -> str | None:
    if start is None:
        return None
    days = (end - start).days
    return "quarter" if 80 <= days <= 100 else "year" if 350 <= days <= 380 else None


def versions(rows: list[dict]) -> dict[tuple[str, str], list[tuple[date, date, str | None, float, date]]]:
    """(ticker, metric) → [(known from = filed, period end, span, value, filed)] sorted by filing date."""
    out: dict[tuple[str, str], list] = {}
    for r in sorted(rows, key=lambda r: r["filed"]):
        sp = span(r.get("period_start"), r["period_end"]) if r["metric"] != "shares_diluted" else "quarter"
        out.setdefault((r["ticker"], r["metric"]), []).append((r["filed"], r["period_end"], sp, float(r["value"]), r["filed"]))
    return out


def as_of(v: list[tuple], d: date) -> list[tuple[date, str | None, float, date]]:
    """Latest version per period known by d: [(period end, span, value, filed)] by period end."""
    known: dict[date, tuple] = {}
    for k, end, sp, val, filed in v[:bisect_right([x[0] for x in v], d)]:
        known[end] = (end, sp, val, filed)
    return [known[e] for e in sorted(known)]


def trailing(income: list[tuple], d: date) -> float | None:
    """Sum of the last four consecutive quarters, else the last fiscal year (annual filers); None when stale."""
    q = [x for x in income if x[1] == "quarter"]
    if len(q) >= 4 and 250 <= (q[-1][0] - q[-4][0]).days <= 300 and (d - q[-1][0]).days <= FRESH["quarter"]:
        return sum(x[2] for x in q[-4:])
    y = [x for x in income if x[1] == "year"]
    if y and (d - y[-1][0]).days <= FRESH["year"] and not (q and q[-1][0] > y[-1][0]):
        return y[-1][2]
    return None


def split_factor(splits: list[tuple[date, float]], after: date) -> float:
    f = 1.0
    for d, r in splits:
        if d > after:
            f *= r
    return f


def shares(v: list[tuple], splits: list[tuple[date, float]], d: date) -> float | None:
    """The diluted share count known by d on today's split basis (splits after a filing multiply it): the median of
    the last three periods, so one mis-scaled filing (WAT's 2025 XBRL: 59.7 billion for 59.7 million) does not count."""
    rows = as_of(v, d)
    if not rows or (d - rows[-1][0]).days > FRESH["year"]:
        return None
    last = sorted(val * split_factor(splits, filed) for _, _, val, filed in rows[-3:])
    return last[len(last) // 2]


def stock_at(fin: dict, splits: dict, ticker: str, close: float | None, d: date,
             ref: float | None = None) -> tuple[float, float] | None:
    """(market value, trailing net income) on d, or None when anything is missing. `ref` is today's checked share
    count: a past count more than twice away from it is a data error or a merger, and the stock sits that date out."""
    if close is None or pd.isna(close):
        return None
    inc = fin.get((ticker, "net_income"))
    sh = fin.get((ticker, "shares_diluted"))
    if not inc or not sh:
        return None
    ni = trailing(as_of(inc, d), d)
    n = shares(sh, splits.get(ticker, []), d)
    if ni is None or not n or (ref and not 0.5 <= n / ref <= 2):
        return None
    return float(close) * n, ni


def implied_ok(fin: dict, ticker: str, n: float, d: date) -> bool:
    """Today's share count against trailing net income ÷ trailing diluted EPS (the filer's own per-share figure);
    True when they agree within CHECK or the EPS is too small to tell."""
    eps = fin.get((ticker, "eps_diluted"))
    ni = trailing(as_of(fin.get((ticker, "net_income"), []), d), d)
    e = trailing(as_of(eps, d), d) if eps else None
    if not e or not ni or abs(e) < 0.25 or (ni > 0) != (e > 0):
        return True
    return CHECK[0] <= n / (ni / e) <= CHECK[1]


def stocks_at(tickers: list[str], fin: dict, splits: dict, closes: pd.Series, d: date,
              ref: dict[str, float] | None = None) -> dict[str, tuple[float, float]]:
    ref = ref or {}
    return {t: x for t in tickers if (x := stock_at(fin, splits, t, closes.get(t), d, ref.get(t)))}


def basket_at(members: list[str], stocks: dict[str, tuple[float, float]]) -> dict:
    per = {t: stocks[t] for t in members if t in stocks}
    mv, ni = sum(x[0] for x in per.values()), sum(x[1] for x in per.values())
    return {"pe": mv / ni if per and ni > 0 else None, "n": len(per), "per": per}


def change(a: dict, b: dict) -> dict | None:
    """Market value and trailing income change from b to a on the stocks present at both dates."""
    common = set(a["per"]) & set(b["per"])
    if not common:
        return None
    mv0, mv1 = sum(b["per"][t][0] for t in common), sum(a["per"][t][0] for t in common)
    ni0, ni1 = sum(b["per"][t][1] for t in common), sum(a["per"][t][1] for t in common)
    return {"mv": mv1 / mv0 - 1, "ni": ni1 / ni0 - 1 if ni0 > 0 and ni1 > 0 else None}


def history_stats(series: list[float], now: float) -> dict:
    s = sorted(series)
    return {"share_below": sum(x < now for x in s) / len(s), "median": s[len(s) // 2], "p10": s[len(s) // 10],
            "p90": s[min(len(s) - 1, len(s) * 9 // 10)], "months": len(s)}


def forward(members: list[str], snaps: dict[str, dict]) -> dict | None:
    """Σ market value ÷ Σ (market value ÷ forward P/E) over the members Yahoo covers (losses included)."""
    mv = earn = 0.0
    n = 0
    for t in members:
        s = snaps.get(t) or {}
        if s.get("marketCap") and s.get("forwardPE"):
            mv += s["marketCap"]
            earn += s["marketCap"] / s["forwardPE"]
            n += 1
    return {"pe": mv / earn, "n": n} if n and earn > 0 else None


async def members() -> dict[str, list[str]]:
    """Basket members, one ticker per company (GOOGL / GOOG share one CIK, so one company's earnings)."""
    rows = await db.fetch_all(
        "SELECT i.ticker, i.sector, i.industry, i.sources, c.cik FROM scout.instruments i "
        "LEFT JOIN scout.sec_companies c USING (ticker) WHERE i.kind = 'stock' ORDER BY i.ticker")
    out: dict[str, list[str]] = {k: [] for k, _ in BASKETS}
    seen: set[str] = set()
    for r in rows:
        if r["cik"]:
            if r["cik"] in seen:
                continue
            seen.add(r["cik"])
        src = r["sources"] or []
        large = "sp500" in src or "ndx" in src
        for key, cond in (("sp500", "sp500" in src), ("ndx", "ndx" in src),
                          ("it", "sp500" in src and r["sector"] == "Information Technology"),
                          ("semis", large and r["industry"] in SEMIS)):
            if cond:
                out[key].append(r["ticker"])
    return out


async def compute(asof: date) -> dict | None:
    from scout.data.yahoo import unadjusted_closes

    groups = await members()
    tickers = sorted({t for v in groups.values() for t in v})
    if not tickers:
        return None
    rows = await db.fetch_all(
        "SELECT ticker, metric, period_end, period_start, filed, value FROM scout.financials "
        "WHERE ticker = ANY(:t) AND metric IN ('net_income', 'shares_diluted', 'eps_diluted') AND period_end >= :s AND filed <= :d",
        {"t": tickers, "s": HISTORY_START - timedelta(days=500), "d": asof})
    fin = versions([dict(r) for r in rows])
    monthly, splits = await unadjusted_closes(tickers, HISTORY_START, "1mo")
    daily, _ = await unadjusted_closes(tickers, asof - timedelta(days=130))
    daily = daily[[d <= asof for d in daily.index]]
    if daily.empty or len(daily) <= max(SESSIONS):
        return None
    snaps = {r["ticker"]: r["payload"] for r in await db.fetch_all(
        "SELECT DISTINCT ON (ticker) ticker, payload FROM scout.research_snapshots WHERE kind = 'ownership' "
        "AND ticker = ANY(:t) AND asof BETWEEN :a AND :d ORDER BY ticker, asof DESC",
        {"t": tickers, "a": asof - timedelta(days=7), "d": asof})}
    d0 = daily.index[-1]
    now = stocks_at(tickers, fin, splits, daily.iloc[-1], d0)
    ref = {t: shares(fin[(t, "shares_diluted")], splits.get(t, []), d0) for t in now}
    dropped = sorted(t for t, (mv, _) in now.items()
                     if not implied_ok(fin, t, ref[t], d0) or
                     ((snaps.get(t) or {}).get("marketCap") and not CHECK[0] <= mv / snaps[t]["marketCap"] <= CHECK[1]))
    for t in dropped:
        now.pop(t)
    keep = [t for t in tickers if t not in dropped]
    past = {n: (daily.index[-1 - n], stocks_at(keep, fin, splits, daily.iloc[-1 - n], daily.index[-1 - n], ref)) for n in SESSIONS}
    months = [stocks_at(keep, fin, splits, row, d, ref) for d, (_, row) in
              zip([(pd.Timestamp(x) + pd.offsets.MonthEnd(0)).date() for x in monthly.index], monthly.iterrows())
              if d < d0.replace(day=1)]
    out = []
    for key, name in BASKETS:
        mem = [t for t in groups[key] if t not in dropped]
        b0 = basket_at(mem, now)
        if b0["pe"] is None:
            continue
        hist = [b["pe"] for m in months if (b := basket_at(mem, m))["pe"] and b["n"] >= 0.6 * len(mem)]
        then = {n: (d, basket_at(mem, st)) for n, (d, st) in past.items()}
        out.append({"key": key, "name": name, "members": len(groups[key]), "used": b0["n"], "pe": b0["pe"],
                    **({"history": history_stats(hist, b0["pe"])} if len(hist) >= 24 else {}),
                    "past": {str(n): {"date": d.isoformat(), "pe": b["pe"], "change": change(b0, b)} for n, (d, b) in then.items()},
                    "forward": forward(mem, snaps)})
    return {"asof": d0.isoformat(), "baskets": out, "dropped": dropped}


def _p(x: float | None) -> str:
    return "—" if x is None else f"{x * 100:+.1f}%"


def text(b: dict) -> str:
    parts = [f"{b['pe']:.1f}（{b['used']}/{b['members']} 家有数据）"]
    for n, p in b["past"].items():
        c = p["change"] or {}
        parts.append(f"{n} 个交易日前 {p['pe']:.1f}，期间总市值 {_p(c.get('mv'))}、滚动净利润 {_p(c.get('ni'))}"
                     if p["pe"] else f"{n} 个交易日前无数据")
    h = b.get("history")
    if h:
        parts.append(f"2015 年以来 {h['months']} 个月末：中位数 {h['median']:.1f}，10%–90% 区间 {h['p10']:.1f}–{h['p90']:.1f}，"
                     f"现在高于其中 {h['share_below'] * 100:.0f}% 的月份")
    f = b.get("forward")
    parts.append(f"远期市盈率 {f['pe']:.1f}（Yahoo，{f['n']}/{b['members']} 家；历史快照不足，不比较）" if f else "远期市盈率无数据")
    return "；".join(parts)


MEANING = ("滚动市盈率 = 成分股总市值 ÷ 过去四个季度净利润合计（亏损计入）。和自身历史比：低于中位数 = 比过去便宜；"
           "总市值跌、净利润涨 = 估值压缩来自价格下跌。历史用今天的成分股（已退出的公司不在内）和各季度最终公布的财报；描述现状，不是买卖信号")


async def items(asof: date) -> list[dict]:
    """Evidence items for the weekly pack (字段, 名称, 数值, 含义)."""
    v = await compute(asof)
    if not v:
        return []
    return [{"字段": f"valuation.{b['key']}", "名称": f"{b['name']} 滚动市盈率（{v['asof']}）", "数值": text(b), "含义": MEANING}
            for b in v["baskets"]]
