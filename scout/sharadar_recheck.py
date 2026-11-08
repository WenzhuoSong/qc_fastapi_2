"""Rechecks on Sharadar data (pre-registered in `scout/knowledge/external_specs/sharadar_rechecks.yaml`).

Recheck 1, low asset growth: the rule of `rank_low_asset_growth` (every 21 sessions the lowest
10 % by total-asset growth, next open, 63 sessions, control = the mean of every other stock of the
universe that day) on the point-in-time universe of `scout.sharadar_research`, 1999 to date.

Low accruals (`low_accruals_sharadar_v1.yaml`): the rule of `rank_low_accruals` the same way, accruals from
first-filed ARQ quarters (net income − operating cash flow over four quarters ÷ average total assets).
"""
from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd

from scout import gate, osap_check
from scout import sharadar_research as sr

EVERY, HOLD, FRAC = 21, 63, 0.1
FIRST = "1999-01-04"
SCOUT_START = "2015-11-17"        # the first rebalance of Scout's own run of this rule
SCOUT_RESULT = {"edge": 0.0088, "t": 3.26}
ACCRUALS_SCOUT_START = "2015-11-27"   # Scout's 2026-11-08 run of rank_low_accruals (after the SEC fix)
ACCRUALS_SCOUT_RESULT = {"edge": 0.0247, "t": 5.94}


def rebalance_days(dates: pd.DatetimeIndex, hold: int = HOLD, every: int = EVERY, first: str = FIRST) -> list[int]:
    start = int(np.searchsorted(dates, pd.Timestamp(first)))
    return list(range(start, len(dates) - hold - 1, every))


def rank_rows(p: sr.Prices, members: dict[pd.Timestamp, list[str]], signal: dict[pd.Timestamp, dict[str, float]],
              days: list[int], sector: dict[str, str], hold: int = HOLD, frac: float = FRAC, lowest: bool = True) -> list[dict]:
    """Per pick: its holding return and the mean of every other stock of the universe that day."""
    rows = []
    for i in days:
        d = p.dates[i]
        cols = [p.col[t] for t in members.get(d, []) if t in p.col]
        rets = {}
        for j in cols:
            r = sr.holding_return(p, j, i, hold)
            if r is not None:
                rets[j] = r
        if len(rets) < 2:
            continue
        sig = {j: signal.get(d, {}).get(p.tickers[j]) for j in rets}
        sig = {j: v for j, v in sig.items() if v is not None and np.isfinite(v)}
        k = max(1, int(round(len(cols) * frac)))
        picks = sorted(sig, key=lambda j: sig[j] if lowest else -sig[j])[:k]
        total, n = sum(rets.values()), len(rets)
        for j in picks:
            r = rets[j]
            sec = sector.get(p.tickers[j])
            rows.append({"date": d, "ret": r, "select": (total - r) / (n - 1), "sector": sec if isinstance(sec, str) else None})
    return rows


def _months(rows: list[dict], spans) -> list[dict]:
    out = []
    for r in rows:
        d = pd.Timestamp(r["date"])
        if any(pd.Timestamp(a) <= d <= pd.Timestamp(b) + pd.offsets.MonthEnd(0) for a, b in spans):
            out.append(r)
    return out


def edge(rows: list[dict]) -> dict:
    mean, t, months = gate.monthly_t([r["date"] for r in rows], [r["ret"] - r["select"] for r in rows])
    return {"edge": None if mean is None else round(mean, 5), "t": None if t is None else round(t, 2), "months": months, "n": len(rows)}


def summarize(rows: list[dict], required_t: float, scout_start: str = SCOUT_START) -> dict:
    if not rows:
        return {"n": 0, "pass": False}
    dates = sorted(r["date"] for r in rows)
    mid = dates[len(dates) // 2]
    first, second = [r for r in rows if r["date"] < mid], [r for r in rows if r["date"] >= mid]
    out = {"n": len(rows), "first_date": str(dates[0].date()), "last_date": str(dates[-1].date()), "split": str(mid.date()),
           "all": edge(rows), "first": edge(first), "second": edge(second),
           "scout_window": edge([r for r in rows if r["date"] >= pd.Timestamp(scout_start)]),
           "bear": edge(_months(rows, osap_check.BEARS)),
           "decades": {f"{y}s": edge([r for r in rows if y <= r["date"].year < y + 10]) for y in (1990, 2000, 2010, 2020)},
           "sectors": {s: edge([r for r in rows if r["sector"] == s]) for s in sorted({r["sector"] for r in rows if r["sector"]})},
           "required_t": required_t}
    a = out["all"]
    out["pass"] = bool(a["t"] is not None and a["t"] >= required_t and (out["first"]["edge"] or 0) > 0 and (out["second"]["edge"] or 0) > 0)
    return out


def report_text(large: dict, small: dict, trials: int, title: str = "复核 1：资产增长最慢 10%",
                scout_start: str = SCOUT_START, scout_result: dict = SCOUT_RESULT) -> str:
    p = lambda x: "—" if x is None else f"{x:+.2%}"  # noqa: E731
    lines = [f"{title}（Sharadar，时点正确、含退市公司；每 21 个交易日，持有 63 日，对照 = 当天股票池其他股票平均）",
             f"本族第 {trials} 次检验，t 门槛 {large['required_t']}"]
    for name, s in (("大中盘（市值前 1000）", large), ("小盘（1001–3000）", small)):
        if not s.get("n"):
            lines.append(f"{name}：无样本")
            continue
        lines.append(f"{name}：{s['first_date']} 至 {s['last_date']}，{s['n']} 笔；全部 {p(s['all']['edge'])}（t {s['all']['t']}），"
                     f"前半 {p(s['first']['edge'])}（{s['first']['t']}）/ 后半 {p(s['second']['edge'])}（{s['second']['t']}），按 {s['split']} 分；"
                     f"熊市月份 {p(s['bear']['edge'])}；{'通过' if s['pass'] else '未通过'}")
        lines.append("  逐年代：" + "，".join(f"{k} {p(v['edge'])}（{v['t']}）" for k, v in s["decades"].items() if v["n"]))
        lines.append(f"  与 Scout 同一时期（{scout_start} 起）：{p(s['scout_window']['edge'])}（t {s['scout_window']['t']}）；"
                     f"Scout 在当前成分股上：{p(scout_result['edge'])}（t {scout_result['t']}）")
        if s.get("coverage") is not None:
            lines.append(f"  有应计值的股票占股票池（调仓日中位数）：{s['coverage']:.0%}")
        lines.append("  行业（探索性）：" + "；".join(f"{k} {p(v['edge'])}（{v['n']} 笔）" for k, v in s["sectors"].items() if v["n"] >= 30))
    return "\n".join(lines)


def run_asset_growth(trials: int) -> dict:
    """Load the local data and compute recheck 1 for both universes (no database writes)."""
    dates = sr.trading_days()
    days = rebalance_days(dates)
    want = dates[days]
    elig = sr.eligible()
    uni = sr.universe(want, elig)
    names = sorted({t for v in uni.values() for t in v["large"] + v["small"]})
    p = sr.prices(names, dates)
    growth = sr.asset_growth(want, names)
    sector = elig["sector"].to_dict()
    req = gate.required_t(trials)
    large = summarize(rank_rows(p, {d: v["large"] for d, v in uni.items()}, growth, days, sector), req)
    small = summarize(rank_rows(p, {d: v["small"] for d, v in uni.items()}, growth, days, sector), req)
    return {"large": large, "small": small, "trials": trials, "stocks": len(names), "asof": str(date.today())}


def accruals_table(f: pd.DataFrame) -> pd.DataFrame:
    """First-filed ARQ quarters (ticker, datekey, reportperiod, netinc, ncfo, assets) → one accruals value per
    quarter: (four-quarter netinc − four-quarter ncfo) ÷ average assets, known from the latest of the four filings
    (the rule of `angles.fundamentals_now`: consecutive quarters 250–300 days end to end, assets 330–400 days apart)."""
    f = f.dropna(subset=["datekey", "reportperiod", "netinc", "ncfo", "assets"])
    f = sr.first_filed(f).sort_values(["ticker", "reportperiod"]).copy()
    f["_ns"] = f["datekey"].astype("datetime64[ns]").astype("int64")
    g = f.groupby("ticker")
    f["ni4"] = g["netinc"].transform(lambda x: x.rolling(4).sum())
    f["cfo4"] = g["ncfo"].transform(lambda x: x.rolling(4).sum())
    f["known"] = pd.to_datetime(g["_ns"].transform(lambda x: x.rolling(4).max()), unit="ns")
    span = (f["reportperiod"] - g["reportperiod"].shift(3)).dt.days
    f["prev_assets"], gap = g["assets"].shift(4), (f["reportperiod"] - g["reportperiod"].shift(4)).dt.days
    avg = np.where((gap >= 330) & (gap <= 400) & (f["prev_assets"] > 0), (f["assets"] + f["prev_assets"]) / 2, f["assets"])
    f["accruals"] = (f["ni4"] - f["cfo4"]) / avg
    ok = (span >= 250) & (span <= 300) & (avg > 0) & f["ni4"].notna() & f["cfo4"].notna()
    return f.loc[ok, ["ticker", "reportperiod", "known", "accruals"]]


def accruals_on(table: pd.DataFrame, days: pd.DatetimeIndex) -> dict[pd.Timestamp, dict[str, float]]:
    """{day: {ticker: accruals}}: the latest value known by that day, its last filing within 400 days."""
    t = table.sort_values(["known", "reportperiod"])
    out = {}
    for d in days:
        known = t[(t["known"] <= d) & (t["known"] > d - pd.Timedelta(days=400))]
        last = known.groupby("ticker").tail(1)
        out[d] = dict(zip(last["ticker"], last["accruals"]))
    return out


def run_accruals(trials: int) -> dict:
    """Load the local data and compute the low-accruals recheck for both universes (no database writes)."""
    dates = sr.trading_days()
    days = rebalance_days(dates)
    want = dates[days]
    elig = sr.eligible()
    uni = sr.universe(want, elig)
    names = sorted({t for v in uni.values() for t in v["large"] + v["small"]})
    p = sr.prices(names, dates)
    f = sr._read("fundamentals", ["ticker", "dimension", "date", "reportperiod", "netinc", "ncfo", "assets"],
                 filters=[("dimension", "=", "ARQ"), ("ticker", "in", names)]).rename(columns={"date": "datekey"})
    f["datekey"], f["reportperiod"] = pd.to_datetime(f["datekey"]), pd.to_datetime(f["reportperiod"])
    acc = accruals_on(accruals_table(f), want)
    sector = elig["sector"].to_dict()
    req = gate.required_t(trials)
    out = {"trials": trials, "stocks": len(names), "asof": str(date.today())}
    for part in ("large", "small"):
        members = {d: v[part] for d, v in uni.items()}
        s = summarize(rank_rows(p, members, acc, days, sector), req, ACCRUALS_SCOUT_START)
        cov = [len(set(members[d]) & set(acc.get(d, {}))) / len(members[d]) for d in want if members.get(d)]
        s["coverage"] = float(np.median(cov)) if cov else None
        out[part] = s
    return out
