"""Rechecks on Sharadar data (pre-registered in `scout/knowledge/external_specs/sharadar_rechecks.yaml`).

Recheck 1, low asset growth: the rule of `rank_low_asset_growth` (every 21 sessions the lowest
10 % by total-asset growth, next open, 63 sessions, control = the mean of every other stock of the
universe that day) on the point-in-time universe of `scout.sharadar_research`, 1999 to date.
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
            rows.append({"date": d, "ret": r, "select": (total - r) / (n - 1), "sector": sector.get(p.tickers[j])})
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


def summarize(rows: list[dict], required_t: float) -> dict:
    if not rows:
        return {"n": 0, "pass": False}
    dates = sorted(r["date"] for r in rows)
    mid = dates[len(dates) // 2]
    first, second = [r for r in rows if r["date"] < mid], [r for r in rows if r["date"] >= mid]
    out = {"n": len(rows), "first_date": str(dates[0].date()), "last_date": str(dates[-1].date()), "split": str(mid.date()),
           "all": edge(rows), "first": edge(first), "second": edge(second),
           "scout_window": edge([r for r in rows if r["date"] >= pd.Timestamp(SCOUT_START)]),
           "bear": edge(_months(rows, osap_check.BEARS)),
           "decades": {f"{y}s": edge([r for r in rows if y <= r["date"].year < y + 10]) for y in (1990, 2000, 2010, 2020)},
           "sectors": {s: edge([r for r in rows if r["sector"] == s]) for s in sorted({r["sector"] for r in rows if r["sector"]})},
           "required_t": required_t}
    a = out["all"]
    out["pass"] = bool(a["t"] is not None and a["t"] >= required_t and (out["first"]["edge"] or 0) > 0 and (out["second"]["edge"] or 0) > 0)
    return out


def report_text(large: dict, small: dict, trials: int) -> str:
    p = lambda x: "—" if x is None else f"{x:+.2%}"  # noqa: E731
    lines = [f"复核 1：资产增长最慢 10%（Sharadar，时点正确、含退市公司；每 21 个交易日，持有 63 日，对照 = 当天股票池其他股票平均）",
             f"本族第 {trials} 次检验，t 门槛 {large['required_t']}"]
    for name, s in (("大中盘（市值前 1000）", large), ("小盘（1001–3000）", small)):
        if not s.get("n"):
            lines.append(f"{name}：无样本")
            continue
        lines.append(f"{name}：{s['first_date']} 至 {s['last_date']}，{s['n']} 笔；全部 {p(s['all']['edge'])}（t {s['all']['t']}），"
                     f"前半 {p(s['first']['edge'])}（{s['first']['t']}）/ 后半 {p(s['second']['edge'])}（{s['second']['t']}），按 {s['split']} 分；"
                     f"熊市月份 {p(s['bear']['edge'])}；{'通过' if s['pass'] else '未通过'}")
        lines.append("  逐年代：" + "，".join(f"{k} {p(v['edge'])}（{v['t']}）" for k, v in s["decades"].items() if v["n"]))
        lines.append(f"  与 Scout 同一时期（{SCOUT_START} 起）：{p(s['scout_window']['edge'])}（t {s['scout_window']['t']}）；"
                     f"Scout 在当前成分股上：{p(SCOUT_RESULT['edge'])}（t {SCOUT_RESULT['t']}）")
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
