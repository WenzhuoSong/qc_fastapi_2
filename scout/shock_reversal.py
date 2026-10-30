"""Shock reversal in quality firms without an earnings event, on the local Sharadar files.

Pre-registered in `scout/knowledge/specs/shock_reversal_quality_no_earnings.yaml` (family
short_term_reversal; Chan 2003, Savor 2012): a one-day drop of ≥ 10 % vs SPY in a top-1000 stock
with gross profits / assets above the day's median and positive TTM earnings, with no 8-K item 2.02
and no 10-Q/10-K in the 7 days before, bought at the next open and held 21 sessions; control =
the mean of every other member over the same window. Pure computation, no database.
"""
from __future__ import annotations

import json
from datetime import date

import numpy as np
import pandas as pd

from scout import gate, osap_check
from scout import sharadar_research as sr
from scout.sharadar_recheck import rebalance_days

KEY = "shock_reversal_quality_no_earnings"
FAMILY = "short_term_reversal"
SAMPLE_START = "2004-09-01"        # 8-K item 2.02 in force from 2004-08-23
LOAD_START = "2004-01-01"
SHOCK = -0.10
NEWS_DAYS = 7
FUND_MAX_AGE = 400
HOLD = 21
REPORTED_HOLDS = (5, 63)
UNSEEN_FROM = "2016-01-01"          # the exploratory look covered 2004–2015
CRISIS = ("2008-01-01", "2009-12-31")


def _filings(tickers: list[str]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """(TTM fundamentals as filed, earnings dates (8-K 2.02 + 10-Q/10-K), all 8-K dates)."""
    art = sr._read("fundamentals", ["ticker", "dimension", "date", "gp", "assets", "netinc"],
                   filters=[("dimension", "=", "ART"), ("ticker", "in", tickers)]).rename(columns={"date": "datekey"})
    art["datekey"] = pd.to_datetime(art["datekey"])
    art = art.dropna(subset=["datekey"]).sort_values("datekey")
    arq = sr._read("fundamentals", ["ticker", "dimension", "date"], filters=[("dimension", "=", "ARQ"), ("ticker", "in", tickers)])
    ev = sr._read("events", ["ticker", "date", "eventcodes"], filters=[("ticker", "in", tickers)])
    ev["date"] = pd.to_datetime(ev["date"])
    earn8k = ev[ev["eventcodes"].fillna("").str.split("|").apply(lambda c: "22" in c)][["ticker", "date"]]
    earn = pd.concat([earn8k, pd.DataFrame({"ticker": arq["ticker"], "date": pd.to_datetime(arq["date"])})]).dropna().drop_duplicates()
    return art, earn, ev[["ticker", "date"]].drop_duplicates()


def _asof(art: pd.DataFrame, keys: pd.DataFrame) -> pd.DataFrame:
    """For each (ticker, day): the latest TTM row filed by that day, within FUND_MAX_AGE days."""
    k = keys.assign(day=keys["day"].astype("datetime64[ns]")).sort_values("day").reset_index()
    art = art.assign(datekey=art["datekey"].astype("datetime64[ns]"))
    m = pd.merge_asof(k, art, left_on="day", right_on="datekey", by="ticker", direction="backward")
    stale = (m["day"] - m["datekey"]).dt.days > FUND_MAX_AGE
    m.loc[stale, ["gp", "assets", "netinc"]] = np.nan
    m["gpa"] = np.where(m["assets"] > 0, m["gp"] / m["assets"], np.nan)
    return m.set_index("index").sort_index()


def _near(dates: pd.DataFrame, keys: pd.DataFrame) -> np.ndarray:
    """Whether each (ticker, day) has a date in [day − NEWS_DAYS, day]."""
    k = keys.assign(day=keys["day"].astype("datetime64[ns]")).sort_values("day").reset_index()
    d = dates.rename(columns={"date": "hit"}).astype({"hit": "datetime64[ns]"}).sort_values("hit")
    m = pd.merge_asof(k, d, left_on="day", right_on="hit", by="ticker", direction="backward")
    near = (m["day"] - m["hit"]).dt.days.le(NEWS_DAYS).fillna(False)
    return near.set_axis(m["index"]).sort_index().to_numpy()


def _window_returns(p: sr.Prices, i: int, cols: list[int], hold: int) -> dict[int, float]:
    """Next open → close `hold` sessions later for each column (sr.holding_return, vectorised for the common case)."""
    a, b = i + 1, i + hold
    if b >= len(p.dates):
        return {}
    c = np.array(cols)
    op, ca, cl, end = p.open[a, c], p.closeadj[a, c], p.close[a, c], p.closeadj[b, c]
    ok = np.isfinite(op) & (op > 0) & np.isfinite(ca) & np.isfinite(cl) & (cl > 0)
    out = {}
    for j, e, x, good in zip(c, ca * op / np.where(cl > 0, cl, np.nan), end, ok):
        if not good:
            continue
        if np.isfinite(x):
            out[int(j)] = float(x / e - 1)
        else:
            r = sr.holding_return(p, int(j), i, hold)
            if r is not None:
                out[int(j)] = r
    return out


def edge(rows: list[dict], hold: int = HOLD) -> dict:
    rs = [r for r in rows if r.get(f"ret{hold}") is not None]
    mean, t, months = gate.monthly_t([r["date"] for r in rs], [r[f"ret{hold}"] - r[f"ctl{hold}"] for r in rs])
    return {"edge": None if mean is None else round(mean, 5), "t": None if t is None else round(t, 2), "months": months, "n": len(rs),
            "hit": round(float(np.mean([r[f"ret{hold}"] > r[f"ctl{hold}"] for r in rs])), 3) if rs else None}


def summarize(rows: list[dict], required_t: float) -> dict:
    rows = [r for r in rows if r.get(f"ret{HOLD}") is not None]
    if not rows:
        return {"n": 0, "pass": False, "required_t": required_t}
    dates = sorted(r["date"] for r in rows)
    mid = dates[len(dates) // 2]
    first, second = [r for r in rows if r["date"] < mid], [r for r in rows if r["date"] >= mid]
    out = {"n": len(rows), "first_date": str(dates[0].date()), "last_date": str(dates[-1].date()), "split": str(mid.date()),
           "all": edge(rows), "first": edge(first), "second": edge(second), "required_t": required_t}
    a = out["all"]
    out["pass"] = bool(a["t"] is not None and a["t"] >= required_t and (out["first"]["edge"] or 0) > 0 and (out["second"]["edge"] or 0) > 0)
    return out


def describe(rows: list[dict]) -> dict:
    """Reported only, no weight in the verdict."""
    crisis = lambda r: pd.Timestamp(CRISIS[0]) <= r["date"] <= pd.Timestamp(CRISIS[1])  # noqa: E731
    bear = set(m for a, b in osap_check.BEARS for m in pd.period_range(a, b, freq="M"))
    return {"holds": {h: edge(rows, h) for h in REPORTED_HOLDS},
            "unseen_2016": edge([r for r in rows if r["date"] >= pd.Timestamp(UNSEEN_FROM)]),
            "explored_2004_2015": edge([r for r in rows if r["date"] < pd.Timestamp(UNSEEN_FROM)]),
            "ex_crisis": edge([r for r in rows if not crisis(r)]),
            "bear": edge([r for r in rows if r["date"].to_period("M") in bear]),
            "strict_no_8k": edge([r for r in rows if not r["any8k"]]),
            "years": {y: edge([r for r in rows if r["date"].year == y]) for y in sorted({r["date"].year for r in rows})},
            "sectors": {s: edge([r for r in rows if r["sector"] == s]) for s in sorted({r["sector"] for r in rows if r["sector"]})}}


def run(trials: int) -> dict:
    """Load the local data and compute the pre-registered test plus the reported splits (no database writes)."""
    full = sr.trading_days()
    grid = full[rebalance_days(full, hold=0)]
    dates = full[full >= LOAD_START]
    grid = grid[grid >= dates[0] - pd.Timedelta(days=45)]
    elig = sr.eligible()
    uni = {d: v["large"] for d, v in sr.universe(grid, elig).items()}
    names = sorted({t for v in uni.values() for t in v})
    p = sr.prices(names, dates)
    spy = sr._read("funds", ["ticker", "date", "closeadj"], filters=[("ticker", "=", "SPY")])
    spy = spy.assign(date=pd.to_datetime(spy["date"])).set_index("date")["closeadj"].reindex(dates)
    ret1 = p.closeadj[1:] / p.closeadj[:-1] - 1 - (spy.to_numpy()[1:] / spy.to_numpy()[:-1] - 1)[:, None]
    gdays = sorted(uni)
    start = int(np.searchsorted(dates, pd.Timestamp(SAMPLE_START)))

    shocks = []                                   # (day index, column, membership day)
    busy = np.full(len(names), -1)                # last session of the stock's open window
    for i in range(max(start, 1), len(dates) - 1):
        g = gdays[int(np.searchsorted(gdays, dates[i], side="right")) - 1] if gdays[0] <= dates[i] else None
        if g is None:
            continue
        cols = [p.col[t] for t in uni[g] if t in p.col]
        hits = [j for j in cols if ret1[i - 1, j] <= SHOCK and busy[j] < i]
        for j in hits:
            shocks.append((i, j, g))
            busy[j] = i + HOLD
    keys = pd.DataFrame({"ticker": [names[j] for _, j, _ in shocks], "day": [dates[i] for i, _, _ in shocks]})
    art, earn, any8k = _filings(names)
    q = _asof(art, keys)
    near_earn, near_8k = _near(earn, keys), _near(any8k, keys)
    med_keys = pd.DataFrame([(t, g) for g in gdays for t in uni[g]], columns=["ticker", "day"])
    med = _asof(art, med_keys).assign(g=med_keys["day"].to_numpy()).groupby("g")["gpa"].median()

    sector = elig["sector"].to_dict()
    cache: dict[tuple, dict[int, float]] = {}
    rows = []
    for n, (i, j, g) in enumerate(shocks):
        cols = [p.col[t] for t in uni[g] if t in p.col]
        r = {"date": dates[i], "ticker": names[j], "shock": float(ret1[i - 1, j]), "sector": sector.get(names[j]),
             "quality": bool(q["gpa"].iat[n] > med.get(g, np.nan) and q["netinc"].iat[n] > 0),
             "earnings": bool(near_earn[n]), "any8k": bool(near_8k[n])}
        for h in (HOLD, *REPORTED_HOLDS):
            key = (i, g, h)
            if key not in cache:
                cache[key] = _window_returns(p, i, cols, h)
            w = cache[key]
            if j in w and len(w) > 1:
                r[f"ret{h}"], r[f"ctl{h}"] = w[j], (sum(w.values()) - w[j]) / (len(w) - 1)
            else:
                r[f"ret{h}"] = r[f"ctl{h}"] = None
        rows.append(r)

    req = gate.required_t(trials)
    rule = [r for r in rows if r["quality"] and not r["earnings"]]
    groups = {"all_shocks": rows, "quality_only": [r for r in rows if r["quality"]],
              "no_earnings_only": [r for r in rows if not r["earnings"]],
              "earnings_shocks": [r for r in rows if r["earnings"]],
              "quality_with_earnings": [r for r in rows if r["quality"] and r["earnings"]]}
    return {"key": KEY, "trials": trials, "asof": str(date.today()), "stocks": len(names),
            "rule": summarize(rule, req), "rule_detail": describe(rule),
            "groups": {k: {h: edge(v, h) for h in (HOLD, *REPORTED_HOLDS)} for k, v in groups.items()},
            "events_per_year": {y: sum(r["date"].year == y for r in rule) for y in sorted({r["date"].year for r in rule})}}


def report_text(res: dict) -> str:
    p = lambda x: "—" if x is None else f"{x:+.2%}"  # noqa: E731

    def e(x: dict) -> str:
        hit = "—" if x["hit"] is None else f"{x['hit']:.0%}"
        return f"{p(x['edge'])}（t {x['t']}，{x['n']} 次，胜率 {hit}）"

    res = json.loads(json.dumps(res, default=str))        # one shape whether fresh or read back (string keys)
    s, d = res["rule"], res["rule_detail"]
    if not s.get("n"):
        return "好公司无财报大跌后的反弹：无样本"
    lines = [f"检验关（Sharadar 点时市值前 1000）：{res['key']} → G1 {'通过' if s['pass'] else '未通过'}",
             f"本族（短期反转）第 {res['trials']} 次检验，t 门槛 {s['required_t']}；{s['first_date']} 至 {s['last_date']}，按 {s['split']} 分前后两半",
             f"主结论（持有 21 日，对照 = 同窗口股票池其他股票平均）：全部 {e(s['all'])}；前半 {e(s['first'])}；后半 {e(s['second'])}",
             "仅报告：持有 5 日 " + e(d["holds"]["5"]) + "；持有 63 日 " + e(d["holds"]["63"]),
             f"仅报告：探索看过的 2004–2015 {e(d['explored_2004_2015'])}；没看过的 2016 起 {e(d['unseen_2016'])}；"
             f"去掉 2008–09 {e(d['ex_crisis'])}；熊市月份 {e(d['bear'])}；7 天内没有任何 8-K {e(d['strict_no_8k'])}",
             "对比组（21 日 / 63 日）：" + "；".join(
                 f"{name} {p(g['21']['edge'])}（t {g['21']['t']}，{g['21']['n']}）/ {p(g['63']['edge'])}（t {g['63']['t']}）" for name, g in (
                     ("所有冲击", res["groups"]["all_shocks"]), ("只加质量", res["groups"]["quality_only"]),
                     ("只加无财报", res["groups"]["no_earnings_only"]), ("有财报的冲击", res["groups"]["earnings_shocks"]),
                     ("好公司有财报", res["groups"]["quality_with_earnings"]))),
             "逐年（21 日）：" + "，".join(f"{y} {p(v['edge'])}（{v['n']}）" for y, v in d["years"].items())]
    return "\n".join(lines)
