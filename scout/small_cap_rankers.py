"""Five literature rankers on small caps (pre-registered in `scout/knowledge/external_specs/small_cap_rankers_v1.yaml`,
insider scope in `small_cap_rankers_v1_p_clarification.yaml`, spot check and outcome policy in
`small_cap_rankers_v1_run_note.yaml`).

Local only (Sharadar, git-ignored `exports/small_cap/`). On each rebalance day the pool is frozen from what is known
at that close: Sharadar market-cap ranks 1001–3000 (`sharadar_research.universe`), tradeable under the previous month's
cost estimate (`small_cap_costs_v1`). Picks come from the frozen pool and scores; a pick or pool stock with no open the
next session is "not filled" and left out (no substitute). Each pick earns its next-open → close `hold` sessions later
return (`sharadar_research.holding_return`) minus 2 × its one-way cost; the control is the mean gross return of the
other filled pool stocks (other picks included). Edges are averaged per rebalance day, then per month.
"""
from __future__ import annotations

import json
import math
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

from scout import cost_model as cm
from scout import gate, osap_check
from scout import sharadar_research as sr

KEY = "small_cap_rankers_v1"
OUT = Path("exports/small_cap")
COSTS = Path("exports/costs/small_cap_costs_v1.parquet")
FIRST = "1999-03-01"
HOLD = 63
NW_LAGS = 2
NW_BAR = 2.0
MOM_LONG, MOM_SKIP = 252, 21
INSIDER_DAYS = 90
YEAR_DAYS = (330, 400)
FUND_STALE_DAYS = 400
EAR_CODE = "22"                       # Sharadar event code for 8-K item 2.02, results of operations
COMMON_CODES = frozenset({"Com", "ClA", "ClB", "ClC", "ClI", "ClK"})
METHODS = {   # name: (every, fraction or None = all qualifying, lowest first, denominator, prior trials in the family)
    "ear_drift": (5, 0.20, False, "announcers", 2),
    "gross_profitability": (21, 0.10, False, "pool", 2),
    "net_buyback": (21, 0.10, True, "pool", 2),
    "insider_cluster": (21, None, False, "pool", 2),
    "momentum_12_1": (21, 0.10, False, "pool", 0),
}
ZH = {"ear_drift": "财报公告漂移", "gross_profitability": "毛利 / 总资产", "net_buyback": "净回购", "insider_cluster": "内部人集中买入",
      "momentum_12_1": "12−1 动量"}
COST_VARIANTS = ("main", "spread2", "impact2", "gross")


# ── costs ──

def cost_table(path: Path = COSTS) -> dict[tuple[str, pd.Period], dict]:
    """(ticker, trade month) → one-way costs (main, spread × 2, impact × 2) and tradeability, from the month before."""
    c = pd.read_parquet(path, columns=["ticker", "spread", "sigma", "adv", "one_way", "tradeable", "applies_to"])
    c = c[c["tradeable"].astype(bool) & np.isfinite(c["one_way"])]
    impact = cm.IMPACT_Y * c["sigma"] * np.sqrt(cm.ORDER_USD / c["adv"])
    out = {}
    for t, m, sp, im, ow in zip(c["ticker"], c["applies_to"], c["spread"], impact, c["one_way"]):
        out[(t, pd.Period(m, "M"))] = {"main": ow, "spread2": sp + im, "impact2": sp / 2 + 2 * im, "gross": 0.0}
    return out


# ── signals (each uses data known before the rebalance day's close) ──

def momentum(p: sr.Prices, i: int, cols: list[int]) -> dict[int, float]:
    if i < MOM_LONG:
        return {}
    a, b = p.closeadj[i - MOM_LONG], p.closeadj[i - MOM_SKIP]
    return {j: float(b[j] / a[j] - 1) for j in cols if np.isfinite(a[j]) and np.isfinite(b[j]) and a[j] > 0}


def fundamentals_signals(names: list[str]) -> dict[str, pd.DataFrame]:
    """Per ticker, the first-filed versions sorted by filing date: gross profitability (ART) and the year change of basic
    shares (ARQ)."""
    f = sr._read("fundamentals", ["ticker", "dimension", "date", "reportperiod", "gp", "assets", "sharesbas"],
                 filters=[("dimension", "in", ["ARQ", "ART"]), ("ticker", "in", names)])
    f = f.rename(columns={"date": "datekey"})
    f["datekey"], f["reportperiod"] = pd.to_datetime(f["datekey"]), pd.to_datetime(f["reportperiod"])
    f = sr.first_filed(f, ("ticker", "dimension", "reportperiod")).sort_values(["ticker", "dimension", "reportperiod"])
    art = f[f["dimension"] == "ART"].copy()
    art["value"] = art["gp"] / art["assets"].where(art["assets"] > 0)
    arq = f[f["dimension"] == "ARQ"].copy()
    g = arq.groupby("ticker")
    prev, prev_p = g["sharesbas"].shift(4), g["reportperiod"].shift(4)
    year = (arq["reportperiod"] - prev_p).dt.days.between(*YEAR_DAYS)
    arq["value"] = (arq["sharesbas"] / prev.where(prev > 0) - 1).where(year)
    return {"gross_profitability": art[["ticker", "datekey", "value"]], "net_buyback": arq[["ticker", "datekey", "value"]]}


def asof_values(table: pd.DataFrame, day: pd.Timestamp) -> dict[str, float]:
    """Latest value per ticker filed before `day` (same-day filings are not used: no filing time), at most 400 days old."""
    t = table[(table["datekey"] < day) & (table["datekey"] >= day - pd.Timedelta(days=FUND_STALE_DAYS))]
    t = t.sort_values("datekey").drop_duplicates("ticker", keep="last")
    return {k: float(v) for k, v in zip(t["ticker"], t["value"]) if np.isfinite(v)}


def ear_events(p: sr.Prices, spy: np.ndarray, names: list[str]) -> list[tuple[int, int, float]]:
    """(stock column, index of the session the return is known, EAR): close before the announcement day → close after
    it, minus SPY over the same sessions."""
    ev = sr._read("events", ["ticker", "date", "eventcodes"], filters=[("ticker", "in", names)])
    ev = ev[ev["eventcodes"].fillna("").str.split("|").apply(lambda x: EAR_CODE in x)]
    out = []
    for t, e in zip(ev["ticker"], pd.to_datetime(ev["date"])):
        j = p.col.get(t)
        k = int(np.searchsorted(p.dates, e))
        if j is None or k < 1 or k + 1 >= len(p.dates):
            continue
        a, b = p.closeadj[k - 1, j], p.closeadj[k + 1, j]
        if np.isfinite(a) and np.isfinite(b) and a > 0 and spy[k - 1] > 0:
            out.append((j, k + 1, float(b / a - 1 - (spy[k + 1] / spy[k - 1] - 1))))
    return out


def ordinary_common(value) -> bool:
    if pd.isna(value):
        return False
    parts = frozenset(x.strip() for x in str(value).split(";") if x.strip())
    return "Com" in parts and parts <= COMMON_CODES


def insider_purchases(names: list[str]) -> pd.DataFrame:
    """Qualifying rows per the p_clarification: Form 4 / 4/A, code P (open market or private), officer or director,
    unambiguous ordinary common, positive shares, a transaction date not after the filing date; owner = normalised name."""
    f = sr._read("insiders", ["ticker", "date", "formtype", "ownername", "isdirector", "isofficer", "transactiondate",
                              "transactioncode", "transactionshares", "securitytitle"],
                 filters=[("ticker", "in", names), ("transactioncode", "=", "P")])
    f["date"], f["transactiondate"] = pd.to_datetime(f["date"], errors="coerce"), pd.to_datetime(f["transactiondate"], errors="coerce")
    f["owner"] = f["ownername"].fillna("").astype(str).str.split().str.join(" ").str.casefold()
    ok = (f["formtype"].isin(["4", "4/A"]) & (f["isofficer"].eq("Y") | f["isdirector"].eq("Y"))
          & f["securitytitle"].map(ordinary_common) & f["owner"].ne("") & (f["transactionshares"] > 0)
          & f["transactiondate"].notna() & (f["transactiondate"] <= f["date"]) & f["date"].notna())
    return f.loc[ok, ["ticker", "date", "owner"]]


def insider_clusters(ins: pd.DataFrame, day: pd.Timestamp) -> set[str]:
    """Tickers with ≥ 2 distinct qualifying insiders filed in [day − 90 days, day − 1 day]."""
    w = ins[(ins["date"] >= day - pd.Timedelta(days=INSIDER_DAYS)) & (ins["date"] < day)]
    n = w.groupby("ticker")["owner"].nunique()
    return set(n[n >= 2].index)


# ── one formation ──

def freeze(pool: list[int], scores: dict[int, float], frac: float | None, lowest: bool, denominator: str) -> list[int]:
    """Picks from the frozen pool and scores only (ties by column for a stable order)."""
    scored = [j for j in pool if j in scores and np.isfinite(scores[j])]
    if frac is None:
        return sorted(scored)
    n = len(pool) if denominator == "pool" else len(scored)
    k = max(1, int(round(n * frac))) if scored else 0
    return sorted(scored, key=lambda j: (scores[j] if lowest else -scores[j], j))[:k]


def formation_rows(p: sr.Prices, i: int, pool: list[int], picks: list[int], costs: dict[int, dict], half: dict[int, int]) -> tuple[list[dict], int]:
    """Rows per filled pick and the number of picks that could not be filled."""
    rets = {}
    for j in pool:
        r = sr.holding_return(p, j, i, HOLD)
        if r is not None:
            rets[j] = r
    if len(rets) < 2:
        return [], sum(j not in rets for j in picks)
    total, n = sum(rets.values()), len(rets)
    rows = []
    for j in picks:
        if j not in rets:
            continue
        r = rets[j]
        ctrl = (total - r) / (n - 1)
        rows.append({"date": p.dates[i], "col": j, "gross": r, "control": ctrl, "half": half.get(j),
                     **{f"edge_{v}": r - 2 * costs[j][v] - ctrl for v in COST_VARIANTS}, "round_trip": 2 * costs[j]["main"]})
    return rows, sum(j not in rets for j in picks)


# ── statistics ──

def monthly(rows: pd.DataFrame, col: str) -> pd.Series:
    """Mean per rebalance day, then per month."""
    if rows.empty:
        return pd.Series(dtype=float)
    daily = rows.groupby("date")[col].mean()
    return daily.groupby(daily.index.to_period("M")).mean()


def tstat(m: pd.Series) -> float | None:
    if len(m) < 3 or m.std(ddof=1) == 0:
        return None
    return float(m.mean() / (m.std(ddof=1) / math.sqrt(len(m))))


def nw_t(m: pd.Series, lags: int = NW_LAGS) -> float | None:
    """t of the mean with a Newey-West (Bartlett) variance."""
    x = m.to_numpy(dtype=float)
    n = len(x)
    if n < lags + 3:
        return None
    e = x - x.mean()
    var = e @ e / n
    for k in range(1, lags + 1):
        var += 2 * (1 - k / (lags + 1)) * (e[k:] @ e[:-k]) / n
    return float(x.mean() / math.sqrt(var / n)) if var > 0 else None


def edge_stats(rows: pd.DataFrame, col: str = "edge_main") -> dict:
    m = monthly(rows, col)
    return {"mean": float(m.mean()) if len(m) else None, "t": tstat(m), "months": int(len(m)), "n": int(len(rows))}


def summarize(rows: pd.DataFrame, req: float, not_filled: int) -> dict:
    if rows.empty:
        return {"n": 0, "pass": False, "required_t": req}
    days = np.sort(rows["date"].unique())
    mid = pd.Timestamp(days[len(days) // 2])
    first, second = rows[rows["date"] < mid], rows[rows["date"] >= mid]
    m = monthly(rows, "edge_main")
    bears = [r for r in osap_check.BEARS]
    in_bear = rows["date"].apply(lambda d: any(pd.Period(a, "M") <= d.to_period("M") <= pd.Period(b, "M") for a, b in bears))
    ex_gfc = ~rows["date"].between("2008-01-01", "2009-12-31")
    out = {
        "n": int(len(rows)), "formations": int(len(days)), "first_date": str(pd.Timestamp(days[0]).date()),
        "last_date": str(pd.Timestamp(days[-1]).date()), "split": str(mid.date()), "required_t": req,
        "all": {**edge_stats(rows), "nw_t": nw_t(m)}, "first": edge_stats(first), "second": edge_stats(second),
        "variants": {v: edge_stats(rows, f"edge_{v}") for v in COST_VARIANTS},
        "picks_per_formation": float(rows.groupby("date").size().mean()), "round_trip": float(rows["round_trip"].mean()),
        "not_filled": int(not_filled), "not_filled_share": not_filled / (not_filled + len(rows)),
        "ranks_1001_2000": edge_stats(rows[rows["half"] == 1]), "ranks_2001_3000": edge_stats(rows[rows["half"] == 2]),
        "since_2016": edge_stats(rows[rows["date"] >= "2016-01-01"]), "since_2022": edge_stats(rows[rows["date"] >= "2022-01-01"]),
        "ex_2008_09": edge_stats(rows[ex_gfc]), "bear_months": edge_stats(rows[in_bear]),
        "years": {int(y): float(v) for y, v in m.groupby(m.index.year).mean().items()},
    }
    a = out["all"]
    checks = {"t": bool(a["t"] is not None and a["t"] >= req),
              "halves": bool((out["first"]["mean"] or 0) > 0 and (out["second"]["mean"] or 0) > 0),
              "nw_t": bool(a["nw_t"] is not None and a["nw_t"] >= NW_BAR),
              "conservative_cost": bool((out["variants"]["spread2"]["mean"] or 0) > 0)}
    out["checks"], out["pass"] = checks, all(checks.values())
    return out


# ── run ──

def rebalance_days(dates: pd.DatetimeIndex, every: int) -> list[int]:
    start = int(np.searchsorted(dates, pd.Timestamp(FIRST)))
    return list(range(start, len(dates) - HOLD - 1, every))


def run() -> dict:
    dates = sr.trading_days()
    elig = sr.eligible()
    days = {m: rebalance_days(dates, spec[0]) for m, spec in METHODS.items()}
    all_days = sorted({i for v in days.values() for i in v})
    uni = sr.universe(dates[all_days], elig)
    names = sorted({t for v in uni.values() for t in v["large"] + v["small"]})
    p = sr.prices(names, dates)
    spy = sr._read("funds", ["ticker", "date", "closeadj"], filters=[("ticker", "=", "SPY")])
    spy = spy.assign(date=pd.to_datetime(spy["date"])).set_index("date")["closeadj"].reindex(dates).to_numpy(dtype=float)
    costs = cost_table()
    fund = fundamentals_signals(names)
    ins = insider_purchases(names)
    ear_by_known: dict[int, list[tuple[int, float]]] = {}
    for j, k, v in ear_events(p, spy, names):
        ear_by_known.setdefault(k, []).append((j, v))

    def scores(method: str, i: int, cols: list[int]) -> dict[int, float]:
        d = dates[i]
        if method == "momentum_12_1":
            return momentum(p, i, cols)
        if method in fund:
            vals = asof_values(fund[method], d)
            return {j: vals[p.tickers[j]] for j in cols if p.tickers[j] in vals}
        if method == "insider_cluster":
            hit = insider_clusters(ins, d)
            return {j: 1.0 for j in cols if p.tickers[j] in hit}
        known = {}
        for k in range(i - METHODS["ear_drift"][0] + 1, i + 1):     # became known in the last 5 sessions, latest wins
            for j, v in ear_by_known.get(k, []):
                known[j] = v
        return {j: known[j] for j in cols if j in known}

    out = {"key": KEY, "asof": str(date.today()), "stocks": len(names), "methods": {}}
    for method, (every, frac, lowest, denom, prior) in METHODS.items():
        req = gate.required_t(prior + 1)
        res = {}
        for part in ("small", "large"):
            rows, nf = [], 0
            for i in days[method]:
                d = dates[i]
                members = uni.get(d, {}).get(part, [])
                month = d.to_period("M")
                c = {p.col[t]: costs[(t, month)] for t in members if t in p.col and (t, month) in costs}
                pool = sorted(c)
                if len(pool) < 2:
                    continue
                rank = {p.col[t]: k for k, t in enumerate(members) if t in p.col}
                half = {j: 1 if rank[j] < len(members) / 2 else 2 for j in pool}
                picks = freeze(pool, scores(method, i, pool), frac, lowest, denom)
                got, missed = formation_rows(p, i, pool, picks, c, half)
                rows += got
                nf += missed
            res[part] = summarize(pd.DataFrame(rows), req, nf)
        out["methods"][method] = res
    return out


def save(res: dict) -> Path:
    OUT.mkdir(parents=True, exist_ok=True)
    path = OUT / "result.json"
    if path.exists():
        raise FileExistsError(f"{path} exists: the pre-registered test runs once")
    path.write_text(json.dumps(res, ensure_ascii=False, indent=1, default=str))
    return path


def _p(x, d: int = 2) -> str:
    return "—" if x is None else f"{x * 100:+.{d}f}%"


def _t(x) -> str:
    return "—" if x is None else f"{x:.2f}"


def report_text(res: dict) -> str:
    lines = [f"小盘五项选股（预登记 {KEY}，只跑一次）：Sharadar 市值第 1001–3000 名、按上月成本可交易，扣往返成本后相对同期其他小盘股（每月超额）",
             "成本模型 small_cap_costs_v1，未经实时报价验证（用户 2026-11-03 决定跳过抽查）", ""]
    for method, r in res["methods"].items():
        s, lg = r["small"], r["large"]
        if not s.get("n"):
            lines.append(f"{ZH[method]}：无样本")
            continue
        a, v = s["all"], s["variants"]
        lines.append(f"{ZH[method]}：{s['first_date']} 至 {s['last_date']}，{s['formations']} 次调仓、{s['n']} 笔（每次约 {s['picks_per_formation']:.0f} 只），"
                     f"平均往返成本 {_p(s['round_trip'])}")
        lines.append(f"  扣成本后每月 {_p(a['mean'])}（t {_t(a['t'])}，Newey-West t {_t(a['nw_t'])}）；前半 {_p(s['first']['mean'])} / 后半 {_p(s['second']['mean'])}（按 {s['split']} 分）")
        lines.append(f"  不扣成本 {_p(v['gross']['mean'])}；价差 × 2 {_p(v['spread2']['mean'])}；冲击 × 2 {_p(v['impact2']['mean'])}")
        lines.append(f"  1001–2000 {_p(s['ranks_1001_2000']['mean'])}，2001–3000 {_p(s['ranks_2001_3000']['mean'])}；2016 年起 {_p(s['since_2016']['mean'])}（t {_t(s['since_2016']['t'])}），"
                     f"2022 年起 {_p(s['since_2022']['mean'])}；去掉 2008–09 {_p(s['ex_2008_09']['mean'])}；熊市月份 {_p(s['bear_months']['mean'])}")
        lines.append(f"  无法成交 {s['not_filled']} 笔（{s['not_filled_share'] * 100:.2f}%{'，⚠️ 超过 1%' if s['not_filled_share'] > 0.01 else ''}）")
        if lg.get("n"):
            lines.append(f"  对照 · 同样方法在市值前 1000：扣成本后每月 {_p(lg['all']['mean'])}（t {_t(lg['all']['t'])}）")
        c = s["checks"]
        lines.append(f"  判定：{'通过' if s['pass'] else '不通过'}（t ≥ {s['required_t']} {'✓' if c['t'] else '✗'}，两半为正 {'✓' if c['halves'] else '✗'}，"
                     f"NW t ≥ {NW_BAR} {'✓' if c['nw_t'] else '✗'}，价差 × 2 仍为正 {'✓' if c['conservative_cost'] else '✗'}）")
    return "\n".join(lines)
