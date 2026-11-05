"""Sector forecasts, pre-registered in `scout/knowledge/external_specs/sector_forecast_v1.yaml` (one run, local only).

F1 industry fundamental acceleration (Sharadar industries 1999–2026: member SUE median and share with revenue
acceleration, from filings strictly before the month end), F2 same-calendar-month seasonality and F3 own
12-month trend (French 49 industries 1926–2026). Each month-end signal is held for the next month. Cross-section
(F1, F2): top fifth minus the equal-weight average of all industries, Newey-West t. Time series (F3): next-month
excess return when the trend is positive vs negative, month-clustered t, and an in-or-cash portfolio.
Probabilities: expanding-window logistic regressions refit every January, scored out of sample against the
historical base rate. SPDR sector ETFs: all three signals, descriptive only.
"""
from __future__ import annotations

import math
from pathlib import Path

import numpy as np
import pandas as pd

from scout import cross_asset as ca
from scout import risk_warning as rw

KEY = "sector_forecast_v1"
FAMILY = "sector_rotation"
PARQUET = Path("exports/sharadar/parquet")
FRENCH49 = ca.FRENCH / "49_Industry_Portfolios.csv"
SPDR = ("XLB", "XLE", "XLF", "XLI", "XLK", "XLP", "XLU", "XLV", "XLY", "XLRE", "XLC")
SHARADAR_START = pd.Period("1999-01", "M")
TOP_N, MIN_PRICE, MIN_MEMBERS = 1000, 5.0, 5
FILED_DAYS = 135
SUE_LAGS, SUE_MIN, SUE_CLIP = 8, 6, 5.0
SEASON_YEARS, SEASON_MIN = 20, 5
TREND_MONTHS = 12
Z_VOL_MONTHS, Z_VOL_MIN = 36, 24
TOP_SHARE = 0.2
MIN_INDUSTRIES = 10
COST = 0.0005
NW_LAG = {1: 2, 3: 4}
T_BAR = 3.62
CAL_SLOPE = (0.5, 1.5)
PROB_MIN_YEARS = 10
POST = pd.Period("2000-01", "M")
NAMES = {"F1": "行业基本面加速", "F2": "同月季节性", "F3": "自身 12 个月趋势"}


# ── data ──

def french49() -> pd.DataFrame:
    """Value-weighted monthly returns (decimals), PeriodIndex, −99.99 as missing."""
    lines = FRENCH49.read_text().splitlines()
    start = next(i for i, l in enumerate(lines) if "Average Value Weighted Returns -- Monthly" in l) + 1
    head = [x.strip() for x in lines[start].split(",")][1:]
    rows = []
    for l in lines[start + 1:]:
        parts = [x.strip() for x in l.split(",")]
        if not parts[0].isdigit():
            break
        rows.append(parts)
    d = pd.DataFrame([r[1:] for r in rows], index=pd.PeriodIndex([r[0] for r in rows], freq="M"), columns=head).astype(float)
    return d.where(d > -99) / 100


def sharadar_monthly() -> dict:
    """Month-end panels for the top 1000 (price ≥ 5): next-month returns, caps, industry and SPDR sector labels."""
    tick = pd.read_parquet(PARQUET / "tickers.parquet", columns=["table", "ticker", "sector", "industry"])
    tick = tick[tick["table"] == "SEP"].drop_duplicates("ticker", keep="last").set_index("ticker")
    px = pd.read_parquet(PARQUET / "stocks.parquet", columns=["ticker", "date", "closeadj", "closeunadj"],
                         filters=[("date", ">=", pd.Timestamp("1998-06-01").date())])
    px["date"] = pd.to_datetime(px["date"])
    px["m"] = px["date"].dt.to_period("M")
    last = px.sort_values(["ticker", "date"]).groupby(["ticker", "m"]).tail(1)
    spy = ca.load(("SPY",))[0]
    month_end = pd.Series(spy.index, index=spy.index.to_period("M")).groupby(level=0).max()
    last = last[last["date"] == last["m"].map(month_end)]                  # priced on the month's last session
    cap = pd.read_parquet(PARQUET / "daily.parquet", columns=["ticker", "date", "marketcap"],
                          filters=[("date", "in", [d.date() for d in month_end])])
    cap["date"] = pd.to_datetime(cap["date"])
    last = last.merge(cap, on=["ticker", "date"], how="inner")
    close = last.pivot(index="m", columns="ticker", values="closeadj").sort_index()
    nxt = close.shift(-1) / close - 1
    # a member that stops trading inside the next month: return to its last close there (delisting proxy)
    last_any = px.sort_values(["ticker", "date"]).groupby(["ticker", "m"])["closeadj"].last().unstack("ticker").reindex(close.index)
    fallback = last_any.shift(-1) / close - 1
    nxt = nxt.fillna(fallback)
    elig = last[(last["closeunadj"] >= MIN_PRICE) & last["marketcap"].notna()].copy()
    elig["rank"] = elig.groupby("m")["marketcap"].rank(ascending=False, method="first")
    elig = elig[elig["rank"] <= TOP_N]
    elig["industry"] = elig["ticker"].map(tick["industry"])
    elig["sector"] = elig["ticker"].map(tick["sector"]).map(rw.SECTOR_OF)
    nx = nxt.stack().rename("ret_next").reset_index()
    elig = elig.merge(nx, on=["m", "ticker"], how="left")
    return {"members": elig[["m", "ticker", "date", "marketcap", "industry", "sector", "ret_next"]], "month_end": month_end}


def group_returns(members: pd.DataFrame, by: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """(next-month cap-weighted return, member count) per month × group, groups with ≥ MIN_MEMBERS."""
    m = members.dropna(subset=[by])
    m = m.assign(w=m["marketcap"], wr=m["marketcap"] * m["ret_next"].fillna(0.0))
    g = m.groupby(["m", by]).agg(w=("w", "sum"), wr=("wr", "sum"), n=("ticker", "size"))
    ret = (g["wr"] / g["w"]).unstack(by)
    n = g["n"].unstack(by)
    return ret.where(n >= MIN_MEMBERS), n


def fundamentals_quarterly() -> pd.DataFrame:
    """Per ticker and quarter (as first reported): filing date, SUE and revenue acceleration flag."""
    f = pd.read_parquet(PARQUET / "fundamentals.parquet", columns=["ticker", "dimension", "calendardate", "date", "eps", "revenue"],
                        filters=[("dimension", "==", "ARQ")])
    f["calendardate"], f["date"] = pd.to_datetime(f["calendardate"]), pd.to_datetime(f["date"])
    f = f.sort_values(["ticker", "calendardate", "date"]).drop_duplicates(["ticker", "calendardate"], keep="first")
    return pd.concat([quarter_signals(g) for _, g in f.groupby("ticker", sort=False)], ignore_index=True)


def quarter_signals(q: pd.DataFrame) -> pd.DataFrame:
    """SUE (seasonal random walk, scaled by the std of the preceding 8 differences, ≥ 6) and TTM revenue
    growth acceleration, for one ticker's consecutive quarters."""
    q = q.sort_values("calendardate").reset_index(drop=True)
    cd = q["calendardate"]
    consecutive = (cd.diff().dt.days.between(80, 100))
    year_ago = (cd - cd.shift(4)).dt.days.between(355, 375)
    diff = (q["eps"] - q["eps"].shift(4)).where(year_ago)
    sd = diff.shift(1).rolling(SUE_LAGS, min_periods=SUE_MIN).std()
    sue = (diff / sd).where(sd > 0).clip(-SUE_CLIP, SUE_CLIP)
    run4 = consecutive.rolling(3).sum() == 3                       # this and the three before are consecutive
    ttm = q["revenue"].rolling(4).sum().where(run4)
    growth = (ttm / ttm.shift(4) - 1).where((ttm.shift(4) > 0) & year_ago)
    accel = (growth > growth.shift(1)).where(growth.notna() & growth.shift(1).notna() & consecutive)
    return pd.DataFrame({"ticker": q["ticker"], "filed": q["date"], "sue": sue, "accel": accel})


def f1_signal(members: pd.DataFrame, quarters: pd.DataFrame, month_end: pd.Series, by: str) -> pd.DataFrame:
    """Month × group signal: mean of the cross-group ranks of median SUE and share accelerating (≥ 5 members with both)."""
    m = members.dropna(subset=[by]).copy()
    m["asof"] = m["m"].map(month_end).astype("datetime64[ns]")
    q = quarters.dropna(subset=["filed"]).sort_values("filed")
    q = q.assign(filed=q["filed"].astype("datetime64[ns]"))
    m = m.sort_values("asof")
    j = pd.merge_asof(m, q.rename(columns={"filed": "filed_at"}), left_on="asof", right_on="filed_at", by="ticker",
                      direction="backward", allow_exact_matches=False)
    fresh = (j["asof"] - j["filed_at"]).dt.days <= FILED_DAYS
    j = j[fresh & j["sue"].notna() & j["accel"].notna()]
    g = j.groupby(["m", by]).agg(sue=("sue", "median"), acc=("accel", "mean"), n=("ticker", "size"))
    g = g[g["n"] >= MIN_MEMBERS]
    sue, acc = g["sue"].unstack(by), g["acc"].unstack(by)
    return (sue.rank(axis=1, pct=True) + acc.rank(axis=1, pct=True)) / 2


# ── signals on return panels (row m uses returns up to and including month m) ──

def f2_signal(ret: pd.DataFrame) -> pd.DataFrame:
    """At month m: mean return of month m+1's calendar month over the previous 20 years (lags 12 … 240 from m+1)."""
    lags = [ret.shift(12 * k - 1) for k in range(1, SEASON_YEARS + 1)]     # shift(11) at row m = month m−11 = m+1−12
    stack = np.stack([x.to_numpy() for x in lags])
    n = np.sum(~np.isnan(stack), axis=0)
    mean = np.where(n >= SEASON_MIN, np.nanmean(np.where(np.isnan(stack), np.nan, stack), axis=0), np.nan)
    return pd.DataFrame(mean, index=ret.index, columns=ret.columns)


def excess_cum(ret: pd.DataFrame, rf: pd.Series, months: int) -> pd.DataFrame:
    gross = np.log1p(ret).rolling(months, min_periods=months).sum()
    return np.expm1(gross.sub(np.log1p(rf).rolling(months, min_periods=months).sum(), axis=0))


def f3_z(ret: pd.DataFrame, rf: pd.Series) -> pd.DataFrame:
    """12-month log excess return over (36-month std of monthly log excess × √12)."""
    ex = np.log1p(ret).sub(np.log1p(rf), axis=0)
    s12 = ex.rolling(TREND_MONTHS, min_periods=TREND_MONTHS).sum()
    vol = ex.rolling(Z_VOL_MONTHS, min_periods=Z_VOL_MIN).std()
    return s12 / (vol * math.sqrt(TREND_MONTHS))


# ── statistics ──

def nw_t(x: pd.Series, lags: int) -> float | None:
    x = x.dropna().to_numpy()
    n = len(x)
    if n < 24:
        return None
    u = x - x.mean()
    v = u @ u / n
    for L in range(1, lags + 1):
        v += 2 * (1 - L / (lags + 1)) * (u[L:] @ u[:-L]) / n
    return float(x.mean() / math.sqrt(v / n)) if v > 0 else None


def forward(ret: pd.DataFrame, h: int) -> pd.DataFrame:
    """Return over months m+1 … m+h at row m (ret row m = return of month m+1)."""
    if h == 1:
        return ret
    return np.expm1(sum(np.log1p(ret.shift(-k)) for k in range(h)))


def cross_section(signal: pd.DataFrame, ret_next: pd.DataFrame, h: int = 1) -> dict:
    """Top fifth (equal weight) minus all with a signal; costs on the top book's turnover; IC; bottom fifth."""
    fr = forward(ret_next, h)
    rows, prev = [], None
    for m in signal.index:
        s = signal.loc[m]
        r = fr.loc[m] if m in fr.index else None
        if r is None:
            continue
        ok = s.notna() & r.notna()
        if ok.sum() < MIN_INDUSTRIES:
            continue
        s, r = s[ok], r[ok]
        k = max(1, int(math.ceil(len(s) * TOP_SHARE)))
        top = s.sort_values(ascending=False).index[:k]
        bot = s.sort_values().index[:k]
        w = pd.Series(1 / k, index=top)
        turn = 2.0 if prev is None else float(w.sub(prev, fill_value=0).abs().sum())
        prev = w
        cost = COST * turn / h                  # overlapping h-month books trade 1/h of the turnover each month
        rows.append({"m": m, "top": float(r[top].mean() - r.mean()) - cost, "bottom": float(r[bot].mean() - r.mean()),
                     "ic": float(s.rank().corr(r.rank())), "n": int(len(s))})
    d = pd.DataFrame(rows).set_index("m")
    return d


def summarize_cs(d: pd.DataFrame, h: int) -> dict:
    if d.empty:
        return {}
    mid = d.index[0] + (d.index[-1] - d.index[0]).n // 2
    def part(x: pd.DataFrame) -> dict:
        return {"months": int(len(x)), "top": float(x["top"].mean()), "t": nw_t(x["top"], NW_LAG[h]),
                "bottom": float(x["bottom"].mean()), "ic": float(x["ic"].mean()), "n_mean": float(x["n"].mean())}
    out = {"all": part(d), "first": part(d[d.index < mid]), "second": part(d[d.index >= mid]), "mid": str(mid)}
    if d.index[0] < POST:
        out["post2000"] = part(d[d.index >= POST])
        out["decades"] = {f"{y}s": float(x["top"].mean()) for y, x in d.groupby((d.index.year // 10) * 10)}
    return out


def cluster_diff(y: np.ndarray, w: np.ndarray, groups: np.ndarray) -> dict:
    X = np.column_stack([np.ones(len(y)), w.astype(float)])
    inv = np.linalg.inv(X.T @ X)
    b = inv @ X.T @ y
    u = y - X @ b
    _, g = np.unique(groups, return_inverse=True)
    s = np.zeros((g.max() + 1, 2))
    np.add.at(s, g, X * u[:, None])
    G = len(s)
    v = inv @ (s.T @ s) @ inv * G / (G - 1)
    return {"diff": float(b[1]), "t": float(b[1] / math.sqrt(v[1, 1])) if v[1, 1] > 0 else None, "months": G}


def time_series(trend: pd.DataFrame, ret_next: pd.DataFrame, rf_next: pd.Series, h: int = 1) -> pd.DataFrame:
    fr = forward(ret_next, h)
    rfh = np.expm1(sum(np.log1p(rf_next.shift(-k)) for k in range(h))) if h > 1 else rf_next
    ex = fr.sub(rfh, axis=0)
    p = pd.DataFrame({"pos": (trend > 0).stack(), "has": trend.notna().stack()})
    p["ex"] = ex.stack()
    return p[p["has"]].dropna(subset=["ex"])


def summarize_ts(p: pd.DataFrame) -> dict:
    if p.empty:
        return {}
    months = p.index.get_level_values(0)
    mid = months.min() + (months.max() - months.min()).n // 2

    def part(x: pd.DataFrame) -> dict:
        mm = x.index.get_level_values(0)
        r = cluster_diff(x["ex"].to_numpy(), x["pos"].to_numpy(), mm.astype(str).to_numpy())
        return {**r, "share_pos": float(x["pos"].mean()), "ex_pos": float(x.loc[x["pos"], "ex"].mean()),
                "ex_neg": float(x.loc[~x["pos"], "ex"].mean()), "obs": int(len(x))}
    out = {"all": part(p), "first": part(p[months < mid]), "second": part(p[months >= mid]), "mid": str(mid)}
    if months.min() < POST:
        out["post2000"] = part(p[months >= POST])
        out["decades"] = {f"{y}s": part(x)["diff"] for y, x in p.groupby((months.year // 10) * 10)}
    return out


def in_or_cash(trend: pd.DataFrame, ret_next: pd.DataFrame, rf_next: pd.Series) -> dict:
    """Each month: equal weight over groups with a trend reading; positive → the group, negative → cash. vs all held."""
    ok = trend.notna() & ret_next.notna()
    n = ok.sum(axis=1)
    held = ret_next.where(ok & (trend > 0))
    cash_w = (ok & (trend <= 0)).sum(axis=1)
    strat = (held.sum(axis=1) + cash_w * rf_next) / n
    hold = ret_next.where(ok).sum(axis=1) / n
    keep = n >= MIN_INDUSTRIES
    strat, hold, rf = strat[keep], hold[keep], rf_next[keep]
    mid = strat.index[0] + (strat.index[-1] - strat.index[0]).n // 2
    def st(r: pd.Series, f: pd.Series) -> dict:
        ex = r - f
        nav = (1 + r).cumprod()
        return {"cagr": float(nav.iloc[-1] ** (12 / len(r)) - 1), "sharpe": float(ex.mean() / ex.std() * math.sqrt(12)),
                "max_dd": float((nav / nav.cummax() - 1).min())}
    out = {}
    for name, r in (("in_or_cash", strat), ("hold", hold)):
        out[name] = {"all": st(r, rf), "first": st(r[r.index < mid], rf[rf.index < mid]), "second": st(r[r.index >= mid], rf[rf.index >= mid])}
    out["cash_share"] = float((cash_w[keep] / n[keep]).mean())
    return out


# ── probabilities ──

def logit_fit(X: np.ndarray, y: np.ndarray, iters: int = 50) -> np.ndarray:
    b = np.zeros(X.shape[1])
    for _ in range(iters):
        p = 1 / (1 + np.exp(-X @ b))
        W = p * (1 - p)
        H = X.T @ (X * W[:, None]) + 1e-6 * np.eye(X.shape[1])
        step = np.linalg.solve(H, X.T @ (y - p))
        b += step
        if np.abs(step).max() < 1e-8:
            break
    return b


def probability(obs: pd.DataFrame, features: list[str]) -> dict:
    """obs: rows with columns m (Period), y (0/1) and features. Refit each January on outcomes known by then
    (rows with m + horizon month < January, i.e. m ≤ previous November for 1-month outcomes); predict that year."""
    obs = obs.dropna(subset=features + ["y"]).sort_values("m")
    years = sorted(set(obs["m"].dt.year))
    first = obs["m"].min().year + PROB_MIN_YEARS
    preds = []
    for yr in years:
        if yr < first:
            continue
        cut = pd.Period(f"{yr - 1}-11", "M")
        train = obs[obs["m"] <= cut]
        test = obs[obs["m"].dt.year == yr]
        if test.empty or len(train) < 100:
            continue
        X = np.column_stack([np.ones(len(train)), train[features].to_numpy(float)])
        b = logit_fit(X, train["y"].to_numpy(float))
        Xt = np.column_stack([np.ones(len(test)), test[features].to_numpy(float)])
        p = 1 / (1 + np.exp(-Xt @ b))
        base = float(train["y"].mean())
        preds.append(pd.DataFrame({"m": test["m"], "y": test["y"], "p": p, "base": base}))
    if not preds:
        return {}
    d = pd.concat(preds)
    mid = d["m"].min() + (d["m"].max() - d["m"].min()).n // 2

    def score(x: pd.DataFrame) -> dict:
        eps = 1e-9
        ll = lambda q: float(-np.mean(x["y"] * np.log(q + eps) + (1 - x["y"]) * np.log(1 - q + eps)))
        return {"obs": int(len(x)), "logloss": ll(x["p"]), "logloss_base": ll(x["base"]),
                "brier": float(np.mean((x["p"] - x["y"]) ** 2)), "brier_base": float(np.mean((x["base"] - x["y"]) ** 2)),
                "hit": float(((x["p"] > 0.5) == (x["y"] > 0.5)).mean()), "p_sd": float(x["p"].std())}
    lp = np.log(d["p"] / (1 - d["p"]))
    slope = logit_fit(np.column_stack([np.ones(len(d)), lp.to_numpy()]), d["y"].to_numpy(float))[1]
    deciles = d.assign(bin=pd.qcut(d["p"], 10, labels=False, duplicates="drop")).groupby("bin").agg(pred=("p", "mean"), actual=("y", "mean"), n=("y", "size"))
    return {"all": score(d), "first": score(d[d["m"] < mid]), "second": score(d[d["m"] >= mid]), "mid": str(mid),
            "calibration_slope": float(slope), "deciles": deciles.round(4).to_dict(orient="records")}


def cs_obs(signal: pd.DataFrame, ret_next: pd.DataFrame) -> pd.DataFrame:
    ok = signal.notna() & ret_next.notna()
    avg = ret_next.where(ok).mean(axis=1)
    y = ret_next.gt(avg, axis=0).where(ok)
    rank = signal.rank(axis=1, pct=True)
    d = pd.DataFrame({"y": y.stack(), "rank": rank.where(ok).stack()}).dropna()
    d["m"] = d.index.get_level_values(0)
    return d.reset_index(drop=True)


def ts_obs(trend: pd.DataFrame, z: pd.DataFrame, ret_next: pd.DataFrame, rf_next: pd.Series) -> pd.DataFrame:
    ex = ret_next.sub(rf_next, axis=0)
    d = pd.DataFrame({"y": (ex > 0).where(ex.notna()).stack(), "sign": np.sign(trend).stack(), "z": z.stack()}).dropna()
    d["m"] = d.index.get_level_values(0)
    return d.reset_index(drop=True)


# ── decision ──

def decide(name: str, primary: dict, prob: dict) -> dict:
    a = primary
    stat = (a["all"].get("t") if name != "F3" else a["all"].get("t")) or 0
    eff = (lambda x: x.get("top")) if name != "F3" else (lambda x: x.get("diff"))
    checks = {"t": stat >= T_BAR, "halves_positive": all((eff(a[h]) or 0) > 0 for h in ("first", "second"))}
    if "post2000" in a and name in ("F2", "F3"):
        checks["post2000_positive"] = (eff(a["post2000"]) or 0) > 0
    checks["prob_beats_base_halves"] = bool(prob) and all(prob[h]["logloss"] < prob[h]["logloss_base"] for h in ("first", "second"))
    checks["calibration"] = bool(prob) and CAL_SLOPE[0] <= prob["calibration_slope"] <= CAL_SLOPE[1]
    return {"checks": checks, "pass": all(checks.values())}


# ── run ──

def run() -> dict:
    res: dict = {"key": KEY}
    fm = ca.french_monthly()
    f49 = french49()
    f49 = f49[f49.index <= fm.index[-1]]
    rf = fm["rf"].reindex(f49.index)
    # row m holds month m+1's return (the month the signal at m predicts)
    nxt49, rf_next49 = f49.shift(-1), rf.shift(-1)
    res["french_range"] = [str(f49.index[0]), str(f49.index[-1])]

    sh = sharadar_monthly()
    mem = sh["members"]
    mem = mem[mem["m"] >= SHARADAR_START - 1]
    ind_next, ind_n = group_returns(mem, "industry")
    sec_next, _ = group_returns(mem, "sector")
    quarters = fundamentals_quarterly()
    f1_ind = f1_signal(mem, quarters, sh["month_end"], "industry")
    f1_sec = f1_signal(mem, quarters, sh["month_end"], "sector")
    res["sharadar_range"] = [str(ind_next.index[0]), str(ind_next.index[-1])]
    res["sharadar_industries_per_month"] = float(ind_next.notna().sum(axis=1).mean())

    # past returns per month (row m = return of month m) for F2/F3 on Sharadar industries and SPDR
    ind_ret = ind_next.shift(1)
    etf = ca.load(SPDR)[0]
    me = etf.groupby(etf.index.to_period("M")).last()
    etf_ret = me / me.shift(1) - 1
    etf_next = etf_ret.shift(-1)
    rf_m = fm["rf"]

    out: dict = {}
    for h in (1, 3):
        out[h] = {
            "F1": summarize_cs(cross_section(f1_ind, ind_next, h), h),
            "F2": summarize_cs(cross_section(f2_signal(f49), nxt49, h), h),
            "F3": summarize_ts(time_series(excess_cum(f49, rf, TREND_MONTHS), nxt49, rf_next49, h)),
        }
    res["primary"], res["secondary_3m"] = out[1], out[3]
    res["f3_portfolio"] = in_or_cash(excess_cum(f49, rf, TREND_MONTHS), nxt49, rf_next49)

    prob = {"F1": probability(cs_obs(f1_ind, ind_next), ["rank"]),
            "F2": probability(cs_obs(f2_signal(f49), nxt49), ["rank"]),
            "F3": probability(ts_obs(excess_cum(f49, rf, TREND_MONTHS), f3_z(f49, rf), nxt49, rf_next49), ["sign", "z"])}
    res["probability"] = prob
    res["decision"] = {n: decide(n, out[1][n], prob[n]) for n in ("F1", "F2", "F3")}

    # descriptive layers
    rf_s = rf_m.reindex(ind_ret.index)
    res["descriptive"] = {
        "sharadar_industries": {"F2": summarize_cs(cross_section(f2_signal(ind_ret), ind_next), 1),
                                "F3": summarize_ts(time_series(excess_cum(ind_ret, rf_s, TREND_MONTHS), ind_next, rf_s.shift(-1)))},
        "spdr": {"F1": spdr_cs(f1_sec, etf_next),
                 "F2": spdr_cs(f2_signal(etf_ret), etf_next),
                 "F3": summarize_ts(time_series(excess_cum(etf_ret, rf_m.reindex(etf_ret.index), TREND_MONTHS), etf_next,
                                                rf_m.reindex(etf_ret.index).shift(-1))),
                 "F3_portfolio": in_or_cash_small(excess_cum(etf_ret, rf_m.reindex(etf_ret.index), TREND_MONTHS), etf_next,
                                                  rf_m.reindex(etf_ret.index).shift(-1))},
    }
    return res


def spdr_cs(signal: pd.DataFrame, nxt: pd.DataFrame) -> dict:
    """11 ETFs: top 3 minus all (descriptive; fewer than MIN_INDUSTRIES allowed here)."""
    cols = [c for c in SPDR if c in signal.columns and c in nxt.columns]
    s, r = signal.reindex(columns=cols), nxt.reindex(columns=cols)
    rows = []
    for m in s.index.intersection(r.index):
        ok = s.loc[m].notna() & r.loc[m].notna()
        if ok.sum() < 6:
            continue
        top = s.loc[m][ok].sort_values(ascending=False).index[:3]
        rows.append({"m": m, "top": float(r.loc[m][top].mean() - r.loc[m][ok].mean())})
    d = pd.DataFrame(rows).set_index("m")["top"] if rows else pd.Series(dtype=float)
    return {"months": int(len(d)), "top": float(d.mean()) if len(d) else None, "t": nw_t(d, 2)} if len(d) else {}


def in_or_cash_small(trend: pd.DataFrame, nxt: pd.DataFrame, rf_next: pd.Series) -> dict:
    ok = trend.notna() & nxt.notna()
    n = ok.sum(axis=1)
    keep = n >= 6
    strat = ((nxt.where(ok & (trend > 0)).sum(axis=1) + (ok & (trend <= 0)).sum(axis=1) * rf_next) / n)[keep]
    hold = (nxt.where(ok).sum(axis=1) / n)[keep]
    f = rf_next[keep]
    def st(r: pd.Series) -> dict:
        ex = r - f.reindex(r.index)
        nav = (1 + r).cumprod()
        return {"cagr": float(nav.iloc[-1] ** (12 / len(r)) - 1), "sharpe": float(ex.mean() / ex.std() * math.sqrt(12)),
                "max_dd": float((nav / nav.cummax() - 1).min())}
    return {"in_or_cash": st(strat.dropna()), "hold": st(hold.dropna())}


# ── report ──

def _p(x: float | None, d: int = 2) -> str:
    return "—" if x is None else f"{x * 100:+.{d}f}%"


def report_text(res: dict) -> str:
    L = [f"板块预测检验 {res['key']}：French {res['french_range']}，Sharadar {res['sharadar_range']}（每月约 {res['sharadar_industries_per_month']:.0f} 个行业）"]
    for tag, block in (("1 个月（主要）", res["primary"]), ("3 个月（次要）", res["secondary_3m"])):
        L.append(f"\n== {tag} ==")
        for n in ("F1", "F2"):
            a = block[n]
            if not a:
                continue
            L.append(f"{n} {NAMES[n]}：最强五分之一 − 平均 每月 {_p(a['all']['top'])}（t {a['all']['t'] or 0:.2f}），"
                     f"前 {_p(a['first']['top'])} / 后 {_p(a['second']['top'])}，最弱五分之一 {_p(a['all']['bottom'])}，IC {a['all']['ic']:+.3f}"
                     + (f"，2000 后 {_p(a['post2000']['top'])}（t {a['post2000']['t'] or 0:.2f}）" if "post2000" in a else ""))
            if "decades" in a:
                L.append("   各年代：" + " ".join(f"{k} {_p(v, 2)}" for k, v in a["decades"].items()))
        a = block["F3"]
        L.append(f"F3 {NAMES['F3']}：趋势为正 vs 为负 下月超额 {_p(a['all']['ex_pos'])} vs {_p(a['all']['ex_neg'])}，差 {_p(a['all']['diff'])}（t {a['all']['t'] or 0:.2f}），"
                 f"前 {_p(a['first']['diff'])} / 后 {_p(a['second']['diff'])}，2000 后 {_p(a['post2000']['diff'])}（t {a['post2000']['t'] or 0:.2f}）")
        L.append("   各年代：" + " ".join(f"{k} {_p(v, 2)}" for k, v in a["decades"].items()))
    fp = res["f3_portfolio"]
    L.append(f"\nF3 组合（French 49）：趋势为正持有、否则现金 年化 {_p(fp['in_or_cash']['all']['cagr'], 1)} 夏普 {fp['in_or_cash']['all']['sharpe']:.2f}"
             f"（前 {fp['in_or_cash']['first']['sharpe']:.2f} / 后 {fp['in_or_cash']['second']['sharpe']:.2f}）回撤 {_p(fp['in_or_cash']['all']['max_dd'], 1)}；"
             f"一直持有 {_p(fp['hold']['all']['cagr'], 1)} 夏普 {fp['hold']['all']['sharpe']:.2f}（前 {fp['hold']['first']['sharpe']:.2f} / 后 {fp['hold']['second']['sharpe']:.2f}）"
             f"回撤 {_p(fp['hold']['all']['max_dd'], 1)}；平均现金 {fp['cash_share'] * 100:.0f}%")
    L.append("\n== 概率（样本外） ==")
    for n, p in res["probability"].items():
        if not p:
            continue
        L.append(f"{n}：对数损失 {p['all']['logloss']:.4f} vs 基础比例 {p['all']['logloss_base']:.4f}（前 {p['first']['logloss']:.4f}/{p['first']['logloss_base']:.4f}，"
                 f"后 {p['second']['logloss']:.4f}/{p['second']['logloss_base']:.4f}），命中 {p['all']['hit'] * 100:.1f}%，校准斜率 {p['calibration_slope']:.2f}，概率离散度 {p['all']['p_sd']:.3f}")
        L.append("   十分组 预测→实际：" + " ".join(f"{x['pred'] * 100:.0f}→{x['actual'] * 100:.0f}" for x in p["deciles"]))
    L.append("\n== 判定 ==")
    for n, d in res["decision"].items():
        L.append(f"{n} {NAMES[n]}：{'通过' if d['pass'] else '不通过'} {d['checks']}")
    L.append("\n== 描述 ==")
    L.append(f"Sharadar 行业 F2：{res['descriptive']['sharadar_industries']['F2'].get('all')}")
    a = res['descriptive']['sharadar_industries']['F3'].get('all') or {}
    L.append(f"Sharadar 行业 F3：差 {_p(a.get('diff'))} t {a.get('t') or 0:.2f}")
    sp = res["descriptive"]["spdr"]
    for n in ("F1", "F2"):
        x = sp[n]
        if x:
            L.append(f"SPDR {n}：前 3 − 平均 每月 {_p(x['top'])}（t {x['t'] or 0:.2f}，{x['months']} 个月）")
    a = sp["F3"].get("all") or {}
    L.append(f"SPDR F3：差 {_p(a.get('diff'))} t {a.get('t') or 0:.2f}；组合 {sp['F3_portfolio']}")
    return "\n".join(L)
