"""Weekly "up calls" v2, pre-registered in `scout/knowledge/external_specs/weekly_up_v2.yaml` (one run, local only).

SPY and the SPDR sector ETFs, decided at each week's last close: say "up next week" only when the model's
probability is ≥ 0.62. About 70 features in seven families (vol-scaled multi-horizon trend, residual relative
strength, volatility, volume / flow proxies, member breadth, market-wide and cross-asset, calendar incl. turn of
month and FOMC weeks), all from data up to the decision close. Models refit every January with time-ordered
cross-validation over the last five training years; when nothing beats the training base rate the model abstains
for that year. M1 elastic-net logistic, M2 LightGBM, M3 their mean.
"""
from __future__ import annotations

import math
import re
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

from scout import cross_asset as ca
from scout import risk_warning as rw
from scout import weekly_direction as wd

KEY = "weekly_up_v2"
FAMILY = "short_term_reversal"
ASSETS = wd.ASSETS
SECTORS = tuple(a for a in ASSETS if a != "SPY")
PARQUET = Path("exports/sharadar/parquet")
THRESHOLD = 0.62
PRECISION_MIN = 0.60
GAP_MIN = 0.04
CALLS_MIN = 6
T_BAR = 3.64
NW_LAG = 4
CV_FOLDS = 5
C_GRID = (0.001, 0.003, 0.01, 0.03, 0.1, 0.3, 1.0)
LGB_GRID = [{"num_leaves": nl, "min_data_in_leaf": md} for nl in (7, 15) for md in (200, 500)]
LGB_BASE = {"objective": "binary", "learning_rate": 0.02, "feature_fraction": 0.7, "bagging_fraction": 0.7, "bagging_freq": 1,
            "lambda_l2": 10.0, "seed": 7, "verbose": -1, "deterministic": True, "force_row_wise": True}
LGB_MAX, LGB_PATIENCE = 1000, 100
COST = 0.0005
FED_CAL = "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"
FED_HIST = "https://www.federalreserve.gov/monetarypolicy/fomchistorical{}.htm"
MACRO_IDS = ("VIXCLS", "VXVCLS", "BAA10Y", "T10Y2Y", "DGS10", "DCOILWTICO", "DTWEXBGS", "DTWEXM")


# ── external data ──

async def load_macro() -> dict[str, pd.Series]:
    from scout.data import fred

    out = {}
    for sid in MACRO_IDS:
        rows = await fred.series(sid, date(1990, 1, 1), date(2026, 10, 28))
        out[sid] = pd.Series({pd.Timestamp(d): v for d, v in rows}, dtype=float).sort_index()
    return out


_HIST = re.compile(r"([A-Z][a-z]+) (\d+)(?:-(?:([A-Z][a-z]+) )?(\d+))? Meeting - (\d{4})")


def parse_fomc_history(html: str) -> list[date]:
    """Statement days (the meeting's last day) of scheduled meetings on a historical-year page."""
    from scout.event_calendar import _MONTHS

    out = []
    for m1, d1, m2, d2, y in _HIST.findall(html):
        month = _MONTHS.get(m2 or m1)
        day = int(d2 or d1)
        if month:
            out.append(date(int(y), month, day))
    return sorted(set(out))


async def load_fomc() -> tuple[set[date], dict[int, str]]:
    """FOMC statement days 2000 → now and a status per year ('ok' or why missing)."""
    import httpx

    from scout.event_calendar import parse_fomc

    days: set[date] = set()
    status: dict[int, str] = {}
    async with httpx.AsyncClient(timeout=30, headers={"User-Agent": "Mozilla/5.0"}, follow_redirects=True) as client:
        try:
            r = await client.get(FED_CAL)
            for m in parse_fomc(r.text):
                days.add(m["date"])
        except Exception as exc:
            status[0] = f"calendar page failed: {type(exc).__name__}"
        for y in range(2000, 2027):
            if any(d.year == y for d in days):
                status[y] = "ok"
                continue
            try:
                r = await client.get(FED_HIST.format(y))
                found = [d for d in parse_fomc_history(r.text) if d.year == y] if r.status_code == 200 else []
            except Exception:
                found = []
            days |= set(found)
            status[y] = "ok" if found else "missing"
    return days, status


def load_members() -> pd.DataFrame:
    """Daily member breadth per sector ETF and 'TOP500': shares above the 50- and 200-day averages and
    (20-day new highs − new lows) / members."""
    tick = pd.read_parquet(PARQUET / "tickers.parquet", columns=["table", "ticker", "sector"])
    tick = tick[tick["table"] == "SEP"].drop_duplicates("ticker", keep="last").set_index("ticker")
    px = pd.read_parquet(PARQUET / "stocks.parquet", columns=["ticker", "date", "closeadj", "closeunadj"],
                         filters=[("date", ">=", pd.Timestamp("1997-01-01").date())])
    px["date"] = pd.to_datetime(px["date"])
    px = px.sort_values(["ticker", "date"], kind="mergesort").reset_index(drop=True)
    g = px.groupby("ticker", sort=False)
    n = g.cumcount()
    cs = g["closeadj"].cumsum()
    shift = lambda k: cs.groupby(px["ticker"], sort=False).shift(k).fillna(0.0)
    sma50 = ((cs - shift(50)) / 50).where(n >= 49)
    sma200 = ((cs - shift(200)) / 200).where(n >= 199)
    prev = g["closeadj"].shift(1)                 # NaN on each ticker's first row, so no window crosses tickers
    hi20 = prev.rolling(20, min_periods=20).max().where(n >= 20)
    lo20 = prev.rolling(20, min_periods=20).min().where(n >= 20)
    px["a50"] = (px["closeadj"] > sma50).where(sma50.notna())
    px["a200"] = (px["closeadj"] > sma200).where(sma200.notna())
    px["hl"] = ((px["closeadj"] > hi20).astype(float) - (px["closeadj"] < lo20).astype(float)).where(hi20.notna())
    px = px.drop(columns=["closeadj"])
    cap = pd.read_parquet(PARQUET / "daily.parquet", columns=["ticker", "date", "marketcap"],
                          filters=[("date", ">=", pd.Timestamp("1998-01-01").date())])
    cap["date"] = pd.to_datetime(cap["date"])
    px = px.merge(cap, on=["ticker", "date"], how="inner")
    px = px[(px["closeunadj"] >= rw.MIN_PRICE) & px["marketcap"].notna() & px["a50"].notna()]
    px["rank"] = px.groupby("date")["marketcap"].rank(ascending=False, method="first")
    top = px[px["rank"] <= rw.TOP_N].copy()
    top["key"] = top["ticker"].map(tick["sector"]).map(rw.SECTOR_OF)
    frames = []
    sec = top[top["key"].notna()].groupby(["date", "key"])[["a50", "a200", "hl"]].mean()
    for col in ("a50", "a200", "hl"):
        frames.append(sec[col].unstack("key").add_suffix(f"|{col}"))
    m = top[top["rank"] <= rw.TOP_MARKET].groupby("date")[["a50", "a200", "hl"]].mean()
    frames.append(m.add_prefix("TOP500|").rename(columns=lambda c: f"TOP500|{c.split('|')[1]}"))
    return pd.concat(frames, axis=1).sort_index()


def load_prices() -> dict[str, pd.DataFrame]:
    """Daily OHLC (split-adjusted), closeadj and volume per asset, up to the last French RF day."""
    f = pd.read_parquet(wd.SHARADAR, columns=["ticker", "date", "open", "high", "low", "close", "closeadj", "volume"],
                        filters=[("ticker", "in", list(ASSETS))])
    f["date"] = pd.to_datetime(f["date"])
    end = ca.french_daily_rf().index[-1]
    return {t: g.set_index("date").sort_index().loc[:end] for t, g in f.groupby("ticker")}


# ── daily features ──

def _ann(x: pd.Series) -> pd.Series:
    return x * math.sqrt(252)


def efficiency(log_c: pd.Series, n: int, step: int) -> pd.Series:
    net = (log_c - log_c.shift(n)).abs()
    path = sum((log_c.shift(k) - log_c.shift(k + step)).abs() for k in range(0, n, step))
    return net / path.replace(0, np.nan)


def asset_features(g: pd.DataFrame, spy: pd.DataFrame | None) -> pd.DataFrame:
    """Families A–D for one asset (rows = its trading days; data up to each row)."""
    c = g["closeadj"]
    lc = np.log(c)
    lr = lc.diff()
    s63 = lr.rolling(63, min_periods=63).std()
    f = pd.DataFrame(index=c.index)
    # A trend, vol-scaled
    for n in (5, 10, 21, 63, 126, 252):
        f[f"z{n}"] = (lc - lc.shift(n)) / (s63 * math.sqrt(n))
    f["z12_1"] = (lc.shift(21) - lc.shift(252)) / (s63 * math.sqrt(231))
    for k in (20, 50, 100, 200):
        f[f"ma{k}"] = (c / c.rolling(k, min_periods=k).mean() - 1) / s63
    for k in (50, 200):
        ma = c.rolling(k, min_periods=k).mean()
        f[f"slope{k}"] = (ma / ma.shift(20) - 1) / s63
    hi, lo = c.rolling(252, min_periods=252).max(), c.rolling(252, min_periods=252).min()
    f["dd252"] = c / hi - 1
    f["dd252_z"] = f["dd252"] / s63
    f["up252_z"] = (c / lo - 1) / s63
    f["er10"] = efficiency(lc, 10, 1)
    f["er63"] = efficiency(lc, 63, 6)
    # B relative (sectors only)
    if spy is not None:
        ls = np.log(spy["closeadj"].reindex(c.index)).diff()
        cov = lr.rolling(252, min_periods=252).cov(ls)
        beta = cov / ls.rolling(252, min_periods=252).var()
        f["beta"] = beta
        f["corr63"] = lr.rolling(63, min_periods=63).corr(ls)
        resid = lr - beta.shift(1) * ls
        rs = resid.rolling(63, min_periods=63).std()
        for n in (5, 21, 63):
            f[f"res{n}"] = resid.rolling(n, min_periods=n).sum() / (rs * math.sqrt(n))
    # C volatility
    for n in (5, 21, 63, 252):
        f[f"vol{n}"] = _ann(lr.rolling(n, min_periods=n).std())
    f["vr5_63"] = f["vol5"] / f["vol63"]
    f["vr21_252"] = f["vol21"] / f["vol252"]
    adj = g["closeadj"] / g["close"]
    o, h, l, cl = g["open"] * adj, g["high"] * adj, g["low"] * adj, g["closeadj"]
    hl2 = np.log(h / l) ** 2
    f["park21"] = _ann(np.sqrt(hl2.rolling(21).mean() / (4 * math.log(2))))
    f["gk21"] = _ann(np.sqrt((0.5 * hl2 - (2 * math.log(2) - 1) * np.log(cl / o) ** 2).rolling(21).mean().clip(lower=0)))
    f["down63"] = _ann(np.sqrt((lr.clip(upper=0) ** 2).rolling(63, min_periods=63).mean()))
    f["volvol"] = f["vol21"].rolling(252, min_periods=126).std()
    f["skew63"] = lr.rolling(63, min_periods=63).skew()
    f["kurt63"] = lr.rolling(63, min_periods=63).kurt()
    f["maxdrop21"] = lr.rolling(21).min() / s63
    f["gap21"] = np.log(o / cl.shift(1)).abs().rolling(21).mean() / s63
    f["vol_pct"] = rw.trailing_share_le(f["vol21"], 756, 504)
    # D volume / flow
    rng = (g["high"] - g["low"]).replace(0, np.nan)
    mfv = (((g["close"] - g["low"]) - (g["high"] - g["close"])) / rng).fillna(0) * g["volume"]
    f["cmf"] = mfv.rolling(20).sum() / g["volume"].rolling(20).sum()
    up = c.diff() > 0
    f["udv"] = g["volume"].where(up, 0).rolling(20).sum() / g["volume"].where(c.diff() < 0, 0).rolling(20).sum().replace(0, np.nan)
    f["vsurge"] = g["volume"].rolling(5).mean() / g["volume"].rolling(60).mean()
    dv = g["close"] * g["volume"]
    f["dvtrend"] = dv.rolling(21).mean() / dv.rolling(126).mean()
    f["cmf_pct"] = rw.trailing_share_le(f["cmf"], 1260, 756)
    f["udv_pct"] = rw.trailing_share_le(f["udv"], 1260, 756)
    return f


def market_features(prices: dict[str, pd.DataFrame], members: pd.DataFrame, macro: dict[str, pd.Series]) -> pd.DataFrame:
    """Family F on SPY's days (macro values from the previous day)."""
    spy = prices["SPY"]["closeadj"]
    idx = spy.index
    lc = np.log(spy)
    s63 = lc.diff().rolling(63, min_periods=63).std()
    f = pd.DataFrame(index=idx)
    for n in (21, 63, 252):
        f[f"spy_z{n}"] = (lc - lc.shift(n)) / (s63 * math.sqrt(n))
    f["mkt_a200"] = members.get("TOP500|a200", pd.Series(dtype=float)).reindex(idx)
    sec = pd.DataFrame({a: prices[a]["closeadj"].reindex(idx) for a in SECTORS if a in prices})
    r21 = sec / sec.shift(21) - 1
    f["dispersion"] = r21.std(axis=1).where(r21.notna().sum(axis=1) >= 5)
    lr = np.log(sec).diff()
    corr = []
    for i in range(len(idx)):
        if i < 63:
            corr.append(np.nan)
            continue
        w = lr.iloc[i - 62: i + 1].dropna(axis=1, how="any")
        if w.shape[1] < 5:
            corr.append(np.nan)
            continue
        cm = np.corrcoef(w.to_numpy().T)
        corr.append(float(cm[np.triu_indices_from(cm, 1)].mean()))
    f["sector_corr"] = corr
    prev = lambda s: s.dropna().reindex(idx, method="ffill").shift(1)
    vix, vix3m = prev(macro["VIXCLS"]), prev(macro["VXVCLS"])
    f["vix"] = vix
    f["vix_z"] = (vix - vix.rolling(252, min_periods=252).mean()) / vix.rolling(252, min_periods=252).std()
    f["vix_term"] = vix / vix3m
    baa = macro["BAA10Y"].dropna()
    f["baa_z"] = prev((baa - baa.rolling(252, min_periods=252).mean()) / baa.rolling(252, min_periods=252).std())
    curve = prev(macro["T10Y2Y"])
    f["curve"], f["curve_chg"] = curve, curve - curve.shift(20)
    y10 = prev(macro["DGS10"])
    f["y10_chg"] = y10 - y10.shift(20)
    oil = prev(macro["DCOILWTICO"].where(macro["DCOILWTICO"] > 0))
    f["oil_chg"] = oil / oil.shift(20) - 1
    bgs, m = prev(macro["DTWEXBGS"]), prev(macro["DTWEXM"])
    f["usd_chg"] = (bgs / bgs.shift(20) - 1).fillna(m / m.shift(20) - 1)
    return f


# ── weekly rows ──

def calendar(week_days: list[pd.DatetimeIndex], all_days: pd.DatetimeIndex, fomc: set[date], fomc_years: dict[int, str]) -> pd.DataFrame:
    """Per target week: turn of month, FOMC statement, short week, month sin / cos."""
    s = pd.Series(all_days, index=all_days)
    per = all_days.to_period("M")
    last = set(s.groupby(per).max())
    first3 = set(s.groupby(per).apply(lambda x: list(x.iloc[:3])).explode())
    rows = []
    for days in week_days:
        y = days[-1].year
        known = fomc_years.get(y) == "ok"
        rows.append({"tom": float(any(d in last or d in first3 for d in days)),
                     "fomc": float(any(d.date() in fomc for d in days)) if known else np.nan,
                     "short_week": float(len(days) < 5),
                     "month_sin": math.sin(2 * math.pi * days[-1].month / 12), "month_cos": math.cos(2 * math.pi * days[-1].month / 12)})
    return pd.DataFrame(rows)


def build(prices: dict[str, pd.DataFrame], members: pd.DataFrame, macro: dict[str, pd.Series],
          fomc: set[date], fomc_years: dict[int, str]) -> pd.DataFrame:
    spy = prices["SPY"]
    days = spy.index
    ends = wd.week_ends(days)
    mkt = market_features(prices, members, macro).reindex(ends)
    rf = ca.french_daily_rf()
    rf_cum = np.log1p(rf).cumsum()
    out = []
    for a in ASSETS:
        if a not in prices:
            continue
        g = prices[a]
        af = asset_features(g, spy if a != "SPY" else None)
        key = "TOP500" if a == "SPY" else a
        br = pd.DataFrame({"a50": members.get(f"{key}|a50"), "a200": members.get(f"{key}|a200"), "hl": members.get(f"{key}|hl")}).reindex(g.index)
        br["a50_chg"] = br["a50"] - br["a50"].shift(20)
        e = ends[ends >= g.index[0]]
        f = pd.concat([af.reindex(e), br.add_prefix("br_").reindex(e), mkt.reindex(e)], axis=1)
        nxt = list(e[1:]) + [pd.NaT]
        tw = [days[(days > d) & (days <= n)] if pd.notna(n) else pd.DatetimeIndex([]) for d, n in zip(e, nxt)]
        cal = calendar([w for w in tw if len(w)], days, fomc, fomc_years)
        has = [len(w) > 0 for w in tw]
        cal.index = e[has]
        f = f.join(cal)
        c = g["closeadj"]
        monthly = c.resample("ME").last().pct_change().dropna()
        f["season"] = [wd.seasonality(c, d, n, monthly) if pd.notna(n) else np.nan for d, n in zip(e, nxt)]
        ce = c.reindex(e)
        f["ret_next"] = ce.shift(-1) / ce - 1
        rc = rf_cum.reindex(e, method="ffill")
        f["rf_next"] = np.expm1(rc.shift(-1) - rc)
        f["target_end"] = nxt
        f["asset"] = a
        f["above200"] = (c / c.rolling(200, min_periods=200).mean()).reindex(e) > 1          # benchmark B1
        out.append(f)
    d = pd.concat(out)
    d.index.name = "date"
    d = d.reset_index()
    d = d[d["ret_next"].notna()]
    d["y"] = (d["ret_next"] > 0).astype(int)
    return d


META = {"date", "asset", "ret_next", "rf_next", "target_end", "y", "above200"}


def feature_cols(d: pd.DataFrame) -> list[str]:
    return [c for c in d.columns if c not in META]


# ── models ──

class Prep:
    """Winsorise 1 / 99 % and standardise with training statistics; median imputation plus flags for features
    that had missing values in training."""

    def __init__(self, train: pd.DataFrame, cols: list[str]):
        x = train[cols].astype(float)
        self.cols = cols
        self.lo, self.hi = x.quantile(0.01), x.quantile(0.99)
        self.med = x.median()
        xc = x.clip(self.lo, self.hi, axis=1).fillna(self.med)
        self.mu, self.sd = xc.mean(), xc.std().replace(0, 1).fillna(1)
        self.flags = [c for c in cols if x[c].isna().any()]

    def __call__(self, df: pd.DataFrame) -> np.ndarray:
        x = df[self.cols].astype(float)
        z = ((x.clip(self.lo, self.hi, axis=1).fillna(self.med) - self.mu) / self.sd).fillna(0.0)
        return np.hstack([z.to_numpy(), x[self.flags].isna().to_numpy(float)]) if self.flags else z.to_numpy()


def logloss(y: np.ndarray, p: np.ndarray) -> float:
    p = np.clip(p, 1e-6, 1 - 1e-6)
    return float(-np.mean(y * np.log(p) + (1 - y) * np.log(1 - p)))


def folds(train: pd.DataFrame) -> list[tuple[pd.DataFrame, pd.DataFrame]]:
    """The last five complete years of `train` as validation folds; each fold trains on rows whose outcome is known
    before the fold's first decision (a one-week gap)."""
    years = sorted(train["date"].dt.year.unique())[-CV_FOLDS:]
    out = []
    for y in years:
        va = train[train["date"].dt.year == y]
        tr = train[pd.to_datetime(train["target_end"]) < va["date"].min()]
        if len(tr) >= 500 and len(va):
            out.append((tr, va))
    return out


def fit_logit(train: pd.DataFrame, cols: list[str], cv: list) -> tuple[object, float | None]:
    from sklearn.linear_model import LogisticRegression

    best, best_gain = None, 0.0
    for C in C_GRID:
        gains = []
        for tr, va in cv:
            prep = Prep(tr, cols)
            m = LogisticRegression(penalty="elasticnet", solver="saga", l1_ratio=0.5, C=C, max_iter=5000, tol=1e-3)
            m.fit(prep(tr), tr["y"])
            gains.append(logloss(va["y"].to_numpy(), m.predict_proba(prep(va))[:, 1])
                         - logloss(va["y"].to_numpy(), np.full(len(va), tr["y"].mean())))
        g = float(np.mean(gains))
        if g < best_gain:
            best, best_gain = C, g
    if best is None:
        return None, None
    prep = Prep(train, cols)
    m = LogisticRegression(penalty="elasticnet", solver="saga", l1_ratio=0.5, C=best, max_iter=5000, tol=1e-3).fit(prep(train), train["y"])
    return (prep, m), best


def fit_lgb(train: pd.DataFrame, cols: list[str], cv: list) -> tuple[object, dict | None]:
    import lightgbm as lgb

    best, best_gain, best_rounds = None, 0.0, None
    for cfg in LGB_GRID:
        gains, rounds = [], []
        for tr, va in cv:
            prep = Prep(tr, cols)
            dtr = lgb.Dataset(prep(tr), label=tr["y"])
            dva = lgb.Dataset(prep(va), label=va["y"], reference=dtr)
            b = lgb.train({**LGB_BASE, **cfg, "metric": "binary_logloss"}, dtr, num_boost_round=LGB_MAX, valid_sets=[dva],
                          callbacks=[lgb.early_stopping(LGB_PATIENCE, verbose=False)])
            p = b.predict(prep(va), num_iteration=b.best_iteration)
            gains.append(logloss(va["y"].to_numpy(), p) - logloss(va["y"].to_numpy(), np.full(len(va), tr["y"].mean())))
            rounds.append(b.best_iteration)
        g = float(np.mean(gains))
        if g < best_gain:
            best, best_gain, best_rounds = cfg, g, int(np.median(rounds))
    if best is None:
        return None, None
    prep = Prep(train, cols)
    b = lgb.train({**LGB_BASE, **best}, lgb.Dataset(prep(train), label=train["y"]), num_boost_round=max(best_rounds, 1))
    return (prep, b), {**best, "rounds": best_rounds}


def walk_forward(d: pd.DataFrame, log=print) -> tuple[pd.DataFrame, list[dict]]:
    cols = feature_cols(d)
    start = d.loc[(d["asset"] == "SPY") & d["cmf_pct"].notna(), "date"].min()
    d = d[d["date"] >= start]
    out, chosen = [], []
    for yr in range(start.year + 1 + CV_FOLDS + (0 if start.month == 1 else 1), d["date"].dt.year.max() + 1):
        train = d[pd.to_datetime(d["target_end"]) < pd.Timestamp(f"{yr}-01-01")]
        test = d[d["date"].dt.year == yr]
        if test.empty:
            continue
        cv = folds(train)
        base = float(train["y"].mean())
        m1, c1 = fit_logit(train, cols, cv)
        m2, c2 = fit_lgb(train, cols, cv)
        p1 = m1[1].predict_proba(m1[0](test))[:, 1] if m1 else np.full(len(test), base)
        p2 = m2[1].predict(m2[0](test)) if m2 else np.full(len(test), base)
        p3 = (p1 + p2) / 2
        out.append(test[["date", "asset", "y", "ret_next", "rf_next", "above200", "cmf_pct", "tom"]].assign(m0=base, m1=p1, m2=p2, m3=p3))
        chosen.append({"year": yr, "m1_C": c1, "m2": c2, "base": base, "train_rows": int(len(train))})
        log(f"{yr}: base {base:.3f} · M1 C={c1} · M2 {c2}")
    return pd.concat(out, ignore_index=True), chosen


# ── evaluation ──

def calls_summary(o: pd.DataFrame, flag: pd.Series) -> dict:
    s = o[flag]
    years = (o["date"].max() - o["date"].min()).days / 365.25
    per_asset_week = o.groupby("date").size().mean()
    if s.empty:
        return {"n": 0, "per_asset_year": 0.0}
    w = (s["y"] - s["m0"]).groupby(s["date"]).mean()
    return {"n": int(len(s)), "per_asset_year": float(len(s) / (per_asset_week * years)), "precision": float(s["y"].mean()),
            "base": float(s["m0"].mean()), "gap": float((s["y"] - s["m0"]).mean()), "t": wd.nw_t(w, NW_LAG),
            "next_ret": float(s["ret_next"].mean())}


def evaluate(o: pd.DataFrame) -> dict:
    mid = o["date"].min() + (o["date"].max() - o["date"].min()) / 2
    parts = {"all": o, "first": o[o["date"] < mid], "second": o[o["date"] >= mid]}
    res: dict = {"range": [str(o["date"].min().date()), str(o["date"].max().date())], "mid": str(mid.date()),
                 "rows": int(len(o)), "base_up": float(o["y"].mean())}
    for m in ("m1", "m2", "m3"):
        r = {h: {"calls": calls_summary(x, x[m] >= THRESHOLD),
                 "logloss": logloss(x["y"].to_numpy(), x[m].to_numpy()), "logloss_base": logloss(x["y"].to_numpy(), x["m0"].to_numpy())}
             for h, x in parts.items()}
        r["calibration"] = wd.calibration(o["y"].to_numpy(), o[m].to_numpy())
        r["by_year"] = {int(y): calls_summary(x, x[m] >= THRESHOLD).get("precision") for y, x in o.groupby(o["date"].dt.year)}
        o2 = o.assign(held=o[m] >= THRESHOLD)
        o2 = o2.sort_values(["asset", "date"])
        prev = o2.groupby("asset")["held"].shift(1)
        sw = (o2["held"] != prev) & ~(prev.isna() & ~o2["held"])
        o2["r"] = np.where(o2["held"], o2["ret_next"], o2["rf_next"]) - COST * sw
        strat, hold, rf = o2.groupby("date")["r"].mean(), o2.groupby("date")["ret_next"].mean(), o2.groupby("date")["rf_next"].mean()
        r["economic"] = {"calls_only": wd.perf(strat, rf), "hold": wd.perf(hold, rf)}
        a, f, s = r["all"], r["first"], r["second"]
        checks = {
            "precision_all": (a["calls"].get("precision") or 0) >= PRECISION_MIN,
            "precision_halves": all((x["calls"].get("precision") or 0) >= PRECISION_MIN for x in (f, s)),
            "gap": (a["calls"].get("gap") or 0) >= GAP_MIN,
            "t": (a["calls"].get("t") or 0) >= T_BAR,
            "frequency": a["calls"]["per_asset_year"] >= CALLS_MIN,
            "no_harm_halves": all(x["logloss"] <= x["logloss_base"] for x in (f, s)),
        }
        r["decision"] = {"checks": checks, "pass": all(checks.values())}
        res[m] = r
    res["benchmarks"] = {name: {h: calls_summary(x, flag(x)) for h, x in parts.items()} for name, flag in (
        ("B1_above_200d", lambda x: x["above200"].fillna(False).astype(bool)),
        ("B2_cmf_lowest_fifth", lambda x: x["cmf_pct"] <= 0.2),
        ("B3_turn_of_month", lambda x: x["tom"] == 1))}
    return res


def run(macro: dict, fomc: set[date], fomc_years: dict[int, str], log=print) -> dict:
    prices = load_prices()
    members = load_members()
    d = build(prices, members, macro, fomc, fomc_years)
    log(f"rows {len(d)}, features {len(feature_cols(d))}")
    o, chosen = walk_forward(d, log)
    o.to_parquet("exports/weekly_up/oos.parquet")
    return {"key": KEY, "features": feature_cols(d), "fomc_years": {str(k): v for k, v in fomc_years.items()},
            "chosen": chosen, **evaluate(o)}


# ── report ──

def _p(x: float | None, d: int = 1) -> str:
    return "—" if x is None else f"{x * 100:.{d}f}%"


def report_text(res: dict) -> str:
    L = [f"周度\"会涨\"判断 {res['key']}：样本外 {res['range']}（{res['rows']} 个资产-周，{len(res['features'])} 个特征），中点 {res['mid']}，下周上涨基础比例 {_p(res['base_up'])}",
         "FOMC 缺失年份：" + (", ".join(k for k, v in res["fomc_years"].items() if v != "ok") or "无")]
    abst = [(c["year"], c["m1_C"] is None, c["m2"] is None) for c in res["chosen"]]
    L.append(f"不表态（交叉验证不如基础比例）的年份：M1 {sum(a for _, a, _ in abst)}/{len(abst)}，M2 {sum(b for _, _, b in abst)}/{len(abst)}")
    for m, name in (("m1", "M1 弹性网逻辑回归"), ("m2", "M2 LightGBM"), ("m3", "M3 平均")):
        r = res[m]
        L.append(f"\n【{name}】")
        for h in ("all", "first", "second"):
            c = r[h]["calls"]
            L.append(f"  {h}: 判断\"会涨\" {c['n']} 次（每资产每年 {c['per_asset_year']:.1f}），准确率 {_p(c.get('precision'))} vs 基础 {_p(c.get('base'))}"
                     f"（差 {(c.get('gap') or 0) * 100:+.1f} 个百分点，t {c.get('t') or 0:.2f}），下周平均 {(c.get('next_ret') or 0) * 100:+.2f}%；"
                     f"全部周对数损失 {r[h]['logloss']:.4f} vs 基础 {r[h]['logloss_base']:.4f}")
        L.append("  各年准确率：" + " ".join(f"{y}:{_p(v, 0)}" for y, v in r["by_year"].items()))
        L.append(f"  校准斜率 {r['calibration']['slope']:.2f}；十分组 " + " ".join(f"{x['pred'] * 100:.0f}→{x['actual'] * 100:.0f}" for x in r["calibration"]["deciles"]))
        e = r["economic"]
        L.append(f"  只在\"会涨\"时持有：年化 {_p(e['calls_only']['cagr'])} 夏普 {e['calls_only']['sharpe']:.2f} 回撤 {_p(e['calls_only']['max_dd'])}；"
                 f"一直持有 {_p(e['hold']['cagr'])} {e['hold']['sharpe']:.2f} {_p(e['hold']['max_dd'])}")
        L.append(f"  判定：{'通过' if r['decision']['pass'] else '不通过'} {r['decision']['checks']}")
    L.append("\n【简单规则对照（只报告）】")
    for k, v in res["benchmarks"].items():
        a, f, s = v["all"], v["first"], v["second"]
        L.append(f"  {k}: {a['n']} 次，准确率 {_p(a.get('precision'))} vs 基础 {_p(a.get('base'))}（t {a.get('t') or 0:.2f}），前 {_p(f.get('precision'))} / 后 {_p(s.get('precision'))}")
    return "\n".join(L)
