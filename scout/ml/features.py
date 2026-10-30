"""Feature store v1 for the ranking model (pre-registered in `scout/knowledge/external_specs/ml_v1.yaml`).

`build` makes one row per (sample date, stock in that day's large / mid universe) with raw features and
the 21 / 63-session labels, all point in time; `process` applies the typed operators. Both read only the
local Sharadar files and write only the git-ignored exports/ml/ (Sharadar-derived: deleted with it).
Wide arrays are dates × stocks; a feature is read off them only at the sampled (date, stock) cells.
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from scout import gate
from scout import sharadar_research as sr
from scout.features import market_regime
from scout.ml import operators as op
from scout.sharadar_exits import SECTOR_ETF

OUT = Path(__file__).resolve().parents[2] / "exports" / "ml"
FIRST = "1999-01-04"
EVERY = 5
HORIZONS = (21, 63)
TS_LAG = 13                          # sample steps ≈ 63 sessions for the rank-change features

PRICE = ["ret_1", "ret_5", "ret_21", "ret_63", "ret_126", "ret_252", "mom_12_1", "vol_21", "vol_63", "downvol_63",
         "beta_252", "idio_252", "mdd_63", "mdd_252", "dist_hi_252", "dist_lo_252", "gap_sma20", "gap_sma50",
         "gap_sma200", "rsi14", "macd_hist_pct", "log_dv21", "dv_ratio", "amihud21", "skew63"]
FUND = ["log_mcap", "bm", "ep", "sp", "ebitda_ev", "gp_assets", "roe", "roa", "accruals", "asset_growth",
        "sales_growth", "share_growth", "leverage", "capex_assets", "rnd_sales"]
EVENTS = ["ear", "ear_days", "insider_net", "inst_chg"]
TS_SOURCES = ["mom_12_1", "vol_63", "bm", "gp_assets"]
CONTEXT = ["spy_ret_21", "spy_ret_63", "spy_ret_252", "spy_vol_21", "vix", "term_spread", "breadth", "sector_rel_63"]
SIGNALS = ["breakout_retest_10d"]
NOMINAL = ["sector", "industry"]
ORDINAL = {"size_bucket": list(range(10)), "regime": ["downtrend", "mixed", "uptrend"], "vol_state": ["low", "high"]}
LABELS = [f"y{n}" for n in HORIZONS] + [f"y{n}_sec" for n in HORIZONS]


def sample_days(dates: pd.DatetimeIndex) -> list[int]:
    start = int(np.searchsorted(dates, pd.Timestamp(FIRST)))
    return list(range(start, len(dates) - 1, EVERY))


# ── price and volume (wide) ─────────────────────────────────────────────

def _rolling_beta(r: pd.DataFrame, m: pd.Series, n: int = 252) -> tuple[pd.DataFrame, pd.DataFrame]:
    mp = min(n, 200)
    mm = m.rolling(n, min_periods=mp).mean()
    mv = m.rolling(n, min_periods=mp).var()
    rm = r.mul(m, axis=0).rolling(n, min_periods=mp).mean()
    cov = rm.sub(r.rolling(n, min_periods=mp).mean().mul(mm, axis=0))
    beta = cov.div(mv, axis=0)
    resid_var = r.rolling(n, min_periods=mp).var() - beta.pow(2).mul(mv, axis=0)
    return beta, np.sqrt(resid_var.clip(lower=0) * 252)


def _max_drawdown(c: np.ndarray, rows: np.ndarray, cols: np.ndarray, w: int) -> np.ndarray:
    """Max drawdown over the w sessions up to each sampled cell."""
    out = np.full(len(rows), np.nan)
    for i in np.unique(rows):
        sel = np.flatnonzero(rows == i)
        block = c[max(0, i - w + 1):i + 1, cols[sel]]
        if block.shape[0] < w // 2:
            continue
        run = np.fmax.accumulate(np.where(np.isfinite(block), block, -np.inf), axis=0)
        dd = np.where(np.isfinite(block) & (run > 0), block / run - 1, np.nan)
        with np.errstate(all="ignore"):
            out[sel] = np.nanmin(dd, axis=0)
    return out


def price_features(p: sr.Prices, spy: pd.Series, rows: np.ndarray, cols: np.ndarray) -> dict[str, np.ndarray]:
    f = p.closeadj / p.close
    c = pd.DataFrame(p.closeadj, index=p.dates)
    h = pd.DataFrame(p.high * f, index=p.dates)
    lo = pd.DataFrame(p.low * f, index=p.dates)
    dv = pd.DataFrame(p.close * p.volume, index=p.dates)
    r = c.pct_change(fill_method=None)
    m = spy.pct_change(fill_method=None)
    take = lambda df: df.to_numpy()[rows, cols]  # noqa: E731
    out = {f"ret_{n}": take(c / c.shift(n) - 1) for n in (1, 5, 21, 63, 126, 252)}
    out["mom_12_1"] = take(c.shift(21) / c.shift(252) - 1)
    out["vol_21"] = take(r.rolling(21, min_periods=15).std() * np.sqrt(252))
    out["vol_63"] = take(r.rolling(63, min_periods=40).std() * np.sqrt(252))
    out["downvol_63"] = take(r.where(r < 0).rolling(63, min_periods=20).std() * np.sqrt(252))
    beta, idio = _rolling_beta(r, m)
    out["beta_252"], out["idio_252"] = take(beta), take(idio)
    arr = c.to_numpy()
    out["mdd_63"] = _max_drawdown(arr, rows, cols, 63)
    out["mdd_252"] = _max_drawdown(arr, rows, cols, 252)
    out["dist_hi_252"] = take(c / h.rolling(252, min_periods=200).max() - 1)
    out["dist_lo_252"] = take(c / lo.rolling(252, min_periods=200).min() - 1)
    for n in (20, 50, 200):
        out[f"gap_sma{n}"] = take(c / c.rolling(n, min_periods=n).mean() - 1)
    delta = c.diff()
    gain = delta.clip(lower=0).ewm(alpha=1 / 14, adjust=False, min_periods=14).mean()
    loss = (-delta.clip(upper=0)).ewm(alpha=1 / 14, adjust=False, min_periods=14).mean()
    out["rsi14"] = take(100 - 100 / (1 + gain / loss.replace(0, np.nan)))
    line = c.ewm(span=12, adjust=False, min_periods=12).mean() - c.ewm(span=26, adjust=False, min_periods=26).mean()
    out["macd_hist_pct"] = take((line - line.ewm(span=9, adjust=False, min_periods=9).mean()) / c)
    dv21 = dv.rolling(21, min_periods=15).mean()
    out["log_dv21"] = take(np.log(dv21.where(dv21 > 0)))
    out["dv_ratio"] = take(dv21 / dv.rolling(126, min_periods=80).mean())
    out["amihud21"] = take(np.log1p((r.abs() / dv.where(dv > 0)).rolling(21, min_periods=15).mean() * 1e9))
    out["skew63"] = take(r.rolling(63, min_periods=40).skew())
    return out


def breakout_retest(p: sr.Prices, rows: np.ndarray, cols: np.ndarray) -> np.ndarray:
    """1 if the breakout-retest rule (scout.detectors.rules D1_BREAKOUT, entry at the breakout level
    ± 0.25 ATR within 10 sessions) filled within the last 10 sessions up to the sampled day."""
    f = p.closeadj / p.close
    o, h, lo, c = (pd.DataFrame(x * f, index=p.dates) for x in (p.open, p.high, p.low, p.close))
    v = pd.DataFrame(p.volume, index=p.dates)
    prev = c.shift(1)
    tr = np.fmax(h - lo, np.fmax((h - prev).abs(), (lo - prev).abs()))
    atr = tr.rolling(14, min_periods=14).mean()
    hi55 = h.rolling(55, min_periods=55).max().shift(1)
    sma50, sma200 = c.rolling(50, min_periods=50).mean(), c.rolling(200, min_periods=200).mean()
    rs = (0.4 * (c / c.shift(63) - 1) + 0.3 * (c / c.shift(126) - 1) + 0.3 * (c / c.shift(252) - 1)).rank(axis=1, pct=True)
    rng = (h - lo).replace(0, np.nan)
    cond = ((c > hi55) & (v / v.rolling(50, min_periods=20).mean().shift(1) >= 1.8) & (c > sma50) & (sma50 > sma200)
            & (rs >= 0.8) & (((c - lo) / rng).fillna(0.5) >= 0.6) & (o / prev - 1 < 0.04))
    fill = np.zeros(c.shape, dtype=bool)
    H, L, P, A = h.to_numpy(), lo.to_numpy(), hi55.to_numpy(), atr.to_numpy()
    for b, j in zip(*np.nonzero(cond.to_numpy())):
        lo_z, hi_z = P[b, j] - 0.25 * A[b, j], P[b, j] + 0.25 * A[b, j]
        for k in range(b + 1, min(b + 11, len(fill))):
            if L[k, j] <= hi_z and H[k, j] >= lo_z:
                fill[k, j] = True
                break
    recent = pd.DataFrame(fill).rolling(10, min_periods=1).max().to_numpy()
    return recent[rows, cols]


# ── fundamentals, events, holders (as-of joins on filing / availability dates) ──

def _asof(keys: pd.DataFrame, table: pd.DataFrame, on: str, cols: list[str], days: int) -> pd.DataFrame:
    """For each (date, ticker) the latest row of `table` with table[on] ≤ date, within `days`."""
    left = keys[["date", "ticker"]].reset_index()
    left["date"] = pd.to_datetime(left["date"]).astype("datetime64[ns]")
    left = left.sort_values("date")
    right = table.dropna(subset=[on]).copy()
    right[on] = pd.to_datetime(right[on]).astype("datetime64[ns]")
    right = right.sort_values(on)
    got = pd.merge_asof(left, right[["ticker", on, *cols]], left_on="date", right_on=on, by="ticker",
                        tolerance=pd.Timedelta(days=days), direction="backward")
    return got.set_index("index").reindex(keys.index)[cols]


def fundamentals(keys: pd.DataFrame, names: list[str]) -> pd.DataFrame:
    cols = ["ticker", "dimension", "date", "reportperiod", "assets", "equity", "revenue", "gp", "netinc", "ncfo",
            "sharesbas", "debt", "capex", "rnd"]
    f = sr._read("fundamentals", cols, filters=[("dimension", "in", ["ARQ", "ART"]), ("ticker", "in", names)])
    f = f.rename(columns={"date": "datekey"})
    f["datekey"], f["reportperiod"] = pd.to_datetime(f["datekey"]), pd.to_datetime(f["reportperiod"])
    f = f.sort_values(["ticker", "dimension", "reportperiod"]).drop_duplicates(["ticker", "dimension", "reportperiod"])
    g = f.groupby(["ticker", "dimension"])
    for c in ("assets", "revenue", "sharesbas", "reportperiod"):
        f[f"{c}_4"] = g[c].shift(4)
    year = ((f["reportperiod"] - f["reportperiod_4"]).dt.days.between(330, 400))
    pos = lambda s: s.where(s > 0)  # noqa: E731
    f["asset_growth"] = (f["assets"] / pos(f["assets_4"]) - 1).where(year)
    f["share_growth"] = (f["sharesbas"] / pos(f["sharesbas_4"]) - 1).where(year)
    f["sales_growth"] = (f["revenue"] / pos(f["revenue_4"]) - 1).where(year)
    f["leverage"] = f["debt"] / pos(f["assets"])
    f["gp_assets"] = f["gp"] / pos(f["assets"])
    f["roe"] = f["netinc"] / pos(f["equity"])
    f["roa"] = f["netinc"] / pos(f["assets"])
    f["accruals"] = ((f["netinc"] - f["ncfo"]) / pos((f["assets"] + f["assets_4"]) / 2)).where(year)
    f["capex_assets"] = -f["capex"] / pos(f["assets"])
    f["rnd_sales"] = f["rnd"] / pos(f["revenue"])
    q = _asof(keys, f[f["dimension"] == "ARQ"], "datekey", ["asset_growth", "share_growth", "leverage"], 400)
    t = _asof(keys, f[f["dimension"] == "ART"], "datekey",
              ["gp_assets", "roe", "roa", "accruals", "sales_growth", "capex_assets", "rnd_sales"], 400)
    d = sr._read("daily", ["ticker", "date", "marketcap", "pe", "pb", "ps", "evebitda"],
                 filters=[("ticker", "in", names), ("date", "in", sorted({x.date() for x in keys["date"]}))])
    d["date"] = pd.to_datetime(d["date"])
    v = keys[["date", "ticker"]].merge(d, on=["date", "ticker"], how="left").set_index(keys.index)
    inv = lambda s: (1 / s).where(s.abs() > 1e-9)  # noqa: E731
    val = pd.DataFrame({"log_mcap": np.log(v["marketcap"].where(v["marketcap"] > 0)), "bm": inv(v["pb"].where(v["pb"] > 0)),
                        "ep": inv(v["pe"]), "sp": inv(v["ps"].where(v["ps"] > 0)), "ebitda_ev": inv(v["evebitda"]),
                        "marketcap": v["marketcap"]}, index=keys.index)
    return pd.concat([val, q, t], axis=1)


def events_holders(keys: pd.DataFrame, names: list[str], p: sr.Prices, spy: np.ndarray, mcap: pd.Series) -> pd.DataFrame:
    dates = p.dates
    ev = sr._read("events", ["ticker", "date", "eventcodes"], filters=[("ticker", "in", names)])
    ev = ev[ev["eventcodes"].fillna("").str.split("|").apply(lambda x: "22" in x)]
    ev["date"] = pd.to_datetime(ev["date"])
    rows = []
    for t, e in zip(ev["ticker"], ev["date"]):
        j = p.col.get(t)
        k = int(np.searchsorted(dates, e))
        if j is None or k < 2 or k + 1 >= len(dates):
            continue
        a, b = p.closeadj[k - 2, j], p.closeadj[k + 1, j]
        if np.isfinite(a) and np.isfinite(b) and a > 0:
            rows.append({"ticker": t, "known": dates[k + 1], "known_on": dates[k + 1], "ear": b / a - 1 - (spy[k + 1] / spy[k - 2] - 1)})
    ear = pd.DataFrame(rows)
    got = (_asof(keys, ear, "known", ["ear", "known_on"], 92) if len(ear)
           else pd.DataFrame(index=keys.index, columns=["ear", "known_on"]))
    out = pd.DataFrame({"ear": got["ear"].astype(float),
                        "ear_days": (keys["date"] - pd.to_datetime(got["known_on"])).dt.days.astype(float)}, index=keys.index)
    ins = sr._read("insiders", ["ticker", "date", "transactioncode", "isofficer", "isdirector", "transactionshares",
                                "transactionpricepershare", "transactionvalue"], filters=[("ticker", "in", names),
                                                                                         ("transactioncode", "in", ["P", "S"])])
    ins = ins.rename(columns={"date": "filingdate"})          # the bulk file calls the filing date `date`
    ins = ins[(ins["isofficer"] == "Y") | (ins["isdirector"] == "Y")].copy()
    val = ins["transactionvalue"].fillna(ins["transactionshares"].abs() * ins["transactionpricepershare"]).abs()
    ins["v"] = np.where(ins["transactioncode"] == "P", val, -val)
    ins["filingdate"] = pd.to_datetime(ins["filingdate"])
    ins = ins.dropna(subset=["v", "filingdate"]).sort_values(["ticker", "filingdate"])
    daily = ins.groupby(["ticker", "filingdate"], as_index=False)["v"].sum()
    daily["cum"] = daily.groupby("ticker")["v"].cumsum()
    now = _asof(keys, daily, "filingdate", ["cum"], 100_000)["cum"]
    back = keys.assign(date=keys["date"] - pd.Timedelta(days=90))
    then = _asof(back, daily, "filingdate", ["cum"], 100_000)["cum"]
    net = now.fillna(0) - then.fillna(0)
    out["insider_net"] = (net / (mcap * 1e6)).where(now.notna())
    h = sr._read("holdings_ticker", ["ticker", "date", "shrunits"], filters=[("ticker", "in", names)])
    h["date"] = pd.to_datetime(h["date"])
    h = h.sort_values(["ticker", "date"])
    prev, prev_d = h.groupby("ticker")["shrunits"].shift(1), h.groupby("ticker")["date"].shift(1)
    h["inst_chg"] = (h["shrunits"] / prev.where(prev > 0) - 1).where((h["date"] - prev_d).dt.days.between(80, 100))
    h["available"] = h["date"] + pd.Timedelta(days=45)
    out["inst_chg"] = _asof(keys, h, "available", ["inst_chg"], 120)["inst_chg"]
    return out


# ── build and process ───────────────────────────────────────────────────

def build(fred: dict[str, list[tuple]], until: str | None = None, part: str = "large",
          hi: str | None = None) -> pd.DataFrame:
    """Raw features and labels, one row per (sample date, stock of that day's `part` universe: "large" =
    market cap top 1000, "small" = ranks 1001–3000, cap_rank continuing from 1000). `until` limits the
    sampled dates (a smoke test inside the discovery period); `hi` also limits the price history read."""
    dates = sr.trading_days()
    if hi:
        dates = dates[dates <= pd.Timestamp(hi)]
    samples = [i for i in sample_days(dates) if until is None or dates[i] <= pd.Timestamp(until)]
    elig = sr.eligible()
    uni = sr.universe(dates[samples], elig)
    names = sorted({t for v in uni.values() for t in v[part]})
    p = sr.prices(names, dates)
    offset = 0 if part == "large" else sr.LARGE
    rows_l, cols_l, rank_l = [], [], []
    for i in samples:
        members = [t for t in uni.get(dates[i], {}).get(part, []) if t in p.col]
        rows_l += [i] * len(members)
        cols_l += [p.col[t] for t in members]
        rank_l += [offset + k for k in range(len(members))]
    rows, cols = np.asarray(rows_l), np.asarray(cols_l)
    keys = pd.DataFrame({"date": dates[rows], "ticker": [p.tickers[j] for j in cols], "i": rows,
                         "sector": elig["sector"].reindex([p.tickers[j] for j in cols]).to_numpy(),
                         "industry": elig["industry"].reindex([p.tickers[j] for j in cols]).to_numpy() if "industry" in elig else None,
                         "cap_rank": rank_l})
    funds = sr._read("funds", ["ticker", "date", "closeadj"], filters=[("ticker", "in", ["SPY", *SECTOR_ETF.values()])])
    funds["date"] = pd.to_datetime(funds["date"])
    fw = funds.pivot(index="date", columns="ticker", values="closeadj").reindex(dates)
    spy = fw["SPY"]
    raw = pd.DataFrame(price_features(p, spy, rows, cols), index=keys.index)
    raw["breakout_retest_10d"] = breakout_retest(p, rows, cols)
    fund = fundamentals(keys, names)
    raw = pd.concat([raw, fund.drop(columns=["marketcap"])], axis=1)
    raw = pd.concat([raw, events_holders(keys, names, p, spy.to_numpy(), fund["marketcap"])], axis=1)
    # context: per date, plus the stock's sector ETF against SPY
    ctx = pd.DataFrame(index=dates)
    for n in (21, 63, 252):
        ctx[f"spy_ret_{n}"] = spy / spy.shift(n) - 1
    ctx["spy_vol_21"] = spy.pct_change(fill_method=None).rolling(21).std() * np.sqrt(252)
    for name, sid in (("vix", "VIXCLS"), ("term_spread", "T10Y2Y")):
        s = pd.Series({pd.Timestamp(d): v for d, v in fred.get(sid, [])}, dtype=float).sort_index()
        ctx[name] = s.reindex(dates, method="ffill").shift(1)        # yesterday's value: known before today's close
    sma200 = pd.DataFrame(p.closeadj, index=dates).rolling(200, min_periods=200).mean().to_numpy()
    above = (p.closeadj[rows, cols] > sma200[rows, cols]).astype(float)
    breadth = pd.Series(above, index=keys.index).groupby(keys["date"]).mean()
    ctx["regime"] = market_regime(spy)
    ctx["vol_state"] = gate.vol_states(spy.to_numpy())
    c = keys[["date"]].join(ctx, on="date")
    for col in ("spy_ret_21", "spy_ret_63", "spy_ret_252", "spy_vol_21", "vix", "term_spread", "regime", "vol_state"):
        raw[col] = c[col].to_numpy()
    raw["breadth"] = keys["date"].map(breadth).to_numpy()
    rel = {s: (fw[e] / fw[e].shift(63) - 1) - (spy / spy.shift(63) - 1) for s, e in SECTOR_ETF.items() if e in fw.columns}
    raw["sector_rel_63"] = [rel[s].iloc[i] if s in rel else np.nan for s, i in zip(keys["sector"], keys["i"])]
    raw["size_bucket"] = (keys["cap_rank"] * 10 // 1000).clip(0, 9).astype(int).rsub(9).to_numpy()   # 9 = largest
    # labels: next open → close N sessions later, minus the date's universe mean (and the sector mean)
    for n in HORIZONS:
        y = np.array([sr.holding_return(p, j, i, n) if i + n < len(dates) else None for i, j in zip(rows, cols)], dtype=float)
        s = pd.Series(y, index=keys.index)
        raw[f"y{n}_raw"] = s
        raw[f"y{n}"] = s - s.groupby(keys["date"]).transform("mean")
        raw[f"y{n}_sec"] = s - s.groupby([keys["date"], keys["sector"]]).transform("mean")
        raw[f"y{n}_end"] = [dates[i + n] if i + n < len(dates) else pd.NaT for i in rows]
    return pd.concat([keys, raw], axis=1)


def process(raw: pd.DataFrame) -> pd.DataFrame:
    """Typed operators → model inputs (plus keys and labels)."""
    cs = PRICE + FUND + EVENTS
    out = [raw[["date", "ticker", "i", "sector", "industry"]]]
    ranked = op.cross_sectional(raw, cs)
    out.append(ranked)
    neutral = op.cross_sectional(raw, ["mom_12_1", "bm", "gp_assets", "vol_63"], group="sector")
    out.append(neutral[[c for c in neutral.columns if not c.endswith("_na")]].add_suffix("_secn"))
    # time series: change of the cross-sectional rank over TS_LAG sample steps, per stock
    rk = pd.concat([raw[["ticker", "date"]], ranked[TS_SOURCES]], axis=1)
    order = sorted(raw["date"].unique())
    step = {d: k for k, d in enumerate(order)}
    rk["k"] = rk["date"].map(step)
    prior = rk.assign(k=rk["k"] + TS_LAG)[["ticker", "k", *TS_SOURCES]]
    joined = rk[["ticker", "k"]].merge(prior, on=["ticker", "k"], how="left", suffixes=("", "_prev"))
    ts = pd.DataFrame({f"{c}_chg": (ranked[c].to_numpy() - joined[c].to_numpy()) for c in TS_SOURCES}, index=raw.index)
    out.append(ts.fillna(0.0).astype("float32"))
    per_date = [c for c in CONTEXT if c != "sector_rel_63"]
    ctx = op.context(raw, per_date).add_prefix("ctx_")
    # the sector ETF against SPY differs by sector within a date: an expanding z-score per sector
    sec = raw[["date", "sector", "sector_rel_63"]].copy()
    first = sec.groupby(["sector", "date"])["sector_rel_63"].first()
    z = first.groupby(level=0, group_keys=False).apply(op.expanding_z)
    ctx["ctx_sector_rel_63"] = sec.join(z.rename("z"), on=["sector", "date"])["z"].fillna(0.0).astype("float32").to_numpy()
    out.append(ctx)
    out.append(raw[SIGNALS].fillna(0).astype("int8"))
    out.append(pd.DataFrame({k: op.ordinal(raw[k], v) for k, v in ORDINAL.items()}, index=raw.index))
    labels = [c for c in raw.columns if c.startswith("y")]
    out.append(raw[labels])
    return pd.concat(out, axis=1)


def save(df: pd.DataFrame, name: str) -> Path:
    OUT.mkdir(parents=True, exist_ok=True)
    path = OUT / f"{name}.parquet"
    df.to_parquet(path, index=False)
    return path
