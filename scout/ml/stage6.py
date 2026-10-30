"""Stage 6 (pre-registered in `scout/knowledge/external_specs/ml_v6.yaml`): every stock feature we have plus
the literature's gaps, with grouped ridge penalties; a sector layer and the two-layer score; a tabular
network ensemble with context only through a gate. Discovery 2004–2021; nothing after 2021-12-31 is read.
"""
from __future__ import annotations

import itertools
import json

import numpy as np
import pandas as pd

from scout import sharadar_research as sr
from scout.ml import baselines as B
from scout.ml import features as F
from scout.ml import gbdt, stage4, stage5, tilt
from scout.ml import operators as op

UNTIL = "2021-12-31"
DISCOVERY = ("2004-01-01", UNTIL)
PRED_FROM = "2002-01-01"            # out-of-sample predictions from here (2002–03 only feed β / the stack weights)
SUBPERIOD = "2008-01-01"
STORE, SECTOR_STORE = "features_v6", "sector_v6"
FFILL_STEPS = 13                    # sample steps ≈ 63 sessions
COVERAGE = 0.5
INNER_MONTHS = 24
GRID = gbdt.RIDGE_V2
OFF = 1e15                          # "group removed"
BETAS = (0.0, 0.25, 0.5, 1.0, 2.0)

FAMILIES = {
    "price": F.PRICE + ["max_ret21", "turnover21", "season"],
    "value": ["log_mcap", "bm", "ep", "sp", "ebitda_ev", "div_yield"],
    "quality": ["gp_assets", "roe", "roa", "accruals", "leverage", "capex_assets", "rnd_sales", "noa", "roe_chg", "gm_chg"],
    "growth": ["asset_growth", "sales_growth", "share_growth"],
    "events": ["ear", "ear_days", "insider_net", "inst_chg", "sue"],
}
STOCK = [c for v in FAMILIES.values() for c in v]
FFILL = [c for f in ("value", "quality", "growth", "events") for c in FAMILIES[f]]
SIGN_FLAGS = {"ep_neg": ("ep", "value", lambda x: x < 0), "ebitda_neg": ("ebitda_ev", "value", lambda x: x < 0),
              "roe_neg": ("roe", "quality", lambda x: x < 0), "rnd_zero": ("rnd_sales", "quality", lambda x: x == 0),
              "insider_zero": ("insider_net", "events", lambda x: x == 0)}
FACTORS = {"value": ("bm", 1), "mom": ("mom_12_1", 1), "quality": ("gp_assets", 1), "small": ("log_mcap", -1),
           "lowvol": ("vol_63", -1), "lowinv": ("asset_growth", -1)}
FM_LAGS = (63, 252)
CTX_GROUPS = {
    "x_market": ["ctx_spy_ret_21", "ctx_spy_ret_63", "ctx_spy_ret_252", "ctx_spy_vol_21", "ctx_vix", "ctx_breadth",
                 "regime_s", "vol_state_s"],
    "x_rates": ["ctx_term_spread", "baa10y", "baa10y_chg63", "dgs10_chg63"],
    "x_disp": ["disp_ret_21", "disp_ret_63", "disp_mom_12_1"],
    "x_factor": [f"fm_{f}_{h}" for f in FACTORS for h in FM_LAGS],
}
CTX = [c for v in CTX_GROUPS.values() for c in v]
GROUPS = ("price", "value", "quality", "growth", "events", "own", "industry", "misc", *CTX_GROUPS)
IND_LEVEL = {"ind_ret21": "ret_21", "ind_ret63": "ret_63", "ind_mom": "mom_12_1", "ind_dvr": "dv_ratio"}
IND_MIN = 5
SECTOR_FEATURES = ["s_ret21", "s_ret63", "s_ret126", "s_ret252", "s_mom", "s_vol63", "s_dvr", "s_above200", "s_bm",
                   "s_ep", "s_gp", "s_sales", "s_ear", "s_ret63_std", "s_etf_rel63"]
SECTOR_X = ["s_mom", "s_ret63", "s_bm", "s_above200", "s_vol63"]


# ── new raw features ──────────────────────────────────────────────────────────

def price_extras(p: sr.Prices, rows: np.ndarray, cols: np.ndarray, mcap: np.ndarray) -> pd.DataFrame:
    c = pd.DataFrame(p.closeadj, index=p.dates)
    r = c.pct_change(fill_method=None)
    dv21 = pd.DataFrame(p.close * p.volume, index=p.dates).rolling(21, min_periods=15).mean()
    take = lambda df: df.to_numpy()[rows, cols]  # noqa: E731
    season = np.stack([take(c.shift(252 * k - 21) / c.shift(252 * k) - 1) for k in range(1, 6)], axis=1)
    enough = np.isfinite(season).sum(axis=1) >= 3
    with np.errstate(all="ignore"):
        return pd.DataFrame({"max_ret21": take(r.rolling(21, min_periods=15).max()),
                             "turnover21": take(dv21) / (mcap * 1e6),
                             "season": np.where(enough, np.nanmean(season, axis=1), np.nan)})


def _year_apart(a: pd.Series, b: pd.Series) -> pd.Series:
    return (a - b).dt.days.between(330, 400)


def fundamental_extras(keys: pd.DataFrame, names: list[str], mcap: pd.Series) -> pd.DataFrame:
    """SUE, NOA (ARQ) and ROE / gross-margin changes, dividend yield (ART), as of the filing date."""
    cols = ["ticker", "dimension", "date", "reportperiod", "netinc", "gp", "revenue", "equity", "debt", "cashneq",
            "assets", "ncfdiv"]
    f = sr._read("fundamentals", cols, filters=[("dimension", "in", ["ARQ", "ART"]), ("ticker", "in", names)])
    f = f.rename(columns={"date": "datekey"})
    f["datekey"], f["reportperiod"] = pd.to_datetime(f["datekey"]), pd.to_datetime(f["reportperiod"])
    f = f[f["datekey"] <= pd.Timestamp(UNTIL)]
    f = f.sort_values(["ticker", "dimension", "reportperiod"]).drop_duplicates(["ticker", "dimension", "reportperiod"])
    pos = lambda s: s.where(s > 0)  # noqa: E731
    q = f[f["dimension"] == "ARQ"].copy()
    g = q.groupby("ticker")
    year = _year_apart(q["reportperiod"], g["reportperiod"].shift(4))
    q["d"] = (q["netinc"] - g["netinc"].shift(4)).where(year)
    sd = q.groupby("ticker")["d"].transform(lambda s: s.shift(1).rolling(8, min_periods=4).std())
    q["sue"] = q["d"] / pos(sd)
    q["noa"] = ((q["debt"] + q["equity"] - q["cashneq"]) / pos(g["assets"].shift(4))).where(year)
    t = f[f["dimension"] == "ART"].copy()
    g = t.groupby("ticker")
    year = _year_apart(t["reportperiod"], g["reportperiod"].shift(4))
    t["roe"], t["gm"] = t["netinc"] / pos(t["equity"]), t["gp"] / pos(t["revenue"])
    g = t.groupby("ticker")
    t["roe_chg"] = (t["roe"] - g["roe"].shift(4)).where(year)
    t["gm_chg"] = (t["gm"] - g["gm"].shift(4)).where(year)
    t["div_ttm"] = (-t["ncfdiv"]).clip(lower=0)
    a = F._asof(keys, q, "datekey", ["sue", "noa"], 200)
    b = F._asof(keys, t, "datekey", ["roe_chg", "gm_chg", "div_ttm"], 400)
    out = pd.concat([a, b], axis=1)
    out["div_yield"] = out.pop("div_ttm") / (mcap * 1e6)
    return out.astype(float)


# ── operators of this stage ──────────────────────────────────────────────────

def forward_fill(raw: pd.DataFrame, cols: list[str], steps: int = FFILL_STEPS) -> pd.DataFrame:
    """Each stock's last known value, for at most `steps` of its own later samples (rows in date order)."""
    return raw.groupby("ticker", sort=False)[cols].ffill(limit=steps)


def low_coverage(values: pd.DataFrame, dates: pd.Series, share: float = COVERAGE) -> pd.DataFrame:
    """True where fewer than `share` of the date's rows have a value (the source has not started yet)."""
    cov = values.notna().groupby(dates.to_numpy()).mean()
    return pd.DataFrame(cov.reindex(dates.to_numpy()).to_numpy() < share, index=values.index, columns=values.columns)


def ranked(values: pd.DataFrame, dates: pd.Series, sectors: pd.Series, prefix: str, flags: bool = True,
           within: bool = True) -> pd.DataFrame:
    """Global rank (`g6_`), within-sector rank (`s6_`) and missing flag (`n6_`) per column; every output of a
    low-coverage (date, column) is 0."""
    cols = list(values.columns)
    frame = pd.concat([pd.DataFrame({"date": dates.to_numpy(), "sector": sectors.to_numpy()}, index=values.index), values], axis=1)
    low = low_coverage(values, dates)
    out = {}
    g = op.cross_sectional(frame, cols)
    s = op.cross_sectional(frame, cols, group="sector") if within else None
    for c in cols:
        m = low[c].to_numpy()
        out[f"{prefix}g_{c}"] = np.where(m, 0.0, g[c].to_numpy())
        if flags:
            out[f"{prefix}n_{c}"] = np.where(m, 0, g[f"{c}_na"].to_numpy())
        if within:
            out[f"{prefix}s_{c}"] = np.where(m, 0.0, s[c].to_numpy())
    return pd.DataFrame(out, index=values.index).astype("float32")


def industry_level(raw: pd.DataFrame) -> pd.DataFrame:
    """Member-cap-weighted industry values (industries with ≥ IND_MIN members that date)."""
    w = np.exp(raw["log_mcap"])
    keys = [raw["date"], raw["industry"]]
    size = raw.groupby(["date", "industry"])["ticker"].transform("size")
    out = {}
    for name, c in IND_LEVEL.items():
        x = raw[c]
        ok = x.notna() & w.notna()
        num = (x * w).where(ok).groupby(keys).transform("sum")
        den = w.where(ok).groupby(keys).transform("sum")
        out[name] = (num / den.where(den > 0)).where(size >= IND_MIN)
    return pd.DataFrame(out, index=raw.index)


def rank_change_flags(raw: pd.DataFrame) -> pd.DataFrame:
    """1 where store v1's rank change had no sample TS_LAG steps earlier (v1 filled those with 0)."""
    order = sorted(raw["date"].unique())
    k = raw["date"].map({d: n for n, d in enumerate(order)})
    have = set(zip(raw["ticker"], k))
    miss = [(t, kk - F.TS_LAG) not in have for t, kk in zip(raw["ticker"], k)]
    return pd.DataFrame({f"n_chg_{c}": np.asarray(miss, dtype="float32") for c in F.TS_SOURCES}, index=raw.index)


def factor_momentum(raw: pd.DataFrame, p: sr.Prices) -> pd.DataFrame:
    """Per sample date: return of each factor's top − bottom tercile (equal weight, formed on the last sample
    date at least `lag` sessions earlier) from that date's close to today's close."""
    last = pd.DataFrame(p.closeadj).ffill().to_numpy()
    dates = sorted(raw["date"].unique())
    i_of = raw.groupby("date")["i"].first().reindex(dates).to_numpy()
    j = raw["ticker"].map(p.col).to_numpy()
    by_date = {d: idx for d, idx in raw.groupby("date").indices.items()}
    out = pd.DataFrame(index=pd.DatetimeIndex(dates))
    for name, (col, sign) in FACTORS.items():
        x = raw[col].to_numpy(dtype=float) * sign
        for lag in FM_LAGS:
            vals = np.full(len(dates), np.nan)
            for k, d in enumerate(dates):
                s = int(np.searchsorted(i_of, i_of[k] - lag, side="right")) - 1
                if s < 0:
                    continue
                idx = by_date[dates[s]]
                v, jj = x[idx], j[idx]
                ok = np.isfinite(v)
                if ok.sum() < 30:
                    continue
                v, jj = v[ok], jj[ok]
                with np.errstate(all="ignore"):
                    ret = last[i_of[k], jj] / last[i_of[s], jj] - 1
                lo, hi = np.nanquantile(v, [1 / 3, 2 / 3])
                vals[k] = np.nanmean(ret[v >= hi]) - np.nanmean(ret[v <= lo])
            out[f"fm_{name}_{lag}"] = vals
    return out


def sector_table(raw: pd.DataFrame) -> pd.DataFrame:
    """One row per (date, sector) of the top-1000 members, features ranked across the date's sectors, and
    the members' mean main labels."""
    d = raw[raw["sector"].notna()].copy()
    d["w"] = np.exp(d["log_mcap"])
    g = d.groupby(["date", "sector"])

    def cw(c):
        ok = d[c].notna() & d["w"].notna()
        return (d[c] * d["w"]).where(ok).groupby([d["date"], d["sector"]]).sum() / d["w"].where(ok).groupby([d["date"], d["sector"]]).sum()
    above = (d["gap_sma200"] > 0).astype(float).where(d["gap_sma200"].notna())
    t = pd.DataFrame({"s_ret21": cw("ret_21"), "s_ret63": cw("ret_63"), "s_ret126": cw("ret_126"), "s_ret252": cw("ret_252"),
                      "s_mom": cw("mom_12_1"), "s_vol63": cw("vol_63"), "s_dvr": cw("dv_ratio"),
                      "s_above200": above.groupby([d["date"], d["sector"]]).mean(),
                      "s_bm": g["bm"].median(), "s_ep": g["ep"].median(), "s_gp": g["gp_assets"].median(),
                      "s_sales": g["sales_growth"].median(), "s_ear": g["ear"].median(), "s_ret63_std": g["ret_63"].std(),
                      "s_etf_rel63": g["sector_rel_63"].first(), "members": g.size(), "i": g["i"].first()})
    for n in F.HORIZONS:
        t[f"y{n}_sector"] = g[f"y{n}"].mean()
        t[f"y{n}_end"] = g[f"y{n}_end"].first()
    t = t.reset_index()
    for c in SECTOR_FEATURES:
        t[c] = t.groupby("date")[c].transform(op.rank_pm1).fillna(0.0)
    return t


# ── build and load ────────────────────────────────────────────────────────────

def build(fred: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    raw = pd.read_parquet(F.OUT / "raw_v1.parquet")
    raw["date"] = pd.to_datetime(raw["date"])
    raw = raw[raw["date"] <= pd.Timestamp(UNTIL)].reset_index(drop=True)
    for n in F.HORIZONS:
        raw[f"y{n}_end"] = pd.to_datetime(raw[f"y{n}_end"])
    dates = sr.trading_days()
    dates = dates[dates <= pd.Timestamp(UNTIL)]
    names = sorted(raw["ticker"].unique())
    p = sr.prices(names, dates)
    j = raw["ticker"].map(p.col)
    if j.isna().any():
        raise ValueError(f"{int(j.isna().sum())} rows without prices")
    rows, cols = raw["i"].to_numpy(), j.astype(int).to_numpy()
    mcap = np.exp(raw["log_mcap"]).to_numpy()
    raw = pd.concat([raw, price_extras(p, rows, cols, mcap),
                     fundamental_extras(raw[["date", "ticker"]], names, pd.Series(mcap, index=raw.index))], axis=1)
    raw[FFILL] = forward_fill(raw, FFILL)
    parts = [raw[["date", "ticker"]]]
    parts.append(ranked(raw[STOCK], raw["date"], raw["sector"], "6"))
    low = low_coverage(raw[STOCK], raw["date"])
    parts.append(pd.DataFrame({f"6f_{k}": np.where(low[c].to_numpy(), 0, fn(raw[c]).to_numpy()) for k, (c, _, fn) in SIGN_FLAGS.items()},
                              index=raw.index).astype("float32"))
    own = stage5.own_history(raw)
    parts.append(ranked(own, raw["date"], raw["sector"], "6o", within=False))
    ind = pd.concat([stage5.industry_relative(raw), industry_level(raw)], axis=1)
    rel = ranked(ind[[c for c in ind.columns if c.endswith("_indrel")]], raw["date"], raw["sector"], "6i", within=False)
    lvl = ranked(ind[list(IND_LEVEL)], raw["date"], raw["sector"], "6i")
    parts += [rel, lvl[[c for c in lvl.columns if c.startswith("6is_") or c.startswith("6in_")]]]
    parts.append(rank_change_flags(raw).add_prefix("6"))
    ctx = stage5.dispersion(raw)
    mf = stage5.macro_frame(fred, dates)
    for c in stage5.MACRO:
        ctx[c] = raw["date"].map(mf[c]).to_numpy()
    fm = factor_momentum(raw, p)
    for c in fm.columns:
        ctx[c] = raw["date"].map(fm[c]).to_numpy()
    parts.append(op.context(pd.concat([raw[["date"]], ctx], axis=1), list(ctx.columns)).add_prefix("6c_"))
    out = pd.concat(parts, axis=1)
    out["fut_vol21"] = stage5.future_vol(p, rows, cols).astype("float32")
    return out, sector_table(raw)


def load(n: int | None) -> pd.DataFrame:
    """Store-v1 rows up to UNTIL joined with the stage-6 store; rows whose horizon-n label (y21 if None)
    exists and ends by UNTIL."""
    df = pd.read_parquet(F.OUT / "features_v1.parquet")
    df["date"] = pd.to_datetime(df["date"])
    df = df[df["date"] <= pd.Timestamp(UNTIL)].reset_index(drop=True)
    for k in F.HORIZONS:
        df[f"y{k}_end"] = pd.to_datetime(df[f"y{k}_end"])
    ext = pd.read_parquet(F.OUT / f"{STORE}.parquet")
    if not (len(ext) == len(df) and (ext["ticker"].to_numpy() == df["ticker"].to_numpy()).all()):
        raise ValueError("stage-6 store is not aligned with store v1")
    df = pd.concat([df, ext.drop(columns=["date", "ticker"])], axis=1)
    h = n or 21
    keep = df[f"y{h}"].notna() & (df[f"y{h}_end"] <= pd.Timestamp(UNTIL))
    return df[keep].reset_index(drop=True)


def context_frame(df: pd.DataFrame) -> pd.DataFrame:
    c = pd.DataFrame({k: df[k] for k in CTX_GROUPS["x_market"][:6]}, index=df.index)
    c["regime_s"] = (df["regime"] - 1).clip(-1, 1)
    c["vol_state_s"] = (df["vol_state"] * 2 - 1).clip(-1, 1)
    for k in CTX[8:]:
        c[k] = df[k if k.startswith("ctx_") else f"6c_{k}"]
    return c[CTX].astype("float32")


# ── design: base columns by group, crosses built per chunk ────────────────────

class Design:
    """Stock-layer inputs. Base columns (by group) are materialised once; the 1404 crosses
    (context × within-sector rank) are built per row chunk."""

    def __init__(self, df: pd.DataFrame):
        cols, groups = [], []
        for fam, feats in FAMILIES.items():
            for c in feats:
                for pre in ("6g_", "6s_", "6n_"):
                    cols.append(pre + c)
                    groups.append(fam)
        for k, (_, fam, _) in SIGN_FLAGS.items():
            cols.append(f"6f_{k}")
            groups.append(fam)
        own = [c for c in df.columns if c.startswith("6og_") or c.startswith("6on_")]
        ind = [c for c in df.columns if c.startswith("6ig_") or c.startswith("6in_") or c.startswith("6is_")]
        misc = [f"{c}_chg" for c in F.TS_SOURCES] + [f"6n_chg_{c}" for c in F.TS_SOURCES] + F.SIGNALS
        cols += own + ind + misc
        groups += ["own"] * len(own) + ["industry"] * len(ind) + ["misc"] * len(misc)
        base = df[cols].astype("float32")
        base["size_bucket"] = (df["size_bucket"] / 9).astype("float32")
        self.sectors = sorted(df["sector"].dropna().unique())
        oh = op.one_hot(df["sector"], "sector", self.sectors).astype("float32")
        self.base = np.concatenate([base.to_numpy(), oh.to_numpy()], axis=1)
        groups += ["misc"] * (1 + oh.shape[1])
        self.base_names = list(base.columns) + list(oh.columns)
        self.S = df[[f"6s_{c}" for c in STOCK]].to_numpy(dtype="float32")
        self.C = context_frame(df).to_numpy()
        cross_groups = [g for g, cs in CTX_GROUPS.items() for _ in cs for _ in STOCK]
        self.names = self.base_names + [f"{c}_x_{s}" for c in CTX for s in STOCK]
        self.groups = np.array(groups + cross_groups)
        self.p = len(self.names)
        assert len(self.groups) == self.p == self.base.shape[1] + self.C.shape[1] * self.S.shape[1]

    def rows(self, idx: np.ndarray) -> np.ndarray:
        cross = (self.C[idx][:, :, None] * self.S[idx][:, None, :]).reshape(len(idx), -1)
        return np.concatenate([self.base[idx], cross], axis=1).astype(np.float64)

    def numeric(self) -> np.ndarray:
        """Network inputs: base columns without the sector one-hot and the size bucket (both embedded)."""
        return self.base[:, :len(self.base_names) - len(self.sectors) - 1]


# ── ridge engine on running Gram sums ─────────────────────────────────────────

class Gram:
    def __init__(self, p: int):
        self.n, self.sx, self.G, self.sy, self.b = 0, np.zeros(p), np.zeros((p, p)), 0.0, np.zeros(p)

    def add(self, X: np.ndarray, t: np.ndarray) -> None:
        self.n += len(X)
        self.sx += X.sum(0)
        self.G += X.T @ X
        self.sy += float(t.sum())
        self.b += X.T @ t

    def standardized(self):
        mu, ybar = self.sx / self.n, self.sy / self.n
        Gc = self.G - self.n * np.outer(mu, mu)
        var = np.diag(Gc) / self.n
        const = var < 1e-12
        sd = np.where(const, 1.0, np.sqrt(np.clip(var, 1e-12, None)))
        bc = self.b - self.n * mu * ybar
        return Gc / np.outer(sd, sd), bc / sd, mu, sd, const


def solve(Gz: np.ndarray, bz: np.ndarray, pen: np.ndarray, const: np.ndarray) -> np.ndarray:
    pen = np.where(const, OFF, pen)
    return np.linalg.solve(Gz + np.diag(pen), bz)


def fast_ic(codes: np.ndarray, ry: np.ndarray, s: np.ndarray, min_rows: int = 21) -> float:
    """Mean over dates of the Spearman correlation of s with y (ry = y's per-date rank)."""
    rs = pd.Series(s).groupby(codes).rank().to_numpy()
    cnt = np.bincount(codes)
    ms, my = np.bincount(codes, rs) / np.maximum(cnt, 1), np.bincount(codes, ry) / np.maximum(cnt, 1)
    ds, dy = rs - ms[codes], ry - my[codes]
    with np.errstate(all="ignore"):
        ic = np.bincount(codes, ds * dy) / np.sqrt(np.bincount(codes, ds * ds) * np.bincount(codes, dy * dy))
    ic = ic[(cnt >= min_rows) & np.isfinite(ic)]
    return float(ic.mean()) if len(ic) else -np.inf


def coordinate_search(score, groups: np.ndarray, order: tuple, start: float, passes: int = 2) -> tuple[dict, float]:
    """Per-group penalties: all groups at `start`, then each group in turn over GRID ∪ {OFF}, `passes` times."""
    pen = {g: start for g in order}
    best = score(pen)
    for _ in range(passes):
        for g in order:
            for a in (*GRID, OFF):
                if a == pen[g]:
                    continue
                trial = {**pen, g: a}
                ic = score(trial)
                if ic > best:
                    pen, best = trial, ic
    return pen, best


def _pen_vector(pen: dict, groups: np.ndarray) -> np.ndarray:
    return np.array([pen[g] for g in groups], dtype=float)


def _months(df: pd.DataFrame, lo: str) -> list:
    return sorted(df.loc[df["date"] >= pd.Timestamp(lo), "date"].dt.to_period("M").unique())


def _split(df: pd.DataFrame, cut_i: int, n: int, months: int = INNER_MONTHS):
    """Rows sorted by i: the training prefix (i + n ≤ cut_i, i.e. labels end ≥ n sessions before the version's
    first sample at cut_i + n), its fit prefix (≤ inner − 1.5 n days) and where the inner validation starts."""
    i = df["i"].to_numpy()
    n_train = int(np.searchsorted(i, cut_i - n, side="right"))
    if n_train == 0:
        return 0, 0, 0
    inner = df["date"].iloc[n_train - 1] - pd.DateOffset(months=months)
    n_fit = int(np.searchsorted(df["date"].to_numpy(), np.datetime64(inner - pd.Timedelta(days=int(n * 1.5))), side="right"))
    v0 = int(np.searchsorted(df["date"].to_numpy(), np.datetime64(inner), side="right"))
    return n_train, n_fit, v0


def run_stock(n: int, chunk: int = 20_000) -> dict:
    """M1 (one α), M2 (grouped penalties) and M3 (stack) share the running Gram sums."""
    df = load(n).sort_values(["i", "ticker"]).reset_index(drop=True)
    D = Design(df)
    t = B.target(df, f"y{n}_sec").fillna(0.0).to_numpy()
    y = df[f"y{n}"].to_numpy()
    order = GROUPS
    gidx = {g: np.flatnonzero(D.groups == g) for g in order}
    acc_train, acc_fit = Gram(D.p), Gram(D.p)
    done_train = done_fit = 0

    def advance(acc, done, upto):
        for k in range(done, upto, chunk):
            idx = np.arange(k, min(k + chunk, upto))
            acc.add(D.rows(idx), t[idx])
        return max(done, upto)

    preds = {k: np.full(len(df), np.nan) for k in ("ridge_one", "ridge_group", "stack")}
    log, sel = [], None
    per = df["date"].dt.to_period("M")
    i_first, month_rows = df.groupby(per)["i"].min(), df.groupby(per).indices
    for m in _months(df, PRED_FROM):
        rows = month_rows[m]
        cut = int(i_first[m]) - n
        n_train, n_fit, v0 = _split(df, cut, n)
        if n_train < 10_000:
            continue
        if sel is None or m.month == 1:
            done_fit = advance(acc_fit, done_fit, n_fit)
            Gz, bz, mu, sd, const = acc_fit.standardized()
            vi = np.arange(v0, n_train)
            Zv = (D.rows(vi) - mu) / sd
            codes = pd.factorize(df["date"].iloc[vi])[0]
            ry = pd.Series(y[vi]).groupby(codes).rank().to_numpy()
            vic = lambda w: fast_ic(codes, ry, Zv @ w)  # noqa: E731
            one = {a: vic(solve(Gz, bz, np.full(D.p, a), const)) for a in GRID}
            a1 = max(one, key=one.get)
            pen, best = coordinate_search(lambda pn: vic(solve(Gz, bz, _pen_vector(pn, D.groups), const)), D.groups, order, a1)
            # stack: one ridge per group, combined on the inner validation
            ga, gs = {}, []
            for g in order:
                ix = gidx[g]
                fits = {a: np.linalg.solve(Gz[np.ix_(ix, ix)] + np.diag(np.where(const[ix], OFF, a)), bz[ix]) for a in GRID}
                ics = {a: fast_ic(codes, ry, Zv[:, ix] @ w) for a, w in fits.items()}
                ga[g] = max(ics, key=ics.get)
                gs.append(Zv[:, ix] @ fits[ga[g]])
            Sv = np.column_stack(gs)
            s_mu, s_sd = Sv.mean(0), Sv.std(0)
            s_sd[s_sd == 0] = 1.0
            Zs = (Sv - s_mu) / s_sd
            tv = t[vi] - t[vi].mean()
            comb = np.linalg.solve(Zs.T @ Zs + np.eye(len(order)), Zs.T @ tv)
            sel = {"a1": a1, "pen": pen, "ga": ga, "comb": comb, "s_mu": s_mu, "s_sd": s_sd}
            log.append({"month": str(m), "ic_one": round(one[a1], 5), "a1": a1, "ic_group": round(best, 5),
                        "pen": {g: (None if v >= OFF else v) for g, v in pen.items()},
                        "stack_alpha": ga, "stack_weights": dict(zip(order, np.round(comb, 4).tolist()))})
            del Zv
        done_train = advance(acc_train, done_train, n_train)
        Gz, bz, mu, sd, const = acc_train.standardized()
        Zr = (D.rows(rows) - mu) / sd
        preds["ridge_one"][rows] = Zr @ solve(Gz, bz, np.full(D.p, sel["a1"]), const)
        preds["ridge_group"][rows] = Zr @ solve(Gz, bz, _pen_vector(sel["pen"], D.groups), const)
        gs = []
        for g in order:
            ix = gidx[g]
            w = np.linalg.solve(Gz[np.ix_(ix, ix)] + np.diag(np.where(const[ix], OFF, sel["ga"][g])), bz[ix])
            gs.append(Zr[:, ix] @ w)
        preds["stack"][rows] = ((np.column_stack(gs) - sel["s_mu"]) / sel["s_sd"]) @ sel["comb"]
    keep = df["date"] >= pd.Timestamp(PRED_FROM)
    for k, v in preds.items():
        _save(k, n, df[keep], v[keep.to_numpy()])
    info = {"inputs": D.names, "groups": D.groups.tolist(), "selections": log}
    (F.OUT / f"trials_v6_stock_y{n}.json").write_text(json.dumps(info, indent=1))
    return info


def preds_path(key: str, n: int):
    return F.OUT / f"preds_v6_{key}_y{n}.parquet"


def _save(key: str, n: int, rows: pd.DataFrame, score) -> None:
    pd.DataFrame({"date": rows["date"].to_numpy(), "ticker": rows["ticker"].to_numpy(),
                  "score": np.asarray(score, dtype=float)}).to_parquet(preds_path(key, n), index=False)


def run_baseline(n: int) -> None:
    """M0: ridge_sec_u1000 (stage-1 design, ridge v2, sector-neutral label) on the new discovery period."""
    df = pd.read_parquet(F.OUT / "features_v1.parquet")
    df["date"] = pd.to_datetime(df["date"])
    df = df[df["date"] <= pd.Timestamp(UNTIL)].reset_index(drop=True)
    for k in F.HORIZONS:
        df[f"y{k}_end"] = pd.to_datetime(df[f"y{k}_end"])
    scored = B.walk_forward(df, n, alphas=GRID, name="v6_baseline", window=DISCOVERY, label=f"y{n}_sec")
    _save("baseline", n, scored, scored["ridge"])


# ── sector layer and the two-layer score ──────────────────────────────────────

def sector_design(t: pd.DataFrame, ctx: pd.DataFrame) -> np.ndarray:
    c = ctx.reindex(t["date"]).to_numpy(dtype=float)
    s = t[SECTOR_X].to_numpy(dtype=float)
    cross = (c[:, :, None] * s[:, None, :]).reshape(len(t), -1)
    return np.concatenate([t[SECTOR_FEATURES].to_numpy(dtype=float), np.nan_to_num(cross)], axis=1)


def run_sector(n: int) -> dict:
    """M4: per (date, sector) ridge on the rank of the sector's mean main label, monthly versions."""
    t = pd.read_parquet(F.OUT / f"{SECTOR_STORE}.parquet")
    t["date"], t[f"y{n}_end"] = pd.to_datetime(t["date"]), pd.to_datetime(t[f"y{n}_end"])
    t = t[t[f"y{n}_sector"].notna() & (t[f"y{n}_end"] <= pd.Timestamp(UNTIL))].sort_values(["i", "sector"]).reset_index(drop=True)
    stock = load(n)
    ctx = context_frame(stock).groupby(stock["date"].to_numpy()).first()
    X = sector_design(t, ctx)
    tgt = t.groupby("date")[f"y{n}_sector"].transform(op.rank_pm1).to_numpy()
    y = t[f"y{n}_sector"].to_numpy()
    pred, log = np.full(len(t), np.nan), []
    per = t["date"].dt.to_period("M")
    i_first, month_rows = t.groupby(per)["i"].min(), t.groupby(per).indices
    for m in _months(t, PRED_FROM):
        rows = month_rows[m]
        n_train, n_fit, v0 = _split(t, int(i_first[m]) - n, n)
        if n_train < 500:
            continue
        acc = Gram(X.shape[1])
        acc.add(X[:n_fit], tgt[:n_fit])
        Gz, bz, mu, sd, const = acc.standardized()
        vi = np.arange(v0, n_train)
        codes = pd.factorize(t["date"].iloc[vi])[0]
        ry = pd.Series(y[vi]).groupby(codes).rank().to_numpy()
        Zv = (X[vi] - mu) / sd
        ics = {a: fast_ic(codes, ry, Zv @ solve(Gz, bz, np.full(len(bz), a), const), min_rows=5) for a in GRID}
        a = max(ics, key=ics.get)
        acc = Gram(X.shape[1])
        acc.add(X[:n_train], tgt[:n_train])
        Gz, bz, mu, sd, const = acc.standardized()
        pred[rows] = ((X[rows] - mu) / sd) @ solve(Gz, bz, np.full(len(bz), a), const)
        log.append({"month": str(m), "alpha": a, "val_ic": round(ics[a], 5)})
    out = t[["date", "sector", "i", f"y{n}_sector", f"y{n}_end"]].copy()
    out["score"] = pred
    out.to_parquet(F.OUT / f"sector_v6_scores_y{n}.parquet", index=False)
    (F.OUT / f"trials_v6_sector_y{n}.json").write_text(json.dumps(log, indent=1))
    return {"versions": len(log)}


def _zscore(s: pd.Series, dates: pd.Series) -> pd.Series:
    g = s.groupby(dates.to_numpy())
    return (s - g.transform("mean")) / g.transform("std").replace(0, np.nan)


def choose_beta(frame: pd.DataFrame, n: int) -> pd.DataFrame:
    """frame: date, i, y, end_i, zs (stock z), zg (sector z). Each January β from the last INNER_MONTHS of rows
    whose labels end by the version's cut; β = 0 with < 12 months. Returns the two-layer score per row."""
    frame = frame.sort_values("i").reset_index(drop=True)
    out = pd.Series(np.nan, index=frame.index)
    per = frame["date"].dt.to_period("M")
    beta, log = 0.0, []
    for m, rows in sorted(frame.groupby(per).indices.items()):
        if m.month == 1 or not log:
            cut = int(frame["i"].iloc[rows].min()) - n
            past = frame[(frame["end_i"] <= cut) & (frame["date"] > frame["date"].iloc[rows[0]] - pd.DateOffset(months=INNER_MONTHS))]
            past = past[past["zs"].notna() & past["zg"].notna()]
            if past["date"].dt.to_period("M").nunique() >= 12:
                codes = pd.factorize(past["date"])[0]
                ry = past.groupby(codes)["y"].rank().to_numpy()
                ics = {b: fast_ic(codes, ry, (past["zs"] + b * past["zg"]).to_numpy()) for b in BETAS}
                beta = max(ics, key=ics.get)
            log.append({"month": str(m), "beta": beta})
        out.iloc[rows] = (frame["zs"].iloc[rows] + beta * frame["zg"].fillna(0.0).iloc[rows]).to_numpy()
    frame["score"] = out
    frame.attrs["beta_log"] = log
    return frame


def run_two_layer(n: int) -> dict:
    df = load(n)
    stock = pd.read_parquet(preds_path("ridge_group", n))
    sec = pd.read_parquet(F.OUT / f"sector_v6_scores_y{n}.parquet")[["date", "sector", "score"]].rename(columns={"score": "sector_score"})
    f = df[["date", "ticker", "i", "sector", f"y{n}"]].merge(stock, on=["date", "ticker"], how="inner")
    f = f.merge(sec, on=["date", "sector"], how="left")
    f["zs"] = _zscore(f["score"], f["date"])
    f["zg"] = _zscore(f["sector_score"], f["date"])
    f["end_i"] = f["i"] + n
    f = f.rename(columns={f"y{n}": "y"})
    res = choose_beta(f[["date", "ticker", "i", "y", "end_i", "zs", "zg"]], n)
    _save("two_layer", n, res, res["score"])
    (F.OUT / f"trials_v6_two_layer_y{n}.json").write_text(json.dumps(res.attrs["beta_log"], indent=1))
    return {"betas": res.attrs["beta_log"]}


# ── tabular network ensemble (context only through a gate) ────────────────────

SEEDS = (7, 8, 9, 10, 11)
DNN_VARIANTS = {"single": (("y21_sec",), 21), "multi": (("y21_sec", "y63_sec", "fut_vol21"), 63)}
CATS6 = {"sector": 4, "size_bucket": 4}
DNN_INNER_MONTHS = 12


def make_net(n_num: int, n_ctx: int, sizes: list[int], heads: int):
    import torch
    from torch import nn

    class PLR(nn.Module):
        def __init__(self):
            super().__init__()
            self.freq = nn.Parameter(torch.randn(n_num, stage5.PLR_K))
            self.w = nn.Parameter(torch.randn(n_num, 2 * stage5.PLR_K, stage5.PLR_D) / np.sqrt(2 * stage5.PLR_K))
            self.b = nn.Parameter(torch.zeros(n_num, stage5.PLR_D))

        def forward(self, x):
            z = 2 * np.pi * self.freq[None] * x[..., None]
            z = torch.cat([torch.sin(z), torch.cos(z)], dim=-1)
            return torch.relu(torch.einsum("bfk,fkd->bfd", z, self.w) + self.b).flatten(1)

    class Net(nn.Module):
        def __init__(self):
            super().__init__()
            self.plr = PLR()
            self.emb = nn.ModuleList([nn.Embedding(s, d) for s, d in zip(sizes, CATS6.values())])
            d_in = n_num * stage5.PLR_D + sum(CATS6.values())
            layers = []
            for h in stage5.HIDDEN:
                layers += [nn.Linear(d_in, h), nn.GELU(), nn.Dropout(0.1)]
                d_in = h
            self.body = nn.Sequential(*layers)
            self.gate = nn.Linear(n_ctx, d_in)
            nn.init.zeros_(self.gate.weight)
            nn.init.constant_(self.gate.bias, 2.0)
            self.out = nn.Linear(d_in, heads)

        def forward(self, x, c, ctx):
            e = [m(c[:, k]) for k, m in enumerate(self.emb)]
            h = self.body(torch.cat([self.plr(x), *e], dim=1))
            return self.out(h * torch.sigmoid(self.gate(ctx)))

    return Net()


def _net_predict(model, X, C, Q, rows, dev):
    import torch
    model.eval()
    out = []
    with torch.no_grad():
        for k in range(0, len(rows), 8 * stage5.BATCH):
            b = rows[k:k + 8 * stage5.BATCH]
            out.append(model(torch.from_numpy(X[b]).to(dev), torch.from_numpy(C[b]).to(dev),
                             torch.from_numpy(Q[b]).to(dev)).float().cpu().numpy())
    return np.concatenate(out)


def _net_train(model, X, C, Q, T, fit_i, val_i, frames, epochs, seed, dev):
    import torch
    torch.manual_seed(seed)
    model.to(dev)
    opt = torch.optim.AdamW(model.parameters(), lr=1e-3, weight_decay=1e-4)
    rng = np.random.default_rng(seed)
    best, state, stale, log = -np.inf, None, 0, []
    for e in range(epochs):
        model.train()
        pick = fit_i[rng.choice(len(fit_i), min(stage5.EPOCH_ROWS, len(fit_i)), replace=False)]
        for k in range(0, len(pick), stage5.BATCH):
            b = pick[k:k + stage5.BATCH]
            loss = stage5.masked_mse(model(torch.from_numpy(X[b]).to(dev), torch.from_numpy(C[b]).to(dev),
                                           torch.from_numpy(Q[b]).to(dev)), torch.from_numpy(T[b]).to(dev))
            opt.zero_grad()
            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            opt.step()
        p = _net_predict(model, X, C, Q, val_i, dev)
        ic = float(np.mean([fast_ic(codes, ry, p[:, h]) for h, codes, ry in frames]))
        log.append({"epoch": e, "val_ic": round(ic, 5)})
        if ic > best:
            best, state, stale = ic, {k: v.detach().clone() for k, v in model.state_dict().items()}, 0
        else:
            stale += 1
            if stale >= stage5.PATIENCE:
                break
    model.load_state_dict(state)
    return model, log


def run_dnn(variant: str) -> dict:
    import torch
    from scout.ml.sequence import device
    heads, purge = DNN_VARIANTS[variant]
    dev = device()
    df = load(63 if variant == "multi" else 21).sort_values(["i", "ticker"]).reset_index(drop=True)
    D = Design(df)
    X = np.ascontiguousarray(D.numeric(), dtype=np.float32)
    Q = np.ascontiguousarray(D.C, dtype=np.float32)
    cats = [df["sector"].map({s: k + 1 for k, s in enumerate(D.sectors)}).fillna(0).astype(int).to_numpy(),
            (df["size_bucket"].astype(int) + 1).clip(0, 10).to_numpy()]
    C = np.stack(cats, axis=1).astype(np.int64)
    sizes = [len(D.sectors) + 1, 11]
    T = np.stack([B.target(df, h).to_numpy() if h != "fut_vol21" else df.groupby("date")[h].transform(op.rank_pm1).to_numpy()
                  for h in heads], axis=1).astype(np.float32)
    horizons = [int(h[1:3]) for h in heads if h != "fut_vol21"]
    score_rows = np.flatnonzero((df["date"] >= pd.Timestamp(DISCOVERY[0])).to_numpy())
    years = sorted(df["date"].iloc[score_rows].dt.year.unique())
    pred = np.zeros((len(df), len(horizons)), dtype=np.float64)
    log = []
    for seed in SEEDS:
        model = None
        for year in years:
            rows = score_rows[df["date"].iloc[score_rows].dt.year.to_numpy() == year]
            n_train, n_fit, v0 = _split(df, int(df["i"].iloc[rows].min()) - purge, purge, DNN_INNER_MONTHS)
            fit_i, val_i = np.arange(n_fit), np.arange(v0, n_train)
            vcodes = pd.factorize(df["date"].iloc[val_i])[0]
            frames = [(k, vcodes, pd.Series(df[f"y{h}"].iloc[val_i].to_numpy()).groupby(vcodes).rank().to_numpy())
                      for k, h in enumerate(horizons)]
            first = model is None
            model = make_net(X.shape[1], Q.shape[1], sizes, len(heads)) if first else model
            model, ep = _net_train(model, X, C, Q, T, fit_i, val_i, frames, stage5.FIRST_EPOCHS if first else stage5.LATER_EPOCHS,
                                   seed * 1000 + int(year), dev)
            p = _net_predict(model, X, C, Q, rows, dev)
            for k in range(len(horizons)):
                pred[rows, k] += pd.Series(p[:, k]).groupby(df["date"].iloc[rows].to_numpy()).rank(pct=True).to_numpy() / len(SEEDS)
            torch.save(model.state_dict(), B.CKPT / f"v6_dnn_{variant}_s{seed}_{year}.pt")
            log.append({"seed": seed, "year": int(year), "epochs": ep})
    for k, h in enumerate(horizons):
        rows = df.iloc[score_rows]
        ok = (rows[f"y{h}"].notna() & (rows[f"y{h}_end"] <= pd.Timestamp(UNTIL))).to_numpy()
        _save(f"dnn_{variant}", h, rows[ok], pred[score_rows[ok], k])
    (F.OUT / f"trials_v6_dnn_{variant}.json").write_text(json.dumps({"inputs": D.base_names, "training": log}, indent=1))
    return {"versions": len(log)}


# ── report ────────────────────────────────────────────────────────────────────

def keys(n: int) -> list[str]:
    base = ["baseline", "ridge_one", "ridge_group", "stack", "two_layer", "dnn_multi"]
    return base + (["dnn_single"] if n == 21 else [])


COMPARE = {  # score → comparators; the first one is the pre-registered decision (A, B, C) where marked
    "ridge_group": ["baseline"], "two_layer": ["ridge_group", "baseline"], "dnn_multi": ["ridge_group", "baseline"],
    "ridge_one": ["baseline"], "stack": ["ridge_group"], "dnn_single": ["dnn_multi"],
}
DECISIONS = {"A": ("ridge_group", "baseline"), "B": ("two_layer", "ridge_group"), "C": ("dnn_multi", "ridge_group")}


def increment(scored: pd.DataFrame, n: int, score: str, base: str) -> dict:
    return stage4.increment(scored, n, score, base)


def tilt_ir(scores: dict[str, pd.DataFrame]) -> dict:
    df, p, R, spy, dates = tilt.load(DISCOVERY[0], DISCOVERY[1])
    reg, (step, band) = tilt.regimes(df), tilt.VARIANTS["m_band"]
    out = {}
    for key, s in scores.items():
        d = df.merge(s.rename(columns={"score": key}), on=["date", "ticker"], how="left")
        st = tilt.stats(tilt.simulate(R, tilt.decisions(d, key, step), len(dates) - 1, band), dates, spy, reg)
        out[key] = {"ir": st["all"]["ir"], "t": st["all"]["t"], "ann_active": st["all"]["ann_active"],
                    "first": st["first"]["ann_active"], "second": st["second"]["ann_active"],
                    "turnover": st["turnover_one_way_per_year"]}
    return out


def pbo(monthly: pd.DataFrame, blocks: int = 16) -> dict:
    """Probability of backtest overfitting (CSCV, Bailey et al.): months × models of mean IC."""
    m = monthly.dropna()
    parts = np.array_split(np.arange(len(m)), blocks)
    vals = m.to_numpy()
    logits = []
    for ins in itertools.combinations(range(blocks), blocks // 2):
        i_rows = np.concatenate([parts[k] for k in ins])
        o_rows = np.concatenate([parts[k] for k in range(blocks) if k not in ins])
        best = int(np.argmax(vals[i_rows].mean(0)))
        oos = vals[o_rows].mean(0)
        w = (pd.Series(oos).rank().iloc[best]) / (len(oos) + 1)
        logits.append(np.log(w / (1 - w)))
    logits = np.array(logits)
    return {"pbo": round(float((logits <= 0).mean()), 3), "combinations": len(logits), "models": list(m.columns)}


def exposures(scored: pd.DataFrame, df: pd.DataFrame, score: str, cols: list[str], top: int = 20) -> list[dict]:
    d = scored[["date", "ticker", score]].merge(df[["date", "ticker", *cols]], on=["date", "ticker"], how="inner")
    codes = pd.factorize(d["date"])[0]
    mid = d["date"].quantile(0.5)
    rows = []
    s = d[score].to_numpy(dtype=float)
    for c in cols:
        x = d[c].to_numpy(dtype=float)
        cnt = np.bincount(codes)
        ds = s - (np.bincount(codes, s) / cnt)[codes]
        dx = x - (np.bincount(codes, x) / cnt)[codes]
        with np.errstate(all="ignore"):
            r = np.bincount(codes, ds * dx) / np.sqrt(np.bincount(codes, ds * ds) * np.bincount(codes, dx * dx))
        first = d.groupby(codes)["date"].first().to_numpy() < mid
        ok = np.isfinite(r)
        if ok.sum() < 10:
            continue
        rows.append({"feature": c, "mean": round(float(r[ok].mean()), 4), "first": round(float(r[ok & first].mean()), 4),
                     "second": round(float(r[ok & ~first].mean()), 4)})
    rows.sort(key=lambda x: -abs(x["mean"]))
    return rows[:top]


def report(n: int) -> dict:
    df = load(n)
    disc = df[(df["date"] >= pd.Timestamp(DISCOVERY[0]))][["date", "ticker", "regime", f"y{n}"]].reset_index(drop=True)
    scores, scored = {}, disc.copy()
    for k in keys(n):
        path = preds_path(k, n)
        if not path.exists():
            continue
        scores[k] = pd.read_parquet(path)[["date", "ticker", "score"]]
        scored = scored.merge(scores[k].rename(columns={"score": k}), on=["date", "ticker"], how="left")
    metrics = gbdt.evaluate(scored, n, tuple(scores))
    inc = {k: {b: increment(scored, n, k, b) for b in bases if b in scores} for k, bases in COMPARE.items() if k in scores}
    sub = scored[scored["date"] >= pd.Timestamp(SUBPERIOD)]
    inc_sub = {k: {b: increment(sub, n, k, b) for b in bases if b in scores} for k, bases in COMPARE.items() if k in scores}
    decisions = {d: {"score": s, "base": b, "holds": bool(inc.get(s, {}).get(b, {}).get("holds")),
                     "candidate": bool(metrics.get(s, {}).get("candidate"))} for d, (s, b) in DECISIONS.items()}
    sec = pd.read_parquet(F.OUT / f"sector_v6_scores_y{n}.parquet")
    sec["date"] = pd.to_datetime(sec["date"])
    sec = sec[(sec["date"] >= pd.Timestamp(DISCOVERY[0])) & sec["score"].notna()]
    sector_ic = B.summary(sec.groupby("date").apply(lambda g: g["score"].rank().corr(g[f"y{n}_sector"].rank())).dropna(), n)
    monthly = pd.DataFrame({k: B.daily_ic(scored[scored[k].notna()], k, f"y{n}") for k in scores})
    monthly = monthly.groupby(monthly.index.to_period("M")).mean()
    D_cols = [c for c in df.columns if c.startswith("6g_")] + [c for c in df.columns if c.startswith("6s_")]
    res = {"metrics": metrics, "increments": inc, "increments_2008": inc_sub, "decisions": decisions,
           "sector_layer_ic": sector_ic, "tilt": tilt_ir(scores), "pbo": pbo(monthly),
           "t_bar": max(3.0, round(float(_bonferroni(len(scores))), 2)),
           "exposures": {k: exposures(scored, df, k, D_cols) for k in ("ridge_group", "two_layer") if k in scores}}
    (F.OUT / f"results_v6_y{n}.json").write_text(json.dumps(res, ensure_ascii=False, indent=2, default=str))
    return res


def _bonferroni(k: int, alpha: float = 0.05) -> float:
    from statistics import NormalDist
    return NormalDist().inv_cdf(1 - alpha / (2 * k))


def report_text(results: dict) -> str:
    p = lambda x: "—" if x is None else f"{x:+.4f}"  # noqa: E731
    q = lambda x: "—" if x is None else f"{x:+.2%}"  # noqa: E731
    lines = [f"ML 第六阶段（挖掘期 {DISCOVERY[0][:7]} 至 {DISCOVERY[1][:7]}，前 1000；2022 年以后未读）"]
    for n, r in results.items():
        lines.append(f"\n持有 {n} 个交易日：模型 | IC（t） | IC 前半 / 后半 | 前 10% 超额（t） | 倾斜组合 IR（t） | 候选 | 增量（相对 → 均值（t）成立否；2008 年起均值（t））")
        for k, v in r["metrics"].items():
            ic, td, tl = v["ic"], v["top_decile"], r["tilt"][k]
            inc, sub = r["increments"].get(k, {}), r["increments_2008"].get(k, {})
            inc_s = "；".join(f"{b} → {p(s['all']['mean'])}（{s['all']['t_nw']}）{'成立' if s['holds'] else '不成立'}；"
                             f"2008 起 {p(sub[b]['all']['mean'])}（{sub[b]['all']['t_nw']}）" for b, s in inc.items() if s) or "基线"
            lines.append(f"{k} | {p(ic['all']['mean'])}（{ic['all']['t_nw']}） | {p(ic['first']['mean'])} / {p(ic['second']['mean'])} | "
                         f"{q(td['all']['mean'])}（{td['all']['t_nw']}） | {tl['ir']}（{tl['t']}） | {'是' if v['candidate'] else '否'} | {inc_s}")
        s = r["sector_layer_ic"]["all"]
        lines.append(f"板块层（11 个板块间）IC {p(s['mean'])}（{s['t_nw']}）；回测过拟合概率 {r['pbo']['pbo']}；t 门槛 {r['t_bar']}")
        lines.append("判定：" + "；".join(f"{d} {v['score']} 相对 {v['base']} {'成立' if v['holds'] else '不成立'}"
                                        f"{'，是候选' if v['candidate'] else ''}" for d, v in r["decisions"].items()))
    return "\n".join(lines)
