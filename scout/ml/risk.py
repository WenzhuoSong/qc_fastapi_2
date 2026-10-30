"""Risk model (pre-registered in `scout/knowledge/external_specs/ml_risk_v1.yaml`): each stock's next-21-session
volatility and the probability that a buy at the next open falls 20 % within 63 sessions, against simple
baselines, judged on loss and calibration. Discovery 2004–2021; nothing after 2021-12-31 is read.
"""
from __future__ import annotations

import json

import numpy as np
import pandas as pd

from scout import sharadar_research as sr
from scout.ml import baselines as B
from scout.ml import features as F
from scout.ml import operators as op
from scout.ml import stage6

UNTIL, DISCOVERY, SUBPERIOD = stage6.UNTIL, stage6.DISCOVERY, stage6.SUBPERIOD
PRED_FROM = DISCOVERY[0]
STORE = "risk_v1"
CRASH_DD, CRASH_N, VOL_N = -0.20, 63, 21
RIDGE_GRID = (1.0, 10.0, 100.0, 1000.0, 10000.0)
C_GRID = (0.001, 0.01, 0.1, 1.0)
LGB = {"num_leaves": 31, "min_data_in_leaf": 1000, "learning_rate": 0.05, "feature_fraction": 0.8,
       "bagging_fraction": 0.7, "bagging_freq": 1, "verbose": -1, "seed": 7}
LGB_TREES, LGB_STOP = 500, 50
LOGS = ["vol_21", "vol_63", "downvol_63", "idio_252"]
STOCK = [f"l_{c}" for c in LOGS] + ["beta_252", "mdd_63", "mdd_252", "dist_hi_252", "dist_lo_252", "ret_21", "ret_63",
                                    "ret_252", "skew63", "log_dv21", "amihud21", "log_mcap", "leverage", "bm", "ep_neg",
                                    "ear", "earnings_due", "max_ret21", "turnover21"]
CONTEXT = ["vix", "spy_vol_21", "spy_ret_63", "term_spread", "6c_baa10y", "6c_baa10y_chg63"]
HAR = ["l_vol_21", "l_vol_63", "l_idio_252"]


# ── labels and store ──────────────────────────────────────────────────────────

def drawdown_label(p: sr.Prices, rows: np.ndarray, cols: np.ndarray, n: int = CRASH_N, dd: float = CRASH_DD) -> np.ndarray:
    """1 if any close in sessions i+1 .. i+n is ≤ (1 + dd) × the next open (dividend-adjusted scale); a stock that
    stops trading for good inside the window also counts its settled value (last close with the delisting
    adjustment). NaN when the window is beyond the data or there is no entry price."""
    T = len(p.dates)
    finite = np.isfinite(p.closeadj)
    last = np.where(finite.any(0), T - 1 - np.argmax(finite[::-1], axis=0), -1)
    out = np.full(len(rows), np.nan)
    for i in np.unique(rows):
        sel = np.flatnonzero(rows == i)
        if i + n >= T:
            continue
        j = cols[sel]
        a = i + 1
        with np.errstate(all="ignore"):
            entry = p.closeadj[a, j] * p.open[a, j] / p.close[a, j]
            low = np.nanmin(p.closeadj[a:i + n + 1][:, j], axis=0)
        stopped = (last[j] >= a) & (last[j] < i + n)
        settled = np.where(stopped, p.closeadj[np.clip(last[j], 0, T - 1), j] * (1 + p.haircut[j]), np.inf)
        low = np.fmin(low, settled)
        ok = np.isfinite(entry) & (entry > 0) & np.isfinite(low)
        out[sel] = np.where(ok, (low / np.where(ok, entry, 1) - 1 <= dd).astype(float), np.nan)
    return out


def build() -> pd.DataFrame:
    raw = pd.read_parquet(F.OUT / "raw_v1.parquet")
    raw["date"] = pd.to_datetime(raw["date"])
    raw = raw[raw["date"] <= pd.Timestamp(UNTIL)].reset_index(drop=True)
    for n in F.HORIZONS:
        raw[f"y{n}_end"] = pd.to_datetime(raw[f"y{n}_end"])
    ext = pd.read_parquet(F.OUT / f"{stage6.STORE}.parquet", columns=["ticker", "fut_vol21", "6c_baa10y", "6c_baa10y_chg63"])
    if not (len(ext) == len(raw) and (ext["ticker"].to_numpy() == raw["ticker"].to_numpy()).all()):
        raise ValueError("stage-6 store is not aligned with raw v1")
    dates = sr.trading_days()
    dates = dates[dates <= pd.Timestamp(UNTIL)]
    p = sr.prices(sorted(raw["ticker"].unique()), dates)
    rows, cols = raw["i"].to_numpy(), raw["ticker"].map(p.col).astype(int).to_numpy()
    mcap = np.exp(raw["log_mcap"]).to_numpy()
    extra = stage6.price_extras(p, rows, cols, mcap)
    raw[["bm", "leverage", "ep", "ear"]] = stage6.forward_fill(raw, ["bm", "leverage", "ep", "ear"])
    out = raw[["date", "ticker", "i", "sector", "y21_raw", "y21_end", "y63_raw", "y63_end", "vol_63", *CONTEXT[:4]]].copy()
    with np.errstate(all="ignore"):
        for c in LOGS:
            out[f"l_{c}"] = np.log(raw[c].where(raw[c] > 0))
    for c in STOCK:
        if c in raw.columns:
            out[c] = raw[c]
    out["ep_neg"] = (raw["ep"] < 0).astype(float).where(raw["ep"].notna())
    out["earnings_due"] = raw["ear_days"].between(63, 100).astype(float).where(raw["ear_days"].notna())
    out["max_ret21"], out["turnover21"] = extra["max_ret21"].to_numpy(), extra["turnover21"].to_numpy()
    out[["6c_baa10y", "6c_baa10y_chg63"]] = ext[["6c_baa10y", "6c_baa10y_chg63"]].to_numpy()
    with np.errstate(all="ignore"):
        out["t_vol"] = np.log(np.clip(ext["fut_vol21"].to_numpy(dtype=float), 1e-4, None))
    out.loc[ext["fut_vol21"].isna().to_numpy(), "t_vol"] = np.nan
    out["t_crash"] = drawdown_label(p, rows, cols)
    out["crash_end"] = [dates[i + CRASH_N] if i + CRASH_N < len(dates) else pd.NaT for i in rows]
    return out


def load() -> pd.DataFrame:
    df = pd.read_parquet(F.OUT / f"{STORE}.parquet")
    for c in ("date", "y21_end", "y63_end", "crash_end"):
        df[c] = pd.to_datetime(df[c])
    return df.sort_values(["i", "ticker"]).reset_index(drop=True)


# ── design ────────────────────────────────────────────────────────────────────

def design(df: pd.DataFrame) -> tuple[np.ndarray, list[str]]:
    """Stock inputs winsorized within the date, missing → the date's median plus a flag; context as is
    (missing → 0); sector one-hot."""
    g = df.groupby("date", sort=False)
    parts, names = [], []
    for c in STOCK:
        x = df[c].astype(float)
        lo, hi = g[c].transform(lambda s: s.quantile(0.01)), g[c].transform(lambda s: s.quantile(0.99))
        x = x.clip(lo, hi)
        med = x.groupby(df["date"]).transform("median")
        parts += [x.fillna(med).fillna(0.0).to_numpy(), x.isna().to_numpy(dtype=float)]
        names += [c, f"{c}_na"]
    for c in CONTEXT:
        parts.append(df[c].astype(float).fillna(0.0).to_numpy())
        names.append(c)
    sectors = sorted(df["sector"].dropna().unique())
    for s in sectors:
        parts.append((df["sector"] == s).to_numpy(dtype=float))
        names.append(f"sector={s}")
    X = np.column_stack(parts)
    keep = X.std(0) > 1e-12
    return X[:, keep], [n for n, k in zip(names, keep) if k]


# ── losses and versions ───────────────────────────────────────────────────────

def qlike(realized_logvol: np.ndarray, pred_var: np.ndarray) -> np.ndarray:
    r = np.exp(2 * realized_logvol) / pred_var
    return r - np.log(r) - 1


def log_loss(y: np.ndarray, p: np.ndarray) -> np.ndarray:
    p = np.clip(p, 1e-6, 1 - 1e-6)
    return -(y * np.log(p) + (1 - y) * np.log(1 - p))


def _versions(df: pd.DataFrame, n: int, step: int = 1):
    """(rows to score, n_train, n_fit, v0) per version: monthly (step 1) or quarterly (step 3) from PRED_FROM."""
    per = df["date"].dt.to_period("M")
    i_first, month_rows = df.groupby(per)["i"].min(), df.groupby(per).indices
    months = [m for m in sorted(month_rows) if m >= pd.Period(PRED_FROM, "M")]
    for k in range(0, len(months), step):
        block = months[k:k + step]
        rows = np.concatenate([month_rows[m] for m in block])
        n_train, n_fit, v0 = stage6._split(df, int(i_first[block[0]]) - n, n)
        if n_train >= 10_000:
            yield block[0], rows, n_train, n_fit, v0


def _std(X: np.ndarray, idx: np.ndarray):
    mu, sd = X[idx].mean(0), X[idx].std(0)
    sd[sd == 0] = 1.0
    return mu, sd


def _ridge(Z: np.ndarray, y: np.ndarray, alpha: float) -> tuple[np.ndarray, float]:
    ybar = y.mean()
    w = np.linalg.solve(Z.T @ Z + alpha * np.eye(Z.shape[1]), Z.T @ (y - ybar))
    return w, ybar


def run_vol() -> dict:
    """B_vol (HAR OLS) and M1 (ridge on all inputs): predicted variance = exp(2 μ + 2 s²), s² the residual
    variance of log vol on the fitted rows (log-normal mean)."""
    df = load()
    ok = df["t_vol"].notna() & (df["y21_end"] <= pd.Timestamp(UNTIL))
    df = df[ok].reset_index(drop=True)
    X, names = design(df)
    H = np.column_stack([df[c].fillna(df.groupby("date")[c].transform("median")).fillna(0.0).to_numpy() for c in HAR])
    y = df["t_vol"].to_numpy()
    pred = {"base": np.full(len(df), np.nan), "m1": np.full(len(df), np.nan)}
    log = []
    for m, rows, n_train, n_fit, v0 in _versions(df, VOL_N):
        tr = np.arange(n_train)
        Hb = np.column_stack([np.ones(n_train), H[tr]])
        beta = np.linalg.lstsq(Hb, y[tr], rcond=None)[0]
        s2 = float(np.var(y[tr] - Hb @ beta))
        pred["base"][rows] = 2 * (np.column_stack([np.ones(len(rows)), H[rows]]) @ beta) + 2 * s2
        fit, val = np.arange(n_fit), np.arange(v0, n_train)
        mu, sd = _std(X, fit)
        Zf, Zv = (X[fit] - mu) / sd, (X[val] - mu) / sd
        losses = {}
        for a in RIDGE_GRID:
            w, yb = _ridge(Zf, y[fit], a)
            s2a = float(np.var(y[fit] - (Zf @ w + yb)))
            losses[a] = float(qlike(y[val], np.exp(2 * (Zv @ w + yb) + 2 * s2a)).mean())
        a = min(losses, key=losses.get)
        mu, sd = _std(X, tr)
        Zt = (X[tr] - mu) / sd
        w, yb = _ridge(Zt, y[tr], a)
        s2 = float(np.var(y[tr] - (Zt @ w + yb)))
        pred["m1"][rows] = 2 * (((X[rows] - mu) / sd) @ w + yb) + 2 * s2
        log.append({"month": str(m), "alpha": a})
    for k, v in pred.items():
        _save(f"vol_{k}", df, v)                       # stored as log predicted variance
    (F.OUT / "trials_risk_vol.json").write_text(json.dumps({"inputs": names, "versions": log}, indent=1))
    return {"versions": len(log)}


def run_crash() -> dict:
    """B_crash (logistic on log vol_63) and M1 (L2 logistic on all inputs), monthly versions."""
    from sklearn.linear_model import LogisticRegression
    df = load()
    ok = df["t_crash"].notna() & (df["crash_end"] <= pd.Timestamp(UNTIL))
    df = df[ok].reset_index(drop=True)
    X, names = design(df)
    v63 = X[:, names.index("l_vol_63")][:, None]
    y = df["t_crash"].to_numpy().astype(int)
    pred = {"base": np.full(len(df), np.nan), "m1": np.full(len(df), np.nan)}
    log = []
    for m, rows, n_train, n_fit, v0 in _versions(df, CRASH_N):
        tr = np.arange(n_train)
        b = LogisticRegression(C=1e6, max_iter=500).fit(v63[tr], y[tr])
        pred["base"][rows] = b.predict_proba(v63[rows])[:, 1]
        fit, val = np.arange(n_fit), np.arange(v0, n_train)
        mu, sd = _std(X, fit)
        losses = {}
        for c in C_GRID:
            mdl = LogisticRegression(C=c, max_iter=500).fit((X[fit] - mu) / sd, y[fit])
            losses[c] = float(log_loss(y[val], mdl.predict_proba((X[val] - mu) / sd)[:, 1]).mean())
        c = min(losses, key=losses.get)
        mu, sd = _std(X, tr)
        mdl = LogisticRegression(C=c, max_iter=500).fit((X[tr] - mu) / sd, y[tr])
        pred["m1"][rows] = mdl.predict_proba((X[rows] - mu) / sd)[:, 1]
        log.append({"month": str(m), "C": c})
    for k, v in pred.items():
        _save(f"crash_{k}", df, v)
    (F.OUT / "trials_risk_crash.json").write_text(json.dumps({"inputs": names, "versions": log}, indent=1))
    return {"versions": len(log)}


def run_lgb(threads: int = 5) -> dict:
    """M2: LightGBM for both targets, quarterly versions, early stopping on the inner validation."""
    import lightgbm as lgb
    out = {}
    for target, n, obj in (("vol", VOL_N, "regression"), ("crash", CRASH_N, "binary")):
        df = load()
        end = "y21_end" if target == "vol" else "crash_end"
        df = df[df[f"t_{target}"].notna() & (df[end] <= pd.Timestamp(UNTIL))].reset_index(drop=True)
        X, names = design(df)
        y = df[f"t_{target}"].to_numpy()
        pred, log = np.full(len(df), np.nan), []
        for m, rows, n_train, n_fit, v0 in _versions(df, n, step=3):
            fit, val = np.arange(n_fit), np.arange(v0, n_train)
            params = {**LGB, "objective": obj, "num_threads": threads}
            booster = lgb.train(params, lgb.Dataset(X[fit], y[fit]), LGB_TREES, valid_sets=[lgb.Dataset(X[val], y[val])],
                                callbacks=[lgb.early_stopping(LGB_STOP, verbose=False)])
            p = booster.predict(X[rows], num_iteration=booster.best_iteration)
            if target == "vol":
                s2 = float(np.var(y[val] - booster.predict(X[val], num_iteration=booster.best_iteration)))
                p = 2 * p + 2 * s2
            pred[rows] = p
            log.append({"quarter": str(m), "trees": int(booster.best_iteration)})
        _save(f"{target}_m2", df, pred)
        out[target] = log
    (F.OUT / "trials_risk_lgb.json").write_text(json.dumps(out, indent=1))
    return {k: len(v) for k, v in out.items()}


def preds_path(key: str):
    return F.OUT / f"preds_risk_{key}.parquet"


def _save(key: str, df: pd.DataFrame, score: np.ndarray) -> None:
    pd.DataFrame({"date": df["date"].to_numpy(), "ticker": df["ticker"].to_numpy(), "score": score}).to_parquet(preds_path(key), index=False)


# ── evaluation ────────────────────────────────────────────────────────────────

def improvement(frame: pd.DataFrame, base: str, model: str, n: int) -> dict:
    """Daily mean loss of base − model (positive = model better)."""
    d = frame.groupby("date")[[base, model]].mean()
    s = B.summary((d[base] - d[model]).dropna(), n)
    s["holds"] = bool(s and s["all"]["mean"] > 0 and s["all"]["t_nw"] is not None and s["all"]["t_nw"] >= 2
                      and s["first"]["mean"] > 0 and s["second"]["mean"] > 0)
    return s


def calibration(y: np.ndarray, p: np.ndarray, bins: int = 10) -> dict:
    from sklearn.linear_model import LogisticRegression
    ok = np.isfinite(p)
    y, p = y[ok], np.clip(p[ok], 1e-6, 1 - 1e-6)
    order = np.argsort(p)
    parts = np.array_split(order, bins)
    table = [{"pred": round(float(p[k].mean()), 4), "actual": round(float(y[k].mean()), 4)} for k in parts]
    ece = float(sum(len(k) * abs(p[k].mean() - y[k].mean()) for k in parts) / len(p))
    logit = np.log(p / (1 - p))[:, None]
    lr = LogisticRegression(C=1e6, max_iter=500).fit(logit, y)
    return {"slope": round(float(lr.coef_[0, 0]), 3), "intercept": round(float(lr.intercept_[0]), 3),
            "ece": round(ece, 4), "deciles": table}


def monthly_auc(frame: pd.DataFrame, col: str) -> float:
    from sklearn.metrics import roc_auc_score
    vals = []
    for _, g in frame.groupby(frame["date"].dt.to_period("M")):
        if g["y"].nunique() == 2:
            vals.append(roc_auc_score(g["y"], g[col]))
    return round(float(np.mean(vals)), 4)


def _halves(frame: pd.DataFrame):
    mid = frame["date"].drop_duplicates().sort_values().iloc[frame["date"].nunique() // 2]
    return frame[frame["date"] < mid], frame[frame["date"] >= mid]


def inverse_vol_portfolio(df: pd.DataFrame, logvar: pd.Series) -> dict:
    """Descriptive: every 4th sample date, weights ∝ 1 / predicted vol vs equal weight, next-21-session returns."""
    d = df.assign(lv=logvar.to_numpy())
    d = d[d["lv"].notna() & d["y21_raw"].notna()]
    dates = sorted(d["date"].unique())[::4]
    rows = []
    for dt in dates:
        g = d[d["date"] == dt]
        w = np.exp(-0.5 * g["lv"].to_numpy())
        rows.append({"date": dt, "inv": float(np.sum(w * g["y21_raw"]) / w.sum()), "ew": float(g["y21_raw"].mean())})
    r = pd.DataFrame(rows).set_index("date")

    def stats(x):
        nav = (1 + x).cumprod()
        per_year = 252 / 21
        return {"ann_return": round(float(nav.iloc[-1] ** (per_year / len(x)) - 1), 4),
                "ann_vol": round(float(x.std() * np.sqrt(per_year)), 4),
                "sharpe": round(float(x.mean() / x.std() * np.sqrt(per_year)), 3),
                "max_dd": round(float((nav / nav.cummax() - 1).min()), 4)}
    mid = len(r) // 2
    return {k: {"all": stats(r[k]), "first": stats(r[k].iloc[:mid]), "second": stats(r[k].iloc[mid:])} for k in ("inv", "ew")}


def report() -> dict:
    df = load()
    res = {}
    # volatility
    v = df[df["t_vol"].notna() & (df["y21_end"] <= pd.Timestamp(UNTIL)) & (df["date"] >= pd.Timestamp(DISCOVERY[0]))]
    v = v[["date", "ticker", "t_vol", "y21_raw", "sector"]].copy()
    for k in ("base", "m1", "m2"):
        v = v.merge(pd.read_parquet(preds_path(f"vol_{k}")).rename(columns={"score": f"lv_{k}"}), on=["date", "ticker"], how="left")
    v = v.dropna(subset=["lv_base", "lv_m1", "lv_m2"])
    for k in ("base", "m1", "m2"):
        v[f"q_{k}"] = qlike(v["t_vol"].to_numpy(), np.exp(v[f"lv_{k}"].to_numpy()))
        v[f"se_{k}"] = (v["t_vol"] - v[f"lv_{k}"] / 2) ** 2
    vol = {"rows": len(v), "dates": int(v["date"].nunique())}
    for k in ("base", "m1", "m2"):
        rank = v.groupby("date").apply(lambda g: g[f"lv_{k}"].rank().corr(g["t_vol"].rank())).mean()
        r2 = v.groupby("date").apply(lambda g: 1 - ((g["t_vol"] - g[f"lv_{k}"] / 2 - (g["t_vol"] - g[f"lv_{k}"] / 2).mean()) ** 2).sum()
                                     / ((g["t_vol"] - g["t_vol"].mean()) ** 2).sum()).mean()
        vol[k] = {"qlike": round(float(v[f"q_{k}"].mean()), 5), "mse_logvol": round(float(v[f"se_{k}"].mean()), 5),
                  "rank_corr": round(float(rank), 4), "cs_r2": round(float(r2), 4)}
    vol["m1_vs_base"] = improvement(v, "q_base", "q_m1", VOL_N)
    vol["m2_vs_base"] = improvement(v, "q_base", "q_m2", VOL_N)
    vol["m2_vs_m1"] = improvement(v, "q_m1", "q_m2", VOL_N)
    sub = v[v["date"] >= pd.Timestamp(SUBPERIOD)]
    vol["since_2008"] = {k: improvement(sub, "q_base", f"q_{k}", VOL_N)["all"] for k in ("m1", "m2")}
    vol["inverse_vol"] = {k: inverse_vol_portfolio(v, v[f"lv_{k}"]) for k in ("base", "m1", "m2")}
    res["vol"] = vol
    # crash
    c = df[df["t_crash"].notna() & (df["crash_end"] <= pd.Timestamp(UNTIL)) & (df["date"] >= pd.Timestamp(DISCOVERY[0]))]
    c = c[["date", "ticker", "t_crash", "y63_raw"]].rename(columns={"t_crash": "y"})
    for k in ("base", "m1", "m2"):
        c = c.merge(pd.read_parquet(preds_path(f"crash_{k}")).rename(columns={"score": f"p_{k}"}), on=["date", "ticker"], how="left")
    c = c.dropna(subset=["p_base", "p_m1", "p_m2"])
    crash = {"rows": len(c), "base_rate": round(float(c["y"].mean()), 4)}
    first, second = _halves(c)
    for k in ("base", "m1", "m2"):
        c[f"ll_{k}"] = log_loss(c["y"].to_numpy(), c[f"p_{k}"].to_numpy())
        top = c[c[f"p_{k}"] >= c.groupby("date")[f"p_{k}"].transform(lambda s: s.quantile(0.9))]
        crash[k] = {"log_loss": round(float(c[f"ll_{k}"].mean()), 5),
                    "brier": round(float(((c[f"p_{k}"] - c["y"]) ** 2).mean()), 5),
                    "auc_monthly": monthly_auc(c, f"p_{k}"),
                    "calibration": calibration(c["y"].to_numpy(), c[f"p_{k}"].to_numpy()),
                    "calibration_first": calibration(first["y"].to_numpy(), first[f"p_{k}"].to_numpy()),
                    "calibration_second": calibration(second["y"].to_numpy(), second[f"p_{k}"].to_numpy()),
                    "top10_actual": round(float(top["y"].mean()), 4),
                    "top10_endpoint_le_m25": round(float((top["y63_raw"] <= -0.25).mean()), 4)}
        cal_ok = all(lo <= crash[k][part]["slope"] <= hi and crash[k][part]["ece"] <= 0.01
                     for part in ("calibration_first", "calibration_second") for lo, hi in [(0.8, 1.2)])
        crash[k]["calibrated"] = bool(cal_ok)
    crash["m1_vs_base"] = improvement(c, "ll_base", "ll_m1", CRASH_N)
    crash["m2_vs_base"] = improvement(c, "ll_base", "ll_m2", CRASH_N)
    crash["m2_vs_m1"] = improvement(c, "ll_m1", "ll_m2", CRASH_N)
    sub = c[c["date"] >= pd.Timestamp(SUBPERIOD)]
    crash["since_2008"] = {k: improvement(sub, "ll_base", f"ll_{k}", CRASH_N)["all"] for k in ("m1", "m2")}
    res["crash"] = crash
    res["decision"] = decide(res)
    (F.OUT / "results_risk_v1.json").write_text(json.dumps(res, ensure_ascii=False, indent=2, default=str))
    return res


def decide(res: dict) -> dict:
    out = {}
    for t in ("vol", "crash"):
        r = res[t]
        ok = {k: r[f"{k}_vs_base"]["holds"] and (t == "vol" or r[k]["calibrated"]) for k in ("m1", "m2")}
        if ok["m1"] and ok["m2"]:
            choice = "m2" if r["m2_vs_m1"]["holds"] else "m1"
        else:
            choice = next((k for k in ("m1", "m2") if ok[k]), None)
        out[t] = {"passes": ok, "choice": choice}
    return out


def report_text(res: dict) -> str:
    p = lambda x: "—" if x is None else f"{x:+.5f}"  # noqa: E731
    v, c = res["vol"], res["crash"]
    lines = [f"风险模型（挖掘期 {DISCOVERY[0][:7]} 至 {DISCOVERY[1][:7]}，前 1000；2022 年以后未读）", "",
             "波动（未来 21 日）：模型 | QLIKE | log 波动 MSE | 截面秩相关 | 截面 R² | 相对基线的 QLIKE 改进（t）"]
    for k, name in (("base", "基线 HAR"), ("m1", "M1 岭回归"), ("m2", "M2 LightGBM")):
        inc = v.get(f"{k}_vs_base")
        s = "—" if inc is None else f"{p(inc['all']['mean'])}（{inc['all']['t_nw']}）{'成立' if inc['holds'] else '不成立'}"
        lines.append(f"{name} | {v[k]['qlike']} | {v[k]['mse_logvol']} | {v[k]['rank_corr']} | {v[k]['cs_r2']} | {s}")
    lines += ["", f"下跌 20%（未来 63 日，基准比例 {c['base_rate']:.1%}）：模型 | 对数损失 | Brier | 月度 AUC | 校准斜率 / ECE（前半；后半）| 预测最高 10% 的实际比例 | 相对基线的改进（t）"]
    for k, name in (("base", "基线（只用 63 日波动）"), ("m1", "M1 逻辑回归"), ("m2", "M2 LightGBM")):
        r = c[k]
        inc = c.get(f"{k}_vs_base")
        s = "—" if inc is None else f"{p(inc['all']['mean'])}（{inc['all']['t_nw']}）{'成立' if inc['holds'] else '不成立'}"
        lines.append(f"{name} | {r['log_loss']} | {r['brier']} | {r['auc_monthly']} | "
                     f"{r['calibration_first']['slope']} / {r['calibration_first']['ece']:.2%}；{r['calibration_second']['slope']} / {r['calibration_second']['ece']:.2%} | "
                     f"{r['top10_actual']:.1%} | {s}")
    d = res["decision"]
    lines.append("\n判定：" + "；".join(f"{'波动' if t == 'vol' else '下跌 20%'}：{'选 ' + x['choice'] if x['choice'] else '都不成立，继续用基线'}"
                                     for t, x in d.items()))
    return "\n".join(lines)
