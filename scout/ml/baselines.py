"""Stage-1 baselines with the forward protocol (pre-registered in `scout/knowledge/external_specs/ml_v1.yaml`).

Every month of the discovery period a new version is made (round1) from the samples whose labels
ended at least N sessions before that month's first sample (purge + embargo); its predictions for that
month are scored once their labels are complete (round0 of later months). Only discovery-period
rows whose labels also end inside the discovery period are scored or used.
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from scout.horizons import nw_t
from scout.ml import features as F
from scout.ml import operators as op

WARMUP_END = "2003-12-31"
DISCOVERY = ("2004-01-01", "2015-12-31")
ALPHAS = (1.0, 10.0, 100.0, 1_000.0, 10_000.0)
INNER_MONTHS = 24
TOP = 0.10
CKPT = F.OUT / "ckpt"


def design(df: pd.DataFrame) -> tuple[np.ndarray, list[str]]:
    """Ridge inputs: ranked numerics (+ flags), sector-neutral ranks, rank changes, context z-scores, signals,
    ordinals, sector one-hot and two interactions."""
    cs = F.PRICE + F.FUND + F.EVENTS
    cols = cs + [f"{c}_na" for c in cs] + [c for c in df.columns if c.endswith("_secn")] + [f"{c}_chg" for c in F.TS_SOURCES]
    cols += [c for c in df.columns if c.startswith("ctx_")] + F.SIGNALS + list(F.ORDINAL)
    X = df[cols].astype("float32").copy()
    for k in F.ORDINAL:
        X[k] = X[k] / max(1, len(F.ORDINAL[k]) - 1)
    sectors = sorted(df["sector"].dropna().unique())
    X = pd.concat([X, op.one_hot(df["sector"], "sector", sectors).astype("float32")], axis=1)
    X["mom_x_spy252"] = df["mom_12_1"] * df["ctx_spy_ret_252"]
    X["mom_x_vix"] = df["mom_12_1"] * df["ctx_vix"]
    return X.to_numpy(dtype=np.float64), list(X.columns)


def target(df: pd.DataFrame, y: str) -> pd.Series:
    """The label's cross-sectional rank in [−1, 1] per date."""
    return df.groupby("date")[y].transform(op.rank_pm1)


def ridge_fit(X: np.ndarray, y: np.ndarray, alpha: float) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    mu, sd = X.mean(0), X.std(0)
    sd[sd == 0] = 1.0
    Z = (X - mu) / sd
    w = np.linalg.solve(Z.T @ Z + alpha * np.eye(Z.shape[1]), Z.T @ (y - y.mean()))
    return w, mu, sd


def ridge_predict(model, X: np.ndarray) -> np.ndarray:
    w, mu, sd = model
    return ((X - mu) / sd) @ w


def daily_ic(df: pd.DataFrame, score: str, y: str) -> pd.Series:
    """Spearman correlation of the score with the label, per sample date."""
    def ic(g):
        g = g[[score, y]].dropna()
        return g[score].rank().corr(g[y].rank()) if len(g) > 20 else np.nan
    return df.groupby("date").apply(ic).dropna()


def top_spread(df: pd.DataFrame, score: str, y: str) -> pd.Series:
    """Per date: mean label of the top 10 % by score minus the mean of every other stock (the gate's control)."""
    def spread(g):
        g = g[[score, y]].dropna()
        if len(g) < 50:
            return np.nan
        k = max(1, int(round(len(g) * TOP)))
        top = g.nlargest(k, score)
        return top[y].mean() - g.drop(top.index)[y].mean()
    return df.groupby("date").apply(spread).dropna()


def summary(s: pd.Series, n: int) -> dict:
    if s.empty:
        return {}
    mid = s.index[len(s) // 2]
    part = lambda x: {"mean": round(float(x.mean()), 5), "t_nw": nw_t(list(x.index), list(x.values), n), "dates": len(x)}  # noqa: E731
    return {"all": part(s), "first": part(s[s.index < mid]), "second": part(s[s.index >= mid]), "split": str(mid.date())}


def walk_forward(df: pd.DataFrame, n: int, alphas: tuple = ALPHAS, name: str = "ridge",
                 window: tuple = DISCOVERY, label: str | None = None) -> pd.DataFrame:
    """Monthly versions of the ridge model for horizon n; returns the rows of `window` (the discovery
    period unless the one-time confirmation run passes its own) with predictions. `label` is the column
    whose per-date rank is the training target (the main label by default); alphas are chosen on the
    main label."""
    y, end = f"y{n}", f"y{n}_end"
    df = df[df[y].notna()].copy()
    df["t"] = target(df, label or y).fillna(0.0)
    X, names = design(df)
    tgt = df["t"].to_numpy()
    hi = pd.Timestamp(window[1]) if window[1] else df[end].max()
    disc = df[(df["date"] >= window[0]) & (df["date"] <= hi) & (df[end] <= hi)]
    months = sorted(disc["date"].dt.to_period("M").unique())
    i_of_date = df.groupby("date")["i"].first()
    pred = pd.Series(np.nan, index=df.index)
    CKPT.mkdir(parents=True, exist_ok=True)
    for m in months:
        rows = disc.index[disc["date"].dt.to_period("M") == m]
        first = df.loc[rows, "date"].min()
        cut = int(i_of_date[first]) - n                          # labels must end ≥ n sessions (embargo) before
        train = df.index[(df["i"] + n) <= cut]
        if len(train) < 10_000:
            continue
        tr_dates = df.loc[train, "date"]
        inner_cut = tr_dates.max() - pd.DateOffset(months=INNER_MONTHS)
        fit_i = train[(tr_dates <= inner_cut - pd.Timedelta(days=int(n * 1.5)))]
        val_i = train[tr_dates > inner_cut]
        best, best_ic = alphas[0], -np.inf
        for a in alphas:                                         # alpha by the last 24 training months, forward
            model = ridge_fit(X[df.index.get_indexer(fit_i)], tgt[df.index.get_indexer(fit_i)], a)
            v = df.loc[val_i, ["date", y]].copy()
            v["s"] = ridge_predict(model, X[df.index.get_indexer(val_i)])
            ic = daily_ic(v, "s", y).mean()
            if ic > best_ic:
                best, best_ic = a, ic
        model = ridge_fit(X[df.index.get_indexer(train)], tgt[df.index.get_indexer(train)], best)
        pred.loc[rows] = ridge_predict(model, X[df.index.get_indexer(rows)])
        (CKPT / f"{name}_y{n}_{m}.json").write_text(json.dumps({"alpha": best, "inner_ic": round(float(best_ic), 4),
                                                                 "train_rows": int(len(train)), "features": names,
                                                                 "coef": [round(float(x), 6) for x in model[0]]}))
    out = disc.copy()
    out["ridge"] = pred.loc[disc.index]
    out["momentum"] = disc["mom_12_1"]
    out["composite"] = (-disc["share_growth"] + disc["gp_assets"] + disc["ear"] + disc["mom_12_1"]) / 4
    return out


def evaluate(df: pd.DataFrame, n: int) -> dict:
    y = f"y{n}"
    res = {}
    for score in ("momentum", "composite", "ridge"):
        d = df[df[score].notna()]
        res[score] = {"ic": summary(daily_ic(d, score, y), n), "top_decile": summary(top_spread(d, score, y), n),
                      "regimes": {r: summary(daily_ic(d[d["regime"] == k], score, y), n)
                                  for k, r in enumerate(F.ORDINAL["regime"])}}
    return res


def report_text(res: dict) -> str:
    p = lambda x: "—" if x is None else f"{x:+.4f}"  # noqa: E731
    q = lambda x: "—" if x is None else f"{x:+.2%}"  # noqa: E731
    lines = [f"ML 第一阶段基线（挖掘期 {DISCOVERY[0][:7]} 至 {DISCOVERY[1][:7]}，前推训练，每月一个版本；确认期未触碰）"]
    for n, r in res.items():
        lines.append(f"\n持有 {n} 个交易日：方法 | IC 全部（重叠校正 t） | IC 前半 / 后半 | 前 10% 超额 全部（t） | 前半 / 后半 | IC 上升 / 震荡 / 下降市")
        for k, v in r.items():
            ic, td = v["ic"], v["top_decile"]
            if not ic:
                continue
            reg = " / ".join(p(v["regimes"][g].get("all", {}).get("mean")) for g in ("uptrend", "mixed", "downtrend"))
            lines.append(f"{k} | {p(ic['all']['mean'])}（{ic['all']['t_nw']}） | {p(ic['first']['mean'])} / {p(ic['second']['mean'])} | "
                         f"{q(td['all']['mean'])}（{td['all']['t_nw']}） | {q(td['first']['mean'])} / {q(td['second']['mean'])} | {reg}")
    return "\n".join(lines)


def run(path: Path = F.OUT / "features_v1.parquet") -> dict:
    df = pd.read_parquet(path)
    df["date"] = pd.to_datetime(df["date"])
    for n in F.HORIZONS:
        df[f"y{n}_end"] = pd.to_datetime(df[f"y{n}_end"])
    df = df[df["date"] <= pd.Timestamp(DISCOVERY[1])].reset_index(drop=True)    # nothing after the discovery period is read
    return {n: evaluate(walk_forward(df, n), n) for n in F.HORIZONS}
