"""Stage 2: LightGBM ranking models with the forward protocol (pre-registered in
`scout/knowledge/external_specs/ml_v2.yaml`), discovery period only.

Each January the 12 pre-drawn configurations are compared on the last 24 months of the available
training data (embargoed from the fitting part), early-stopped there; each quarter the chosen
configuration is refitted on every sample whose label ended at least N sessions earlier, and that
version predicts the next three months. Ridge v2 (wider grid) and the composite run alongside.
"""
from __future__ import annotations

import itertools
import json

import numpy as np
import pandas as pd

from scout.ml import baselines as B
from scout.ml import features as F

SPACE = {"num_leaves": (15, 31, 63), "min_data_in_leaf": (200, 1000, 5000), "learning_rate": (0.02, 0.05),
         "feature_fraction": (0.5, 0.8), "lambda_l2": (0.0, 10.0)}
FIXED = {"bagging_fraction": 0.7, "bagging_freq": 1, "verbose": -1, "seed": 7}
N_CONFIGS, SEED = 12, 7
MAX_TREES, STOP = 1000, 50
RIDGE_V2 = (1e3, 3e3, 1e4, 3e4, 1e5, 3e5, 1e6)
SHAP_ROWS = 5000
OBJECTIVES = ("lgb_reg", "lgb_rank")


def configs(space: dict = SPACE) -> list[dict]:
    grid = [dict(zip(space, v)) for v in itertools.product(*space.values())]
    pick = sorted(np.random.default_rng(SEED).choice(len(grid), N_CONFIGS, replace=False))
    return [grid[k] for k in pick]


def matrix(df: pd.DataFrame) -> pd.DataFrame:
    """Every model input of the feature store (no hand interactions) plus sector / industry as categories."""
    cs = F.PRICE + F.FUND + F.EVENTS
    cols = cs + [f"{c}_na" for c in cs] + [c for c in df.columns if c.endswith("_secn")] + [f"{c}_chg" for c in F.TS_SOURCES]
    cols += [c for c in df.columns if c.startswith("ctx_")] + F.SIGNALS + list(F.ORDINAL)
    X = df[cols].astype("float32").copy()
    for c in F.NOMINAL:
        cats = sorted(df[c].dropna().unique())
        X[c] = pd.Categorical(df[c], categories=cats)
    return X


def decile(df: pd.DataFrame, y: str) -> pd.Series:
    return df.groupby("date")[y].transform(
        lambda s: np.floor((s.rank(method="first") - 1) / s.notna().sum() * 10).clip(0, 9)).fillna(0).astype(int)


def _groups(dates: pd.Series) -> list[int]:
    return dates.groupby(dates, sort=False).size().tolist()


def fit(objective: str, params: dict, X: pd.DataFrame, t: pd.Series, rel: pd.Series, dates: pd.Series,
        rounds: int, valid: tuple | None = None, threads: int = 5):
    import lightgbm as lgb

    p = {**FIXED, **params, "num_threads": threads}
    if objective == "lgb_reg":
        p["objective"] = "regression"
        d = lgb.Dataset(X, t, free_raw_data=False)
        v = lgb.Dataset(valid[0], valid[1], reference=d) if valid else None
    else:
        p.update(objective="lambdarank", lambdarank_truncation_level=100, eval_at=[100])
        order = np.argsort(dates.to_numpy(), kind="stable")
        X, rel, dates = X.iloc[order], rel.iloc[order], dates.iloc[order]
        d = lgb.Dataset(X, rel, group=_groups(dates), free_raw_data=False)
        v = None
        if valid:
            vo = np.argsort(valid[3].to_numpy(), kind="stable")
            v = lgb.Dataset(valid[0].iloc[vo], valid[2].iloc[vo], group=_groups(valid[3].iloc[vo]), reference=d)
    cb = [lgb.early_stopping(STOP, verbose=False)] if v is not None else []
    return lgb.train(p, d, num_boost_round=rounds, valid_sets=[v] if v is not None else None, callbacks=cb)


def _versions(df: pd.DataFrame, disc: pd.DataFrame, n: int, X: pd.DataFrame, rel: pd.Series,
              models: dict[str, str], space: dict, threads: int) -> tuple[dict, dict, list, dict]:
    """The forward protocol: each January choose a configuration per model on the last 24 months of the
    available training data (early-stopped there), each quarter refit on every sample whose label ended
    ≥ n sessions earlier; that version predicts the quarter. `models` maps a name to its objective."""
    y = f"y{n}"
    i_of_date = df.groupby("date")["i"].first()
    quarters = sorted({(d.year, (d.month - 1) // 3) for d in disc["date"]})
    preds = {k: pd.Series(np.nan, index=disc.index) for k in models}
    chosen, log, shap_years = {}, [], {}
    for year, q in quarters:
        rows = disc.index[(disc["date"].dt.year == year) & ((disc["date"].dt.month - 1) // 3 == q)]
        first = df.loc[rows, "date"].min()
        cut = int(i_of_date[first]) - n
        train = df.index[(df["i"] + n) <= cut]
        if q == 0 or not chosen:                                 # January: choose the configuration again
            tr_dates = df.loc[train, "date"]
            inner = tr_dates.max() - pd.DateOffset(months=24)
            fit_i = train[tr_dates <= inner - pd.Timedelta(days=int(n * 1.5))]
            val_i = train[tr_dates > inner]
            for name, obj in models.items():
                best = None
                for k, cfg in enumerate(configs(space)):
                    m = fit(obj, cfg, X.loc[fit_i], df.loc[fit_i, "t"], rel.loc[fit_i], df.loc[fit_i, "date"], MAX_TREES,
                            (X.loc[val_i], df.loc[val_i, "t"], rel.loc[val_i], df.loc[val_i, "date"]), threads)
                    v = df.loc[val_i, ["date", y]].copy()
                    v["s"] = m.predict(X.loc[val_i], num_iteration=m.best_iteration)
                    ic = float(B.daily_ic(v, "s", y).mean())
                    log.append({"year": year, "objective": name, "config": k, "trees": int(m.best_iteration), "val_ic": round(ic, 5)})
                    if best is None or ic > best[0]:
                        best = (ic, cfg, max(1, int(m.best_iteration)))     # the early stop decides (ml_v2.yaml)
                chosen[name] = best
        for name, obj in models.items():
            ic, cfg, trees = chosen[name]
            m = fit(obj, cfg, X.loc[train], df.loc[train, "t"], rel.loc[train], df.loc[train, "date"], trees, None, threads)
            preds[name].loc[rows] = m.predict(X.loc[rows])
            m.save_model(str(B.CKPT / f"{name}_y{n}_{year}Q{q + 1}.txt"))
            if q == 3:                                           # the version used in December: SHAP on that year's rows
                yr = disc.index[disc["date"].dt.year == year]
                sample = np.random.default_rng(SEED + year).choice(yr, min(SHAP_ROWS, len(yr)), replace=False)
                contrib = m.predict(X.loc[sample], pred_contrib=True)[:, :-1]
                shap_years.setdefault(name, {})[year] = dict(zip(X.columns, np.abs(contrib).mean(0).round(6).tolist()))
    return preds, chosen, log, shap_years


def _discovery(df: pd.DataFrame, n: int) -> pd.DataFrame:
    end = f"y{n}_end"
    return df[(df["date"] >= B.DISCOVERY[0]) & (df["date"] <= B.DISCOVERY[1]) & (df[end] <= pd.Timestamp(B.DISCOVERY[1]))]


def walk_forward(df: pd.DataFrame, n: int, threads: int = 5) -> tuple[pd.DataFrame, dict]:
    """Quarterly versions for horizon n; returns discovery rows with every model's score, plus the trial log."""
    y = f"y{n}"
    df = df[df[y].notna()].reset_index(drop=True)
    df["t"] = B.target(df, y)
    rel = decile(df, y)
    X = matrix(df)
    disc = _discovery(df, n)
    preds, chosen, log, shap_years = _versions(df, disc, n, X, rel, {o: o for o in OBJECTIVES}, SPACE, threads)
    out = disc.copy()
    for obj in OBJECTIVES:
        out[obj] = preds[obj]
    ridge = B.walk_forward(df.drop(columns=["t"]), n, alphas=RIDGE_V2, name="ridge_v2")
    out["ridge_v2"] = ridge["ridge"].reindex(out.index)
    out["composite"] = (-disc["share_growth"] + disc["gp_assets"] + disc["ear"] + disc["mom_12_1"]) / 4
    return out, {"trials": log, "chosen": {o: {"val_ic": c[0], "config": c[1], "trees": c[2]} for o, c in chosen.items()},
                 "shap": shap_years}


def evaluate(df: pd.DataFrame, n: int, scores: tuple = (*OBJECTIVES, "ridge_v2", "composite")) -> dict:
    y = f"y{n}"
    res = {}
    for score in scores:
        d = df[df[score].notna()]
        ic, td = B.summary(B.daily_ic(d, score, y), n), B.summary(B.top_spread(d, score, y), n)
        cand = bool(ic and ic["all"]["t_nw"] is not None and ic["all"]["t_nw"] >= 3 and ic["first"]["mean"] > 0
                    and ic["second"]["mean"] > 0 and td["first"]["mean"] > 0 and td["second"]["mean"] > 0)
        res[score] = {"ic": ic, "top_decile": td, "candidate": cand,
                      "regimes": {r: B.summary(B.daily_ic(d[d["regime"] == k], score, y), n) for k, r in enumerate(F.ORDINAL["regime"])}}
    return res


def shap_stability(shap: dict, top: int = 10) -> dict:
    """Per objective: features by mean |SHAP| over the years, and how many years each was in the yearly top 10."""
    out = {}
    for obj, years in shap.items():
        frame = pd.DataFrame(years).T
        tops = frame.apply(lambda r: set(r.nlargest(top).index), axis=1)
        mean = frame.mean().sort_values(ascending=False)
        out[obj] = [{"feature": f, "mean_abs": round(float(v), 5), "years_in_top": int(sum(f in s for s in tops))}
                    for f, v in mean.head(15).items()]
    return out


def run(horizon: int, threads: int = 5) -> dict:
    path = F.OUT / "features_v1.parquet"
    df = pd.read_parquet(path)
    df["date"] = pd.to_datetime(df["date"])
    for k in F.HORIZONS:
        df[f"y{k}_end"] = pd.to_datetime(df[f"y{k}_end"])
    df = df[df["date"] <= pd.Timestamp(B.DISCOVERY[1])].reset_index(drop=True)    # nothing after discovery is read
    B.CKPT.mkdir(parents=True, exist_ok=True)
    scored, info = walk_forward(df, horizon, threads)
    res = {"metrics": evaluate(scored, horizon), "chosen": info["chosen"], "shap": shap_stability(info["shap"]),
           "trials": len(info["trials"])}
    (F.OUT / f"trials_v2_y{horizon}.json").write_text(json.dumps(info["trials"], indent=1))
    return res


def report_text(results: dict) -> str:
    p = lambda x: "—" if x is None else f"{x:+.4f}"  # noqa: E731
    q = lambda x: "—" if x is None else f"{x:+.2%}"  # noqa: E731
    lines = [f"ML 第二阶段（挖掘期 {B.DISCOVERY[0][:7]} 至 {B.DISCOVERY[1][:7]}，每季度一个版本，每年选参数；确认期未触碰）"]
    for n, r in results.items():
        lines.append(f"\n持有 {n} 个交易日（尝试 {r['trials']} 次）：方法 | IC（t） | IC 前半 / 后半 | 前 10% 超额（t） | 前半 / 后半 | 上升 / 震荡 / 下降市 IC | 候选")
        for k, v in r["metrics"].items():
            ic, td = v["ic"], v["top_decile"]
            reg = " / ".join(p(v["regimes"][g].get("all", {}).get("mean")) for g in ("uptrend", "mixed", "downtrend"))
            lines.append(f"{k} | {p(ic['all']['mean'])}（{ic['all']['t_nw']}） | {p(ic['first']['mean'])} / {p(ic['second']['mean'])} | "
                         f"{q(td['all']['mean'])}（{td['all']['t_nw']}） | {q(td['first']['mean'])} / {q(td['second']['mean'])} | {reg} | "
                         f"{'是' if v['candidate'] else '否'}")
        for obj, feats in r["shap"].items():
            lines.append(f"{obj} 重要特征（平均 |SHAP|，进入当年前 10 的年数）：" +
                         "、".join(f"{x['feature']}（{x['years_in_top']}）" for x in feats[:10]))
    return "\n".join(lines)


# ── stage 2b (ml_v2b.yaml): cumulative changes against memorising periods ─────────────

SPACE_2B = {**SPACE, "min_data_in_leaf": (5000, 20000, 50000)}
INTERACT = ["mom_12_1", "vol_63", "log_mcap", "bm"]
DATE_LEVEL = ["regime", "vol_state"]            # ordinals that change only with the date (SPY)
VARIANTS = {
    "b1_no_industry": {"industry": False, "interact": False, "space": SPACE, "label": "y"},
    "b2_ctx_interact": {"industry": False, "interact": True, "space": SPACE, "label": "y"},
    "b3_big_leaves": {"industry": False, "interact": True, "space": SPACE_2B, "label": "y"},
    "b4_sector_label": {"industry": False, "interact": True, "space": SPACE_2B, "label": "y_sec"},
}
RIDGE_V2_IC = {21: 0.0206, 63: 0.0155}           # stage-2 discovery IC a variant must beat


def matrix_2b(df: pd.DataFrame, v: dict) -> pd.DataFrame:
    X = matrix(df)
    if not v["industry"]:
        X = X.drop(columns=["industry"])
    if v["interact"]:
        ctx = [c for c in X.columns if c.startswith("ctx_")]
        level = {c: X[c] for c in ctx}
        level["regime"] = (X["regime"] - 1).clip(-1, 1)
        level["vol_state"] = (X["vol_state"] * 2 - 1).clip(-1, 1)
        X = X.drop(columns=ctx + DATE_LEVEL)
        inter = {f"{c}_x_{f}": (level[c] * df[f]).astype("float32") for c in level for f in INTERACT}
        X = pd.concat([X, pd.DataFrame(inter, index=X.index)], axis=1)
    return X


def walk_forward_2b(df: pd.DataFrame, n: int, name: str, threads: int = 5) -> tuple[pd.DataFrame, dict]:
    v = VARIANTS[name]
    y = f"y{n}"
    df = df[df[y].notna()].reset_index(drop=True)
    df["t"] = B.target(df, y if v["label"] == "y" else f"y{n}_sec").fillna(0.0)
    X = matrix_2b(df, v)
    disc = _discovery(df, n)
    preds, chosen, log, shap_years = _versions(df, disc, n, X, df["t"], {name: "lgb_reg"}, v["space"], threads)
    out = disc.copy()
    out[name] = preds[name]
    c = chosen[name]
    return out, {"trials": log, "chosen": {"val_ic": c[0], "config": c[1], "trees": c[2]}, "shap": shap_years}


def run_2b(horizon: int, threads: int = 5, done: dict | None = None, save=None) -> dict:
    """Every variant in order; `done` holds variants finished by an earlier interrupted run, `save` is
    called after each variant so a crash keeps what was finished."""
    df = pd.read_parquet(F.OUT / "features_v1.parquet")
    df["date"] = pd.to_datetime(df["date"])
    for k in F.HORIZONS:
        df[f"y{k}_end"] = pd.to_datetime(df[f"y{k}_end"])
    df = df[df["date"] <= pd.Timestamp(B.DISCOVERY[1])].reset_index(drop=True)    # nothing after discovery is read
    B.CKPT.mkdir(parents=True, exist_ok=True)
    res = dict(done or {})
    for name in VARIANTS:
        if name in res:
            continue
        scored, info = walk_forward_2b(df, horizon, name, threads)
        m = evaluate(scored, horizon, (name,))[name]
        m["beats_ridge_v2"] = bool(m["ic"] and m["ic"]["all"]["mean"] > RIDGE_V2_IC[horizon])
        m["candidate_2b"] = m["candidate"] and m["beats_ridge_v2"]
        res[name] = {"metrics": m, "chosen": info["chosen"], "shap": shap_stability(info["shap"])[name],
                     "trials": info["trials"]}
        if save:
            save(res)
    return res


def report_2b(results: dict) -> str:
    p = lambda x: "—" if x is None else f"{x:+.4f}"  # noqa: E731
    q = lambda x: "—" if x is None else f"{x:+.2%}"  # noqa: E731
    lines = [f"ML 第 2b 阶段（挖掘期 {B.DISCOVERY[0][:7]} 至 {B.DISCOVERY[1][:7]}，LightGBM 回归，逐步累加改动；确认期未触碰）"]
    for n, r in results.items():
        lines.append(f"\n持有 {n} 个交易日（岭回归 v2 IC {RIDGE_V2_IC[int(n)]:+.4f}）：变体 | IC（t） | IC 前半 / 后半 | "
                     "前 10% 超额（t） | 前半 / 后半 | 树数中位 | 候选")
        for k, v in r.items():
            m, ic, td = v["metrics"], v["metrics"]["ic"], v["metrics"]["top_decile"]
            trees = int(np.median([t["trees"] for t in v["trials"]]))
            lines.append(f"{k} | {p(ic['all']['mean'])}（{ic['all']['t_nw']}） | {p(ic['first']['mean'])} / {p(ic['second']['mean'])} | "
                         f"{q(td['all']['mean'])}（{td['all']['t_nw']}） | {q(td['first']['mean'])} / {q(td['second']['mean'])} | "
                         f"{trees} | {'是' if m['candidate_2b'] else '否'}")
        for k, v in r.items():
            lines.append(f"{k} 重要特征：" + "、".join(f"{x['feature']}（{x['years_in_top']}）" for x in v["shap"][:8]))
    return "\n".join(lines)
