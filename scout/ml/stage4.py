"""Stage 4 (pre-registered in `scout/knowledge/external_specs/ml_v4.yaml`), discovery only: ridge v2 and
the b4 tree setup trained on {sector-neutral, residual} labels over {top 1000, top 3000}, evaluated on
the top 1000 by IC, top decile and the ml_tilt_v1 monthly-band tilt portfolio, each against its
(sector-neutral, top 1000) baseline.
"""
from __future__ import annotations

import json

import numpy as np
import pandas as pd

from scout.ml import baselines as B
from scout.ml import features as F
from scout.ml import gbdt, tilt, wide

STORES = {"u1000": "features_v1", "u3000": "features_v2"}
LABELS = ("sec", "res")
COMBOS = [(m, lab, st) for m in ("ridge", "lgb") for lab in LABELS for st in STORES]
BASE = {"ridge": "ridge_sec_u1000", "lgb": "lgb_sec_u1000"}


def name(model: str, label: str, store: str) -> str:
    return f"{model}_{label}_{store}"


def preds_path(key: str, n: int):
    return F.OUT / f"preds_v4_{key}_y{n}.parquet"


def load(store: str, n: int) -> pd.DataFrame:
    df = pd.read_parquet(F.OUT / f"{STORES[store]}.parquet")
    df["date"] = pd.to_datetime(df["date"])
    for k in F.HORIZONS:
        df[f"y{k}_end"] = pd.to_datetime(df[f"y{k}_end"])
    if store == "u1000":
        raw = pd.read_parquet(F.OUT / "raw_v1.parquet", columns=["vol_63"])
        df["vol_63_raw"], df["in_large"] = raw["vol_63"].to_numpy(), 1      # same row order (built together)
    df = df[df["date"] <= pd.Timestamp(B.DISCOVERY[1])]                    # nothing after discovery is read
    df = df[df[f"y{n}"].notna()].reset_index(drop=True)
    df["y_res"] = wide.residual_label(df, n, df["vol_63_raw"])
    return df


def label_col(label: str, n: int) -> str:
    return f"y{n}_sec" if label == "sec" else "y_res"


def _save(key: str, n: int, rows: pd.DataFrame, score: pd.Series) -> None:
    keep = rows["in_large"] == 1
    pd.DataFrame({"date": rows.loc[keep, "date"], "ticker": rows.loc[keep, "ticker"],
                  "score": score.loc[keep[keep].index]}).to_parquet(preds_path(key, n), index=False)


def run_lgb(label: str, store: str, n: int, threads: int = 5) -> dict:
    key = name("lgb", label, store)
    df = load(store, n)
    v = gbdt.VARIANTS["b4_sector_label"]
    df["t"] = B.target(df, label_col(label, n)).fillna(0.0)
    X = gbdt.matrix_2b(df, v)
    disc = gbdt._discovery(df, n)
    B.CKPT.mkdir(parents=True, exist_ok=True)
    preds, chosen, log, _ = gbdt._versions(df, disc, n, X, df["t"], {f"v4_{key}": "lgb_reg"}, v["space"], threads)
    _save(key, n, disc, preds[f"v4_{key}"])
    info = {"trials": log, "chosen": {k: {"val_ic": c[0], "config": c[1], "trees": c[2]} for k, c in chosen.items()}}
    (F.OUT / f"trials_v4_{key}_y{n}.json").write_text(json.dumps(info, indent=1))
    return info


def run_ridge(label: str, store: str, n: int) -> None:
    key = name("ridge", label, store)
    df = load(store, n)
    scored = B.walk_forward(df, n, alphas=gbdt.RIDGE_V2, name=f"v4_{key}", label=label_col(label, n))
    _save(key, n, scored, scored["ridge"])


def increment(scored: pd.DataFrame, n: int, score: str, base: str) -> dict:
    y = f"y{n}"
    d = scored[scored[score].notna() & scored[base].notna()]
    s = B.summary((B.daily_ic(d, score, y) - B.daily_ic(d, base, y)).dropna(), n)
    s["holds"] = bool(s and s["all"]["mean"] > 0 and s["all"]["t_nw"] is not None and s["all"]["t_nw"] >= 2
                      and s["first"]["mean"] > 0 and s["second"]["mean"] > 0)
    return s


def tilt_ir(scores: dict[str, pd.DataFrame]) -> dict:
    """ml_tilt_v1's chosen variant (monthly, band) over the discovery period for each score."""
    df, p, R, spy, dates = tilt.load(B.DISCOVERY[0], B.DISCOVERY[1])
    reg, (step, band) = tilt.regimes(df), tilt.VARIANTS["m_band"]
    out = {}
    for key, s in scores.items():
        d = df.merge(s.rename(columns={"score": key}), on=["date", "ticker"], how="left")
        st = tilt.stats(tilt.simulate(R, tilt.decisions(d, key, step), len(dates) - 1, band), dates, spy, reg)
        out[key] = {"ir": st["all"]["ir"], "t": st["all"]["t"], "ann_active": st["all"]["ann_active"],
                    "first": st["first"]["ann_active"], "second": st["second"]["ann_active"],
                    "turnover": st["turnover_one_way_per_year"]}
    return out


def report(n: int) -> dict:
    base = load("u1000", n)
    disc = gbdt._discovery(base, n)[["date", "ticker", "regime", f"y{n}"]].reset_index(drop=True)
    scores = {}
    for m, lab, st in COMBOS:
        key = name(m, lab, st)
        path = F.OUT / f"b4_scores_y{n}.parquet" if key == "lgb_sec_u1000" else preds_path(key, n)
        s = pd.read_parquet(path)
        scores[key] = s.rename(columns={"b4": "score"})[["date", "ticker", "score"]]
    scored = disc.copy()
    for key, s in scores.items():
        scored = scored.merge(s.rename(columns={"score": key}), on=["date", "ticker"], how="left")
    metrics = gbdt.evaluate(scored, n, tuple(scores))
    inc = {k: increment(scored, n, k, BASE[k.split("_")[0]]) for k in scores if k not in BASE.values()}
    res = {"metrics": metrics, "increments": inc, "tilt": tilt_ir(scores)}
    (F.OUT / f"results_v4_y{n}.json").write_text(json.dumps(res, ensure_ascii=False, indent=2, default=str))
    return res


def report_text(results: dict) -> str:
    p = lambda x: "—" if x is None else f"{x:+.4f}"  # noqa: E731
    q = lambda x: "—" if x is None else f"{x:+.2%}"  # noqa: E731
    lines = [f"ML 第四阶段（挖掘期 {B.DISCOVERY[0][:7]} 至 {B.DISCOVERY[1][:7]}，只在前 1000 评估；确认期未触碰）"]
    for n, r in results.items():
        lines.append(f"\n持有 {n} 个交易日：组合 | IC（t） | IC 前半 / 后半 | 前 10% 超额（t） | 倾斜组合 IR（t） | 相对基线的 IC 增量（t） | 候选")
        for k, v in r["metrics"].items():
            ic, td, tl = v["ic"], v["top_decile"], r["tilt"][k]
            inc = r["increments"].get(k)
            inc_s = "基线" if inc is None else f"{p(inc['all']['mean'])}（{inc['all']['t_nw']}）{'成立' if inc['holds'] else '不成立'}"
            lines.append(f"{k} | {p(ic['all']['mean'])}（{ic['all']['t_nw']}） | {p(ic['first']['mean'])} / {p(ic['second']['mean'])} | "
                         f"{q(td['all']['mean'])}（{td['all']['t_nw']}） | {tl['ir']}（{tl['t']}） | {inc_s} | {'是' if v['candidate'] else '否'}")
    return "\n".join(lines)
