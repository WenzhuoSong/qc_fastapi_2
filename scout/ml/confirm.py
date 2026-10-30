"""The one-time confirmation test (pre-registered in `scout/knowledge/external_specs/ml_confirm_v1.yaml`):
the 21-session composite on 2016+ with ridge v2 as a reported control. The result file doubles as the
run-once lock."""
from __future__ import annotations

import json

import pandas as pd

from scout.ml import baselines as B
from scout.ml import features as F
from scout.ml import gbdt
from scout.simulate import ROUND_TRIP_COST

N = 21
START = "2016-01-01"
RESULT = F.OUT / "confirm_v1.json"


def composite(df: pd.DataFrame) -> pd.Series:
    return (-df["share_growth"] + df["gp_assets"] + df["ear"] + df["mom_12_1"]) / 4


def metrics(d: pd.DataFrame, score: str, n: int = N) -> dict:
    y = f"y{n}"
    d = d[d[score].notna()]
    ic = B.daily_ic(d, score, y)
    gross = B.top_spread(d, score, y)
    net = gross - ROUND_TRIP_COST                    # the top decile turns over fully every holding period
    s_ic, s_net = B.summary(ic, n), B.summary(net, n)
    yearly = pd.DataFrame({"ic": ic, "net": net})
    yearly = yearly.groupby(yearly.index.year).mean().round(5)
    return {"ic": s_ic, "top_decile_gross": B.summary(gross, n), "top_decile_net": s_net,
            "passed": bool(s_net and s_net["all"]["t_nw"] is not None and s_net["all"]["t_nw"] >= 3
                           and s_net["first"]["mean"] > 0 and s_net["second"]["mean"] > 0
                           and s_ic["first"]["mean"] > 0 and s_ic["second"]["mean"] > 0),
            "regimes": {r: B.summary(B.daily_ic(d[d["regime"] == k], score, y), n) for k, r in enumerate(F.ORDINAL["regime"])},
            "yearly": {int(k): v for k, v in yearly.to_dict("index").items()}}


def run() -> dict:
    if RESULT.exists():
        raise SystemExit(f"{RESULT} exists: the confirmation test runs once (ml_confirm_v1.yaml)")
    df = pd.read_parquet(F.OUT / "features_v1.parquet")
    df["date"] = pd.to_datetime(df["date"])
    for k in F.HORIZONS:
        df[f"y{k}_end"] = pd.to_datetime(df[f"y{k}_end"])
    df = df.reset_index(drop=True)
    scored = B.walk_forward(df, N, alphas=gbdt.RIDGE_V2, name="confirm_ridge_v2", window=(START, None))
    scored["composite"] = composite(scored)
    res = {"spec": "ml_confirm_v1", "horizon": N, "period": [str(scored["date"].min().date()), str(scored["date"].max().date())],
           "cost": ROUND_TRIP_COST, "composite": metrics(scored, "composite"), "ridge_v2": metrics(scored, "ridge")}
    RESULT.write_text(json.dumps(res, ensure_ascii=False, indent=2, default=str))
    return res


def report_text(res: dict) -> str:
    p = lambda x: "—" if x is None else f"{x:+.4f}"  # noqa: E731
    q = lambda x: "—" if x is None else f"{x:+.2%}"  # noqa: E731
    lines = [f"ML 确认期检验（{res['period'][0]} 至 {res['period'][1]}，{res['horizon']} 日，只运行一次；扣成本 {res['cost']:.2%} / 次）",
             "方法 | IC（t） | IC 前半 / 后半 | 前 10% 超额 扣成本（t） | 前半 / 后半 | 未扣成本 | 上升 / 震荡 / 下降市 IC | 通过"]
    for k in ("composite", "ridge_v2"):
        v = res[k]
        ic, net, gross = v["ic"], v["top_decile_net"], v["top_decile_gross"]
        reg = " / ".join(p(v["regimes"][g].get("all", {}).get("mean")) for g in ("uptrend", "mixed", "downtrend"))
        lines.append(f"{k} | {p(ic['all']['mean'])}（{ic['all']['t_nw']}） | {p(ic['first']['mean'])} / {p(ic['second']['mean'])} | "
                     f"{q(net['all']['mean'])}（{net['all']['t_nw']}） | {q(net['first']['mean'])} / {q(net['second']['mean'])} | "
                     f"{q(gross['all']['mean'])} | {reg} | {'是' if v['passed'] else '否'}{'' if k == 'composite' else '（对照，只报告）'}")
    lines.append("逐年（综合分 IC / 扣成本超额）：" + "，".join(
        f"{y} {p(v['ic'])} / {q(v['net'])}" for y, v in res["composite"]["yearly"].items()))
    return "\n".join(lines)
