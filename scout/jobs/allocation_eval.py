"""Allocation evaluation framework v1 (`scout/knowledge/external_specs/allocation_eval_v1.yaml`).

compute: local only (French files, FRED CPIAUCSL / INDPRO fetched once and saved, DGS10 from risk_parity_sleeve_v1's
         saved copy, carver_trend_v1's saved sleeve returns; no database), writes git-ignored
         `exports/allocation_eval/result.json`, refuses a second run.
record:  a brief (kind `allocation_eval`) with the report; not a gate run and no knowledge status (a judgment on seen data).
"""
import asyncio
import json
import sys
from datetime import date
from pathlib import Path

import pandas as pd

from scout import allocation_eval as ae
from scout import cross_asset as ca
from scout import risk_parity as rp
from scout import trend_sleeve as ts

OUT = Path("exports/allocation_eval/result.json")
RAW = Path("exports/allocation_eval/fred.json")
DGS10 = Path("exports/risk_parity/dgs10.json")


def compute() -> None:
    from scout.data import fred

    if OUT.exists():
        raise SystemExit("already run (one run per framework version)")
    raw = {k: asyncio.run(fred.series(k, date(1947, 1, 1))) for k in ("CPIAUCSL", "INDPRO")}
    if any(not v or v[0][0] > date(1948, 1, 1) for v in raw.values()):
        raise SystemExit("FRED CPIAUCSL / INDPRO unavailable or too short")
    OUT.parent.mkdir(parents=True, exist_ok=True)
    RAW.write_text(json.dumps({k: [(str(d), x) for d, x in v] for k, v in raw.items()}))
    daily_y = [(date.fromisoformat(d), v) for d, v in json.loads(DGS10.read_text())]
    y = rp.month_end_yields(daily_y)
    bd = rp.bond_returns(y)
    fm = ca.french_monthly()
    eq, rf = fm["mkt"], fm["rf"]
    p, _ = rp.portfolios(eq, bd, rf)
    p = {k: v for k, v in p.items() if k != "rp"}
    env = ae.environments(ae.monthly_last(raw["CPIAUCSL"]), ae.monthly_last(raw["INDPRO"]), y, eq, bd)
    episodes = ae.bear_episodes(eq)
    long_idx = p["core_now"].index
    long = ae.profile(p, rf, long_idx, env, episodes, ae.DECISION)
    carver = json.loads(ts.CARVER.read_text())["monthly"]["long_only"]
    trend = pd.Series({pd.Period(m, "M"): x for m, x in carver.items()})
    short_idx = long_idx[(long_idx >= ae.SHORT_START)].intersection(trend.index)
    ps = {**p, ae.TREND: ts.blend(p["core_now"].reindex(short_idx), trend.reindex(short_idx), 0.2)}
    short = ae.profile(ps, rf, short_idx, env, episodes, (*ae.DECISION, ae.TREND))
    lc, sc = ae.choose(long, ae.DECISION), ae.choose(short, (*ae.DECISION, ae.TREND))
    last = env.drop(columns="corr").dropna(how="all")
    labels = {c: bool(last[c].dropna().iloc[-1]) for c in last.columns}
    words = {"infl_up": ("通胀上升", "通胀回落"), "growth_up": ("增长加速", "增长放缓"), "rates_up": ("利率上升", "利率下降"),
             "corr_pos": ("股债正相关", "股债负相关")}
    monitor = {"month": str(last.index[-1]), "labels": "、".join(words[c][0 if v else 1] + f"（{last[c].dropna().index[-1]}）"
                                                                for c, v in labels.items()),
               "corr": float(env["corr"].dropna().iloc[-1])}
    res = {"key": ae.KEY, "long": long, "long_choice": lc, "short": short, "short_choice": sc, "final": ae.final(lc, sc),
           "monitor": monitor}
    OUT.write_text(json.dumps(res, ensure_ascii=False, indent=1, default=str))
    print(ae.report_text(json.loads(OUT.read_text())), flush=True)


async def record() -> dict:
    from scout import db

    res = json.loads(OUT.read_text())
    if await db.fetch_one("SELECT 1 FROM scout.briefs WHERE kind = 'allocation_eval' AND payload->>'key' = :k", {"k": ae.KEY}):
        raise SystemExit("already recorded")
    await db.execute("INSERT INTO scout.briefs (kind, asof, content, payload) VALUES ('allocation_eval', :d, :c, CAST(:p AS JSONB))",
                     {"d": date.today(), "c": ae.report_text(res), "p": db.jsonb(res)})
    return {"pick": res["final"]["pick"]}


def main() -> None:
    if sys.argv[1:] == ["compute"]:
        compute()
    elif sys.argv[1:] == ["record"]:
        from scout.jobs._run import run

        run("allocation_eval", record, alert=False)
    else:
        raise SystemExit("usage: python -m scout.jobs.allocation_eval compute | record")


if __name__ == "__main__":
    main()
