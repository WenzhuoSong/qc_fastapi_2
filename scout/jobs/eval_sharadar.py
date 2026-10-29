"""Rechecks on the local Sharadar data: `python -m scout.jobs.eval_sharadar asset_growth`.

Pre-registered in `scout/knowledge/external_specs/sharadar_rechecks.yaml`; each recheck runs once.
Reads only the git-ignored local files; writes summary numbers (never Sharadar rows) to
`scout.gate_runs`, a brief (kind `sharadar_recheck`) and the knowledge entry.
"""
import sys
from datetime import date

from scout import db, gate, learned, sharadar_recheck
from scout.jobs._run import run

KEY = "rank_low_asset_growth_sharadar"


async def asset_growth() -> dict:
    if await db.fetch_one("SELECT 1 FROM scout.gate_runs WHERE key = :k", {"k": KEY}):
        raise SystemExit("already run (one run per pre-registration)")
    trials = await gate.trials_in("low_asset_growth") + 1
    res = sharadar_recheck.run_asset_growth(trials)
    text = sharadar_recheck.report_text(res["large"], res["small"], trials)
    print(text, flush=True)
    passed = res["large"]["pass"]
    spec = {"key": KEY, "family": "low_asset_growth", "recheck_of": "rank_low_asset_growth",
            "registration": "scout/knowledge/external_specs/sharadar_rechecks.yaml#recheck_1_asset_growth"}
    await db.execute("INSERT INTO scout.gate_runs (key, family, spec, backtest, critique, passed) VALUES "
                     "(:k, 'low_asset_growth', CAST(:s AS JSONB), CAST(:b AS JSONB), CAST(:c AS JSONB), :ok)",
                     {"k": KEY, "s": db.jsonb(spec), "b": db.jsonb(res), "ok": passed,
                      "c": db.jsonb({"source": "none", "note": "无偏数据复核按预登记只用检验关的数值条件"})})
    await db.execute("INSERT INTO scout.briefs (kind, asof, content, payload) VALUES ('sharadar_recheck', :d, :c, CAST(:p AS JSONB))",
                     {"d": date.today(), "c": text, "p": db.jsonb({"recheck": "asset_growth", **res})})
    en = await learned.get("rank_low_asset_growth")
    lg = res["large"]
    summary = (f"无偏数据复核（Sharadar，时点正确的市值前 1000，含退市公司，{lg['first_date']} 至 {lg['last_date']}）："
               f"{lg['all']['edge']:+.2%}（t {lg['all']['t']}），前半 {lg['first']['edge']:+.2%} / 后半 {lg['second']['edge']:+.2%}；"
               f"Scout 同期（{sharadar_recheck.SCOUT_START} 起）{lg['scout_window']['edge']:+.2%}（t {lg['scout_window']['t']}）；"
               f"门槛 t {lg['required_t']}，{'通过' if passed else '未通过'}")
    update = {"key": en["key"], "evidence": [*en.get("evidence", []), {"source": "sharadar_recheck", "date": str(date.today()), "summary": summary}]}
    if not passed and en["status"] in learned.OPEN:
        update.update(status="rejected", polarity="negative")
    await learned.write(update, "system", ("无偏数据复核通过，继续前向验证" if passed else "无偏数据复核未通过：改为否定，前向验证停止")[:900])
    return {"passed": passed, "large_t": lg["all"]["t"], "trials": trials}


async def main() -> dict:
    jobs = {"asset_growth": asset_growth}
    if len(sys.argv) < 2 or sys.argv[1] not in jobs:
        raise SystemExit(f"usage: python -m scout.jobs.eval_sharadar {'|'.join(jobs)}")
    return await jobs[sys.argv[1]]()


if __name__ == "__main__":
    run("eval_sharadar", main, alert=False)
