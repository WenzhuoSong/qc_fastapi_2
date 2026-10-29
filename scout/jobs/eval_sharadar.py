"""Rechecks on the local Sharadar data: `python -m scout.jobs.eval_sharadar asset_growth | exits | b0`.

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


async def exits() -> dict:
    from scout import exit_redeploy, exit_vs_hold, sharadar_exits
    from scout.data import fred

    if await db.fetch_one("SELECT 1 FROM scout.briefs WHERE kind = 'sharadar_recheck' AND payload->>'recheck' = 'exits'"):
        raise SystemExit("already run (one run per pre-registration)")
    rf = await fred.series("DGS3MO", date(1997, 12, 1))
    if not rf:
        raise SystemExit("FRED DGS3MO unavailable")
    res = sharadar_exits.run(rf)
    labels = {**exit_vs_hold.POLICIES, **exit_redeploy.LABELS}
    text = "\n\n".join(["复核 2：卖出方式（Sharadar，时点正确的市值前 1000，含退市公司，1999 至今）",
                         exit_vs_hold.report_text(res["exit_vs_hold"]), exit_redeploy.report_text(res["exit_redeploy"]),
                         "熊市年份（组合年度收益）：\n" + sharadar_exits.bear_text(res["bear_years"], labels)])
    print(text, flush=True)
    await db.execute("INSERT INTO scout.briefs (kind, asof, content, payload) VALUES ('sharadar_recheck', :d, :c, CAST(:p AS JSONB))",
                     {"d": date.today(), "c": text, "p": db.jsonb({"recheck": "exits", **res})})
    for key, part, lab in (("exit_vs_hold", res["exit_vs_hold"], exit_vs_hold.POLICIES), ("exit_redeploy", res["exit_redeploy"], exit_redeploy.LABELS)):
        en = await learned.get(key)
        if not en:
            continue
        pick = part["decision"]["pick"]
        h = part["policies"]["hold"]["portfolio"]
        best = sorted((k for k in part["policies"] if k != "hold"), key=lambda k: -part["policies"][k]["portfolio"]["sharpe"])[:2]
        summary = (f"无偏数据复核（Sharadar，1999 至今，含 2000–2002、2008 熊市）：一直持有 夏普 {h['sharpe']:.2f} 最大回撤 {h['max_dd']:+.1%}；"
                   + "；".join(f"{lab[k]} 夏普 {part['policies'][k]['portfolio']['sharpe']:.2f} 回撤 {part['policies'][k]['portfolio']['max_dd']:+.1%}" for k in best)
                   + (f"；满足原规格条件：{lab[pick]}" if pick else "；没有方案满足原规格条件"))
        await learned.write({"key": key, "evidence": [*en.get("evidence", []), {"source": "sharadar_recheck", "date": str(date.today()),
                                                                              "summary": summary[:1500]}]},
                            "system", "补充无偏数据复核证据（状态不变，持仓计划由用户决定）")
    return {"exit_vs_hold_pick": res["exit_vs_hold"]["decision"]["pick"], "exit_redeploy_pick": res["exit_redeploy"]["decision"]["pick"]}


async def b0_replay() -> dict:
    """B0 replay: local files only (exports/b0/sharadar_replay/), nothing to the database."""
    from scout import sharadar_b0
    from scout.data import fred

    if (sharadar_b0.OUT / "result.json").exists():
        raise SystemExit("already run (one run per pre-registration)")
    rf = await fred.series("DGS3MO", date(1997, 12, 1))
    if not rf:
        raise SystemExit("FRED DGS3MO unavailable")
    res = sharadar_b0.run(rf)
    path = sharadar_b0.write(res)
    print(path.read_text(), flush=True)
    return {"written": str(path)}


async def main() -> dict:
    jobs = {"asset_growth": asset_growth, "exits": exits, "b0": b0_replay}
    if len(sys.argv) < 2 or sys.argv[1] not in jobs:
        raise SystemExit(f"usage: python -m scout.jobs.eval_sharadar {'|'.join(jobs)}")
    return await jobs[sys.argv[1]]()


if __name__ == "__main__":
    run("eval_sharadar", main, alert=False)
