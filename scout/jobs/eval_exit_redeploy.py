"""Exit, then redeploy: `python -m scout.jobs.eval_exit_redeploy register | run`.

register: stores the pre-registered design (`scout/knowledge/exit_specs/exit_redeploy.yaml`)
          on a `hypothesis` knowledge entry.
run:      the study (`scout.exit_redeploy`), once: prints and stores the report as a brief
          (kind `exit_redeploy`) and moves the entry to `backtested` (a variant met the
          pre-registered rule; the user decides whether the holdings plan changes) or `rejected`.
"""
import sys
from datetime import date
from pathlib import Path

import yaml

from scout import db, exit_redeploy, exit_vs_hold, learned, rankers
from scout.jobs._run import run
from scout.pipeline import load_panel

SPEC = Path(__file__).resolve().parents[1] / "knowledge" / "exit_specs" / "exit_redeploy.yaml"


async def main() -> dict:
    if len(sys.argv) < 2 or sys.argv[1] not in ("register", "run"):
        raise SystemExit("usage: python -m scout.jobs.eval_exit_redeploy register | run")
    spec = yaml.safe_load(SPEC.read_text())
    if sys.argv[1] == "register":
        en = await learned.write({"key": spec["key"], "kind": "method", "polarity": "positive", "status": "hypothesis",
                                  "title": "卖出后换仓：换入股票池或资产增长选股，配合大盘与行业过滤", "claim": spec["question"],
                                  "mechanism": "止损的主要代价是卖出后资金闲置；换入新的目标保持满仓，大盘过滤避开大熊市，行业过滤只买走强的行业。",
                                  "context": {"kind": ["stock"]}, "applies": {"topics": ["exit"]}, "spec": spec, "evidence": []},
                                 "system", "预登记")
        return {"registered": en["key"], "version": en["version"]}
    en = await learned.get(spec["key"])
    if not en or en["status"] != "hypothesis":
        raise SystemExit("not registered, or already run (one run per pre-registration)")
    p = await load_panel(lookback_days=None)
    rf = exit_vs_hold.rf_daily(p.dates, [(r["date"], r["value"]) for r in await db.fetch_all(
        "SELECT date, value FROM scout.macro_series WHERE series = 'DGS3MO' ORDER BY date")])
    angle = await rankers.prepare(exit_redeploy.PICKS_SPEC, p, rankers.arrays(p)[1],
                                  rankers.rebalance_dates(len(p.dates), exit_redeploy.PICKS_SPEC))
    rows, means, off = exit_redeploy.run(p, rf, angle)
    res = exit_redeploy.summarize(rows, means, rf, len(p.dates), p.dates, off)
    text = exit_redeploy.report_text(res)
    print(text, flush=True)
    await db.execute("INSERT INTO scout.briefs (kind, asof, content, payload) VALUES ('exit_redeploy', :d, :c, CAST(:p AS JSONB))",
                     {"d": date.today(), "c": text, "p": db.jsonb(res)})
    pick = res["decision"]["pick"]
    status = "backtested" if pick else "rejected"
    best = sorted(exit_redeploy.VARIANTS, key=lambda k: -res["policies"][k]["portfolio"]["sharpe"])[:3]
    summary = (f"{res['n']} 笔买入（{res['first']} 至 {res['last']}），20 个方案；一直持有 夏普 {res['policies']['hold']['portfolio']['sharpe']:.2f} "
               f"最大回撤 {res['policies']['hold']['portfolio']['max_dd']:+.1%}；夏普最高的三个："
               + "；".join(f"{exit_redeploy.LABELS[k]} {res['policies'][k]['portfolio']['sharpe']:.2f} / {res['policies'][k]['portfolio']['max_dd']:+.1%}"
                          for k in best)
               + (f"；满足条件：{exit_redeploy.LABELS[pick]}" if pick else "；没有方案满足条件"))
    await learned.write({"key": spec["key"], "status": status, "polarity": "positive" if pick else "negative",
                         "evidence": [{"source": "exit_redeploy", "date": str(date.today()), "summary": summary[:1500],
                                       "stats": {"decision": res["decision"]}}]},
                        "system", "有方案满足事先定的条件，是否改持仓计划等用户决定" if pick else "没有方案满足事先定的条件")
    return {"n": res["n"], "pick": pick, "status": status}


if __name__ == "__main__":
    run("eval_exit_redeploy", main, alert=False)
