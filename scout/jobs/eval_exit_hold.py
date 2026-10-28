"""Exit rules vs simply holding: `python -m scout.jobs.eval_exit_hold register | run`.

register: stores the pre-registered design (`scout/knowledge/exit_specs/exit_vs_hold.yaml`)
          on a `hypothesis` knowledge entry.
run:      the study (`scout.exit_vs_hold`), once: prints and stores the report as a brief
          (kind `exit_vs_hold`) and moves the entry to `backtested` (a policy met the
          pre-registered rule; the user decides whether the holdings plan changes) or
          `rejected`.
"""
import sys
from datetime import date
from pathlib import Path

import yaml

from scout import db, exit_vs_hold, learned
from scout.jobs._run import run
from scout.pipeline import load_panel

SPEC = Path(__file__).resolve().parents[1] / "knowledge" / "exit_specs" / "exit_vs_hold.yaml"


async def main() -> dict:
    if len(sys.argv) < 2 or sys.argv[1] not in ("register", "run"):
        raise SystemExit("usage: python -m scout.jobs.eval_exit_hold register | run")
    spec = yaml.safe_load(SPEC.read_text())
    if sys.argv[1] == "register":
        en = await learned.write({"key": spec["key"], "kind": "method", "polarity": "positive", "status": "hypothesis",
                                  "title": "卖出方式：止损与跟踪止损相对一直持有", "claim": spec["question"],
                                  "mechanism": "止损限制单笔和组合的下行，代价是错过离场后的上涨；文献认为宽止损可能在风险调整后不差于持有。",
                                  "context": {"kind": ["stock"]}, "applies": {"topics": ["exit"]}, "spec": spec, "evidence": []},
                                 "system", "预登记")
        return {"registered": en["key"], "version": en["version"]}
    en = await learned.get(spec["key"])
    if not en or en["status"] != "hypothesis":
        raise SystemExit("not registered, or already run (one run per pre-registration)")
    p = await load_panel(lookback_days=None)
    rf = exit_vs_hold.rf_daily(p.dates, [(r["date"], r["value"]) for r in await db.fetch_all(
        "SELECT date, value FROM scout.macro_series WHERE series = 'DGS3MO' ORDER BY date")])
    rows, means = exit_vs_hold.run(p, rf)
    res = exit_vs_hold.summarize(rows, means, rf, len(p.dates), p.dates)
    text = exit_vs_hold.report_text(res)
    print(text, flush=True)
    await db.execute("INSERT INTO scout.briefs (kind, asof, content, payload) VALUES ('exit_vs_hold', :d, :c, CAST(:p AS JSONB))",
                     {"d": date.today(), "c": text, "p": db.jsonb(res)})
    pick = res["decision"]["pick"]
    status = "backtested" if pick else "rejected"
    summary = (f"{res['n']} 笔买入（{res['first']} 至 {res['last']}）；" + "；".join(
        f"{v['label']} 夏普 {v['portfolio']['sharpe']:.2f} 最大回撤 {v['portfolio']['max_dd']:+.1%}"
        for v in res["policies"].values()) + (f"；满足条件：{exit_vs_hold.POLICIES[pick]}" if pick else "；没有方案满足条件"))
    await learned.write({"key": spec["key"], "status": status, "polarity": "positive" if pick else "negative",
                         "evidence": [{"source": "exit_vs_hold", "date": str(date.today()), "summary": summary[:1500],
                                       "stats": {"decision": res["decision"]}}]},
                        "system", ("有方案满足事先定的条件，是否改持仓计划等用户决定" if pick else "没有方案满足事先定的条件")[:900])
    return {"n": res["n"], "pick": pick, "status": status}


if __name__ == "__main__":
    run("eval_exit_hold", main, alert=False)
