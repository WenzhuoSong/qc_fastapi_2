"""Holding cap of the holdings plan: `python -m scout.jobs.eval_hold_cap register | run`.

register: stores the pre-registered design (`scout/knowledge/exit_specs/hold_cap.yaml`) on a
          `hypothesis` knowledge entry.
run:      the study (`scout.hold_cap`), once: prints and stores the report as a brief (kind
          `hold_cap`) and moves the entry to `backtested` (a longer cap met the rule; the user
          decides) or `rejected`.
"""
import sys
from datetime import date
from pathlib import Path

import yaml

from scout import db, exit_redeploy, exit_vs_hold, hold_cap, horizons, learned, rankers
from scout.detectors import registry
from scout.jobs._run import run
from scout.pipeline import load_panel

SPEC = Path(__file__).resolve().parents[1] / "knowledge" / "exit_specs" / "hold_cap.yaml"


async def main() -> dict:
    if len(sys.argv) < 2 or sys.argv[1] not in ("register", "run"):
        raise SystemExit("usage: python -m scout.jobs.eval_hold_cap register | run")
    spec = yaml.safe_load(SPEC.read_text())
    if sys.argv[1] == "register":
        en = await learned.write({"key": spec["key"], "kind": "method", "polarity": "positive", "status": "hypothesis",
                                  "title": "持仓计划的持有上限：60 / 126 / 252 个交易日", "claim": spec["question"],
                                  "mechanism": "资产增长、盈利质量这类优势按文献在一年左右兑现；60 天上限可能在优势兑现前卖出。",
                                  "context": {"kind": ["stock"]}, "applies": {"topics": ["exit"]}, "spec": spec, "evidence": []},
                                 "system", "预登记")
        return {"registered": en["key"], "version": en["version"]}
    en = await learned.get(spec["key"])
    if not en or en["status"] != "hypothesis":
        raise SystemExit("not registered, or already run (one run per pre-registration)")
    p = await load_panel(lookback_days=None)
    rf = exit_vs_hold.rf_daily(p.dates, [(r["date"], r["value"]) for r in await db.fetch_all(
        "SELECT date, value FROM scout.macro_series WHERE series = 'DGS3MO' ORDER BY date")])
    pool = exit_redeploy.series(p, {}, rf)[1][("pool", "none")]
    asset = (await learned.get("rank_low_asset_growth"))["spec"]
    angle = await rankers.prepare(asset, p, rankers.arrays(p)[1], horizons.rank_days(len(p.dates), asset))
    det = next(d for d in registry(include_candidates=True) if d.name == "breakout_retest")
    res = hold_cap.summarize(hold_cap.run(p, hold_cap.buys(p, angle, det, asset), pool), len(p.dates))
    text = hold_cap.report_text(res)
    print(text, flush=True)
    await db.execute("INSERT INTO scout.briefs (kind, asof, content, payload) VALUES ('hold_cap', :d, :c, CAST(:p AS JSONB))",
                     {"d": date.today(), "c": text, "p": db.jsonb(res)})
    picks = {f"{s}:{e}": res[s]["decision"].get(f"{e}|pick") for s in res if res[s].get("n") for e in hold_cap.EXITS}
    ok = {k: v for k, v in picks.items() if v}
    status = "backtested" if ok else "rejected"
    summary = "；".join(
        f"{hold_cap.STRATEGIES[s]} {res[s]['n']} 笔：" + "，".join(
            f"{hold_cap.EXITS[k.split('|')[0]]} 上限 {k.split('|')[1]} vs 60 {d['vs_cap60']['all']['edge']:+.2%}（t {d['vs_cap60']['all']['t']}）"
            for k, d in res[s]["decision"].items() if not k.endswith("pick"))
        for s in res if res[s].get("n"))
    await learned.write({"key": spec["key"], "status": status, "polarity": "positive" if ok else "negative",
                         "evidence": [{"source": "hold_cap", "date": str(date.today()), "summary": summary[:1500], "stats": {"picks": picks}}]},
                        "system", ("较长的上限满足事先定的条件：" + "、".join(f"{k}→{v}" for k, v in ok.items()) + "；是否改持仓计划等用户决定")
                        if ok else "较长的上限都不满足事先定的条件")
    return {"picks": picks, "status": status}


if __name__ == "__main__":
    run("eval_hold_cap", main, alert=False)
