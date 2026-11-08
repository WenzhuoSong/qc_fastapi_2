"""Strong stocks bought directly and held (pre-registered in `scout/knowledge/external_specs/strong_stock_hold_v1.yaml`).

compute: local only (Sharadar files; no database), writes the formations and summary to git-ignored
         `exports/strong_hold/result.json` and refuses a second run.
record:  registers the entry (hypothesis), then records the run in `scout.gate_runs` (family trend_template),
         a brief (kind `gate`) and the knowledge entry (rejected when it failed).
"""
import json
import sys
from datetime import date
from pathlib import Path

from scout import strong_hold as sh

OUT = Path("exports/strong_hold/result.json")
SPEC = Path("scout/knowledge/external_specs/strong_stock_hold_v1.yaml")
FAMILY = "trend_template"


def compute() -> None:
    if OUT.exists():
        raise SystemExit("already run (one run per pre-registration)")
    raw = sh.run()
    ev = {k: sh.evaluate(raw[k]) for k in ("main", "top10", "w126")}
    res = {"key": sh.KEY, "formations": raw["main"]["rows"], "evaluation": ev, "spy": sh.spy_portfolios(raw["spy_all"]),
           "decision": sh.decide(ev["main"])}
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(res, ensure_ascii=False, indent=1, default=str))
    print(sh.report_text(json.loads(OUT.read_text())), flush=True)


def summary(res: dict) -> str:
    ev, dec = res["evaluation"]["main"], res["decision"]
    a, f, s, x = ev["all"], ev["first"], ev["second"], ev["ex_2008_09"]
    return (f"Sharadar 点时市值前 1000，1999 起每 21 个交易日买入强势股（相对强度前 20%、上升趋势、63 日涨 > 10%），20% 移动止损最长 252 日："
            f"{ev['formations']} 期，入选 {sh._p(ev['ret'])} 对同日随机非强势股 {sh._p(ev['control'])}，差 {sh._p(a['mean'])}（NW t {sh._t(a['t'])}，"
            f"门槛 {dec['required_t']}）；前半 {sh._p(f['mean'])} / 后半 {sh._p(s['mean'])}；去掉 2008–09 {sh._p(x['mean'])}；"
            f"不止损持有满一年 {sh._p(ev['hold_same'])}，止损的影响 {sh._p(ev['stop_cost']['mean'])}；被止损 {ev['stopped']:.0%}。本族第 {sh.TRIALS} 次检验")


async def record() -> dict:
    from scout import db, gate, learned

    res = json.loads(OUT.read_text())
    if await db.fetch_one("SELECT 1 FROM scout.gate_runs WHERE key = :k", {"k": sh.KEY}):
        raise SystemExit("already recorded")
    if sh.TRIALS != await gate.trials_in(FAMILY) + 1:
        raise SystemExit(f"family count changed: computed as trial {sh.TRIALS}, database says {await gate.trials_in(FAMILY) + 1}")
    spec = gate.load_spec(str(SPEC))
    if not await learned.get(sh.KEY):
        await learned.write({"key": sh.KEY, "spec": spec, "kind": "rule", "polarity": "positive", "status": "hypothesis",
                             "title": spec["title"], "claim": spec["claim"], "mechanism": spec["mechanism"],
                             "context": {"kind": ["stock"]}, "applies": {}, "evidence": []},
                            "system", "预登记（规格 2026-11-08 在运行前提交到 git，数据库登记在运行之后：本地运行不连数据库）")
    passed = res["decision"]["passed"]
    formations = res.pop("formations")
    res["formation_count"] = len(formations)
    await db.execute("INSERT INTO scout.gate_runs (key, family, spec, backtest, critique, passed) VALUES "
                     "(:k, :f, CAST(:s AS JSONB), CAST(:b AS JSONB), CAST(:c AS JSONB), :ok)",
                     {"k": sh.KEY, "f": FAMILY, "s": db.jsonb(spec), "b": db.jsonb(res), "ok": passed,
                      "c": db.jsonb({"source": "none", "note": "G1 通过后才运行批判关" if passed else "G1 未通过，未运行批判关"})})
    await db.execute("INSERT INTO scout.briefs (kind, asof, content, payload) VALUES ('gate', :d, :c, CAST(:p AS JSONB))",
                     {"d": date.today(), "c": sh.report_text(res), "p": db.jsonb(res)})
    en = await learned.get(sh.KEY)
    update = {"key": sh.KEY, "evidence": [*en.get("evidence", []), {"source": "gate", "date": str(date.today()), "summary": summary(res)}]}
    if not passed:
        update.update(status="rejected", polarity="negative")
    await learned.write(update, "system", "G1 通过，待批判关" if passed else "检验关未通过（G1）")
    return {"passed": passed}


def main() -> None:
    if sys.argv[1:] == ["compute"]:
        compute()
    elif sys.argv[1:] == ["record"]:
        from scout.jobs._run import run

        run("strong_hold", record, alert=False)
    else:
        raise SystemExit("usage: python -m scout.jobs.strong_hold compute | record")


if __name__ == "__main__":
    main()
