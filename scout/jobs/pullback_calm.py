"""Pullback in calm markets (pre-registered in `scout/knowledge/external_specs/pullback_calm_market_v1.yaml`).

compute: local only (Sharadar files; no database), writes the trades and summary to git-ignored
         `exports/pullback_calm/result.json` and refuses a second run.
record:  registers the entry (hypothesis), then records the run in `scout.gate_runs` (family pullback_in_uptrend),
         a brief (kind `gate`) and the knowledge entry (rejected when it failed).
"""
import asyncio
import json
import sys
from datetime import date
from pathlib import Path

from scout import pullback_calm as pc

OUT = Path("exports/pullback_calm/result.json")
SPEC = Path("scout/knowledge/external_specs/pullback_calm_market_v1.yaml")
FAMILY = "pullback_in_uptrend"


def compute() -> None:
    if OUT.exists():
        raise SystemExit("already run (one run per pre-registration)")
    raw = pc.run()
    ev = pc.evaluate(raw["rows"])
    res = {"key": pc.KEY, "signals": raw["signals"], "rows": raw["rows"], "evaluation": ev, "decision": pc.decide(ev)}
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(res, ensure_ascii=False, indent=1, default=str))
    print(pc.report_text(json.loads(OUT.read_text())), flush=True)


def summary(res: dict) -> str:
    p = res["evaluation"]["primary"]
    i, o, d = p["in"], p["out"], p["in_minus_out"]
    return (f"Sharadar 点时市值前 1000，判定样本 1999-01..2014-09（Scout 未看过）：条件内（大盘过滤正常 + SPY 低波动）{i['n']} 笔，"
            f"对同日其他股票 {i['select']:+.2%}（t {i['select_t']:.2f}，门槛 {res['decision']['required_t']}），"
            f"条件外 {o['n']} 笔 {o['select']:+.2%}；条件内减条件外 {d['mean']:+.2%}（t {d['t']:.2f}）；"
            f"条件内对直接持有该股 {i['hold']:+.2%}。条件内扣费收益 {i['ret']:+.2%}、止损 {i['states'].get('stopped', 0):.0%}，"
            f"条件外 {o['ret']:+.2%}、{o['states'].get('stopped', 0):.0%}：平稳市况抬高的是所有股票，不是这个形态。本族第 {pc.TRIALS} 次检验")


async def record() -> dict:
    from scout import db, gate, learned

    res = json.loads(OUT.read_text())
    if await db.fetch_one("SELECT 1 FROM scout.gate_runs WHERE key = :k", {"k": pc.KEY}):
        raise SystemExit("already recorded")
    if pc.TRIALS != await gate.trials_in(FAMILY) + 1:
        raise SystemExit(f"family count changed: computed as trial {pc.TRIALS}, database says {await gate.trials_in(FAMILY) + 1}")
    spec = gate.load_spec(str(SPEC))
    if not await learned.get(pc.KEY):
        await learned.write({"key": pc.KEY, "spec": spec, "kind": "rule", "polarity": "positive", "status": "hypothesis",
                             "title": spec["title"], "claim": spec["claim"], "mechanism": spec["mechanism"],
                             "context": {"kind": ["stock"]}, "applies": {"detectors": ["pullback_in_uptrend"]}, "evidence": []},
                            "system", "预登记（规格 2026-11-08 在运行前提交到 git，数据库登记在运行之后：本地运行不连数据库）")
    passed = res["decision"]["passed"]
    rows = res.pop("rows")
    res["fills"] = len(rows)
    await db.execute("INSERT INTO scout.gate_runs (key, family, spec, backtest, critique, passed) VALUES "
                     "(:k, :f, CAST(:s AS JSONB), CAST(:b AS JSONB), CAST(:c AS JSONB), :ok)",
                     {"k": pc.KEY, "f": FAMILY, "s": db.jsonb(spec), "b": db.jsonb(res), "ok": passed,
                      "c": db.jsonb({"source": "none", "note": "G1 通过后才运行批判关" if passed else "G1 未通过，未运行批判关"})})
    await db.execute("INSERT INTO scout.briefs (kind, asof, content, payload) VALUES ('gate', :d, :c, CAST(:p AS JSONB))",
                     {"d": date.today(), "c": pc.report_text({**res, "rows": rows}), "p": db.jsonb(res)})
    en = await learned.get(pc.KEY)
    update = {"key": pc.KEY, "evidence": [*en.get("evidence", []), {"source": "gate", "date": str(date.today()), "summary": summary(res)}]}
    if not passed:
        update.update(status="rejected", polarity="negative")
    await learned.write(update, "system", "G1 通过，待批判关" if passed else "检验关未通过（G1）")
    return {"passed": passed}


def main() -> None:
    if sys.argv[1:] == ["compute"]:
        compute()
    elif sys.argv[1:] == ["record"]:
        from scout.jobs._run import run

        run("pullback_calm", record, alert=False)
    else:
        raise SystemExit("usage: python -m scout.jobs.pullback_calm compute | record")


if __name__ == "__main__":
    main()
