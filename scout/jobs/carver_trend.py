"""Carver-style multi-asset trend (pre-registered in `scout/knowledge/external_specs/carver_trend_v1.yaml`).

compute: local only (Sharadar ETF files; no database), writes the summary to git-ignored
         `exports/carver_trend/result.json` and refuses a second run.
record:  registers the entry (hypothesis), then records the run in `scout.gate_runs` (family cross_asset_trend),
         a brief (kind `gate`) and the knowledge entry (rejected when it failed).
"""
import json
import sys
from datetime import date
from pathlib import Path

from scout import carver_trend as ct

OUT = Path("exports/carver_trend/result.json")
SPEC = Path("scout/knowledge/external_specs/carver_trend_v1.yaml")
FAMILY = "cross_asset_trend"


def compute() -> None:
    if OUT.exists():
        raise SystemExit("already run (one run per pre-registration)")
    raw = ct.run()
    ev = ct.evaluate(raw)
    res = {"key": ct.KEY, "start": raw["start"], "end": raw["px_end"], "evaluation": ev, "decision": ct.decide(ev), "extra": raw["extra"],
           "monthly": {k: {str(m): float(x) for m, x in ct.ca.monthly(v["ret"]).items()} for k, v in raw["results"].items()}}
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(res, ensure_ascii=False, indent=1, default=str))
    print(ct.report_text(json.loads(OUT.read_text())), flush=True)


def summary(res: dict) -> str:
    v, dec = res["evaluation"]["variants"], res["decision"]
    s, h, d = v["system"], v["hold"], res["evaluation"]["system_vs_hold"]["all"]
    return (f"12 只资产类别 ETF，{res['evaluation']['from']} 起，Carver 式（6 速度 EWMAC 合成、速度上限 0.13、波动目标 12%、10% 缓冲，做多做空）："
            f"夏普 {s['sharpe']:.2f}（前 {s['sharpe_first']:.2f} / 后 {s['sharpe_second']:.2f}），一直做多的风险平价 {h['sharpe']:.2f}"
            f"（{h['sharpe_first']:.2f} / {h['sharpe_second']:.2f}）；每月差 {d['mean']:+.2%}（t {d['t']:.2f}，门槛 {dec['required_t']}）；"
            f"最大回撤 {s['max_dd']:+.1%} 对 {h['max_dd']:+.1%}；多速度合成有用 = {dec['combination_helps']}，速度上限有用 = {dec['speed_limit_helps']}。"
            f"本族第 {ct.TRIALS} 次检验")


async def record() -> dict:
    from scout import db, gate, learned

    res = json.loads(OUT.read_text())
    if await db.fetch_one("SELECT 1 FROM scout.gate_runs WHERE key = :k", {"k": ct.KEY}):
        raise SystemExit("already recorded")
    if ct.TRIALS != await gate.trials_in(FAMILY) + 1:
        raise SystemExit(f"family count changed: computed as trial {ct.TRIALS}, database says {await gate.trials_in(FAMILY) + 1}")
    spec = gate.load_spec(str(SPEC))
    if not await learned.get(ct.KEY):
        await learned.write({"key": ct.KEY, "spec": spec, "kind": "rule", "polarity": "positive", "status": "hypothesis",
                             "title": spec["title"], "claim": spec["question"], "mechanism": "; ".join(spec["literature"]),
                             "context": {"kind": ["etf"]}, "applies": {}, "evidence": []},
                            "system", "预登记（规格 2026-11-08 在运行前提交到 git，数据库登记在运行之后：本地运行不连数据库）")
    passed = res["decision"]["passed"]
    res.pop("monthly")
    await db.execute("INSERT INTO scout.gate_runs (key, family, spec, backtest, critique, passed) VALUES "
                     "(:k, :f, CAST(:s AS JSONB), CAST(:b AS JSONB), CAST(:c AS JSONB), :ok)",
                     {"k": ct.KEY, "f": FAMILY, "s": db.jsonb(spec), "b": db.jsonb(res), "ok": passed,
                      "c": db.jsonb({"source": "none", "note": "按预登记只用数值条件"})})
    await db.execute("INSERT INTO scout.briefs (kind, asof, content, payload) VALUES ('gate', :d, :c, CAST(:p AS JSONB))",
                     {"d": date.today(), "c": ct.report_text(res), "p": db.jsonb(res)})
    en = await learned.get(ct.KEY)
    update = {"key": ct.KEY, "evidence": [*en.get("evidence", []), {"source": "gate", "date": str(date.today()), "summary": summary(res)}]}
    update["status"] = "backtested" if passed else "rejected"
    if not passed:
        update["polarity"] = "negative"
    await learned.write(update, "system", "检验通过（数值条件），待用户决定是否前向记录" if passed else "检验未通过")
    return {"passed": passed}


def main() -> None:
    if sys.argv[1:] == ["compute"]:
        compute()
    elif sys.argv[1:] == ["record"]:
        from scout.jobs._run import run

        run("carver_trend", record, alert=False)
    else:
        raise SystemExit("usage: python -m scout.jobs.carver_trend compute | record")


if __name__ == "__main__":
    main()
