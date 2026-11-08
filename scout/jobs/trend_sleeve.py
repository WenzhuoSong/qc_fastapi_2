"""Core rule plus a 20 % trend sleeve (pre-registered in `scout/knowledge/external_specs/trend_sleeve_v1.yaml`).

compute: local only (carver_trend_v1's result file and the Sharadar ETF prices; no database), writes the summary
         to git-ignored `exports/trend_sleeve/result.json` and refuses a second run.
record:  registers the entry (hypothesis), then records the run in `scout.gate_runs` (family cross_asset_trend),
         a brief (kind `gate`) and the knowledge entry (backtested when a blend qualifies, rejected otherwise).
"""
import json
import sys
from datetime import date
from pathlib import Path

from scout import trend_sleeve as ts

OUT = Path("exports/trend_sleeve/result.json")
SPEC = Path("scout/knowledge/external_specs/trend_sleeve_v1.yaml")
FAMILY = "cross_asset_trend"


def compute() -> None:
    if OUT.exists():
        raise SystemExit("already run (one run per pre-registration)")
    s, rf = ts.load()
    ev = ts.evaluate(ts.portfolios(s, rf), rf)
    res = {"key": ts.KEY, "evaluation": ev, "decision": ts.decide(ev)}
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(res, ensure_ascii=False, indent=1, default=str))
    print(ts.report_text(json.loads(OUT.read_text())), flush=True)


async def record() -> dict:
    from scout import db, gate, learned

    res = json.loads(OUT.read_text())
    if await db.fetch_one("SELECT 1 FROM scout.gate_runs WHERE key = :k", {"k": ts.KEY}):
        raise SystemExit("already recorded")
    spec = gate.load_spec(str(SPEC))
    dec, v = res["decision"], res["evaluation"]["variants"]
    if not await learned.get(ts.KEY):
        await learned.write({"key": ts.KEY, "spec": spec, "kind": "rule", "polarity": "positive", "status": "hypothesis",
                             "title": spec["title"], "claim": spec["question"], "mechanism": "趋势配置与股票低相关、在股市大跌时为正（carver_trend_v1）",
                             "context": {"kind": ["portfolio"]}, "applies": {}, "evidence": []},
                            "system", "预登记（规格 2026-11-08 在运行前提交到 git，数据库登记在运行之后：本地运行不连数据库）")
    await db.execute("INSERT INTO scout.gate_runs (key, family, spec, backtest, critique, passed) VALUES "
                     "(:k, :f, CAST(:s AS JSONB), CAST(:b AS JSONB), CAST(:c AS JSONB), :ok)",
                     {"k": ts.KEY, "f": FAMILY, "s": db.jsonb(spec), "b": db.jsonb(res), "ok": dec["passed"],
                      "c": db.jsonb({"source": "none", "note": "组合层面的配置规则，按预登记的判定标准，不经批判关"})})
    text = ts.report_text(res)
    await db.execute("INSERT INTO scout.briefs (kind, asof, content, payload) VALUES ('gate', :d, :c, CAST(:p AS JSONB))",
                     {"d": date.today(), "c": text, "p": db.jsonb(res)})
    summary = "；".join(f"{ts.LABELS[k]} 夏普 {v[k]['sharpe']:.2f}（{v[k]['sharpe_first']:.2f} / {v[k]['sharpe_second']:.2f}）回撤 {v[k]['max_dd']:+.1%}"
                       for k in (*ts.PRIMARY, "core_now", "core_cash"))
    en = await learned.get(ts.KEY)
    update = {"key": ts.KEY, "evidence": [*en.get("evidence", []), {"source": "gate", "date": str(date.today()), "summary": summary}],
              "status": "backtested" if dec["passed"] else "rejected", "polarity": "positive" if dec["passed"] else "negative"}
    await learned.write(update, "system", f"通过：{ts.LABELS[dec['pick']]}，是否采用由用户决定" if dec["passed"] else "两个组合都未满足预登记的全部条件")
    return {"passed": dec["passed"], "pick": dec["pick"]}


def main() -> None:
    if sys.argv[1:] == ["compute"]:
        compute()
    elif sys.argv[1:] == ["record"]:
        from scout.jobs._run import run

        run("trend_sleeve", record, alert=False)
    else:
        raise SystemExit("usage: python -m scout.jobs.trend_sleeve compute | record")


if __name__ == "__main__":
    main()
