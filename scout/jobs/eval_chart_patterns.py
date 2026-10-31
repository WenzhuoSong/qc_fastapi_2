"""Chart patterns (Lo, Mamaysky & Wang 2000): `python -m scout.jobs.eval_chart_patterns compute | record`.

Pre-registered in `scout/knowledge/specs/chart_patterns_lmw.yaml`; runs once.
compute: local only (Sharadar files, no database), writes summary numbers to git-ignored
         `exports/chart_patterns/result.json` and refuses a second run.
record:  registers one knowledge entry per pattern (hypothesis) if needed, then records the run in
         `scout.gate_runs`, a brief (kind `gate`) and each entry (rejected when G1 fails).
"""
import json
import sys
from datetime import date
from pathlib import Path

from scout import chart_patterns as cp

OUT = Path("exports/chart_patterns/result.json")
SPEC = Path("scout/knowledge/specs/chart_patterns_lmw.yaml")


def compute() -> dict:
    if OUT.exists():
        raise SystemExit("already run (one run per pre-registration)")
    res = cp.run()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(res, ensure_ascii=False, indent=1, default=str))
    print(cp.report_text(json.loads(OUT.read_text())), flush=True)
    return {n: s["verdict"] for n, s in res["patterns"].items()}


async def record() -> dict:
    from scout import db, gate, learned

    res = json.loads(OUT.read_text())
    if await db.fetch_one("SELECT 1 FROM scout.gate_runs WHERE key = :k", {"k": cp.KEY}):
        raise SystemExit("already recorded")
    spec = gate.load_spec(str(SPEC))
    text = cp.report_text(res)
    passed = {n for n, s in res["patterns"].items() if s["verdict"] == "pass"}
    await db.execute("INSERT INTO scout.gate_runs (key, family, spec, backtest, critique, passed) VALUES "
                     "(:k, :f, CAST(:s AS JSONB), CAST(:b AS JSONB), CAST(:c AS JSONB), :ok)",
                     {"k": cp.KEY, "f": cp.FAMILY, "s": db.jsonb(spec), "b": db.jsonb(res), "ok": False,
                      "c": db.jsonb({"source": "none", "note": "G1 通过的形态待批判关" if passed else "全部 G1 未通过，未运行批判关"})})
    await db.execute("INSERT INTO scout.briefs (kind, asof, content, payload) VALUES ('gate', :d, :c, CAST(:p AS JSONB))",
                     {"d": date.today(), "c": text, "p": db.jsonb(res)})
    for n, s in res["patterns"].items():
        key = f"chart_pattern_{n.lower()}"
        if not await learned.get(key):
            await learned.write({"key": key, "spec": spec, "kind": "rule", "polarity": "positive", "status": "hypothesis",
                                 "title": f"{cp.ZH[n]}（{n}）之后的走势", "claim": spec["claim"], "mechanism": spec["mechanism"],
                                 "context": {"kind": ["stock"]}, "applies": {}, "evidence": []},
                                "system", "预登记（规格 2026-10-31 在运行前提交到 git，数据库登记在运行之后）")
        a = s.get("all") or {}
        summary = (f"Sharadar 点时市值前 1000，{s.get('first_date')} 至 {s.get('last_date')}，{s.get('n')} 次：21 日 "
                   f"{(a.get('edge') or 0):+.2%}（t {a.get('t')}，门槛 ±{s['required_t']}），前半 {(s.get('first') or {}).get('edge') or 0:+.2%} / "
                   f"后半 {(s.get('second') or {}).get('edge') or 0:+.2%}；判定 {s['verdict']}")
        en = await learned.get(key)
        update = {"key": key, "evidence": [*en.get("evidence", []), {"source": "gate", "date": str(date.today()), "summary": summary}]}
        if s["verdict"] != "pass":
            update.update(status="rejected", polarity="negative")
        await learned.write(update, "system", ("G1 通过，待批判关" if s["verdict"] == "pass" else "检验关未通过（G1）"))
    return {"passed_g1": sorted(passed)}


def main() -> None:
    if sys.argv[1:] == ["compute"]:
        print(compute())
    elif sys.argv[1:] == ["record"]:
        from scout.jobs._run import run

        run("eval_chart_patterns", record, alert=False)
    else:
        raise SystemExit(__doc__)


if __name__ == "__main__":
    main()
