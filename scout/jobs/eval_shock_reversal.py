"""Shock reversal in quality firms without an earnings event: `python -m scout.jobs.eval_shock_reversal compute <trials> | record`.

Pre-registered in `scout/knowledge/specs/shock_reversal_quality_no_earnings.yaml`; runs once.
compute: local only (Sharadar files, no database), writes summary numbers to git-ignored
         `exports/shock_reversal/result.json` and refuses a second run; <trials> is this run's
         number in the short_term_reversal family (count in `scout.gate_runs` + 1).
record:  registers the entry (hypothesis) if needed, then records the run in `scout.gate_runs`,
         a brief (kind `gate`) and the knowledge entry (rejected when G1 fails).
"""
import asyncio
import json
import sys
from datetime import date
from pathlib import Path

from scout import shock_reversal as sk

OUT = Path("exports/shock_reversal/result.json")
SPEC = Path("scout/knowledge/specs/shock_reversal_quality_no_earnings.yaml")


def compute(trials: int) -> dict:
    if OUT.exists():
        raise SystemExit("already run (one run per pre-registration)")
    res = sk.run(trials)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(res, ensure_ascii=False, indent=1, default=str))
    print(sk.report_text(json.loads(OUT.read_text())), flush=True)
    return {"pass": res["rule"]["pass"], "t": res["rule"].get("all", {}).get("t")}


async def record() -> dict:
    from scout import db, gate, learned

    res = json.loads(OUT.read_text())
    if await db.fetch_one("SELECT 1 FROM scout.gate_runs WHERE key = :k", {"k": sk.KEY}):
        raise SystemExit("already recorded")
    if res["trials"] != await gate.trials_in(sk.FAMILY) + 1:
        raise SystemExit(f"family count changed: computed as trial {res['trials']}, database says {await gate.trials_in(sk.FAMILY) + 1}")
    spec = gate.load_spec(str(SPEC))
    if not await learned.get(sk.KEY):
        await learned.write({"key": sk.KEY, "spec": spec, "kind": "rule", "polarity": "positive", "status": "hypothesis",
                             "title": spec["title"], "claim": spec["claim"], "mechanism": spec["mechanism"],
                             "context": {"kind": ["stock"]}, "applies": {}, "evidence": []},
                            "system", "预登记（规格 2026-10-30 在运行前提交到 git，数据库登记在运行之后，因运行时数据库不可用）")
    s, passed = res["rule"], res["rule"]["pass"]
    text = sk.report_text(res)
    await db.execute("INSERT INTO scout.gate_runs (key, family, spec, backtest, critique, passed) VALUES "
                     "(:k, :f, CAST(:s AS JSONB), CAST(:b AS JSONB), CAST(:c AS JSONB), :ok)",
                     {"k": sk.KEY, "f": sk.FAMILY, "s": db.jsonb(spec), "b": db.jsonb(res), "ok": False,
                      "c": db.jsonb({"source": "none", "note": "G1 通过后才运行批判关" if passed else "G1 未通过，未运行批判关"})})
    await db.execute("INSERT INTO scout.briefs (kind, asof, content, payload) VALUES ('gate', :d, :c, CAST(:p AS JSONB))",
                     {"d": date.today(), "c": text, "p": db.jsonb(res)})
    a = s.get("all") or {}
    summary = (f"Sharadar 点时市值前 1000，{s.get('first_date')} 至 {s.get('last_date')}，{s.get('n')} 次：21 日 {a.get('edge', 0):+.2%}"
               f"（t {a.get('t')}，门槛 {s['required_t']}），前半 {(s.get('first') or {}).get('edge') or 0:+.2%} / "
               f"后半 {(s.get('second') or {}).get('edge') or 0:+.2%}；本族第 {res['trials']} 次检验")
    en = await learned.get(sk.KEY)
    update = {"key": sk.KEY, "evidence": [*en.get("evidence", []), {"source": "gate", "date": str(date.today()), "summary": summary}]}
    if not passed:
        update.update(status="rejected", polarity="negative")
    await learned.write(update, "system", ("G1 通过，待批判关" if passed else "检验关未通过（G1）")[:900])
    return {"passed_g1": passed, "t": a.get("t")}


def main() -> None:
    if len(sys.argv) >= 3 and sys.argv[1] == "compute":
        print(compute(int(sys.argv[2])))
    elif len(sys.argv) == 2 and sys.argv[1] == "record":
        from scout.jobs._run import run

        run("eval_shock_reversal", record, alert=False)
    else:
        raise SystemExit("usage: python -m scout.jobs.eval_shock_reversal compute <trials> | record")


if __name__ == "__main__":
    main()
