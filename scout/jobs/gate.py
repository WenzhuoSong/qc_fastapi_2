"""The gate: `python -m scout.jobs.gate register <spec.yaml>` | `run <key> [<key> ...]`.

register: G0, stores the pre-registered spec on a `hypothesis` entry (see `scout.gate`).
run:      G1 backtest + G2 critique for a registered entry; the run is recorded in
          `scout.gate_runs`, the entry gets the evidence and moves to `backtested` or
          `rejected`, and the report is stored as a brief (`gate`).
"""
import sys
from datetime import date

from scout import db, gate
from scout.jobs._run import run


async def main() -> dict:
    if len(sys.argv) < 3 or sys.argv[1] not in ("register", "run"):
        raise SystemExit("usage: python -m scout.jobs.gate register <spec.yaml> | run <key> [<key> ...]")
    if sys.argv[1] == "register":
        entry = await gate.register(gate.load_spec(sys.argv[2]))
        return {"registered": entry["key"], "version": entry["version"], "status": entry["status"]}
    from scout.pipeline import load_panel

    p = await load_panel(lookback_days=None) if len(sys.argv) > 3 else None   # several keys share one panel
    out = {}
    for key in sys.argv[2:]:
        res = await gate.run(key, p)
        text = gate.report_text(res)
        print(text, flush=True)
        await db.execute("INSERT INTO scout.briefs (kind, asof, content, payload) VALUES ('gate', :d, :c, CAST(:p AS JSONB))",
                         {"d": date.today(), "c": text, "p": db.jsonb(res)})
        out[res["key"]] = {"passed": res["passed"], "status": res["status"]}
    return out


if __name__ == "__main__":
    run("gate", main, alert=False)
