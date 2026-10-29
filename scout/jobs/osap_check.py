"""External replication on Open Source Asset Pricing: `python -m scout.jobs.osap_check` (local only).

Reads the git-ignored local data (`exports/research_data/osap/deciles_selected.csv`), Scout's own
verdicts from `scout.gate_runs`, prints and stores the report as a brief (kind `osap_replication`)
and appends the result to each strategy's knowledge entry as evidence (source `osap`, status
unchanged). Pre-registered in `scout/knowledge/external_specs/osap_replication.yaml`.
"""
import json
from datetime import date

from scout import db, learned, osap_check
from scout.jobs._run import run


async def scout_verdicts() -> dict[str, dict]:
    """The latest original (not plan-exit) gate run of each mapped ranking spec."""
    out = {}
    for key in osap_check.MAPPING:
        row = await db.fetch_one("SELECT spec, backtest, passed FROM scout.gate_runs WHERE key = :k ORDER BY id DESC LIMIT 1", {"k": key})
        if not row:
            continue
        spec = row["spec"] if isinstance(row["spec"], dict) else json.loads(row["spec"])
        bt = row["backtest"] if isinstance(row["backtest"], dict) else json.loads(row["backtest"])
        sel = bt["summary"]["vs_select"]
        out[key] = {"passed": bool(row["passed"]), "edge": sel["edge"], "t": sel["t"], "hold": int(spec["hold"])}
    return out


async def main() -> dict:
    if await db.fetch_one("SELECT 1 FROM scout.briefs WHERE kind = 'osap_replication' LIMIT 1"):
        raise SystemExit("already run (one run per pre-registration)")
    res = osap_check.analyze(osap_check.load(), await scout_verdicts())
    text = osap_check.report_text(res)
    print(text, flush=True)
    await db.execute("INSERT INTO scout.briefs (kind, asof, content, payload) VALUES ('osap_replication', :d, :c, CAST(:p AS JSONB))",
                     {"d": date.today(), "c": text, "p": db.jsonb(res)})
    written = 0
    for key, r in res.items():
        en = await learned.get(key)
        v = r[osap_check.PRIMARY]
        if not en or not v:
            continue
        summary = (f"外部复现（Open Source Asset Pricing {r['signal']}，{osap_check.START[:7]} 至 {osap_check.END[:7]}，市值加权第 10 组 − 十组平均）："
                   f"月均 {v['all']['edge']:+.2f}%（t {v['all']['t']}），前半 {v['first']['edge']:+.2f}% / 后半 {v['second']['edge']:+.2f}%，"
                   f"发表后 {v['post_pub']['edge']:+.2f}%（t {v['post_pub']['t']}），2015-11 起 {v['overlap']['edge']:+.2f}%；判断：{r['category']}。"
                   "因子层面的外部证据，不改变检验关判定。")
        await learned.write({"key": key, "evidence": [*en.get("evidence", []), {"source": "osap", "date": str(date.today()), "summary": summary}]},
                            "system", "补充外部证据（Open Source Asset Pricing）")
        written += 1
    return {"signals": len(res), "evidence_written": written}


if __name__ == "__main__":
    run("osap_check", main, alert=False)
