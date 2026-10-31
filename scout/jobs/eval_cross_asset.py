"""Cross-asset trend (pre-registered in `scout/knowledge/external_specs/cross_asset_trend_v1.yaml`):
`python -m scout.jobs.eval_cross_asset compute | record`.

compute: local only (Sharadar ETF prices, French T-bill rate; no database), writes summary numbers to
         git-ignored `exports/cross_asset/result.json` and refuses a second run.
record:  the knowledge entry `cross_asset_trend` (status backtested when a variant is a core candidate,
         rejected otherwise; only the user confirms), the run in `scout.gate_runs` and a brief (kind `gate`).
"""
import json
import sys
from datetime import date
from pathlib import Path

import yaml

from scout import cross_asset as ca

OUT = Path("exports/cross_asset/result.json")
SPEC = Path("scout/knowledge/external_specs/cross_asset_trend_v1.yaml")


def compute() -> dict:
    if OUT.exists():
        raise SystemExit("already run (one run per pre-registration)")
    res = ca.run()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(res, ensure_ascii=False, indent=1, default=str))
    print(ca.report_text(json.loads(OUT.read_text())), flush=True)
    return {"pick": res["decision"]["pick"]}


async def record() -> dict:
    from scout import db, learned

    res = json.loads(OUT.read_text())
    if await db.fetch_one("SELECT 1 FROM scout.gate_runs WHERE key = :k", {"k": ca.KEY}):
        raise SystemExit("already recorded")
    spec = yaml.safe_load(SPEC.read_text())
    text = ca.report_text(res)
    pick = res["decision"]["pick"]
    await db.execute("INSERT INTO scout.gate_runs (key, family, spec, backtest, critique, passed) VALUES "
                     "(:k, :f, CAST(:s AS JSONB), CAST(:b AS JSONB), CAST(:c AS JSONB), :ok)",
                     {"k": ca.KEY, "f": ca.FAMILY, "s": db.jsonb(spec), "b": db.jsonb(res), "ok": bool(pick),
                      "c": db.jsonb({"source": "none", "note": "组合层面的配置规则，按预登记的判定标准，不经批判关"})})
    await db.execute("INSERT INTO scout.briefs (kind, asof, content, payload) VALUES ('gate', :d, :c, CAST(:p AS JSONB))",
                     {"d": date.today(), "c": text, "p": db.jsonb(res)})
    key = "cross_asset_trend"
    if not await learned.get(key):
        await learned.write({"key": key, "spec": spec, "kind": "rule", "polarity": "positive", "status": "hypothesis",
                             "title": "跨资产趋势跟踪作为核心仓规则", "claim": spec["question"],
                             "mechanism": "；".join(spec["literature"][:3]), "context": {"kind": ["portfolio"]},
                             "applies": {}, "evidence": []},
                            "system", "预登记（规格 2026-10-31 在运行前提交到 git，数据库登记在运行之后）")
    en = await learned.get(key)
    update = {"key": key, "evidence": [*en.get("evidence", []), {"source": "cross_asset", "date": str(date.today()),
                                                                  "summary": text}]}
    update.update(status="backtested" if pick else "rejected", polarity="positive" if pick else "negative")
    await learned.write(update, "system", f"候选：{ca.TRIALS[pick]}，待用户决定" if pick else "没有变体满足预登记的全部条件")
    return {"pick": pick}


def main() -> None:
    if sys.argv[1:] == ["compute"]:
        print(compute())
    elif sys.argv[1:] == ["record"]:
        from scout.jobs._run import run

        run("eval_cross_asset", record, alert=False)
    else:
        raise SystemExit(__doc__)


if __name__ == "__main__":
    main()
