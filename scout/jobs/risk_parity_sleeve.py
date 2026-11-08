"""Core rule plus a 20 % risk-parity sleeve (pre-registered in `scout/knowledge/external_specs/risk_parity_sleeve_v1.yaml`).

compute: local only (French files, FRED DGS10 fetched once and saved, Sharadar SPY / IEF for the reported later
         sample; no database), writes the summary to git-ignored `exports/risk_parity/result.json`, refuses a second run.
record:  registers the entry, then records the run in `scout.gate_runs` (family risk_parity_sleeve), a brief
         (kind `gate`) and the knowledge entry (backtested when it passed, rejected otherwise).
"""
import asyncio
import json
import sys
from datetime import date
from pathlib import Path

import pandas as pd

from scout import cross_asset as ca
from scout import risk_parity as rp

OUT = Path("exports/risk_parity/result.json")
RAW = Path("exports/risk_parity/dgs10.json")
SPEC = Path("scout/knowledge/external_specs/risk_parity_sleeve_v1.yaml")
FAMILY = "risk_parity_sleeve"


def compute() -> None:
    from scout.data import fred

    if OUT.exists():
        raise SystemExit("already run (one run per pre-registration)")
    daily = asyncio.run(fred.series("DGS10", date(1962, 1, 1)))
    if not daily or daily[0][0] > date(1962, 2, 1):
        raise SystemExit("FRED DGS10 unavailable or too short")
    OUT.parent.mkdir(parents=True, exist_ok=True)
    RAW.write_text(json.dumps([(str(d), v) for d, v in daily]))
    bd = rp.bond_returns(rp.month_end_yields(daily))
    fm = ca.french_monthly()
    eq, rf = fm["mkt"], fm["rf"]
    p, lev = rp.portfolios(eq, bd, rf)
    judged = rp.evaluate(p, rf, *rp.JUDGED)
    later = rp.evaluate(p, rf, pd.Period("2003-09", "M"), p["core_now"].index[-1])
    px, rfd = ca.load(("SPY", "IEF"))
    etf = ca.monthly(px.pct_change().dropna())
    rf_m = (1 + rfd).groupby(rfd.index.to_period("M")).prod() - 1
    complete = {px.index[i].to_period("M") for i in ca.month_end_rows(px.index)}
    etf = etf[etf.index.isin(complete)]
    pe, _ = rp.portfolios(etf["SPY"], etf["IEF"], rf_m)
    both = pd.DataFrame({"built": bd, "ief": etf["IEF"]}).dropna()
    res = {"key": rp.KEY, "judged": judged, "decision": rp.decide(judged), "later": later,
           "later_etf": rp.evaluate(pe, rf_m, pd.Period("2003-09", "M"), pe["core_now"].index[-1]),
           "leverage": {"judged_mean": float(lev[(lev.index >= rp.JUDGED[0]) & (lev.index <= rp.JUDGED[1])].mean()),
                        "judged_max": float(lev[(lev.index >= rp.JUDGED[0]) & (lev.index <= rp.JUDGED[1])].max())},
           "data_check": {"corr": float(both["built"].corr(both["ief"])), "months": int(len(both)),
                          "ann_diff": float((1 + both["built"]).prod() ** (12 / len(both)) - (1 + both["ief"]).prod() ** (12 / len(both)))}}
    OUT.write_text(json.dumps(res, ensure_ascii=False, indent=1, default=str))
    print(rp.report_text(json.loads(OUT.read_text())), flush=True)


async def record() -> dict:
    from scout import db, gate, learned

    res = json.loads(OUT.read_text())
    if await db.fetch_one("SELECT 1 FROM scout.gate_runs WHERE key = :k", {"k": rp.KEY}):
        raise SystemExit("already recorded")
    spec = gate.load_spec(str(SPEC))
    passed = res["decision"]["passed"]
    if not await learned.get(rp.KEY):
        await learned.write({"key": rp.KEY, "spec": spec, "kind": "rule", "polarity": "positive", "status": "hypothesis",
                             "title": spec["title"], "claim": spec["question"], "mechanism": "股债风险平价与股票低相关；Asness, Frazzini & Pedersen (2012)",
                             "context": {"kind": ["portfolio"]}, "applies": {}, "evidence": []},
                            "system", "预登记（规格 2026-11-08 在运行前提交到 git，数据库登记在运行之后：本地运行不连数据库）")
    await db.execute("INSERT INTO scout.gate_runs (key, family, spec, backtest, critique, passed) VALUES "
                     "(:k, :f, CAST(:s AS JSONB), CAST(:b AS JSONB), CAST(:c AS JSONB), :ok)",
                     {"k": rp.KEY, "f": FAMILY, "s": db.jsonb(spec), "b": db.jsonb(res), "ok": passed,
                      "c": db.jsonb({"source": "none", "note": "组合层面的配置规则，按预登记的判定标准，不经批判关"})})
    text = rp.report_text(res)
    await db.execute("INSERT INTO scout.briefs (kind, asof, content, payload) VALUES ('gate', :d, :c, CAST(:p AS JSONB))",
                     {"d": date.today(), "c": text, "p": db.jsonb(res)})
    v = res["judged"]["variants"]
    summary = (f"1965–2003（未见）：80% 核心 + 20% 风险平价 夏普 {v['core_rp']['sharpe']:.2f}（{v['core_rp']['sharpe_first']:.2f} / "
               f"{v['core_rp']['sharpe_second']:.2f}）回撤 {v['core_rp']['max_dd']:+.1%}；现行规则 {v['core_now']['sharpe']:.2f} 回撤 {v['core_now']['max_dd']:+.1%}；"
               f"80/20 现金 {v['core_cash']['sharpe']:.2f}；{'通过' if passed else '未通过'}")
    en = await learned.get(rp.KEY)
    update = {"key": rp.KEY, "evidence": [*en.get("evidence", []), {"source": "gate", "date": str(date.today()), "summary": summary}],
              "status": "backtested" if passed else "rejected", "polarity": "positive" if passed else "negative"}
    await learned.write(update, "system", "通过，是否采用由用户决定" if passed else "未满足预登记的全部条件")
    return {"passed": passed}


def main() -> None:
    if sys.argv[1:] == ["compute"]:
        compute()
    elif sys.argv[1:] == ["record"]:
        from scout.jobs._run import run

        run("risk_parity_sleeve", record, alert=False)
    else:
        raise SystemExit("usage: python -m scout.jobs.risk_parity_sleeve compute | record")


if __name__ == "__main__":
    main()
