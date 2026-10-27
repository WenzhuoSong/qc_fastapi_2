"""Regression check for the AI interpretation workflow.

Runs `scout.insight.generate` on a fixed, varied ticker set and reports how
often the LLM output passes the deterministic checks first time, how often it
falls back to rule readings, and which checks fail most. Run after changing
prompts, lenses, the feature dictionary or the model:

    python -m scout.jobs.eval_insight [TICKER ...]
"""
import sys
import time
from collections import Counter

from scout import db, insight
from scout.jobs._run import run

# Strong / weak / ETFs / sectors / defensives / high-beta — deliberately mixed.
EVAL_SET = [
    "NVDA", "AAPL", "MSFT", "AVGO", "PLTR", "TSLA", "CRWV", "COIN", "LLY", "JPM",
    "XOM", "PBF", "CART", "KO", "WMT", "XLE", "XLK", "SMH", "TLT", "GLD",
]


async def evaluate(tickers: list[str]) -> dict:
    rows, issue_kinds = [], Counter()
    for t in tickers:
        t0 = time.time()
        rec = await insight.generate(t)
        if rec is None:
            rows.append({"ticker": t, "status": "no_data"})
            continue
        hist = rec["validation"]["history"]
        for h in hist:
            for issue in h["issues"]:
                issue_kinds[issue.split("：")[0][:24]] += 1
        rows.append({
            "ticker": t, "source": rec["source"], "attempts": rec["validation"]["attempts"],
            "first_pass": rec["validation"].get("first_pass", bool(hist) and not hist[0]["issues"]),
            "seconds": round(time.time() - t0, 1), "fallback_groups": rec["validation"].get("fallback_groups", []),
            "driver": rec["insight"].get("driver"), "confidence": rec["insight"].get("confidence"),
        })
        print(f"{t:6} {rec['source']:8} attempts={rows[-1]['attempts']} first_pass={rows[-1]['first_pass']} "
              f"{rows[-1]['seconds']}s  {rec['insight']['headline']}", flush=True)
    done = [r for r in rows if "source" in r]
    n = len(done) or 1
    summary = {
        "tickers": len(tickers),
        "evaluated": len(done),
        "first_pass_rate": round(sum(r["first_pass"] for r in done) / n, 3),
        "llm_rate": round(sum(r["source"] == "llm" for r in done) / n, 3),
        "fallback_rate": round(sum(r["source"] == "fallback" for r in done) / n, 3),
        "partial_rate": round(sum(r["source"] == "partial" for r in done) / n, 3),
        "avg_seconds": round(sum(r["seconds"] for r in done) / n, 1),
        "issue_kinds": dict(issue_kinds.most_common()),
        "rows": rows,
    }
    await db.execute(
        "INSERT INTO scout.briefs (kind, asof, content, payload) VALUES ('insight_eval', CURRENT_DATE, :c, CAST(:p AS JSONB))",
        {"c": f"first_pass={summary['first_pass_rate']} llm={summary['llm_rate']} fallback={summary['fallback_rate']}",
         "p": db.jsonb(summary)},
    )
    return {k: v for k, v in summary.items() if k != "rows"}


if __name__ == "__main__":
    picked = [a.upper() for a in sys.argv[1:]] or EVAL_SET
    run("eval_insight", lambda: evaluate(picked), alert=False)
