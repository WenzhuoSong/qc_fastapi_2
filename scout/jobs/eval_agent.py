"""Evaluation of the Q&A agent: `python -m scout.jobs.eval_agent [question numbers]`.

A fixed set of questions; for each, whether the expected tools were called and whether every number in the
answer appears in some tool output (the same idea as the thesis number guard: numbers come from Python).
Writes a brief (kind `agent_eval`). Rerun after changing the agent's prompt, tools or model.
"""
import asyncio
import re
import sys
import time

from scout import db
from scout.agent import SYSTEM_PROMPT, answer

QUESTIONS: list[tuple[str, set[str]]] = [
    ("我现在的持仓怎么样？哪只离止损最近？", {"get_holdings"}),
    ("PBF 最近为什么跌？是行业还是它自己的问题？", {"get_snapshot"}),
    ("MU 的支撑和阻力在哪里？现在算不算在买点？", {"get_price_map"}),
    ("上升趋势回调这个规则有用吗？", {"get_knowledge"}),
    ("我的卫星仓还能买多少？大盘过滤现在是什么状态？", {"get_holdings"}),
    ("SHOP 的 AI 解读怎么说的？", {"get_ai_insight"}),
    ("PBF 的估值和盈利预期怎么样？", {"get_fundamentals"}),
    ("CART 最近有什么异动，原因是什么？", {"get_events"}),
    ("我上周的交易执行得怎么样？", {"get_trade_journal"}),
    ("Scout 之前对 NVDA 的判断准不准？", {"get_ledger"}),
    ("今天的活跃机会有哪些，哪些是通过检验的规则？", {"list_opportunities"}),
    ("今天大盘怎么样，哪些板块最强？", {"get_market_overview"}),
    ("相对强度最高的 5 只股票是哪些？", {"screen"}),
    ("NVDA 下一次财报是什么时候？", {"get_earnings"}),
    ("如果 SHOP 跌到 140，按我的计划要卖吗？", {"get_holdings"}),
    ("今天盘前简报说了什么？", {"get_briefs"}),
    ("哪些板块在从落后转向改善？", {"get_sectors"}),
    ("Scout 现在看好哪个行业？之前的行业判断准吗？", {"get_outlook"}),
    ("最近有内部人集中买入的股票吗？", {"get_screens"}),
    ("突破回踩的前向验证进行到哪一步了？", {"get_research"}),
    ("这周有什么重要的经济数据和财报？", {"get_calendar"}),
    ("我的自选股里最近谁涨得最多？", {"get_watchlist"}),
    ("我的持仓是不是太集中了？组合风险怎么样？", {"get_holdings"}),
    ("内部人交易数据更新到什么时候了？", {"get_data_status"}),
    ("我想在 75 附近再买 200 股 PBF，你觉得怎么样？", {"check_trade_idea"}),
    ("打算买点 NVDA，财报前布局，合适吗？", {"check_trade_idea"}),
    ("SHOP 一直跌，我准备卖掉，要不要卖？", {"check_trade_idea"}),
]

NUM = re.compile(r"(?<![\w.])[-+−]?\d[\d,]*\.?\d*")
DATE = re.compile(r"\d{4}-\d{2}-\d{2}|\d{1,2}-\d{2}(?!\d)|\d{4} ?年 ?\d{1,2} ?月 ?\d{1,2} ?日|\d{1,2} ?月 ?\d{1,2} ?日")


def numbers(text: str) -> set[str]:
    """Numbers with at least two digits (small counts like '3 只' are noise), dates removed, normalised."""
    out = set()
    for m in NUM.findall(DATE.sub(" ", text)):
        n = m.replace(",", "").replace("−", "-").lstrip("+").rstrip(".")
        if len(n.replace("-", "").replace(".", "")) >= 2:
            out.add(n.lstrip("-"))
    return out


def unsupported(answer_text: str, outputs: list[str]) -> list[str]:
    pool = " ".join(outputs).replace(",", "")
    return sorted(n for n in numbers(answer_text) if n not in pool)


async def run(selected: list[int] | None = None) -> dict:
    rows = []
    for i, (q, expect) in enumerate(QUESTIONS, 1):
        if selected and i not in selected:
            continue
        trace: list = []
        t0 = time.time()
        try:
            text = await answer(q, channel="eval", remember=False, trace=trace)
            error = None
        except Exception as exc:
            text, error = "", f"{type(exc).__name__}: {exc}"[:200]
        used = {x["tool"] for x in trace}
        bad = unsupported(text, [x["output"] for x in trace] + [q, SYSTEM_PROMPT])
        rows.append({"n": i, "question": q, "tools": sorted(used), "expected_ok": expect <= used, "unsupported": bad,
                     "seconds": round(time.time() - t0, 1), "error": error, "answer": text})
        print(f"{i:2d} {'✓' if expect <= used else '✗'} tools={sorted(used)} unsupported={bad} {rows[-1]['seconds']}s "
              f"{error or text[:80].replace(chr(10), ' ')}", flush=True)
    n = len(rows)
    summary = {"questions": n, "tools_ok": sum(r["expected_ok"] for r in rows) / n if n else None,
               "numbers_ok": sum(not r["unsupported"] for r in rows) / n if n else None,
               "errors": sum(bool(r["error"]) for r in rows), "avg_seconds": round(sum(r["seconds"] for r in rows) / n, 1) if n else None}
    await db.execute("INSERT INTO scout.briefs (kind, asof, content, payload) VALUES ('agent_eval', CURRENT_DATE, :c, CAST(:p AS JSONB))",
                     {"c": str(summary), "p": db.jsonb({"summary": summary, "rows": rows})})
    return summary


def main() -> None:
    from scout.jobs._run import run as job

    selected = [int(x) for x in sys.argv[1:]] or None
    job("eval_agent", lambda: run(selected), alert=False)


if __name__ == "__main__":
    main()
