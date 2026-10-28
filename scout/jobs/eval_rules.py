"""Fair benchmarks for every rule: `python -m scout.jobs.eval_rules`.

The base-rate table judges a rule against "holding the same stock at its average drift",
and that drift is the stock's mean over the whole stored history: it uses the future, and
with today's index members (yesterday's winners) it is a very high bar. This study keeps
every rule, signal, fill and exit exactly as the replay has them and compares each filled
trade with benchmarks that use nothing the signal could not have known (`scout.gate`, G1):
SPY and the equal-weighted universe over the same entry → exit window, a timing control
(same stock, random other dates) and a selection control (random other stocks, same date).
Stored as a brief (`rule_benchmarks`). One registered idea goes through `scout.jobs.gate`.
"""
from __future__ import annotations

from datetime import date

from scout import db, gate
from scout.detectors import registry
from scout.gate import monthly_t  # noqa: F401  (kept importable here)
from scout.jobs._run import run
from scout.pipeline import load_panel


def _f(e: dict) -> str:
    return "—" if e["edge"] is None else f"{e['edge']:+.2%}" + (f"（t {e['t']:+.1f}）" if e["t"] is not None else "")


async def study() -> dict:
    p = await load_panel(lookback_days=None)
    m = gate.Market(p)
    out: dict = {"detectors": {}, "seed": gate.SEED, "draws": gate.DRAWS}
    lines = ["规则的公平基准（12 年回放，信号、成交、出场与回放完全相同；差值按每个信号配对，t 值按月度均值计算）",
             "规则 | 成交笔数 | 平均净收益 | 超额：SPY / 等权股票池 | 时机对照（同股随机日） 前半 / 后半 | 选股对照（同日随机股） 前半 / 后半"]
    for det in registry(include_candidates=True):
        res = gate.summarize(gate.backtest(p, det, m))
        if not res["n"]:
            continue
        res = {"label": det.label, "live": det.live, **res}
        out["detectors"][det.name] = res
        lines.append(f"{det.label}{'' if det.live else '（候选）'} | {res['n']} | {res['avg_net']:+.2%} | "
                     f"{_f(res['vs_spy'])} / {_f(res['vs_ew'])} | {_f(res['vs_timing_first'])} / {_f(res['vs_timing_second'])} | "
                     f"{_f(res['vs_select_first'])} / {_f(res['vs_select_second'])}")
    text = "\n".join(lines)
    print(text)
    await db.execute("INSERT INTO scout.briefs (kind, asof, content, payload) VALUES ('rule_benchmarks', :d, :c, CAST(:p AS JSONB))",
                     {"d": date.today(), "c": text, "p": db.jsonb(out)})
    return {k: v["n"] for k, v in out["detectors"].items()}


if __name__ == "__main__":
    run("eval_rules", study, alert=False)
