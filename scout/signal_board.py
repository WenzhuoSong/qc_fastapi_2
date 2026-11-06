"""Signal board (user's request 2026-11-07): every rule signal by stage, with its rule's verdict.

候选区 = the signal waits for its entry zone (within the rule's trigger window); 触发区 = filled and tracked to
its exit; 完成区 = closed (target, stop, holding limit) or expired without a fill. Sources: the forward-test record
(`scout.forward_trades`, role 'signal': the levels that are scored, one signal per stock per cooldown) and the
opportunity cards (`scout.opportunities`, live rules). Each row carries the knowledge base's status of its rule,
so a rejected rule's signal reads as a record, not an idea. Shown under 策略筛选 (`/screens?s=board`) and to the
agent (`get_screens('board')`). Descriptive bookkeeping: nothing here is a recommendation.
"""
from __future__ import annotations

from bisect import bisect_right
from datetime import date, timedelta

from scout import db, learned
from scout.universe import BENCHMARK

DONE_DAYS = 45                    # closed signals stay on the board this many calendar days
STAGES = {"candidate": "候选区", "triggered": "触发区", "done": "完成区"}
STAGE_NOTE = {"candidate": "信号已出现，等价格进入买入区（规则的触发窗口内）",
              "triggered": "已在买入区成交，按规则的止损 / 目标 / 持有上限跟踪",
              "done": f"已结束，或窗口内没回到买入区而作废（最近 {DONE_DAYS} 天）"}
_STAGE_OF = {"pending": "candidate", "new": "candidate", "open": "triggered", "triggered": "triggered"}
END_ZH = {"untriggered": "没回到买入区，作废", "expired_untriggered": "没回到买入区，作废", "hit_target": "到目标",
          "stopped": "止损", "trailed": "跟踪止损离场", "expired": "持有到期卖出"}


def stage(state: str) -> str:
    return _STAGE_OF.get(state, "done")


def verdicts(entries: list[dict]) -> dict[str, str]:
    """Rule → its status in the knowledge base (the entry with a registered spec wins)."""
    by_rule: dict[str, dict] = {}
    for en in entries:
        rule = learned.rule_of(en)
        if rule and (rule not in by_rule or en.get("spec")):
            by_rule[rule] = en
    return {r: learned.STATUS_ZH.get(en["status"], en["status"]) for r, en in by_rule.items()}


def sessions_after(dates: list[date], start: date, end: date) -> int:
    """Trading sessions in (start, end]."""
    return max(0, bisect_right(dates, end) - bisect_right(dates, start))


def describe(r: dict, close: float | None, dates: list[date], today: date) -> str:
    """One plain line on where the signal stands."""
    st = stage(r["state"])
    if st == "candidate":
        left = r["window"] - sessions_after(dates, r["asof"], today)
        where = ""
        if close:
            if close > r["entry_hi"]:
                where = f"，收盘 {close:.2f} 高于买入区 {close / r['entry_hi'] - 1:.1%}"
            elif close < r["entry_lo"]:
                where = f"，收盘 {close:.2f} 低于买入区 {r['entry_lo'] / close - 1:.1%}"
            else:
                where = f"，收盘 {close:.2f} 已在买入区内（信号日之后的交易日触及才算成交）"
        return f"等价格进入买入区，还剩 {max(left, 0)} 个交易日{where}"
    if st == "triggered":
        held = sessions_after(dates, r["entry_date"], today) + 1 if r.get("entry_date") else None
        pnl = f"，浮动 {close / r['entry_price'] - 1:+.1%}" if close and r.get("entry_price") else ""
        day = f"，持有第 {held} / {r['limit']} 天" if held else ""
        return f"{r['entry_date']:%m-%d} 以 {r['entry_price']:.2f} 成交{day}{pnl}" if r.get("entry_date") else f"已成交{pnl}"
    end = END_ZH.get(r["state"], r["state"])
    if r.get("ret") is None:
        return end
    vs = f"，同期 SPY {r['spy_ret']:+.1%}" if r.get("spy_ret") is not None else ""
    return f"{end}：{r['ret']:+.1%}（{r['entry_date']:%m-%d} → {r['exit_date']:%m-%d}）{vs}" \
        if r.get("entry_date") and r.get("exit_date") else f"{end}：{r['ret']:+.1%}{vs}"


async def load(today: date | None = None) -> dict:
    """{'asof': data date, 'rows': [...] by stage then signal date (newest first), 'counts': {stage: n}}."""
    from scout.detectors import registry

    dets = {d.name: d for d in registry(include_candidates=True)}
    since = (today or date.today()) - timedelta(days=DONE_DAYS)
    fwd = await db.fetch_all(
        "SELECT key, detector, ticker, asof, entry_lo, entry_hi, stop, target1, state, entry_date, entry_price, exit_date, "
        "exit_price, ret, spy_ret FROM scout.forward_trades WHERE role = 'signal' "
        "AND (state IN ('pending', 'open') OR COALESCE(exit_date, asof) >= :s)", {"s": since})
    opp = await db.fetch_all(
        "SELECT o.id, o.primary_detector AS detector, o.ticker, o.asof, o.entry_lo, o.entry_hi, o.stop, o.target1, "
        "o.status AS state, o.triggered_on AS entry_date, o.closed_on AS exit_date, "
        "COALESCE(x.entry_price, (SELECT price FROM scout.opportunity_events v WHERE v.opportunity_id = o.id "
        "AND v.event_type = 'triggered')) AS entry_price, x.exit_price, x.return_pct AS ret, x.spy_return_pct AS spy_ret "
        "FROM scout.opportunities o LEFT JOIN scout.opportunity_outcomes x ON x.opportunity_id = o.id "
        "WHERE o.status IN ('new', 'triggered') OR COALESCE(o.closed_on, o.asof) >= :s", {"s": since})
    rows, seen = [], set()
    for src, group in (("前向检验", fwd), ("机会卡片", opp)):
        for r in group:
            k = (r["detector"], r["ticker"], r["asof"])
            if k in seen:                       # the forward record is the scored one
                continue
            seen.add(k)
            det = dets.get(r["detector"])
            rows.append({**r, "source": src, "label": det.label if det else r["detector"],
                         "window": det.trigger_window if det else 3,
                         "limit": det.exit.holding_limit(det.horizon_days) if det else None})
    if not rows:
        return {"asof": None, "rows": [], "counts": {k: 0 for k in STAGES}}
    first = min(r["asof"] for r in rows)
    spy = await db.fetch_all("SELECT date FROM scout.bars_daily WHERE ticker = :t AND date > :d ORDER BY date",
                             {"t": BENCHMARK, "d": first})
    dates = [x["date"] for x in spy]
    asof = dates[-1] if dates else first
    closes = {x["ticker"]: x["close"] for x in await db.fetch_all(
        "SELECT ticker, close FROM scout.bars_daily WHERE date = :d AND ticker = ANY(:t)",
        {"d": asof, "t": sorted({r["ticker"] for r in rows})})}
    status = verdicts(await learned.current())
    for r in rows:
        r["stage"] = stage(r["state"])
        r["close"] = closes.get(r["ticker"])
        r["verdict"] = status.get(r["detector"], "未检验")
        r["text"] = describe(r, r["close"], dates, asof)
    order = list(STAGES)
    # within a stage, rules the knowledge base rejected go last: their signals are a record, not ideas
    rows.sort(key=lambda r: (order.index(r["stage"]), r["verdict"] in ("否定", "已失效"), -r["asof"].toordinal(), r["ticker"]))
    return {"asof": asof, "rows": rows, "counts": {k: sum(r["stage"] == k for r in rows) for k in STAGES}}


async def tracked(detector: str, tickers: list[str], asof: date) -> dict[str, dict]:
    """For a rule screen: today's hits whose forward record keeps an earlier signal (one per stock per cooldown)."""
    from scout.base_rates import COOLDOWN_SESSIONS

    if not tickers:
        return {}
    prev = await db.fetch_all("SELECT date FROM scout.bars_daily WHERE ticker = :t AND date < :d ORDER BY date DESC LIMIT :n",
                              {"t": BENCHMARK, "d": asof, "n": COOLDOWN_SESSIONS - 1})
    if not prev:
        return {}
    rows = await db.fetch_all(
        "SELECT DISTINCT ON (ticker) ticker, asof, entry_lo, entry_hi, stop, state FROM scout.forward_trades "
        "WHERE role = 'signal' AND detector = :det AND ticker = ANY(:t) AND asof < :d AND asof >= :s "
        "ORDER BY ticker, asof DESC", {"det": detector, "t": tickers, "d": asof, "s": prev[-1]["date"]})
    return {r["ticker"]: dict(r) for r in rows}
