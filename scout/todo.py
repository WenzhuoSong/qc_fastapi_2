"""Today's to-do list and the daily candidates (user's request 2026-11-25: a stop signal went unnoticed among the
pushes; the user wants about 30 minutes a day with Scout).

To-do: at most TODO_MAX items that need the user's hand — a sell the plan calls for (with a button that drafts the
sell at the open after the signal), a holding reporting within EARNINGS_DAYS, the satellite over its cap, the market
filter turning off, today's candidates waiting for a decision. An item comes back every time the list is sent (after
the close and before the open) until it is handled; "明天再提醒" hides it until the next market day.

Candidates: at most CANDIDATES a day, stocks that the most stock playbooks currently call 买入 (weekly
`playbooks` brief; playbooks with evidence count double), not held, not shown in the last RECENT_DAYS days, none
while the market filter is off or the satellite is over its cap. Each card carries the same checks as a recorded buy
and the size the user's own settings give (risk per trade, volatility cap, satellite room). The user answers 打算买入
/ 观察 / 跳过; every candidate is a ledger judgment (kind `candidate`, the decision in its payload) scored at 5 / 20 /
60 sessions, so `decision_stats` shows over time whether the user's picks beat the ones skipped and SPY.
"""
from __future__ import annotations

import logging
from datetime import date, timedelta

from scout import db, ledger, portfolio, universe

logger = logging.getLogger("scout.todo")

TODO_MAX = 6
EARNINGS_DAYS = 3                  # calendar days ahead: a holding's report this close is a decision (hold through it?)
CANDIDATES = 3
RECENT_DAYS = 30                   # a stock shown as a candidate is not shown again within this many days
MIN_BOOKS = 3                      # a candidate needs this many playbooks saying 买入
STOCK_SCHOOLS = ("价值投资", "成长股", "技术与趋势", "事件与投机")
DECISIONS = {"plan": "📝 打算买入", "watch": "👀 观察", "skip": "⏭ 跳过"}
BOOKS_SHOWN = 5


# ── to-do state ──

async def _states(keys: list[str]) -> dict[str, dict]:
    if not keys:
        return {}
    return {r["key"]: r for r in await db.fetch_all("SELECT * FROM scout.todo_state WHERE key = ANY(:k)", {"k": keys})}


def visible(state: dict | None, today: date) -> bool:
    if not state:
        return True
    if state["status"] == "done":
        return False
    return not (state["status"] == "snoozed" and state.get("snooze_until") and state["snooze_until"] > today)


async def set_state(key: str, status: str, until: date | None = None) -> None:
    await db.execute(
        "INSERT INTO scout.todo_state (key, status, snooze_until) VALUES (:k, :s, :u) "
        "ON CONFLICT (key) DO UPDATE SET status = EXCLUDED.status, snooze_until = EXCLUDED.snooze_until, updated_at = now()",
        {"k": key, "s": status, "u": until})


# ── items ──

async def items(today: date) -> list[dict]:
    """[{key, text, buttons: [(label, callback)]}], the most urgent first."""
    from scout import market_filter, position_risk
    from scout.telegram import next_step

    out: list[tuple[int, dict]] = []
    held = await portfolio.trades(open_only=True)
    for t in held:
        m = portfolio.metrics(t)
        step = next_step(t, m)
        if not step.startswith("⚠️"):
            continue
        pl = t.get("plan") or {}
        since = pl.get("stop_hit") or pl.get("filter_off") or "due"
        sig = position_risk.adherence(t, [], today).get("signal")
        waited = f"（已过 {sig['sessions']} 个交易日）" if sig else ""
        key = f"sell:{t['id']}:{since}"
        out.append((0, {"key": key, "text": f"{t['ticker']}：{step[2:].strip()}{waited}",
                        "buttons": [(f"✅ 已卖出 {t['ticker']}", f"sd:{t['id']}"), ("⏸ 明天再提醒", f"tz:{key}")]}))
    if out:                            # a stop to act on: the user's own record of stop signals beside it
        from scout import behavior

        try:
            record = await behavior.lines_for("sell_due", today)
        except Exception as exc:
            logger.warning("execution record failed: %s", exc)
            record = []
        if record:
            for _, x in out:
                x["text"] += "。" + record[0]
    tickers = sorted({t["ticker"] for t in held})
    if tickers:
        for r in await db.fetch_all(
                "SELECT DISTINCT ON (ticker) ticker, event_date FROM scout.market_events WHERE event_type = 'earnings' "
                "AND ticker = ANY(:t) AND event_date BETWEEN :a AND :b ORDER BY ticker, event_date",
                {"t": tickers, "a": today, "b": today + timedelta(days=EARNINGS_DAYS)}):
            key = f"earn:{r['ticker']}:{r['event_date']}"
            days = (r["event_date"] - today).days
            out.append((2, {"key": key, "text": f"{r['ticker']} {r['event_date']:%m-%d} 发财报（{'今天' if days == 0 else f'{days} 天后'}）："
                                                f"决定是否带着它过财报；跌破止损同样按收盘判断，但跳空可能越过止损",
                            "buttons": [("👌 知道了", f"td2:{key}")]}))
    sat = portfolio.satellite(await portfolio.get_settings())
    if sat and sat["over"]:
        out.append((1, {"key": "sat:over", "text": f"卫星仓占账户 {sat['pct'] * 100:.1f}%，超过你定的 {sat['cap_pct'] * 100:.0f}% 上限"
                                                   f"（超出约 ${sat['used'] - sat['cap']:,.0f}）：按计划不新买入个股，决定是否减仓",
                        "buttons": [("⏸ 明天再提醒", "tz:sat:over")]}))
    mf = await market_filter.current()
    if mf and mf.get("off"):
        key = f"filter:{mf.get('since')}"
        out.append((1, {"key": key, "text": "大盘过滤处于避险：按计划不新买入；核心仓按规则卖出转现金",
                        "buttons": [("👌 知道了", f"td2:{key}")]}))
    pending = await db.fetch_all("SELECT ticker FROM scout.judgments WHERE kind = 'candidate' AND asof >= :d "
                                 "AND (payload->>'decision') IS NULL ORDER BY id", {"d": today - timedelta(days=3)})
    if pending:
        out.append((3, {"key": f"cand:{today}", "text": f"{len(pending)} 个候选等你决定：{'、'.join(r['ticker'] for r in pending)}"
                                                         "（卡片在收盘汇总之后，点按钮即可）", "buttons": []}))
    states = await _states([x["key"] for _, x in out])
    shown = [x for _, x in sorted(out, key=lambda kv: kv[0]) if visible(states.get(x["key"]), today)]
    return shown[:TODO_MAX]


def render(rows: list[dict], today: date) -> tuple[str, dict | None]:
    from scout.telegram import esc

    if not rows:
        return f"✅ <b>今日待办 {today:%m-%d}</b>：没有需要你处理的事。", None
    lines = [f"🗂 <b>今日待办 {today:%m-%d}</b>（处理完点按钮；没处理的下次还会出现）"]
    lines += [f"{i}. {esc(x['text'])}" for i, x in enumerate(rows, 1)]
    keyboard = [[{"text": f"{i}. {label}" if len(rows) > 1 else label, "callback_data": cb[:64]} for label, cb in x["buttons"]]
                for i, x in enumerate(rows, 1) if x["buttons"]]
    return "\n".join(lines), ({"inline_keyboard": keyboard} if keyboard else None)


async def send(today: date | None = None) -> int:
    """Push the list; returns the number of items."""
    from scout import telegram
    from scout.market_calendar import us_equity_market_status

    today = today or date.fromisoformat(us_equity_market_status().market_date)
    rows = await items(today)
    text, markup = render(rows, today)
    await telegram.send(text, reply_markup=markup)
    return len(rows)


async def snooze(key: str) -> str:
    from scout.market_calendar import is_us_equity_trading_day, us_equity_market_status

    nxt = date.fromisoformat(us_equity_market_status().market_date) + timedelta(days=1)
    while not is_us_equity_trading_day(nxt):
        nxt += timedelta(days=1)
    await set_state(key, "snoozed", nxt)
    return "好的，下一个交易日再提醒"


async def sell_draft(trade_id: int) -> tuple[str, dict | None]:
    """The sell the plan called for, drafted at the open of the session after the signal (confirm to record)."""
    from scout import trade_intake

    t = next((x for x in await portfolio.trades(open_only=True) if x["id"] == trade_id), None)
    if not t:
        return "这笔持仓已经没有未卖出的股数了。", None
    pl = t.get("plan") or {}
    since = pl.get("stop_hit") or pl.get("filter_off")
    bar = await db.fetch_one("SELECT date, open FROM scout.bars_daily WHERE ticker = :t AND date > :d ORDER BY date LIMIT 1",
                             {"t": t["ticker"], "d": date.fromisoformat(since) if since else t["opened_on"]})
    if not bar or not bar.get("open"):
        return (f"信号后的交易日还没有开盘价。成交后直接发「卖了 {t['ticker']} 全部 成交价」记录。"), None
    p = {"ticker": t["ticker"], "side": "sell", "price": round(float(bar["open"]), 2), "shares": None, "all_shares": True,
         "date": bar["date"].isoformat(), "reason": "按计划：" + ("大盘过滤避险" if pl.get("filter_off") else "回落止损触发")}
    checks = await trade_intake.sell_checks(p, bar["date"])
    checks.append(f"价格按 {bar['date']} 开盘价 {p['price']:.2f} 记录；实际成交价不同的话点取消，"
                  f"然后发「卖了 {t['ticker']} 全部 成交价 日期」")
    row = await db.execute_returning(
        "INSERT INTO scout.trade_drafts (raw, parsed, checks) VALUES (:r, CAST(:p AS JSONB), CAST(:c AS JSONB)) RETURNING id",
        {"r": f"todo:sell:{trade_id}", "p": db.jsonb({**p, "day": bar["date"].isoformat()}), "c": db.jsonb(checks)})
    return trade_intake.card(p, bar["date"], checks, []), trade_intake.keyboard(row["id"])


# ── candidates ──

def consensus(res: dict) -> list[dict]:
    """Stocks by how many stock playbooks call 买入 (those with evidence count double): [{ticker, score, books}]."""
    books = {b["key"]: b for b in res.get("playbooks") or [] if b.get("school") in STOCK_SCHOOLS}
    out = []
    for t, acts in (res.get("by_ticker") or {}).items():
        mine = [books[k] for k, a in acts if a == "买入" and k in books]
        if len(mine) < MIN_BOOKS:
            continue
        mine.sort(key=lambda b: b["status"]["grade"] != "有依据")
        score = sum(2 if b["status"]["grade"] == "有依据" else 1 for b in mine)
        out.append({"ticker": t, "score": score, "books": [{"name": b["name"], "status": b["status"]["text"],
                                                            "grade": b["status"]["grade"]} for b in mine]})
    return sorted(out, key=lambda x: (-x["score"], x["ticker"]))


async def blocked() -> str | None:
    """Why there are no candidates today under the user's own rules, or None."""
    from scout import market_filter

    mf = await market_filter.current()
    if mf and mf.get("off"):
        return "大盘过滤处于避险，按你的计划不新买入，今天不出候选"
    sat = portfolio.satellite(await portfolio.get_settings())
    if sat and sat["over"]:
        return f"卫星仓超过 {sat['cap_pct'] * 100:.0f}% 上限，按你的计划不新买入个股，今天不出候选"
    return None


async def pick(asof: date, n: int = CANDIDATES) -> list[dict]:
    from scout import playbooks

    res = await playbooks.latest()
    if not res:
        return []
    held = {t["ticker"] for t in await portfolio.trades(open_only=True)}
    recent = {r["ticker"] for r in await db.fetch_all(
        "SELECT DISTINCT ticker FROM scout.judgments WHERE kind = 'candidate' AND asof > :d", {"d": asof - timedelta(days=RECENT_DAYS)})}
    pushed = {r["ticker"] for r in await db.fetch_all(
        "SELECT ticker FROM scout.opportunities WHERE asof = :d AND pushed_at IS NOT NULL", {"d": asof})}
    stocks = {r["ticker"] for r in await db.fetch_all("SELECT ticker FROM scout.instruments WHERE kind = 'stock'")}
    out = [c for c in consensus(res) if c["ticker"] in stocks and c["ticker"] not in held | recent | pushed]
    return [{**c, "playbooks_asof": res.get("asof")} for c in out[:n]]


async def card(c: dict, i: int, n: int) -> tuple[str, float | None]:
    """The decision card and the last close."""
    from scout import knowledge, trade_intake
    from scout.market_calendar import us_equity_market_status
    from scout.telegram import esc

    t = c["ticker"]
    inst = await db.fetch_one("SELECT name, sector FROM scout.instruments WHERE ticker = :t", {"t": t}) or {}
    last = await db.fetch_one("SELECT date, close FROM scout.bars_daily WHERE ticker = :t ORDER BY date DESC LIMIT 1", {"t": t})
    lines = [f"🔍 <b>候选 {i}/{n} · {esc(t)}</b> {esc(inst.get('name') or '')}" + (f"（{esc(inst['sector'])}）" if inst.get("sector") else "")]
    if not last:
        return "\n".join(lines + ["没有行情"]), None
    lines.append(f"最近收盘 {last['close']:.2f}（{last['date']}）")
    shown = "、".join(f"{b['name']}（{b['status']}）" for b in c["books"][:BOOKS_SHOWN])
    more = f" 等 {len(c['books'])} 个" if len(c["books"]) > BOOKS_SHOWN else ""
    lines.append(f"<b>为什么出现</b>：{esc(shown)}{more}打法给出「买入」（打法简报 {c.get('playbooks_asof')}）。"
                 "这些打法按各自规则机械计算，标「否定」或「未检验」的没有可靠优势；被多个选中不等于会涨，买不买由你判断")
    settings = await portfolio.get_settings()
    feat = await db.fetch_one("SELECT payload FROM scout.features_daily WHERE ticker = :t ORDER BY date DESC LIMIT 1", {"t": t})
    vol = ((feat or {}).get("payload") or {}).get("vol_63")
    sz = portfolio.size(last["close"], portfolio.holding_stop(last["close"]), settings, vol)
    shares = (sz or {}).get("shares") or 1
    day = trade_intake.trade_day({"date": None}, date.fromisoformat(us_equity_market_status().market_date))
    # the share-count lines of the intake check assume a chosen size; here the size line below says it once
    checks = [x for x in await trade_intake.buy_checks({"ticker": t, "price": last["close"], "shares": shares}, day)
              if not x.startswith(("仓位：", "持仓计划：", "卫星仓："))]
    size = portfolio.size_text(sz)
    lines.append("<b>按收盘价的买入前检查</b>（和记录交易时同一套）：")
    lines.append(f"• 止损：按收盘价买入时初始 {portfolio.holding_stop(last['close']):.2f}（回落 20%），之后按买入以来最高收盘的 80% 只升不降")
    lines += [f"• {esc(x)}" for x in checks]
    if size:
        lines.append(f"• 仓位（按你的设置，波动越大股数越少）：{esc(size)}")
    if vol:
        lines.append(f"• 年化波动 {knowledge.fmt('vol_63', vol)}")
    lines.append("想深入看：发「/why " + esc(t) + "」或直接问「" + esc(t) + " 值得买吗」")
    return "\n".join(lines), last["close"]


def keyboard(judgment_id: int) -> dict:
    return {"inline_keyboard": [[{"text": label, "callback_data": f"cd:{k}:{judgment_id}"} for k, label in DECISIONS.items()]]}


async def send_candidates(asof: date, push: bool = True) -> dict:
    from scout import telegram

    why_not = await blocked()
    if why_not:
        if push:
            await telegram.send(f"🔍 {why_not}。")
        return {"blocked": why_not}
    picks = await pick(asof)
    sent = []
    for i, c in enumerate(picks, 1):
        text, close = await card(c, i, len(picks))
        inst = await db.fetch_one("SELECT sector FROM scout.instruments WHERE ticker = :t", {"t": c["ticker"]}) or {}
        await ledger.record("candidate", c["ticker"], asof, 1, sector_etf=universe.SECTOR_ETFS.get(inst.get("sector") or ""),
                            source_ref="playbooks", payload={"books": [b["name"] for b in c["books"]], "score": c["score"],
                                                             "decision": None})
        row = await db.fetch_one("SELECT id FROM scout.judgments WHERE kind = 'candidate' AND ticker = :t AND asof = :d AND lens = ''",
                                 {"t": c["ticker"], "d": asof})
        if push and row:
            await telegram.send(text, reply_markup=keyboard(row["id"]))
        sent.append(c["ticker"])
    return {"sent": sent}


async def decide(judgment_id: int, decision: str) -> str:
    row = await db.fetch_one("SELECT ticker, payload FROM scout.judgments WHERE id = :i AND kind = 'candidate'", {"i": judgment_id})
    if not row or decision not in DECISIONS:
        return "找不到这个候选。"
    await db.execute("UPDATE scout.judgments SET payload = payload || CAST(:p AS JSONB) WHERE id = :i",
                     {"i": judgment_id, "p": db.jsonb({"decision": decision, "decided_at": date.today().isoformat()})})
    t = row["ticker"]
    if decision in ("plan", "watch"):
        await universe.add_watch(t)
    if decision == "plan":
        return (f"已记为「打算买入」并加入自选。买入后发「买了 {t} 股数 成交价，理由……，跌破 X 说明错了」记录；"
                "盘前简报和收盘汇总会跟踪它")
    return {"watch": f"已加入自选观察：{t} 有大的变化会出现在收盘汇总", "skip": f"已跳过 {t}"}[decision]


async def decision_stats(horizon: int = 20) -> dict:
    """Candidates by the user's decision: count scored, mean return vs SPY at `horizon` sessions."""
    rows = await db.fetch_all(
        "SELECT coalesce(j.payload->>'decision', 'none') AS d, count(*) AS n, avg(o.excess_spy) AS x "
        "FROM scout.judgments j JOIN scout.judgment_outcomes o ON o.judgment_id = j.id "
        "WHERE j.kind = 'candidate' AND o.horizon = :h GROUP BY 1", {"h": horizon})
    return {r["d"]: {"n": r["n"], "excess": r["x"]} for r in rows}


def stats_line(stats: dict, horizon: int = 20) -> str | None:
    zh = {"plan": "打算买入", "watch": "观察", "skip": "跳过", "none": "没回答"}
    parts = [f"{zh[k]} {v['n']} 个，平均相对 SPY {v['excess'] * 100:+.1f}%" for k, v in stats.items() if k in zh and v["excess"] is not None]
    if not parts:
        return None
    return f"候选 {horizon} 个交易日后（按你的决定分组）：" + "；".join(parts) + "。样本少时只看方向，不下结论"
