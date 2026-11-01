"""Trades told to Scout in plain language (Telegram): parse → check → confirm → journal.

"买了 PBF 300 股 75.76，理由……；跌破 71.9 说明错了" is parsed by a small model into fields (structured output);
nothing is guessed: a missing price or share count is asked for. Python then checks the trade against the
user's own rules (buy zone, 20 % plan stop, size, satellite room, market filter, risk tier, same-industry
cooldown, rejected-rule cards, own and sector peers' earnings, the market / sector / company split). The
draft is shown with 【确认记录】【取消】; only a confirmation writes the trade (with its reasoning in the plan
and a `user_thesis` ledger entry for buys). Scout never places orders.
"""
from __future__ import annotations

import json
import logging
import re
from datetime import date, timedelta

from scout import db, knowledge, portfolio, universe
from scout.config import get_scout_settings

logger = logging.getLogger("scout.trade_intake")

INTENT = re.compile(r"^\s*(我)?(今天|昨天|刚才|刚刚)?\s*(买了|买入了?|卖了|卖出了?|加仓了?|减仓了?|清仓了?)")
QUESTION = re.compile(r"[？?吗呢么]\s*$")


def is_trade(text: str) -> bool:
    """A report of a trade (not a question about one): says it happened, carries a number or 全部 / 清仓."""
    return bool(INTENT.match(text)) and not QUESTION.search(text) and bool(re.search(r"\d|全部|清仓", text))
MAX_PRICE_GAP = 0.15          # a fill this far from the last close is almost surely a typo
DRAFT_HOURS = 24
SOURCES = {"opportunity": "Scout 机会卡", "price_map": "价位图买点", "own": "自己的判断", "other": "其他"}

SCHEMA = {
    "type": "object", "additionalProperties": False,
    "required": ["side", "ticker", "shares", "all_shares", "price", "date", "reason", "falsification",
                 "falsify_price", "falsify_below", "source", "horizon_sessions"],
    "properties": {
        "side": {"type": "string", "enum": ["buy", "sell"]},
        "ticker": {"type": "string", "description": "美股代码（大写）；只写了公司名时给出它的代码"},
        "shares": {"type": ["number", "null"], "description": "股数；没写就是 null"},
        "all_shares": {"type": "boolean", "description": "卖出全部 / 清仓时为 true"},
        "price": {"type": ["number", "null"], "description": "成交价；没写就是 null"},
        "date": {"type": ["string", "null"], "description": "YYYY-MM-DD；写了'昨天'等相对日期时按今天换算；没写就是 null"},
        "reason": {"type": ["string", "null"], "description": "用户写的理由，原话精简，不要补充"},
        "falsification": {"type": ["string", "null"], "description": "用户写的'什么情况说明错了'，没写就是 null"},
        "falsify_price": {"type": ["number", "null"], "description": "证伪条件里的价格（如'跌破 71.9'），没有就是 null"},
        "falsify_below": {"type": "boolean", "description": "证伪价格是跌破（true）还是涨破（false）"},
        "source": {"type": "string", "enum": list(SOURCES), "description": "机会卡 / 价位图买点 / 自己的判断 / 其他"},
        "horizon_sessions": {"type": ["integer", "null"], "description": "用户写的打算持有或验证的交易日数，没写就是 null"},
    },
}

PROMPT = """把用户关于一笔美股交易的描述解析成字段。今天是 {today}。
只提取用户写了的内容，不要推测或补充：没写的价格、股数、证伪条件都填 null。
理由和证伪条件用用户的原话精简，不要改写意思。来源：提到机会卡 / 信号 / Scout 提醒填 opportunity，提到买点 / 价位图填 price_map，
自己的分析或想法填 own，看不出来填 other。"""


# ── parse ──

async def parse(text: str, today: date) -> dict:
    from scout import responses

    settings = get_scout_settings()
    data = await responses.create({
        "model": settings.scout_parse_model, "instructions": PROMPT.format(today=today.isoformat()),
        "input": [{"role": "user", "content": text}],
        "text": {"format": {"type": "json_schema", "name": "trade", "schema": SCHEMA, "strict": True}}})
    return json.loads(responses.output_text(data))


def problems(p: dict, last_close: float | None, held_shares: float | None) -> list[str]:
    """What must be fixed before a draft can be confirmed (asked back, never guessed)."""
    out = []
    if not p.get("ticker"):
        out.append("没看出是哪只股票")
    if not p.get("price") or p["price"] <= 0:
        out.append("缺少成交价")
    if p["side"] == "buy" and (not p.get("shares") or p["shares"] <= 0):
        out.append("缺少股数")
    if p["side"] == "sell":
        if held_shares is None:
            out.append(f"没有 {p.get('ticker')} 的持仓记录")
        elif not p.get("all_shares") and (not p.get("shares") or p["shares"] <= 0):
            out.append("缺少卖出股数（全部卖出可以写'全部'或'清仓'）")
        elif not p.get("all_shares") and p["shares"] > held_shares + 1e-9:
            out.append(f"卖出 {p['shares']:g} 股超过持有的 {held_shares:g} 股")
    if last_close and p.get("price") and abs(p["price"] / last_close - 1) > MAX_PRICE_GAP:
        out.append(f"成交价 {p['price']:.2f} 与最近收盘 {last_close:.2f} 相差超过 {MAX_PRICE_GAP:.0%}，请确认没有写错")
    return out


def trade_day(p: dict, today: date) -> date:
    """The stated date, else today; a non-trading day moves to the session before."""
    from scout.market_calendar import is_us_equity_trading_day, previous_us_equity_trading_day

    d = date.fromisoformat(p["date"]) if p.get("date") else today
    return d if is_us_equity_trading_day(d) else previous_us_equity_trading_day(d)


# ── checks (all numbers from Python) ──

async def buy_checks(p: dict, day: date) -> list[str]:
    from scout import levels, market_filter, position_risk

    t, price, shares = p["ticker"], p["price"], p["shares"]
    lines = []
    pm = await levels.get_map(t) or {}
    zone = next((e for e in pm.get("entries") or [] if e["zone_lo"] <= price <= e["zone_hi"]), None)
    if pm.get("entries"):
        lines.append(f"价位图（{pm.get('asof')}）：" + (f"在「{zone['name']}」区间 {zone['zone_lo']:.2f}–{zone['zone_hi']:.2f} 内"
                     if zone else "不在任何买点区间内：" + "；".join(f"{e['name']} {e['zone_lo']:.2f}–{e['zone_hi']:.2f}"
                                                         for e in pm["entries"])))
    stop = portfolio.holding_stop(price)
    settings = await portfolio.get_settings()
    feat = await db.fetch_one("SELECT payload FROM scout.features_daily WHERE ticker = :t ORDER BY date DESC LIMIT 1", {"t": t})
    f = (feat or {}).get("payload") or {}
    lines.append(f"持仓计划：初始止损 {stop:.2f}（成交价回落 20%），之后按买入以来最高收盘的 80% 只升不降；"
                 f"跌到止损约亏 ${shares * (price - stop):,.0f}")
    sz = portfolio.size(price, stop, settings, f.get("vol_63"))
    if sz and sz.get("configured"):
        lines.append(f"仓位：你买 {shares:g} 股（${shares * price:,.0f}，占账户 {shares * price / settings['account_size']:.1%}）；"
                     f"按你的设置建议约 {sz['shares']} 股"
                     + {"satellite": "（受卫星仓剩余额度限制）", "vol": "（受波动上限限制）", "position": "（受单只仓位上限限制）"}.get(sz["capped"], "")
                     + ("，超出建议" if shares > sz["shares"] * 1.05 else ""))
    sat = portfolio.satellite(settings)
    if sat:
        after = sat["used"] + shares * price
        lines.append(f"卫星仓：买入后 ${after:,.0f}，占账户 {after / settings['account_size']:.1%}（上限 {sat['cap_pct']:.0%}）"
                     + ("，⚠️ 超过上限" if after > sat["cap"] else ""))
    mf = await market_filter.current()
    if mf and mf["off"]:
        lines.append("⚠️ 大盘过滤处于避险：按计划这段时间不新买入")
    tier = position_risk.tier(f.get("vol_63_pct"))
    if tier:
        lines.append(f"风险档位：第 {tier['tier']} 档（{tier['label']}）")
    loss = await portfolio.recent_loss_same_industry({"ticker": t, "opened_on": day, "id": 0})
    if loss:
        lines.append(f"⚠️ 冷却期：{loss['sessions']} 个交易日前刚在同一细分行业亏损卖出 {loss['ticker']}（{loss['pnl_pct']:+.1%}）")
    from scout import learned

    quiet = learned.held_back(await learned.current())
    opp = await db.fetch_one("SELECT primary_detector, asof FROM scout.opportunities WHERE ticker = :t AND status IN ('new', 'triggered') "
                             "ORDER BY asof DESC LIMIT 1", {"t": t})
    if opp and opp["primary_detector"] in quiet:
        lines.append(f"⚠️ 当前的机会卡（{opp['asof']}，{opp['primary_detector']}）来自未通过检验的规则，不能当作买入理由")
    if f.get("sector_ret_20d") is not None and f.get("own_ret_20d") is not None:
        lines.append(f"近 20 日：大盘 {knowledge.fmt('market_ret_20d', f.get('market_ret_20d'))}、"
                     f"行业 {knowledge.fmt('sector_ret_20d', f['sector_ret_20d'])}、公司自身 {knowledge.fmt('own_ret_20d', f['own_ret_20d'])}")
    lines += await earnings_lines(t, day)
    held = [x for x in await portfolio.trades(open_only=True) if x["ticker"] != t]
    inst = await db.fetch_one("SELECT sector, industry FROM scout.instruments WHERE ticker = :t", {"t": t}) or {}
    same = [x["ticker"] for x in held if inst.get("industry") and x.get("industry") == inst["industry"]]
    if same:
        lines.append(f"⚠️ 同一细分行业已有持仓：{'、'.join(same)}（走势高度相关，接近同一笔仓位）")
    return lines


async def earnings_lines(t: str, day: date) -> list[str]:
    """The stock's own report and its sector's most traded peers' reports in the next 7 days."""
    from scout import premarket

    rows = await db.fetch_all("SELECT ticker, event_date, payload FROM scout.market_events WHERE event_type = 'earnings' "
                              "AND event_date BETWEEN :a AND :b", {"a": day, "b": day + timedelta(days=7)})
    out = [f"⚠️ 自己 {r['event_date']:%m-%d} 发财报" for r in rows if r["ticker"] == t]
    inst = await db.fetch_one("SELECT sector FROM scout.instruments WHERE ticker = :t", {"t": t}) or {}
    if inst.get("sector"):
        major = await premarket._tickers_ranked_by_liquidity()
        sector_of = {r["ticker"]: r["sector"] for r in await db.fetch_all(
            "SELECT ticker, sector FROM scout.instruments WHERE ticker = ANY(:t)", {"t": list(major)})}
        peers = sorted((x for x in major if sector_of.get(x) == inst["sector"] and x != t), key=major.get)[:premarket.PEERS]
        due = [f"{r['ticker']} {r['event_date']:%m-%d}" for r in rows if r["ticker"] in peers]
        if due:
            out.append("同行业大公司 7 天内财报：" + "、".join(due))
    return out


async def sell_checks(p: dict, day: date) -> list[str]:
    from scout import position_risk

    held = [x for x in await portfolio.trades(open_only=True) if x["ticker"] == p["ticker"]]
    t = min(held, key=lambda x: (x["opened_on"], x["id"]))
    m = portfolio.metrics(t)
    sig = position_risk.adherence(t, [], day).get("signal")
    pl = t.get("plan") or {}
    lines = [f"持仓：{t['opened_on']} 买入 {m['remaining']:g} 股 @ {t['entry_price']:.2f}，系统止损 {t['stop']:.2f}" if t.get("stop")
             else f"持仓：{t['opened_on']} 买入 {m['remaining']:g} 股 @ {t['entry_price']:.2f}"]
    if pl.get("stop_hit") or sig:
        lines.append("按计划：回落止损已触发，这次卖出符合计划")
    else:
        lines.append("按计划：止损没有触发，计划要求继续持有；这次卖出会记为「提前离场」")
    n = m["remaining"] if p.get("all_shares") else p["shares"]
    lines.append(f"这次卖出 {n:g} 股 @ {p['price']:.2f}，相对成本 {p['price'] / t['entry_price'] - 1:+.1%}")
    return lines


# ── draft and confirmation ──

def card(p: dict, day: date, checks: list[str], issues: list[str]) -> str:
    from scout.telegram import esc

    side = "买入" if p["side"] == "buy" else "卖出"
    qty = "全部" if p.get("all_shares") else (f"{p['shares']:g} 股" if p.get("shares") else "？股")
    head = f"📝 <b>记录{side}</b>：{esc(p.get('ticker') or '？')} {qty} @ {p['price']:.2f}，{day}" if p.get("price") else \
        f"📝 <b>记录{side}</b>：{esc(p.get('ticker') or '？')} {qty}"
    lines = [head]
    if p.get("date") is None:
        lines.append(f"（没写日期，按 {day} 记录）")
    if p["side"] == "buy":
        lines.append(f"来源：{SOURCES.get(p.get('source'), '其他')}")
        lines.append(f"理由：{esc(p.get('reason') or '（没写）')}")
        lines.append(f"证伪：{esc(p.get('falsification') or '（没写，建议补上：什么情况说明你错了）')}")
        if p.get("horizon_sessions"):
            lines.append(f"验证期：{p['horizon_sessions']} 个交易日")
    elif p.get("reason"):
        lines.append(f"理由：{esc(p['reason'])}")
    if issues:
        lines += ["", "❗ 需要补充或改正：" + "；".join(esc(x) for x in issues), "请重新发送一条完整的描述。"]
    elif checks:
        lines += ["", "<b>检查</b>"] + [f"• {esc(x)}" for x in checks]
    lines.append("\n只是记录你做了什么，Scout 不会下单。")
    return "\n".join(lines)


def keyboard(draft_id: int) -> dict:
    return {"inline_keyboard": [[{"text": "✅ 确认记录", "callback_data": f"td:ok:{draft_id}"},
                                 {"text": "✖️ 取消", "callback_data": f"td:no:{draft_id}"}]]}


async def handle(text: str) -> tuple[str, dict | None]:
    """A trade message → (reply, keyboard or None)."""
    from scout.market_calendar import us_equity_market_status

    today = date.fromisoformat(us_equity_market_status().market_date)
    p = await parse(text, today)
    p["ticker"] = universe.yahoo_symbol(p.get("ticker") or "")
    day = trade_day(p, today)
    last = await db.fetch_one("SELECT close FROM scout.bars_daily WHERE ticker = :t ORDER BY date DESC LIMIT 1", {"t": p["ticker"]})
    held = [x for x in await portfolio.trades(open_only=True) if x["ticker"] == p["ticker"]]
    held_shares = sum(portfolio.metrics(x)["remaining"] for x in held) if held else None
    issues = problems(p, (last or {}).get("close"), held_shares)
    if not last and not issues:
        issues.append(f"{p['ticker']} 不在 Scout 的标的池（没有行情），可以先 /watch add {p['ticker']}")
    if issues:
        return card(p, day, [], issues), None
    checks = await (buy_checks(p, day) if p["side"] == "buy" else sell_checks(p, day))
    row = await db.execute_returning(
        "INSERT INTO scout.trade_drafts (raw, parsed, checks) VALUES (:r, CAST(:p AS JSONB), CAST(:c AS JSONB)) RETURNING id",
        {"r": text, "p": db.jsonb({**p, "day": day.isoformat()}), "c": db.jsonb(checks)})
    return card(p, day, checks, []), keyboard(row["id"])


async def decide(draft_id: int, ok: bool) -> str:
    """The user's answer to a draft: write the trade (or the sell), or drop it."""
    from scout import ledger
    from scout.telegram import esc

    d = await db.fetch_one("SELECT * FROM scout.trade_drafts WHERE id = :i", {"i": draft_id})
    if not d or d["status"] != "pending":
        return "这条记录已经处理过了。"
    if d["created_at"].timestamp() < __import__("time").time() - DRAFT_HOURS * 3600:
        await db.execute("UPDATE scout.trade_drafts SET status = 'expired', decided_at = now() WHERE id = :i", {"i": draft_id})
        return "这条草稿已超过 24 小时，请重新发送。"
    if not ok:
        await db.execute("UPDATE scout.trade_drafts SET status = 'cancelled', decided_at = now() WHERE id = :i", {"i": draft_id})
        return "已取消，没有记录。"
    p, day = d["parsed"], date.fromisoformat(d["parsed"]["day"])
    if p["side"] == "sell":
        held = [x for x in await portfolio.trades(open_only=True) if x["ticker"] == p["ticker"]]
        if not held:
            return f"没有 {esc(p['ticker'])} 的持仓记录。"
        t = min(held, key=lambda x: (x["opened_on"], x["id"]))
        res = await portfolio.add_sell(t["id"], day, p["price"], None if p.get("all_shares") else p["shares"], p.get("reason") or "")
        trade_id, text = t["id"], (f"✅ 已记录：{esc(p['ticker'])} 卖出 {res['shares']:g} 股 @ {p['price']:.2f}"
                                   + ("，已全部卖出并平仓。" if res["closed"] else f"，剩余 {res['remaining']:g} 股。"))
    else:
        note = "；".join(x for x in (f"理由：{p['reason']}" if p.get("reason") else "",
                                     f"证伪：{p['falsification']}" if p.get("falsification") else "") if x)
        thesis = {k: p.get(k) for k in ("reason", "falsification", "falsify_price", "falsify_below", "source", "horizon_sessions")}
        trade_id = await portfolio.add_trade({"ticker": p["ticker"], "opened_on": day, "entry_price": p["price"],
                                              "shares": p["shares"], "note": note or None, "plan": {"thesis": thesis}})
        row = await db.fetch_one("SELECT * FROM scout.trades WHERE id = :i", {"i": trade_id})
        msgs = await portfolio.manage_trade(row)
        row = await db.fetch_one("SELECT stop FROM scout.trades WHERE id = :i", {"i": trade_id})
        inst = await db.fetch_one("SELECT sector FROM scout.instruments WHERE ticker = :t", {"t": p["ticker"]}) or {}
        try:
            await ledger.record("user_thesis", p["ticker"], day, 1, sector_etf=universe.SECTOR_ETFS.get(inst.get("sector") or ""),
                                source_ref=f"trade:{trade_id}", payload=thesis)
        except Exception:
            logger.exception("ledger entry for trade %s failed", trade_id)
        text = (f"✅ 已记录：{esc(p['ticker'])} 买入 {p['shares']:g} 股 @ {p['price']:.2f}（{day}），系统止损 {row['stop']:.2f}。"
                + ("\n" + "\n".join(f"• {esc(m)}" for m in msgs) if msgs else "")
                + "\n理由已写进这笔交易，并登记进判断台账（之后 5 / 20 / 60 个交易日对照 SPY 和行业打分）。")
    await db.execute("UPDATE scout.trade_drafts SET status = 'confirmed', trade_id = :t, decided_at = now() WHERE id = :i",
                     {"t": trade_id, "i": draft_id})
    return text
