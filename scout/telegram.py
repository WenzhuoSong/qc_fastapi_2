"""Telegram delivery: card rendering, push, commands and feedback buttons."""
from __future__ import annotations

import html
import logging
from typing import Any

import httpx

from scout import db, knowledge, portfolio, universe
from scout.config import get_scout_settings

logger = logging.getLogger("scout.telegram")
# httpx logs full request URLs at INFO, and Telegram URLs embed the bot token.
logging.getLogger("httpx").setLevel(logging.WARNING)

MAX_CHARS = 3900
FEEDBACK_ACTIONS = {"up": "👍 有用", "down": "👎 没用", "star": "⭐ 关注", "dismiss": "🙈 忽略"}
STATUS_LABELS = {
    "new": "待触发", "triggered": "已触发", "hit_target": "达到目标", "stopped": "止损", "trailed": "跟进止损离场",
    "expired": "到期", "expired_untriggered": "未触发过期",
}
CONFIDENCE_LABELS = {"low": "低", "medium": "中", "high": "高"}

HELP_TEXT = (
    "<b>Scout 美股机会观察器</b>\n"
    "/ideas — 活跃机会\n"
    "/idea NVDA — 某只股票的最新机会卡\n"
    "/why NVDA — 多视角解读（趋势、形态、动量、波动、大盘、催化）\n"
    "/watch list | add NVDA [主题] | rm NVDA\n"
    "/stats — 规则记分板\n"
    "/sectors — 板块热度、轮动与市场关注点\n"
    "/premarket — 今天的盘前简报\n"
    "/trades — 我的持仓、止损和下一步止盈建议\n"
    "/sell CART 66 61.50 [2026-10-28] — 记录卖出（代码 股数 价格 [日期]，股数可写 all）\n"
    "直接发文字即可提问（例如：今天最强的板块是什么？）"
)


def esc(x: Any) -> str:
    return html.escape(str(x), quote=False)


def pct(x: float | None, digits: int = 1, sign: bool = False) -> str:
    if x is None:
        return "—"
    return f"{x * 100:+.{digits}f}%" if sign else f"{x * 100:.{digits}f}%"


def market_split_text(price: dict) -> str | None:
    """'Is it the stock or the market?' — today's move split by 60-day beta."""
    if price.get("idio_ret_1d") is None or price.get("market_ret_1d") is None:
        return None
    text = (
        f"今日 {pct(price.get('ret_1d'), 1, True)}（大盘贡献 {pct(price['market_ret_1d'], 1, True)}"
        f"｜个股自身 {pct(price['idio_ret_1d'], 1, True)}）"
    )
    if price.get("idio_ret_20d") is not None:
        text += f" · 20 日扣除大盘后 {pct(price['idio_ret_20d'], 1, True)}"
    if price.get("beta_60") is not None:
        text += f" · β {price['beta_60']:.2f}"
    return text


def sector_text(ctx: dict | None) -> str | None:
    if not ctx:
        return None
    icon = {"热": "🔥", "温": "🌤", "冷": "🧊"}.get(ctx.get("热度"), "")
    move = f"{ctx['上周象限']}→" if ctx.get("上周象限") and ctx["上周象限"] != ctx.get("轮动象限") else ""
    return (f"板块：{ctx['板块']}（{move}{ctx.get('轮动象限')} · {icon}{ctx.get('热度')} · 资金{ctx.get('资金流向')}"
            f" · 站上 50 日线 {pct(ctx.get('站上 50 日线比例'), 0)}）")


def position_text(why: dict) -> str | None:
    pos = (why or {}).get("position") or {}
    if not pos.get("bucket"):
        return None
    text = f"位置：{pos['label']}（距高点 {pct(pos.get('dist_52w_high'), 1, True)}）"
    if pos.get("edge_vs_hold") is not None:
        text += f" · 同规则同位置历史相对持有 {pct(pos['edge_vs_hold'], 1, True)}（{pos['n']} 次）"
    if why.get("chase_risk"):
        text += " · ⚠️追高风险"
    return text


def score_text(why: dict) -> str | None:
    parts = (why or {}).get("score_parts") or []
    if not parts:
        return None
    text = " + ".join(f"{p['label']} {p['value']:.2f}×{p['weight']:.2f}" for p in parts)
    adj = "；".join(f"{a['label']} {a['value']:+.2f}" if a["key"] != "feedback_weight" and a["key"] != "watch"
                   else a["label"] if a["key"] == "watch" else f"{a['label']} ×{a['value']:.2f}"
                   for a in why.get("adjustments") or [])
    return f"评分 {why['score']:.2f} = {text}" + (f"（调整：{adj}）" if adj else "")


def exit_text(o: dict) -> str:
    """One line: how this trade is exited, with the live trailing stop if any."""
    ex = (o.get("evidence") or {}).get("exit") or {}
    if ex.get("kind") == "trail_low":
        text = f"跟进止损（{ex['lookback']} 日低点，只升不降）· 最长 {ex['max_hold']} 日"
        cur = o.get("current_stop")
        if cur and o.get("status") == "triggered" and cur > o["stop"]:
            text += f" · 当前止损已上移至 {cur:.2f}"
        return text
    return f"到目标 1 止盈 · 周期 {o['horizon_days']} 日"


def base_rate_text(br: dict) -> str:
    """Rule-level replay stats, net of costs, judged against just holding the stock."""
    head = f"规则历史 {br['n']} 次（{br.get('regime')}）"
    if br.get("avg_edge_vs_hold") is None:  # rows computed before costs were modelled
        return f"{head} · 胜率 {pct(br.get('win_rate'), 0)} · 平均超额 {pct(br.get('avg_excess'), 1, True)}"
    return (
        f"{head} · 扣费胜率 {pct(br.get('win_rate_net'), 0)} · 相对持有 {pct(br.get('avg_edge_vs_hold'), 1, True)}"
        f" · 跑赢持有 {pct(br.get('beat_hold_rate'), 0)}"
    )


def outcome_text(br: dict) -> str | None:
    """How replayed trades of this rule actually ended: the target is the best
    case, not the expected one."""
    if not br.get("n") or br.get("target_rate") is None:
        return None
    tgt, stop, trail = br["target_rate"], br.get("stop_rate") or 0, br.get("trail_rate") or 0
    parts = [f"达标 {pct(tgt, 0)}", f"止损 {pct(stop, 0)}"]
    if trail:
        parts.append(f"跟踪止损离场 {pct(trail, 0)}")
    parts.append(f"到期离场 {pct(max(0.0, 1 - tgt - stop - trail), 0)}")
    text = "历史结局：" + " · ".join(parts)
    if br.get("median_return") is not None:
        text += f" · 扣费收益中位数 {pct(br['median_return'] - (br.get('cost_bps') or 0) / 1e4, 1, True)}"
    if br.get("avg_days"):
        text += f" · 平均持有 {br['avg_days']:.0f} 天"
    return text


def render_card(o: dict, settings: dict | None = None) -> str:
    ev, th, br = o.get("evidence") or {}, knowledge.humanize_obj(o.get("thesis") or {}), o.get("base_rate") or {}
    setup = ev.get("setup") or {}
    labels = setup.get("label") or o["primary_detector"]
    extra = [d for d in (o.get("detectors") or []) if d != o["primary_detector"]]
    head = f"🟢 <b>{esc(o['ticker'])}</b> · {esc(labels)}"
    if extra:
        head += f" +{len(extra)}"
    head += f"　评分 {o['score']:.2f}"
    if ev.get("theme"):
        head += f"　#{esc(ev['theme'])}"
    lines = [head]
    from scout.detectors import registry

    if o.get("status") in ("new", "triggered") and not any(d.name == o["primary_detector"] and d.live for d in registry()):
        lines.append("⚠️ 该规则已降为候选（回放跑输持有），只跟踪到结束，不建议据此新开仓。")
    if th.get("headline"):
        lines.append(f"<i>{esc(th['headline'])}</i>")
    trailing = (ev.get("exit") or {}).get("kind") == "trail_low"
    lines.append(
        f"入场 {o['entry_lo']:.2f}–{o['entry_hi']:.2f} | 保护性止损 {o['stop']:.2f} | "
        f"{'参考目标' if trailing else '目标'} {o['target1']:.2f}"
        + (f" / {o['target2']:.2f}" if (o.get("target2") or 0) > o["target1"] else "")
    )
    move = market_split_text(ev.get("price") or {})
    if move:
        lines.append(f"📈 {move}")
    why = ev.get("why") or {}
    if why.get("conditions"):
        met = sum(c["ok"] for c in why["conditions"])
        lines.append(f"✅ 入选理由：{esc(why['rule'])}（{met}/{len(why['conditions'])} 条件满足）")
        lines += [f"　· {esc(c['label'])}：{esc(c['actual'])} {esc(c['op'])} {esc(c['threshold'])}" for c in why["conditions"]]
    pos = position_text(why)
    if pos:
        lines.append(f"🧭 {esc(pos)}")
    sec = sector_text(ev.get("sector_context"))
    if sec:
        lines.append(f"🗺 {esc(sec)}")
    lines.append(f"出场：{esc(exit_text(o))} | R:R {o.get('risk_reward') or 0:.1f} | 状态 {STATUS_LABELS.get(o.get('status'), o.get('status'))}")
    if br.get("n"):
        lines.append(f"📊 {base_rate_text(br)}")
        outcome = outcome_text(br)
        if outcome:
            lines.append(f"🎯 {esc(outcome)}")
    for flag in ev.get("event_flags") or []:
        lines.append(f"⚡ 事件驱动·{esc(flag['type'])}：{esc(flag['headline'])}（技术规则统计参考价值有限）")
    if settings is not None and o.get("status") in ("new", "triggered"):
        sz = portfolio.size_text(portfolio.size((o["entry_lo"] + o["entry_hi"]) / 2, o.get("current_stop") or o["stop"], settings))
        if sz:
            lines.append(f"📐 {esc(sz)}")
    score = score_text(why)
    if score:
        lines.append(f"🧮 {esc(score)}")
    if th.get("thesis"):
        lines.append(f"\n📌 {esc(th['thesis'])}")
    for c in th.get("catalysts") or []:
        lines.append(f"➕ {esc(c)}")
    for c in th.get("counter_evidence") or []:
        lines.append(f"➖ {esc(c)}")
    if th.get("key_risk"):
        lines.append(f"⚠️ {esc(th['key_risk'])}")
    if th.get("confidence"):
        lines.append(f"置信度：{CONFIDENCE_LABELS.get(th['confidence'], th['confidence'])} — {esc(th.get('confidence_reason', ''))}")
    lines.append("<i>历史基准基于当前成分股回放，存在幸存者偏差。非投资建议。</i>")
    return "\n".join(lines)


def feedback_keyboard(opp_id: str) -> dict:
    return {"inline_keyboard": [[
        {"text": label, "callback_data": f"fb:{action}:{opp_id}"} for action, label in FEEDBACK_ACTIONS.items()
    ]]}


async def _api(method: str, payload: dict) -> dict:
    token = get_scout_settings().bot_token
    if not token:
        logger.warning("telegram not configured; skipping %s", method)
        return {"ok": False, "description": "not configured"}
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(f"https://api.telegram.org/bot{token}/{method}", json=payload)
    data = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else {}
    if not data.get("ok"):
        logger.warning("telegram %s failed: %s", method, resp.text[:300])
    return data


def _chunks(text: str) -> list[str]:
    if len(text) <= MAX_CHARS:
        return [text]
    out, buf = [], ""
    for line in text.split("\n"):
        if len(buf) + len(line) + 1 > MAX_CHARS and buf:
            out.append(buf)
            buf = ""
        buf += line[:MAX_CHARS] + "\n"
    if buf:
        out.append(buf)
    return out


async def send(text: str, *, reply_markup: dict | None = None, chat_id: str | None = None) -> int | None:
    chat = chat_id or get_scout_settings().chat_id
    message_id = None
    parts = _chunks(text)
    for i, part in enumerate(parts):
        payload: dict = {"chat_id": chat, "text": part, "parse_mode": "HTML", "disable_web_page_preview": True}
        if reply_markup and i == len(parts) - 1:
            payload["reply_markup"] = reply_markup
        data = await _api("sendMessage", payload)
        if not data.get("ok") and "parse" in str(data.get("description", "")).lower():
            payload.pop("parse_mode")
            data = await _api("sendMessage", payload)
        message_id = (data.get("result") or {}).get("message_id", message_id)
    return message_id


async def push_card(o: dict) -> int | None:
    message_id = await send(render_card(o, await portfolio.get_settings()), reply_markup=feedback_keyboard(o["id"]))
    if message_id:
        await db.execute(
            "UPDATE scout.opportunities SET pushed_at = now(), tg_message_id = :m WHERE id = :id",
            {"m": message_id, "id": o["id"]},
        )
    return message_id


async def load_opportunity(opp_id: str) -> dict | None:
    return await db.fetch_one("SELECT * FROM scout.opportunities WHERE id = :id", {"id": opp_id})


# ─────────────────────────── inbound ───────────────────────────

async def handle_update(update: dict) -> None:
    settings = get_scout_settings()
    if "callback_query" in update:
        cq = update["callback_query"]
        if str((cq.get("message") or {}).get("chat", {}).get("id")) != str(settings.chat_id):
            return
        await _handle_callback(cq)
        return
    msg = update.get("message") or {}
    chat_id = str((msg.get("chat") or {}).get("id"))
    if chat_id != str(settings.chat_id):
        logger.warning("ignoring message from unauthorised chat %s", chat_id)
        return
    text = (msg.get("text") or "").strip()
    if not text:
        return
    try:
        reply = await dispatch(text)
    except Exception as exc:
        logger.exception("command failed")
        reply = friendly_error(exc)
    if reply:
        await send(reply, chat_id=chat_id)


def friendly_error(exc: Exception) -> str:
    name = type(exc).__name__
    if name == "AuthenticationError":
        return "⚠️ OpenAI key 无效或已过期，问答暂不可用。请在 Railway 的 Shared Variables 更新 OPENAI_API_KEY，并重新部署 Scout 服务。"
    if name in ("RateLimitError", "InsufficientQuotaError"):
        return "⚠️ OpenAI 额度不足或请求过于频繁，请稍后再试或检查账户余额。"
    if name in ("APITimeoutError", "APIConnectionError"):
        return "⚠️ 连接 OpenAI 超时，请稍后再试。"
    return f"⚠️ 处理失败：{esc(name)}（详情见 scout-web 日志）"


async def _handle_callback(cq: dict) -> None:
    data = cq.get("data") or ""
    parts = data.split(":", 2)
    if len(parts) == 3 and parts[0] == "fb" and parts[1] in FEEDBACK_ACTIONS:
        await db.execute(
            "INSERT INTO scout.feedback (opportunity_id, action) VALUES (:id, :a)",
            {"id": parts[2], "a": parts[1]},
        )
        if parts[1] == "star":
            opp = await load_opportunity(parts[2])
            if opp:
                await universe.add_watch(opp["ticker"])
        await _api("answerCallbackQuery", {"callback_query_id": cq["id"], "text": f"已记录：{FEEDBACK_ACTIONS[parts[1]]}"})


async def dispatch(text: str) -> str:
    if not text.startswith("/"):
        from scout.agent import answer

        return await answer(text, channel="telegram")
    cmd, *args = text.split()
    cmd = cmd.split("@")[0].lower()
    if cmd in ("/start", "/help"):
        return HELP_TEXT
    if cmd == "/ideas":
        return await ideas_text()
    if cmd == "/idea" and args:
        return await idea_text(args[0])
    if cmd == "/why" and args:
        return await why_text(args[0])
    if cmd == "/watch":
        return await watch_cmd(args)
    if cmd == "/stats":
        return await stats_text()
    if cmd in ("/sectors", "/market"):
        return await sectors_text()
    if cmd in ("/premarket", "/pre"):
        return await premarket_text()
    if cmd in ("/trades", "/positions"):
        return await trades_text()
    if cmd == "/sell":
        return await sell_cmd(args)
    return HELP_TEXT


def next_step(t: dict, m: dict) -> str:
    """The take-profit step for one open trade, in words."""
    from scout import levels

    pl = t.get("plan") or {}
    if "target1" not in pl:
        return ""
    for st in levels.exit_plan(pl, t["entry_price"], t["shares"], m["remaining"], t.get("stop")):
        if st["state"] == "due":
            return "⚠️ " + levels.due_advice(st, m.get("price"), t.get("stop"))
        if st["state"] == "pending":
            return (f"下一步：到目标 1 {st['price']:.2f} 后止损改用吊灯止损（不卖）" if st["stage"] == "目标 1"
                    else f"{m['remaining']:g} 股跟随吊灯止损")
    return ""


async def trades_text() -> str:
    held = await portfolio.trades(open_only=True)
    if not held:
        return "没有记录的持仓。在 Dashboard「我的交易」页记录买入后，这里和盘前简报会跟踪它们。"
    lines = ["<b>我的持仓</b>（按最近收盘价；止损和止盈由系统每天收盘后更新）"]
    total = 0.0
    for t in held:
        m = portfolio.metrics(t)
        qty = f"{m['remaining']:g}/{t['shares']:g}" if m["sold"] else f"{t['shares']:g}"
        if "pnl" not in m:
            lines.append(f"• <b>{esc(t['ticker'])}</b> {qty} 股 @ {t['entry_price']:.2f} · 无行情")
            continue
        total += m["pnl"]
        stop = f" · 止损 {t['stop']:.2f}（{pct(m['to_stop'], 1, True)}）" if "to_stop" in m else ""
        r = f" · {m['r']:+.1f}R" if "r" in m else ""
        step = next_step(t, m)
        lines.append(f"• <b>{esc(t['ticker'])}</b> {qty} 股 @ {t['entry_price']:.2f} → {m['price']:.2f} "
                     f"{m['pnl']:+,.0f}（{pct(m['pnl_pct'], 1, True)}）{r}{stop}" + (f"\n  {esc(step)}" if step else ""))
    lines.append(f"合计盈亏 {total:+,.0f}（含已实现）")
    return "\n".join(lines)


async def sell_cmd(args: list[str]) -> str:
    """/sell TICKER SHARES|all PRICE [YYYY-MM-DD]: a sell against the oldest open buy of that ticker."""
    from datetime import date

    from scout.market_calendar import us_equity_market_status

    usage = "用法：/sell 代码 股数 价格 [日期]，例如 /sell CART 66 61.50 或 /sell CART all 61.50 2026-10-28"
    if len(args) < 3:
        return usage
    try:
        ticker = universe.yahoo_symbol(args[0])
        shares = None if args[1].lower() in ("all", "全部") else float(args[1])
        price = float(args[2].replace("$", "").replace(",", ""))
        day = date.fromisoformat(args[3]) if len(args) > 3 else date.fromisoformat(us_equity_market_status().market_date)
    except ValueError:
        return usage
    held = [t for t in await portfolio.trades(open_only=True) if t["ticker"] == ticker]
    if not held:
        return f"没有 {esc(ticker)} 的持仓记录。"
    t = min(held, key=lambda x: (x["opened_on"], x["id"]))
    try:
        res = await portfolio.add_sell(t["id"], day, price, shares)
    except ValueError as exc:
        return esc(str(exc))
    if res["closed"]:
        return f"✅ 已记录：{esc(ticker)} 卖出 {res['shares']:g} 股 @ {price:.2f}，这笔持仓已全部卖出并平仓。"
    t2 = next(x for x in await portfolio.trades(open_only=True) if x["id"] == t["id"])
    m = portfolio.metrics(t2)
    step = next_step(t2, m)
    return (f"✅ 已记录：{esc(ticker)} 卖出 {res['shares']:g} 股 @ {price:.2f}，剩余 {m['remaining']:g} 股，"
            f"已实现 {m['realized']:+,.0f}。止损 {t2['stop']:.2f}" + (f"\n{esc(step)}" if step else ""))


async def premarket_text() -> str:
    from scout import premarket

    rec = await premarket.latest()
    if rec is None:
        return "还没有盘前简报，交易日北京时间 21:00 左右生成。"
    note = "" if rec.get("source") == "llm" else "\n（AI 简报未通过校验，要点为规则整理）"
    return esc(rec["content"]) + note


async def why_text(ticker: str) -> str:
    """Multi-lens interpretation (same workflow as the dashboard), cached per data date."""
    from scout import insight

    rec = await insight.latest(ticker)
    if rec is None:
        rec = await insight.generate(ticker)
        if rec is None:
            return f"{esc(ticker.upper())} 不在 Scout 的标的池中，可用 /watch add {esc(ticker.upper())} 加入。"
        rec["content"] = insight.render_text(insight.Insight(**rec["insight"]), rec["pack"])
    note = "" if rec.get("source") == "llm" else "\n（AI 解读未通过校验，以上为规则读数）"
    return esc(rec["content"]) + note


async def sectors_text() -> str:
    from scout import market_focus, sectors

    asof, rows = await sectors.latest("sector")
    if not rows:
        return "还没有板块数据，收盘扫描后会生成。"
    rows.sort(key=lambda r: -(r.get("heat") or 0))
    lines = [f"<b>板块概览</b>（{asof}，按热度排序）"]
    for r in rows:
        move = f"{sectors.QUADRANTS.get(r.get('quadrant_prev'))}→" if r.get("quadrant_prev") and r["quadrant_prev"] != r.get("quadrant") else ""
        lines.append(
            f"{sectors.HEAT_ICON.get(r.get('heat_label'), '')} <b>{esc(r['name'])}</b> {r['key']} · {move}{sectors.QUADRANTS.get(r.get('quadrant'))}"
            f" · 相对 SPY {pct(r.get('rel_20'), 1, True)} · 站上50日线 {pct(r.get('pct_above_50'), 0)} · 资金{esc(r.get('flow_label') or '')}"
        )
    focus = await market_focus.latest()
    if focus:
        lines.append("")
        lines.append(esc(focus["content"]))
    lines.append("\n完整视图见 Dashboard「市场全景」")
    return "\n".join(lines)


async def ideas_text() -> str:
    rows = await db.fetch_all(
        "SELECT * FROM scout.opportunities WHERE status IN ('new', 'triggered') ORDER BY score DESC LIMIT 15"
    )
    if not rows:
        return "当前没有活跃机会。"
    lines = ["<b>活跃机会</b>"]
    for o in rows:
        label = (o.get("evidence") or {}).get("setup", {}).get("label", o["primary_detector"])
        lines.append(
            f"• <b>{esc(o['ticker'])}</b> {esc(label)} {o['score']:.2f} | {STATUS_LABELS.get(o['status'])} | "
            f"入场 {o['entry_lo']:.2f}–{o['entry_hi']:.2f} 止损 {o['stop']:.2f} 目标 {o['target1']:.2f} | {o['asof']}"
        )
    lines.append("\n/idea TICKER 查看完整卡片")
    return "\n".join(lines)


async def idea_text(ticker: str) -> str:
    t = universe.yahoo_symbol(ticker)
    o = await db.fetch_one(
        "SELECT * FROM scout.opportunities WHERE ticker = :t ORDER BY asof DESC, score DESC LIMIT 1", {"t": t}
    )
    if o:
        return render_card(o, await portfolio.get_settings())
    from scout.agent import answer

    return await answer(f"{t} 目前没有规则触发的机会卡。请基于数据评估它当前的形态、相对强度和风险，并说明需要什么条件才会成为机会。", channel="telegram")


async def watch_cmd(args: list[str]) -> str:
    if not args or args[0] == "list":
        rows = await universe.load_watchlist()
        by_theme: dict[str, list[str]] = {}
        for r in rows:
            by_theme.setdefault(r["theme"] or "未分类", []).append(r["ticker"])
        return "<b>Watchlist</b>\n" + "\n".join(f"{esc(k)}：{' '.join(v)}" for k, v in by_theme.items())
    if args[0] == "add" and len(args) >= 2:
        await universe.add_watch(args[1], " ".join(args[2:]) or None)
        return f"已加入 {esc(args[1].upper())}，下次收盘扫描生效。"
    if args[0] in ("rm", "remove", "del") and len(args) >= 2:
        await universe.remove_watch(args[1])
        return f"已移除 {esc(args[1].upper())}。"
    return "用法：/watch list | add NVDA [主题] | rm NVDA"


async def stats_text() -> str:
    from scout.tracker import scoreboard

    rows = await scoreboard()
    if not rows:
        return "还没有机会记录。"
    lines = ["<b>规则记分板</b>（实盘 vs 历史，均已扣除交易成本）"]
    for r in rows:
        br = r.get("base_rate") or {}
        lines.append(
            f"• {esc(r['detector'])}：共 {r['total']}，活跃 {r['active']}，已结束 {r['closed']}，"
            f"扣费胜率 {pct(r['win_rate_net'], 0)}，扣费超额 {pct(r['avg_net_excess'], 1, True)} | "
            f"历史扣费胜率 {pct(br.get('win_rate_net', br.get('win_rate')), 0)} "
            f"相对持有 {pct(br.get('avg_edge_vs_hold'), 1, True)} | "
            f"👍{r['thumbs_up']} 👎{r['thumbs_down']}"
        )
    return "\n".join(lines)
