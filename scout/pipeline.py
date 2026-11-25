"""End-of-day and weekly pipelines."""
from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime, timedelta

from scout import (bars, base_rates, conditional_rates, context, db, events, ledger, opportunities, portfolio, sector_trend, sectors,
                   structure, telegram, thesis, tracker, universe)
from scout.config import get_scout_settings
from scout.data.provider import get_provider
from scout.features import SNAPSHOT_FEATURES, FeaturePanel, build_panel
from scout.market_calendar import previous_us_equity_trading_day, us_equity_market_status

logger = logging.getLogger("scout.pipeline")

PANEL_LOOKBACK_DAYS = 420          # ≥ 252 sessions + 200-day SMA warm-up headroom
MAX_PER_DETECTOR = 3               # keep one rule (e.g. a whole sector turning) from flooding the cards


async def prepare(full_backfill: bool = False, replace: bool = True) -> dict:
    """Migrations, universe and bars. Idempotent."""
    settings = get_scout_settings()
    out: dict = {"migrations": await db.apply_migrations()}
    out["watchlist_seeded"] = await universe.seed_watchlist()
    count = await db.fetch_one("SELECT count(*) AS n FROM scout.instruments WHERE kind = 'stock'")
    if full_backfill or not count or count["n"] < 400:
        out["instruments"] = await universe.refresh_instruments()
    await universe.ensure_watchlist_instruments()
    tickers = [r["ticker"] for r in await universe.load_scan_instruments()]
    provider = get_provider()
    if full_backfill:
        out["bars"] = await bars.backfill(provider, tickers, settings.scout_history_years, replace=replace)
    else:
        out["bars"] = await bars.refresh(provider, tickers, settings.scout_history_years)
    return out


PRICE_SAMPLE = 30                  # most traded stocks checked before the scan, plus SPY and the holdings
PRICE_BAD_SHARE = 0.2              # this share of the sample off Finnhub by > 0.5 %: the vendor's closes are not final yet
PRICE_RETRIES = 3
PRICE_WAIT_S = 300


async def price_sample() -> list[str]:
    rows = await db.fetch_all(
        "SELECT ticker FROM scout.trades WHERE closed_on IS NULL UNION SELECT 'SPY' UNION SELECT ticker FROM ("
        " SELECT f.ticker FROM scout.features_daily f JOIN scout.instruments i USING (ticker)"
        " WHERE f.date = (SELECT max(date) FROM scout.features_daily) AND i.kind = 'stock'"
        " ORDER BY (f.payload->>'dollar_vol20')::float DESC NULLS LAST LIMIT :n) top", {"n": PRICE_SAMPLE})
    return sorted(r["ticker"] for r in rows)


def closes_unsettled(checked: int, off: list) -> bool:
    return len(off) >= 3 and len(off) >= PRICE_BAD_SHARE * checked


async def settle_closes(wait_s: float = PRICE_WAIT_S) -> dict:
    """Right after the bell Yahoo sometimes serves closes that are not final (2026-11-18: 84 of 129 stocks off by up to
    4 %, corrected the next day). Before anything is computed, a sample is checked against Finnhub; while a fifth or
    more is off, wait and fetch the bars again (at most PRICE_RETRIES times), then go on either way."""
    from scout import research_data

    tickers = await price_sample()
    tries = []
    for attempt in range(PRICE_RETRIES + 1):
        checked, off = await research_data.price_offsets(tickers)
        tries.append({"checked": checked, "off": len(off)})
        if not closes_unsettled(checked, off) or attempt == PRICE_RETRIES:
            break
        logger.warning("closes not settled: %d of %d off Finnhub; refetching in %ds", len(off), checked, wait_s)
        await asyncio.sleep(wait_s)
        await prepare()
    return {"tries": tries, "settled": not closes_unsettled(checked, off), "checked": checked,
            "off": [o["ticker"] for o in off][:20]}


def unsettled_text(check: dict) -> str | None:
    if check.get("settled", True):
        return None
    return (f"⚠️ <b>收盘价核对</b>：抽查 {check['checked']} 只，{len(check['off'])} 只的 Yahoo 收盘价与 Finnhub 相差超过 0.5%，"
            f"重新拉取 {PRICE_RETRIES} 次仍未更正；今天的扫描和汇总可能用了未定稿的价格，下一次拉取时日线会更正")


async def load_panel(lookback_days: int | None = PANEL_LOOKBACK_DAYS) -> FeaturePanel:
    settings = get_scout_settings()
    instruments = {r["ticker"]: r for r in await universe.load_scan_instruments()}
    since = date.today() - timedelta(days=lookback_days) if lookback_days else None
    frames = await bars.load(list(instruments), since=since)
    return build_panel(frames, instruments, settings.scout_min_price, settings.scout_min_dollar_volume)


STRUCTURE_FIELDS = ("swing_trend", "swing_break", "support", "dist_support", "resistance", "dist_resistance", "updown_vol_20")


def panel_bars(p: FeaturePanel, ticker: str, asof: date, n: int = structure.LOOKBACK) -> list[dict]:
    """One ticker's last `n` sessions up to `asof`, as bar dicts (for scout.structure)."""
    end = p.dates.get_loc(asof) + 1
    cols = {k: p[k][ticker].to_numpy()[max(0, end - n):end] for k in ("open", "high", "low", "close", "volume")}
    dates = p.dates[max(0, end - n):end]
    return [
        {"date": d, **{k: float(cols[k][i]) for k in cols}}
        for i, d in enumerate(dates) if cols["close"][i] == cols["close"][i]   # skip NaN
    ]


async def store_features(p: FeaturePanel, asof: date) -> int:
    """Daily snapshot: SNAPSHOT_FEATURES plus price structure (support/resistance, swings)."""
    def rows():
        for ticker in p["close"].columns:
            payload = {k: round(v, 6) for k, v in p.row(ticker, asof, SNAPSHOT_FEATURES).items()}
            if "close" not in payload:
                continue
            s = structure.compute(panel_bars(p, ticker, asof))
            payload.update({k: (round(s[k], 6) if isinstance(s[k], float) else s[k]) for k in STRUCTURE_FIELDS if k in s})
            yield {"t": ticker, "d": asof, "p": db.jsonb(payload)}

    return await db.execute_many(
        "INSERT INTO scout.features_daily (ticker, date, payload) VALUES (:t, :d, CAST(:p AS JSONB)) "
        "ON CONFLICT (ticker, date) DO UPDATE SET payload = EXCLUDED.payload",
        rows(),
    )


async def ensure_base_rates(force: bool = False) -> dict:
    rates = {} if force else await base_rates.load()
    if rates:
        return rates
    logger.info("computing base rates from full history")
    full = await load_panel(lookback_days=None)
    rates = base_rates.compute(full)
    await base_rates.store(rates)
    await events.store_base_rates(events.compute_base_rates(full))     # same full history, same week
    return rates


async def store_signal_hits(p: FeaturePanel, rates: dict, asof: date) -> int:
    """Every hit of every rule today (live and candidate), with its explanation —
    the strategy screener shows these; the opportunity board keeps the Top N."""
    from scout.detectors import registry

    rows = []
    for det in registry(include_candidates=True):
        for cand in opportunities.scan(p, rates, asof, detectors=[det]):
            rows.append({"d": asof, "det": det.name, "t": cand.ticker, "s": cand.score, "p": db.jsonb({
                "why": opportunities.why_block(cand),
                "levels": {"entry_lo": cand.levels.entry_lo, "entry_hi": cand.levels.entry_hi, "stop": cand.levels.stop,
                           "target1": cand.levels.target1},
                "live": det.live,
            })})
    await db.execute("DELETE FROM scout.signal_hits WHERE asof = :d", {"d": asof})
    return await db.execute_many(
        "INSERT INTO scout.signal_hits (asof, detector, ticker, score, payload) VALUES (:d, :det, :t, :s, CAST(:p AS JSONB))",
        rows,
    )


async def bet_context(top: list) -> dict[str, str]:
    """Holdings (core ETFs left out: they move with everything) and the open cards not scanned again today."""
    from scout import levels

    today = {c.ticker for c in top}
    held = await db.fetch_all("SELECT DISTINCT ticker FROM scout.trades WHERE closed_on IS NULL")
    cards = await db.fetch_all("SELECT DISTINCT ticker FROM scout.opportunities WHERE status IN ('new', 'triggered')")
    out = {r["ticker"]: "机会卡" for r in cards if r["ticker"] not in today}
    out.update({r["ticker"]: "持仓" for r in held if not levels.is_core(r["ticker"]) and r["ticker"] not in today})
    return out


def select_top(candidates: list, n: int, per_detector: int = MAX_PER_DETECTOR) -> list:
    """Highest scores first, at most `per_detector` cards from any one rule."""
    picked, counts = [], {}
    for c in candidates:
        if counts.get(c.primary.name, 0) >= per_detector:
            continue
        picked.append(c)
        counts[c.primary.name] = counts.get(c.primary.name, 0) + 1
        if len(picked) >= n:
            break
    return picked


def expected_session(now: datetime | None = None) -> tuple[date | None, str]:
    """The session whose close an EOD scan covers; None on weekends/holidays."""
    status = us_equity_market_status(now)
    today = date.fromisoformat(status.market_date)
    if not status.is_trading_day:
        return None, status.reason
    if status.reason == "after_close":
        return today, "after_close"
    return previous_us_equity_trading_day(today), "before_close"


def days_to_next_earnings(earnings: dict[str, list[dict]], asof: date) -> dict[str, int]:
    out = {}
    for t, events in earnings.items():
        nxt = [e["date"] for e in events if e["date"] >= asof]
        if nxt:
            out[t] = (nxt[0] - asof).days
    return out


async def run_eod(push: bool = True, force: bool = False) -> dict:
    """After-close scan. Skips market holidays; a session is scanned and pushed
    once — `force` rescans (e.g. to regenerate cards) without the guards."""
    settings = get_scout_settings()
    session, reason = expected_session()
    if session is None:
        if not force:
            return {"skipped": reason}
        session = previous_us_equity_trading_day(date.today())
    summary: dict = {"session": session.isoformat(), "prepare": await prepare()}
    deferred: list[str] = []           # pushes folded into the digest
    managed: list[dict] = []
    prop_signals: list[dict] = []
    try:                               # closes checked against Finnhub before anything is computed from them
        summary["price_check"] = await settle_closes()
        if text := unsettled_text(summary["price_check"]):
            deferred.append(text)
    except Exception as exc:
        logger.warning("price check failed: %s", exc)
        summary["price_check"] = f"failed: {type(exc).__name__}"
    p = await load_panel()
    asof: date = p.dates[-1]
    summary["asof"] = asof.isoformat()
    if asof < session:
        raise RuntimeError(f"bar for {session} not available yet (latest {asof}); data source may be lagging")
    summary["features"] = await store_features(p, asof)
    sector_rows = sectors.compute(p, asof)
    sector_prev = await sectors.before(asof)
    for r in sector_rows:              # trend states that changed since the previous session (digest)
        r["trend_changes"] = sector_trend.changes(sector_prev.get(r["key"]), r)
    summary["sectors"] = await sectors.store(asof, sector_rows)
    try:                               # historical norms of SPY, sectors and themes (descriptive)
        from scout import norms

        summary["norms"] = await norms.store(asof)
    except Exception as exc:
        logger.warning("norms failed: %s", exc)
        summary["norms"] = f"failed: {type(exc).__name__}"
    sector_ctx = {r["sector"]: sectors.card_context(r) for r in sector_rows if r["kind"] == "sector"}

    transitions = await tracker.update_all()
    summary["transitions"] = [t.__dict__ for t in transitions]
    try:                               # multi-angle features (financials, events, insiders), point in time
        from scout import angles

        summary["angles"] = await angles.snapshot(p, asof)
    except Exception as exc:
        logger.warning("angle features failed: %s", exc)
        summary["angles"] = f"failed: {type(exc).__name__}"
    try:                               # the user's B0 ranking on Scout's data (information only)
        from scout import b0

        summary["b0_rank"] = await b0.store_b0_rank(p, asof)
    except Exception as exc:
        logger.warning("b0 ranking failed: %s", exc)
        summary["b0_rank"] = f"failed: {type(exc).__name__}"
    try:                               # forward test of gated rules (G3); never blocks the scan
        from scout import forward

        summary["forward"] = {"recorded": await forward.record(p, asof), "updated": await forward.update(),
                              "ranks_updated": await forward.update_ranks(p)}
    except Exception as exc:
        logger.warning("forward record failed: %s", exc)
        summary["forward"] = f"failed: {type(exc).__name__}"
    try:                               # the user's holdings: system stop / target after this close
        managed = await portfolio.manage_open_trades()
        summary["trades_managed"] = managed
        if managed:                    # in the digest; sent alone only when the session was already scanned
            deferred.append("📌 <b>持仓止损更新</b>\n" + "\n".join(
                f"• <b>{telegram.esc(m['ticker'])}</b>：" + "；".join(telegram.esc(x) for x in m["messages"]) for m in managed))
    except Exception as exc:
        logger.warning("trade management failed: %s", exc)
        summary["trades_managed"] = f"failed: {type(exc).__name__}"
    try:                               # AI spend of the last 24 hours, pushed once when unusually high
        from scout import llm_usage

        summary["llm_usage"] = await llm_usage.check_alert(push)
    except Exception as exc:
        logger.warning("llm usage check failed: %s", exc)
    try:                               # news passed along the supply chain (inference, scored in the ledger)
        from scout import propagation

        prop = await propagation.run(asof)
        summary["propagation"] = {k: v for k, v in prop.items() if k != "signals_list"}
        prop_signals = prop["signals_list"]
        held = {t["ticker"] for t in await portfolio.trades(open_only=True)}
        watch = {r["ticker"] for r in await universe.load_watchlist()}
        text = propagation.push_text(prop_signals, held, watch)
        if text:
            deferred.append(text)
    except Exception as exc:
        logger.warning("propagation failed: %s", exc)
        summary["propagation"] = f"failed: {type(exc).__name__}"
    mfilter = None
    try:                               # the user's market filter (no new buys while risk is off)
        from scout import intraday, market_filter

        mfilter = await market_filter.current(asof)
        summary["market_filter"] = {"off": mfilter["off"], "since": str(mfilter["since"])} if mfilter else None
        if push and mfilter and mfilter["changed_today"] and await intraday._claim(
                [{"key": f"market_filter:{mfilter['since']}", "kind": "market_filter", "ticker": None, "payload": None}]):
            await telegram.send(("🛑 " if mfilter["off"] else "🟢 ") + "<b>大盘过滤状态改变</b>\n"
                                + telegram.esc(market_filter.text(mfilter))
                                + ("\n按计划不新买入；已有持仓照常按回落 20% 管理，不因此卖出。" if mfilter["off"]
                                   else "\n恢复正常：可以按计划新买入。"))
    except Exception as exc:
        logger.warning("market filter failed: %s", exc)
        summary["market_filter"] = f"failed: {type(exc).__name__}"
    try:                               # the ledger must never block the scan
        summary["ledger"] = {"sector_heat": await ledger.record_sector_heat(sector_rows, asof),
                             "sector_trend": await ledger.record_sector_trend(sector_rows, sector_prev, asof),
                             "scored": await ledger.score_due()}
    except Exception as exc:
        logger.warning("ledger update failed: %s", exc)
        summary["ledger"] = f"failed: {type(exc).__name__}"
    already = await db.fetch_one(
        "SELECT 1 AS x FROM scout.briefs WHERE kind = 'eod' AND asof = :d LIMIT 1", {"d": asof}
    )
    if already and not force:
        summary["skipped_scan"] = "session already scanned"
        if push:
            for text in deferred:
                await telegram.send(text)
        return summary

    summary["earnings_rows"] = await context.refresh_earnings()
    earnings = await context.earnings_map(asof)
    rates = await ensure_base_rates()
    watch = {r["ticker"]: r["theme"] for r in await universe.load_watchlist()}

    candidates = opportunities.scan(
        p, rates, asof,
        recent=await opportunities.recent_keys(asof),
        watchlist=watch,
        feedback_weights=await opportunities.feedback_weights(),
        days_to_earnings=days_to_next_earnings(earnings, asof),
    )
    summary["candidates"] = len(candidates)
    summary["signal_hits"] = await store_signal_hits(p, rates, asof)
    market = opportunities.market_context(p, asof)
    from scout import learned

    quiet = learned.held_back(await learned.current())
    top = select_top(candidates, settings.scout_top_n)
    bets: dict = {}
    try:                               # cards that move together are one bet, not several
        idio = p["idio_ret_1d"]          # before the signal day: the day that fired many cards at once would tie them all
        bets = opportunities.same_bets([(c.ticker, c.score) for c in top], idio[idio.index < asof], await bet_context(top))
    except Exception as exc:
        logger.warning("same-bet grouping failed: %s", exc)
    saved = []
    for c in top:
        news = await context.load_news(c.ticker)
        c.context = {
            "news": news,
            "event_flags": context.event_flags(news),
            "earnings": context.earnings_summary(earnings.get(c.ticker, []), asof),
            "sector_context": sector_ctx.get(c.sector),
        }
        ev = opportunities.evidence(c, market)
        if c.ticker in bets:
            ev["same_bet"] = bets[c.ticker]
        # a rule the knowledge base rejects gets the plain template: an LLM text would sell a card that is only a record
        th, source = (thesis.template_thesis(ev, rejected=True), "template") if c.primary.name in quiet else await thesis.write_thesis(ev)
        try:                           # history shown on the card, after the thesis (the LLM never sees it)
            ev["conditional"] = await conditional_rates.for_signal(c.primary.name, c.sector, market.get("regime"))
        except Exception as exc:
            logger.warning("conditional rate for %s failed: %s", c.ticker, exc)
        await opportunities.save(c, ev, th.model_dump(), source)
        saved.append(c.id)
    summary["saved"] = saved
    try:
        summary["ledger_opportunities"] = await ledger.backfill_opportunities()
    except Exception as exc:
        logger.warning("ledger opportunities failed: %s", exc)

    focus = None
    try:                               # an LLM hiccup must not fail the scan
        from scout import market_focus

        focus = await market_focus.generate(market)
        summary["market_focus"] = focus["source"] if focus else None
    except Exception as exc:
        logger.warning("market focus failed: %s", exc)
        summary["market_focus"] = f"failed: {type(exc).__name__}"

    cards = [o for o in [await telegram.load_opportunity(i) for i in saved] if o]
    muted = [o for o in cards if o["primary_detector"] in quiet]
    scan = scan_lines(candidates, transitions, saved)
    if mfilter and mfilter["off"]:
        scan.append("大盘过滤处于避险：下面的机会卡只作观察。")
    bet_line = opportunities.same_bet_summary(bets)
    if bet_line:
        scan.append(bet_line)
    if muted:
        scan.append(f"今天有 {len(muted)} 张规则机会卡只记录、不推送：这些规则没有通过公平基准检验"
                    "（见 Dashboard「知识库」），仍在机会看板和判断台账里跟踪。")
    g: dict = {}
    short = None                       # what Telegram gets; the full digest is the stored brief
    try:
        from scout import digest

        g = await digest.gather(asof, earnings, prop_signals, managed if isinstance(managed, list) else [])
        try:                           # reviews of today's events and tomorrow's lines
            from scout import event_calendar

            g["events"] = await event_calendar.run_eod(asof)
        except Exception as exc:
            logger.warning("event calendar failed: %s", exc)
        try:                           # prediction markets: snapshot everything followed, report the day's moves
            from scout import prediction

            g["prediction"] = (await prediction.run(asof, "eod")).get("moves") or []
        except Exception as exc:
            logger.warning("prediction markets failed: %s", exc)
        user = await portfolio.get_settings()
        g = await digest.enrich(g, p, asof, sector_rows, user.get("account_size"))
        read = None
        try:                           # why the market moved today (its summary replaces the digest's market lines)
            from scout import market_read

            read = await market_read.generate(p, asof, sector_rows, g, mfilter)
            summary["market_read"] = read["source"]
        except Exception as exc:
            logger.warning("market read failed: %s", exc)
            summary["market_read"] = f"failed: {type(exc).__name__}"
        brief = digest.render(asof, market, sector_rows, focus, mfilter, g, portfolio.satellite(user), scan, read=read)
        short = digest.render_short(asof, market, mfilter, g, portfolio.satellite(user), read=read)
    except Exception as exc:           # the digest must not cost the session its brief
        logger.warning("digest failed: %s", exc)
        brief = "\n\n".join([render_eod_brief(asof, market, sector_rows, focus), *deferred, "\n".join(scan)])
    await db.execute(
        "INSERT INTO scout.briefs (kind, asof, content, payload) VALUES ('eod', :d, :c, CAST(:p AS JSONB))",
        {"d": asof, "c": brief, "p": db.jsonb({"market": market, "saved": saved, "candidates": len(candidates),
                                        "market_day": g.get("market_day"),
                                        "notes": g.get("notes"), "steps": g.get("steps")})},
    )
    if push:
        from scout import todo

        try:                           # the to-do list first: what needs the user's hand
            summary["todo"] = await todo.send(session)
        except Exception as exc:
            logger.warning("to-do list failed: %s", exc)
        await telegram.send(short or brief)
        pushed = 0
        for o in cards:
            if o["primary_detector"] in quiet:
                continue
            lead = ((o.get("evidence") or {}).get("same_bet") or {}).get("lead")
            if lead and lead != o["ticker"]:      # the group's leader carries the bet
                continue
            if o["score"] >= settings.scout_push_threshold and pushed < settings.scout_max_push_cards:
                if await telegram.push_card(o):
                    pushed += 1
        summary["pushed"] = pushed
    try:                               # up to three candidates for the user to decide on (scored in the ledger)
        from scout import todo

        summary["candidates_sent"] = await todo.send_candidates(asof, push=push)
    except Exception as exc:
        logger.warning("candidates failed: %s", exc)
        summary["candidates_sent"] = f"failed: {type(exc).__name__}"
    summary["held_back"] = [o["id"] for o in muted]
    return summary


def render_eod_brief(asof: date, market: dict, sector_rows: list[dict] | None = None, focus: dict | None = None) -> str:
    """Market part of the brief; the fallback when the digest fails."""
    pct = telegram.pct
    lines = [
        f"🌇 <b>Scout 收盘扫描 {asof}</b>",
        f"大盘：{telegram.esc(opportunities.state_text(market, detail=True))}\nSPY 当日 {pct(market.get('spy_ret_1d'), 1, True)}，"
        f"20 日 {pct(market.get('spy_ret_20d'), 1, True)} | QQQ 20 日 {pct(market.get('qqq_ret_20d'), 1, True)} | "
        f"SMH 20 日 {pct(market.get('smh_ret_20d'), 1, True)}",
    ]
    rotation = sectors.rotation_line(sector_rows or [])
    if rotation:
        lines.append(rotation)
    if focus and focus.get("focus"):
        f = focus["focus"]
        lines.append(f"\n🧭 <b>市场关注点</b>：{telegram.esc(f['headline'])}")
        lines += [f"{i}. {telegram.esc(t['title'])}" for i, t in enumerate(f.get("themes", []), 1)]
    return "\n".join(lines)


def scan_lines(candidates, transitions, saved: list[str]) -> list[str]:
    """Opportunity status changes and today's rule hits (the last part of the digest)."""
    pct = telegram.pct
    lines = []
    if transitions:
        icon = {"triggered": "▶️", "hit_target": "🎯", "stopped": "🛑", "trailed": "🏁", "expired": "⌛", "expired_untriggered": "⌛"}
        lines.append("机会状态变化：")
        for t in transitions:
            ret = f" {pct(t.return_pct, 1, True)}" if t.return_pct is not None else ""
            lines.append(f"{icon.get(t.new, '•')} {t.ticker} → {telegram.STATUS_LABELS.get(t.new, t.new)}{ret}")
    if candidates:
        by_det: dict[str, int] = {}
        for c in candidates:
            by_det[c.primary.label] = by_det.get(c.primary.label, 0) + 1
        lines.append(f"今日规则触发 {len(candidates)} 个候选（{'，'.join(f'{k} {v}' for k, v in by_det.items())}），"
                     f"入选 {len(saved)} 张机会卡：")
        chosen = set(saved)
        for c in candidates:
            if c.id in chosen:
                lines.append(f"• <b>{c.ticker}</b> {c.primary.label} {c.score:.2f}")
    else:
        lines.append("今日没有规则触发。")
    return lines


async def run_weekly(push: bool = True) -> dict:
    summary: dict = {"prepare": await prepare(full_backfill=True)}
    rates = await ensure_base_rates(force=True)
    summary["base_rates"] = {d: b.get("all", {}).get("n", 0) for d, b in rates.items()}
    try:                               # multi-angle data for the whole universe: SEC filings, financials, insider trades
        from scout import research_data

        summary["sec_universe"] = await research_data.universe_sec(date.today())
        summary["insiders"] = await research_data.insiders(date.today())
    except Exception as exc:
        logger.warning("universe SEC refresh failed: %s", exc)
        summary["sec_universe"] = f"failed: {type(exc).__name__}"
    try:                               # company profiles: new annual reports since last week (10-K Item 1 / 20-F Item 4)
        from scout import company_profile

        summary["company_profiles"] = await company_profile.refresh()
    except Exception as exc:
        logger.warning("company profiles failed: %s", exc)
        summary["company_profiles"] = f"failed: {type(exc).__name__}"
    try:                               # event calendar: rule dates, the Fed's schedule, the week's web search
        from scout import event_calendar

        summary["calendar"] = await event_calendar.refresh(date.today(), web=True)
    except Exception as exc:
        logger.warning("event calendar failed: %s", exc)
        summary["calendar"] = f"failed: {type(exc).__name__}"
    try:                               # playbooks: every catalogued way to trade on the latest close, read by the weekly report
        from scout import playbooks

        res = await playbooks.compute()
        await playbooks.store(res)
        summary["playbooks"] = {"asof": res["asof"], "missing": [b["key"] for b in res["playbooks"] if b.get("missing")]}
    except Exception as exc:
        logger.warning("playbooks failed: %s", exc)
        summary["playbooks"] = f"failed: {type(exc).__name__}"
    try:                               # the weekly market report: market → sectors → stocks, facts store + Scout data, ledger-scored
        from scout import weekly_view

        summary["views"] = await weekly_view.run(push=False)        # in the weekly push below; the full text on the web
    except Exception as exc:
        logger.warning("views desk failed: %s", exc)
        summary["views"] = f"failed: {type(exc).__name__}"
    try:                               # industry outlook (v3 P4): drivers + at most three theses
        from scout import outlook

        summary["outlook"] = await outlook.generate()
    except Exception as exc:
        logger.warning("sector outlook failed: %s", exc)
        summary["outlook"] = f"failed: {type(exc).__name__}"
    report = await render_weekly_report()
    await db.execute(
        "INSERT INTO scout.briefs (kind, asof, content) VALUES ('weekly', :d, :c)", {"d": date.today(), "c": report}
    )
    if push:
        await telegram.send(await render_weekly_short())
    return summary


async def render_weekly_short() -> str:
    """The one weekly push (user's request 2026-11-25): the week's story line from the daily market reads, the market
    view in four lines, sectors in one, the outlook in one, the user's candidate decisions, the calibration. The full
    report, the 14 views, the playbooks and the scoreboard stay on the web and behind /weekly."""
    from scout import market_read, outlook, todo, weekly_view

    esc, today = telegram.esc, date.today()
    lines = [f"🗓 <b>Scout 周报 {today}</b>（完整版发 /weekly）"]
    reads = await db.fetch_all("SELECT DISTINCT ON (asof) asof, payload FROM scout.briefs WHERE kind = 'market_read' AND asof > :d "
                               "ORDER BY asof, id DESC", {"d": today - timedelta(days=7)})
    if reads:
        wd = "一二三四五六日"
        lines.append("<b>【本周故事线】</b>")
        lines += [f"· {r['asof']:%m-%d} 周{wd[r['asof'].weekday()]}：{esc(r['payload']['read']['headline'])}"
                  f"（{esc(r['payload']['read']['story_status'])}）" for r in reads]
    from scout import market_memory

    try:
        threads = await market_memory.theme_lines(today)
    except Exception as exc:
        logger.warning("theme lines failed: %s", exc)
        threads = []
    if threads:
        lines.append("<b>【进行中的主线】</b>")
        lines += [f"· {esc(x)}" for x in threads]
    wv = await weekly_view.latest()
    m = (wv or {}).get("market")
    if m and str(wv.get("asof", "")) >= str(today - timedelta(days=3)):
        lines += ["<b>【市场】</b>", esc(m["week"]),
                  f"{weekly_view.ICON[m['stance']]} 未来 20 个交易日 SPY：{esc(m['stance'])}（把握{esc(m['confidence'])}，记入判断台账）",
                  f"未来两周：{esc(m['ahead'])}", f"对你的组合：{esc(m['for_you'])}"]
        heat = {x["ETF"]: x.get("热度") for x in (wv.get("pack") or {}).get("sectors", [])}
        hot = [x["etf"] for x in m["sectors"] if heat.get(x["etf"]) == "热"]
        cold = [x["etf"] for x in m["sectors"] if heat.get(x["etf"]) == "冷"]
        if hot or cold:
            lines.append(f"板块：热 {'、'.join(hot) or '无'}；冷 {'、'.join(cold) or '无'}")
    ol = await outlook.latest()
    if ol and ol["asof"] >= str(today - timedelta(days=7)):
        theses = ((ol.get("outlook") or {}).get("theses")) or []
        lines.append("行业前景：" + ("；".join(f"{t['target']} {t['stance']}（{t['horizon']} 日）：{t['title']}" for t in theses)
                                if theses else "本周没有新判断"))
    picks = todo.stats_line(await todo.decision_stats(20))
    if picks:
        lines.append("<b>【你的候选决定】</b>\n" + esc(picks))
    from scout import behavior

    try:
        record = await behavior.lines_for("week", today)
    except Exception as exc:
        logger.warning("execution record failed: %s", exc)
        record = []
    if record:
        lines.append("<b>【你的执行】</b>\n" + "\n".join(esc(x.removeprefix("你的记录：")) for x in record))
    cal = [r for r in await ledger.calibration(20) if r["enough"] and r["kind"] in ("insight", "view", "sector_heat", "candidate")]
    if cal:
        lines.append("<b>【判断校准】</b>20 日命中率：" + esc("；".join(
            f"{ledger.KIND_ZH[r['kind']]}·{ledger.STANCE_ZH[r['stance']]} {r['hit_rate'] * 100:.0f}%（{r['n']}）"
            for r in sorted(cal, key=lambda r: -r["n"])[:4])))
    lines.append(f"（{market_read.NOTE}；14 篇个股和板块观点、打法简报、规则记分板在网页上）")
    return "\n".join(lines)


async def render_weekly_report() -> str:
    since = date.today() - timedelta(days=7)
    closed = await db.fetch_all(
        "SELECT o.ticker, o.primary_detector, oc.outcome, oc.return_pct, oc.excess_pct "
        "FROM scout.opportunity_outcomes oc JOIN scout.opportunities o ON o.id = oc.opportunity_id "
        "WHERE oc.closed_on >= :d ORDER BY oc.closed_on", {"d": since},
    )
    new = await db.fetch_one("SELECT count(*) AS n FROM scout.opportunities WHERE asof >= :d", {"d": since})
    pct = telegram.pct
    lines = [f"🗓 <b>Scout 周报 {date.today()}</b>", f"本周新机会 {new['n'] if new else 0} 个，结束 {len(closed)} 个。"]
    for r in closed:
        lines.append(
            f"• {r['ticker']} {r['primary_detector']} → {telegram.STATUS_LABELS.get(r['outcome'], r['outcome'])} "
            f"{pct(r['return_pct'], 1, True)}（超额 {pct(r['excess_pct'], 1, True)}）"
        )
    _, sector_rows = await sectors.latest("sector")
    rotation = sectors.rotation_line(sector_rows)
    if rotation:
        lines.append(f"\n本周{rotation}")
    from scout import outlook

    ol = await outlook.latest()
    if ol and ol["asof"] >= str(since):
        lines.append("\n<b>行业前景</b>\n" + telegram.esc("\n".join(outlook.render_text(ol).split("\n")[1:])))
    cal = [r for r in await ledger.calibration(20) if r["enough"] and r["kind"] in ("insight", "insight_lens", "sector_heat")]
    if cal:
        from scout.insight import LENS_TITLES

        best = sorted(cal, key=lambda r: -r["hit_rate"])[:3]
        worst = sorted(cal, key=lambda r: r["hit_rate"])[:2]
        def label(r):
            name = LENS_TITLES.get(r["lens"], r["lens"]) if r["kind"] == "insight_lens" else ledger.KIND_ZH[r["kind"]]
            return f"{name}·{ledger.STANCE_ZH[r['stance']]} {r['hit_rate'] * 100:.0f}%（{r['n']}）"
        lines.append("\n<b>判断校准（20 日命中率）</b>\n最准：" + telegram.esc("；".join(label(r) for r in best))
                     + "\n最弱：" + telegram.esc("；".join(label(r) for r in worst)))
    from scout import todo

    picks = todo.stats_line(await todo.decision_stats(20))
    if picks:
        lines.append("\n<b>你的候选决定</b>\n" + telegram.esc(picks))
    rated = await db.fetch_one("SELECT count(*) AS n FROM scout.judgments WHERE human_rating IS NOT NULL "
                               "AND made_at >= date_trunc('month', now())")
    lines.append(f"本月人工抽检 {rated['n']}/10（Dashboard「判断台账」）")
    health = base_rates.rule_health(await base_rates.load())
    if health:
        lines.append("\n<b>规则体检</b>\n" + "\n".join(telegram.esc(x) for x in health))
    lines.append("")
    lines.append(await telegram.stats_text())
    return "\n".join(lines)

