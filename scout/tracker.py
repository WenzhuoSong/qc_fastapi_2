"""Opportunity lifecycle tracking against stored daily bars.

new ──(price trades in entry zone)──► triggered ──► hit_target | stopped | expired
 └──(no fill within the trigger window)──► expired_untriggered
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date

from scout import bars as bar_store
from scout import db
from scout.detectors import Levels
from scout.simulate import Bar, SimResult, simulate
from scout.universe import BENCHMARK

logger = logging.getLogger("scout.tracker")

ACTIVE = ("new", "triggered")
_STATE_TO_STATUS = {
    "pending": "new",
    "open": "triggered",
    "untriggered": "expired_untriggered",
    "hit_target": "hit_target",
    "stopped": "stopped",
    "expired": "expired",
}


@dataclass
class Transition:
    opportunity_id: str
    ticker: str
    old: str
    new: str
    price: float | None
    return_pct: float | None


def levels_of(row: dict) -> Levels:
    return Levels(row["entry_lo"], row["entry_hi"], row["stop"], row["target1"], row["target2"] or row["target1"])


def _bars_after(frame, after: date) -> list[Bar]:
    if frame is None:
        return []
    sub = frame[frame.index > after]
    return [Bar(d, r["open"], r["high"], r["low"], r["close"]) for d, r in sub.iterrows()]


def _spy_return(spy, start: date, end: date) -> float | None:
    if spy is None or start not in spy.index or end not in spy.index:
        return None
    return float(spy.at[end, "close"] / spy.at[start, "open"] - 1)


async def update_all() -> list[Transition]:
    opps = await db.fetch_all(
        "SELECT * FROM scout.opportunities WHERE status = ANY(:s) ORDER BY asof", {"s": list(ACTIVE)}
    )
    if not opps:
        return []
    since = min(o["asof"] for o in opps)
    tickers = sorted({o["ticker"] for o in opps} | {BENCHMARK})
    frames = await bar_store.load(tickers, since=since)
    spy = frames.get(BENCHMARK)
    transitions = []
    for o in opps:
        res = simulate(_bars_after(frames.get(o["ticker"]), o["asof"]), levels_of(o), o["horizon_days"])
        new_status = _STATE_TO_STATUS[res.state]
        await _record_events(o["id"], res)
        if new_status == o["status"]:
            continue
        await _apply(o, res, new_status, spy)
        transitions.append(Transition(
            o["id"], o["ticker"], o["status"], new_status,
            res.exit_price if res.closed else res.entry_price, res.return_pct,
        ))
    return transitions


async def _record_events(opp_id: str, res: SimResult) -> None:
    for kind, d, price in res.events:
        await db.execute(
            "INSERT INTO scout.opportunity_events (opportunity_id, event_type, event_date, price) "
            "VALUES (:id, :k, :d, :p) ON CONFLICT (opportunity_id, event_type) DO NOTHING",
            {"id": opp_id, "k": kind, "d": d, "p": price},
        )


async def _apply(o: dict, res: SimResult, status: str, spy) -> None:
    closed = res.closed
    await db.execute(
        "UPDATE scout.opportunities SET status = :s, triggered_on = COALESCE(:tr, triggered_on), "
        "closed_on = :cl, updated_at = now() WHERE id = :id",
        {"s": status, "tr": res.entry_date, "cl": res.exit_date if closed else None, "id": o["id"]},
    )
    if not closed:
        return
    spy_ret = _spy_return(spy, res.entry_date, res.exit_date) if res.entry_date else None
    ret = res.return_pct
    await db.execute(
        """
        INSERT INTO scout.opportunity_outcomes (
            opportunity_id, outcome, entry_price, exit_price, return_pct, spy_return_pct, excess_pct,
            mfe_pct, mae_pct, days_held, closed_on)
        VALUES (:id, :o, :ep, :xp, :r, :sr, :ex, :mfe, :mae, :days, :cl)
        ON CONFLICT (opportunity_id) DO NOTHING
        """,
        {
            "id": o["id"], "o": status, "ep": res.entry_price, "xp": res.exit_price, "r": ret,
            "sr": spy_ret, "ex": (ret - spy_ret) if (ret is not None and spy_ret is not None) else None,
            "mfe": res.mfe, "mae": res.mae, "days": res.days_held, "cl": res.exit_date,
        },
    )


async def scoreboard() -> list[dict]:
    """Live performance by detector, next to the replay base rate."""
    return await db.fetch_all(
        """
        SELECT o.primary_detector AS detector,
               count(*) AS total,
               count(*) FILTER (WHERE o.status IN ('new', 'triggered')) AS active,
               count(oc.*) FILTER (WHERE oc.outcome <> 'expired_untriggered') AS closed,
               avg(CASE WHEN oc.return_pct > 0 THEN 1.0 ELSE 0.0 END)
                   FILTER (WHERE oc.outcome <> 'expired_untriggered') AS win_rate,
               avg(oc.return_pct) FILTER (WHERE oc.outcome <> 'expired_untriggered') AS avg_return,
               avg(oc.excess_pct) FILTER (WHERE oc.outcome <> 'expired_untriggered') AS avg_excess,
               (SELECT stats FROM scout.detector_base_rates b
                 WHERE b.detector = o.primary_detector AND b.regime = 'all') AS base_rate,
               (SELECT count(*) FROM scout.feedback f JOIN scout.opportunities o2 ON o2.id = f.opportunity_id
                 WHERE o2.primary_detector = o.primary_detector AND f.action IN ('up', 'star')) AS thumbs_up,
               (SELECT count(*) FROM scout.feedback f JOIN scout.opportunities o2 ON o2.id = f.opportunity_id
                 WHERE o2.primary_detector = o.primary_detector AND f.action IN ('down', 'dismiss')) AS thumbs_down
        FROM scout.opportunities o
        LEFT JOIN scout.opportunity_outcomes oc ON oc.opportunity_id = o.id
        GROUP BY o.primary_detector
        ORDER BY o.primary_detector
        """
    )
