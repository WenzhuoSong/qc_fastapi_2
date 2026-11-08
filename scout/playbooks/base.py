"""Playbook contract (catalogue: `docs/playbook_catalog_v1_zh.md`).

A playbook is a named way to trade (an investor's or a book's rules, or Scout's approximation of them where the
author left no fixed rule). Each one reads the same context and returns, per ticker it has a view on, an action:

  买入  its buy condition holds now (fresh: this week for event-type entries, or a top rank for screens)
  持有  it would keep holding (in its trend / still qualifies), but the entry is not fresh
  卖出  its sell condition holds now (for a screen: ranked in the bottom fifth, i.e. what it would avoid)

and a reason built in Python. Nothing here is a recommendation: every playbook carries its test status, and most
were rejected on unbiased data. Pure computation on `Ctx`; loading lives in `scout.playbooks.data`.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Callable

import numpy as np
import pandas as pd

BUY, HOLD, SELL = "买入", "持有", "卖出"
SCHOOLS = ("价值投资", "成长股", "技术与趋势", "大盘择时", "资产配置", "事件与投机", "出场规则", "问题清单")


@dataclass
class Verdict:
    action: str
    why: str
    score: float | None = None          # orders the buys within a playbook (higher first)


@dataclass(frozen=True)
class Playbook:
    key: str
    name: str
    school: str
    author: str
    idea: str
    buy_rule: str
    sell_rule: str
    evidence: str                       # outside evidence in one sentence
    fn: Callable[["Ctx"], dict[str, Verdict]] | None = None
    kb: str | None = None               # knowledge entry whose status applies (the rule itself)
    kin: str | None = None              # a close relative that was tested (status shown as 近亲)
    status: str | None = None           # a fixed status where no entry applies (e.g. the market filter)
    version: str = "按原作者的规则"
    missing: str | None = None          # why it cannot be computed with Scout's data (fn is None)


@dataclass
class Ctx:
    asof: date
    close: pd.DataFrame                 # date × ticker (stocks and ETFs), ~420 calendar days
    high: pd.DataFrame
    low: pd.DataFrame
    open: pd.DataFrame
    volume: pd.DataFrame
    f: dict[str, pd.DataFrame]          # the scan's feature frames (scout.features.build_panel)
    stocks: list[str]                   # liquid stocks of the scan universe today
    etfs: list[str]
    sectors: dict[str, str | None]
    sources: dict[str, list[str]] = field(default_factory=dict)       # ticker → index lists (sp500, sp400, ndx)
    names: dict[str, str] = field(default_factory=dict)
    prior_high: dict[str, float] = field(default_factory=dict)       # highest high before the loaded window (2014 on)
    fund: pd.DataFrame = field(default_factory=pd.DataFrame)         # ticker × fundamental fields, as filed by asof
    angle: dict[str, dict[str, float]] = field(default_factory=dict)  # ranker → {ticker: value} (scout.rankers.prepare)
    snaps: dict[str, dict] = field(default_factory=dict)             # ticker → latest ownership snapshot (research pool)
    macro: dict[str, pd.Series] = field(default_factory=dict)        # FRED series id → values by date
    holdings: list[dict] = field(default_factory=list)               # open trades: ticker, opened_on, entry_price
    watchlist: list[str] = field(default_factory=list)
    arrays: tuple | None = None                                      # (scout.rankers arrays, stock order) for the tested rankers
    long_close: pd.DataFrame = field(default_factory=pd.DataFrame)   # closes over ~3 years (residual momentum)
    cache: dict = field(default_factory=dict)

    def last(self, name: str) -> pd.Series:
        """Today's row of a feature frame (or a price frame)."""
        frame = self.f.get(name) if name in self.f else getattr(self, name)
        return frame.iloc[-1]

    def series(self, ticker: str, what: str = "close") -> pd.Series:
        frame = getattr(self, what)
        return frame[ticker].dropna() if ticker in frame.columns else pd.Series(dtype=float)


# ── helpers shared by the playbooks ──

def pct(x: float | None, d: int = 1) -> str:
    return "—" if x is None or not np.isfinite(x) else f"{x:+.{d}%}"


def num(x: float | None, d: int = 2) -> str:
    return "—" if x is None or not np.isfinite(x) else f"{x:.{d}f}"


def month_end_closes(s: pd.Series) -> pd.Series:
    """Last close of each completed month: the running month counts only when the series ends on its last session."""
    s = s.dropna()
    if s.empty:
        return s
    g = s.groupby(s.index.to_period("M")).last()
    if not _is_month_end(s.index[-1]):
        g = g[g.index < s.index[-1].to_period("M")]
    return g


def _is_month_end(d) -> bool:
    from scout.market_calendar import is_us_equity_trading_day

    day = pd.Timestamp(d).date()
    nxt = date.fromordinal(day.toordinal() + 1)
    while not is_us_equity_trading_day(nxt):
        nxt = date.fromordinal(nxt.toordinal() + 1)
    return nxt.month != day.month


def ranks(s: pd.Series, ascending: bool = False) -> pd.Series:
    """Percentile ranks in (0, 1]: 1 = best (highest when ascending is False)."""
    s = s.replace([np.inf, -np.inf], np.nan).dropna()
    return s.rank(pct=True, ascending=not ascending)


def crossed_up(a: pd.Series, b: pd.Series | float, within: int) -> bool:
    """Whether a went from ≤ b to > b in the last `within` sessions (a and b aligned)."""
    d = (a - b).dropna()
    if len(d) <= within:
        return False
    tail = d.iloc[-within - 1:]
    return bool((tail.iloc[-1] > 0) and (tail.iloc[:-1] <= 0).any())


def crossed_down(a: pd.Series, b: pd.Series | float, within: int) -> bool:
    d = (a - b).dropna()
    if len(d) <= within:
        return False
    tail = d.iloc[-within - 1:]
    return bool((tail.iloc[-1] < 0) and (tail.iloc[:-1] >= 0).any())


def weekly_closes(s: pd.Series) -> pd.Series:
    """Close of each completed week (Friday or the week's last session); the running week is kept as the last row."""
    s = s.dropna()
    return s.groupby(s.index.to_period("W-FRI")).last()


FRESH_DAYS = 5                          # an entry or exit event counts as this week's when it happened within 5 sessions
