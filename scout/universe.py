"""Universe: core watchlist, context ETFs and the index-based scan universe."""
from __future__ import annotations

import io
import logging
from dataclasses import dataclass

import httpx
import pandas as pd

from scout import db

logger = logging.getLogger("scout.universe")

# Initial watchlist (2026-10): themes that dominate current US market flow.
# Maintained at runtime via /watch; this seed only fills an empty table.
SEED_WATCHLIST: dict[str, str] = {
    # AI compute & semis (October semis drawdown is the headline theme)
    "NVDA": "AI 算力", "AVGO": "AI 算力", "AMD": "AI 算力", "TSM": "AI 算力",
    "MU": "AI 存储", "MRVL": "AI 网络", "ASML": "半导体设备", "ARM": "AI 算力",
    # AI infrastructure / neoclouds
    "ANET": "AI 网络", "VRT": "数据中心", "CRWV": "Neocloud", "NBIS": "Neocloud",
    # Hyperscalers
    "MSFT": "超大规模云", "GOOGL": "超大规模云", "AMZN": "超大规模云",
    "META": "超大规模云", "ORCL": "超大规模云",
    # AI software
    "PLTR": "AI 软件", "NOW": "AI 软件", "CRWD": "网络安全",
    # AI power
    "VST": "AI 电力", "CEG": "AI 电力", "GEV": "AI 电力", "OKLO": "核能",
    # Oil > $100
    "XOM": "能源", "CVX": "能源", "OXY": "能源",
    # Crypto-linked
    "COIN": "加密", "MSTR": "加密", "HOOD": "加密/券商",
    # Speculative tech
    "IONQ": "量子", "RGTI": "量子", "RKLB": "航天",
    # Mega caps
    "TSLA": "电动车/机器人", "AAPL": "消费科技", "LLY": "GLP-1",
}

SECTOR_ETFS: dict[str, str] = {
    "Information Technology": "XLK",
    "Financials": "XLF",
    "Health Care": "XLV",
    "Energy": "XLE",
    "Industrials": "XLI",
    "Consumer Staples": "XLP",
    "Consumer Discretionary": "XLY",
    "Utilities": "XLU",
    "Materials": "XLB",
    "Real Estate": "XLRE",
    "Communication Services": "XLC",
}

SECTOR_ZH: dict[str, str] = {
    "Information Technology": "信息技术", "Financials": "金融", "Health Care": "医疗保健", "Energy": "能源",
    "Industrials": "工业", "Consumer Staples": "必需消费", "Consumer Discretionary": "可选消费",
    "Utilities": "公用事业", "Materials": "原材料", "Real Estate": "房地产", "Communication Services": "通信服务",
}

THEME_ETFS: dict[str, str] = {
    "SPY": "S&P 500", "QQQ": "Nasdaq-100", "IWM": "Russell 2000", "RSP": "等权 S&P 500",
    "SMH": "半导体", "IGV": "软件", "XBI": "生物科技", "KRE": "地区银行",
    "ITA": "军工", "URA": "铀", "ARKK": "高成长", "IBIT": "比特币",
    "GLD": "黄金", "TLT": "长债", "USO": "原油", "UUP": "美元",
}

BENCHMARK = "SPY"

_WIKI_PAGES = {
    "sp500": "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
    "sp400": "https://en.wikipedia.org/wiki/List_of_S%26P_400_companies",
    "ndx": "https://en.wikipedia.org/wiki/List_of_NASDAQ-100_companies",
}
_SYMBOL_COLUMNS = ("Symbol", "Ticker", "Ticker symbol")
_USER_AGENT = "scout-market-observer/1.0 (personal research)"


@dataclass
class Instrument:
    ticker: str
    name: str | None
    sector: str | None
    industry: str | None
    kind: str
    sources: list[str]


def yahoo_symbol(symbol: str) -> str:
    return symbol.strip().upper().replace(".", "-")


def context_etfs() -> dict[str, str]:
    out = dict(THEME_ETFS)
    out.update({etf: sector for sector, etf in SECTOR_ETFS.items()})
    return out


def parse_constituents(html: str, source: str) -> list[Instrument]:
    tables = pd.read_html(io.StringIO(html))
    for table in tables:
        symbol_col = next((c for c in _SYMBOL_COLUMNS if c in table.columns), None)
        if symbol_col is None or len(table) < 50:
            continue
        # Nasdaq-100 lists ICB industries; sectors then come from the S&P lists.
        has_gics = "GICS Sector" in table.columns
        name_col = next((c for c in ("Security", "Company") if c in table.columns), None)
        out = []
        for _, row in table.iterrows():
            symbol = str(row[symbol_col]).strip()
            if not symbol or symbol.lower() == "nan":
                continue
            out.append(
                Instrument(
                    ticker=yahoo_symbol(symbol),
                    name=str(row[name_col]) if name_col else None,
                    sector=str(row["GICS Sector"]) if has_gics else None,
                    industry=str(row.get("GICS Sub-Industry")) if "GICS Sub-Industry" in table.columns else None,
                    kind="stock",
                    sources=[source],
                )
            )
        if len(out) >= 50:
            return out
    raise ValueError(f"no constituent table found for {source}")


async def fetch_index_constituents() -> list[Instrument]:
    merged: dict[str, Instrument] = {}
    async with httpx.AsyncClient(timeout=30, headers={"User-Agent": _USER_AGENT}, follow_redirects=True) as client:
        for source, url in _WIKI_PAGES.items():
            try:
                resp = await client.get(url)
                resp.raise_for_status()
                rows = parse_constituents(resp.text, source)
            except Exception as exc:  # one index failing must not empty the universe
                logger.warning("constituents fetch failed for %s: %s", source, exc)
                continue
            for inst in rows:
                if inst.ticker in merged:
                    merged[inst.ticker].sources.append(source)
                else:
                    merged[inst.ticker] = inst
    return list(merged.values())


async def refresh_instruments() -> dict:
    """Refresh scout.instruments from index pages; never shrinks on fetch failure."""
    stocks = await fetch_index_constituents()
    rows = [
        {"t": i.ticker, "n": i.name, "s": i.sector, "i": i.industry, "k": "stock", "src": db.jsonb(i.sources)}
        for i in stocks
    ]
    rows += [
        {"t": t, "n": label, "s": None, "i": None, "k": "etf", "src": db.jsonb(["context"])}
        for t, label in context_etfs().items()
    ]
    await db.execute_many(
        """
        INSERT INTO scout.instruments (ticker, name, sector, industry, kind, sources, updated_at)
        VALUES (:t, :n, :s, :i, :k, CAST(:src AS JSONB), now())
        ON CONFLICT (ticker) DO UPDATE SET
            name = COALESCE(EXCLUDED.name, scout.instruments.name),
            sector = COALESCE(EXCLUDED.sector, scout.instruments.sector),
            industry = COALESCE(EXCLUDED.industry, scout.instruments.industry),
            kind = EXCLUDED.kind, sources = EXCLUDED.sources, in_scan = true, updated_at = now()
        """,
        rows,
    )
    if len(stocks) >= 400:
        # Index members that dropped out stay in history but leave the scan.
        await db.execute(
            "UPDATE scout.instruments SET in_scan = false "
            "WHERE kind = 'stock' AND updated_at < now() - interval '1 hour' "
            "AND ticker NOT IN (SELECT ticker FROM scout.watchlist WHERE active)"
        )
    return {"stocks": len(stocks), "etfs": len(context_etfs())}


async def seed_watchlist() -> int:
    existing = await db.fetch_one("SELECT count(*) AS n FROM scout.watchlist")
    if existing and existing["n"]:
        return 0
    await db.execute_many(
        "INSERT INTO scout.watchlist (ticker, theme) VALUES (:t, :theme) ON CONFLICT DO NOTHING",
        [{"t": t, "theme": theme} for t, theme in SEED_WATCHLIST.items()],
    )
    await ensure_watchlist_instruments()
    return len(SEED_WATCHLIST)


async def ensure_watchlist_instruments() -> None:
    """Watchlist names outside the indices (e.g. OKLO, IONQ) still get scanned."""
    await db.execute(
        """
        INSERT INTO scout.instruments (ticker, kind, sources)
        SELECT w.ticker, 'stock', '["watchlist"]'::jsonb FROM scout.watchlist w
        WHERE w.active
        ON CONFLICT (ticker) DO UPDATE SET in_scan = true
        """
    )


async def load_watchlist() -> list[dict]:
    return await db.fetch_all(
        "SELECT ticker, theme, note, added_at FROM scout.watchlist WHERE active ORDER BY theme, ticker"
    )


async def add_watch(ticker: str, theme: str | None = None) -> None:
    await db.execute(
        "INSERT INTO scout.watchlist (ticker, theme, active) VALUES (:t, :theme, true) "
        "ON CONFLICT (ticker) DO UPDATE SET active = true, theme = COALESCE(EXCLUDED.theme, scout.watchlist.theme)",
        {"t": yahoo_symbol(ticker), "theme": theme},
    )
    await ensure_watchlist_instruments()


async def remove_watch(ticker: str) -> None:
    await db.execute("UPDATE scout.watchlist SET active = false WHERE ticker = :t", {"t": yahoo_symbol(ticker)})


async def load_scan_instruments() -> list[dict]:
    return await db.fetch_all(
        "SELECT ticker, name, sector, industry, kind FROM scout.instruments WHERE in_scan ORDER BY ticker"
    )


# ── Research pool (v2): the ~150 names that get deep data (filings, estimates,
# options, documents, memos). The full scan universe only gets light data. ──
RESEARCH_TOP_LIQUID = 100
OPTIONS_MARKET_ETFS = ("SPY", "QQQ", "IWM")


async def research_pool() -> list[str]:
    """Holdings ∪ open opportunities ∪ watchlist ∪ the most traded stocks (stocks only)."""
    rows = await db.fetch_all(
        """
        SELECT ticker FROM scout.trades WHERE closed_on IS NULL
        UNION SELECT ticker FROM scout.opportunities WHERE status IN ('new', 'triggered')
        UNION SELECT ticker FROM scout.watchlist WHERE active
        UNION SELECT ticker FROM (
            SELECT f.ticker FROM scout.features_daily f JOIN scout.instruments i USING (ticker)
            WHERE f.date = (SELECT max(date) FROM scout.features_daily) AND i.kind = 'stock'
            ORDER BY (f.payload->>'dollar_vol20')::float DESC NULLS LAST LIMIT :n
        ) top
        """, {"n": RESEARCH_TOP_LIQUID})
    etfs = await db.fetch_all("SELECT ticker FROM scout.instruments WHERE kind = 'etf'")
    etf_set = {r["ticker"] for r in etfs}
    return sorted({r["ticker"] for r in rows} - etf_set)
