"""External facts: what OpenAI's web search finds that Scout's own data does not hold, kept as checked, dated facts
(user's decision 2026-11-04: use the search for what Scout cannot get, always with its source, and keep it for later).

Every search that feeds a Scout text goes through one path:
1. `search` — OpenAI's hosted web search (the search and the page reading happen on OpenAI's side; Scout only gets
   the answer and the cited URLs), asked to quote the exact source sentence for each fact;
2. `extract` — a structured call turns the answer into facts: subject, type, a one-sentence summary, number and unit,
   the verbatim quote, URL, outlet, date;
3. `verify` — Scout opens each URL itself and looks for the quote (letters and digits compared; `close` when ≥ 90 %
   of its words and all its numbers are on the page): verified / unreachable (paywall, blocked) / mismatch;
4. `store` — one row per fact in `scout.external_facts`, append-only (seen again → last_seen moves).

Packs for the LLM carry verified facts in full; unreachable ones without their numbers; mismatches never. So a number
an LLM writes from outside always stands on a page Scout opened itself.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
from datetime import date, timedelta

import httpx

from scout import db
from scout.config import get_scout_settings

logger = logging.getLogger("scout.external_facts")

SEARCH_MODEL = "gpt-4o-mini"
FACT_TYPES = ("分析师评级", "目标价", "业绩与指引", "合同与订单", "产品与技术", "监管与诉讼", "并购与资本", "管理层",
              "政策", "宏观", "行业", "市场观点", "其他")
STATUS_ZH = {"verified": "已打开来源核对原文", "unreachable": "来源打不开（付费墙或拦截），未核对", "mismatch": "来源里找不到这句原文"}
QUOTE_RULE = ("\n\nFor every fact you report, quote the exact sentence from the source page in double quotes, "
              "and give its URL and publication date. Do not paraphrase inside the quotes.")
MAX_PAGE = 3_000_000
FETCH_CONCURRENCY = 6
MIN_QUOTE = 20                       # normalised characters; shorter quotes prove nothing

EXTRACT_SCHEMA = {
    "type": "object", "additionalProperties": False, "required": ["facts"],
    "properties": {"facts": {"type": "array", "items": {
        "type": "object", "additionalProperties": False,
        "required": ["subject", "fact_type", "claim_zh", "value", "unit", "quote", "url", "outlet", "published"],
        "properties": {
            "subject": {"type": "string", "description": "股票代码（大写）、板块 ETF 代码，或 MACRO（宏观、政策、市场整体）"},
            "fact_type": {"type": "string", "enum": list(FACT_TYPES)},
            "claim_zh": {"type": "string", "description": "一句中文概括，只写原文说了的"},
            "value": {"type": ["number", "null"], "description": "这条事实里最关键的一个数字（目标价、金额、百分比等），没有就 null"},
            "unit": {"type": ["string", "null"], "description": "数字的单位，如 美元、%、亿美元、bp"},
            "quote": {"type": "string", "description": "检索结果里给出的原文句子，一字不改；没有原文引号的事实不要收录"},
            "url": {"type": "string"},
            "outlet": {"type": "string", "description": "媒体或机构名称"},
            "published": {"type": "string", "description": "YYYY-MM-DD；不知道就空字符串"},
        }}}},
}


async def search(prompt: str, purpose: str, model: str | None = None) -> dict:
    """OpenAI's hosted web search; {"text", "sources": [{"title","url"}]} or {}."""
    from scout import responses

    if not get_scout_settings().openai_api_key:
        return {}
    model = model or SEARCH_MODEL
    try:
        body = {"model": model, "tools": [{"type": "web_search_preview" if model == SEARCH_MODEL else "web_search"}], "input": prompt}
        if model != SEARCH_MODEL:
            body["reasoning"] = {"effort": "low"}
        data = await responses.create(body, timeout=240, purpose=purpose)
    except Exception as exc:
        logger.warning("web search (%s) failed: %s %s", purpose, type(exc).__name__, exc)
        return {}
    sources, seen = [], set()
    for item in data.get("output") or []:
        for part in item.get("content") or [] if item.get("type") == "message" else []:
            for ann in part.get("annotations") or []:
                url = clean_url(ann.get("url") or "")
                if ann.get("type") == "url_citation" and url and url not in seen:
                    seen.add(url)
                    sources.append({"title": ann.get("title") or url, "url": url})
    text = responses.output_text(data).strip()
    return {"text": text, "sources": sources} if text else {}


def clean_url(url: str) -> str:
    return re.sub(r"[?&]utm_[^&]+", "", url).rstrip("?&")


async def extract(found: dict, subjects: list[str], today: date, purpose: str) -> list[dict]:
    """The search answer as facts (only those with a quote and a URL the search cited or wrote out)."""
    from scout import responses

    if not found:
        return []
    data = await responses.create({
        "model": get_scout_settings().scout_parse_model,
        "instructions": (f"今天是 {today}。把检索结果整理成事实列表。只收录带原文引号和来源网址的事实；quote 原样照抄检索结果里的引号内容；"
                         f"对象优先用这些代码：{', '.join(subjects) or 'MACRO'}；与它们无关的宏观、政策、市场整体写 MACRO。"),
        "input": [{"role": "user", "content": found["text"] + "\n\n来源：\n" + "\n".join(s["url"] for s in found["sources"])}],
        "text": {"format": {"type": "json_schema", "name": "facts", "schema": EXTRACT_SCHEMA, "strict": True}}},
        timeout=120, purpose=f"{purpose}_extract")
    cited = {s["url"] for s in found["sources"]}
    out = []
    for f in json.loads(responses.output_text(data) or "{}").get("facts", []):
        url = clean_url(f.get("url") or "")
        if not url.startswith("http") or (url not in cited and url not in found["text"]):
            continue
        if len(norm(f.get("quote") or "")) < MIN_QUOTE:
            continue
        try:
            pub = date.fromisoformat(f["published"]) if f.get("published") else None
        except ValueError:
            pub = None
        if pub and pub > today:
            pub = None
        out.append({"subject": (f.get("subject") or "MACRO").upper()[:32], "fact_type": f["fact_type"], "claim": f["claim_zh"].strip(),
                    "value": f.get("value"), "unit": (f.get("unit") or None), "quote": f["quote"].strip()[:400], "url": url,
                    "outlet": (f.get("outlet") or "")[:120], "published": pub})
    return out


# ── verification ──

def norm(s: str) -> str:
    """Letters, digits and CJK only, lower case: the comparison survives markup, dashes, quotes and spacing."""
    s = s.replace("’", "'").replace("“", '"').replace("”", '"')
    return re.sub(r"[^0-9a-z一-鿿]+", "", s.lower())


def check(quote: str, page: str) -> str | None:
    """exact = the quote is on the page; close = ≥ 90 % of its words and every number are; None otherwise."""
    q, hay = norm(quote), norm(page)
    if len(q) < MIN_QUOTE:
        return None
    if q in hay:
        return "exact"
    words = [w for w in re.findall(r"[a-z]{4,}|[一-鿿]{2,}", quote.lower())]
    nums = re.findall(r"\d+(?:\.\d+)?", quote.replace(",", ""))
    if len(words) < 5:
        return None
    page_l, page_n = page.lower(), page.replace(",", "")
    covered = sum(w in page_l for w in words) / len(words)
    return "close" if covered >= 0.9 and all(n in page_n for n in nums) else None


async def fetch_text(client: httpx.AsyncClient, url: str) -> str | None:
    """The page as plain text, or None when it cannot be read (status, paywall stub, not HTML / text)."""
    from scout.company_profile import html_text

    try:
        resp = await client.get(url)
    except Exception:
        return None
    if resp.status_code != 200 or len(resp.content) > MAX_PAGE:
        return None
    ctype = resp.headers.get("content-type", "")
    if "html" not in ctype and "text" not in ctype:
        return None
    text = html_text(resp.text)
    return text if len(text) >= 400 else None          # a stub: consent wall, script-only page


async def verify(facts: list[dict]) -> list[dict]:
    """Open each source once and look for each quote; sets status and match."""
    pages: dict[str, str | None] = {}
    sem = asyncio.Semaphore(FETCH_CONCURRENCY)
    async with httpx.AsyncClient(timeout=25, follow_redirects=True, headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9"}) as client:
        async def one(url: str) -> None:
            async with sem:
                pages[url] = await fetch_text(client, url)
        await asyncio.gather(*(one(u) for u in {f["url"] for f in facts}))
    for f in facts:
        page = pages.get(f["url"])
        if page is None:
            f["status"], f["match"] = "unreachable", None
        else:
            m = check(f["quote"], page)
            f["status"], f["match"] = ("verified", m) if m else ("mismatch", None)
    return facts


# ── storage ──

def fingerprint(f: dict) -> str:
    return hashlib.sha1(f"{f['subject']}|{f['url']}|{norm(f['quote'])}".encode()).hexdigest()


def subject_kind(subject: str) -> str:
    from scout.universe import SECTOR_ETFS

    return "macro" if subject == "MACRO" else "sector" if subject in SECTOR_ETFS.values() else "stock"


async def store(facts: list[dict], purpose: str) -> list[dict]:
    """Insert new facts, touch the ones seen before; returns the stored rows (with ids)."""
    out = []
    for f in facts:
        row = await db.execute_returning(           # fetch_one would roll the insert back
            "INSERT INTO scout.external_facts (fingerprint, subject, subject_kind, fact_type, claim, value, unit, quote, url, outlet, "
            "published, status, match, purpose) VALUES (:fp, :subject, :kind, :fact_type, :claim, :value, :unit, :quote, :url, :outlet, "
            ":published, :status, :match, :purpose) "
            "ON CONFLICT (fingerprint) DO UPDATE SET last_seen = now(), "
            "status = CASE WHEN scout.external_facts.status = 'verified' THEN 'verified' ELSE EXCLUDED.status END, "
            "match = coalesce(scout.external_facts.match, EXCLUDED.match) RETURNING *",
            {**f, "fp": fingerprint(f), "kind": subject_kind(f["subject"]), "purpose": purpose[:48]})
        if row:
            out.append(dict(row))
    return out


async def collect(prompt: str, subjects: list[str], purpose: str, model: str | None = None, today: date | None = None) -> dict:
    """Search → extract → verify → store. Returns {"facts": stored rows, "sources", "text"}."""
    today = today or date.today()
    found = await search(prompt + QUOTE_RULE, purpose, model)
    if not found:
        return {"facts": [], "sources": [], "text": ""}
    try:
        facts = await verify(await extract(found, subjects, today, purpose))
        rows = await store(facts, purpose)
    except Exception as exc:
        logger.warning("fact extraction (%s) failed: %s", purpose, exc)
        rows = []
    return {"facts": rows, "sources": found["sources"], "text": found["text"]}


async def recall(subjects: list[str], days: int = 30, today: date | None = None, limit: int = 60) -> list[dict]:
    """Usable facts about the subjects seen in the last `days` (verified first, then unreachable; never mismatches)."""
    today = today or date.today()
    return [dict(r) for r in await db.fetch_all(
        "SELECT * FROM scout.external_facts WHERE subject = ANY(:s) AND status IN ('verified', 'unreachable') "
        "AND coalesce(published, first_seen::date) >= :d AND first_seen::date <= :t "
        "ORDER BY (status = 'verified') DESC, coalesce(published, first_seen::date) DESC LIMIT :n",
        {"s": [s.upper() for s in subjects], "d": today - timedelta(days=days), "t": today, "n": limit})]


_DIGITS = re.compile(r"\d+(?:[.,]\d+)*")


def pack_rows(rows: list[dict]) -> dict:
    """Facts for an LLM pack. Verified facts in full; unreachable ones with their numbers masked, so no number from a
    page Scout could not open can pass the number guard."""
    ok = [{"id": r["id"], "对象": r["subject"], "类型": r["fact_type"], "内容": r["claim"],
           **({"数值": f"{r['value']:g}{r['unit'] or ''}"} if r.get("value") is not None else {}),
           "原文": r["quote"], "来源": r["outlet"] or r["url"], "日期": str(r["published"] or "")}
          for r in rows if r["status"] == "verified"]
    weak = [{"id": r["id"], "对象": r["subject"], "类型": r["fact_type"], "内容": _DIGITS.sub("…", r["claim"]),
             "来源": r["outlet"] or r["url"], "日期": str(r["published"] or "")}
            for r in rows if r["status"] == "unreachable"]
    return {"已核对": ok, "未核对（来源打不开，数字已隐去，不可引用数字）": weak,
            "说明": "外部报道。已核对 = Scout 自己打开来源找到了原文；未核对的只能作为'有报道称'，不能当作事实"}


def label(r: dict) -> str:
    return f"{r['outlet'] or '来源'} {r['published'] or ''}".strip() + ("" if r["status"] == "verified" else "，未核对")
