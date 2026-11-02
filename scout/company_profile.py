"""Company profiles and supply-chain relations from the annual report, point in time.

Source: the latest 10-K (Item 1 Business) or 20-F (Item 4 Information on the Company) on EDGAR, plus the
customer-concentration sentences elsewhere in the filing (ASC 280 requires naming customers above 10 % of
revenue). A small model turns the text into a structured profile — segments, end markets, customers,
suppliers, competitors, partners, advantages, business risks — and every item must carry a quote that is
found verbatim in the filing (numbers in an item must appear in its quote); anything else is dropped, so the
profile says only what the company itself wrote. Named companies are matched to Scout tickers. Each filing
adds a version (`scout.company_profiles`, `scout.company_relations`); nothing is overwritten.
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
from datetime import date

from scout import db, universe
from scout.config import get_scout_settings
from scout.data import sec

logger = logging.getLogger("scout.company_profile")

ANNUAL_FORMS = ("10-K", "20-F")
MAX_SECTION = 150_000          # characters of the business section sent to the model
MIN_SECTION = 2_000            # shorter than this is a table-of-contents hit, not the section
FALLBACK_SKIP = 3_000          # cover page skipped when a report has no item headings (INTC, GE, MCD, ASML style)
MAX_SNIPPETS = 20
CONCURRENCY = 6
RELATIONS = {"customers": "customer", "suppliers": "supplier", "competitors": "competitor", "partners": "partner"}

SECTIONS = {
    # no \b after the start heading: the extracted text joins a heading to the next word ("BusinessOur Company")
    # "Item 1. Business", or energy companies' "Items 1 and 2. Business and Properties"
    "10-K": (r"items?\s*1\s*\.?\s*(?:and\s*2\s*\.?\s*)?[:\-–—]?\s*business", r"item\s*1a\s*[.:\-–—]?\s*risk\s+factors|item\s*2\s*[.:\-–—]?\s*properties"),
    "20-F": (r"item\s*4\s*[.:\-–—]?\s*information\s+on\s+the\s+company",
             r"item\s*4a\s*[.:\-–—]?\s*unresolved|item\s*5\s*[.:\-–—]?\s*operating\s+and\s+financial"),
}
SENTENCE = re.compile(r"(?<=[.;])\s+(?=[A-Z(])")
SHARE = re.compile(r"\d{1,3}(\.\d+)?\s?%")
WHO = re.compile(r"\b(customers?|distributors?|accounted for|represented)\b", re.I)


# ── text ──

def html_text(raw: str) -> str:
    """Plain text from a filing document (HTML or text), whitespace collapsed."""
    if "<" in raw[:2000]:
        import lxml.html

        try:
            tree = lxml.html.fromstring(raw.encode("utf-8", "ignore"))
            for bad in tree.xpath("//script|//style|//ix:header", namespaces={"ix": "http://www.xbrl.org/2013/inlineXBRL"}):
                bad.drop_tree()
            raw = tree.text_content()
        except Exception:
            raw = re.sub(r"<[^>]+>", " ", raw)
    raw = raw.replace("\xa0", " ").replace("’", "'").replace("“", '"').replace("”", '"')
    return re.sub(r"\s+", " ", raw).strip()


def section(text: str, form: str) -> str | None:
    """The business section: the longest span from a start heading to the first end heading after it (the
    table of contents and cross-references give short spans, so the longest is the section itself)."""
    start_re, end_re = SECTIONS[form]
    starts = [m.start() for m in re.finditer(start_re, text, re.I)]
    ends = [m.start() for m in re.finditer(end_re, text, re.I)]
    best = None
    for s in starts:
        e = next((x for x in ends if x > s), None)
        if e and (best is None or e - s > best[1] - best[0]):
            best = (s, e)
    if not best or best[1] - best[0] < MIN_SECTION:
        return None
    return text[best[0]:best[1]]


def snippets(text: str) -> list[str]:
    """Sentences naming a customer share of revenue, anywhere in the filing (segment notes, risk factors)."""
    out: list[str] = []
    for s in SENTENCE.split(text):
        s = s.strip()
        if 40 <= len(s) <= 600 and SHARE.search(s) and WHO.search(s) and re.search(r"revenue|sales", s, re.I) and s not in out:
            out.append(s)
        if len(out) >= MAX_SNIPPETS:
            break
    return out


# ── the model's profile and its validation ──

ITEM = {"type": "object", "additionalProperties": False, "required": ["name", "detail", "quote"],
        "properties": {"name": {"type": "string"}, "detail": {"type": ["string", "null"]},
                       "quote": {"type": "string", "description": "原文中一字不差的英文句子或片段（≤ 300 字符）"}}}
POINT = {"type": "object", "additionalProperties": False, "required": ["point", "quote"],
         "properties": {"point": {"type": "string"}, "quote": {"type": "string"}}}
SCHEMA = {
    "type": "object", "additionalProperties": False,
    "required": ["summary", "segments", "end_markets", "customers", "suppliers", "competitors", "partners",
                 "advantages", "business_risks", "not_disclosed"],
    "properties": {
        "summary": {"type": "string", "description": "2–3 句中文：公司做什么、靠什么赚钱、主要卖给谁"},
        "segments": {"type": "array", "items": ITEM, "description": "业务分部或主要产品线；detail 写中文说明和披露的收入占比"},
        "end_markets": {"type": "array", "items": ITEM, "description": "终端市场 / 应用领域；detail 写中文说明"},
        "customers": {"type": "array", "items": ITEM, "description": "原文点名的客户；detail 写披露的收入占比或关系"},
        "suppliers": {"type": "array", "items": ITEM, "description": "原文点名的供应商、代工厂或关键原材料来源；detail 写供应什么"},
        "competitors": {"type": "array", "items": ITEM, "description": "原文点名的竞争对手"},
        "partners": {"type": "array", "items": ITEM, "description": "原文点名的合作方 / 渠道 / 联盟"},
        "advantages": {"type": "array", "items": POINT, "description": "公司自己写的竞争优势，中文概括"},
        "business_risks": {"type": "array", "items": POINT, "description": "业务层面的主要风险（集中度、依赖单一供应商、周期等），中文概括"},
        "not_disclosed": {"type": "array", "items": {"type": "string"}, "description": "原文明确没有披露的重要信息，如'未点名主要客户'"},
    },
}
PROMPT = """你是卖方研究员，正在根据公司年报的业务部分整理公司画像。只用原文里写了的内容，不要补充你自己知道的信息。
每一条都要附 quote：从原文中一字不差地摘出的英文句子或片段（不超过 300 字符），用来证明这一条。原文没写的就不要写，
可以在 not_disclosed 里说明（例如"未点名主要客户""未披露各分部收入占比"）。
客户、供应商、竞争对手、合作方只写原文点名的公司（name 用原文里的名称）；收入占比等数字必须在 quote 里出现。
文末的 CONCENTRATION 是从年报其他部分找到的客户集中度句子，也可以引用。用简体中文概括（即使原文是繁体），name 和 quote 保留原文。"""


def _norm(s: str) -> str:
    """Letters and digits only, lower case: quotes survive symbols, dashes and spacing the text extraction changed."""
    return re.sub(r"[^0-9a-z]+", "", s.lower())


NUM = re.compile(r"\d+(?:\.\d+)?")
NEAR = 600                      # characters around a quote where the numbers of its item may stand


def _near(quote: str, source: str) -> str:
    """The source text around the quote's position (numbers in a table row or the next sentence count)."""
    words = [w for w in re.findall(r"[A-Za-z]{4,}", quote)][:6]
    if not words:
        return quote
    m = re.search(r"\W+".join(map(re.escape, words[:3])), source, re.I)
    return source[max(0, m.start() - NEAR):m.end() + NEAR] if m else quote


def validate(profile: dict, source: str) -> tuple[dict, dict]:
    """Keep the items whose quote is in the source and whose numbers stand in or right around their quote."""
    hay = _norm(source)
    kept: dict = {"summary": profile.get("summary"), "not_disclosed": profile.get("not_disclosed") or []}
    dropped: list[dict] = []
    for key in ("segments", "end_markets", "customers", "suppliers", "competitors", "partners", "advantages", "business_risks"):
        good = []
        for item in profile.get(key) or []:
            q = _norm(item.get("quote") or "")
            reason = None
            if len(q) < 12:
                reason = "quote too short"
            elif q not in hay:
                reason = "quote not in filing"
            else:
                around = _near(item.get("quote") or "", source).replace(",", "")
                nums = {n for n in NUM.findall((item.get("detail") or "").replace(",", "")) if n not in around}
                if nums:
                    reason = f"numbers not in quote: {sorted(nums)}"
            if reason:
                dropped.append({"field": key, "name": item.get("name") or item.get("point"), "reason": reason})
            else:
                good.append(item)
        kept[key] = good
    flat = source.replace(",", "")
    loose = sorted({n for n in NUM.findall((kept.get("summary") or "").replace(",", "")) if n not in flat})
    if loose:                                   # the summary must not carry a number the filing does not
        dropped.append({"field": "summary", "name": "summary", "reason": f"numbers not in filing: {loose}"})
        kept["summary"] = None
    n = sum(len(kept[k]) for k in kept if isinstance(kept[k], list) and k != "not_disclosed")
    return kept, {"kept": n, "dropped": len(dropped), "dropped_items": dropped[:30]}


# ── matching names to tickers ──

SUFFIXES = r"\b(incorporated|inc|corporation|corp|company|co|ltd|limited|plc|holdings?|group|llc|lp|sa|ag|nv|se|the|class [a-c])\b"
ALIASES = {"google": "GOOGL", "alphabet": "GOOGL", "facebook": "META", "meta": "META", "amazon": "AMZN", "aws": "AMZN",
           "amazon web services": "AMZN", "microsoft": "MSFT", "apple": "AAPL", "nvidia": "NVDA", "intel": "INTC",
           "amd": "AMD", "advanced micro devices": "AMD", "tsmc": "TSM", "taiwan semiconductor manufacturing": "TSM",
           "taiwan semiconductor": "TSM", "walmart": "WMT", "wal-mart": "WMT", "costco": "COST", "target": "TGT",
           "home depot": "HD", "the home depot": "HD", "lowe's": "LOW", "mckesson": "MCK", "cardinal health": "CAH",
           "cencora": "COR", "amerisourcebergen": "COR", "ibm": "IBM", "oracle": "ORCL", "cisco": "CSCO", "dell": "DELL",
           "hp": "HPQ", "hewlett packard enterprise": "HPE", "boeing": "BA", "lockheed martin": "LMT", "general motors": "GM",
           "ford": "F", "tesla": "TSLA", "qualcomm": "QCOM", "broadcom": "AVGO", "micron": "MU", "texas instruments": "TXN",
           "asml": "ASML", "google cloud": "GOOGL", "microsoft azure": "MSFT", "azure": "MSFT", "oracle cloud": "ORCL", "applied materials": "AMAT", "lam research": "LRCX", "kla": "KLAC", "arm": "ARM",
           "u.s. government": None, "united states government": None, "department of defense": None}


def name_key(name: str) -> str:
    s = re.sub(r"[.,()&]", " ", name.lower().replace("’", "'"))
    s = re.sub(SUFFIXES, " ", s)
    return re.sub(r"\s+", " ", s).strip()


def name_index(instruments: list[dict]) -> dict[str, str]:
    idx: dict[str, str] = {}
    for r in instruments:
        if r.get("name") and r.get("kind") == "stock":
            idx.setdefault(name_key(r["name"]), r["ticker"])
    return idx


def match(name: str, idx: dict[str, str]) -> str | None:
    k = name_key(name)
    if k in ALIASES:
        return ALIASES[k]
    if k in idx:
        return idx[k]
    hits = {t for n, t in idx.items() if len(k) >= 5 and (n.startswith(k + " ") or k.startswith(n + " ")) and len(n) >= 5}
    return hits.pop() if len(hits) == 1 else None


def relations(ticker: str, profile: dict, idx: dict[str, str]) -> list[dict]:
    out = []
    for key, rel in RELATIONS.items():
        for item in profile.get(key) or []:
            rt = match(item["name"], idx)
            if rt == ticker:
                continue
            out.append({"relation": rel, "name": item["name"], "related_ticker": rt, "detail": item.get("detail"), "quote": item["quote"]})
    return out


# ── fetch, build, store ──

async def latest_annual(ticker: str, cik: str) -> dict | None:
    sub = await sec.submissions(cik)
    rows = [f for f in sec.filings(sub, ticker, cik, set(ANNUAL_FORMS)) if f.get("url")]
    return max(rows, key=lambda f: f["filed"]) if rows else None


async def fetch_doc(f: dict) -> dict:
    raw = await sec.document(f["url"])
    if raw is None:
        return {**f, "section": None, "snippets": [], "status": "fetch_failed"}
    text = html_text(raw)
    sec_text = section(text, f["form"])
    status = "ok"
    if not sec_text and len(text) > FALLBACK_SKIP + MIN_SECTION:
        # reports in their own format (headings only in a cross-reference index at the end): the opening part
        # describes the business; every item still has to quote the filing
        sec_text, status = text[FALLBACK_SKIP:FALLBACK_SKIP + MAX_SECTION], "fallback"
    return {**f, "section": sec_text, "snippets": snippets(text), "status": status if sec_text else "no_section"}


async def extract(doc: dict) -> tuple[dict, dict, str]:
    from scout import responses

    settings = get_scout_settings()
    body = doc["section"][:MAX_SECTION] + ("\n\nCONCENTRATION:\n" + "\n".join(doc["snippets"]) if doc["snippets"] else "")
    req = {"model": settings.scout_parse_model, "instructions": PROMPT, "input": [{"role": "user", "content": body}],
           "text": {"format": {"type": "json_schema", "name": "company_profile", "schema": SCHEMA, "strict": True}}}
    data = await responses.create(req, timeout=240, purpose="company_profile")
    raw = json.loads(responses.output_text(data))
    kept, report = validate(raw, body)
    if report["dropped"]:                 # one correction pass: re-quote exactly, or leave the item out
        fix = "以下条目没有通过校验（quote 必须是原文中一字不差的片段，detail 里的数字必须出现在原文对应位置附近）。" \
              "请重新输出完整的画像：能找到原文依据的改正 quote，找不到的删去：\n" + \
              "\n".join(f"- {d['field']} / {d['name']}：{d['reason']}" for d in report["dropped_items"])
        data2 = await responses.create({**req, "previous_response_id": data["id"], "input": [{"role": "user", "content": fix}]},
                                       timeout=240, purpose="company_profile")
        kept2, report2 = validate(json.loads(responses.output_text(data2)), body)
        if report2["kept"] >= report["kept"]:
            kept, report = kept2, {**report2, "first_pass": {"kept": report["kept"], "dropped": report["dropped"]}}
    return kept, report, settings.scout_parse_model


async def store_doc(doc: dict) -> None:
    await db.execute(
        "INSERT INTO scout.company_docs (accession, ticker, cik, form, filed, report_date, url, section, snippets, chars, status) "
        "VALUES (:a, :t, :c, :f, :d, :r, :u, :s, CAST(:sn AS JSONB), :n, :st) ON CONFLICT (accession) DO UPDATE SET "
        "section = EXCLUDED.section, snippets = EXCLUDED.snippets, chars = EXCLUDED.chars, status = EXCLUDED.status, fetched_at = now()",
        {"a": doc["accession"], "t": doc["ticker"], "c": doc["cik"], "f": doc["form"], "d": doc["filed"], "r": doc.get("report_date"),
         "u": doc["url"], "s": doc.get("section"), "sn": db.jsonb(doc.get("snippets") or []),
         "n": len(doc.get("section") or ""), "st": doc["status"]})


async def store_profile(doc: dict, profile: dict, report: dict, model: str, idx: dict[str, str]) -> int:
    rels = relations(doc["ticker"], profile, idx)
    await db.execute(
        "INSERT INTO scout.company_profiles (ticker, accession, filed, profile, validation, model) "
        "VALUES (:t, :a, :d, CAST(:p AS JSONB), CAST(:v AS JSONB), :m) ON CONFLICT (accession) DO UPDATE SET "      # same filing re-read
        "profile = EXCLUDED.profile, validation = EXCLUDED.validation, model = EXCLUDED.model, created_at = now()",
        {"t": doc["ticker"], "a": doc["accession"], "d": doc["filed"], "p": db.jsonb(profile), "v": db.jsonb(report), "m": model})
    await db.execute("DELETE FROM scout.company_relations WHERE accession = :a", {"a": doc["accession"]})
    if rels:
        await db.execute_many(
            "INSERT INTO scout.company_relations (ticker, accession, filed, relation, name, related_ticker, detail, quote) "
            "VALUES (:t, :a, :d, :r, :n, :rt, :de, :q)",
            [{"t": doc["ticker"], "a": doc["accession"], "d": doc["filed"], "r": x["relation"], "n": x["name"],
              "rt": x["related_ticker"], "de": x["detail"], "q": x["quote"]} for x in rels])
    return len(rels)


async def refresh(tickers: list[str] | None = None, force: bool = False) -> dict:
    """Profile every scanned stock whose latest annual report has no profile yet (or the given tickers)."""
    inst = await db.fetch_all("SELECT ticker, name, kind FROM scout.instruments WHERE in_scan")
    idx = name_index(inst)
    names = tickers or [r["ticker"] for r in inst if r["kind"] == "stock"]
    ciks = await sec.cik_map()
    done = {r["accession"] for r in await db.fetch_all("SELECT accession FROM scout.company_profiles")}
    counts = {"tickers": len(names), "profiled": 0, "skipped": 0, "no_cik": 0, "no_annual": 0, "no_section": 0,
              "fetch_failed": 0, "failed": 0, "relations": 0}
    sem = asyncio.Semaphore(CONCURRENCY)

    async def one(t: str) -> None:
        cik = (ciks.get(t) or (None,))[0]
        if not cik:
            counts["no_cik"] += 1
            return
        f = await latest_annual(t, cik)
        if not f:
            counts["no_annual"] += 1
            return
        if f["accession"] in done and not force:
            counts["skipped"] += 1
            return
        doc = await fetch_doc(f)
        await store_doc(doc)
        if doc["status"] not in ("ok", "fallback"):
            counts[doc["status"]] += 1
            return
        if doc["status"] == "fallback":
            counts["fallback"] = counts.get("fallback", 0) + 1
        async with sem:
            try:
                profile, report, model = await extract(doc)
            except Exception as exc:
                logger.warning("profile %s failed: %s", t, exc)
                counts["failed"] += 1
                return
        counts["relations"] += await store_profile(doc, profile, report, model, idx)
        counts["profiled"] += 1

    await asyncio.gather(*(one(t) for t in names))
    return counts


# ── reading ──

async def latest(ticker: str) -> dict | None:
    """The latest profile with its relations, and the filings of other companies that name this one."""
    t = universe.yahoo_symbol(ticker)
    row = await db.fetch_one("SELECT p.*, d.form, d.url FROM scout.company_profiles p JOIN scout.company_docs d USING (accession) "
                             "WHERE p.ticker = :t ORDER BY p.filed DESC LIMIT 1", {"t": t})
    named_by = await db.fetch_all(
        "SELECT DISTINCT ON (r.ticker, r.relation) r.ticker, r.relation, r.name, r.detail, r.quote, r.filed FROM scout.company_relations r "
        "WHERE r.related_ticker = :t AND r.accession = (SELECT accession FROM scout.company_profiles p WHERE p.ticker = r.ticker "
        "ORDER BY p.filed DESC LIMIT 1) ORDER BY r.ticker, r.relation, r.filed DESC", {"t": t})
    if not row and not named_by:
        return None
    rels = await db.fetch_all("SELECT relation, name, related_ticker, detail, quote FROM scout.company_relations WHERE accession = :a "
                              "ORDER BY relation, id", {"a": row["accession"]}) if row else []
    return {"ticker": t, "profile": (row or {}).get("profile"), "form": (row or {}).get("form"), "filed": (row or {}).get("filed"),
            "url": (row or {}).get("url"), "validation": (row or {}).get("validation"), "relations": rels, "named_by": named_by}


async def neighbours(ticker: str) -> dict[str, list[str]]:
    """Tickers related to this one, both directions: {related ticker: [relations in words]}."""
    p = await latest(ticker)
    out: dict[str, list[str]] = {}
    if not p:
        return out
    zh = {"customer": "客户", "supplier": "供应商", "competitor": "竞争对手", "partner": "合作方"}
    for r in p["relations"]:
        if r["related_ticker"]:
            out.setdefault(r["related_ticker"], []).append(zh[r["relation"]])
    for r in p["named_by"]:
        mirror = {"customer": "供应商（对方的年报说它是客户）", "supplier": "客户（对方的年报说它是供应商）",
                  "competitor": "竞争对手", "partner": "合作方"}[r["relation"]]
        out.setdefault(r["ticker"], []).append(mirror)
    return out
