"""Knowledge base (v3 P1): what Scout has learned, and how sure it is.

Every entry is a claim with its mechanism, the conditions it holds under, the
evidence (stored study briefs), a status on the gate and a re-check date.
Negative findings are kept as entries too, so an idea that failed is not
tried again under another name. Entries are versioned in `scout.knowledge`:
a status change or an evidence update adds a version, nothing is overwritten.

Status gate (no step may be skipped):
  hypothesis → backtested → forward → confirmed      (judgment kind: hypothesis → forward)
  any open status → rejected; confirmed → retired; rejected / retired → hypothesis (re-test)
Only the user moves an entry to `confirmed`.

The AI interpretation reads the entries that match a stock's context
(`for_insight`); confirmed ones may be used as grounds, backtested / forward
ones only as "not yet verified", negative ones say an approach does not work.
"""
from __future__ import annotations

import logging
from datetime import date
from functools import lru_cache
from pathlib import Path

import yaml

from scout import db

logger = logging.getLogger("scout.learned")

SEED = Path(__file__).parent / "knowledge" / "learned.yaml"
KINDS = ("rule", "feature", "regularity", "method", "judgment")
POLARITIES = ("positive", "negative")
STATUSES = ("hypothesis", "backtested", "forward", "confirmed", "rejected", "retired")
OPEN = ("hypothesis", "backtested", "forward")
NEXT = {"hypothesis": ("backtested", "rejected"), "backtested": ("forward", "rejected"), "forward": ("confirmed", "rejected"),
        "confirmed": ("retired",), "rejected": ("hypothesis",), "retired": ("hypothesis",)}
USER_ONLY = {"confirmed"}
ACTORS = ("seed", "system", "user", "import")       # import = results brought in from B0 (scout.b0)
FIELDS = ("kind", "polarity", "status", "title", "claim", "mechanism", "context", "applies", "evidence", "invalidation",
          "recheck_on", "supersedes", "spec")
JSON_FIELDS = ("context", "applies", "evidence")          # never null; `spec` (the pre-registration, scout.gate) may be

KIND_ZH = {"rule": "交易规则", "feature": "特征", "regularity": "市场规律", "method": "分析方法", "judgment": "AI 判断方法"}
STATUS_ZH = {"hypothesis": "假设", "backtested": "回测通过（待前向验证）", "forward": "前向验证中", "confirmed": "已确认",
             "rejected": "否定", "retired": "已失效"}
# How the AI may use an entry, by status (what the prompt sees as 用法).
USE_ZH = {"backtested": "尚未验证：引用时写明\"回测通过、尚未前向验证\"",
          "forward": "尚未验证：引用时写明\"前向验证中\"",
          "confirmed": "已确认：可以作为依据",
          "rejected": "否定：经检验无效，不能当作看多或看空的理由"}
SHOWN_TO_AI = tuple(USE_ZH)


class KnowledgeError(ValueError):
    pass


def allowed_next(entry: dict, kind: str | None = None) -> tuple[str, ...]:
    """Statuses an entry may move to from its current status."""
    cur = entry["status"]
    out = NEXT.get(cur, ())
    if (kind or entry.get("kind")) == "judgment" and cur == "hypothesis":
        out = ("forward", "rejected")          # AI judgments cannot be backtested (training-data look-ahead)
    return out


def validate(entry: dict) -> list[str]:
    issues = []
    for f in ("key", "kind", "polarity", "status", "title", "claim"):
        if not entry.get(f):
            issues.append(f"缺少 {f}")
    if entry.get("kind") and entry["kind"] not in KINDS:
        issues.append(f"kind 不合法：{entry['kind']}")
    if entry.get("polarity") and entry["polarity"] not in POLARITIES:
        issues.append(f"polarity 不合法：{entry['polarity']}")
    if entry.get("status") and entry["status"] not in STATUSES:
        issues.append(f"status 不合法：{entry['status']}")
    if entry.get("status") != "hypothesis" and not entry.get("evidence"):
        issues.append("除假设外，每条知识都要有证据")
    for ev in entry.get("evidence") or []:
        if not (ev.get("source") and ev.get("date") and ev.get("summary")):
            issues.append("每条证据都要有 source、date、summary")
    for f in ("context", "applies"):
        if not isinstance(entry.get(f) or {}, dict):
            issues.append(f"{f} 必须是对象")
    return issues


def check_transition(old: dict | None, new: dict, actor: str) -> list[str]:
    if actor not in ACTORS:
        return [f"未知的修改者：{actor}"]
    if old is None:
        if actor == "import":
            return [] if new["status"] in ("hypothesis", "backtested", "forward", "rejected") else ["导入的知识不能直接是已确认"]
        if actor != "seed" and new["status"] != "hypothesis":
            return ["新知识只能从假设开始（预登记），再逐关推进"]
        return []
    if new["status"] == old["status"]:
        return []
    if new["status"] not in allowed_next(old, new.get("kind")):
        return [f"不能从「{STATUS_ZH[old['status']]}」直接到「{STATUS_ZH[new['status']]}」"]
    if new["status"] in USER_ONLY and actor != "user":
        return ["只有用户能把知识确认为「已确认」"]
    return []


# ── storage ─────────────────────────────────────────────────────────────

_CURRENT = ("SELECT DISTINCT ON (key) * FROM scout.knowledge {where} ORDER BY key, version DESC")


def _row(r: dict) -> dict:
    out = dict(r)
    for f in JSON_FIELDS:
        out[f] = out.get(f) if out.get(f) is not None else ({} if f != "evidence" else [])
    return out


async def current(key: str | None = None) -> list[dict]:
    """Latest version of every entry (or of one key)."""
    rows = await db.fetch_all(_CURRENT.format(where="WHERE key = :k" if key else ""), {"k": key} if key else None)
    return [_row(r) for r in rows]


async def get(key: str) -> dict | None:
    rows = await current(key)
    return rows[0] if rows else None


async def history(key: str) -> list[dict]:
    return [_row(r) for r in await db.fetch_all(
        "SELECT * FROM scout.knowledge WHERE key = :k ORDER BY version DESC", {"k": key})]


async def write(entry: dict, actor: str, note: str = "") -> dict:
    """Add a version of `entry` (new or changed) after the gate checks."""
    old = await get(entry["key"])
    new = {**(old or {}), **entry}
    issues = validate(new) + check_transition(old, new, actor)
    if issues:
        raise KnowledgeError("；".join(issues))
    version = (old["version"] + 1) if old else 1
    recheck = new.get("recheck_on")
    params = {"key": new["key"], "version": version, "changed_by": actor, "note": note or None,
              **{f: new.get(f) for f in FIELDS},
              "recheck_on": date.fromisoformat(str(recheck)) if recheck else None}
    for f in JSON_FIELDS:
        params[f] = db.jsonb(new.get(f) or ({} if f != "evidence" else []))
    params["spec"] = db.jsonb(new["spec"]) if new.get("spec") else None
    await db.execute(
        "INSERT INTO scout.knowledge (key, version, kind, polarity, status, title, claim, mechanism, context, applies, evidence, "
        "invalidation, recheck_on, supersedes, spec, changed_by, note) VALUES (:key, :version, :kind, :polarity, :status, :title, "
        ":claim, :mechanism, CAST(:context AS JSONB), CAST(:applies AS JSONB), CAST(:evidence AS JSONB), :invalidation, "
        ":recheck_on, :supersedes, CAST(:spec AS JSONB), :changed_by, :note)", params)
    return {**new, "version": version}


async def move(key: str, status: str, actor: str, note: str) -> dict:
    """Change an entry's status (a new version); the reason is required."""
    if not note.strip():
        raise KnowledgeError("状态变更必须写明理由")
    old = await get(key)
    if not old:
        raise KnowledgeError(f"没有这条知识：{key}")
    return await write({"key": key, "status": status}, actor, note)


@lru_cache()
def seed_entries() -> list[dict]:
    entries = yaml.safe_load(SEED.read_text(encoding="utf-8")) or []
    for item in entries:
        for ev in item.get("evidence") or []:
            ev["date"] = str(ev["date"])
        if item.get("recheck_on"):
            item["recheck_on"] = str(item["recheck_on"])
    return entries


async def seed() -> list[str]:
    """Insert seed entries whose key is not in the database yet; never overwrites."""
    have = {r["key"] for r in await db.fetch_all("SELECT DISTINCT key FROM scout.knowledge")}
    added = []
    for item in seed_entries():
        if item["key"] in have:
            continue
        await write(item, "seed", item.get("note") or "首批条目（来自已存的研究结果）")
        added.append(item["key"])
    if added:
        logger.info("knowledge seeded: %s", ", ".join(added))
    return added


# ── use ─────────────────────────────────────────────────────────────────

def matches(entry: dict, ctx: dict) -> bool:
    """Context conditions given as lists must contain the stock's value when it is known."""
    for k, allowed in (entry.get("context") or {}).items():
        if isinstance(allowed, list) and ctx.get(k) is not None and ctx[k] not in allowed:
            return False
    return True


def relevant(entries: list[dict], *, detector: str | None, lenses: set[str], ctx: dict) -> list[dict]:
    """Entries the interpretation of one stock should see: about its opportunity's rule or about a
    lens it reads, valid in its context, in a status the AI may use."""
    out = []
    for en in entries:
        if en["status"] not in SHOWN_TO_AI or not matches(en, ctx):
            continue
        ap = en.get("applies") or {}
        if (detector and detector in (ap.get("detectors") or [])) or lenses & set(ap.get("lenses") or []):
            out.append(en)
    order = {s: i for i, s in enumerate(("confirmed", "rejected", "forward", "backtested"))}
    return sorted(out, key=lambda en: (order.get(en["status"], 9), en["key"]))


def context_block(entries: list[dict]) -> tuple[list[dict], dict[str, dict]]:
    """(what the prompt sees as context.knowledge, scope per citation key for the lens groups)."""
    block, scope = [], {}
    for i, en in enumerate(entries):
        key = f"kb_{i + 1}"
        block.append({"key": key, "条目": en["title"], "结论": en["claim"], "状态": STATUS_ZH[en["status"]],
                      "用法": USE_ZH[en["status"]], "证据": "；".join(f"{ev['source']} {ev['date']}：{ev['summary']}"
                                                            for ev in en.get("evidence") or []),
                      **({"机制": en["mechanism"]} if en.get("mechanism") else {})})
        ap = en.get("applies") or {}
        scope[key] = {"id": en["key"], "lenses": list(ap.get("lenses") or []), "detectors": list(ap.get("detectors") or []),
                      "rule": rule_of(en)}
    return block, scope


async def for_insight(detector: str | None, lenses: set[str], ctx: dict) -> list[dict]:
    try:
        return relevant(await current(), detector=detector, lenses=lenses, ctx=ctx)
    except Exception as exc:          # the interpretation must not fail on the knowledge base
        logger.warning("knowledge lookup failed: %s", exc)
        return []


def rule_of(entry: dict) -> str | None:
    """The rule an entry is the verdict on: its registered spec's, else the first rule it is about."""
    if entry.get("spec") and entry["spec"].get("detector"):
        return entry["spec"]["detector"]
    detectors = (entry.get("applies") or {}).get("detectors") or []
    return detectors[0] if entry["kind"] == "rule" and detectors else None


def held_back(entries: list[dict]) -> set[str]:
    """Rules whose cards are recorded but not pushed: the knowledge base says they do not work
    (rejected or retired after the fair-benchmark test). Scanning, the board and the ledger go on,
    so the rules keep being measured forward."""
    return {r for en in entries if en["kind"] == "rule" and en["status"] in ("rejected", "retired")
            for r in [rule_of(en)] if r}


def due(entries: list[dict], today: date) -> list[dict]:
    """Entries past their re-check date (open and confirmed ones)."""
    return [en for en in entries if en.get("recheck_on") and en["status"] in (*OPEN, "confirmed")
            and date.fromisoformat(str(en["recheck_on"])) <= today]
