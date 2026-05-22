# Agentix Sprint 8 — 最终实施指南

> 本文档是第三次修订版，基于代码级审查补全了所有与当前实现的接口差异。
> 可直接作为开发蓝本，但 TICKER_ROLES 完整 universe 需要对照
> `group_contract.py` 补全后再开工。

---

## 目录

1. [开发顺序总览](#1-开发顺序总览)
2. [8A-0：Telegram 语义修正（立即可做）](#2-8a-0telegram-语义修正)
3. [8A-1：execution_policy.py（完整版）](#3-8a-1execution_policypy)
4. [8A-2：target_builder + executor preflight](#4-8a-2target_builder--executor-preflight)
5. [8A-3：QC fallback policy 同步部署](#5-8a-3qc-fallback-policy-同步部署)
6. [8B：QC ACK callback](#6-8bqc-ack-callback)
7. [8C：Hedge Intent Path](#7-8chedge-intent-path)
8. [附录：DB migration、测试矩阵、DoD](#8-附录)

---

## 1. 开发顺序总览

```
8A-0  Telegram: "Order executed" → "Command submitted"       ← 独立，立即 ship
  │
8A-1  新建 execution_policy.py（完整 universe）
  │   + test_execution_policy.py
  │   + test_policy_contract.py
  │
8A-2  target_builder 调 apply_policy_caps()
  │   executor preflight（下发 QC 前的最终防线）
  │
8A-3  QC 侧更新 fallback policy，部署到 QuantConnect Live
  │   （8A-3 需要同步部署，否则 FastAPI 改完 QC 旧逻辑仍拒绝）
  │
8B    ACK callback：HMAC + DB 持久化 + 状态机 + Telegram 二阶段
  │
8C    Hedge Intent：风险收缩计划（trim → cash → defensive → hedge ETF）
```

每个 step 独立可 ship，不阻塞下一步开始开发。

---

## 2. 8A-0：Telegram 语义修正

**文件：`agents/executor.py` 第 106–115 行附近**

当前代码：
```python
# agents/executor.py (约 line 110)
result = await tool_send_weight_command(payload)
if result.get("success"):
    await tool_send_telegram({"text": "✅ Order executed ..."})
```

`result["success"]` 只代表 QC Live Command API **收到了请求**，不是 QC 算法接受并执行。

修改后：
```python
result = await tool_send_weight_command(payload)
if result.get("success"):
    await tool_send_telegram({
        "text": (
            f"📤 *Command submitted* `{command_id[:8]}`\n"
            f"{_format_weight_summary(target_weights)}\n"
            f"_Awaiting QC algorithm execution. "
            f"QC confirmation tracking not yet enabled._"
        )
    })
else:
    await tool_send_telegram({
        "text": (
            f"⚠️ *QC command submission failed* `{command_id[:8]}`\n"
            f"Error: {result.get('error', 'unknown')}\n"
            f"*No weights were changed.*"
        )
    })
```

**验收**
```bash
grep -rn "Order executed" agents/ tools/
# → 零结果
```

---

## 3. 8A-1：execution_policy.py（完整版）

### 3.1 与现有代码的关系

三个文件的职责边界，必须在代码注释和 README 里明确：

| 文件 | 回答的问题 | 数据结构 |
|---|---|---|
| `group_contract.py` | 这个 ticker 属于哪个 exposure 篮子？ | `PRIMARY_GROUP`, `FACTOR_TAGS` |
| `execution_policy.py` | 这个 ticker 最多能下多少权重？ | `TICKER_ROLES`, `ROLE_POLICIES` |
| `position_governance.py` | 现在允许加/减/持有/退出？ | per-ticker decision |
| `target_builder` / `executor` | 最终能不能真的下发？ | preflight checks |

### 3.2 zero weight 顺序修正

**原文问题**：先判断 UNKNOWN/WATCHLIST 再判断 zero，导致 UNKNOWN ticker 的 0% 目标也被拒。
当 target_builder 或 position_governance 需要**显式清掉**一个 ticker 时，这会阻断正常 flatten 操作。

正确顺序：

```python
def check_weight_allowed(ticker: str, proposed_weight: float) -> tuple[bool, str]:
    # ① 零权重或负权重（平仓）永远允许，优先判断
    if proposed_weight <= 0.0:
        return True, "zero/non-positive weight — removal always allowed"

    # ② 正权重才检查 role
    role = get_role(ticker)
    if role == TickerRole.UNKNOWN:
        return False, (
            f"{ticker} UNKNOWN — not registered in execution_policy.TICKER_ROLES. "
            f"Add ticker before trading."
        )
    if role == TickerRole.WATCHLIST:
        return False, f"{ticker} WATCHLIST — observation only, not tradable"

    # ③ 检查 hard cap
    policy = ROLE_POLICIES[role]
    if proposed_weight > policy.max_single_weight:
        return False, (
            f"{ticker} ({role.value}) {proposed_weight:.2%} "
            f"> hard cap {policy.max_single_weight:.2%}"
        )
    return True, "within policy"
```

### 3.3 TICKER_ROLES 完整 Universe

**警告**：以下列表是基于 `group_contract.py` 已知 ticker 补全的**示例**，
开发前必须对照当前 `group_contract.py` 和实盘持仓逐项确认，不能直接复制。

```python
# services/execution_policy.py

TICKER_ROLES: Dict[str, TickerRole] = {

    # ── CORE ──────────────────────────────────────────────────
    "SPY":  TickerRole.CORE,
    "QQQ":  TickerRole.CORE,
    "IWM":  TickerRole.CORE,
    "VTI":  TickerRole.CORE,
    "VOO":  TickerRole.CORE,

    # ── SECTOR ────────────────────────────────────────────────
    "XLK":  TickerRole.SECTOR,
    "XLF":  TickerRole.SECTOR,
    "XLE":  TickerRole.SECTOR,
    "XLV":  TickerRole.SECTOR,
    "XLI":  TickerRole.SECTOR,
    "XLY":  TickerRole.SECTOR,
    "XLP":  TickerRole.SECTOR,
    "XLU":  TickerRole.SECTOR,
    "XLRE": TickerRole.SECTOR,
    "XLB":  TickerRole.SECTOR,
    "XLC":  TickerRole.SECTOR,

    # ── THEMATIC（原 satellite 中具有主题属性的上移）─────────────
    "SOXX": TickerRole.THEMATIC,
    "PSI":  TickerRole.THEMATIC,   # 原 5% → 7.5%（PSI 拒绝问题的直接修复）
    "FTXL": TickerRole.THEMATIC,
    "SMH":  TickerRole.THEMATIC,
    "XSD":  TickerRole.THEMATIC,
    "AIQ":  TickerRole.THEMATIC,
    "BOTZ": TickerRole.THEMATIC,
    "CIBR": TickerRole.THEMATIC,
    "ARKK": TickerRole.THEMATIC,
    "ARKW": TickerRole.THEMATIC,
    "ARKG": TickerRole.THEMATIC,

    # ── SATELLITE ─────────────────────────────────────────────
    "DRAM": TickerRole.SATELLITE,

    # ── 固定收益 / 防御性（视为 satellite，hedge intent 可加权）──
    "TLT":  TickerRole.SATELLITE,
    "IEF":  TickerRole.SATELLITE,
    "BND":  TickerRole.SATELLITE,
    "SGOV": TickerRole.SATELLITE,
    "BIL":  TickerRole.SATELLITE,
    "GLD":  TickerRole.SATELLITE,

    # ── HEDGE（hedge_only=True，只能通过 hedge intent path 进入）──
    "SQQQ": TickerRole.HEDGE,
    "SPXS": TickerRole.HEDGE,
    "UVXY": TickerRole.HEDGE,
    "VXX":  TickerRole.HEDGE,
    "SOXS": TickerRole.HEDGE,      # group_contract 里有，归 hedge

    # ── WATCHLIST（永不执行）──────────────────────────────────
    "TQQQ": TickerRole.WATCHLIST,  # 3x 杠杆，默认只观察
    "SOXL": TickerRole.WATCHLIST,  # group_contract 里有，默认 watchlist
                                   # 如需交易，需显式移至 THEMATIC/SATELLITE
}
```

**IMPORTANT**：`SOXL` 和 `TQQQ` 暂归 WATCHLIST，如果策略后续需要交易，
在 TICKER_ROLES 里改 role 即可生效，不需要改其他文件。

### 3.4 group_contract 补全 hedge primary group

当前 `group_contract.py` 里没有完整 hedge group，但 contract test 要求：
**execution_policy 中每个可交易 ticker（包括 HEDGE role）都必须在 `PRIMARY_GROUP` 中有记录。**

需要在 `group_contract.py` 增加：

```python
# services/group_contract.py — 新增 hedge primary group

# Primary groups
PRIMARY_GROUP.update({
    "SQQQ": "hedges",
    "SPXS": "hedges",
    "UVXY": "hedges",
    "VXX":  "hedges",
    "SOXS": "hedges",
})

# Factor tags（hedge 工具的 factor 是 inverse_equity / volatility）
FACTOR_TAGS.update({
    "SQQQ": ("inverse_equity", "hedges"),
    "SPXS": ("inverse_equity", "hedges"),
    "UVXY": ("volatility", "hedges"),
    "VXX":  ("volatility", "hedges"),
    "SOXS": ("inverse_equity", "semiconductors"),
})

GROUP_DEFINITIONS["hedges"] = {
    "description": "Inverse / volatility hedge instruments",
    "max_group_weight": 0.08,    # 与 execution_policy HEDGE group cap 一致
    "basket_review_threshold": 0.06,
}
```

### 3.5 policy cap 释放的权重归 CASH

`apply_policy_caps()` 压低某个 ticker 的权重后，释放的部分默认进入 CASH，
不自动重新分配给其他风险资产（"安全层只收紧"原则）。

```python
def apply_policy_caps(
    raw_targets: Dict[str, float],
) -> tuple[Dict[str, float], List[dict], float]:
    """
    返回 (capped_targets, cap_events, cash_raised_by_policy)。
    cash_raised_by_policy 写入 ledger 字段 cash_raised_by_policy_cap。
    """
    capped = {}
    cap_events = []
    total_released = 0.0

    for ticker, w in raw_targets.items():
        if w <= 0.0:
            capped[ticker] = w
            continue

        allowed, reason = check_weight_allowed(ticker, w)
        if not allowed:
            policy = get_policy(ticker)
            cap = policy.max_single_weight  # UNKNOWN/WATCHLIST cap = 0.0
            w_capped = min(w, cap)
            released = w - w_capped
            total_released += released
            cap_events.append({
                "ticker": ticker,
                "role": get_role(ticker).value,
                "original": round(w, 4),
                "capped_to": round(w_capped, 4),
                "released_to_cash": round(released, 4),
                "reason": reason,
            })
            capped[ticker] = w_capped
        else:
            capped[ticker] = w

    # Group cap 比例压缩（释放的同样计入 cash）
    group_results = check_portfolio_exposure(capped)
    for g in [r for r in group_results if r["violated"]]:
        before = {t: capped[t] for t in capped if get_role(t).value == g["role"]}
        capped = _scale_down_group(capped, g["role"], g["cap"])
        after = {t: capped[t] for t in before}
        group_released = sum(before[t] - after[t] for t in before)
        total_released += group_released
        cap_events.append({
            "group_role": g["role"],
            "original_total": round(g["current_total"], 4),
            "cap": g["cap"],
            "released_to_cash": round(group_released, 4),
            "action": "proportional_scale_down",
        })

    # target_builder 调用方负责把 total_released 加入 CASH 权重
    return capped, cap_events, round(total_released, 4)
```

`target_builder` 调用后：
```python
capped_targets, cap_events, cash_raised = apply_policy_caps(raw_targets)
capped_targets["CASH"] = capped_targets.get("CASH", 0.0) + cash_raised
assert sum(capped_targets.values()) <= 1.0 + 1e-6, "target sum overflow after cap"
```

### 3.6 contract test

```python
# tests/test_policy_contract.py

from services.execution_policy import TICKER_ROLES, TickerRole
from services.group_contract import PRIMARY_GROUP

def test_all_tradable_tickers_have_primary_group():
    tradable = {
        t for t, r in TICKER_ROLES.items()
        if r not in (TickerRole.WATCHLIST, TickerRole.UNKNOWN)
    }
    missing = tradable - set(PRIMARY_GROUP.keys())
    assert not missing, (
        f"Tickers in execution_policy (tradable) but missing "
        f"from group_contract.PRIMARY_GROUP:\n{sorted(missing)}"
    )

def test_watchlist_tickers_zero_cap():
    from services.execution_policy import check_weight_allowed
    watchlist = [t for t, r in TICKER_ROLES.items() if r == TickerRole.WATCHLIST]
    for ticker in watchlist:
        allowed, _ = check_weight_allowed(ticker, 0.01)
        assert not allowed, f"{ticker} watchlist should block positive weight"

def test_zero_weight_always_allowed():
    from services.execution_policy import check_weight_allowed
    for ticker in ["UNKNOWN_TICKER_XYZ", "TQQQ", "SQQQ", "SPY"]:
        allowed, reason = check_weight_allowed(ticker, 0.0)
        assert allowed, f"Zero weight should always be allowed for {ticker}, got: {reason}"

def test_unknown_ticker_blocked_positive():
    from services.execution_policy import check_weight_allowed
    allowed, reason = check_weight_allowed("COMPLETELY_UNKNOWN", 0.05)
    assert not allowed
    assert "UNKNOWN" in reason

def test_hedge_ticker_at_cap():
    from services.execution_policy import check_weight_allowed
    allowed_at_cap, _ = check_weight_allowed("SQQQ", 0.03)
    assert allowed_at_cap
    allowed_over_cap, _ = check_weight_allowed("SQQQ", 0.04)
    assert not allowed_over_cap

def test_psi_thematic_cap():
    """PSI 拒绝问题的直接回归测试"""
    from services.execution_policy import check_weight_allowed
    allowed_75, _ = check_weight_allowed("PSI", 0.075)
    assert allowed_75, "PSI@7.5% should pass (thematic cap)"
    allowed_76, _ = check_weight_allowed("PSI", 0.076)
    assert not allowed_76, "PSI@7.6% should be blocked (over thematic cap)"
```

---

## 4. 8A-2：target_builder + executor preflight

### 4.1 为什么需要两道检查

```
target_builder  →  risk_manager  →  position_governance  →  executor  →  QC
      ↑                                                          ↑
  构造时 cap                                               下发前 preflight
```

中间路径（SEMI_AUTO 的 /confirm、手动 proposal 覆盖、紧急 override）都可能绕过 target_builder
直接到达 executor。executor 的 preflight 是**最终防线**，不能省。

### 4.2 target_builder 修改

```python
# services/target_builder.py

from services.execution_policy import apply_policy_caps, policy_snapshot

class TargetBuilder:

    def build(self, ...) -> dict:
        raw_targets = self._construct_raw_targets(...)

        # policy cap：cap 超限，cash 接收释放权重
        capped_targets, cap_events, cash_raised = apply_policy_caps(raw_targets)
        capped_targets["CASH"] = capped_targets.get("CASH", 0.0) + cash_raised

        self._diagnostics.update({
            "policy_version": policy_snapshot()["version"],
            "policy_cap_events": cap_events,
            "cash_raised_by_policy_cap": cash_raised,
            "raw_llm_adjusted_weights_consumed": False,
            "target_construction_source": "deterministic_target_builder",
            "target_builder_gated": True,
        })

        return {"targets": capped_targets, "diagnostics": self._diagnostics}
```

### 4.3 executor preflight（新增函数）

```python
# agents/executor.py — 在 tool_send_weight_command() 调用前

from services.execution_policy import (
    check_weight_allowed,
    check_portfolio_exposure,
    policy_snapshot,
)

def preflight_execution_weights(
    weights: dict[str, float],
) -> dict:
    """
    executor 下发 QC 前的最终 preflight。
    独立于 target_builder，防止 SEMI_AUTO / override 路径绕过。

    返回：
        {
            "allowed": bool,
            "blocked_tickers": [...],
            "cap_violations": [...],
            "group_violations": [...],
            "policy_version": str,
        }
    """
    blocked = []
    cap_violations = []

    for ticker, w in weights.items():
        if w <= 0.0:
            continue
        allowed, reason = check_weight_allowed(ticker, w)
        if not allowed:
            cap_violations.append({
                "ticker": ticker,
                "weight": w,
                "reason": reason,
            })
            if "UNKNOWN" in reason or "WATCHLIST" in reason:
                blocked.append(ticker)

    group_results = check_portfolio_exposure(weights)
    group_violations = [g for g in group_results if g["violated"]]

    overall_allowed = len(blocked) == 0 and len(group_violations) == 0

    return {
        "allowed": overall_allowed,
        "blocked_tickers": blocked,
        "cap_violations": cap_violations,
        "group_violations": group_violations,
        "policy_version": policy_snapshot()["version"],
    }


# executor 主逻辑修改（tool_send_weight_command 调用前）：
async def execute_weights(self, target_weights: dict, command_id: str):
    preflight = preflight_execution_weights(target_weights)

    if not preflight["allowed"]:
        msg = (
            f"⛔ *Execution blocked by preflight* `{command_id[:8]}`\n"
            f"Blocked tickers: {preflight['blocked_tickers']}\n"
            f"Group violations: {preflight['group_violations']}\n"
            f"*No command sent to QC.*"
        )
        await tool_send_telegram({"text": msg})
        self._log_preflight_block(command_id, preflight)
        return

    # cap violations 但未 blocked（超 single cap 但有 allow 的情况不存在，仅 log）
    if preflight["cap_violations"]:
        self.logger.warning(
            f"[PREFLIGHT] cap violations (non-blocking): "
            f"{preflight['cap_violations']}"
        )

    result = await tool_send_weight_command({
        "target": "SetWeights",       # ← 保持现有 schema，不改为 type=set_weights
        "command_id": command_id,
        "weights": target_weights,
    })
    # ... Telegram submitted 消息（8A-0 已修改）
```

### 4.4 QC command schema 兼容说明

**不改现有 payload schema。** 当前 FastAPI 用：
```python
{"target": "SetWeights", "command_id": ..., "weights": ...}
```
当前 QC 算法用 `target == "SetWeights"` 解析。

Sprint 8A 新增 `target="PolicySync"` 用于 policy 同步，
与 SetWeights 并存，QC 侧分支处理：

```python
# quantconnect_files/test1.py

def on_command(self, data):
    target = self._get_field(data, "target", "")

    if target == "PolicySync":           # 新增，不破坏旧逻辑
        self._handle_policy_sync(data)
        return

    if target == "SetWeights":           # 现有逻辑不变
        self._handle_set_weights(data)
        return

    self.Log(f"[CMD] Unknown target={target}, ignoring")
```

**不引入 `type` 字段**，避免 FastAPI 和 QC 两侧同时改、同时部署的风险。

---

## 5. 8A-3：QC fallback policy 同步部署

**这一步必须和 8A-1/8A-2 同步部署到 QuantConnect Live，否则：**
- FastAPI 改完后 PSI@7.5% 不再发往 QC
- 但 QC 旧 fallback 仍然把 PSI 当 satellite 5% 处理
- 如果有手动命令或 /confirm 绕过 FastAPI，QC 还是会拒绝

```python
# quantconnect_files/test1.py — 更新 _FALLBACK_POLICY

_FALLBACK_POLICY = {
    "version": "sprint8a_fallback",
    "caps": {
        "core":      {"max_single": 0.25, "max_total_group": 0.75},
        "sector":    {"max_single": 0.15, "max_total_group": 0.45},
        "thematic":  {"max_single": 0.075, "max_total_group": 0.25},  # PSI 在此
        "satellite": {"max_single": 0.05, "max_total_group": 0.20},
        "hedge":     {"max_single": 0.03, "max_total_group": 0.08},
        "watchlist": {"max_single": 0.0,  "max_total_group": 0.0},
        "unknown":   {"max_single": 0.0,  "max_total_group": 0.0},
    },
    "roles": {
        # 保持与 execution_policy.TICKER_ROLES 一致
        # CI contract test 负责保证两者同步
        "SPY": "core",  "QQQ": "core",   "IWM": "core",
        "XLK": "sector","XLF": "sector", "XLE": "sector",
        "XLV": "sector","XLI": "sector",
        "SOXX":"thematic","PSI":"thematic","FTXL":"thematic",
        "SMH": "thematic","XSD":"thematic","AIQ":"thematic",
        "BOTZ":"thematic","CIBR":"thematic",
        "DRAM":"satellite",
        "TLT": "satellite","IEF":"satellite","BND":"satellite",
        "SGOV":"satellite","BIL":"satellite","GLD":"satellite",
        "SQQQ":"hedge","SPXS":"hedge","UVXY":"hedge",
        "VXX": "hedge","SOXS":"hedge",
        "TQQQ":"watchlist","SOXL":"watchlist",
    }
}
```

**部署检查**：更新后在 QC 日志里确认：
```
[POLICY] Loaded fallback policy version=sprint8a_fallback
```
并验证 PSI@7.5% 命令被接受，PSI@8.0% 命令被拒绝。

---

## 6. 8B：QC ACK callback

### 6.1 DB migration（先做 schema，再做逻辑）

当前 `ExecutionLog` 里 `command_id` 埋在 JSONB 里，查询和状态更新都麻烦。
先加独立列：

```sql
-- migration: add_execution_ack_columns.sql
ALTER TABLE execution_logs
    ADD COLUMN IF NOT EXISTS command_id      VARCHAR(64),
    ADD COLUMN IF NOT EXISTS qc_status       VARCHAR(32)  DEFAULT 'submitted',
    ADD COLUMN IF NOT EXISTS qc_ack_at       TIMESTAMP,
    ADD COLUMN IF NOT EXISTS qc_rejection_reason TEXT;

CREATE INDEX IF NOT EXISTS idx_execution_logs_command_id
    ON execution_logs (command_id);
```

现有 `command_payload`、`qc_response`、`status`、`retry_count` 列保留，
新列是**扩展**不是替换。`qc_status` 独立于 `status`（`status` 是 FastAPI 内部状态）。

### 6.2 QC ACK endpoint 认证

**QC ACK Header 写法修正**

原文用 `content.Headers.Add()`（Content Header，不对）。
正确的是在 `HttpRequestMessage` 上加 Request Header：

```python
# quantconnect_files/test1.py — QC 侧 ACK 发送

import json, hashlib, hmac
from System.Net.Http import HttpClient, HttpRequestMessage, HttpMethod, StringContent
from System.Text import Encoding
from System.Uri import Uri

def _send_ack(self, cmd_id: str, status: str, reason: str = ""):
    payload = {
        "cmd_id": cmd_id,
        "status": status,
        "reason": reason,
        "qc_timestamp": str(self.Time),
    }
    body_str  = json.dumps(payload)
    body_bytes = body_str.encode("utf-8")

    # HMAC 签名
    sig = hmac.new(
        self._webhook_secret.encode(),
        body_bytes,
        hashlib.sha256,
    ).hexdigest()

    try:
        client = HttpClient()
        client.Timeout = System.TimeSpan.FromSeconds(5)

        request = HttpRequestMessage(
            HttpMethod.Post,
            Uri(f"{self._fastapi_base_url}/execution/qc_ack"),
        )
        # ← Request Header，不是 Content Header
        request.Headers.Add("X-QC-Signature", sig)
        request.Content = StringContent(body_str, Encoding.UTF8, "application/json")

        response = client.SendAsync(request).Result
        self.Log(f"[ACK] Sent status={status} http={int(response.StatusCode)}")
    except Exception as e:
        self.Log(f"[ACK] Non-fatal send failure: {e}")
        # ACK 失败不影响交易本身执行
```

**FastAPI 侧认证**：

```python
# services/qc_webhook_auth.py

import hmac, hashlib, os
from fastapi import Request, HTTPException

def verify_qc_signature(request_body: bytes, x_qc_signature: str) -> bool:
    secret = os.environ["QC_WEBHOOK_SECRET"]
    expected = hmac.new(
        secret.encode(), request_body, hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, x_qc_signature or "")


# routes/execution.py
@router.post("/execution/qc_ack")
async def receive_qc_ack(request: Request, ack: QCExecutionAck):
    body = await request.body()
    sig  = request.headers.get("X-QC-Signature", "")
    if not verify_qc_signature(body, sig):
        raise HTTPException(status_code=401, detail="Invalid QC signature")

    await _persist_ack_to_db(ack)
    return {"received": True}
```

### 6.3 状态机（DB 轮询，不用内存 Event）

```python
# services/execution_ack_tracker.py

import asyncio
from db.models import ExecutionLog   # 用现有 model，加新字段

POLL_INTERVAL = 1   # 秒
TIMEOUT       = 30  # 秒

async def wait_for_qc_ack(cmd_id: str) -> str:
    """
    轮询 DB 直到 qc_status 有结果或超时。
    不用 asyncio.Event，避免多 worker / 重启问题。
    """
    for _ in range(TIMEOUT // POLL_INTERVAL):
        await asyncio.sleep(POLL_INTERVAL)
        row = await ExecutionLog.get_by_command_id(cmd_id)
        if row and row.qc_status in ("accepted", "rejected"):
            return row.qc_status

    # 超时：写入 timeout 状态
    await ExecutionLog.update_qc_status(cmd_id, "timeout_no_ack")
    return "timeout_no_ack"
```

Telegram 二阶段消息（8A-0 已修改 Stage 1，8B 加 Stage 2）：

```python
# agents/communicator.py

async def report_qc_ack(self, cmd_id: str, qc_status: str, row):
    if qc_status == "accepted":
        await self.telegram.send(
            f"✅ *QC accepted* `{cmd_id[:8]}`\n"
            f"Executed at {row.qc_ack_at}"
        )
    elif qc_status == "rejected":
        await self.telegram.send(
            f"❌ *QC rejected* `{cmd_id[:8]}`\n"
            f"Reason: {row.qc_rejection_reason}\n"
            f"*No position change occurred.*"
        )
    elif qc_status == "timeout_no_ack":
        await self.telegram.send(
            f"⚠️ *Execution timeout* `{cmd_id[:8]}`\n"
            f"No QC ack within {TIMEOUT}s.\n"
            f"Circuit set to ALERT. Verify positions manually."
        )
```

---

## 7. 8C：Hedge Intent Path

### 7.1 Pipeline context 字段修正

原文使用的字段名与当前 pipeline 不符，以下是对照修正：

| 原文字段 | 实际字段 | 说明 |
|---|---|---|
| `ctx["scorecard"]["requires_human"]` | `ctx["scorecard"]["require_human_confirmation"]` | 字段名不同 |
| `ctx["market_brief"]["breadth_score"]` | `ctx["market_brief"]["key_facts"]["breadth_pct"]` | 嵌套路径不同 |
| `ctx["risk_out"]["net_long_exposure"]` | 从 `ctx["holdings"]["weights"]` 计算 | `risk_out` 在 hedge stage 之后才存在 |
| `ctx["market_regime"]` | 需要映射当前实际 regime labels | 不假设只有 defensive/alert |

正确的 stage 输入：

```python
# services/pipeline.py — Sprint 8C

async def _run_hedge_intent_stage(self, ctx: dict) -> dict:
    from services.hedge_intent import evaluate_hedge_intent

    # net_long_exposure 从当前持仓计算（hedge stage 在 risk_out 之前）
    current_holdings = ctx.get("holdings", {}).get("weights", {})
    net_long = sum(
        w for w in current_holdings.values() if w > 0
    )

    # breadth_pct 从 key_facts 取，默认 0.5（中性）
    breadth = (
        ctx.get("market_brief", {})
           .get("key_facts", {})
           .get("breadth_pct", 0.5)
    )

    # human confirmation flag
    require_human = (
        ctx.get("scorecard", {})
           .get("require_human_confirmation", False)
    )

    # regime：保留原始 string，hedge_intent 内部映射
    regime_raw = ctx.get("market_regime", "normal")

    plan = evaluate_hedge_intent(
        vix_level                = ctx["market_brief"].get("vix", 20.0),
        portfolio_drawdown_pct   = ctx["holdings"].get("drawdown_pct", 0.0),
        net_long_exposure        = net_long,
        market_regime_raw        = regime_raw,
        current_holdings         = current_holdings,
        scorecard_requires_human = require_human,
        market_breadth_pct       = breadth,
    )

    ctx["hedge_intent"] = {
        "triggered":            plan.triggered,
        "reasons":              plan.trigger_reasons,
        "severity":             plan.severity,
        "trim_targets":         plan.trim_targets,
        "cash_raise_pct":       plan.target_cash_raise_pct,
        "defensive_candidates": plan.defensive_candidates,
        "hedge_instrument":     plan.hedge_instrument,
        "hedge_weight":         plan.hedge_weight,
        "add_hedge_etf":        plan.add_hedge_etf,
    }
    return ctx
```

### 7.2 regime 映射

不假设外部 regime label 的具体值，hedge_intent 内部做映射：

```python
# services/hedge_intent.py

_DEFENSIVE_REGIMES = {
    "defensive", "alert", "risk_off",
    "bear", "high_vol", "DEFENSIVE", "ALERT",
}

def _is_defensive_regime(regime_raw: str) -> bool:
    return regime_raw in _DEFENSIVE_REGIMES
```

### 7.3 风险收缩优先原则（保持修订版设计）

```
触发顺序（所有条件为确定性规则，不调 LLM）：

条件 A：VIX > 25 AND breadth_pct < 0.35
条件 B：drawdown < -5% AND regime is defensive
条件 C：require_human_confirmation AND net_long > 70%

任一条件满足 → triggered

severity score → 0–1
  < 0.3  → trim only + small cash raise（5%）
  0.3–0.6 → trim + cash raise（10%）+ defensive ETF 候选
  ≥ 0.7  → 以上全部 + Hedge ETF（SQQQ/UVXY）作为最后层
```

hedge ETF 不是第一反应，`severity >= 0.7` 才进入，并且 weight 不超过 HEDGE role 3% hard cap。

### 7.4 普通策略路径拦截 hedge ticker

在 `position_governance.py` 加入 hedge_only 检查：

```python
# services/position_governance.py

from services.execution_policy import get_policy

def _check_hedge_only_guard(
    ticker: str,
    entered_via_hedge_path: bool,
) -> tuple[bool, str]:
    """
    hedge_only=True 的 ticker 只能通过 hedge intent path 进入。
    普通策略评分路径发来的正权重建议一律拦截。
    """
    policy = get_policy(ticker)
    if policy.hedge_only and not entered_via_hedge_path:
        return False, (
            f"{ticker} is hedge_only — must enter via hedge intent path, "
            f"not via strategy scoring"
        )
    return True, "ok"
```

---

## 8. 附录

### 8.1 Decision Ledger 字段分阶段加入

避免 8A 完成后大量 `null` 字段让 dashboard 看起来残缺：

**8A 阶段加入（有数据）：**
```
ticker_role, single_cap, group_cap
policy_version, policy_cap_applied, policy_cap_original
policy_group_scaled, cash_raised_by_policy_cap
```

**8B 阶段加入（8B 完成后才有数据）：**
```
cmd_id, qc_status, qc_rejection_reason, qc_timestamp
```

**8C 阶段加入（8C 完成后才有数据）：**
```
entered_via_hedge_path, hedge_trigger_reasons
```

### 8.2 测试矩阵

| 测试文件 | 覆盖要点 | Phase |
|---|---|---|
| `test_execution_policy.py` | check_weight_allowed（含零权重顺序）、apply_policy_caps、cash released | 8A |
| `test_policy_contract.py` | tradable ↔ group_contract 覆盖、PSI 7.5%/7.6%、UNKNOWN block | 8A |
| `test_target_builder_policy.py` | cap 调用、cash 加入、diagnostics 字段 | 8A |
| `test_executor_preflight.py` | UNKNOWN block、group violation block、bypass 场景 | 8A |
| `test_communicator_semantic.py` | grep "Order executed" = 0 | 8A |
| `test_qc_ack_auth.py` | HMAC 正确/错误签名、401 响应 | 8B |
| `test_execution_ack_tracker.py` | submitted→accepted, →rejected, →timeout 状态机 | 8B |
| `test_hedge_intent.py` | 触发组合、severity 各区间、风险收缩层级、hedge_only 拦截 | 8C |

### 8.3 测试隔离修复（全局 grep）

```bash
# 找所有 module-level sys.modules stub
grep -rn "sys\.modules\[" tests/
grep -rn "sys\.modules\.setdefault" tests/
grep -rn "types\.ModuleType" tests/

# 重点检查 test_cron_audit.py，确认所有 stub 都在 setUp/tearDown 里
```

所有 stub 必须用 `patch.dict` 或 `setUp/tearDown` 配对，不能放在 module 顶层。

### 8.4 Sprint 8A Definition of Done

```
□ grep "Order executed" agents/ executor.py → 零结果
□ execution_policy.py 覆盖当前全量 universe（对照 group_contract 确认）
□ PSI@7.5% 通过 FastAPI preflight + QC fallback
□ PSI@7.6% 被 target_builder apply_policy_caps 拦截，cash 接收释放量
□ UNKNOWN ticker 正权重被 executor preflight 拦截，零权重允许
□ group_contract.py 新增 hedges primary group + factor tags
□ contract test 通过：tradable ↔ group_contract 完整覆盖
□ decision ledger 包含 ticker_role / policy_version / cap_applied / cash_raised
□ QC 侧 fallback 已部署，日志确认 version=sprint8a_fallback
□ 所有 P0 test 通过，无 module-level import 污染
```