# Agentix Sprint 8 — 代码级修正 delta（第四版）

> 前三版的架构方向和开发顺序已经稳定，本文档只记录 8 处代码级差异。
> 开发时以本文档为准，覆盖前版对应段落。

---

## 修正总览

| # | 文件 | 问题 | 影响 |
|---|---|---|---|
| 1 | `group_contract.py` | `GROUP_DEFINITIONS` 要用 `GroupDefinition` dataclass | 不修直接破 get_default_group_limit |
| 2 | `execution_policy.py` | TICKER_ROLES 缺 QC 实盘 universe 的大量 ticker | preflight 误判实盘 ticker 为 UNKNOWN |
| 3 | `group_contract.py` | PRIMARY_GROUP / FACTOR_TAGS 同样缺失上述 ticker | contract test 全部失败 |
| 4 | migration SQL | 表名 `execution_logs` 应为 `execution_log` | migration 打到不存在的表 |
| 5 | 8B DB 层 | ExecutionLog helper 是伪代码，应建 `execution_log_store.py` | 保持项目函数式风格 |
| 6 | `target_builder.py` | 当前是函数 `build_target_weights()`，不是 class | 不能按文档新建 class |
| 7 | `executor.py` preflight | `overall_allowed` 只 block UNKNOWN，漏掉 single cap 超限 | executor 最后防线失效 |
| 8 | `tool_send_weight_command` | 工具内部自己拼 `target`，调用方不需要传 | 文档简化，避免误导 |

---

## 修正 1：`GROUP_DEFINITIONS["hedges"]` 用 GroupDefinition dataclass

`group_contract.py` 里已有的 `GROUP_DEFINITIONS` 值是 `GroupDefinition` dataclass 实例，
直接赋 dict 会破坏 `get_default_group_limit()` 和现有 tests。

```python
# services/group_contract.py — 正确写法

# 在 group_contract.py 内直接使用已有 GroupDefinition dataclass；不要 self-import。

GROUP_DEFINITIONS["hedges"] = GroupDefinition(
    name="hedges",
    tickers=("SQQQ", "SPXS", "UVXY", "VIXY", "VXX", "SOXS"),
    limit_pct=0.08,                  # 与 execution_policy HEDGE group cap 一致
    loss_review_threshold=-0.03,     # hedge 工具更严格触发 review
    asset_type="hedge",
)

# TQQQ / SOXL / SPXL 归 HEDGE，允许通过受控 hedge/tactical path 交易。
```

---

## 修正 2 & 3：完整 universe 对齐（execution_policy + group_contract）

**硬前置要求**：`execution_policy.TICKER_ROLES` 必须完整覆盖 QC 实盘 universe，
否则 executor preflight 会把合法 ticker 判为 `UNKNOWN` 并阻断下发。

以下列表基于 `quantconnect_files/test1.py` 第 34 行附近的实盘 universe 补全，
开发前仍需对照当前文件最终确认。

### execution_policy.py — 完整 TICKER_ROLES

```python
TICKER_ROLES: Dict[str, TickerRole] = {

    # ── CORE ───────────────────────────────────────────────────
    "SPY":   TickerRole.CORE,
    "QQQ":   TickerRole.CORE,
    "IWM":   TickerRole.CORE,
    "RSP":   TickerRole.CORE,    # equal-weight S&P，归 core

    # ── SECTOR ─────────────────────────────────────────────────
    "XLK":   TickerRole.SECTOR,
    "XLF":   TickerRole.SECTOR,
    "XLE":   TickerRole.SECTOR,
    "XLV":   TickerRole.SECTOR,
    "XLI":   TickerRole.SECTOR,
    "XLY":   TickerRole.SECTOR,
    "XLP":   TickerRole.SECTOR,
    "XLU":   TickerRole.SECTOR,
    "XLRE":  TickerRole.SECTOR,
    "XLB":   TickerRole.SECTOR,
    "XLC":   TickerRole.SECTOR,
    "ITA":   TickerRole.SECTOR,  # aerospace & defense
    "XAR":   TickerRole.SECTOR,  # aerospace & defense alt
    "IBB":   TickerRole.SECTOR,  # biotech
    "XBI":   TickerRole.SECTOR,  # biotech alt

    # ── THEMATIC ───────────────────────────────────────────────
    "SOXX":  TickerRole.THEMATIC,
    "PSI":   TickerRole.THEMATIC,   # 原 satellite 5% → thematic 7.5%
    "FTXL":  TickerRole.THEMATIC,
    "SMH":   TickerRole.THEMATIC,
    "XSD":   TickerRole.THEMATIC,
    "AIQ":   TickerRole.THEMATIC,   # AI & big data
    "BOTZ":  TickerRole.THEMATIC,   # robotics & automation
    "CIBR":  TickerRole.THEMATIC,   # cybersecurity
    "HACK":  TickerRole.THEMATIC,   # cybersecurity alt
    "IGV":   TickerRole.THEMATIC,   # software
    "ICLN":  TickerRole.THEMATIC,   # clean energy
    "TAN":   TickerRole.THEMATIC,   # solar
    "URA":   TickerRole.THEMATIC,   # uranium / nuclear
    "GRID":  TickerRole.THEMATIC,   # smart grid
    "VUG":   TickerRole.THEMATIC,   # growth factor tilt
    "VTV":   TickerRole.THEMATIC,   # value factor tilt
    "USMV":  TickerRole.THEMATIC,   # min volatility factor

    # ── SATELLITE ──────────────────────────────────────────────
    "DRAM":  TickerRole.SATELLITE,

    # 国际 / 新兴市场
    "VEA":   TickerRole.SATELLITE,  # developed ex-US
    "VWO":   TickerRole.SATELLITE,  # emerging markets

    # 固定收益 / 防御性资产
    "TLT":   TickerRole.SATELLITE,
    "IEF":   TickerRole.SATELLITE,
    "BND":   TickerRole.SATELLITE,
    "SGOV":  TickerRole.SATELLITE,
    "GLD":   TickerRole.SATELLITE,

    # ── HEDGE（hedge_only=True，只能通过 hedge intent path 进入）──
    "SQQQ":  TickerRole.HEDGE,
    "SPXS":  TickerRole.HEDGE,
    "UVXY":  TickerRole.HEDGE,
    "VXX":   TickerRole.HEDGE,
    "VIXY":  TickerRole.HEDGE,   # QC universe 里有，归 hedge
    "SOXS":  TickerRole.HEDGE,

    # ── HEDGE / TACTICAL（hedge_only=True，只能通过受控路径进入）──
    "TQQQ":  TickerRole.HEDGE,      # 3x 多头杠杆，严格 cap
    "SOXL":  TickerRole.HEDGE,      # 3x 半导体杠杆，严格 cap
    "SPXL":  TickerRole.HEDGE,      # 3x S&P 杠杆，严格 cap
}
```

### group_contract.py — 补全 PRIMARY_GROUP 和 FACTOR_TAGS

以下是需要新增的条目（已有的不重复）：

```python
# services/group_contract.py — 新增条目

PRIMARY_GROUP.update({
    # 新增 CORE
    "RSP":  "broad_market",
    # 新增 SECTOR
    "ITA":  "defense",
    "XAR":  "defense",
    "IBB":  "biotech",
    "XBI":  "biotech",
    # 新增 THEMATIC
    "HACK": "cybersecurity",
    "IGV":  "software",
    "ICLN": "clean_energy",
    "TAN":  "clean_energy",
    "URA":  "nuclear_energy",
    "GRID": "clean_energy",
    "VUG":  "factor_growth",
    "VTV":  "factor_value",
    "USMV": "factor_minvol",
    # 新增 SATELLITE
    "VEA":  "international",
    "VWO":  "emerging_markets",
    # 新增 HEDGE（需要新 group "hedges"）
    "SQQQ": "hedges",
    "SPXS": "hedges",
    "UVXY": "hedges",
    "VXX":  "hedges",
    "VIXY": "hedges",
    "SOXS": "hedges",
    "TQQQ": "hedges",
    "SOXL": "hedges",
    "SPXL": "hedges",
})

FACTOR_TAGS.update({
    "RSP":  ("broad_market",),
    "ITA":  ("defense",),
    "XAR":  ("defense",),
    "IBB":  ("biotech", "healthcare"),
    "XBI":  ("biotech", "healthcare"),
    "HACK": ("cybersecurity", "tech_growth"),
    "IGV":  ("software", "tech_growth"),
    "ICLN": ("clean_energy",),
    "TAN":  ("clean_energy", "solar"),
    "URA":  ("nuclear_energy",),
    "GRID": ("clean_energy",),
    "VUG":  ("factor_growth", "broad_market"),
    "VTV":  ("factor_value", "broad_market"),
    "USMV": ("factor_minvol", "broad_market"),
    "VEA":  ("international",),
    "VWO":  ("emerging_markets",),
    "SQQQ": ("inverse_equity", "hedges"),
    "SPXS": ("inverse_equity", "hedges"),
    "UVXY": ("volatility", "hedges"),
    "VXX":  ("volatility", "hedges"),
    "VIXY": ("volatility", "hedges"),
    "SOXS": ("inverse_equity", "hedges"),
    "TQQQ": ("leveraged", "broad_market", "hedges"),
    "SOXL": ("leveraged", "semiconductors", "hedges"),
    "SPXL": ("leveraged", "broad_market", "hedges"),
})

# GROUP_DEFINITIONS 新增（使用 GroupDefinition dataclass）
GROUP_DEFINITIONS.update({
    "hedges": GroupDefinition(
        name="hedges",
        tickers=("TQQQ", "SQQQ", "SOXL", "SOXS", "SPXL", "SPXS", "UVXY", "VXX", "VIXY"),
        limit_pct=0.08,
        loss_review_threshold=-0.03,
        asset_type="hedge",
    ),
    "factor_growth": GroupDefinition(
        name="factor_growth",
        tickers=("VUG",),
        limit_pct=0.15,
        loss_review_threshold=-0.05,
        asset_type="thematic",
    ),
    "factor_value": GroupDefinition(
        name="factor_value",
        tickers=("VTV",),
        limit_pct=0.15,
        loss_review_threshold=-0.05,
        asset_type="thematic",
    ),
    "factor_minvol": GroupDefinition(
        name="factor_minvol",
        tickers=("USMV",),
        limit_pct=0.15,
        loss_review_threshold=-0.05,
        asset_type="thematic",
    ),
    "defense": GroupDefinition(
        name="defense",
        tickers=("ITA", "XAR"),
        limit_pct=0.15,
        loss_review_threshold=-0.05,
        asset_type="sector",
    ),
    "biotech": GroupDefinition(
        name="biotech",
        tickers=("IBB", "XBI"),
        limit_pct=0.15,
        loss_review_threshold=-0.07,  # biotech 波动更大
        asset_type="sector",
    ),
    "clean_energy": GroupDefinition(
        name="clean_energy",
        tickers=("ICLN", "TAN", "GRID"),
        limit_pct=0.15,
        loss_review_threshold=-0.07,
        asset_type="thematic",
    ),
    "nuclear_energy": GroupDefinition(
        name="nuclear_energy",
        tickers=("URA",),
        limit_pct=0.075,
        loss_review_threshold=-0.07,
        asset_type="thematic",
    ),
    "cybersecurity": GroupDefinition(
        name="cybersecurity",
        tickers=("CIBR", "HACK"),
        limit_pct=0.15,
        loss_review_threshold=-0.05,
        asset_type="thematic",
    ),
    "software": GroupDefinition(
        name="software",
        tickers=("IGV",),
        limit_pct=0.075,
        loss_review_threshold=-0.05,
        asset_type="thematic",
    ),
    "international": GroupDefinition(
        name="international",
        tickers=("VEA",),
        limit_pct=0.10,
        loss_review_threshold=-0.05,
        asset_type="satellite",
    ),
    "emerging_markets": GroupDefinition(
        name="emerging_markets",
        tickers=("VWO",),
        limit_pct=0.10,
        loss_review_threshold=-0.05,
        asset_type="satellite",
    ),
})
```

---

## 修正 4：DB migration 表名

```sql
-- migration: add_execution_ack_columns.sql
-- 正确表名是 execution_log（不是 execution_logs）
-- 参见 db/models.py: __tablename__ = "execution_log"

ALTER TABLE execution_log
    ADD COLUMN IF NOT EXISTS command_id           VARCHAR(64),
    ADD COLUMN IF NOT EXISTS qc_status            VARCHAR(32)  DEFAULT 'submitted',
    ADD COLUMN IF NOT EXISTS qc_ack_at            TIMESTAMP,
    ADD COLUMN IF NOT EXISTS qc_rejection_reason  TEXT;

CREATE INDEX IF NOT EXISTS idx_execution_log_command_id
    ON execution_log (command_id);
```

---

## 修正 5：ExecutionLog DB 操作用 service 层

不在 SQLAlchemy model 上加 class method，新建 service 函数保持项目风格：

```python
# services/execution_log_store.py（新建）

"""
执行日志的 DB 操作层。
只做数据存取，不含业务逻辑。
"""

from datetime import datetime, timezone
from sqlalchemy import select, update
from db.models import ExecutionLog
from db.session import AsyncSessionLocal


async def create_submitted_log(
    command_id: str,
    target_weights: dict,
    pipeline_run_id: str | None = None,
) -> ExecutionLog:
    async with AsyncSessionLocal() as session:
        log = ExecutionLog(
            command_id=command_id,
            command_payload={"weights": target_weights},
            qc_status="submitted",
            status="submitted",
        )
        session.add(log)
        await session.commit()
        return log


async def update_qc_status(
    command_id: str,
    qc_status: str,
    rejection_reason: str | None = None,
) -> None:
    async with AsyncSessionLocal() as session:
        await session.execute(
            update(ExecutionLog)
            .where(ExecutionLog.command_id == command_id)
            .values(
                qc_status=qc_status,
                qc_ack_at=datetime.now(timezone.utc),
                qc_rejection_reason=rejection_reason,
            )
        )
        await session.commit()


async def get_qc_status(command_id: str) -> str | None:
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(ExecutionLog.qc_status)
            .where(ExecutionLog.command_id == command_id)
        )
        row = result.scalar_one_or_none()
        return row


async def mark_timeout(command_id: str) -> None:
    await update_qc_status(command_id, "timeout_no_ack")
```

`wait_for_qc_ack()` 改为调用此 service：

```python
# services/execution_ack_tracker.py

from services.execution_log_store import get_qc_status, mark_timeout

async def wait_for_qc_ack(cmd_id: str, timeout: int = 30) -> str:
    for _ in range(timeout):
        await asyncio.sleep(1)
        status = await get_qc_status(cmd_id)
        if status in ("accepted", "rejected"):
            return status
    await mark_timeout(cmd_id)
    return "timeout_no_ack"
```

---

## 修正 6：target_builder 是函数，不是 class

直接在 `build_target_weights()` 内部接入 `apply_policy_caps()`，不新建 class：

```python
# services/target_builder.py — 修改 build_target_weights()

from services.execution_policy import apply_policy_caps, policy_snapshot

def build_target_weights(
    base_weights: dict,
    current_weights: dict,
    market_scorecard: dict,
    decision_style: dict,
    position_governance: dict,
    validated_advisory_deltas: dict,
    **kwargs,
) -> dict:
    # ... 现有逻辑，生成 raw_targets ...
    raw_targets = _construct_raw_targets(...)

    # ← 新增：policy cap，释放权重进 CASH
    capped_targets, cap_events, cash_raised = apply_policy_caps(raw_targets)
    capped_targets["CASH"] = capped_targets.get("CASH", 0.0) + cash_raised

    assert sum(v for v in capped_targets.values() if v > 0) <= 1.0 + 1e-6, \
        "target sum overflow after policy cap"

    diagnostics = {
        # ... 现有 diagnostics ...
        "policy_version":              policy_snapshot()["version"],
        "policy_cap_events":           cap_events,
        "cash_raised_by_policy_cap":   cash_raised,
        "raw_llm_adjusted_weights_consumed": False,
        "target_construction_source":  "deterministic_target_builder",
        "target_builder_gated":        True,
    }

    return {"targets": capped_targets, "diagnostics": diagnostics}
```

---

## 修正 7：executor preflight 的 `overall_allowed` 逻辑

原文只 block `UNKNOWN`/`WATCHLIST`，漏掉 single cap 超限（例如 PSI@8%，role=thematic 但超 7.5%）。

**executor 是最后防线，不做自动 cap，只做 block。** 自动 cap 应发生在 target_builder。

```python
# agents/executor.py — preflight_execution_weights()

def preflight_execution_weights(weights: dict[str, float]) -> dict:
    cap_violations  = []
    group_violations = []

    for ticker, w in weights.items():
        if w <= 0.0:
            continue  # 零权重/清仓永远放行
        allowed, reason = check_weight_allowed(ticker, w)
        if not allowed:
            cap_violations.append({
                "ticker": ticker,
                "weight": round(w, 4),
                "reason": reason,
            })

    group_results = check_portfolio_exposure(weights)
    group_violations = [g for g in group_results if g["violated"]]

    # executor 对任何 cap_violation 或 group_violation 都 block
    # 不做自动 cap，不做部分放行
    overall_allowed = (
        len(cap_violations) == 0
        and len(group_violations) == 0
    )

    return {
        "allowed":         overall_allowed,
        "cap_violations":  cap_violations,
        "group_violations": group_violations,
        "policy_version":  policy_snapshot()["version"],
    }
```

blocked 时 Telegram 消息：

```python
if not preflight["allowed"]:
    details = "\n".join(
        f"  {v['ticker']}: {v['weight']:.2%} — {v['reason']}"
        for v in preflight["cap_violations"]
    )
    await tool_send_telegram({
        "text": (
            f"⛔ *Execution blocked by preflight* `{command_id[:8]}`\n"
            f"Cap violations:\n{details}\n"
            f"Group violations: {preflight['group_violations']}\n"
            f"*No command sent to QC.*"
        )
    })
    return
```

---

## 修正 8：`tool_send_weight_command()` 调用方简化

`tools/qc_tools.py` 内部自己构造 `"target": "SetWeights"`，
调用方不需要、也不应该传 `target`，否则会让人误以为可以传任意 target。

```python
# agents/executor.py — 调用方正确写法

result = await tool_send_weight_command({
    "command_id": command_id,
    "weights": target_weights,
    # 不传 target，工具内部处理
})

# PolicySync 用独立工具（Sprint 8A-3）
# result = await tool_send_policy_sync({"payload": policy_snapshot()})
```

如果 8A-3 需要发 `target="PolicySync"`，应该在 `qc_tools.py` 里新增
`tool_send_policy_sync()` 函数，而不是让调用方自己拼 `target` 字段。

---

## 8A 开工前 checklist

按评审建议，8A-0 + 8A-1 可以立即开始：

```
Step 1  改 Telegram：grep 确认 "Order executed" → 0
Step 2  新建 services/execution_policy.py（完整 TICKER_ROLES）
Step 3  补全 services/group_contract.py
        - PRIMARY_GROUP / FACTOR_TAGS 新增所有缺失 ticker
        - GROUP_DEFINITIONS 新增 hedges / leveraged / 各主题 group
        - 使用 GroupDefinition dataclass，不用 dict
Step 4  写 tests/test_execution_policy.py
        - zero weight 顺序测试
        - PSI@7.5% pass / @7.6% block 回归
        - UNKNOWN 正权重 block / 零权重 pass
Step 5  写 tests/test_policy_contract.py
        - tradable ↔ PRIMARY_GROUP 完整覆盖断言
Step 6  run: uv run python -m pytest tests/test_policy_contract.py -v
        全部绿灯后才进 8A-2（target_builder + executor preflight）
```

Step 6 的 contract test 是 8A-2 的门禁：如果 universe 还有缺口，
preflight 上线后会阻断实盘合法 ticker，必须先把测试跑绿。
