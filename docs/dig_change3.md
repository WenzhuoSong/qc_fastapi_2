# Agentix Development Roadmap v3
# 基于代码实际状态的校正版开发方案

Version: 2026-05-20
校正依据：源码静态分析（position_governance.py, target_builder.py,
          proposal_shaper.py, sector_rotation.py, pipeline.py,
          strategies/__init__.py）

---

## 一、当前代码实际状态（校正后）

### 1.1 已经实现，v2 文档误判为"待做"

| 功能 | 实现位置 | 实际状态 |
|------|---------|---------|
| Basket Review Detection | position_governance.py:940 (_explanation_facts) | ✅ 已实现 |
| Explanation Correctness（hard_risk 优先） | position_governance.py:921, 997 | ✅ 已实现 |
| Sector Momentum 计算 | sector_rotation.py:54 | ✅ 已实现（但仅输出给 LLM） |
| target_builder gated mode | pipeline.py:1112 | ✅ 已是 live path |
| 策略注册（6个） | strategies/__init__.py:22 | ✅ momentum/mean_reversion/low_vol/dual_momentum/risk_parity/equal_weight |
| Proposal Pre-flight | proposal_shaper.py:41 | ✅ 已实现基础版 |

### 1.2 真正的缺口（v2 文档判断准确）

| 缺口 | 影响 | 优先级 |
|------|------|--------|
| 无 Portfolio Construction 层 | basket/factor 风险低估，turnover 分配次优 | P0 |
| GROUP_LIMITS 定义分散在多个文件 | position_governance / proposal_shaper / 未来 optimizer 三套定义会漂移 | P0 |
| sector_rotation 输出只给 LLM，不进 deterministic target | 轮动信号未实质影响仓位构建 | P1 |
| target_builder diagnostics.mode 仍写 "shadow" | 审计日志误导，risk_out 显示错误系统状态 | P1 |
| Thesis 管理无定期刷新机制 | 亏损仓位 thesis_status 可能数天不更新 | P1 |
| 策略 certification 状态不清晰 | 6 个策略已注册，但 live fit 是否足够未知 | P2 |
| Walk-forward 验证缺失 | strategy_support 可信度依赖样本数，没有滚动验证 | P2 |

### 1.3 需要立刻修复的 bug（不是功能开发）

```
Bug 1：target_builder diagnostics["mode"] = "shadow"
  位置：services/target_builder.py
  问题：gated mode 已是 live path，但诊断字段仍写 shadow
  影响：decision ledger、daily analyst、dashboard 显示错误系统状态
  修复：改为 "target_builder_gated"，并在 risk_out 记录
        raw_llm_adjusted_weights_consumed = false

Bug 2（待确认）：GROUP_LIMITS 多处定义
  需要确认：position_governance、proposal_shaper、sector_rotation
           是否各自维护了 group limits 的副本
  影响：limits 不一致时三个模块会做出相互矛盾的判断
```

---

## 二、重新校准的优先级

```
P0（必须先做，阻塞后续开发）：
  1. Bug fix：diagnostics.mode
  2. GROUP 定义统一合约

P1（核心能力缺口）：
  3. Portfolio Construction Model（组合层面构建）
  4. sector_rotation 升级为 deterministic input
  5. Thesis 定期刷新机制

P2（信号质量提升）：
  6. 策略 certification 状态审计
  7. Walk-forward 验证

P3（持续改进）：
  8. Performance Attribution
  9. 执行质量追踪
```

---

## 三、Sprint 计划

---

### Sprint 1：修复 bug + 统一 GROUP 合约（3-5 天）

**目标：** 消除审计误导，为 Portfolio Construction 打好基础。

#### Task 1.1：修复 diagnostics.mode bug

```python
# services/target_builder.py
# 修复前（错误）
diagnostics = {
    "mode": "shadow",
    ...
}

# 修复后
diagnostics = {
    "mode": "target_builder_gated",
    "raw_llm_adjusted_weights_consumed": False,
    "target_construction_source": "deterministic_target_builder",
    ...
}
```

同步修改 risk_out 记录：

```python
# agents/risk_manager.py
risk_out["target_construction_mode"] = target_builder_result.diagnostics["mode"]
risk_out["raw_llm_adjusted_weights_consumed"] = False

# 加 assertion（safety invariant）
assert risk_out["raw_llm_adjusted_weights_consumed"] == False, \
    "Safety violation: LLM weights consumed as execution input"
```

**验收：**
- dashboard 显示 `target_construction_mode: target_builder_gated`
- step log 中不出现 `mode: shadow`

#### Task 1.2：GROUP 定义统一合约

**问题背景：**

当前 `SECTOR_GROUPS`、`GROUP_LIMITS` 可能分散在：
- `services/position_governance.py`
- `services/proposal_shaper.py`
- `services/sector_rotation.py`
- `services/target_builder.py`

**修复：** 抽取到单一 source of truth

```python
# services/group_contract.py
"""
GROUP_CONTRACT 是整个系统中 group/sector 定义的唯一来源。
任何模块需要 group 信息都应该 import 这里，不能自定义副本。
"""

from dataclasses import dataclass

@dataclass(frozen=True)
class GroupDefinition:
    name: str
    tickers: tuple[str, ...]
    limit_pct: float          # 组合暴露上限
    loss_review_threshold: float   # 亏损 review 阈值
    asset_type: str           # core / sector / thematic / leveraged

GROUP_CONTRACT: dict[str, GroupDefinition] = {
    "tech_growth": GroupDefinition(
        name="tech_growth",
        tickers=("QQQ", "XLK", "FTXL", "PSI", "SOXX"),
        limit_pct=0.35,
        loss_review_threshold=-0.04,
        asset_type="sector_thematic_mix",
    ),
    "semiconductors": GroupDefinition(
        name="semiconductors",
        tickers=("SOXX", "PSI", "FTXL"),
        limit_pct=0.25,
        loss_review_threshold=-0.04,
        asset_type="thematic",
    ),
    "defensive_bonds": GroupDefinition(
        name="defensive_bonds",
        tickers=("TLT", "BIL", "XLU", "XLP"),
        limit_pct=0.35,
        loss_review_threshold=-0.05,
        asset_type="defensive",
    ),
    "cyclicals": GroupDefinition(
        name="cyclicals",
        tickers=("XLI", "XLE", "IWM"),
        limit_pct=0.30,
        loss_review_threshold=-0.05,
        asset_type="sector",
    ),
    "real_estate": GroupDefinition(
        name="real_estate",
        tickers=("XLRE",),
        limit_pct=0.15,
        loss_review_threshold=-0.04,
        asset_type="sector",
    ),
}

def get_group(ticker: str) -> str | None:
    """给定 ticker 返回所属 group，不在任何 group 返回 None"""
    for group_name, defn in GROUP_CONTRACT.items():
        if ticker in defn.tickers:
            return group_name
    return None

def get_group_definition(group_name: str) -> GroupDefinition | None:
    return GROUP_CONTRACT.get(group_name)
```

**迁移方式：**

```python
# 所有模块改为：
from services.group_contract import GROUP_CONTRACT, get_group

# 替换所有本地定义的 SECTOR_GROUPS / GROUP_LIMITS 副本
# 迁移后加测试：
def test_no_duplicate_group_definitions():
    """确保没有模块自己维护 group 定义副本"""
    import ast, os
    forbidden_patterns = ["SECTOR_GROUPS", "GROUP_LIMITS", "sector_groups ="]
    for root, _, files in os.walk("services"):
        for f in files:
            if f.endswith(".py") and f != "group_contract.py":
                content = open(os.path.join(root, f)).read()
                for pattern in forbidden_patterns:
                    assert pattern not in content, \
                        f"Duplicate group definition in {f}: {pattern}"
```

**验收：**
- 全局搜索确认 `SECTOR_GROUPS` / `GROUP_LIMITS` 只在 group_contract.py 定义
- position_governance、proposal_shaper、sector_rotation 都从 group_contract import

---

### Sprint 2：Portfolio Construction Model（2-3 周）

**目标：** 在 target_builder 之前加一个组合层面的构建层，
处理因子暴露约束、basket 集中度、turnover 预算分配。

#### 2.1 设计边界

```
现有 target_builder 职责（保留）：
  - per-ticker governance 调整
  - validated advisory delta 应用
  - final weight clip
  - lifecycle trace 记录

新增 PortfolioConstructionModel 职责：
  - 组合层面因子暴露约束
  - basket 集中度约束（利用 GROUP_CONTRACT）
  - turnover 预算最优分配（而非等比例 clip）
  - effective diversification 计算

Pipeline 顺序：
  proposal_shaper
    -> PortfolioConstructionModel（新）
    -> target_builder（保留，做 per-ticker 精细调整）
    -> risk_manager（验证）
```

#### 2.2 核心实现

```python
# services/portfolio_construction.py

from services.group_contract import GROUP_CONTRACT, get_group

class PortfolioConstructionModel:
    """
    组合层面的构建模型。
    不引入 CVXPY，使用分层启发式：
      Step 1: 应用因子暴露约束（来自 GROUP_CONTRACT）
      Step 2: 应用 basket 集中度约束（active basket reviews）
      Step 3: 按信号强度分配 turnover 预算
      Step 4: 计算组合诊断指标
    
    设计约束：
      - 不 import agents/*
      - 不读 LLM 原始权重
      - 同样输入产生同样输出（deterministic）
    """

    def construct(
        self,
        base_weights: dict[str, float],
        current_weights: dict[str, float],
        signal_strengths: dict[str, float],  # per-ticker signal，来自 Playground
        basket_reviews: dict[str, "BasketReview"],
        scorecard_permission: str,
        turnover_budget: float,
    ) -> "PortfolioConstructionResult":

        weights = dict(base_weights)

        # Step 1: 因子暴露约束
        weights = self._apply_factor_limits(weights)

        # Step 2: basket 集中度约束
        weights = self._apply_basket_constraints(weights, basket_reviews)

        # Step 3: 信号加权的 turnover 分配
        weights = self._allocate_turnover_budget(
            weights, current_weights, signal_strengths, turnover_budget
        )

        # Step 4: 正规化
        weights = self._normalize(weights)

        return PortfolioConstructionResult(
            target_weights=weights,
            factor_exposures=self._calc_factor_exposures(weights),
            effective_n=self._calc_effective_n(weights),
            construction_steps=[
                "factor_limit_applied",
                "basket_constraint_applied",
                "turnover_budget_allocated",
                "normalized",
            ],
            violations=self._check_violations(weights),
        )

    def _apply_factor_limits(
        self, weights: dict
    ) -> dict:
        result = dict(weights)
        for group_name, defn in GROUP_CONTRACT.items():
            group_total = sum(result.get(t, 0) for t in defn.tickers)
            if group_total > defn.limit_pct:
                scale = defn.limit_pct / group_total
                for t in defn.tickers:
                    if t in result:
                        result[t] *= scale
        return result

    def _apply_basket_constraints(
        self, weights: dict, basket_reviews: dict
    ) -> dict:
        """
        active basket_review 时进一步压低组内权重
        basket_review_max = limit_pct * 0.7（额外收紧 30%）
        """
        result = dict(weights)
        for group_name, review in basket_reviews.items():
            defn = GROUP_CONTRACT.get(group_name)
            if not defn:
                continue
            reduced_limit = defn.limit_pct * 0.7
            group_total = sum(result.get(t, 0) for t in defn.tickers)
            if group_total > reduced_limit:
                scale = reduced_limit / group_total
                for t in defn.tickers:
                    if t in result:
                        result[t] *= scale
        return result

    def _allocate_turnover_budget(
        self,
        target: dict,
        current: dict,
        signal_strengths: dict,
        budget: float,
    ) -> dict:
        """
        当 turnover 超预算时：
        - 优先保留 signal 最强的 ticker 调整
        - 而不是等比例 clip 所有 ticker（当前 target_builder 的做法）
        """
        deltas = {
            t: target.get(t, 0) - current.get(t, 0)
            for t in set(target) | set(current)
        }
        total_to = sum(abs(d) for d in deltas.values()) / 2

        if total_to <= budget:
            return target

        # 按 signal strength * |delta| 排序（信号弱的调整优先放弃）
        priority = sorted(
            deltas.keys(),
            key=lambda t: abs(signal_strengths.get(t, 0)) * abs(deltas[t]),
            reverse=True,
        )

        result = dict(current)
        remaining = budget * 2

        for ticker in priority:
            delta = deltas[ticker]
            if abs(delta) <= remaining:
                result[ticker] = target.get(ticker, 0)
                remaining -= abs(delta)
            elif remaining > 0:
                sign = 1 if delta > 0 else -1
                result[ticker] = current.get(ticker, 0) + sign * remaining
                remaining = 0
            # remaining == 0：不再调整
        return result

    def _calc_factor_exposures(self, weights: dict) -> dict:
        return {
            group_name: sum(weights.get(t, 0) for t in defn.tickers)
            for group_name, defn in GROUP_CONTRACT.items()
        }

    def _calc_effective_n(self, weights: dict) -> float:
        """
        Effective N（等效持仓数）= 1 / sum(w^2)
        越高表示越分散
        """
        w = [v for v in weights.values() if v > 0]
        if not w:
            return 0.0
        return 1.0 / sum(x**2 for x in w)

    def _normalize(self, weights: dict) -> dict:
        total = sum(v for v in weights.values() if v > 0)
        if total == 0:
            return weights
        return {t: v / total for t, v in weights.items()}

    def _check_violations(self, weights: dict) -> list[str]:
        violations = []
        for group_name, defn in GROUP_CONTRACT.items():
            exposure = sum(weights.get(t, 0) for t in defn.tickers)
            if exposure > defn.limit_pct:
                violations.append(
                    f"{group_name}_exposure_{exposure:.1%}_exceeds_limit_{defn.limit_pct:.1%}"
                )
        return violations
```

#### 2.3 Pipeline 接入

```python
# services/pipeline.py 新增步骤

# 在 proposal_shaper 之后，target_builder 之前：
portfolio_construction_result = PortfolioConstructionModel().construct(
    base_weights=quant_baseline.base_weights,
    current_weights=current_holdings_weights,
    signal_strengths=strategy_signal_strengths,  # 从 Playground 获取
    basket_reviews=position_governance_out.basket_reviews,
    scorecard_permission=market_scorecard.permission,
    turnover_budget=effective_constraints.turnover_limit,
)

# target_builder 接收 portfolio construction 的输出作为起点
target_builder_result = build_target_weights(
    portfolio_construction_target=portfolio_construction_result.target_weights,
    # ... 其他参数不变
)
```

#### 2.4 PortfolioConstructionResult 进入 Decision Ledger

```python
"portfolio_construction": {
    "factor_exposures": {
        "tech_growth": 0.312,
        "semiconductors": 0.086,
        "defensive_bonds": 0.068,
        ...
    },
    "effective_n": 7.3,
    "violations": [],
    "basket_constraints_applied": ["semiconductors"],
    "turnover_budget_used": 0.089,
    "construction_steps": [...],
}
```

**验收：**
```
✓ tech_growth 组内总暴露不超过 GROUP_CONTRACT.limit_pct
✓ basket_review 触发时组内权重自动收紧到 limit * 0.7
✓ 信号强的 ticker 在 turnover 受限时优先保留调整幅度
✓ 同样输入多次运行结果完全一致
✓ 不 import agents/*，不读 LLM 原始权重
✓ violations 字段在超限时非空
```

---

### Sprint 3：sector_rotation 升级为 Deterministic Input（1-2 周）

**当前问题：**
`sector_rotation.py` 已有轮动计算，但输出是 prompt formatter 格式，
只给 LLM 看，没有进入 target_builder 或 portfolio_construction。

**目标：** 把 sector_rotation 的 leaders/laggards 信号接入 signal_strengths，
让它影响 portfolio_construction 的 turnover 预算分配。

#### 3.1 提取 deterministic signal

```python
# services/sector_rotation.py 新增方法

def get_deterministic_signals(
    self,
    universe: list[str],
) -> dict[str, float]:
    """
    返回 per-ticker 的 signal_strength（-1 到 1）
    用于 PortfolioConstructionModel 的 turnover 预算分配
    
    这是 deterministic 的，不依赖 LLM
    """
    rotation = self.calculate(universe)  # 已有的计算方法

    signals = {}
    for ticker in universe:
        factor = rotation.get_factor(ticker)
        if factor == "leading":
            signals[ticker] = 0.8
        elif factor == "mild_leading":
            signals[ticker] = 0.4
        elif factor == "neutral":
            signals[ticker] = 0.0
        elif factor == "mild_lagging":
            signals[ticker] = -0.4
        elif factor == "lagging":
            signals[ticker] = -0.8
        else:
            signals[ticker] = 0.0  # unknown

    return signals

def get_rotation_regime(self) -> str:
    """
    返回当前轮动状态（用于 scorecard / evidence_bundle）
    active_rotation / systemic_selloff / mixed
    """
    ...
```

#### 3.2 接入 evidence_bundle

```python
# evidence_bundle 新增字段（deterministic，不是 LLM 解读）
"sector_rotation": {
    "rotation_regime": "active_rotation",
    "signals": {
        "XLK":  0.8,   # leading
        "XLE":  0.4,   # mild_leading
        "XLU": -0.8,   # lagging
        "SOXX":-0.8,   # lagging
    },
    "leaders": ["XLK", "XLE"],
    "laggards": ["XLU", "SOXX", "XLRE"],
    "calculated_at": "...",
}
```

**接入 portfolio_construction：**

```python
signal_strengths = evidence_bundle["sector_rotation"]["signals"]
# 合并 Playground strategy_confidence 和 sector_rotation signals
combined_signals = merge_signals(
    strategy_signals=playground_signals,
    rotation_signals=sector_rotation_signals,
    weights=(0.6, 0.4)  # strategy 信号权重更高
)
```

**验收：**
```
✓ lagging 板块 ticker 在 turnover 受限时优先放弃调整
✓ leading 板块 ticker 在 turnover 受限时优先保留调整
✓ sector_rotation.signals 进入 decision ledger source_effects
✓ LLM 仍可以消费 sector_rotation 的解读文字，但执行不依赖它
```

---

### Sprint 4：Thesis 定期刷新机制（1-2 周）

**当前问题：**
thesis_status 有字段有 validator，但没有触发 researcher 主动 review 的机制。
亏损仓位可能数天 thesis_status 不更新。

#### 4.1 Thesis Review Scheduler

```python
# services/thesis_scheduler.py

class ThesisReviewScheduler:
    """
    Deterministic 调度器，决定哪些仓位需要今天 review thesis
    不做 thesis 判断本身，只决定"是否要 review"
    """

    def get_review_required(
        self,
        ticker: str,
        position_state: str,
        last_thesis_review_at: datetime | None,
        pnl_change_since_review: float,
        basket_review_active: bool,
    ) -> tuple[bool, str]:
        """返回 (required, reason)"""

        # loss_review / basket_review / hard_risk 总是 review
        if position_state in ["loss_review", "basket_loss_review", "hard_risk_review"]:
            return True, f"position_state_{position_state}_requires_daily_review"

        # 从未 review 过
        if last_thesis_review_at is None:
            return True, "never_reviewed"

        days_elapsed = (datetime.utcnow() - last_thesis_review_at).days

        # 正常仓位：5 天复查一次
        if days_elapsed >= 5:
            return True, f"scheduled_review_{days_elapsed}d_elapsed"

        # PnL 大幅变化触发
        if abs(pnl_change_since_review) >= 0.03:
            return True, f"pnl_change_{pnl_change_since_review:.1%}_triggers_review"

        return False, "no_review_needed"
```

#### 4.2 Researcher Thesis Review 格式

```python
# 给 researcher agent 的结构化 prompt 输入
THESIS_REVIEW_INPUT = {
    "review_purpose": "thesis_review",
    "ticker": "FTXL",
    "current_state": {
        "position_state": "loss_review",
        "unrealized_pnl": -0.066,
        "holding_days": 18,
        "last_thesis_status": "weakening",
        "last_review_at": "2026-05-15T00:00:00Z",
    },
    "evidence": {
        "sector_rotation": {
            "classification": "lagging",
            "signal": -0.8,
        },
        "basket_review": {
            "active": True,
            "group": "semiconductors",
            "n_losers": 3,
        },
        "news": {"bias": "negative", "hard_risk": False},
        "strategy_support": "advisory",
        "yfinance_relative_strength_20d": "negative",
    },
    "question": (
        "Based only on the structured evidence above, assess whether "
        "the original thesis for holding FTXL is: intact, weakening, "
        "or broken. State the primary evidence for your assessment. "
        "Do not speculate beyond the provided evidence."
    ),
}

# LLM 输出格式（强约束）
THESIS_REVIEW_OUTPUT_SCHEMA = {
    "ticker": str,
    "thesis_status": Literal["intact", "weakening", "broken", "unknown"],
    "confidence": float,  # 0.0 - 1.0
    "key_evidence": list[str],  # 最多 3 条
    "thesis_change_from_last": Literal["deteriorated", "improved", "no_change"],
    "execution_authority": Literal["none"],  # 必须是 none，不接受其他值
}
```

#### 4.3 Validator 加强

```python
# position_governance.py 的 thesis validator 新增规则

def _validate_thesis_status(
    self,
    proposed_status: str,
    current_position_state: str,
    deterministic_evidence: dict,
) -> str:
    """
    Validator 可以拒绝明显矛盾的 thesis 判断
    """

    # hard_risk 时 thesis 不能是 intact
    if current_position_state == "hard_risk_review":
        if proposed_status == "intact":
            return "weakening"  # 强制降级

    # basket_review + loss_review 时 thesis 不能是 intact
    if (deterministic_evidence.get("basket_review_active") and
            current_position_state == "loss_review"):
        if proposed_status == "intact":
            return "weakening"  # 强制降级

    # 策略 support 已经是 none 时 thesis 不能是 intact
    if deterministic_evidence.get("strategy_support") == "none":
        if proposed_status == "intact":
            return "unknown"  # 降级为 unknown，不是 intact

    return proposed_status  # 其他情况接受 LLM 判断
```

**验收：**
```
✓ loss_review 仓位每日触发 thesis review
✓ 5 天未 review 的正常仓位触发 scheduled review
✓ PnL 变化 > 3% 触发 review
✓ hard_risk + proposed intact -> validator 强制改为 weakening
✓ thesis_status 有 last_review_at 时间戳，可以追踪更新频率
```

---

### Sprint 5：策略 Certification 状态审计（1 周）

**目标：** 确认 6 个已注册策略的实际 certification 状态，
避免 strategy_support 被虚高估计。

#### 5.1 Strategy Certification 状态审计脚本

```python
# scripts/audit_strategy_certification.py

def audit_all_strategies():
    """
    对所有注册策略输出当前 certification 状态
    用于判断哪些策略的 suggested_use 是可信的
    """
    strategies = get_all_registered_strategies()

    report = []
    for strategy in strategies:
        cert = strategy.certification
        report.append({
            "name": strategy.name,
            "status": cert.status,
            "historical_samples": cert.historical.samples,
            "historical_sharpe": cert.historical.sharpe,
            "live_samples": cert.live.qc_snapshots,
            "live_fit": cert.live.fit,
            "suggested_use": strategy.suggested_use,
            "promotion_eligible": _check_promotion_eligible(cert),
            "risk": _assess_risk(cert),
        })

    return report

# 期望输出示例：
# name               status              hist_samples  sharpe  live  suggested_use
# momentum           research_supported  290           0.82    7     advisory
# mean_reversion     experimental        45            0.41    2     watch_only
# low_vol            research_supported  290           0.71    7     advisory
# dual_momentum      experimental        90            0.55    3     watch_only
# risk_parity        experimental        60            0.48    1     watch_only
# equal_weight       research_supported  290           0.63    7     advisory
```

**这个审计的目的：**
确认哪些策略真正可以是 advisory（影响 add），
哪些应该是 watch_only（只能影响 hold/monitor），
防止 6 个策略注册了但都是 advisory 导致约束形同虚设。

---

### Sprint 6：Walk-forward 验证（2 周）

**目标：** 对有足够历史数据的策略做滚动验证，
提高 strategy_support 的信号质量。

```python
class WalkForwardValidator:
    """
    参考：Interpretable Hypothesis-Driven Trading (Dec 2025)
    用 34 个独立测试区间做滚动验证，避免过拟合
    """

    def validate(
        self,
        strategy_name: str,
        train_window_days: int = 252,
        test_window_days: int = 21,
        n_periods: int = 12,
        min_periods_for_certification: int = 8,  # 至少 8/12 期为正
    ) -> WalkForwardResult:

        results = []
        for i in range(n_periods):
            # 严格的 out-of-sample 划分
            test_end = today - timedelta(days=i * test_window_days)
            test_start = test_end - timedelta(days=test_window_days)
            train_end = test_start
            train_start = train_end - timedelta(days=train_window_days)

            model = self._fit(strategy_name, train_start, train_end)
            result = self._evaluate(model, test_start, test_end)
            results.append(result)

        sharpes = [r.sharpe for r in results]
        hit_rates = [r.hit_rate for r in results]
        n_positive = sum(1 for s in sharpes if s > 0)

        # 晋升到 advisory 的条件（比 certification 宽松）
        advisory_eligible = (
            mean(hit_rates) > 0.52 and
            n_positive >= min_periods_for_certification and
            stdev(hit_rates) < 0.15  # 稳定性要求
        )

        return WalkForwardResult(
            strategy_name=strategy_name,
            n_periods=n_periods,
            avg_sharpe=mean(sharpes),
            avg_hit_rate=mean(hit_rates),
            hit_rate_std=stdev(hit_rates),
            n_positive_periods=n_positive,
            advisory_eligible=advisory_eligible,
            summary=self._summarize(results),
        )
```

---

## 四、完成后的系统对齐检查

| 行业标准层（QC Framework） | Agentix 对应 | Sprint 完成后状态 |
|--------------------------|-------------|-----------------|
| Universe Selection | Playground 候选 + Scorecard | ✅ 已完成 |
| Alpha Model | Researcher LLM + sector_rotation（deterministic） | Sprint 3 完成 |
| Portfolio Construction | PortfolioConstructionModel | Sprint 2 完成 |
| Risk Management | Risk Manager + Position Governance | ✅ 已完成 |
| Execution | Position Manager + Executor | ✅ 已完成 |
| Post-trade Analytics | Decision Ledger + Performance Attribution | Sprint 6 后 |

**FinRL-X weight-centric 接口对齐（Sprint 2-3 完成后）：**

```
weight = R( T( A( S(universe) ) ) )

S = Playground 候选 ∩ Scorecard 允许
A = PortfolioConstructionModel（因子约束 + basket + turnover 优化）
T = SmartOrder timing（未来 Sprint）
R = Risk Manager + Position Governance 验证

LLM 影响 A 层的 signal_strengths（通过 validated advisory delta）
不改变 S / R 的语义，不直接影响 T
```

---

## 五、不做的事（维持 v2 结论）

```
❌ 引入 CVXPY 或其他数值优化库
   - 用分层启发式替代，稳定性更好

❌ 让 LLM 直接输出 portfolio weights
   - 已经完成的迁移不回退

❌ 新增第三方数据源
   - 先把 sector_rotation 从"LLM 消费"升级到"deterministic input"

❌ 高频或日内 alpha

❌ 强化学习替代 governance
   - 样本量不足

❌ 用 advisory quality backfill 放宽执行约束
   - 等 counterfactual model 成熟
```

---

## 六、总体时间估算

```
Sprint 1:  3-5 天    bug fix + GROUP_CONTRACT
Sprint 2:  2-3 周    Portfolio Construction Model
Sprint 3:  1-2 周    sector_rotation -> deterministic input
Sprint 4:  1-2 周    Thesis 定期刷新
Sprint 5:  1 周      Strategy certification 审计
Sprint 6:  2 周      Walk-forward 验证

总计：约 8-10 周到 Sprint 6 完成
```

---

*版本：2026-05-20*
*基础：system_design_handbook.md + 源码静态分析*
*校正：基于 position_governance.py / target_builder.py / sector_rotation.py / pipeline.py 实际代码*