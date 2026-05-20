# Agentix Development Roadmap v2
# 基于专业交易系统研究的新开发方案

Version: 2026-05-20
Base: system_design_handbook.md (current production state)

---

## 一、专业系统的核心参考

### 1.1 行业标准架构对比

专业量化系统（QC Algorithm Framework、FinRL-X、机构级 quant fund）
经过多年演进，形成了高度一致的分层模型：

```
Universe Selection     选股范围
  -> Alpha Model       信号生成（不感知 portfolio 状态）
  -> Portfolio Construction  持仓构建（优化问题）
  -> Risk Overlay      风险约束层
  -> Execution         执行层（寻找最优价格）
  -> Post-trade Analytics  事后分析
```

**关键原则（来自 QC 官方文档）：**

> Alpha model should not be influenced by the portfolio.
> Insights should not react to open positions.
> Separation of concerns: each component is isolated, responsibilities don't overlap.

**FinRL-X 的抽象（2026 最新论文）：**

```
weight = S(selection) · A(allocation) · T(timing) · R(risk_overlay)
```

所有组件通过统一的 weight-centric interface 通信，
LLM 信号（sentiment）可以进入 A 或 T 层，但不改变下游执行语义。

**机构级 quant fund 的四个部门（真实实践）：**

```
Research Dept    -> 发现 alpha 信号，记录 alpha 库
Trading Dept     -> 分析当前市场，从 alpha 库选择合适的 alpha
Risk Mgmt Dept   -> 评估并调整策略，适配风险偏好
Portfolio Manager -> 综合 trader 想法，构建最终策略执行
```

### 1.2 当前系统与行业标准的对比

| 行业标准层 | Agentix 对应模块 | 对齐程度 |
|-----------|----------------|---------|
| Universe Selection | Strategy Playground 候选 + scorecard 筛选 | ✅ 基本对齐 |
| Alpha Model | Researcher/Synthesizer LLM | ⚠️ LLM 仍有权重泄漏 |
| Portfolio Construction | target_builder（新建） | ⚠️ 迁移中 |
| Risk Overlay | Risk Manager + Position Governance | ✅ 较强 |
| Execution | Position Manager + Executor | ✅ 基本对齐 |
| Post-trade Analytics | Decision Ledger + Daily Analyst | ⚠️ 部分完成 |

**最大差距：**

当前系统缺少一个真正意义上的 **Portfolio Construction 层**。
target_builder 刚建立，但还没有引入：
- 组合层面的优化（不只是单 ticker clip）
- 因子暴露约束
- 相关性感知的仓位构建
- 系统性的 turnover 优化

这是下一阶段最重要的能力缺口。

---

## 二、当前系统状态评估

### 2.1 已经成熟的部分

```
数据层：
  ✅ QC / yfinance / news / knowledge 四层分离
  ✅ freshness metadata 分源标注
  ✅ evidence_bundle 统一接口

信号层：
  ✅ Strategy Playground historical evidence
  ✅ Market Scorecard deterministic permission
  ✅ Knowledge Resolver 合并静态知识和计算事实
  ✅ Confidence Calibrator 单点修改

治理层：
  ✅ Position Governance 状态机
  ✅ LLM Advisory Validator
  ✅ Advisory Quality Diagnostics
  ✅ Position Explanation Report

审计层：
  ✅ Decision Ledger 聚合审计
  ✅ Execution Audit 防误报
  ✅ Daily Memory backfill

迁移中：
  🔄 target_builder（shadow mode）
  🔄 LLM adjusted_weights 降级为 diagnostic
  🔄 explanation 从模板改为状态机导出
```

### 2.2 当前最大问题

**问题 1：Portfolio Construction 层缺失**
系统只有 per-ticker 的 clip 逻辑，没有组合层面的优化。
结果：高相关仓位被当作独立处理，basket risk 低估。

**问题 2：Alpha 信号稀疏**
Playground 只有 3 个策略，历史 replay 样本有限。
结果：advisory support 门槛低，几乎所有 ETF 都能维持 advisory 状态。

**问题 3：Proposal 质量低导致高拒绝率**
上游 proposal 没有组合感知，单轮 turnover 经常超限，
被 Risk Manager 整体拒绝，形成"生成大提案 → 全部拒绝"的循环。

**问题 4：Sector Momentum 缺失**
没有板块轮动信号，系统无法区分"板块性回调"和"系统性风险"。

**问题 5：Thesis Management 不闭环**
thesis_status 有框架但没有更新机制，
亏损仓位的持仓理由没有得到主动的、定期的重新评估。

---

## 三、新开发方案

### 3.1 总体目标

```
从"能挡风险"升级到"能主动管理仓位"

当前水平：
  数据有了 -> 信号有了 -> 风险能挡住 -> 但 proposal 质量低

目标水平：
  数据有了 -> 信号有了 -> 组合构建优化 -> 风险验证 -> 高质量执行
```

### 3.2 分阶段路线图

---

## Sprint 1：修复当前已知问题（1-2 周）

**目标：** 不改架构，只修正当前 deterministic 层的错误行为。

### Task 1.1 Explanation Correctness Fix

**问题：** XLRE why_hold 输出"no deterministic rule requires reduction"
**修复：** `_build_explanation()` 按 severity 优先级路由

```python
SEVERITY_ORDER = [
    "hard_risk",
    "basket_loss_review",
    "deep_loss_weak_support",
    "loss_review",
    "high_atr",
    "group_concentration",
    "winner_trim",
    "weak_support",
    "normal_hold",
]

def build_explanation(position_state, reason_codes, basket_context=None):
    primary = get_highest_severity(reason_codes, SEVERITY_ORDER)
    
    # hard_risk 路径：唯一且不可 fallback
    if primary == "hard_risk" or position_state == "hard_risk_review":
        return Explanation(
            why_hold=[
                "hard-risk event is active",
                "automatic execution requires human confirmation",
                "manual trim/exit review is required"
            ],
            why_not_add=["hard-risk blocks all adds"],
            why_not_exit=["exit requires explicit manual confirmation"],
            next_trigger="resolve hard-risk event or manual confirmation"
        )
    # 其他路径按 severity 严格路由，禁止 fallback 到低 severity
    ...
```

**验收：**
- `pytest tests/test_position_governance.py -k "hard_risk"`
- XLRE 类仓位的 why_hold 包含 "hard-risk event is active"

### Task 1.2 Basket Review Detection

**问题：** FTXL/PSI/SOXX 三行解释完全相同，没有组合级上下文
**修复：** 在 portfolio_summary 层加 `detect_basket_reviews()`

```python
def detect_basket_reviews(
    position_decisions: list,
    knowledge_base: KnowledgeBase,
    threshold: int = 2  # 触发条件：同 group >= 2 个 loss_review
) -> dict[str, BasketReview]:
    
    group_losers = defaultdict(list)
    for d in position_decisions:
        if d.position_state in ["loss_review", "basket_loss_review"]:
            group = knowledge_base.get_group(d.ticker)
            if group:
                group_losers[group].append(d.ticker)
    
    return {
        group: BasketReview(
            group=group,
            loss_tickers=tickers,
            n_loss_review=len(tickers),
            severity="elevated",
            trigger="correlated_loss_cluster"
        )
        for group, tickers in group_losers.items()
        if len(tickers) >= threshold
    }
```

**验收：**
- 半导体篮子（FTXL/PSI/SOXX）触发 basket_review
- 每个 ticker 的 explanation 包含 basket 上下文，不再是相同文字

### Task 1.3 Proposal Pre-flight Expansion

**问题：** loss_review ticker 出现在 add proposal 中
**修复：** `is_add_allowed()` 前置检查

```python
def is_add_allowed(ticker, governance_state, scorecard, knowledge_base) -> tuple[bool, str]:
    """返回 (allowed, reason)"""
    
    checks = [
        (governance_state.position_state not in
         ["loss_review", "basket_loss_review", "hard_risk_review"],
         "position_in_loss_or_risk_review"),
        
        (not governance_state.basket_review,
         "basket_review_blocks_add"),
        
        (not governance_state.atr_high,
         "high_atr_blocks_add"),
        
        (scorecard.permission not in
         ["hold_or_trim", "defensive_only", "cash_only", "reduce_risk_only"],
         "scorecard_permission_blocks_add"),
        
        (governance_state.group_exposure < governance_state.group_limit,
         "group_exposure_limit_reached"),
        
        (governance_state.strategy_support in ["primary", "advisory"],
         "insufficient_strategy_support"),
    ]
    
    for passed, reason in checks:
        if not passed:
            return False, reason
    
    return True, "allowed"
```

### Task 1.4 Telegram Data Quality Split

**问题：** `data=limited` 合并显示，误导用户
**修复：** communicator 分源显示

```
# 旧格式（错误）
Data quality: limited

# 新格式（正确）
Data quality
  QC heartbeat:    fresh (15min ago)
  Daily snapshot:  stale (3d ago)
  Live fit:        insufficient (7 samples, need 30)
  yfinance:        strong (290 samples)
  News cache:      fresh (2h ago)
```

**验收：**
- Telegram 不出现单行 `data=limited`
- 每个子项独立显示

---

## Sprint 2：完成 target_builder 迁移（2-3 周）

**目标：** 把 target_builder 从 shadow mode 推进到 gated mode。

### Task 2.1 Target Builder Shadow Mode 验证

**当前状态：** target_builder 写 step log，不影响执行
**本 task：** 建立 old vs new 对比指标，确定 shadow mode 退出条件

```python
# step log 里记录对比
{
  "target_builder_diff": {
    "old_path": "synthesizer_adjusted_weights",
    "new_path": "target_builder_target_weights",
    "per_ticker_diff": {
      "QQQ": {
        "old_target": 0.118,
        "new_target": 0.115,
        "delta": -0.003,
        "divergence_reason": "governance_clip_applied"
      }
    },
    "max_abs_diff": 0.012,
    "within_tolerance": true  # tolerance = 0.02
  }
}
```

**Shadow mode 退出条件（明确定义）：**
```
条件 1：连续 5 个交易日 max_abs_diff < 0.02
条件 2：没有 target_builder 导致的 schema 或 runtime 错误
条件 3：人工 review step log 确认 new path 行为合理
满足以上全部 -> 进入 gated mode
```

### Task 2.2 Target Builder Gated Mode

**改动：** Risk Manager 输入切换，behind config flag

```python
# pipeline.py
TARGET_BUILDER_ENABLED = os.getenv("TARGET_BUILDER_ENABLED", "false") == "true"

if TARGET_BUILDER_ENABLED:
    risk_input = target_builder_output["target_weights"]
    target_construction_mode = "target_builder_gated"
else:
    risk_input = shaped_synthesizer_weights  # legacy
    target_construction_mode = "legacy_shaped_llm"

risk_out = risk_manager.check(
    proposed_weights=risk_input,
    target_construction_mode=target_construction_mode,
    ...
)
```

**Safety check：**
```python
# Risk Manager 必须记录并断言
assert risk_out["raw_llm_adjusted_weights_consumed"] == False
```

### Task 2.3 Per-ticker Lifecycle Trace

**新增字段到 Decision Ledger：**

```python
"trade_lifecycle": {
  "base_weight": 0.045,           # quant_baseline
  "strategy_target": 0.040,       # playground weighted
  "diagnostic_llm_target": 0.030, # synthesizer（diagnostic only）
  "validated_advisory_delta": -0.005,  # validator 裁剪后
  "governance_target": 0.035,     # governance adjustment
  "risk_checked_target": 0.035,   # risk manager 验证后
  "final_target": 0.035,          # position manager 后
  "changed_by": ["governance_basket_clip", "validated_llm_delta"],
  "target_construction_mode": "target_builder_gated"
}
```

---

## Sprint 3：Portfolio Construction 层（3-4 周）

**目标：** 引入组合层面的构建逻辑，不只是 per-ticker clip。

这是整个 roadmap 里最重要的新能力。

### 3.0 为什么需要 Portfolio Construction 层

当前 target_builder 的问题：
```
for ticker in universe:
    target[ticker] = clip(base + governance_adj + llm_delta, max_single)

这是逐 ticker 的贪心算法，没有组合约束。
结果：
- 半导体篮子合计暴露 = FTXL + PSI + SOXX + XLK + QQQ 的总和
  可能超过风险预算，但 per-ticker 都没有超限
- turnover 约束在 risk manager 层才被发现，太晚了
- 没有优化：不知道哪个 ticker 的 marginal risk contribution 最高
```

### Task 3.1 Factor Exposure 计算

```python
class FactorExposureCalculator:
    """
    计算组合的因子暴露
    参考：Barra 风险因子模型的简化版
    """
    
    FACTOR_GROUPS = {
        "tech_growth":    ["QQQ", "XLK", "FTXL", "PSI", "SOXX"],
        "energy":         ["XLE"],
        "defensive":      ["XLU", "XLP", "TLT", "BIL"],
        "real_estate":    ["XLRE"],
        "industrial":     ["XLI"],
        "small_cap":      ["IWM"],
        "broad_market":   ["SPY"],
    }
    
    def calculate(self, weights: dict[str, float]) -> FactorExposure:
        exposures = {}
        for factor, tickers in self.FACTOR_GROUPS.items():
            exposures[factor] = sum(
                weights.get(t, 0) for t in tickers
            )
        
        # 相关性调整：tech_growth 内部相关性高，需要额外惩罚
        tech_corr_penalty = self._calc_intragroup_correlation_penalty(
            weights, "tech_growth"
        )
        
        return FactorExposure(
            exposures=exposures,
            effective_diversification=self._calc_effective_n(weights),
            tech_corr_penalty=tech_corr_penalty
        )
    
    def _calc_intragroup_correlation_penalty(
        self, weights, group
    ) -> float:
        """
        组内多个高权重持仓 = 因子暴露放大
        简化：用 HHI（Herfindahl-Hirschman Index）衡量组内集中度
        """
        group_tickers = self.FACTOR_GROUPS[group]
        group_weights = [weights.get(t, 0) for t in group_tickers]
        total = sum(group_weights)
        if total == 0:
            return 0
        shares = [w / total for w in group_weights]
        hhi = sum(s**2 for s in shares)
        return hhi * total  # 越集中、组内总权重越大，惩罚越高
```

### Task 3.2 Portfolio Construction Model

```python
class PortfolioConstructionModel:
    """
    参考 QC Algorithm Framework 的 Portfolio Construction Model 设计
    输入：alpha insights + constraints
    输出：target weights（满足所有约束的组合最优解）
    
    不使用复杂的 CVXPY 优化（避免引入不稳定依赖），
    而是使用分层启发式方法：
    1. 从 base weights 出发
    2. 应用因子暴露约束
    3. 应用 basket 集中度约束
    4. 应用 turnover 预算约束
    5. 正规化
    """
    
    def construct(
        self,
        base_weights: dict,
        current_weights: dict,
        alpha_signals: dict,      # per-ticker signal strength
        factor_limits: dict,      # factor exposure limits
        basket_reviews: dict,     # active basket reviews
        scorecard: Scorecard,
        turnover_budget: float,
    ) -> PortfolioConstructionResult:
        
        # Step 1: 应用因子暴露约束
        weights = self._apply_factor_constraints(
            base_weights, factor_limits
        )
        
        # Step 2: 应用 basket 集中度约束
        weights = self._apply_basket_constraints(
            weights, basket_reviews
        )
        
        # Step 3: 应用 alpha signal 调整（有界）
        weights = self._apply_alpha_signals(
            weights, alpha_signals, max_signal_delta=0.02
        )
        
        # Step 4: turnover 预算分配
        # 把有限的 turnover 预算分配给 signal 最强的 ticker
        weights = self._allocate_turnover_budget(
            weights, current_weights, turnover_budget
        )
        
        # Step 5: 正规化
        weights = self._normalize(weights)
        
        return PortfolioConstructionResult(
            target_weights=weights,
            factor_exposures=self.factor_calc.calculate(weights),
            construction_steps=[...],
            violations=[],
        )
    
    def _apply_basket_constraints(
        self, weights, basket_reviews
    ) -> dict:
        """
        basket_review 时：
        - 组内所有 ticker 权重上限降低
        - 组内亏损 ticker 不能超过 watchlist_max_single
        """
        result = dict(weights)
        for group, review in basket_reviews.items():
            group_total = sum(
                result.get(t, 0)
                for t in self.factor_groups[group]
            )
            if group_total > review.max_group_weight:
                # 等比例缩减
                scale = review.max_group_weight / group_total
                for t in self.factor_groups[group]:
                    result[t] = result.get(t, 0) * scale
        return result
    
    def _allocate_turnover_budget(
        self, target, current, budget
    ) -> dict:
        """
        当 turnover 超预算时，优先保留 signal 最强的调整
        而不是等比例 clip 所有 ticker
        """
        deltas = {
            t: target.get(t, 0) - current.get(t, 0)
            for t in set(target) | set(current)
        }
        total_turnover = sum(abs(d) for d in deltas.values()) / 2
        
        if total_turnover <= budget:
            return target
        
        # 按信号强度排序，优先保留重要调整
        sorted_tickers = sorted(
            deltas.keys(),
            key=lambda t: abs(deltas[t]),
            reverse=True
        )
        
        result = dict(current)
        remaining_budget = budget * 2  # 买卖双向
        
        for ticker in sorted_tickers:
            delta = deltas[ticker]
            if abs(delta) <= remaining_budget:
                result[ticker] = target.get(ticker, 0)
                remaining_budget -= abs(delta)
            else:
                # 部分应用
                sign = 1 if delta > 0 else -1
                result[ticker] = current.get(ticker, 0) + sign * remaining_budget
                remaining_budget = 0
                break
        
        return result
```

### Task 3.3 把 Portfolio Construction 接入 Pipeline

```
当前 pipeline（Sprint 2 完成后）：
  target_builder -> risk_manager

新 pipeline：
  portfolio_construction_model
    -> target_builder（做 per-ticker 精细调整）
    -> risk_manager（验证）

Portfolio Construction Model 的职责：
  组合层面的因子暴露控制
  basket 集中度控制
  turnover 预算分配优化

Target Builder 的职责：
  per-ticker 的 governance/advisory 调整
  final weight clip
  lifecycle trace 记录
```

---

## Sprint 4：Alpha 信号增强（3-4 周）

**目标：** 增加信号多样性，减少对单一 strategy 的依赖。

### Task 4.1 Sector Momentum 信号

```python
class SectorMomentumSignal:
    """
    用 yfinance 计算板块相对动量
    这是 deterministic 的，不依赖 LLM
    """
    
    def calculate(
        self,
        universe: list[str],
        lookback_short: int = 20,   # 短期：1个月
        lookback_long: int = 60,    # 长期：3个月
        benchmark: str = "SPY"
    ) -> SectorMomentumResult:
        
        prices = yfinance.download(universe + [benchmark], period="3mo")
        
        results = {}
        spy_return_short = self._calc_return(prices, benchmark, lookback_short)
        spy_return_long = self._calc_return(prices, benchmark, lookback_long)
        
        for ticker in universe:
            ret_short = self._calc_return(prices, ticker, lookback_short)
            ret_long = self._calc_return(prices, ticker, lookback_long)
            
            rel_short = ret_short - spy_return_short
            rel_long = ret_long - spy_return_long
            
            # 复合信号：短期和长期一致时更强
            signal_strength = 0.6 * rel_short + 0.4 * rel_long
            
            results[ticker] = SectorMomentum(
                ticker=ticker,
                relative_short=rel_short,
                relative_long=rel_long,
                signal_strength=signal_strength,
                classification=self._classify(signal_strength),
                # leading / mild_leading / neutral / mild_lagging / lagging
            )
        
        return SectorMomentumResult(
            signals=results,
            benchmark=benchmark,
            calculated_at=datetime.utcnow(),
            rotation_regime=self._detect_rotation_regime(results)
        )
    
    def _detect_rotation_regime(self, signals) -> str:
        """
        判断当前是否处于明显轮动环境
        """
        leading = [t for t, s in signals.items() if "leading" in s.classification]
        lagging = [t for t, s in signals.items() if "lagging" in s.classification]
        
        # 分化明显 = 轮动
        if len(leading) >= 2 and len(lagging) >= 2:
            return "active_rotation"
        # 大家都跌 = 系统性回调
        elif len(lagging) >= len(signals) * 0.7:
            return "systemic_selloff"
        else:
            return "mixed"
```

**接入 evidence_bundle：**

```python
# evidence_bundle 新增字段
"sector_momentum": {
  "rotation_regime": "active_rotation",
  "signals": {
    "XLK": {"classification": "leading", "signal_strength": 0.045},
    "XLE": {"classification": "mild_leading", "signal_strength": 0.012},
    "XLU": {"classification": "lagging", "signal_strength": -0.038},
    "SOXX": {"classification": "lagging", "signal_strength": -0.052},
  },
  "calculated_at": "...",
  "lookback_short_days": 20,
  "lookback_long_days": 60
}
```

### Task 4.2 Thesis Status 主动更新机制

**当前问题：** thesis_status 只在 LLM propose 时才更新，亏损仓位可能数天不更新

**设计：** 定期触发 thesis review，而不是被动等待

```python
class ThesisReviewScheduler:
    """
    决定哪些仓位需要主动触发 thesis review
    这是 deterministic 的调度逻辑
    """
    
    REVIEW_TRIGGERS = {
        "loss_review":         True,   # 总是需要 review
        "basket_loss_review":  True,
        "hard_risk_review":    True,
        "normal_hold": {
            "days_since_review": 5,    # 正常仓位 5 天 review 一次
            "pnl_change_threshold": 0.03,  # PnL 变化 > 3% 触发
        }
    }
    
    def get_review_required(
        self,
        ticker: str,
        position_state: str,
        last_thesis_review: datetime | None,
        pnl_change_since_review: float,
    ) -> bool:
        
        trigger = self.REVIEW_TRIGGERS.get(position_state)
        
        if trigger is True:
            return True
        
        if isinstance(trigger, dict):
            days_elapsed = (datetime.utcnow() - last_thesis_review).days
            if days_elapsed >= trigger["days_since_review"]:
                return True
            if abs(pnl_change_since_review) >= trigger["pnl_change_threshold"]:
                return True
        
        return False
```

**Thesis Review 结构（LLM 输入格式）：**

```python
# 给 Researcher agent 的结构化 prompt 输入
{
  "review_purpose": "thesis_review",
  "ticker": "FTXL",
  "current_state": {
    "position_state": "loss_review",
    "unrealized_pnl": -0.066,
    "holding_days": 18,
    "last_thesis_status": "weakening",
    "last_review_at": "2026-05-15"
  },
  "evidence": {
    "sector_momentum": {"classification": "lagging", "signal": -0.052},
    "basket_review": {"group": "semiconductors", "n_losers": 3},
    "news": {"bias": "negative", "hard_risk": false},
    "strategy_support": "advisory",
    "yfinance_relative_strength": "negative_20d"
  },
  "question": "Is the original thesis for holding FTXL still intact, 
               weakening, or broken? What specific evidence supports 
               your assessment?"
}
```

**LLM 输出（有界）：**

```python
{
  "ticker": "FTXL",
  "thesis_status": "weakening",    # intact / weakening / broken / unknown
  "confidence": 0.71,
  "key_evidence": [
    "semiconductor basket has 3 correlated losers",
    "sector momentum lagging SPY by -5.2% over 20d",
    "strategy support remains advisory, not primary"
  ],
  "thesis_change_from_last": "no_change",  # deteriorated / improved / no_change
  "recommended_review": "manual_trim_review",
  "execution_authority": "none"  # 必须是 none
}
```

### Task 4.3 Walk-forward Strategy Validation

参考 2025 最新论文的 walk-forward validation 方法：

```python
class WalkForwardValidator:
    """
    对 Playground 策略做滚动窗口验证
    避免过拟合，提高 strategy_support 的可信度
    
    参考：
    - "Interpretable Hypothesis-Driven Trading" (Dec 2025)
    - 使用 34 个独立测试区间的滚动验证
    """
    
    def validate(
        self,
        strategy_name: str,
        train_window: int = 252,  # 1 年训练
        test_window: int = 21,    # 1 个月测试
        n_periods: int = 12,      # 12 个滚动窗口
    ) -> WalkForwardResult:
        
        results = []
        for i in range(n_periods):
            train_end = today - timedelta(days=i * test_window)
            train_start = train_end - timedelta(days=train_window)
            test_end = train_end + timedelta(days=test_window)
            
            # 用训练数据校准策略
            calibrated = self._calibrate(strategy_name, train_start, train_end)
            
            # 在测试数据上评估（严格 out-of-sample）
            result = self._evaluate(calibrated, train_end, test_end)
            results.append(result)
        
        # 聚合指标
        hit_rates = [r.hit_rate for r in results]
        sharpes = [r.sharpe for r in results]
        
        return WalkForwardResult(
            n_periods=n_periods,
            avg_hit_rate=mean(hit_rates),
            hit_rate_stability=stdev(hit_rates),  # 越低越稳定
            avg_sharpe=mean(sharpes),
            periods_positive=sum(1 for s in sharpes if s > 0),
            # 用于 strategy certification
            certification_eligible=(
                mean(hit_rates) > 0.52 and
                stdev(hit_rates) < 0.15 and
                sum(1 for s in sharpes if s > 0) >= n_periods * 0.67
            )
        )
```

---

## Sprint 5：执行质量提升（2-3 周）

**目标：** 减少 FULL_AUTO 场景下的执行摩擦，提高实际成交质量。

### Task 5.1 Smart Order Routing

```python
class SmartOrderRouter:
    """
    根据 delta 大小、ATR、时段选择最优执行方式
    参考机构级执行算法的简化版
    """
    
    def route(
        self,
        ticker: str,
        delta_weight: float,
        portfolio_value: float,
        live_atr: float,
        time_of_day: str,  # "open" / "midday" / "close"
    ) -> OrderSpec:
        
        delta_value = abs(delta_weight) * portfolio_value
        
        # 小单：市价立即成交
        if delta_value < SMALL_ORDER_THRESHOLD:
            return OrderSpec(type="MARKET", timing="immediate")
        
        # 高 ATR 时段：用 TWAP 分散
        if live_atr > ATR_HIGH_THRESHOLD:
            return OrderSpec(
                type="TWAP",
                duration_minutes=30,
                max_participation_rate=0.10
            )
        
        # 开盘/收盘避免：用 limit 单
        if time_of_day in ["open", "close"]:
            slippage = live_atr * 0.5  # 用 ATR 的一半作为 limit 缓冲
            return OrderSpec(
                type="LIMIT",
                limit_offset=slippage,
                timing="15min_after_open" if time_of_day == "open" else "30min_before_close"
            )
        
        # 正常时段：limit 单
        return OrderSpec(
            type="LIMIT",
            limit_offset=live_atr * 0.3,
            timing="immediate"
        )
```

### Task 5.2 Execution Quality Tracking

```python
# execution_audit 新增字段
{
  "implementation_shortfall": {
    "decision_price": 45.23,      # 决策时的价格
    "fill_price": 45.31,          # 实际成交价格
    "slippage_bps": 17.7,         # 滑点（基点）
    "market_impact_bps": 12.0,    # 市场冲击
    "timing_cost_bps": 5.7,       # 时机成本
  },
  "fill_quality": "good",         # good / acceptable / poor
  "partial_fill_pct": 1.0,        # 1.0 = 完全成交
}
```

---

## Sprint 6：系统性能基准和回测（持续）

**目标：** 建立系统级别的 performance attribution，知道每个组件的贡献。

### Task 6.1 Performance Attribution

参考机构级 performance attribution 框架：

```python
class PerformanceAttribution:
    """
    分解 portfolio return 的来源
    
    total_return = alpha_contribution
                 + factor_contribution  
                 + execution_cost
                 + cash_drag
    """
    
    def attribute(
        self,
        period_start: date,
        period_end: date,
    ) -> AttributionResult:
        
        # 1. 策略 alpha 贡献（来自 Playground 策略的选股）
        alpha_contrib = self._calc_alpha_contribution(period_start, period_end)
        
        # 2. 因子暴露贡献（来自 sector/factor tilts）
        factor_contrib = self._calc_factor_contribution(period_start, period_end)
        
        # 3. 执行成本（滑点 + 佣金 + 市场冲击）
        execution_cost = self._calc_execution_cost(period_start, period_end)
        
        # 4. LLM advisory 的贡献（accepted advisories 的边际效果）
        advisory_contrib = self._calc_advisory_contribution(period_start, period_end)
        
        return AttributionResult(
            alpha_contribution=alpha_contrib,
            factor_contribution=factor_contrib,
            execution_cost=execution_cost,
            advisory_contribution=advisory_contrib,
            total_return=alpha_contrib + factor_contrib - execution_cost + advisory_contrib,
        )
```

### Task 6.2 System-level Backtest

```python
class SystemBacktest:
    """
    对整个 pipeline 做回测，而不只是 strategy
    包括：治理规则、turnover 限制、basket review 的效果
    """
    
    def run(
        self,
        start: date,
        end: date,
        config: SystemConfig,
    ) -> BacktestResult:
        
        for trading_day in trading_days(start, end):
            # 模拟完整 pipeline
            evidence = self._build_evidence(trading_day)
            scorecard = self._run_scorecard(evidence)
            governance = self._run_governance(evidence, scorecard)
            targets = self._run_target_builder(governance)
            risk_result = self._run_risk_check(targets)
            
            if risk_result.approved:
                self._simulate_execution(targets, trading_day)
        
        return BacktestResult(
            sharpe=self._calc_sharpe(),
            max_drawdown=self._calc_max_drawdown(),
            turnover_avg=self._calc_avg_turnover(),
            governance_block_rate=self._calc_block_rate(),
            advisory_acceptance_rate=self._calc_advisory_rate(),
        )
```

---

## 四、优先级总览

```
Sprint 1（1-2 周）   修复 explanation、basket review、pre-flight、Telegram
Sprint 2（2-3 周）   完成 target_builder 迁移到 gated mode
Sprint 3（3-4 周）   Portfolio Construction Model（组合层面构建）
Sprint 4（3-4 周）   Sector Momentum 信号 + Thesis 主动更新
Sprint 5（2-3 周）   执行质量提升
Sprint 6（持续）     Performance Attribution + System Backtest
```

---

## 五、与行业标准的对齐检查

完成以上 Sprint 后，系统应该对齐到：

| 行业标准层 | 对应实现 | 状态 |
|-----------|---------|------|
| Universe Selection | Playground 候选 + Scorecard 筛选 | ✅ |
| Alpha Model | Researcher LLM + Sector Momentum + Walk-forward | ✅ |
| Portfolio Construction | PortfolioConstructionModel（Sprint 3） | Sprint 3 |
| Risk Overlay | Risk Manager + Position Governance | ✅ |
| Execution | SmartOrderRouter（Sprint 5） | Sprint 5 |
| Post-trade Analytics | Performance Attribution（Sprint 6） | Sprint 6 |

**与 FinRL-X weight-centric 接口对齐：**

```
S（selection）  = Playground 候选 ∩ Scorecard 允许
A（allocation） = PortfolioConstructionModel 输出
T（timing）     = SmartOrderRouter 决策
R（risk）       = Risk Manager 验证

weight = R(T(A(S(universe))))
LLM 影响 A 层（通过 validated advisory delta），
不改变其他层的语义。
```

---

## 六、不做的事

```
❌ 引入 CVXPY 或其他复杂优化库
   - 稳定性风险高，调试困难
   - 用分层启发式方法替代

❌ 让 LLM 直接输出 portfolio weights
   - 这是已经完成的迁移的目标，不回退

❌ 新增第三方数据源（Bloomberg、Refinitiv）
   - 当前 QC + yfinance + news 已经足够
   - 先把现有数据用好

❌ 高频交易或日内 alpha
   - 当前执行框架是日频，不适合日内策略

❌ 强化学习替代当前 governance
   - RL 需要大量样本和环境模拟
   - 当前样本量不足以稳定训练

❌ 把 advisory quality backfill 用于放宽执行约束
   - 等 counterfactual model 成熟后再考虑
```

---

## 七、验收标准（Sprint 3 完成后的系统状态）

```
Portfolio Construction：
  ✓ tech_growth 组内总暴露受因子限制约束
  ✓ basket_review 时组内总权重自动降低
  ✓ turnover 预算分配优先信号强的 ticker
  ✓ 同样输入，多次运行结果完全一致

Sector Momentum：
  ✓ evidence_bundle 包含 sector_momentum 字段
  ✓ rotation_regime 能区分 active_rotation 和 systemic_selloff
  ✓ lagging 板块 ticker 加入 trim_review 候选

Thesis Management：
  ✓ loss_review 仓位每日触发 thesis review
  ✓ thesis_status 有明确的 owner 和更新时间戳
  ✓ LLM thesis proposal 不能直接修改 action_permission

整体系统：
  ✓ 大 proposal 被 risk 整体拒绝的频率下降
  ✓ basket 内亏损仓位有组合级解释而非相同模板
  ✓ 每轮 Telegram 清楚显示 proposed vs final
  ✓ performance attribution 有第一个版本的输出
```

---

*版本：2026-05-20*
*基础文档：system_design_handbook.md*
*参考：QC Algorithm Framework, FinRL-X, TradingAgents, 机构级 quant 实践*