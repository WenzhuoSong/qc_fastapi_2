# AI Trading System Design
# LLM 与 Deterministic 层的分工架构

> 本文档面向开发 Agent 分析使用。
> 核心命题：LLM 负责语义判断，Deterministic Function 负责约束执行。
> 两者不可互换，边界必须在代码层面强制执行。

---

## 一、核心设计原则

### 1.1 LLM 的能力边界

LLM 可靠的前提是：**提供足够的结构化信息，并且只让它做适合它做的事。**

| 判断类型 | 适合 LLM | 原因 |
|----------|----------|------|
| 解释为什么持有某仓位 | ✅ | 语义任务，允许合理差异 |
| 识别 thesis 是否弱化 | ✅ | 语义判断，结合多源证据 |
| 综合多源证据写研究摘要 | ✅ | LLM 核心能力 |
| 判断具体 trim 多少 % | ⚠️ 部分 | 可以 propose，但必须经过 validator clip |
| 判断是否超过 turnover cap | ❌ | 数学约束，必须是代码 |
| 保证 reason_codes 不自相矛盾 | ❌ | 需要结构化生成，不能靠 prompt |
| 决定 final_action | ❌ | 必须 deterministic，跨运行一致 |

### 1.2 判断由谁来做的测试

```
测试 1：一致性测试
  同样输入运行两次，结果必须完全一致 -> Deterministic Function
  结果可以有合理差异               -> LLM 适合

测试 2：风险测试
  判断出错会直接影响执行或误导用户  -> Deterministic Function
  判断出错只影响解释质量           -> LLM 可以承担（但要有 fallback）
```

### 1.3 最终分工边界

```
LLM 负责（语义层）：
  - 读取 evidence，输出 thesis_status 判断
  - 生成 advisory proposal（结构化格式）
  - 写 research summary 和解释文字
  - 识别跨 ticker 的主题关联

Deterministic Function 负责（约束层）：
  - 计算 position_state（状态机）
  - 决定 action_permission
  - 验证 LLM proposal 是否在允许范围内
  - 生成 reason_codes
  - 把 reason_codes 映射到解释结构
  - 检查 turnover / delta / basket exposure
  - 写 audit trail
  - 计算 final_target_weight
  - 计算价格和数量

LLM 绝对不能做：
  - 决定 final_action
  - 绕过任何 deterministic check
  - 生成 reason_codes（只能消费）
  - 独立判断某个约束是否适用
  - 计算订单数量和价格
```

---

## 二、整体架构层次

```
Layer 0: 数据采集与存储
Layer 1: 证据结构化（evidence_bundle）
Layer 2: 信号生成与校准
Layer 3: LLM 研究与提案生成
Layer 4: 提案塑形（Proposal Shaping）
Layer 5: 风险与治理（Deterministic）
Layer 6: 执行与审计
Layer 7: 反馈与记忆
Layer 8: 通知与可观测性
```

**核心约束：每一层只向下一层传递结构化输出，不允许跨层直接影响执行。**

完整数据流：

```
raw data (QC / yfinance / news / base knowledge)
  -> evidence_bundle
  -> market_scorecard / knowledge_resolver / strategy_confidence
  -> researcher + synthesizer (LLM)
  -> proposal_shaper (deterministic pre-flight)
  -> risk_manager (deterministic)
  -> position_governance (deterministic)
  -> decision_ledger (aggregation + audit)
  -> position_manager (execution gate)
  -> executor -> execution_audit
  -> daily_memory / analyst_review
  -> communicator (Telegram)
```

---

## 三、各层详细设计

### Layer 0：数据采集与存储

**三类数据严格分开存储：**

| 类型 | 内容 | 时效 | 存储 |
|------|------|------|------|
| 实时/日内 | 价格、持仓、ATR、heartbeat | 分钟级 | 时序数据库或内存缓存 |
| 日频历史 | OHLCV、factor features、forward return | 每日 EOD | PostgreSQL |
| 静态知识 | ETF 基本面、策略假设、风控原则 | 手动维护 | YAML + 版本控制 |

**关键设计约束：**

```python
# 每个数据字段必须携带三个元数据字段
{
  "value": ...,
  "source": "qc_intraday",
  "as_of": "2026-05-19T13:30:00Z",
  "is_stale": false
}
```

- 实时数据缺失不能用历史数据静默替代，必须显式标注 `missing`
- heartbeat 新鲜 ≠ live evidence 充分，必须分层标注
- QC heartbeat / daily snapshot / live fit 三者独立标注，不能合并成一个 `data=limited`

---

### Layer 1：证据结构化（evidence_bundle）

**四类信息源的职责划分：**

```
QC 实时数据    -> 现在发生什么、现在能不能交易
yfinance 历史  -> 历史上这类信号靠不靠谱
base knowledge -> 这个 ETF/策略本质上是什么（常识边界）
news evidence  -> 外部事件是否改变了 thesis 或风险条件
```

**base knowledge 的核心价值：**
- 告诉系统 ETF 是 core / satellite / thematic / leveraged
- 告诉系统哪些 ETF 属于同一个 basket（SOXX/PSI/FTXL = semiconductor basket）
- 提供策略适用的 regime 条件
- 提供风控原则（high ATR no-add、leveraged ETF caution）
- 这是给 LLM 的"常识参考"，让它不会把高相关 ETF 当作独立风险

**evidence_bundle 输出结构：**

```python
{
  "market": {...},
  "news_evidence": {
    "bias": "negative",
    "confidence": "high",
    "hard_risk_tickers": ["XLRE", "XLU"],
    "source_state": "fresh"
  },
  "strategy": {...},
  "knowledge": {
    "assets": {...},      # 来自 base knowledge YAML
    "regimes": {...},
    "risk_principles": {...}
  },
  "positions": {...},
  "data_quality": {
    "qc_heartbeat": "fresh",
    "daily_snapshot": "stale_3d",
    "yfinance_history": "strong_290samples",
    "live_fit": "insufficient_7samples",
    "news_cache": "fresh"
  }
}
```

**Freshness 语义约束：**

```
每个信息源自己持有 freshness_policy，ledger 只透传不重新判断
news:     stale if > 4h
yfinance: stale if > 1d
QC live:  stale if > 30min

共同 schema：
{
  "source": "qc_intraday",
  "as_of": "...",
  "evaluated_at": "...",
  "is_stale": false,
  "state": "fresh",
  "policy": "qc_intraday_30m"
}
```

---

### Layer 2：信号生成与校准

#### 2.1 Strategy Playground（历史信号，deterministic）

```
输入：yfinance 历史数据
输出：
  - historical_evidence（Sharpe / hit_rate / drawdown / samples）
  - suggested_use: primary / advisory / watch_only / disabled
  - confidence: float
  - certification_status: experimental / research_supported / advisory

规则：
  - primary/advisory 可以影响 proposal
  - watch_only 不能创建 add
  - disabled 不能影响任何分配
```

#### 2.2 Market Scorecard（deterministic）

```
输入：news_evidence + strategy_confidence + regime
输出：market_permission

权限等级（从宽到严）：
  full_auto
  small_overweight_only
  human_required
  hold_or_trim
  defensive_only
  cash_only
  reduce_risk_only

规则：
  - scorecard 是 deterministic，不依赖 LLM
  - 一旦设定，下游只能在此范围内操作，不能放宽
```

#### 2.3 Knowledge Resolver + Confidence Calibrator（deterministic）

```
Resolver 输出：
  - advisory_context    -> 给 LLM agent（解释上下文）
  - hard_constraints    -> 给 deterministic 层（执行约束）
  - conflicts           -> 可观测性（regime 冲突、live 不足等）
  - missing_knowledge   -> 分级处理（info / warning / blocking）

Calibrator 规则：
  - 是唯一允许修改 strategy confidence 的模块
  - 每次 pipeline 只能修改一次
  - 其他所有模块消费 post-calibration confidence，不能自己调整
```

---

### Layer 3：LLM 研究与提案生成

#### 3.1 Researcher Agent

```
输入：evidence_bundle（含 base knowledge + news + history + QC）
输出：per-ticker 研究摘要（结构化）

输出格式：
{
  "ticker": "FTXL",
  "thesis_assessment": "semiconductor thesis weakening due to...",
  "supporting_evidence": ["news_negative", "basket_loss_review"],
  "conflicting_evidence": ["strategy_advisory_still_active"],
  "confidence": 0.65
}

约束：
  - 不输出 action 建议
  - 不输出 weight 建议
  - 只输出 thesis 判断和证据摘要
```

#### 3.2 Synthesizer Agent

```
输入：researcher 输出 + evidence_bundle
输出：advisory_proposals（结构化，可选）

输出格式（LLM 唯一被允许的决策输出格式）：
{
  "ticker": "FTXL",
  "llm_advisory": "trim",        # hold / trim / exit / add
  "target_weight": 0.03,         # LLM 建议的目标权重
  "confidence": 0.62,
  "thesis_status": "weakening",  # intact / weakening / broken / unknown
  "reason": "semiconductor thesis weakening, live consensus defensive"
}

约束：
  - LLM 只能 propose thesis_status，不能执行
  - target_weight 是建议值，会被 validator clip
  - 散文解释是附属品，不是决策输入
  - 没有 proposal 也可以，系统继续 deterministic 运行
```

---

### Layer 4：提案塑形（Proposal Shaping，deterministic）

这一层在 LLM proposal 进入 risk manager 之前做 **pre-flight check**，减少无效提案。

#### 4.1 核心检查函数

```python
def is_add_allowed(ticker: str, governance_state: PositionState, 
                   scorecard: Scorecard) -> bool:
    """
    在生成 add proposal 之前必须通过此检查
    任何一项 False 直接拒绝，不进入 LLM proposal 流程
    """
    if governance_state.position_state == "loss_review":
        return False  # 亏损 review 禁止 add
    
    if governance_state.basket_review:
        return False  # basket 联合亏损禁止 add
    
    if governance_state.atr_high:
        return False  # 高波动禁止 add
    
    if scorecard.permission in ["hold_or_trim", "defensive_only", 
                                "cash_only", "reduce_risk_only"]:
        return False  # scorecard 限制
    
    if governance_state.group_exposure >= governance_state.group_limit:
        return False  # 板块暴露超限
    
    return True


def estimate_turnover_budget(proposed_weights: dict, 
                             current_weights: dict,
                             limit: float) -> TurnoverCheck:
    """
    在 proposal 进入 risk manager 之前做 turnover 预估
    超限就在此层裁掉，不等 risk manager 整体拒绝
    """
    estimated_turnover = sum(
        abs(proposed_weights.get(t, 0) - current_weights.get(t, 0))
        for t in set(proposed_weights) | set(current_weights)
    ) / 2
    
    return TurnoverCheck(
        estimated=estimated_turnover,
        limit=limit,
        within_budget=estimated_turnover <= limit,
        excess=max(0, estimated_turnover - limit)
    )
```

#### 4.2 动态约束调整

```python
def get_effective_constraints(scorecard: Scorecard, 
                              data_quality: DataQuality) -> Constraints:
    """
    根据市场状态动态调整约束上限
    bull + data_limited 时自动压低 turnover
    """
    base_turnover_limit = 0.10
    base_max_delta = 0.02
    
    # human_required 时压低 turnover
    if scorecard.human_required:
        base_turnover_limit *= 0.5
    
    # live fit 不足时压低 delta
    if data_quality.live_fit == "insufficient":
        base_max_delta *= 0.5
    
    # data_limited 时额外保守
    if data_quality.overall == "limited":
        base_turnover_limit *= 0.7
    
    return Constraints(
        turnover_limit=base_turnover_limit,
        max_single_delta=base_max_delta
    )
```

---

### Layer 5：风险与治理（完全 Deterministic）

#### 5.1 Risk Manager

```python
def check_risk(proposed_weights: dict, current_weights: dict,
               scorecard: Scorecard, constraints: Constraints) -> RiskResult:
    
    checks = []
    
    # 1. Turnover check
    turnover = calculate_turnover(proposed_weights, current_weights)
    checks.append(Check("turnover", turnover <= constraints.turnover_limit,
                        f"{turnover:.1%} vs limit {constraints.turnover_limit:.1%}"))
    
    # 2. Style limit check
    # 3. Equity exposure check
    # 4. Max single delta check
    
    approved = all(c.passed for c in checks)
    
    return RiskResult(
        approved=approved,
        checks=checks,
        # approved=False 时强制 diagnostic_only，不允许执行
    )
```

#### 5.2 Position Governance（核心状态机）

**Position State 枚举（完全 deterministic）：**

```python
def get_position_state(ticker: str, pnl: float, atr: float,
                       support: str, basket_review: bool,
                       news_hard_risk: bool, asset_type: str) -> PositionState:
    """
    状态机：输入确定，输出确定，跨运行一致
    这是整个系统最重要的 deterministic function 之一
    """
    if news_hard_risk:
        return PositionState.HARD_RISK_REVIEW
    
    # satellite/thematic ETF 使用更严格的阈值
    loss_review_threshold = -0.04
    trim_threshold = -0.08
    if asset_type in ["satellite", "thematic", "leveraged"]:
        loss_review_threshold = -0.04  # 同等起点
        # 但 basket_review 时更快升级
    
    if pnl <= trim_threshold and support in ["weak", "none"]:
        return PositionState.TRIM_CANDIDATE
    
    if basket_review and pnl <= loss_review_threshold and support == "advisory":
        return PositionState.BASKET_LOSS_REVIEW  # 升级为组合级 review
    
    if pnl <= loss_review_threshold:
        return PositionState.LOSS_REVIEW
    
    if atr > ATR_HIGH_THRESHOLD:
        return PositionState.HIGH_ATR_REVIEW
    
    return PositionState.NORMAL_HOLD
```

**Action Permission（deterministic）：**

```python
PERMISSION_MAP = {
    PositionState.HARD_RISK_REVIEW:    ["trim", "exit"],      # trim_or_exit
    PositionState.BASKET_LOSS_REVIEW:  ["hold", "trim"],      # no add
    PositionState.LOSS_REVIEW:         ["hold", "trim"],      # no add
    PositionState.TRIM_CANDIDATE:      ["trim", "exit"],
    PositionState.HIGH_ATR_REVIEW:     ["hold"],              # no add, no exit
    PositionState.NORMAL_HOLD:         ["hold", "add", "trim"],
}
```

**Explanation 生成（从状态机导出，不是模板）：**

```python
def build_explanation(position_state: PositionState,
                      reason_codes: list[str],
                      action_permission: list[str],
                      basket_context: dict | None) -> Explanation:
    """
    关键设计约束：
    - explanation 必须从 position_state 和 reason_codes 严格导出
    - 不允许 fallback 到低 severity 的通用文案
    - Severity 优先级：
      hard_risk > basket_loss > deep_loss > high_atr > concentration > weak_support > normal
    """
    
    # 确定 primary reason（按 severity 排序取最高）
    primary = get_highest_severity_reason(reason_codes)
    
    why_hold = []
    why_not_add = []
    why_not_exit = []
    
    # hard_risk 路径：不能 fallback 到其他模板
    if position_state == PositionState.HARD_RISK_REVIEW:
        why_hold = [
            "hard-risk event is active",
            "automatic execution is blocked by human confirmation requirement",
            "manual trim/exit review is required"
        ]
        why_not_exit = ["exit requires explicit manual confirmation"]
        # 禁止出现 "no deterministic rule requires reduction"
    
    # basket_loss 路径
    elif position_state == PositionState.BASKET_LOSS_REVIEW:
        basket_name = basket_context.get("group", "correlated basket")
        n_losers = basket_context.get("n_loss_review", 0)
        why_hold = [
            f"{basket_name} basket has {n_losers} correlated loss-review positions",
            "add is blocked; trim review is elevated for basket risk"
        ]
        why_not_add = [
            "basket-level loss review blocks all adds within this group"
        ]
    
    # loss_review + advisory 路径（更诚实的措辞）
    elif position_state == PositionState.LOSS_REVIEW:
        why_hold = ["loss is within hold-review threshold"]
        why_not_add = [
            "position is in unrealized loss review",
            "only advisory support remains; this is not strong enough to justify adding"
            # 不能说 "strategy support remains advisory" 暗示支持充足
        ]
    
    # ... 其他路径
    
    next_trigger = build_next_trigger(position_state, reason_codes)
    
    return Explanation(
        why_hold=why_hold,
        why_not_add=why_not_add,
        why_not_exit=why_not_exit,
        next_trigger=next_trigger
    )
```

**LLM Advisory Validator（deterministic）：**

```python
def validate_advisory(proposal: LLMProposal,
                      governance_state: GovernanceState,
                      scorecard: Scorecard,
                      constraints: Constraints) -> ValidationResult:
    
    # 1. action 必须在 deterministic 允许范围内
    if proposal.llm_advisory not in governance_state.allowed_actions:
        return reject(proposal, "outside_action_permission")
    
    # 2. delta clip
    max_delta = constraints.max_single_delta
    actual_delta = abs(proposal.target_weight - governance_state.current_weight)
    if actual_delta > max_delta:
        proposal = clip_to_max_delta(proposal, max_delta)
        result_type = "accepted_clipped"
    else:
        result_type = "accepted"
    
    # 3. human_required 下不允许增加风险
    if scorecard.human_required and proposal.increases_risk():
        return reject(proposal, "human_required_blocks_risk_increase")
    
    # 4. add advisory 需要 primary/advisory 策略支持
    if proposal.llm_advisory == "add":
        if governance_state.strategy_support not in ["primary", "advisory"]:
            return reject(proposal, "insufficient_strategy_support_for_add")
    
    # 5. exit advisory 没有 hard_risk 时转换为 hold_review
    if proposal.llm_advisory == "exit":
        if not governance_state.hard_risk and not governance_state.exit_trigger:
            return convert(proposal, "hold_review", "unsupported_exit_converted")
    
    # 记录每一个 accept/reject/convert 决策
    log_advisory_decision(proposal, result_type)
    
    return ValidationResult(result=result_type, final_proposal=proposal)
```

**Basket Review 检测（portfolio 层）：**

```python
def detect_basket_reviews(position_decisions: list[PositionDecision],
                          knowledge_base: KnowledgeBase) -> dict[str, BasketReview]:
    """
    触发条件：同一 group 内 >= 2 个 ticker 处于 loss_review
    结果：触发 group_cluster_loss_review，升级所有 basket 内 ticker 的处理
    """
    group_loss_counts = defaultdict(list)
    
    for decision in position_decisions:
        if decision.position_state in ["loss_review", "hard_risk_review"]:
            group = knowledge_base.get_group(decision.ticker)
            group_loss_counts[group].append(decision.ticker)
    
    basket_reviews = {}
    for group, tickers in group_loss_counts.items():
        if len(tickers) >= 2:  # 触发条件
            basket_reviews[group] = BasketReview(
                group=group,
                loss_tickers=tickers,
                n_loss_review=len(tickers),
                severity="elevated"
            )
    
    return basket_reviews
```

---

### Layer 6：执行与审计

#### 6.1 从 LLM 输出到最终订单的完整转化

**这是回答"买卖多少、价格"的核心链路，完全 deterministic：**

```python
def calculate_final_order(ticker: str,
                          current_weight: float,
                          final_target_weight: float,
                          portfolio_value: float,
                          live_price: float) -> Order:
    """
    LLM 不参与此函数
    输入来自 position_governance 的 final_target_weight
    """
    
    # 数量计算
    target_value = final_target_weight * portfolio_value
    current_value = current_weight * portfolio_value
    delta_value = target_value - current_value
    quantity = int(delta_value / live_price)  # 向下取整
    
    if quantity == 0:
        return Order.noop(ticker, reason="delta_too_small")
    
    # 价格策略
    direction = "BUY" if quantity > 0 else "SELL"
    
    if abs(delta_value) < SMALL_ORDER_THRESHOLD:
        order_type = "MARKET"
        limit_price = None
    else:
        order_type = "LIMIT"
        slippage = SLIPPAGE_BUY if direction == "BUY" else -SLIPPAGE_SELL
        limit_price = round(live_price * (1 + slippage), 2)
    
    return Order(
        ticker=ticker,
        direction=direction,
        quantity=abs(quantity),
        order_type=order_type,
        limit_price=limit_price,
        source_weight=final_target_weight
    )


def calculate_final_target_weight(ticker: str,
                                  base_weight: float,
                                  governance_adjustment: float,
                                  validated_llm_delta: float,
                                  constraints: Constraints) -> float:
    """
    Target weight 的形成：三项相加，然后 clip
    LLM 的影响只体现在 validated_llm_delta（已经被 validator clip 过）
    """
    raw_target = base_weight + governance_adjustment + validated_llm_delta
    
    return clip(
        raw_target,
        min_val=0.0,
        max_val=constraints.max_single_position
    )
```

**买卖决策的职责归属：**

```
买谁？
  Playground 策略选出候选
  -> Scorecard 筛掉不符合市场权限的
  -> Governance 筛掉 loss_review / basket_review / hard_risk 的
  -> LLM 可以影响：候选的优先级（通过 confidence 调整）
  -> 代码决定：最终 ticker 列表

卖谁？
  Position Governance 决定哪些 ticker 进入 trim / exit 路径
  -> LLM 可以影响：thesis_status = weakening/broken（触发 trim 升级）
  -> 代码决定：是否真的 trim，trim 到多少

买卖多少？
  完全由代码决定
  LLM 的 target_weight 建议会被 validator clip，不直接使用

价格？
  完全由代码决定
  基于 QC 实时价格 + 执行策略参数

时机？
  Position Manager 决定（日内限制、ATR 过高暂停等）
  LLM 不参与
```

#### 6.2 Execution Audit（防误报）

```python
# Telegram 只能说"已执行"当且仅当 execution_audit 确认 filled
EXECUTION_STATUS_HIERARCHY = [
    "proposed",    # 系统内部提案
    "sent",        # 已发送到 broker/QC
    "accepted",    # broker 接受
    "filled",      # 实际成交
]

# 四个状态必须分开记录，不能合并
{
  "proposed_action": "trim",
  "sent": true,
  "accepted": true,
  "filled": false,        # partial fill
  "filled_quantity": 0,
  "fill_price": null,
  "execution_status": "sent_not_filled"
}
```

---

### Layer 7：Decision Ledger（审计核心）

**每个 ticker 必须能回答：**

```python
{
  "ticker": "FTXL",
  
  "trade_lifecycle": {
    "base_weight": 0.045,          # Playground 建议
    "strategy_target": 0.040,      # 策略加权后
    "synthesizer_target": 0.030,   # LLM 建议（clip 前）
    "risk_clipped_target": 0.045,  # risk manager clip 后
    "governance_target": 0.030,    # governance 调整后
    "final_target": 0.030          # 最终执行目标
  },
  
  "source_effects": {
    "qc":        ["unrealized_loss_review", "high_atr"],
    "yfinance":  ["empirical_profile_available"],
    "knowledge": ["basket_review", "satellite_loss_threshold"],
    "news":      ["hard_risk"],
    "scorecard": ["human_required"],
    "risk":      ["risk_rejected"]
  },
  
  "proposed_action": "trim",
  "final_action": "none",
  "execution_status": "not_sent",
  
  "reason_codes": ["basket_review", "unrealized_loss_review", "human_required"],
  
  "explanation": {
    "why_hold": [...],       # 来自 governance explanation function
    "why_not_add": [...],    # 来自 governance explanation function
    "why_not_exit": [...],
    "next_trigger": "..."
  },
  
  "governance_available": true  # false 时不猜测，只报 warning
}
```

**Ledger 的边界约束：**
- Ledger 是聚合层，不是决策层
- `position_governance.position_decisions` 是 hold/trim/add 的唯一来源
- Ledger 不重新计算任何 governance 决策
- 缺少 governance 输出时报 `position_governance_missing` warning，不猜测

---

### Layer 8：通知与可观测性（Telegram）

**格式约束：**

```
Decision evidence
  News: negative/high | hard risk: XLRE, XLU
  QC heartbeat: fresh
  Daily snapshot: stale (3d ago)
  Live fit: insufficient (7 samples)
  yfinance: strong (290 samples)
  Strategy: advisory only, no primary
  Scorecard: human_required
  Final: no execution this round

Top decisions（最多显示 5 条，按 severity 排序）
  XLRE: hard_risk_review | manual trim/exit review required
  FTXL: basket_loss_review | semiconductors basket | advisory=weak-positive | no add
  XLK: trim -> none | risk_rejected, human_required
  PSI: loss_review | -7.0% | hold_review, no add
  QQQ: hold | winner, advisory support
```

**严格约束：**
- 区分 `proposed action` 和 `final action`
- 区分 `risk rejected` 和 `execution failed`
- 数据质量显示必须分源，不能合并成 `data=limited`
- 完整 ledger 存 step log，不全量打印到 Telegram
- Telegram 不能说"已执行"，除非 execution_audit 确认 filled

---

## 四、板块轮动分析框架

### 4.1 信息源各自的轮动分析能力

```
yfinance 历史
  -> 计算各板块 ETF 相对 SPY 的 20日/60日 relative strength
  -> 识别 leading / neutral / lagging 板块
  -> 历史轮动规律（哪些板块在类似 regime 下领涨）

QC 实时
  -> 当前各板块的 ATR 和日内动量
  -> 当前组合板块暴露分布

base knowledge
  -> 哪些板块在 risk-off 通常防御（XLU、XLP、TLT）
  -> 哪些是高 beta（半导体、thematic）
  -> 板块间的历史相关性

news
  -> 判断当前是"板块轮动"还是"系统性回调"
  -> 识别板块特定风险事件
```

### 4.2 轮动信号的 deterministic 计算

```python
def calculate_sector_momentum(tickers: list[str],
                              lookback_days: int = 20) -> dict[str, str]:
    """
    用 yfinance 计算各板块相对 SPY 的 relative strength
    输出：leading / neutral / lagging
    这个计算是 deterministic 的，不依赖 LLM
    """
    spy_return = get_return(["SPY"], lookback_days)["SPY"]
    
    result = {}
    for ticker in tickers:
        ticker_return = get_return([ticker], lookback_days)[ticker]
        relative = ticker_return - spy_return
        
        if relative > LEADING_THRESHOLD:
            result[ticker] = "leading"
        elif relative < LAGGING_THRESHOLD:
            result[ticker] = "lagging"
        else:
            result[ticker] = "neutral"
    
    return result
```

### 4.3 区分轮动与系统性回调

```
判断逻辑（deterministic）：

if SPY 跌幅 > threshold AND 各板块跌幅接近 SPY:
    -> 系统性回调，不是轮动机会
    -> 提高 defensive 权重，降低 add 权限

if 部分板块正收益 AND 部分板块负收益 AND 分化明显:
    -> 轮动信号，识别 leading 板块
    -> sector_momentum = leading 的可以保留/小幅加仓候选
    -> sector_momentum = lagging 的加入 trim_review 候选

LLM 的角色：
    解释为什么会有轮动（宏观逻辑、政策背景、资金流向）
    不决定轮动的 target weight
```

---

## 五、开发优先级清单

### 5.0 当前系统迁移判断（repo-specific）

当前系统已经不是从零开始：

```
已经存在并应该复用：
  - services/evidence_bundle.py
  - services/market_scorecard.py
  - services/proposal_shaper.py
  - services/position_governance.py
  - services/decision_ledger.py
  - services/execution_audit.py

当前最大差距：
  - Stage 5 synthesizer 仍然输出 adjusted_weights
  - Risk Manager / Position Governance 再 deterministic clip
  - 也就是说当前是：
      LLM proposes weights -> Python clips/governs
    目标是：
      LLM proposes thesis/advisory -> Python constructs final target weights
```

因此本计划不应该新建一套平行架构，而应该沿着现有 pipeline 逐步收回
LLM 对 post-analysis 层的权重控制权。

---

### 5.1 Post-LLM 层改造总目标

目标边界：

```
LLM 输出：
  - research_report
  - thesis_status proposal
  - advisory action proposal
  - confidence / rationale

LLM 不再输出作为执行输入的：
  - final adjusted_weights
  - final_action
  - reason_codes
  - executable target_weights

Python 输出：
  - deterministic_action_permission
  - validated_advisory_delta
  - final_target_weights
  - rebalance_actions
  - execution status
  - decision ledger rows
```

迁移原则：

```
1. 先加审计字段，再切换执行输入
2. 先 shadow mode 对比，再替换 live path
3. 每一步都保持旧字段兼容，避免 dashboard / Telegram / tests 同时断裂
4. LLM 权重输出先降级为 diagnostic，再删除执行依赖
```

---

### 5.2 分阶段迁移计划

#### Phase A：修正现有 deterministic guardrail（低风险，先做）

目标：不改变主数据流，只修复解释、审计、proposal pre-flight 的明显问题。

范围：

```
1. hard_risk explanation 修正
   - position_governance / explanation builder 必须优先识别 hard_risk
   - hard_risk 不能 fallback 到普通 hold 文案

2. loss_review + advisory 文案修正
   - advisory 只能表达“仍有弱支持/参考价值”
   - 不能表达成“支持充足”

3. proposal_shaper 扩展 is_add_allowed
   - loss_review no add
   - basket_review no add
   - high_atr no add
   - scorecard reduce-risk permissions no add
   - group exposure over limit no add
   - 输入字段必须来自稳定 deterministic contract：
     current_weights / holdings_meta / market_scorecard / decision_style /
     governance reason_codes
   - 不依赖 Phase B 新增的 LLM advisory schema

4. Telegram / communicator 数据质量拆分
   - QC heartbeat
   - daily snapshot
   - yfinance history
   - live fit
   - news cache
```

验收：

```
pytest tests/test_position_governance.py
pytest tests/test_proposal_shaper.py
pytest tests/test_decision_ledger.py
pytest tests/test_communicator_scorecard.py

Phase B 后重新运行：
pytest tests/test_proposal_shaper.py
确保 is_add_allowed 不需要因为 advisory schema 改动而修改
```

#### Phase B：建立 Post-LLM Advisory Contract（中风险，兼容旧路径）

目标：把 synthesizer 的 post-analysis 输出从“权重主导”改成“advisory 主导”，但暂时保留 adjusted_weights 作为 shadow/diagnostic。

新增/调整 contract：

```python
{
  "research_summary": {...},
  "position_advisory_proposals": [
    {
      "ticker": "FTXL",
      "llm_advisory": "hold_review|trim_review|trim|exit|add|hold",
      "thesis_status": "intact|weakening|broken|unknown",
      "confidence": 0.62,
      "target_weight": 0.03,       # diagnostic only in Phase B
      "delta_hint": -0.01,         # advisory only
      "reason": "..."
    }
  ],
  "adjusted_weights": {...},       # legacy shadow only
  "diagnostic_weight_rationale": "..."
}
```

代码路径：

```
agents/synthesizer.py
  - prompt/schema 明确 adjusted_weights 是 diagnostic/legacy
  - position_advisory_proposals 必填或显式 []

services/position_governance.py
  - _apply_llm_advisory_overrides 保持唯一 validator
  - validator_result 必须记录 accept/reject/convert/clip

services/decision_ledger.py
  - ledger 记录 llm_advisory、validator_result、validated_delta
  - ledger 不使用 LLM reason_codes
```

验收：

```
同一 proposal：
  - add outside allowed_actions -> rejected_add_not_allowed
  - exit without hard risk / exit trigger -> converted_exit_to_hold_review
  - target delta > llm_advisory_max_* -> clipped
  - reason_codes 只能来自 governance/risk/static mapping
```

#### Phase C：新增 Deterministic Target Builder（核心改造）

目标：新增一个明确的 post-LLM deterministic target construction 层，接管最终 target weight 形成。

建议新增模块：

```
services/target_builder.py
```

职责：

```python
def build_target_weights(
    *,
    base_weights: dict[str, float],
    current_weights: dict[str, float],
    market_scorecard: dict,
    decision_style: dict,
    position_governance: dict,
    validated_advisory: list[dict],
    constraints: dict,
) -> TargetBuildResult:
    """
    Deterministically construct executable target weights.

    Inputs:
      - quant baseline / strategy weights
      - scorecard permissions
      - governance allowed_actions
      - validated advisory deltas only

    Does not consume raw LLM adjusted_weights.
    """
```

输出：

```python
{
  "target_weights": {...},
  "target_build_steps": [
    "base_weight",
    "scorecard_clip",
    "governance_adjustment",
    "validated_llm_delta",
    "turnover_clip",
    "normalization"
  ],
  "per_ticker": {
    "FTXL": {
      "base_weight": 0.045,
      "current_weight": 0.050,
      "governance_adjustment": -0.010,
      "validated_llm_delta": -0.005,
      "final_target": 0.035,
      "reason_codes": [...]
    }
  },
  "turnover": {...},
  "violations": [...]
}
```

Pipeline 迁移方式：

```
Phase C.1 shadow mode:
  old path: synthesizer.adjusted_weights -> proposal_shaper -> risk_manager
  new path: target_builder output written to step log only
  compare old_target vs new_target, no execution effect

  exit criteria before gated mode:
    - at least 10 full pipeline runs or 5 trading days of shadow logs
    - no target_builder exceptions
    - every ticker has target_builder per_ticker lifecycle fields
    - all hard blocks match old deterministic governance:
        hard_risk / loss_review no-add / cash_only / reduce_risk_only
    - aggregate turnover diff is reviewed and explained when > 2%
    - max single-ticker target diff is reviewed and explained when > 1.5%
    - manual review signs off on all unexplained diffs

Phase C.2 gated mode:
  if TARGET_BUILDER_ENABLED:
      risk_manager input = target_builder.target_weights
  else:
      risk_manager input = shaped synthesizer adjusted_weights

Phase C.3 final mode:
  risk_manager never consumes raw LLM adjusted_weights
  LLM adjusted_weights removed or retained only in diagnostics
```

验收：

```
同一 input 多次运行 target_builder 输出完全一致
target_builder 不 import agents/*
target_builder 不读取 natural language rationale
target_builder 不消费 raw adjusted_weights
```

#### Phase D：Risk Manager 角色收窄

目标：Risk Manager 从“修改 LLM proposal 后审批”转成“验证 deterministic target 后审批/拒绝”。

调整方向：

```
当前：
  LLM adjusted_weights
    -> risk overlays
    -> scorecard/style enforcement
    -> checks

目标：
  deterministic target_builder target_weights
    -> risk checks
    -> final approval/rejection
```

保留：

```
Risk Manager 仍负责：
  - turnover check
  - cost check
  - cash/equity/style checks
  - stale evidence execution block
  - human confirmation execution block
  - approval token

Risk Manager 不再负责：
  - 把 LLM 权重改造成可执行 target
  - 用 overlay 隐式替代 governance target builder
```

#### Phase E：Ledger / Telegram / Dashboard 完整切换

目标：所有用户可见输出都从 deterministic target lifecycle 读取，而不是从 LLM 权重叙事读取。

Ledger 必须包含：

```
trade_lifecycle:
  base_weight
  diagnostic_llm_target
  validated_advisory_delta
  governance_target
  risk_checked_target
  final_target

source_effects:
  qc / yfinance / knowledge / news / scorecard / risk

final_action:
  from risk + governance + execution audit
```

Telegram 必须显示：

```
Decision evidence:
  QC heartbeat: ...
  Daily snapshot: ...
  Live fit: ...
  yfinance: ...
  News cache: ...

Top decisions:
  proposed_action != final_action 时必须明确显示
  risk_rejected != execution_failed 必须明确区分
```

#### Phase F：删除旧权重执行依赖

完成条件：

```
1. run_risk_manager_async 不再需要 synthesizer_out.adjusted_weights ✅
   - Risk Manager 只消费 target_builder_gated.target_weights
   - target_builder 缺失时只能回退到 deterministic base_weights，不能回退到 LLM adjusted_weights

2. tests 中没有“LLM adjusted_weights 是执行输入”的假设 ✅
   - 风险执行测试必须断言 raw_llm_adjusted_weights_consumed = false
   - adjusted_weights 相关测试只覆盖 diagnostic/contract/proposal-shaper 兼容层

3. dashboard/ledger 仍可显示 diagnostic_llm_target ✅
   - diagnostic_llm_target 只用于可观测性，不进入 rebalance execution source

4. FULL_AUTO / SEMI_AUTO 都只执行 deterministic target_builder 输出 ✅
   - Pipeline 默认开启 target_builder gated input
   - Risk Manager 的 target_construction_mode 必须是 target_builder_gated 或 deterministic_base_fallback

5. 文档更新 pipeline data flow ✅
   - base_weights -> LLM advisory -> target_builder target_weights -> risk validation -> execution
```

---

### 5.3 推荐实施顺序

```
Sprint 1:
  - Phase A 全部
  - 补测试：hard_risk explanation / advisory wording / no-add preflight

Sprint 2:
  - Phase B advisory contract
  - decision_ledger 记录 validator_result
  - synthesizer adjusted_weights 标记 diagnostic

Sprint 3:
  - Phase C target_builder shadow mode
  - 增加 old/new target diff step log
  - 不改变执行

Sprint 4:
  - target_builder gated mode
  - risk_manager 输入切换 behind config flag
  - Phase D：Risk Manager 角色收窄
    - risk_manager 接收 target_builder target_weights
    - risk_manager 不再消费 raw LLM adjusted_weights
    - overlays 只作为 risk check / rejection reason，不再隐式构造 target
  - dashboard/Telegram 读 deterministic lifecycle

Sprint 5:
  - 删除旧执行依赖
  - 更新 pipeline docstring 和 tests
  - adjusted_weights 仅保留 diagnostic 或完全移除
```

---

### 5.4 不建议做的迁移方式

```
❌ 一次性重写 pipeline.py
❌ 新建一套 parallel pipeline，和旧 pipeline 长期并存
❌ 让 Risk Manager 同时承担 target_builder 和审批职责
❌ 只改 prompt，声称 LLM 不会越界
❌ 删除 adjusted_weights 前没有 shadow comparison
❌ 先改 Telegram/dashboard，再改 ledger/source of truth
```

### 立刻可以做（改 1-2 个 function，风险低）

```
1. 修 hard_risk explanation bug
   - _why_hold() 遇到 hard_risk 必须返回 hard-risk review 文案
   - 禁止 fallback 到 "no deterministic rule requires reduction"

2. loss_review + advisory 的措辞修正
   - "strategy support remains advisory" -> "only advisory support remains; 
      this is not strong enough to justify adding"

3. loss_review 状态下禁止 add proposal
   - proposal shaper 加 is_add_allowed() pre-flight check

4. Telegram 拆开 data=limited 显示
   - QC heartbeat / daily snapshot / live fit 三行分开显示
```

### 需要设计但不复杂（1 周内可完成）

```
5. basket_review 写进 explanation
   - detect_basket_reviews() 函数
   - FTXL/PSI/SOXX 必须出现组合级风险解释

6. satellite/thematic ETF 的差异化亏损阈值
   - asset_type 参数进入 get_position_state()
   - 配置化，不 hardcode

7. bull + data_limited 时自动压低 turnover
   - get_effective_constraints() 函数
   - human_required 时 turnover_limit *= 0.5
```

### 需要认真设计（2 周以上）

```
8. explanation_facts 中间结构重构
   - decision row -> explanation_facts -> why_hold/why_not_add/...
   - 防止模板漂移

9. sector_momentum 字段
   - yfinance relative strength 横截面计算
   - 进入 evidence_bundle，辅助轮动判断

10. thesis_status 写入规范
    - position_governance 是唯一 owner
    - LLM 只能 propose，validator 决定是否接受
    - 专项测试：LLM broken 不能直接触发 exit
```

---

## 六、常见反模式（应该避免）

```
❌ 让 LLM 决定 final_action
❌ 用 prompt 约束数学边界（"不要超过 2%"）
❌ 让 LLM 生成 reason_codes
❌ explanation 用模板后填充（不从状态机导出）
❌ 不同状态共用同一个 explanation 模板
❌ 缺数据时静默降级（应显式标注 missing）
❌ 把 heartbeat fresh 等同于 live evidence 充分
❌ advisory support 在高亏损时视为强支持
❌ 把 QC/yfinance 数据合并成一个 confidence 数字
❌ Telegram 说"已执行"但没有 execution_audit 确认
❌ decision_ledger 重新计算 governance 决策
❌ 同一 basket 多个亏损仓位当作独立风险处理
```

---

## 七、验收标准（每个 Phase 的 Definition of Done）

```
Position State Machine：
  ✓ 同样输入，跨运行输出完全一致
  ✓ hard_risk 状态不能输出普通 hold 解释
  ✓ basket_review 触发时所有 basket 内 ticker 的 explanation 包含组合上下文

Proposal Shaping：
  ✓ loss_review ticker 不出现在 add proposal 中
  ✓ basket_review 状态下整个 basket 的 add 都被阻止
  ✓ turnover 预估在 proposal 层完成，不等 risk manager 整体拒绝

Decision Ledger：
  ✓ 每个 holding 有 ledger row
  ✓ risk rejected -> final_action = none
  ✓ 缺少 governance 输出时报 warning，不猜测
  ✓ source_effects 映射是静态的，不由数据决定

Telegram：
  ✓ proposed action 和 final action 分开显示
  ✓ 数据质量分源显示（不合并成 data=limited）
  ✓ 最多 5 条 ticker 行，按 severity 排序
  ✓ 不出现"已执行"除非 execution_audit 确认 filled

Explanation Correctness：
  ✓ hard_risk_review ticker 的 why_hold 包含 manual review 语言
  ✓ basket loser 的 why_hold 包含 basket 上下文
  ✓ advisory support 在亏损时不被描述为"支持充足"
```

---

*文档版本：2026-05-20*
*适用系统：Agentix multi-agent trading system*
*核心原则：LLM 是研究员，Deterministic Function 是裁判*
