# noble-flow 最小化计划

日期：2026-08-13

## 目标

将 `noble-flow` 收敛为 QC live 的只读配套系统。QC 继续独立负责策略、风险和执行；Railway 只负责事实接入、持久化、监控、通知和保留的新闻数据管道。

硬约束：

- `execution_authority=none` 永久保持；
- 不改变当前 B0 的排名、仓位、ATR、MOO 或 no-trade band；
- FastAPI 或 Railway 故障不得阻塞 QC；
- 新闻 pipeline 保留；
- 旧数据库表先冻结，不能与代码同步删除。

## 目标 Railway 拓扑

只保留六个 service：

1. `main-service`：health、observer ingestion、observer status、projection status、只读 operator API；
2. `Postgres`：raw events、receipts、projections、news、cron audit；
3. `monitor`：只读 observer/operator Dashboard；
4. `cron-morning-health`：observer、数据库、projection、新闻 freshness 健康通知；
5. `cron-post-market-report`：账户、持仓、现金、当日风险/订单事实摘要；
6. `cron-pre-fetch-news`：保留现有新闻 pipeline。

## Railway 停用候选

以下服务不属于当前 QC observer 最小运行面：

- `cron-daily-analyst`
- `cron-daily-signal-freeze`
- `cron-daily-signal-validation-refresh`
- `cron-dynamic-scheduler`
- `cron-hourly-analysis`
- `cron-monthly-analyst`
- `cron-pending-check`
- `cron-playground-analysis`
- `cron-position-monitor`
- `cron-qc-yfinance-feature-audit`
- `cron-quarterly-analyst`
- `cron-validation-observation-refresh`
- `cron-weekend-trading-review`
- `cron-weekly-analyst`
- `cron-weekly-performance-attribution`
- `cron-yfinance-backfill`

其中没有 schedule 的 service 当前均已退出且没有下一次运行；有 schedule 的旧研究任务先暂停一个完整交易周，再删除。yfinance 研究任务迁回本地 `strategy_research_lab`，不再占用 live Railway 项目。

## 目标代码边界

生产入口只允许依赖以下模块族：

```text
core/
  app.py
  config.py
  database.py
  observer/
    contracts.py
    auth.py
    ingestion.py
    projections.py
    health.py
  operator/
    account_view.py
    post_market_report.py
    notifications.py
  news/
    pipeline.py
dashboard/
  app.py
jobs/
  morning_health.py
  post_market_report.py
  pre_fetch_news.py
```

生产 API 只保留：

- `GET /health`
- `POST /api/observer/events`
- `GET /api/observer/status`
- `GET /api/observer/projections/status`
- `GET /api/ops/live/latest`
- `GET /api/ops/live/latest/text`

旧 `command`、`execution`、agent pipeline、portfolio mutation 和 legacy webhook 路由不再挂载。代码先移入 `legacy/` 或独立 archive 分支，经过一个完整交易周后再物理删除。

## 数据库策略

- 不在第一轮 drop 任何表；
- observer 与 news 表继续读写；
- 旧 agent、signal、execution command 表改为只读冻结；
- 记录最后读写时间，连续 90 天无人读取后再导出并 drop；
- migration 从应用启动逻辑中拆出，生产启动不再遍历全部历史 migration。

## 实施顺序

### A. 建立最小入口，不改生产

- 创建 minimal app、Dashboard 和三个 job 入口；
- 共用当前 Postgres，只读旧表；
- 添加 observer contract、news contract 和 execution fence 测试；
- 修复数据库连接池：启用 `pool_pre_ping`，observer ingestion 对瞬时断连重试一次。

### B. Shadow 验证

- minimal app 在 staging 接收复制的 observer payload；
- 连续验证一个完整周度周期；
- raw event hash、P0/P1/P2 high-water mark、projection count 与生产严格一致；
- Dashboard、morning health、post-market report 输出对齐。

### C. 切换生产入口

- `main-service` 和 `monitor` 切到 minimal entrypoint；
- QC endpoint 不变；
- 保留旧镜像和 Railway deployment，至少观察一个完整周度周期；
- 任何 sequence gap、projection backlog 或通知缺失立即回滚。

### D. 停用旧 jobs

- 先移除 schedule 或暂停服务，不立即删除；
- 观察七个交易日；
- 确认没有 observer、news、Dashboard 或通知依赖后删除 Railway service。

### E. 删除旧代码

- 将研究工具迁到 `/Users/wenzhuosong/Work/trading/strategy_research_lab`；
- 删除生产仓库中的 agents、旧 strategies、旧 execution/control-plane 代码和无用 cron；
- Docker build context 排除 `tests/`、`tools/`、研究数据和文档；
- 重新生成最小 requirements，仅保留实际 import 的依赖。

## 验收门槛

- B0 交易行为零变化；
- `execution_authority=none`；
- 两个完整周度决策周期无 P0 gap；
- P1/P2 freshness 正常；
- projection pending 为 0；
- decision -> intent -> submitted -> filled 可完整关联；
- 新闻 cache 正常更新；
- morning/post-market 通知正常；
- Railway 服务数量从 21 降到 6；
- 瞬时数据库断连不再导致 observer 500 或事件丢失。

## 当前决策

现在不直接删除生产资源。下一项实现工作是 A：建立 minimal entrypoint，并先修复数据库连接池可靠性；完成本地测试后再部署 staging shadow。
