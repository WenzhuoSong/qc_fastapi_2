# QC FastAPI 2 部署指南

## Railway 部署步骤

### 1. 准备工作

确保已有以下账号：
- Railway 账号 (railway.app)
- OpenAI API Key (platform.openai.com)
- QuantConnect 账号和 API Token
- Telegram Bot Token (通过 @BotFather 创建)

### 2. 创建 Railway 项目

```bash
# 安装 Railway CLI（可选）
npm i -g @railway/cli

# 登录
railway login

# 初始化项目（在 qc_fastapi_2 目录）
cd /Users/wenzhuo.song/Personal/qc_fastapi_2
railway init
```

### 3. 添加 PostgreSQL 服务

在 Railway Dashboard：
1. 点击 "New" → "Database" → "PostgreSQL"
2. Railway 自动生成 `DATABASE_URL` 环境变量
3. 记录连接信息（自动注入到应用）

### 4. 配置环境变量

在 Railway Dashboard → Variables 添加：

```env
# OpenAI
OPENAI_API_KEY=sk-...
OPENAI_MODEL=gpt-4o
OPENAI_MODEL_MINI=gpt-4o-mini

# QC Webhook Auth
WEBHOOK_USER=qc
WEBHOOK_SECRET=your-strong-secret-here

# QC API
QC_API_URL=https://www.quantconnect.com/api/v2
QC_USER_ID=your-qc-user-id
QC_API_TOKEN=your-qc-api-token
QC_PROJECT_ID=your-project-id

# Telegram
TELEGRAM_BOT_TOKEN=your-bot-token
TELEGRAM_CHAT_ID=your-chat-id

# 授权模式
AUTHORIZATION_MODE=SEMI_AUTO

# Railway 会自动设置：
# DATABASE_URL (asyncpg 格式会自动转换)
# PORT (动态分配)
```

### 5. 部署应用

#### 方式 1: 通过 Railway CLI

```bash
railway up
```

#### 方式 2: 通过 GitHub

1. 将代码推送到 GitHub
2. 在 Railway Dashboard 连接 GitHub 仓库
3. Railway 自动检测 Dockerfile 并部署

### 6. 配置 Telegram Webhook

部署成功后，获取 Railway 生成的域名（例如 `qc-fastapi-2-production.up.railway.app`），然后：

```bash
curl "https://api.telegram.org/bot{YOUR_BOT_TOKEN}/setWebhook?url=https://qc-fastapi-2-production.up.railway.app/api/telegram"
```

验证 webhook：
```bash
curl "https://api.telegram.org/bot{YOUR_BOT_TOKEN}/getWebhookInfo"
```

### 7. 配置 QC Webhook

在 QuantConnect 项目中设置 webhook URL：

```csharp
// QC Algorithm 中
private string webhookUrl = "https://qc-fastapi-2-production.up.railway.app/api/webhook/qc";
private string webhookUser = "qc";
private string webhookSecret = "your-strong-secret-here";

public void SendToBackend(object payload)
{
    var json = JsonConvert.SerializeObject(payload);
    var compressed = GzipCompress(json);

    var headers = new Dictionary<string, string>
    {
        {"x-webhook-user", webhookUser},
        {"x-webhook-secret", webhookSecret}
    };

    Download(webhookUrl, compressed, headers);
}
```

### 8. 验证部署

检查健康状态：
```bash
curl https://qc-fastapi-2-production.up.railway.app/health
```

检查系统状态：
```bash
curl https://qc-fastapi-2-production.up.railway.app/api/status
```

测试 Telegram 命令：
发送 `/status` 到你的 Telegram Bot

### 9. 监控日志

在 Railway Dashboard 查看实时日志：
- 部署日志（Deployments tab）
- 运行时日志（右侧面板）
- 查找调度器启动信息：`"Scheduler started. Jobs: hourly_analysis..."`

## 本地开发设置

### 1. 克隆并安装

```bash
cd /Users/wenzhuo.song/Personal/qc_fastapi_2
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. 启动本地数据库

```bash
docker run -d --name qc-fastapi-2-pg \
  -e POSTGRES_DB=qc_fastapi_2 \
  -e POSTGRES_USER=qc_fastapi_2 \
  -e POSTGRES_PASSWORD=password \
  -p 5432:5432 postgres:16
```

### 3. 配置本地环境

```bash
cp .env.example .env
# 编辑 .env 填入凭证
```

### 4. 启动应用

```bash
uvicorn main:app --reload
```

### 5. 本地测试 Telegram

使用 ngrok 暴露本地端口：
```bash
ngrok http 8000
```

然后设置 webhook 到 ngrok URL：
```bash
curl "https://api.telegram.org/bot{TOKEN}/setWebhook?url=https://xxxx.ngrok.io/api/telegram"
```

## 故障排查

### 数据库连接错误

检查 `DATABASE_URL` 格式：
```
postgresql+asyncpg://user:pass@host:5432/dbname
```

Railway 提供的格式可能是 `postgresql://`，代码会自动转换。

### APScheduler 未启动

检查日志中是否有：
```
Scheduler started. Jobs: hourly_analysis, post_market_report, morning_health
```

如果缺失，检查 `scheduler/runner.py` 的时区设置。

### Telegram 命令无响应

1. 验证 webhook 状态：
   ```bash
   curl "https://api.telegram.org/bot{TOKEN}/getWebhookInfo"
   ```

2. 检查 `TELEGRAM_CHAT_ID` 是否正确（必须是字符串）

3. 查看 Railway 日志中的 Telegram 请求

### QC Webhook 失败

1. 检查 `x-webhook-user` 和 `x-webhook-secret` headers
2. 确认 payload 是 gzip 压缩的
3. 查看 Agentix 日志中的解压错误

## 生产运维

### 切换授权模式

通过 Telegram：
```
/pause  # 切换到 MANUAL
```

或通过 API：
```bash
curl -X POST https://qc-fastapi-2-production.up.railway.app/api/command/pause \
  -H "Content-Type: application/json" \
  -d '{"pause": true, "reason": "Market volatility"}'
```

### 手动触发分析

（需要在 `api/command.py` 添加 `/trigger_analysis` 端点）

### 数据库备份

Railway 提供自动备份，也可手动导出：
```bash
# 通过 Railway CLI
railway db dump > backup.sql
```

### 扩展建议

- **监控**: 集成 Sentry 或 Datadog
- **日志**: 集成 Logtail 或 Better Stack
- **告警**: Telegram 通知 + PagerDuty
- **指标**: Prometheus + Grafana
