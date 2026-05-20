# config.py
from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    # 数据库
    database_url: str = "postgresql://user:pass@localhost:5432/qc_fastapi_2"

    # OpenAI
    openai_api_key: str
    openai_model: str = "gpt-4o-mini"        # OPENAI_MODEL (轻量任务)
    openai_model_heavy: str = "gpt-4o"       # OPENAI_MODEL_HEAVY (主模型)

    # Webhook鉴权
    webhook_user: str = "qc"
    webhook_secret: str

    # QC REST API
    qc_api_url: str = "https://www.quantconnect.com/api/v2"
    qc_user_id: str
    qc_api_token: str
    qc_project_id: str

    # Telegram
    tg_bot_token: str     # TG_BOT_TOKEN
    tg_chat_id: str       # TG_CHAT_ID

    # 运行模式
    authorization_mode: str = "SEMI_AUTO"  # FULL_AUTO | SEMI_AUTO | MANUAL

    # 风控参数默认値（会被 system_config 覆盖）
    max_drawdown: float = 0.15
    max_single_position: float = 0.20
    min_cash_pct: float = 0.05
    max_sector_concentration: float = 0.60
    rebalance_threshold: float = 0.02
    max_trade_cost_pct: float = 0.005
    max_hist_vol: float = 0.35
    max_broad_market: float = 0.40

    # SEMI_AUTO 超时（分钟）
    semi_auto_timeout_minutes: int = 20

    # Emergency 配置
    emergency_auto_liquidate: bool = False  # 收到 emergency 包时是否自动清仓

    # Proposal Invalidation 配置 (P2-1: 防 Panic)
    proposal_invalidation_vix_threshold: float = 35.0       # VIX 超过此值则 proposal 作废
    proposal_invalidation_portfolio_drift_threshold: float = 0.03  # 组合价值相对变化超过此值则 proposal 作废

    # Circuit Breaker 配置 (Phase 3)
    vix_alert_threshold: float = 30.0              # VIX > 此值 → ALERT
    vix_defensive_threshold: float = 40.0          # VIX > 此值 → DEFENSIVE
    drawdown_alert_threshold: float = 0.10         # 回撤 > 此值 → ALERT (10%)
    rejection_window_hours: int = 2                # 连续拒绝计数时间窗口（小时）
    rejection_count_threshold: int = 3             # 超过此数量拒绝 → ALERT
    llm_failure_window_hours: int = 1               # LLM失败率统计时间窗口（小时）
    llm_failure_rate_threshold: float = 0.50       # LLM失败率超过此值 → ALERT (50%)
    circuit_cooldown_minutes: int = 30             # ALERT 自动恢复冷却时间（分钟）
    persistent_alert_hours: int = 2                 # ALERT 持续超过此时长 → 自动升级 DEFENSIVE

    # 新闻 API
    finnhub_api_key: str = ""
    alphavantage_api_key: str = ""

    class Config:
        env_file = ".env"
        case_sensitive = False


@lru_cache()
def get_settings() -> Settings:
    return Settings()
