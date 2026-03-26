# config.py
from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    # 数据库
    database_url: str = "postgresql://user:pass@localhost:5432/qc_fastapi_2"

    # OpenAI
    openai_api_key: str
    openai_model: str = "gpt-4o"
    openai_model_mini: str = "gpt-4o-mini"

    # Webhook鉴权
    webhook_user: str = "qc"
    webhook_secret: str

    # QC REST API
    qc_api_url: str       # https://www.quantconnect.com/api/v2
    qc_user_id: str
    qc_api_token: str
    qc_project_id: str

    # Telegram
    telegram_bot_token: str
    telegram_chat_id: str

    # 运行模式
    authorization_mode: str = "SEMI_AUTO"  # FULL_AUTO | SEMI_AUTO | MANUAL

    # 风控参数默认値（会被 system_config 覆盖）
    max_drawdown: float = 0.15
    max_single_position: float = 0.20
    min_cash_pct: float = 0.05
    max_sector_concentration: float = 0.60
    rebalance_threshold: float = 0.02
    max_trade_cost_pct: float = 0.005

    # SEMI_AUTO 超时（分钟）
    semi_auto_timeout_minutes: int = 20

    class Config:
        env_file = ".env"
        case_sensitive = False


@lru_cache()
def get_settings() -> Settings:
    return Settings()
