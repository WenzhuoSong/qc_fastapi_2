"""Configuration for the minimal noble-flow observer, dashboard and news jobs."""
from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", case_sensitive=False, extra="ignore")

    database_url: str = "postgresql://user:pass@localhost:5432/noble_flow"

    # Observer authentication. The ingestion endpoint rejects requests when no
    # secret is configured; read-only services do not need one.
    webhook_secret: str = ""
    qc_webhook_secret: str = ""
    qc_observer_require_request_v2: bool = False
    qc_observer_allowed_accounts: str = ""
    qc_observer_allowed_deployments: str = ""
    qc_observer_max_clock_skew_seconds: int = 600

    # Retained news pipeline.
    openai_api_key: str = ""
    openai_model: str = "gpt-4o-mini"
    finnhub_api_key: str = ""
    alphavantage_api_key: str = ""

    # Read-only operator notifications.
    tg_bot_token: str = ""
    tg_chat_id: str = ""


@lru_cache()
def get_settings() -> Settings:
    return Settings()
