"""Settings for the shared news pipeline (`cron.pre_fetch_news`) and its Telegram alerts."""
from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", case_sensitive=False, extra="ignore")

    database_url: str = "postgresql://user:pass@localhost:5432/noble_flow"

    openai_api_key: str = ""
    openai_model: str = "gpt-4o-mini"
    finnhub_api_key: str = ""
    alphavantage_api_key: str = ""

    tg_bot_token: str = ""
    tg_chat_id: str = ""


@lru_cache()
def get_settings() -> Settings:
    return Settings()
