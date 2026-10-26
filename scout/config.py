"""Scout settings. Independent of the noble-flow observer settings."""
from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class ScoutSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", case_sensitive=False, extra="ignore")

    database_url: str = "postgresql://user:pass@localhost:5432/noble_flow"

    # LLM (thesis writer + Q&A). Empty key → Python template fallback only.
    openai_api_key: str = ""
    scout_llm_model: str = "gpt-4o-mini"
    scout_llm_timeout_s: float = 45.0
    # The ticker "AI 解读" runs a few times a day: use a stronger model than the cards/Q&A.
    scout_insight_model: str = "gpt-4o"

    # Optional enrichment. Empty key → skipped.
    finnhub_api_key: str = ""

    # Telegram. A dedicated Scout bot is recommended; falls back to the ops bot.
    scout_tg_bot_token: str = ""
    scout_tg_chat_id: str = ""
    scout_tg_webhook_secret: str = ""
    tg_bot_token: str = ""
    tg_chat_id: str = ""

    # Dashboard basic auth.
    scout_dashboard_user: str = ""
    scout_dashboard_password: str = ""

    # Engine knobs.
    scout_top_n: int = 8
    scout_push_threshold: float = 0.60
    scout_max_push_cards: int = 3
    scout_min_price: float = 5.0
    scout_min_dollar_volume: float = 20_000_000.0
    scout_history_years: int = 3

    @property
    def bot_token(self) -> str:
        return self.scout_tg_bot_token or self.tg_bot_token

    @property
    def chat_id(self) -> str:
        return self.scout_tg_chat_id or self.tg_chat_id


@lru_cache()
def get_scout_settings() -> ScoutSettings:
    return ScoutSettings()
