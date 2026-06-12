import os


def ensure_test_settings() -> None:
    defaults = {
        "OPENAI_API_KEY": "test-openai-key",
        "WEBHOOK_SECRET": "test-webhook-secret",
        "QC_USER_ID": "test-qc-user",
        "QC_API_TOKEN": "test-qc-token",
        "QC_PROJECT_ID": "test-qc-project",
        "TG_BOT_TOKEN": "test-tg-token",
        "TG_CHAT_ID": "test-tg-chat",
    }
    for key, value in defaults.items():
        os.environ.setdefault(key, value)
