FROM python:3.11-slim

WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONFAULTHANDLER=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Runtime code only; .dockerignore keeps tests, docs and research data out.
COPY config.py constants.py ./
COPY cron ./cron
COPY db ./db
COPY scout ./scout
COPY services ./services
COPY tools/__init__.py tools/notify_tools.py ./tools/

EXPOSE ${PORT:-8000}
# Default: Scout web. Cron services override the start command:
#   python -m scout.jobs.eod | python -m scout.jobs.weekly | python -m cron.pre_fetch_news
CMD ["sh", "-c", "uvicorn scout.web.app:app --host 0.0.0.0 --port ${PORT:-8000}"]
