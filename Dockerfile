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

RUN mkdir -p api db services dashboard cron tools audit
COPY main.py config.py constants.py ./
COPY api/__init__.py api/observer.py ./api/
COPY db/__init__.py db/models.py db/session.py db/queries.py ./db/
COPY services/__init__.py \
     services/execution_authority.py \
     services/qc_observer_health.py \
     services/qc_observer_ingestion.py \
     services/qc_observer_projections.py \
     services/qc_webhook_auth.py \
     services/cron_audit.py \
     services/alphavantage_client.py \
     services/finnhub_client.py \
     services/news_summarizer.py \
     services/rss_fetcher.py \
     services/retry_protocol.py \
     services/execution_policy.py \
     ./services/
COPY dashboard/__init__.py dashboard/app.py dashboard/view_models.py ./dashboard/
COPY cron/__init__.py cron/morning_health.py cron/post_market_report.py cron/pre_fetch_news.py ./cron/
COPY tools/__init__.py tools/notify_tools.py ./tools/
COPY audit/__init__.py audit/calculators.py audit/service.py audit/worker.py ./audit/

EXPOSE ${PORT:-8000}
CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000}"]
