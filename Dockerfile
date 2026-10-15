FROM python:3.11-slim AS dependencies
ENV PIP_DISABLE_PIP_VERSION_CHECK=1 PIP_NO_CACHE_DIR=1
WORKDIR /build
COPY requirements.txt ./
RUN python -m venv /opt/venv && /opt/venv/bin/pip install -r requirements.txt && /opt/venv/bin/pip check

FROM python:3.11-slim
ENV PATH="/opt/venv/bin:$PATH" PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
RUN groupadd --gid 10001 conversation && useradd --uid 10001 --gid conversation --no-create-home conversation
WORKDIR /app
COPY --from=dependencies /opt/venv /opt/venv
COPY main.py worker.py telegram_worker.py voice_worker.py alembic.ini ./
COPY core/ ./core/
COPY db/ ./db/
COPY models/ ./models/
COPY schemas/ ./schemas/
COPY services/ ./services/
COPY api/ ./api/
COPY migrations/ ./migrations/
USER conversation
EXPOSE 8000 8001 8002 8003
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]
