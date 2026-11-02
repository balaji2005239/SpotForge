FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir fastapi "uvicorn[standard]" "sqlalchemy>=2.0" "psycopg[binary]" pydantic pydantic-settings alembic httpx aiokafka celery redis
COPY libs/spotcommon /app/libs/spotcommon
COPY services/settlement /app/services/settlement
ENV PYTHONPATH=/app/libs/spotcommon:/app/services/settlement
EXPOSE 8005
CMD ["uvicorn", "settlement_svc.main:app", "--host", "0.0.0.0", "--port", "8005"]
