FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir fastapi "uvicorn[standard]" "sqlalchemy>=2.0" "psycopg[binary]" pydantic pydantic-settings alembic redis httpx aiokafka
COPY libs/spotcommon /app/libs/spotcommon
COPY libs/clearing_core /app/libs/clearing_core
COPY services/market /app/services/market
ENV PYTHONPATH=/app/libs/spotcommon:/app/libs/clearing_core:/app/services/market
EXPOSE 8004
CMD ["uvicorn", "market_svc.main:app", "--host", "0.0.0.0", "--port", "8004"]
