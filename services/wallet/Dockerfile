FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir fastapi "uvicorn[standard]" "sqlalchemy>=2.0" "psycopg[binary]" pydantic pydantic-settings alembic httpx aiokafka
COPY libs/spotcommon /app/libs/spotcommon
COPY libs/clearing_core /app/libs/clearing_core
COPY services/wallet /app/services/wallet
ENV PYTHONPATH=/app/libs/spotcommon:/app/libs/clearing_core:/app/services/wallet
EXPOSE 8001
CMD ["uvicorn", "wallet_svc.main:app", "--host", "0.0.0.0", "--port", "8001"]
