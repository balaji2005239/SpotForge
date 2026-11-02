FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir fastapi "uvicorn[standard]" "sqlalchemy>=2.0" "psycopg[binary]" pydantic pydantic-settings alembic httpx aiokafka
COPY libs/spotcommon /app/libs/spotcommon
COPY libs/clearing_core /app/libs/clearing_core
COPY services/capacity /app/services/capacity
ENV PYTHONPATH=/app/libs/spotcommon:/app/libs/clearing_core:/app/services/capacity
EXPOSE 8003
CMD ["uvicorn", "capacity_svc.main:app", "--host", "0.0.0.0", "--port", "8003"]
