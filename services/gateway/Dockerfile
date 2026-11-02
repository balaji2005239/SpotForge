FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir fastapi "uvicorn[standard]" "sqlalchemy>=2.0" "psycopg[binary]" pydantic pydantic-settings alembic redis httpx pyjwt
COPY libs/spotcommon /app/libs/spotcommon
COPY services/gateway /app/services/gateway
ENV PYTHONPATH=/app/libs/spotcommon:/app/services/gateway
EXPOSE 8000
CMD ["uvicorn", "gateway_svc.main:app", "--host", "0.0.0.0", "--port", "8000"]
