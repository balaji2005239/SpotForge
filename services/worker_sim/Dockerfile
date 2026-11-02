FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir httpx
COPY services/worker_sim /app/services/worker_sim
ENV PYTHONPATH=/app/services/worker_sim
CMD ["python", "-m", "worker_sim.agent"]
