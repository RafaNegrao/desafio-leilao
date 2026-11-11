FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY src ./src
COPY scripts ./scripts
ENV PYTHONPATH=/app/src PYTHONUNBUFFERED=1
CMD ["python", "scripts/run_worker.py"]
