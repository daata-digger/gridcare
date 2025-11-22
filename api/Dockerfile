FROM python:3.11-slim
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app
ENV PYTHONPATH=/app
ENV PG_HOST=db PG_PORT=5432 PG_DB=postgres PG_USER=postgres PG_PASS=postgres

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
