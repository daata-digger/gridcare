FROM python:3.10-slim

WORKDIR /app

# Install build deps only if needed by httpx (none heavy here)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Copy only what the dashboard needs
COPY app.py /app/app.py
COPY static /app/static

EXPOSE 8081
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8081"]
