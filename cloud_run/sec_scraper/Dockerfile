FROM python:3.11-slim
ENV PYTHONUNBUFFERED=True
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential libxml2-dev libxslt-dev && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir google-cloud-run gunicorn google-cloud-bigquery

COPY . .

# Default command starts the Web App
CMD ["gunicorn", "--bind", ":8080", "--workers", "1", "--worker-class", "uvicorn.workers.UvicornWorker", "main:app"]