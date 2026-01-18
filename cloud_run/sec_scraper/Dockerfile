FROM python:3.11-slim

ENV PYTHONUNBUFFERED=True
ENV APP_HOME=/app
WORKDIR $APP_HOME

# Install dependencies
RUN apt-get update && apt-get install -y build-essential && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt google-cloud-run gunicorn

# Copy all files from your local directory to /app
COPY . .

# ENTRYPOINT with a 'Discovery' step
ENTRYPOINT ["/bin/bash", "-c", "\
    echo '--- 📂 File Discovery ---' && \
    find . -maxdepth 2 -not -path '*/.*' && \
    echo '------------------------' && \
    if [ \"$1\" = \"job\" ]; then \
        echo '🚀 Starting Job...' && \
        python scraper_job.py; \
    else \
        echo '🌐 Starting Gateway...' && \
        gunicorn --bind :$PORT --workers 1 --worker-class uvicorn.workers.UvicornWorker main:app; \
    fi" \
, "--"]