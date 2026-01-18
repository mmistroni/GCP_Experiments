# Use a slim version for speed and security
FROM python:3.11-slim

# 1. Set environment variables
ENV PYTHONUNBUFFERED=True
ENV APP_HOME=/app
WORKDIR $APP_HOME

# 2. Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libxml2-dev \
    libxslt-dev \
    && rm -rf /var/lib/apt/lists/*

# 3. Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir google-cloud-run gunicorn pydantic-ai pydantic-settings

# 4. Copy local code
COPY . .

# 5. Create the entrypoint script
# FIX: Removed "-m app." so it runs the file directly from the WORKDIR
RUN echo '#!/bin/bash\n\
echo "📂 Current Directory Content:" && ls -R\n\
if [ "$1" = "job" ]; then\n\
    echo "🚀 Starting SEC Scraper Worker Job..."\n\
    exec python scraper_job.py\n\
else\n\
    echo "🌐 Starting FastAPI Gateway..."\n\
    exec gunicorn --bind :$PORT --workers 1 --worker-class uvicorn.workers.UvicornWorker --threads 8 --timeout 3600 main:app\n\
fi' > /entrypoint.sh && chmod +x /entrypoint.sh

# Default to running the web service if no args provided
ENTRYPOINT ["/entrypoint.sh"]