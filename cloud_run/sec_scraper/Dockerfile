# Use a slim version to keep the image small and secure
FROM python:3.11-slim

# 1. Set environment variables
ENV PYTHONUNBUFFERED=True
ENV APP_HOME=/app
WORKDIR /app/cloud_run/sec_scraper

# 2. Install system dependencies (The ONLY place for --no-install-recommends)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# 3. Install Python dependencies
# We copy only the requirements first to leverage Docker's layer caching
COPY requirements.txt .

# FIX: Removed --no-install-recommends (not a pip flag) 
# ADDED: --no-cache-dir to keep the image small
RUN pip install --no-cache-dir -r requirements.txt

# 4. Copy local code to the container
COPY . .

# 5. Run the web service
# Using uvicorn directly is perfect for Pydantic AI's async nature
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080", "--workers", "1"]