FROM python:3.11-slim

# Set environment variables
ENV PYTHONUNBUFFERED=True
ENV APP_HOME=/app
WORKDIR $APP_HOME

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-install-recommends -r requirements.txt

# Copy local code to the container
COPY . .

# Run the web service on container startup
# 3600 second timeout is handled by the Gunicorn/Uvicorn workers
CMD exec uvicorn main:app --host 0.0.0.0 --port 8080 --workers 1