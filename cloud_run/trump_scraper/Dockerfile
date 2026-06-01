# Use an official lightweight Python runtime
FROM python:3.11-slim

# Install system dependencies required for running a headless browser infrastructure
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    curl \
    gnupg \
    unzip \
    ca-certificates \
    chromium \
    chromium-driver \
    && rm -rf /var/lib/apt/lists/*

# Set environment variables to keep Python clean and predictable
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Establish working directory inside the container image
WORKDIR /app

# Copy python dependency manifests and install them
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy over your application scripts 
COPY . .

# Expose port 8080 (Cloud Run routes all incoming web hook invocations to 8080 by default)
EXPOSE 8080

# Run your scraper engine script
CMD ["python", "trump_scraper.py"]