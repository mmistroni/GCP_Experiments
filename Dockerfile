# Use a slim Debian-based Python image for a smaller footprint
FROM python:3.12-slim-buster

# Set environment variables for non-interactive commands and Python
ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100

# Install system dependencies that might be needed by some Python packages
# (e.g., for cryptography, lxml, numpy, etc.)
# This list is a best guess; you might need to add more based on specific package build requirements
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    libffi-dev \
    libssl-dev \
    zlib1g-dev \
    curl \
    git \
    openssh-client \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory inside the container
WORKDIR /app

# Copy only requirements.txt first to leverage Docker cache
COPY requirements.txt .

# Install Python dependencies. Using --no-deps ensures only direct dependencies are installed
# and helps with caching, but for complex requirements.txt, sometimes it's better without.
# Given your large list, let's stick to standard pip install
RUN pip install -r requirements.txt

# Copy the rest of your application code
COPY . .

# Optional: Set a default command or entrypoint if your container is meant to run something directly
# For a development container, this might not be necessary as you'll run commands interactively
# CMD ["python", "your_pipeline_script.py"]