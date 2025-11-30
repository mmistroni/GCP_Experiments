# Start from the base Python image
FROM python:3.10-slim-bullseye

# Set the working directory
WORKDIR /workspaces/GCP_Experiments

# Step 1: Install OS Dependencies (Including python3-venv, build tools, etc.)
# Note: I am assuming you need 'build-essential' based on your original log for any compiled packages.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        git \
        curl \
        build-essential \
        python3-venv \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Step 2: Create Venv and Install Python Dependencies
# Use 'python' which is aliased to 'python3' in the official Python Docker image.
# We also use explicit full paths to the pip binary inside the new Venv for robustness.
COPY requirements.txt .
RUN python -m venv .venv && \
    /workspaces/GCP_Experiments/.venv/bin/pip install --no-cache-dir -r requirements.txt

# Step 3: Switch to a Non-Root User (Recommended Security Practice)
# The default non-root user in Codespaces is 'vscode' (UID/GID 1000).
RUN groupadd -g 1000 vscode && useradd -u 1000 -g 1000 -m vscode
RUN chown -R vscode:vscode /workspaces/GCP_Experiments
USER vscode