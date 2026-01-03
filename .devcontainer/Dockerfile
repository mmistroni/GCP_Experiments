# Start from the base Python image
FROM python:3.10-slim-bullseye

# 1. Install OS Dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        git \
        curl \
        build-essential \
        python3-venv \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 2. Setup User early to handle permissions
RUN groupadd -g 1000 vscode && useradd -u 1000 -g 1000 -m vscode

# 3. Set the working directory
WORKDIR /workspaces/GCP_Experiments

# 4. Handle Requirements and Venv
# Because "context": ".." is set in devcontainer.json, 
# Docker looks at the repo root.
COPY requirements.txt .

RUN python -m venv .venv && \
    .venv/bin/pip install --no-cache-dir --upgrade pip && \
    .venv/bin/pip install --no-cache-dir -r requirements.txt

# 5. Fix permissions so the 'vscode' user can use the venv
RUN chown -R vscode:vscode /workspaces/GCP_Experiments

USER vscode