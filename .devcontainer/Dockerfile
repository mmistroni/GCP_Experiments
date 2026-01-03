FROM python:3.10-slim-bullseye

# Install OS Dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        git curl build-essential python3-venv \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Setup User
RUN groupadd -g 1000 vscode && useradd -u 1000 -g 1000 -m vscode
WORKDIR /workspaces/GCP_Experiments
RUN chown -R vscode:vscode /workspaces/GCP_Experiments

USER vscode