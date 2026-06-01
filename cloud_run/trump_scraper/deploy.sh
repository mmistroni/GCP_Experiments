#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# --- CONFIGURATION VARIABLES ---
JOB_NAME="trump-scraper-job"
REGION="europe-west2"
MEMORY="2Gi"
CPU="1"
TASKS=1
MAX_RETRIES=2

echo "============================================================"
echo "🚀 Initializing Google Cloud Run Job Deployment Source..."
echo "============================================================"

# Execute source deployment through Cloud Build
gcloud run jobs deploy "$JOB_NAME" \
    --source . \
    --tasks "$TASKS" \
    --max-retries "$MAX_RETRIES" \
    --memory "$MEMORY" \
    --cpu "$CPU" \
    --region "$REGION"

echo "============================================================"
echo "✅ Deployment Successful! Job configured."
echo "============================================================"