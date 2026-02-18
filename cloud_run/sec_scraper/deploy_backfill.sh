#!/bin/bash

# --- CONFIGURATION ---
IMAGE_NAME="gcr.io/datascience-projects/sec-scraper"
REGION="us-central1"
JOB_NAME="form4-backfill-worker-job"

# 1. Build and push the image 
# (Ensures the BigQuery MERGE and 'issuer' fixes are included)
echo "🏗️ Building and pushing image..."
gcloud builds submit --tag $IMAGE_NAME .

# 2. Deploy the Form 4 BACKFILL Worker
# AGENT_MODE=BACKFILL triggers the historical logic
# AGENT_LIMIT=0 means "get everything available"
echo "🚀 Deploying Backfill Job: $JOB_NAME"
gcloud run jobs deploy $JOB_NAME \
  --image $IMAGE_NAME \
  --command "python" \
  --args "scraper_job_form4.py" \
  --region $REGION \
  --tasks 1 \
  --max-retries 0 \
  --task-timeout=3600s \
  --cpu=2 \
  --memory=2Gi \
  --set-env-vars AGENT_MODE=BACKFILL,AGENT_YEARS=2024,AGENT_LIMIT=0

# 3. Trigger the execution immediately
echo "🎬 Starting the backfill execution now..."
gcloud run jobs execute $JOB_NAME --region $REGION