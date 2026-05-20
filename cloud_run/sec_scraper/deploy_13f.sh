#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# --- GLOBAL CONFIGURATION ---
PROJECT_ID="datascience-projects"
IMAGE_NAME="gcr.io/$PROJECT_ID/sec-13f-scraper"
REGION="us-central1"

# --- JOB CONFIGURATIONS ---
QUARTERLY_JOB_NAME="sec-13f-worker-job"
DAILY_JOB_NAME="sec-13f-daily-job"

# Parameters for the quarterly job run
TARGET_YEAR=2020
TARGET_QTR=1
LIMIT=25

echo "🛠️  Building and Pushing Shared Container Image: $IMAGE_NAME..."
# 1. Build and push the unified image to Artifact Registry / GCR
gcloud builds submit --tag $IMAGE_NAME .

echo "--------------------------------------------------------"
echo "📦  Deploying Quarterly Cloud Run Job: $QUARTERLY_JOB_NAME..."
echo "--------------------------------------------------------"

# 2. Deploy/Update the Quarterly 13F Worker Job
gcloud run jobs deploy $QUARTERLY_JOB_NAME \
  --image $IMAGE_NAME \
  --command "python" \
  --args "scraper.py" \
  --region $REGION \
  --tasks 1 \
  --max-retries 0 \
  --cpu 1 \
  --memory 2Gi \
  --set-env-vars YEAR=$TARGET_YEAR,QUARTER=$TARGET_QTR,SCRAPER_LIMIT=$LIMIT

echo "--------------------------------------------------------"
echo "📦  Deploying Daily Cloud Run Job: $DAILY_JOB_NAME..."
echo "--------------------------------------------------------"

# 3. Deploy/Update the Daily 13F Worker Job using the alternative script argument
gcloud run jobs deploy $DAILY_JOB_NAME \
  --image $IMAGE_NAME \
  --command "python" \
  --args "scraper_13f_daily.py" \
  --region $REGION \
  --tasks 1 \
  --max-retries 0 \
  --cpu 1 \
  --memory 2Gi

echo "🚀 Deployment Complete for Both Tasks!"
echo "💡 To execute the daily job manually: gcloud run jobs execute $DAILY_JOB_NAME --region $REGION"