#!/bin/bash

# Configuration
PROJECT_ID="datascience-projects"
IMAGE_NAME="gcr.io/$PROJECT_ID/sec-13f-scraper"
REGION="us-central1"
JOB_NAME="sec-13f-worker-job"

# Set target quarter and limit
TARGET_YEAR=2020
TARGET_QTR=1
LIMIT=25  # Set to 0 for unlimited, or higher for "Full Blow"

echo "🛠️  Building and Pushing Image: $IMAGE_NAME..."

# 1. Build and push the image to GCR
gcloud builds submit --tag $IMAGE_NAME .

echo "📦  Deploying Cloud Run Job: $JOB_NAME..."

# 2. Deploy the 13F Worker (Job)
# We use --service-account if you have a specific one, otherwise it uses default compute
gcloud run jobs deploy $JOB_NAME \
  --image $IMAGE_NAME \
  --command "python" \
  --args "scraper.py" \
  --region $REGION \
  --tasks 1 \
  --max-retries 0 \
  --cpu 1 \
  --memory 2Gi \
  --set-env-vars YEAR=$TARGET_YEAR,QUARTER=$TARGET_QTR,SCRAPER_LIMIT=$LIMIT

echo "🚀 Deployment Complete. Kicking off the 13F Scraper Job now..."

# 3. Execute the Job immediately
gcloud run jobs execute $JOB_NAME --region $REGION

echo "✅ Job started. Monitor logs with: gcloud beta run jobs executions describe [EXECUTION_NAME] --region $REGION"