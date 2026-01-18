#!/bin/bash
set -e # Exit on error

PROJECT_ID=$(gcloud config get-value project)
REGION="us-central1"
SERVICE_NAME="sec-13f-gateway"
JOB_NAME="sec-13f-worker-job"
IMAGE_URL="gcr.io/$PROJECT_ID/sec-13f-unified"

# 1. Build the Unified Image
echo "🛠️ Building container image..."
gcloud builds submit --tag $IMAGE_URL

# 2. Deploy/Update the Gateway Service
echo "🚀 Deploying Gateway (API)..."
gcloud run deploy $SERVICE_NAME \
  --image $IMAGE_URL \
  --region $REGION \
  --allow-unauthenticated \
  --set-env-vars PROJECT_ID=$PROJECT_ID,JOB_NAME=$JOB_NAME,REGION=$REGION

# 3. Create/Update the Worker Job
echo "🏗️ Ensuring Worker Job exists..."
gcloud run jobs deploy $JOB_NAME \
  --image $IMAGE_URL \
  --command "/entrypoint.sh" \
  --args "job" \
  --tasks 1 \
  --max-retries 0 \
  --task-timeout 3600 \
  --memory 2Gi \
  --cpu 1 \
  --region $REGION \
  --set-env-vars PROJECT_ID=$PROJECT_ID