#!/bin/bash
IMAGE_NAME="gcr.io/datascience-projects/sec-scraper"
REGION="us-central1"
# Change these three variables to target different quarters
TARGET_YEAR="2024"
TARGET_QTR="3"
TARGET_LIMIT="5000"

echo "🛠️ Submitting build to Google Cloud Build..."
gcloud builds submit --tag $IMAGE_NAME .

echo "📦 Deploying Cloud Run Job: form4-quarterly-backfill..."

# Deploy as a Cloud Run JOB (better for long-running sync tasks)
gcloud run jobs deploy form4-quarterly-backfill \
  --image $IMAGE_NAME \
  --command "python" \
  --args "scraper_job_sync_quarterly.py" \
  --region $REGION \
  --tasks 1 \
  --max-retries 0 \
  --set-env-vars \
AGENT_YEAR=$TARGET_YEAR,\
AGENT_QTR=$TARGET_QTR,\
AGENT_LIMIT=$TARGET_LIMIT,\
GOOGLE_CLOUD_PROJECT=datascience-projects \
  --task-timeout=24h \
  --memory=512Mi \
  --cpu=1

echo "✅ Deployment Complete for $TARGET_YEAR Q$TARGET_QTR."
echo "🏃 To start the job now, run: gcloud run jobs execute form4-quarterly-backfill --region $REGION"