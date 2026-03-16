#!/bin/bash
IMAGE_NAME="gcr.io/datascience-projects/sec-scraper"
REGION="us-central1"

# 1. Build and push the image
gcloud builds submit --tag $IMAGE_NAME .

# 4. Deploy the Form 4 Backfill Worker (Job)
gcloud run jobs deploy form4-manual-worker-job \
  --image $IMAGE_NAME \
  --command "python" \
  --args "scraper_job_form4.py" \
  --region $REGION \
  --tasks 1 \
  --max-retries 0 \
  --set-env-vars AGENT_MODE=DAILY,GOOGLE_CLOUD_PROJECT=datascience-projects \
  --task-timeout=3h \
  
echo "🚀 Deployment Complete. Kicking off the 13F Scraper Job now..."
