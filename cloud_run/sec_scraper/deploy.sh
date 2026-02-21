#!/bin/bash
IMAGE_NAME="gcr.io/datascience-projects/sec-scraper"
REGION="us-central1"

# 1. Build and push the image
gcloud builds submit --tag $IMAGE_NAME .

# 2. Deploy the Web App (Gateway)
gcloud run deploy sec-13f-gateway \
  --image $IMAGE_NAME \
  --region $REGION \
  --allow-unauthenticated \
  --set-env-vars YEAR=2020,QTR=1

# 3. Deploy the 13F Worker (Job)
gcloud run jobs deploy sec-13f-worker-job \
  --image $IMAGE_NAME \
  --command "python" \
  --args "scraper.py" \
  --region $REGION \
  --tasks 1 \
  --max-retries 0 \
  --set-env-vars YEAR=2020,QTR=1

# 4. Deploy the Form 4 Backfill Worker (Job)
gcloud run jobs deploy form4-manual-worker-job \
  --image $IMAGE_NAME \
  --command "python" \
  --args "scraper_job_form4.py" \
  --region $REGION \
  --tasks 1 \
  --max-retries 0 \
  --set-env-vars YEAR=2020,QTR=1

echo "🚀 Deployment Complete. Kicking off the 13F Scraper Job now..."

# 5. EXECUTE THE JOB IMMEDIATELY
# We specify the region here to avoid the prompt.
# We use --update-env-vars to ensure it starts exactly on the target quarter.
gcloud run jobs execute sec-13f-worker-job \
  --region $REGION \
  --update-env-vars YEAR=2020,QTR=1

echo "✅ Job started in $REGION. Check Google Cloud Console for live logs."