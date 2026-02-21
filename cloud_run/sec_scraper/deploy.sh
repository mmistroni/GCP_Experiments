#!/bin/bash
IMAGE_NAME="gcr.io/datascience-projects/sec-scraper"

# 1. Build and push the image
gcloud builds submit --tag $IMAGE_NAME .

# 2. Deploy the Web App (Gateway)
# Adding ENV vars here so the Uvicorn app knows the default target
gcloud run deploy sec-13f-gateway \
  --image $IMAGE_NAME \
  --region us-central1 \
  --allow-unauthenticated \
  --set-env-vars YEAR=2020,QTR=1

# 3. Deploy the 13F Worker (Job)
# Jobs are better for long-running scrapers than Services
gcloud run jobs deploy sec-13f-worker-job \
  --image $IMAGE_NAME \
  --command "python" \
  --args "scraper.py" \
  --region us-central1 \
  --tasks 1 \
  --max-retries 0 \
  --set-env-vars YEAR=2020,QTR=1

# 4. Deploy the Form 4 Backfill Worker (Job)
gcloud run jobs deploy form4-manual-worker-job \
  --image $IMAGE_NAME \
  --command "python" \
  --args "scraper_job_form4.py" \
  --region us-central1 \
  --tasks 1 \
  --max-retries 0 \
  --set-env-vars YEAR=2020,QTR=1

echo "✅ Deployment Complete. You can now execute the job from the console or via CLI."