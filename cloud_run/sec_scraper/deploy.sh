#!/bin/bash
IMAGE_NAME="gcr.io/datascience-projects/sec-scraper"

# 1. Build and push the image
gcloud builds submit --tag $IMAGE_NAME .

# 2. Deploy the Web App (Manager)
gcloud run deploy sec-13f-gateway \
  --image $IMAGE_NAME \
  --region us-central1 \
  --allow-unauthenticated

# 3. Deploy the Worker (Chef)
# Note: We override the entrypoint to run the scraper script specifically
gcloud run jobs deploy sec-13f-worker-job \
  --image $IMAGE_NAME \
  --command "python" \
  --args "scraper_job.py" \
  --region us-central1 \
  --tasks 1 \
  --max-retries 0


# 4. Deploy the f orm4 backfill Worker (Chef)
# Note: We override the entrypoint to run the scraper script specifically
gcloud run jobs deploy form4-backfill-worker-job \
  --image $IMAGE_NAME \
  --command "python" \
  --args "scraper_form4_backfill.py" \
  --region us-central1 \
  --tasks 1 \

# 5. Deploy the f orm4 backfill Worker (Chef)
# Note: We override the entrypoint to run the scraper script specifically
gcloud run jobs deploy form4-manual-worker-job \
  --image $IMAGE_NAME \
  --command "python" \
  --args "scraper_form4.py" \
  --region us-central1 \
  --tasks 1 \
