#!/bin/bash

# 1. Auto-detect Project ID from local gcloud config
PROJECT_ID=$(gcloud config get-value project)
REGION="us-central1"
SERVICE_NAME="sec-13f-scraper"
IMAGE_URL="gcr.io/$PROJECT_ID/$SERVICE_NAME"

if [ -z "$PROJECT_ID" ]; then
    echo "❌ Error: No Google Cloud Project detected. Run 'gcloud config set project [PROJECT_ID]' first."
    exit 1
fi

# 2. Auto-detect the Default Compute Service Account
# This is usually [project-number]-compute@developer.gserviceaccount.com
SERVICE_ACCOUNT=$(gcloud iam service-accounts list \
    --filter="displayName:Default compute service account" \
    --format="value(email)")

echo "🏗️  Environment Detected:"
echo "   - Project: $PROJECT_ID"
echo "   - Region:  $REGION"
echo "   - Account: $SERVICE_ACCOUNT"
echo "------------------------------------------------"

# 3. Enable necessary Google APIs (in case this is a fresh project)
echo "🔌 Ensuring APIs are enabled..."
gcloud services enable run.googleapis.com cloudbuild.googleapis.com

# 4. Build the image using Cloud Build (remote build)
echo "🛠️  Building container image for $IMAGE_URL..."
gcloud builds submit --tag $IMAGE_URL

# 5. Deploy to Cloud Run with optimized settings for SEC scraping
echo "🚀 Deploying $SERVICE_NAME to Cloud Run..."
gcloud run deploy $SERVICE_NAME \
  --image $IMAGE_URL \
  --platform managed \
  --region $REGION \
  --service-account $SERVICE_ACCOUNT \
  --timeout 3600 \
  --memory 2Gi \
  --cpu 1 \
  --allow-unauthenticated \
  --update-env-vars PROJECT_ID=$PROJECT_ID

echo "------------------------------------------------"
echo "✅ Deployment Successful!"
echo "🔗 Service URL: $(gcloud run services describe $SERVICE_NAME --platform managed --region $REGION --format='value(status.url)')"