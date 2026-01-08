#!/bin/bash

# Configuration
PROJECT_ID="datascience-projects"
SERVICE_NAME="sec-13f-scraper"
REGION="us-central1"
IMAGE_URL="gcr.io/$PROJECT_ID/$SERVICE_NAME"

echo "🛠️ Building container image..."
gcloud builds submit --tag $IMAGE_URL

echo "🚀 Deploying to Cloud Run..."
gcloud run deploy $SERVICE_NAME \
  --image $IMAGE_URL \
  --platform managed \
  --region $REGION \
  --allow-unauthenticated \
  --memory 2Gi \
  --cpu 1 \
  --timeout 3600 \
  --service-account="your-service-account@$PROJECT_ID.iam.gserviceaccount.com"

echo "✅ Deployment Complete!"