#!/bin/bash

set -e

JOB_NAME="trump-scraper-job"
REGION="europe-west2"

echo "============================================================"
echo "⚡ Triggering Cloud Run Job Execution Instance..."
echo "============================================================"

# Trigger execution thread
gcloud run jobs execute "$JOB_NAME" --region "$REGION"

echo "============================================================"
echo "🎯 Job dispatched! Monitor live output inside Cloud Logging."
echo "============================================================"