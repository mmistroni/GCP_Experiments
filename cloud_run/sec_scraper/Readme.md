This code is being build with Bard/Gemini from local laptop

we need to run this command to enable gcloud runs locally
printf '%s' "$GCP_SA_KEY" > /workspaces/GCP_Experiments/gcp_key.json
export GOOGLE_APPLICATION_CREDENTIALS="/workspaces/GCP_Experiments/gcp_key.json"
export GOOGLE_CLOUD_PROJECT="datascience-projects"