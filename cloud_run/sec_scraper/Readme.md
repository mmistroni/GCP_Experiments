This code is being build with Bard/Gemini from local laptop

we need to run this command to enable gcloud runs locally
echo $GCP_SA_KEY > /workspaces/gcp_key.json
export GOOGLE_APPLICATION_CREDENTIALS="/workspaces/GCP_Experiments/gcp_key.json"
export GOOGLE_CLOUD_PROJECT="datascience-projects"