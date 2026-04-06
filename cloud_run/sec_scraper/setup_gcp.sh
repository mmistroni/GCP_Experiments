#!/bin/bash

# 1. Define the pathing
CONF_DIR="/workspaces/GCP_Experiments"
KEY_FILE="$CONF_DIR/gcp_key.json"

# 2. Ensure the directory exists
mkdir -p "$CONF_DIR"

# 3. Write the Service Account Key from your Secret/Env Var
if [ -z "$GCP_SA_KEY" ]; then
    echo "⚠️ Warning: GCP_SA_KEY environment variable is empty!"
else
    printf '%s' "$GCP_SA_KEY" > "$KEY_FILE"
    echo "✅ GCP Key file written to $KEY_FILE"
fi

# 4. Export the Variables
export GOOGLE_APPLICATION_CREDENTIALS="$KEY_FILE"
export GOOGLE_CLOUD_PROJECT="datascience-projects"

# 5. Confirmation
echo "🚀 Environment Variables Set:"
echo "   PROJECT: $GOOGLE_CLOUD_PROJECT"
echo "   AUTH_PATH: $GOOGLE_APPLICATION_CREDENTIALS"