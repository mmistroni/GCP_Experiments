import os
from fastapi import FastAPI, HTTPException
from google.cloud import run_v2

app = FastAPI()

# Config - Adjust these to your actual IDs
PROJECT_ID = "datascience-projects"
REGION = "us-central1"
JOB_NAME = "sec-13f-worker-job"

@app.post("/scrape")
async def trigger_scrape(year: int, qtr: int):
    client = run_v2.JobsClient()
    parent = f"projects/{PROJECT_ID}/locations/{REGION}/jobs/{JOB_NAME}"
    
    # Injecting parameters as Environment Variables for the Worker
    overrides = {
        "container_overrides": [
            {
                "env": [
                    {"name": "YEAR", "value": str(year)},
                    {"name": "QTR", "value": str(qtr)}
                ]
            }
        ]
    }
    
    try:
        request = run_v2.RunJobRequest(name=parent, overrides=overrides)
        operation = client.run_job(request=request)
        return {
            "status": "Worker Dispatched",
            "execution": operation.operation.name,
            "message": f"Scraping {year} Q{qtr} in the background."
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))