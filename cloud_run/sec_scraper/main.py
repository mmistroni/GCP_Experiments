import os
from fastapi import FastAPI, HTTPException
from google.cloud import run_v2

app = FastAPI()

# Make sure these match your actual setup
PROJECT_ID = os.getenv("PROJECT_ID", "datascience-projects")
REGION = "us-central1"
JOB_NAME = "sec-13f-worker-job"

@app.post("/scrape")
async def trigger_scrape(year: int, qtr: int):
    # The client needs the PROJECT_ID to initialize
    client = run_v2.JobsClient()
    
    parent = f"projects/{PROJECT_ID}/locations/{REGION}/jobs/{JOB_NAME}"
    
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
        # Construct the formal request message
        request = run_v2.RunJobRequest(
            name=parent,
            overrides=overrides
        )
        
        operation = client.run_job(request=request)
        return {
            "status": "Job Started", 
            "execution_id": operation.operation.name,
            "details": f"Scraping {year} Q{qtr}"
        }
    except Exception as e:
        # Print the error to logs so you can see it in Cloud Run
        print(f"ERROR: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to launch job: {str(e)}")