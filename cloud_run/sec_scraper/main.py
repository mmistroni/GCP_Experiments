import os
from fastapi import FastAPI, HTTPException
from google.cloud import run_v2
from typing import Optional

app = FastAPI()

# Config - Adjust these to your actual IDs
PROJECT_ID = "datascience-projects"
REGION = "us-central1"
JOB_NAME = "sec-13f-worker-job"
JOB_NAME_FORM4_BACKFILL = "form4-backfill-worker-job"
JOB_NAME_FORM4_MANUAL = "form4-manual-worker-job"

@app.post("/scrape")
async def trigger_scrape(year: Optional[int], qtr: Optional[int]):
    client = run_v2.JobsClient()
    parent = f"projects/{PROJECT_ID}/locations/{REGION}/jobs/{JOB_NAME}"
    
    # 1. INFER Year and Quarter if not provided
    if year is None or qtr is None:
        now = datetime.datetime.now()
        # Today is Jan 21, 2026. We want to scrape Q4 of 2025.
        if 1 <= now.month <= 3:    # Jan-Mar -> Get Q4 of prev year
            target_year = now.year - 1
            target_qtr = 4
        elif 4 <= now.month <= 6:  # Apr-Jun -> Get Q1
            target_year = now.year
            target_qtr = 1
        elif 7 <= now.month <= 9:  # Jul-Sep -> Get Q2
            target_year = now.year
            target_qtr = 2
        else:                      # Oct-Dec -> Get Q3
            target_year = now.year
            target_qtr = 3
    else:
        # Use what was passed manually
        target_year = year
        target_qtr = qtr




    # Injecting parameters as Environment Variables for the Worker
    overrides = {
        "container_overrides": [
            {
                "env": [
                    {"name": "YEAR", "value": str(target_year)},
                    {"name": "QTR", "value": str(target_qtr)}
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
            "message": f"Scraping {target_year} Q{target_qtr} in the background."
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/form4_backfill")
async def trigger_scrape(year: Optional[int], qtr: Optional[int]):
    client = run_v2.JobsClient()
    parent = f"projects/{PROJECT_ID}/locations/{REGION}/jobs/{JOB_NAME_FORM4_BACKFILL}"

    try:
        request = run_v2.RunJobRequest(name=parent)
        operation = client.run_job(request=request)
        return {
            "status": "Worker Dispatched",
            "execution": operation.operation.name,
            "message": f"Scraping form4 bkfill"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/form4_manual")
async def trigger_scrape(year: Optional[int], qtr: Optional[int]):
    client = run_v2.JobsClient()
    parent = f"projects/{PROJECT_ID}/locations/{REGION}/jobs/{JOB_NAME_FORM4_MANUAL}"

    try:
        request = run_v2.RunJobRequest(name=parent)
        operation = client.run_job(request=request)
        return {
            "status": "Worker Dispatched",
            "execution": operation.operation.name,
            "message": f"Scraping form4 DAILY"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))