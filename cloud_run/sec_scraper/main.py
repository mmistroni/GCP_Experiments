from fastapi import FastAPI, BackgroundTasks
from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional
import logging

from cloud_run.sec_scraper.scraper import run_master_scraper

app = FastAPI(title="SEC 13F Scraper: Manual & Smart Mode")

# --- MODELS ---
class ScrapeRequest(BaseModel):
    # Notice we use Optional here to allow the override
    year: Optional[int] = Field(None, ge=2021, le=2026)
    qtr: Optional[int] = Field(None, ge=1, le=4)
    limit: int = Field(default=10000, gt=0)

# --- SMART LOGIC HELPER ---
def get_automated_period():
    now = datetime.now()
    month = now.month
    year = now.year

    if month == 3:   return year - 1, 4
    elif month == 6: return year, 1
    elif month == 9: return year, 2
    elif month == 12: return year, 3
    else:
        # Fallback: get the most recently completed quarter
        target_qtr = (month - 1) // 3
        return (year, target_qtr) if target_qtr > 0 else (year - 1, 4)

# --- ENDPOINT ---
@app.post("/scrape")
async def trigger_scrape(background_tasks: BackgroundTasks, request: Optional[ScrapeRequest] = None):
    """
    Priority:
    1. Manual Override (if year/qtr provided in JSON)
    2. Smart Logic (if JSON is empty or fields are null)
    """
    # 1. Check for Manual Override
    if request and request.year and request.qtr:
        target_year = request.year
        target_qtr = request.qtr
        mode = "MANUAL OVERRIDE"
    else:
        # 2. Apply Smart Logic
        target_year, target_qtr = get_automated_period()
        mode = "SMART LOGIC"

    limit = request.limit if request else 10000

    logging.info(f"Running in {mode} for {target_year} Q{target_qtr}")

    background_tasks.add_task(
        run_master_scraper, 
        year=target_year, 
        qtr=target_qtr, 
        limit=limit
    )

    return {
        "status": "Accepted",
        "mode": mode,
        "target": f"{target_year} Q{target_qtr}",
        "limit": limit
    }