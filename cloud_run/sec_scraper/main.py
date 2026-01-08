from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional
import logging

# Assuming your project structure: cloud_run/sec_scraper/scraper.py
from cloud_run.sec_scraper.scraper import run_master_scraper

app = FastAPI(title="SEC 13F Smart Scraper Service")

# --- MODELS ---
class ScrapeRequest(BaseModel):
    # Optional fields allow the "Smart Logic" to take over if they are missing
    year: Optional[int] = Field(None, ge=2021, le=2026)
    qtr: Optional[int] = Field(None, ge=1, le=4)
    limit: int = Field(default=10000, gt=0)

# --- UTILS ---
def calculate_target_period():
    """
    Calculates the quarter to scrape based on the current date.
    March (3) -> Q4 of prev year
    June (6) -> Q1 of current year
    Sept (9) -> Q2 of current year
    Dec (12) -> Q3 of current year
    """
    now = datetime.now()
    month = now.month
    year = now.year

    if month == 3:   # Running in March
        return year - 1, 4
    elif month == 6: # Running in June
        return year, 1
    elif month == 9: # Running in Sept
        return year, 2
    elif month == 12: # Running in Dec
        return year, 3
    else:
        # Fallback logic if run outside the target months: 
        # Get the most recently completed quarter
        target_qtr = (month - 1) // 3
        target_year = year
        if target_qtr == 0:
            target_qtr = 4
            target_year -= 1
        return target_year, target_qtr

# --- ENDPOINTS ---
@app.post("/scrape")
async def trigger_scrape(background_tasks: BackgroundTasks, request: Optional[ScrapeRequest] = None):
    """
    Triggers the 13F scraper. If year/qtr are omitted, it calculates them
    automatically based on the current reporting cycle.
    """
    # Initialize values
    if request is None or (request.year is None and request.qtr is None):
        target_year, target_qtr = calculate_target_period()
        limit = request.limit if request else 10000
    else:
        target_year = request.year
        target_qtr = request.qtr
        limit = request.limit

    logging.info(f"Targeting Year: {target_year}, Qtr: {target_qtr} (Limit: {limit})")

    # Add to FastAPI BackgroundTasks for Cloud Run safety
    background_tasks.add_task(
        run_master_scraper, 
        year=target_year, 
        qtr=target_qtr, 
        limit=limit
    )

    return {
        "status": "Accepted",
        "message": f"Scraper logic initiated for {target_year} Q{target_qtr}",
        "executed_at": datetime.now().isoformat(),
        "params": {
            "year": target_year,
            "qtr": target_qtr,
            "limit": limit
        }
    }

@app.get("/health")
def health_check():
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}