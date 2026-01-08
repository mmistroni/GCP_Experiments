from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel, Field
from cloud_run.sec_scraper.scraper import run_master_scraper

app = FastAPI(title="SEC 13F Scraper Service")

class ScrapeRequest(BaseModel):
    year: int = Field(..., ge=2021, le=2026)
    qtr: int = Field(..., ge=1, le=4)
    limit: int = Field(default=10000, gt=0)

@app.post("/scrape")
async def trigger_scrape(request: ScrapeRequest, background_tasks: BackgroundTasks):
    """
    Triggers the 13F scraper for a specific year and quarter.
    """
    # BackgroundTasks allows the API to return '202' immediately
    background_tasks.add_task(
        run_master_scraper, 
        year=request.year, 
        qtr=request.qtr, 
        limit=request.limit
    )
    return {
        "status": "Accepted",
        "message": f"Scraper started for {request.year} Q{request.qtr}",
        "params": request.dict()
    }

@app.get("/health")
def health_check():
    return {"status": "healthy"}