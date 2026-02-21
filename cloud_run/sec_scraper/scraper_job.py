import os
import sys
# Import the actual logic from your existing scraper.py
from scraper import run_master_scraper 
import logging

if __name__ == "__main__":
    # Get the instructions from the Cloud Run Job environment
    year_env = os.getenv("YEAR")
    qtr_env = os.getenv("QTR")
    logging.info(f'Running for {year_env}/{qtr_env}')
    if not year_env or not qtr_env:
        print("❌ CRITICAL ERROR: YEAR or QTR environment variables not found.")
        sys.exit(1)

    try:
        # Just call your real scraper function
        # It will use the Table ID already defined in scraper.py
        run_master_scraper()
    except Exception as e:
        print(f"❌ SCRAPER CRASHED: {str(e)}")
        sys.exit(1)