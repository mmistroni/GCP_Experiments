import os
import sys
# Import the actual logic from your existing scraper.py
from scraper_form4 import run_form4_job
import logging

if __name__ == "__main__":
    # Get the instructions from the Cloud Run Job environment
    try:
        # Just call your real scraper function
        # It will use the Table ID already defined in scraper.py
        run_form4_job()
    except Exception as e:
        print(f"❌ SCRAPER CRASHED: {str(e)}")
        sys.exit(1)