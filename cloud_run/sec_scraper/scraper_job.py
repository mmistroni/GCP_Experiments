import os
import sys
import time
from google.cloud import bigquery

import os
import requests
from google.cloud import bigquery

# SEC requires a specific User-Agent
HEADERS = {'User-Agent': 'YourName your@email.com'}

def run_scraper(year: int, qtr: int):
    client = bigquery.Client()
    table_id = "datascience-projects.your_dataset.your_table"

    print(f"🕵️ Fetching SEC index for {year} Q{qtr}...")
    index_url = f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{qtr}/master.idx"
    
    response = requests.get(index_url, headers=HEADERS)
    if response.status_code != 200:
        print(f"❌ Failed to fetch index: {response.status_code}")
        return

    # Logic to parse the index and find 13F filings...
    # For each filing found:
    # 1. Download XML
    # 2. Parse XML
    # 3. Prepare row for BigQuery
    
    rows_to_insert = [
        {"filing_date": "2025-01-18", "ticker": "AAPL", "shares": 1000}, # Example row
    ]

    errors = client.insert_rows_json(table_id, rows_to_insert)
    if not errors:
        print("✅ New rows have been added.")
    else:
        print(f"❌ Errors encountered: {errors}")


if __name__ == "__main__":
    # Pull variables injected by the Manager
    year_env = os.getenv("YEAR")
    qtr_env = os.getenv("QTR")

    if not year_env or not qtr_env:
        print("❌ CRITICAL ERROR: YEAR or QTR environment variables not found.")
        sys.exit(1)

    try:
        run_scraper(year=int(year_env), qtr=int(qtr_env))
    except Exception as e:
        print(f"❌ SCRAPER CRASHED: {str(e)}")
        sys.exit(1)