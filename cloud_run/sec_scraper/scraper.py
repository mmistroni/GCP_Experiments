import time
import logging
import requests
import pandas as pd
from lxml import etree
from google.cloud import bigquery
import os
import sys

# 1. FORCE UNBUFFERED LOGGING
# Ensures logs appear in Real-Time in the Google Cloud Console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    stream=sys.stdout 
)
logger = logging.getLogger("13f_feature_agent")

HEADERS = {
    'User-Agent': 'Institutional Research mmapplausetest@gmail.com', 
    'Accept-Encoding': 'gzip, deflate, br',
    'Host': 'www.sec.gov',
    'Connection': 'keep-alive'
}


def get_with_retry(session, url, name="Request"):
    """Enhanced retry with mandatory governor and aggressive backoff."""
    for attempt in range(1, 4):
        try:
            # 🏎️ THE GOVERNOR: Mandatory 0.15s sleep before EVERY request.
            # This keeps you at ~6.6 requests per second, safely below the 10 limit.
            time.sleep(0.15) 

            logger.info(f"📡 {name} | Attempt {attempt} | URL: {url}")
            start_time = time.time()
            
            res = session.get(url, timeout=15)
            duration = time.time() - start_time
            
            # SEC sometimes returns 200 with a "Rate Limit" HTML body
            is_rate_limited_html = res.status_code == 200 and b"Request Rate Threshold Exceeded" in res.content

            if res.status_code == 200 and not is_rate_limited_html:
                if len(res.content) > 200: # Slightly higher threshold for safety
                    logger.info(f"📥 {name} | Status: 200 | Time: {duration:.2f}s | Size: {len(res.content)}")
                    return res
                else:
                    logger.warning(f"⚠️ {name} | Tiny/Empty response. Content: {res.text[:100]}")
            
            elif res.status_code == 429 or is_rate_limited_html:
                # 🛑 AGGRESSIVE BACKOFF: Wait longer on Cloud Run
                wait_time = 10 * attempt 
                logger.warning(f"⏳ {name} | RATE LIMIT (429). Sleeping {wait_time}s...")
                time.sleep(wait_time)
            
            elif res.status_code == 403:
                logger.error(f"🚫 {name} | 403 FORBIDDEN. Check User-Agent or IP.")
                return None
            
        except requests.exceptions.Timeout:
            logger.error(f"⏳ {name} | TIMEOUT. SEC is slow.")
            time.sleep(5)
        except Exception as e:
            logger.error(f"💥 {name} | Unexpected Error: {str(e)}")
            
    return None

def run_master_scraper(year: int, qtr: int, limit: int = 10000):
    client = bigquery.Client()
    table_id = f"{client.project}.gcp_shareloader.all_holdings_master"
    
    # Initialize Persistent Session
    session = requests.Session()
    session.headers.update(HEADERS)

    # --- PHASE 1: Master Index ---
    idx_url = f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{qtr}/master.idx"
    logger.info(f"🔍 PHASE 1: Fetching Master Index for {year} Q{qtr}")
    
    idx_res = get_with_retry(session, idx_url, "MasterIndex")
    if not idx_res:
        logger.critical("💀 Critical Failure: Could not retrieve Master Index. Exiting job.")
        return

    lines = [l for l in idx_res.text.splitlines() if '13F-HR' in l][:limit]
    logger.info(f"📊 PHASE 2: Processing {len(lines)} filings.")

    # --- PHASE 2: Loop ---
    batch = []
    for i, line in enumerate(lines):
        parts = line.split('|')
        cik, name, path = parts[0], parts[1], parts[4]
        acc = path.split('/')[-1].replace('.txt', '').replace('-', '')

        logger.info(f"🔄 [{i+1}/{len(lines)}] Start Processing: {name} (CIK: {cik})")
        
        # Directory Check
        dir_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc}/index.json"
        dir_res = get_with_retry(session, dir_url, f"Dir-{acc}")
        
        if not dir_res: continue

        # (Parsing Logic remains the same, but add log before BQ upload)
        # ...
        
        if len(batch) >= 500:
            logger.info(f"💾 DATABASE: Uploading batch of {len(batch)} to {table_id}")
            # client.load_table_from_dataframe(...)
            batch = []

    logger.info("🏁 JOB COMPLETE")

if __name__ == "__main__":
    run_master_scraper(year=2025, qtr=1)