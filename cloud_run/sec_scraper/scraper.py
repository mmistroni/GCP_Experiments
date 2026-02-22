import os
import requests
import logging
from lxml import etree
from google.cloud import bigquery
from requests.exceptions import ReadTimeout, ConnectTimeout

# Configuration
PROJECT_ID = "datascience-projects"
DATASET_ID = "gcp_shareloader"
QUEUE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.scraping_queue"
MASTER_TABLE = f"{PROJECT_ID}.{DATASET_ID}.all_holdings_master"
HEADERS = {'User-Agent': 'YourName/1.0 (your@email.com)'}

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

client = bigquery.Client(project=PROJECT_ID)

def seed_queue_from_sec(year, qtr):
    """Fills the queue with strict typing and autodetect off."""
    idx_url = f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{qtr}/master.idx"
    res = requests.get(idx_url, headers=HEADERS)
    lines = res.text.split('\n')
    
    new_rows = []
    for line in lines[10:]:
        if "|13F-HR|" in line:
            parts = line.split('|')
            if len(parts) < 5: continue
            
            # CIK and Accession MUST be strings
            new_rows.append({
                "cik": str(parts[0]).strip().zfill(10),
                "company_name": str(parts[1]).strip(),
                "accession_number": str(parts[4].split('/')[-1].replace('.txt', '')),
                "dir_url": f"https://www.sec.gov/Archives/{parts[4].replace('.txt', '').replace('-', '')}/index.json",
                "status": "pending",
                "year": int(year),
                "qtr": int(qtr),
                "retries": 0
            })

    if new_rows:
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("cik", "STRING"),
                bigquery.SchemaField("company_name", "STRING"),
                bigquery.SchemaField("accession_number", "STRING"),
                bigquery.SchemaField("dir_url", "STRING"),
                bigquery.SchemaField("status", "STRING"),
                bigquery.SchemaField("year", "INTEGER"),
                bigquery.SchemaField("qtr", "INTEGER"),
                bigquery.SchemaField("retries", "INTEGER"),
            ],
            write_disposition="WRITE_APPEND",
            autodetect=False 
        )
        client.load_table_from_json(new_rows, QUEUE_TABLE, job_config=job_config).result()
        logger.info(f"✅ Seeded {len(new_rows)} filings.")

def process_batch(year, qtr):
    """Processes pending rows, avoiding those that failed > 2 times."""
    query = f"""
        SELECT * FROM `{QUEUE_TABLE}`
        WHERE status IN ('pending', 'error_timeout')
        AND (retries < 2 OR retries IS NULL)
        AND year = {year} AND qtr = {qtr}
        LIMIT 25
    """
    df = client.query(query).to_dataframe()
    if df.empty:
        return False

    for _, row in df.iterrows():
        acc_num = row['accession_number']
        try:
            # Short timeouts to prevent the 'slow crawl'
            # (Connect timeout, Read timeout)
            response = requests.get(row['dir_url'], headers=HEADERS, timeout=(3.05, 10))
            
            # 1. Handle SEC Blocking (Stop the job)
            if response.status_code in [403, 429]:
                logger.critical("🚨 SEC Blocking detected. Stopping job.")
                return False 

            # 2. Handle missing files (Permanent error)
            if response.status_code == 404:
                update_status(acc_num, 'error_404', row['retries'])
                continue

            # ... (Your logic to find XML URL inside index.json) ...
            xml_url = "extracted_url_logic_here" 
            
            # 3. Process XML and Load to BigQuery
            # (Simplified for brevity, use your existing Pydantic model here)
            # if success:
            update_status(acc_num, 'done', 0)

        except (ReadTimeout, ConnectTimeout):
            logger.warning(f"🕒 Timeout on {row['company_name']}. Skipping.")
            update_status(acc_num, 'error_timeout', row['retries'])
            
        except Exception as e:
            logger.error(f"💥 Error on {row['company_name']}: {e}")
            update_status(acc_num, 'error_data', row['retries'])
            
    return True

def update_status(acc_num, status, current_retries):
    """Updates status and increments retry count."""
    retries = (current_retries or 0) + 1
    # If we hit the limit, change status to permanent failure
    final_status = status if retries < 2 else f"FATAL_{status}"
    
    query = f"""
        UPDATE `{QUEUE_TABLE}`
        SET status = '{final_status}', retries = {retries}
        WHERE accession_number = '{acc_num}'
    """
    client.query(query).result()

def run_master_scraper():
    # Year/Qtr could be passed as Env Vars in Cloud Run
    year, qtr = 2020, 2
    
    # Optional: only seed if queue is empty
    # seed_queue_from_sec(year, qtr)
    
    while True:
        logger.info("🔄 Fetching next batch of 25...")
        has_more = process_batch(year, qtr)
        if not has_more:
            logger.info("🏁 No more pending items. Work finished.")
            break

if __name__ == "__main__":
    run_master_scraper()