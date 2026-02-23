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
# IMPORTANT: SEC requires a descriptive User-Agent
HEADERS = {'User-Agent': 'YourCompany/1.0 (contact@yourdomain.com)'}

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

client = bigquery.Client(project=PROJECT_ID)

def seed_queue_from_sec(year, qtr):
    """Fills the queue with strict typing and autodetect OFF."""
    idx_url = f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{qtr}/master.idx"
    logger.info(f"🌐 Seeding queue from: {idx_url}")
    
    res = requests.get(idx_url, headers=HEADERS)
    lines = res.text.split('\n')
    new_rows = []

    for line in lines[10:]:
        if "|13F-HR" in line:  # Catches 13F-HR and 13F-HR/A
            parts = line.split('|')
            if len(parts) < 5: continue
            
            acc_num = parts[4].split('/')[-1].replace('.txt', '')
            new_rows.append({
                "cik": str(parts[0]).strip().zfill(10), # String + Padded
                "company_name": str(parts[1]).strip(),
                "accession_number": str(acc_num),
                "dir_url": f"https://www.sec.gov/Archives/{parts[4].replace('.txt', '').replace('-', '')}/index.json",
                "status": "pending",
                "year": int(year),
                "qtr": int(qtr),
                "retries": 0,
                "last_error": None
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
                bigquery.SchemaField("last_error", "STRING"),
            ],
            write_disposition="WRITE_APPEND",
            autodetect=False 
        )
        client.load_table_from_json(new_rows, QUEUE_TABLE, job_config=job_config).result()
        logger.info(f"✅ Successfully seeded {len(new_rows)} items into the queue.")

def update_queue_status(acc_num, status, current_retries, error_msg=None):
    """Updates BQ with the new status and error details."""
    retries = (current_retries or 0) + 1
    # Move to FATAL if we've tried too many times
    final_status = status if retries < 2 else f"FATAL_{status}"
    
    # Escape single quotes for SQL and truncate long errors
    clean_error = str(error_msg).replace("'", "''")[:1000] if error_msg else ""
    
    sql = f"""
        UPDATE `{QUEUE_TABLE}`
        SET status = '{final_status}', 
            retries = {retries},
            last_error = '{clean_error}'
        WHERE accession_number = '{acc_num}'
    """
    client.query(sql).result()

def process_batch(year, qtr):
    """Processes 25 rows that are pending or have retryable errors."""
    query = f"""
        SELECT * FROM `{QUEUE_TABLE}`
        WHERE (status = 'pending' OR status LIKE 'error%')
        AND status NOT LIKE 'FATAL%'
        AND (retries < 2 OR retries IS NULL)
        AND year = {year} AND qtr = {qtr}
        LIMIT 25
    """
    df = client.query(query).to_dataframe()
    if df.empty:
        return False

    with requests.Session() as session:
        session.headers.update(HEADERS)
        for _, row in df.iterrows():
            acc_num = row['accession_number']
            curr_ret = row['retries']
            
            try:
                # 1. Get the directory JSON (Short timeouts)
                res = session.get(row['dir_url'], timeout=(3.05, 10))
                
                if res.status_code in [403, 429]:
                    logger.critical("🛑 SEC RATE LIMIT HIT. Cooling down...")
                    return False 
                
                res.raise_for_status()
                # (Insert your specific XML URL extraction logic here)
                # For example: xml_url = extract_infotable_url(res.json())
                
                # 2. Scrape/Parse/Load (Simulated logic)
                # ...
                
                # 3. SUCCESS: Mark as done
                client.query(f"UPDATE `{QUEUE_TABLE}` SET status='done', retries=0, last_error=NULL WHERE accession_number='{acc_num}'").result()
                logger.info(f"✔️ Processed: {row['company_name']}")

            except (ReadTimeout, ConnectTimeout) as e:
                logger.warning(f"🕒 Timeout on {row['company_name']}")
                update_queue_status(acc_num, 'error_timeout', curr_ret, str(e))
                
            except Exception as e:
                logger.error(f"❌ Error on {row['company_name']}: {str(e)}")
                update_queue_status(acc_num, 'error_data', curr_ret, str(e))
                
    return True

def run_master_scraper():
    # Configure your target here
    target_year, target_qtr = 2020, 2
    
    # To run, ensure you've performed the 'Soft Reset' SQL first
    while True:
        logger.info("🚀 Starting next batch...")
        work_remains = process_batch(target_year, target_qtr)
        if not work_remains:
            logger.info("🏁 Queue cleared or SEC blocked. Job exiting.")
            break

if __name__ == "__main__":
    run_master_scraper()