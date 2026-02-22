import os
import time
import datetime
import logging
import requests
from typing import Optional, List
from lxml import etree
from pydantic import BaseModel, field_validator, ValidationError
from google.cloud import bigquery

# --- CONFIG & LOGGING ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("13f_full_lifecycle")

HEADERS = {'User-Agent': 'Institutional Research mmapplausetest@gmail.com', 'Host': 'www.sec.gov'}
PROJECT_ID = "datascience-projects"
QUEUE_TABLE = f"{PROJECT_ID}.gcp_shareloader.scraping_queue"
MASTER_TABLE = f"{PROJECT_ID}.gcp_shareloader.all_holdings_master"

# --- BQ SCHEMA ---
STRICT_BQ_SCHEMA = [
    bigquery.SchemaField("cik", "STRING"),
    bigquery.SchemaField("manager_name", "STRING"),
    bigquery.SchemaField("issuer_name", "STRING"),
    bigquery.SchemaField("cusip", "STRING"),
    bigquery.SchemaField("value_usd", "INTEGER"),
    bigquery.SchemaField("shares", "INTEGER"),
    bigquery.SchemaField("put_call", "STRING"),
    bigquery.SchemaField("filing_date", "DATETIME"),
    bigquery.SchemaField("accession_number", "STRING"),
]

# --- PYDANTIC MODEL ---
class HoldingRow(BaseModel):
    cik: str
    manager_name: str
    issuer_name: str
    cusip: str
    value_usd: int
    shares: int
    put_call: Optional[str] = None
    filing_date: str
    accession_number: str

    @field_validator('cik', 'accession_number')
    @classmethod
    def force_str(cls, v): return str(v).strip().zfill(10) if 'cik' in str(v) else str(v).strip()

# --- STEP 1: SEEDING (The resurrected logic) ---
def seed_queue_from_sec(client, year, qtr):
    idx_url = f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{qtr}/master.idx"
    logger.info(f"🌐 Fetching index from: {idx_url}")
    
    res = requests.get(idx_url, headers=HEADERS)
    lines = res.text.split('\n')
    new_rows = []

    for line in lines[10:]:
        if "|13F-HR|" in line:
            parts = line.split('|')
            if len(parts) < 5: continue
            
            # FORCE EVERYTHING TO STRING HERE
            new_rows.append({
                "cik": str(parts[0]).strip().zfill(10), # Force string + padding
                "company_name": str(parts[1]).strip(),
                "accession_number": str(parts[4].split('/')[-1].replace('.txt', '')),
                "dir_url": f"https://www.sec.gov/Archives/{parts[4].replace('.txt', '').replace('-', '')}/index.json",
                "status": "pending",
                "year": int(year),
                "qtr": int(qtr)
            })

    if new_rows:
        # THIS IS THE VITAL PART: Define the strict schema for the Load Job
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("cik", "STRING"),
                bigquery.SchemaField("company_name", "STRING"),
                bigquery.SchemaField("accession_number", "STRING"),
                bigquery.SchemaField("dir_url", "STRING"),
                bigquery.SchemaField("status", "STRING"),
                bigquery.SchemaField("year", "INTEGER"),
                bigquery.SchemaField("qtr", "INTEGER"),
            ],
            write_disposition="WRITE_APPEND",
            autodetect=False  # <--- CRITICAL: Tells BQ "Don't guess, use my schema"
        )

        logger.info(f"📤 Loading {len(new_rows)} rows to {QUEUE_TABLE}...")
        job = client.load_table_from_json(new_rows, QUEUE_TABLE, job_config=job_config)
        job.result() # This will now work because types are locked
        logger.info("✨ Seed complete.")
        
# --- STEP 2: PROCESSING (The loop logic) ---
def process_queue_batch(client, year, qtr, limit=25):
    query = f"SELECT * FROM `{QUEUE_TABLE}` WHERE status='pending' AND year={year} AND qtr={qtr} LIMIT {limit}"
    df = client.query(query).to_dataframe()
    if df.empty: return 0

    session = requests.Session()
    session.headers.update(HEADERS)
    job_config = bigquery.LoadJobConfig(schema=STRICT_BQ_SCHEMA, write_disposition="WRITE_APPEND", autodetect=False)
    p_date = {1:f"{year}-03-31", 2:f"{year}-06-30", 3:f"{year}-09-30", 4:f"{year}-12-31"}.get(qtr)

    success_count = 0
    for _, row in df.iterrows():
        acc_num = str(row['accession_number']).strip()
        try:
            time.sleep(0.11) # SEC Compliance
            dir_res = session.get(row['dir_url'], timeout=15).json()
            items = dir_res.get('directory', {}).get('item', [])
            xml_name = next((i['name'] for i in items if 'infotable.xml' in i['name'].lower()), None)
            
            if not xml_name:
                client.query(f"UPDATE `{QUEUE_TABLE}` SET status='no_xml' WHERE CAST(accession_number AS STRING)='{acc_num}'").result()
                continue

            xml_url = row['dir_url'].replace('index.json', xml_name)
            xml_res = session.get(xml_url, timeout=30)
            root = etree.fromstring(xml_res.content)
            nodes = root.xpath("//*[local-name()='infoTable']")

            batch_data = []
            for node in nodes:
                # Simplified extraction for brevity, use your previous XPATHs here
                h = HoldingRow(
                    cik=str(row['cik']), manager_name=row['company_name'],
                    issuer_name=node.xpath("string(*[local-name()='nameOfIssuer'])"),
                    cusip=node.xpath("string(*[local-name()='cusip'])"),
                    value_usd=int(node.xpath("string(*[local-name()='value'])") or 0),
                    shares=int(node.xpath("string(*[local-name()='shrsOrPrnAmt']/*[local-name()='sshprnamt'])") or 0),
                    filing_date=p_date, accession_number=acc_num
                )
                batch_data.append(h.model_dump())

            if batch_data:
                client.load_table_from_json(batch_data, MASTER_TABLE, job_config=job_config).result()
                client.query(f"UPDATE `{QUEUE_TABLE}` SET status='done' WHERE CAST(accession_number AS STRING)='{acc_num}'").result()
                success_count += 1
        except Exception as e:
            logger.error(f"💥 Error on {row['company_name']}: {e}")
    return success_count

# --- STEP 3: ORCHESTRATION ---
def run_master_scraper():
    client = bigquery.Client(project=PROJECT_ID)
    year = int(os.getenv("YEAR", 2020))
    qtr = int(os.getenv("QTR", 1))

    # Check if we need to seed
    count_q = f"SELECT count(*) as count FROM `{QUEUE_TABLE}` WHERE year={year} AND qtr={qtr}"
    if client.query(count_q).to_dataframe().iloc[0]['count'] == 0:
        seed_queue_from_sec(client, year, qtr)

    # Process until empty
    while True:
        if process_queue_batch(client, year, qtr) == 0: break
        logger.info("📈 Batch complete, continuing...")

    # Cleanup
    logger.info(f"🧹 Mission complete for {year} Q{qtr}.")
    # client.query(f"DELETE FROM `{QUEUE_TABLE}` WHERE year={year} AND qtr={qtr}").result()

if __name__ == "__main__":
    run_master_scraper()