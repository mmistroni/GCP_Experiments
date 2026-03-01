import os
import time
import logging
import requests
import datetime
from lxml import etree
from google.cloud import bigquery
from google.api_core.exceptions import Forbidden, BadRequest
from pydantic import BaseModel, field_validator
from typing import Optional

# --- CONFIGURATION ---
PROJECT_ID = "datascience-projects"
DATASET_ID = "gcp_shareloader"
QUEUE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.scraping_queue"
MASTER_TABLE = f"{PROJECT_ID}.{DATASET_ID}.all_holdings_master"
STAGING_TABLE = f"{PROJECT_ID}.{DATASET_ID}.temp_holdings_staging"

HEADERS = {'User-Agent': 'YourCompany/1.0 (contact_mm@yourdomain.com)'}

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)
client = bigquery.Client(project=PROJECT_ID)

class Holding(BaseModel):
    accession_number: str
    filing_date: str
    issuer_name: Optional[str] = "Unknown"
    cusip: str
    value_usd: float = 0.0
    shares: float = 0.0
    cik: str
    manager_name: str
    put_call: Optional[str] = None

    @field_validator('filing_date')
    @classmethod
    def format_for_datetime(cls, v):
        if v and "T" not in v and " " not in v:
            return f"{v} 00:00:00"
        return v

def seed_queue_if_needed(year, qtr):
    """Fetches the SEC index and populates the scraping_queue table if empty."""
    logger.info(f"Checking queue for {year} Q{qtr}...")
    check_query = f"SELECT count(*) as cnt FROM `{QUEUE_TABLE}` WHERE year={year} AND qtr={qtr}"
    count = list(client.query(check_query))[0].cnt
    
    if count > 0:
        logger.info(f"Queue already has {count} items. Skipping seed.")
        return

    master_idx_url = f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{qtr}/master.idx"
    logger.info(f"Fetching SEC Index: {master_idx_url}")
    idx_res = requests.get(master_idx_url, headers=HEADERS)
    lines = idx_res.text.split('\n')
    
    queue_data = []
    for line in lines:
        if '|13F-HR|' in line:
            parts = line.split('|')
            cik = parts[0]
            name = parts[1]
            path = parts[4]
            acc_num = path.split('/')[-1].split('.')[0]
            dir_path = path.replace('-', '').replace('.txt', '')
            dir_url = f"https://www.sec.gov/Archives/{dir_path}/index.json"
            
            queue_data.append({
                "cik": str(cik).zfill(10),
                "company_name": name,
                "accession_number": acc_num,
                "dir_url": dir_url,
                "status": "pending",
                "year": year,
                "qtr": qtr,
                "retries": 0
            })

    if queue_data:
        logger.info(f"Seeding {len(queue_data)} filings into queue...")
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        client.load_table_from_json(queue_data, QUEUE_TABLE, job_config=job_config).result()

def parse_xml_to_holdings(xml_content, acc_num, filing_date):
    try:
        tree = etree.fromstring(xml_content.encode('utf-8') if isinstance(xml_content, str) else xml_content)
        nodes = tree.xpath('//*[local-name()="infoTable"]')
        extracted_data = []
        for node in nodes:
            def gv(tag):
                res = node.xpath(f'./*[local-name()="{tag}"]/text()')
                return res[0] if res else None
            extracted_data.append({
                "accession_number": acc_num,
                "filing_date": filing_date,
                "issuer_name": gv("nameOfIssuer"),
                "cusip": gv("cusip"),
                "value_usd": gv("value") or 0.0, 
                "shares": gv("sshPrnamt") or 0.0,
                "put_call": gv("putCall")
            })
        return extracted_data
    except Exception as e:
        logger.error(f"❌ Parser failure: {e}")
        return []

def execute_dml_safe(sql, description):
    for i in range(3):
        try:
            client.query(sql).result()
            time.sleep(5)
            return True
        except Forbidden:
            wait = (i + 1) * 60
            logger.warning(f"🚫 403 Quota hit. Sleeping {wait}s...")
            time.sleep(wait)
    return False

def process_batch(year, qtr):
    # Keep the batch size small (25) to avoid hitting BQ DML quotas
    SCRAPER_LIMIT = 25 
    query = f"SELECT * FROM `{QUEUE_TABLE}` WHERE status IN ('pending', 'error_data') AND year={year} AND qtr={qtr} LIMIT {SCRAPER_LIMIT}"
    logger.info('Querhing scraping queue///')
    try:
        df = client.query(query).to_dataframe()
    except Forbidden:
        logger.warning("🚫 BigQuery Quota reached. Sleeping 60s before retry.")
        time.sleep(60)
        return True

    if df.empty: 
        return False

    all_holdings = []
    success_acc = []
    failed_acc = []

    with requests.Session() as session:
        session.headers.update(HEADERS)
        for _, row in df.iterrows():
            acc_num = row['accession_number']
            logger.info(f'Querying...{acc_num}')
            
            try:
                time.sleep(1.5) # SEC Rate limit compliance
                dir_res = session.get(row['dir_url'], timeout=30)
                # 2. Check status BEFORE calling .json()
                if dir_res.status_code == 503:
                    logger.warning("🚨 SEC Server Overloaded (503). Entering 10-minute cooldown...")
                    time.sleep(600) # 10 Minutes
                    return True # Exit current batch, try again after sleep
                if dir_res.status_code == 403:
                    logger.error(f"🚫 403 Forbidden for {acc_num}. We are being rate-limited. Sleeping 5 mins...")
                    time.sleep(300) # Wait 5 mins to clear the SEC "cool-down"
                    continue
                if dir_res.status_code != 200:
                    logger.error(f"❌ Error {dir_res.status_code} for {acc_num}")
                    failed_acc.append(acc_num)
                    continue
                try:
                    items = dir_res.json().get('directory', {}).get('item', [])
                except ValueError:
                    logger.error(f"❌ Received HTML instead of JSON for {acc_num}. Likely a soft-block.")
                    failed_acc.append(acc_num)
                    continue        
                # Identify the correct XML file
                xml_items = [i for i in items if i['name'].lower().endswith('.xml')]
                xml_name = next((i['name'] for i in xml_items if 'infotable' in i['name'].lower()), None)
                
                if not xml_name:
                    candidates = [i for i in xml_items if 'primary' not in i['name'].lower() and 'doc' not in i['name'].lower()]
                    if candidates:
                        xml_name = max(candidates, key=lambda x: int(x['size'] or 0))['name']

                if xml_name:
                    xml_url = row['dir_url'].replace('index.json', xml_name)
                    xml_res = session.get(xml_url, timeout=15)
                    
                    # For Q3 2020, use the specific filing date logic
                    holdings = parse_xml_to_holdings(xml_res.text, acc_num, "2020-09-30")
                    
                    for h in holdings:
                        h['cik'] = str(row['cik']).zfill(10)
                        h['manager_name'] = row['company_name']
                        all_holdings.append(Holding(**h).model_dump())
                    
                    success_acc.append(acc_num)
                    logger.info(f"✔️ {row['company_name']} parsed successfully.")
                else:
                    failed_acc.append(acc_num)
            except Exception as e:
                logger.error(f"❌ Filing {acc_num} failed: {e}")
                failed_acc.append(acc_num)

    # --- SEQUENTIAL DATABASE UPDATES ---

    # 1. Update Failures (DML)
    if failed_acc:
        # Pre-join strings to avoid f-string backslash syntax errors
        failed_list_str = ", ".join([f"'{a}'" for a in failed_acc])
        fail_sql = f"UPDATE `{QUEUE_TABLE}` SET status='error_data' WHERE accession_number IN ({failed_list_str})"
        execute_dml_safe(fail_sql, "Failure Update")

    # 2. Upload Data (Not DML - Safe from Quota)
    if all_holdings:
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("cik", "STRING"),
                bigquery.SchemaField("manager_name", "STRING"),
                bigquery.SchemaField("issuer_name", "STRING"),
                bigquery.SchemaField("cusip", "STRING"),
                bigquery.SchemaField("value_usd", "FLOAT"),
                bigquery.SchemaField("shares", "FLOAT"),
                bigquery.SchemaField("put_call", "STRING"),
                bigquery.SchemaField("filing_date", "STRING"),
                bigquery.SchemaField("accession_number", "STRING"),
            ],
            write_disposition="WRITE_TRUNCATE",
            autodetect=False 
        )
        client.load_table_from_json(all_holdings, STAGING_TABLE, job_config=job_config).result()

        # 3. Merge Data (DML)
        merge_sql = f"""
            MERGE `{MASTER_TABLE}` T
            USING `{STAGING_TABLE}` S
            ON T.accession_number = S.accession_number AND T.cusip = S.cusip
            WHEN NOT MATCHED THEN
                INSERT (cik, manager_name, issuer_name, cusip, value_usd, shares, put_call, filing_date, accession_number)
                VALUES (S.cik, S.manager_name, S.issuer_name, S.cusip, CAST(S.value_usd AS INT64), CAST(S.shares AS INT64), S.put_call, CAST(S.filing_date AS DATETIME), S.accession_number)
        """
        execute_dml_safe(merge_sql, "Data Merge")
        
        # 4. Update Success (DML)
        if success_acc:
            success_list_str = ", ".join([f"'{a}'" for a in success_acc])
            success_sql = f"UPDATE `{QUEUE_TABLE}` SET status='done' WHERE accession_number IN ({success_list_str})"
            execute_dml_safe(success_sql, "Success Update")

    return True


if __name__ == "__main__":
    YEAR = int(os.getenv('YEAR', 2020))
    QUARTER = int(os.getenv('QUARTER', 1))
    logger.info(f'Kicking off scraper for {YEAR},{QUARTER}')
    seed_queue_if_needed(YEAR, QUARTER)
    last_progress = time.time()
    while True:
        if time.time() - last_progress > 1200: break
        if process_batch(YEAR, QUARTER): last_progress = time.time()
        else: break
        time.sleep(10)