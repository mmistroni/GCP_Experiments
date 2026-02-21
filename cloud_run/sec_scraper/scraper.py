import os
import time
import datetime
import logging
import requests
from typing import Optional
from lxml import etree
from pydantic import BaseModel, field_validator, ValidationError
from google.cloud import bigquery

# 1. CONFIGURATION & LOGGING
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("13f_auto_orchestrator")

HEADERS = {'User-Agent': 'Institutional Research mmapplausetest@gmail.com', 'Host': 'www.sec.gov'}
PROJECT_ID = "datascience-projects"
QUEUE_TABLE = f"{PROJECT_ID}.gcp_shareloader.scraping_queue"
MASTER_TABLE = f"{PROJECT_ID}.gcp_shareloader.all_holdings_master"

# 2. STRICT SCHEMA DEFINITION
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

# 3. DATA VALIDATION MODEL
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
    def ensure_string(cls, v):
        return str(v).strip().zfill(10) if 'cik' in str(v) else str(v).strip()

    @field_validator('value_usd', 'shares', mode='before')
    @classmethod
    def clean_int(cls, v):
        if isinstance(v, str):
            return int(float(v.replace(',', '') or 0))
        return v or 0

# 4. UTILITY FUNCTIONS
def infer_current_quarter():
    """Calculates the most recently completed quarter if none provided."""
    now = datetime.datetime.now()
    year = now.year
    qtr = (now.month - 1) // 3
    if qtr == 0:
        year -= 1
        qtr = 4
    return year, qtr

def process_queue_batch(client, year, qtr, limit=20):
    """Processes a single batch of managers with defensive SQL casting."""
    query = f"""
        SELECT * FROM `{QUEUE_TABLE}` 
        WHERE status = 'pending' AND year={year} AND qtr={qtr} 
        LIMIT {limit}
    """
    df = client.query(query).to_dataframe()
    if df.empty: return 0

    session = requests.Session()
    session.headers.update(HEADERS)
    job_config = bigquery.LoadJobConfig(schema=STRICT_BQ_SCHEMA, write_disposition="WRITE_APPEND", autodetect=False)
    p_date = {1: f"{year}-03-31", 2: f"{year}-06-30", 3: f"{year}-09-30", 4: f"{year}-12-31"}.get(qtr)

    success_count = 0
    for _, row in df.iterrows():
        validated_holdings = []
        acc_num = str(row['accession_number']).strip()
        manager_name = str(row['company_name'])
        
        try:
            # SEC Data Retrieval
            time.sleep(0.12) # Rate limit protection
            dir_res = session.get(row['dir_url'], timeout=(5, 20))
            if dir_res.status_code != 200: continue

            items = dir_res.json().get('directory', {}).get('item', [])
            xml_name = next((i['name'] for i in items if 'infotable.xml' in i['name'].lower()), None)
            
            if not xml_name:
                client.query(f"UPDATE `{QUEUE_TABLE}` SET status='no_xml' WHERE CAST(accession_number AS STRING) = '{acc_num}'").result()
                continue

            xml_res = session.get(row['dir_url'].replace('index.json', xml_name), timeout=(10, 60))
            root = etree.fromstring(xml_res.content)
            nodes = root.xpath("//*[local-name()='infoTable']")

            for info in nodes:
                try:
                    shares_xpath = "*[local-name()='shrsOrPrnAmt']/*[translate(local-name(), 'A', 'a')='sshprnamt']"
                    holding = HoldingRow(
                        cik=str(row['cik']),
                        manager_name=manager_name,
                        issuer_name=info.xpath("string(*[local-name()='nameOfIssuer'])"),
                        cusip=info.xpath("string(*[local-name()='cusip'])"),
                        value_usd=info.xpath("string(*[local-name()='value'])"),
                        shares=info.xpath(f"string({shares_xpath})"),
                        put_call=info.xpath("string(*[local-name()='putCall'])") or None,
                        filing_date=p_date,
                        accession_number=acc_num
                    )
                    validated_holdings.append(holding.model_dump())
                except ValidationError: continue 

            if validated_holdings:
                job = client.load_table_from_json(validated_holdings, MASTER_TABLE, job_config=job_config)
                job.result(timeout=120) 
                client.query(f"UPDATE `{QUEUE_TABLE}` SET status='done' WHERE CAST(accession_number AS STRING) = '{acc_num}'").result()
                logger.info(f"✅ {manager_name}: {len(validated_holdings)} rows saved.")
                success_count += 1
            else:
                client.query(f"UPDATE `{QUEUE_TABLE}` SET status='empty' WHERE CAST(accession_number AS STRING) = '{acc_num}'").result()

        except Exception as e:
            logger.error(f"💥 Failed {manager_name}: {e}")
            
    return success_count

# 5. ORCHESTRATION LOOP
def run_master_scraper():
    client = bigquery.Client(project=PROJECT_ID)
    
    # Inference: Use Env Vars or Auto-Detect
    year = int(os.getenv("YEAR", infer_current_quarter()[0]))
    qtr = int(os.getenv("QTR", infer_current_quarter()[1]))
    
    logger.info(f"🤖 TARGET: {year} Q{qtr} | STARTING CONTINUOUS LOOP")

    total_mgrs = 0
    while True:
        processed = process_queue_batch(client, year, qtr, limit=25)
        if processed == 0:
            logger.info("🏁 No more pending items found.")
            break
        total_mgrs += processed
        logger.info(f"📈 Total managers processed this run: {total_mgrs}")

    # Final Cleanup
    check_query = f"SELECT count(*) as count FROM `{QUEUE_TABLE}` WHERE status='pending' AND year={year} AND qtr={qtr}"
    pending = client.query(check_query).to_dataframe().iloc[0]['count']
    
    if pending == 0:
        logger.info(f"🧹 Clearing completed queue for {year} Q{qtr}")
        client.query(f"DELETE FROM `{QUEUE_TABLE}` WHERE year={year} AND qtr={qtr}").result()

if __name__ == "__main__":
    run_master_scraper()