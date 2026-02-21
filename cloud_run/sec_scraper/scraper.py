import os
import sys
import time
import logging
import requests
from typing import Optional, List
from lxml import etree
from pydantic import BaseModel, Field, field_validator, ValidationError
from google.cloud import bigquery

# 1. LOGGING & CONFIG
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("13f_pydantic_agent")

HEADERS = {'User-Agent': 'Institutional Research mmapplausetest@gmail.com', 'Host': 'www.sec.gov'}
PROJECT_ID = "datascience-projects"

# 2. PYDANTIC MODELS (The Gatekeepers)
class HoldingRow(BaseModel):
    cik: str
    manager_name: str
    issuer_name: str
    cusip: str
    value_usd: int
    shares: int
    put_call: Optional[str] = None
    filing_date: str # ISO Format YYYY-MM-DD
    accession_number: str

    @field_validator('cik', 'accession_number')
    @classmethod
    def ensure_string(cls, v):
        return str(v).strip()

    @field_validator('value_usd', 'shares', mode='before')
    @classmethod
    def clean_numbers(cls, v):
        if isinstance(v, str):
            return int(float(v.replace(',', '') or 0))
        return v or 0

# 3. CORE LOGIC
def process_queue_batch(client, year, qtr, limit=20):
    queue_table = f"{PROJECT_ID}.gcp_shareloader.scraping_queue"
    master_table = f"{PROJECT_ID}.gcp_shareloader.all_holdings_master"
    
    # Query pending rows
    query = f"SELECT * FROM `{queue_table}` WHERE status = 'pending' AND year={year} AND qtr={qtr} LIMIT {limit}"
    df = client.query(query).to_dataframe()
    if df.empty: return 0

    session = requests.Session()
    session.headers.update(HEADERS)
    
    # BigQuery config - we explicitly map the Pydantic dicts
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    q_dates = {1: f"{year}-03-31", 2: f"{year}-06-30", 3: f"{year}-09-30", 4: f"{year}-12-31"}
    p_date = q_dates.get(qtr)

    success_count = 0
    for _, row in df.iterrows():
        validated_holdings = []
        acc_num = str(row['accession_number']).strip()
        
        try:
            # Fetch XML logic...
            dir_res = session.get(row['dir_url'], timeout=30)
            items = dir_res.json().get('directory', {}).get('item', [])
            xml_name = next((i['name'] for i in items if 'infotable.xml' in i['name'].lower()), None)
            
            if not xml_name:
                client.query(f"UPDATE `{queue_table}` SET status='no_xml' WHERE accession_number='{acc_num}'")
                continue

            xml_res = session.get(row['dir_url'].replace('index.json', xml_name), timeout=60)
            root = etree.fromstring(xml_res.content)
            nodes = root.xpath("//*[local-name()='infoTable']")

            for info in nodes:
                try:
                    # Create Pydantic Model for each row
                    holding = HoldingRow(
                        cik=str(row['cik']),
                        manager_name=row['company_name'],
                        issuer_name=info.xpath("string(*[local-name()='nameOfIssuer'])"),
                        cusip=info.xpath("string(*[local-name()='cusip'])"),
                        value_usd=info.xpath("string(*[local-name()='value'])"),
                        shares=info.xpath(f"string(*[local-name()='shrsOrPrnAmt']/*[translate(local-name(), 'A', 'a')='sshprnamt'])"),
                        put_call=info.xpath("string(*[local-name()='putCall'])") or None,
                        filing_date=p_date,
                        accession_number=acc_num
                    )
                    validated_holdings.append(holding.model_dump()) # Converts to BQ-friendly dict
                except ValidationError as ve:
                    logger.warning(f"Validation failed for a row in {row['company_name']}: {ve.json()}")
                    continue

            if validated_holdings:
                # UPLOAD TO BIGQUERY
                job = client.load_table_from_json(validated_holdings, master_table, job_config=job_config)
                job.result(timeout=120) 
                
                # UPDATE QUEUE - Note the quotes around '{acc_num}' to fix your 400 error
                update_q = f"UPDATE `{queue_table}` SET status='done' WHERE accession_number = '{acc_num}'"
                client.query(update_q).result()
                
                logger.info(f"✅ VERIFIED & SAVED: {row['company_name']} ({len(validated_holdings)} rows)")
                success_count += 1

        except Exception as e:
            logger.error(f"💥 Critical Failure on {row['company_name']}: {e}")
            
    return success_count

def run_master_scraper():
    client = bigquery.Client(project=PROJECT_ID)
    year = int(os.getenv("YEAR", 2020))
    qtr = int(os.getenv("QTR", 1))
    
    # Process 10 batches (Total 200 managers)
    for i in range(10):
        processed = process_queue_batch(client, year, qtr)
        if processed == 0:
            logger.info("🏁 No more pending items found.")
            break
        logger.info(f"📈 Batch {i+1} complete.")

if __name__ == "__main__":
    run_master_scraper()