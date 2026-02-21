import os
import sys
import time
import logging
import requests
from typing import Optional, List
from lxml import etree
from pydantic import BaseModel, field_validator, ValidationError
from google.cloud import bigquery

# 1. LOGGING & GLOBAL CONFIG
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("13f_strict_agent")

HEADERS = {'User-Agent': 'Institutional Research mmapplausetest@gmail.com', 'Host': 'www.sec.gov'}
PROJECT_ID = "datascience-projects"

# 2. STRICT BIGQUERY SCHEMA (Prevents 'Type Guessing' errors)
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

# 3. PYDANTIC MODEL (Data Cleaning & Validation)
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
    def force_string_and_pad(cls, v):
        # zfill(10) ensures CIKs are always 10 chars (SEC standard)
        return str(v).strip().zfill(10) if 'cik' in cls.__name__ else str(v).strip()

    @field_validator('value_usd', 'shares', mode='before')
    @classmethod
    def handle_numeric_strings(cls, v):
        if isinstance(v, str):
            # Remove commas and handle decimals before converting to INT
            return int(float(v.replace(',', '') or 0))
        return v or 0

# 4. CORE PROCESSING LOGIC
def process_queue_batch(client, year, qtr, limit=20):
    queue_table = f"{PROJECT_ID}.gcp_shareloader.scraping_queue"
    master_table = f"{PROJECT_ID}.gcp_shareloader.all_holdings_master"
    
    query = f"""
        SELECT * FROM `{queue_table}` 
        WHERE status = 'pending' AND year={year} AND qtr={qtr} 
        LIMIT {limit}
    """
    df = client.query(query).to_dataframe()
    if df.empty: return 0

    session = requests.Session()
    session.headers.update(HEADERS)
    
    # CRITICAL: Disable autodetect and provide explicit schema
    job_config = bigquery.LoadJobConfig(
        schema=STRICT_BQ_SCHEMA,
        write_disposition="WRITE_APPEND",
        autodetect=False 
    )

    q_dates = {1: f"{year}-03-31", 2: f"{year}-06-30", 3: f"{year}-09-30", 4: f"{year}-12-31"}
    p_date = q_dates.get(qtr)

    success_count = 0
    for _, row in df.iterrows():
        validated_holdings = []
        acc_num = str(row['accession_number']).strip()
        
        try:
            # Step A: Fetch Directory JSON
            dir_res = session.get(row['dir_url'], timeout=30)
            items = dir_res.json().get('directory', {}).get('item', [])
            xml_name = next((i['name'] for i in items if 'infotable.xml' in i['name'].lower()), None)
            
            if not xml_name:
                client.query(f"UPDATE `{queue_table}` SET status='no_xml' WHERE accession_number='{acc_num}'")
                continue

            # Step B: Fetch XML & Parse
            xml_url = row['dir_url'].replace('index.json', xml_name)
            xml_res = session.get(xml_url, timeout=60)
            root = etree.fromstring(xml_res.content)
            nodes = root.xpath("//*[local-name()='infoTable']")

            for info in nodes:
                try:
                    # Map XML to Pydantic Model
                    shares_xpath = "*[local-name()='shrsOrPrnAmt']/*[translate(local-name(), 'A', 'a')='sshprnamt']"
                    holding = HoldingRow(
                        cik=str(row['cik']),
                        manager_name=str(row['company_name']),
                        issuer_name=info.xpath("string(*[local-name()='nameOfIssuer'])"),
                        cusip=info.xpath("string(*[local-name()='cusip'])"),
                        value_usd=info.xpath("string(*[local-name()='value'])"),
                        shares=info.xpath(f"string({shares_xpath})"),
                        put_call=info.xpath("string(*[local-name()='putCall'])") or None,
                        filing_date=p_date,
                        accession_number=acc_num
                    )
                    validated_holdings.append(holding.model_dump())
                except ValidationError:
                    continue 

            if validated_holdings:
                # Step C: Strict Upload
                job = client.load_table_from_json(validated_holdings, master_table, job_config=job_config)
                job.result(timeout=120) 
                
                # Step D: Update Queue with Quoted String ID
                update_q = f"UPDATE `{queue_table}` SET status='done' WHERE accession_number = '{acc_num}'"
                client.query(update_q).result()
                
                logger.info(f"💾 SAVED & VERIFIED: {row['company_name']} ({len(validated_holdings)} rows)")
                success_count += 1

        except Exception as e:
            logger.error(f"💥 Error processing {row['company_name']}: {e}")
            
    return success_count

def run_master_scraper():
    client = bigquery.Client(project=PROJECT_ID)
    year = int(os.getenv("YEAR", 2020))
    qtr = int(os.getenv("QTR", 1))
    
    logger.info(f"🚀 Job Starting for {year} Q{qtr}")
    
    # Process batches until empty or limit reached
    for i in range(15): # Up to 300 managers per run
        processed = process_queue_batch(client, year, qtr, limit=20)
        if processed == 0: break
        logger.info(f"🔄 Completed Batch {i+1}")

if __name__ == "__main__":
    run_master_scraper()