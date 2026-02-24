import os
import time
import logging
import requests
from lxml import etree
from google.cloud import bigquery
from requests.exceptions import ReadTimeout, ConnectTimeout

# --- CONFIGURATION & ENV VARS ---
PROJECT_ID = "datascience-projects"
DATASET_ID = "gcp_shareloader"

QUEUE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.scraping_queue"
MASTER_TABLE = f"{PROJECT_ID}.{DATASET_ID}.all_holdings_master"
STAGING_TABLE = f"{PROJECT_ID}.{DATASET_ID}.temp_holdings_staging"

HEADERS = {'User-Agent': 'YourCompany/1.0 (contact@yourdomain.com)'}

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)
client = bigquery.Client(project=PROJECT_ID)

from pydantic import BaseModel, Field, field_validator
from typing import Optional

class Holding(BaseModel):
    accession_number: str
    filing_date: str
    nameOfIssuer: Optional[str] = "Unknown"
    cusip: str
    value: float = 0.0
    sshPrnamt: float = 0.0
    sshPrnamtType: Optional[str] = None
    investmentDiscretion: Optional[str] = None

    @field_validator('cusip', 'accession_number', mode='before')
    @classmethod
    def force_string(cls, v):
        return str(v).strip() if v is not None else ""

    @field_validator('value', 'sshPrnamt', mode='before')
    @classmethod
    def force_float(cls, v):
        try:
            return float(v) if v else 0.0
        except (ValueError, TypeError):
            return 0.0


# --- STAGE 0: SEEDING LOGIC ---

def seed_queue_if_needed(year, qtr):
    """Checks if the queue for the target quarter is empty and seeds if necessary."""
    check_query = f"SELECT count(*) as cnt FROM `{QUEUE_TABLE}` WHERE year={year} AND qtr={qtr}"
    count = list(client.query(check_query))[0].cnt
    
    if count > 0:
        logger.info(f"ℹ️ Queue already contains {count} items for {year} Q{qtr}. Skipping seed.")
        return

    idx_url = f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{qtr}/master.idx"
    logger.info(f"🌐 Seeding queue from: {idx_url}")
    
    res = requests.get(idx_url, headers=HEADERS)
    lines = res.text.split('\n')
    new_rows = []

    for line in lines[10:]:
        if "|13F-HR" in line:
            parts = line.split('|')
            if len(parts) < 5: continue
            
            acc_num = parts[4].split('/')[-1].replace('.txt', '')
            new_rows.append({
                "cik": str(parts[0]).strip().zfill(10),
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
        # Forcing CIK as STRING here as well during seeding
        job_config = bigquery.LoadJobConfig(
            schema=[bigquery.SchemaField("cik", "STRING")],
            autodetect=True,
            write_disposition="WRITE_APPEND"
        )
        client.load_table_from_json(new_rows, QUEUE_TABLE, job_config=job_config).result()
        logger.info(f"✅ Successfully seeded {len(new_rows)} items.")

# --- STAGE 1 & 2: FETCHING & PARSING ---

def parse_xml_to_holdings(xml_content, acc_num, filing_date):
    try:
        tree = etree.fromstring(xml_content.encode('utf-8'))
        nodes = tree.xpath('//*[local-name()="infoTable"]')
        
        extracted_data = []
        for node in nodes:
            def get_val(tag):
                res = node.xpath(f'./*[local-name()="{tag}"]/text()')
                return res[0] if res else None

            extracted_data.append({
                "accession_number": acc_num,
                "filing_date": filing_date,
                "nameOfIssuer": get_val("nameOfIssuer"),
                "cusip": get_val("cusip"),
                "value": get_val("value"), # Let Pydantic force_float handle it
                "sshPrnamt": get_val("sshPrnamt"),
                "sshPrnamtType": get_val("sshPrnamtType"),
                "investmentDiscretion": get_val("investmentDiscretion")
            })
        return extracted_data
    except Exception as e:
        logger.error(f"❌ Parser failure for {acc_num}: {e}")
        return []

# --- STAGE 3: BATCH PROCESSING & ATOMIC MERGE ---

def process_batch(year, qtr):
    limit_clause = f"LIMIT {ENV_LIMIT}" if ENV_LIMIT > 0 else ""
    
    query = f"""
        SELECT * FROM `{QUEUE_TABLE}` 
        WHERE status IN ('pending', 'error_data') 
        AND year={year} AND qtr={qtr} 
        {limit_clause}
    """
    
    df = client.query(query).to_dataframe()
    if df.empty:
        return False

    all_holdings = []
    success_acc_nums = []

    with requests.Session() as session:
        session.headers.update(HEADERS)
        for _, row in df.iterrows():
            acc_num = row['accession_number']
            try:
                time.sleep(0.12)
                dir_res = session.get(row['dir_url'], timeout=10)
                dir_res.raise_for_status()
                
                items = dir_res.json().get('directory', {}).get('item', [])
                xml_name = next((i['name'] for i in items if 'infotable' in i['name'].lower() and i['name'].endswith('.xml')), None)
                
                if xml_name:
                    xml_url = row['dir_url'].replace('index.json', xml_name)
                    xml_res = session.get(xml_url, timeout=15)
                    holdings = parse_xml_to_holdings(xml_res.text, acc_num, "2020-03-31")
                    
                    if holdings:
                        for h in holdings:
                            try:
                                # This cleans and validates the data immediately
                                validated_holding = Holding(**h)
                                all_holdings.append(validated_holding.model_dump())
                            except Exception as ve:
                                logger.warning(f"Skipping bad row for {acc_num}: {ve}")
                        
                        success_acc_nums.append(acc_num)
                        logger.info(f"✔️ {row['company_name']}: {len(holdings)} holdings.")
                    else:
                        raise Exception("Empty holdings list after parse.")
                else:
                    raise Exception("No XML infoTable found in directory.")

            except Exception as e:
                logger.error(f"⚠️ Failed {row['company_name']}: {e}")
                client.query(f"UPDATE `{QUEUE_TABLE}` SET status='error_data', last_error='{str(e)[:100]}' WHERE accession_number='{acc_num}'")

    if all_holdings:
        # --- AMENDED JOB CONFIG ---
        # 1. DELETE the staging table first to clear any cached/bad schemas
        client.delete_table(STAGING_TABLE, not_found_ok=True)
        # Explicitly forcing CIK (from queue) and CUSIP (from holdings) as STRING
        # 2. Hardcode the schema for the staging table
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("accession_number", "STRING"),
                bigquery.SchemaField("filing_date", "STRING"),
                bigquery.SchemaField("nameOfIssuer", "STRING"),
                bigquery.SchemaField("cusip", "STRING"),
                bigquery.SchemaField("value", "FLOAT"), # Explicitly named 'value'
                bigquery.SchemaField("sshPrnamt", "FLOAT"),
                bigquery.SchemaField("sshPrnamtType", "STRING"),
                bigquery.SchemaField("investmentDiscretion", "STRING"),
            ],
            write_disposition="WRITE_TRUNCATE",
            autodetect=False  # FORCE it to use our schema
        )
        
        client.load_table_from_json(all_holdings, STAGING_TABLE, job_config=job_config).result()

        # 2. Atomic Merge to Master (Deduplicates)
        # We map S.value (from Staging) to T.value_usd (in Master)
        # 2. Atomic Merge to Master
        # We explicitly CAST S.value to INT64 to match the Master Table's type
        merge_sql = f"""
            MERGE `{MASTER_TABLE}` T
            USING `{STAGING_TABLE}` S
            ON T.accession_number = S.accession_number AND T.cusip = S.cusip
            WHEN MATCHED THEN
                UPDATE SET 
                    T.value_usd = CAST(S.value AS INT64)
            WHEN NOT MATCHED THEN
                INSERT (accession_number, filing_date, nameOfIssuer, cusip, value_usd, sshPrnamt, sshPrnamtType, investmentDiscretion)
                VALUES (S.accession_number, S.filing_date, S.nameOfIssuer, S.cusip, CAST(S.value AS INT64), CAST(S.sshPrnamt AS INT64), S.sshPrnamtType, S.investmentDiscretion)
        """
        client.query(merge_sql).result()
        
        acc_list = ", ".join([f"'{a}'" for a in success_acc_nums])
        client.query(f"UPDATE `{QUEUE_TABLE}` SET status='done' WHERE accession_number IN ({acc_list})").result()
        logger.info(f"✨ Successfully merged {len(success_acc_nums)} managers.")
    
    return True if ENV_LIMIT > 0 else False

# --- MAIN EXECUTION ---

if __name__ == "__main__":
    ENV_LIMIT = int(os.getenv("SCRAPER_LIMIT", "25"))
    target_year = int(os.getenv("YEAR", 2020))
    target_qtr  = int(os.getenv("QUARTER", 1))
    
    seed_queue_if_needed(target_year, target_qtr)
    
    while True:
        logger.info(f"🚀 Starting batch (Limit: {ENV_LIMIT})...")
        work_remains = process_batch(target_year, target_qtr)
        if not work_remains:
            logger.info("🏁 No more work found. Exiting.")
            break