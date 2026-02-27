import os
import time
import logging
import requests
import datetime
from lxml import etree
from google.cloud import bigquery
from pydantic import BaseModel, Field, field_validator
from typing import Optional

# --- CONFIGURATION ---
PROJECT_ID = "datascience-projects"
DATASET_ID = "gcp_shareloader"

QUEUE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.scraping_queue"
MASTER_TABLE = f"{PROJECT_ID}.{DATASET_ID}.all_holdings_master"
STAGING_TABLE = f"{PROJECT_ID}.{DATASET_ID}.temp_holdings_staging"

# Tip: Use a dynamic User-Agent to avoid 503s
HEADERS = {'User-Agent': f'YourCompany/1.0 (contact_mm@yourdomain.com) {datetime.datetime.now().strftime("%Y%m%d")}'}

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)
client = bigquery.Client(project=PROJECT_ID)

class Holding(BaseModel):
    accession_number: str
    filing_date: str
    nameOfIssuer: Optional[str] = "Unknown"
    cusip: str
    value: float = 0.0
    sshPrnamt: float = 0.0
    sshPrnamtType: Optional[str] = None
    investmentDiscretion: Optional[str] = None

    @field_validator('filing_date')
    @classmethod
    def format_for_datetime(cls, v):
        # Fixes the 'DATE vs DATETIME' issue by ensuring a timestamp exists
        if v and "T" not in v and " " not in v:
            return f"{v} 00:00:00"
        return v

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

# ... (seed_queue_if_needed and parse_xml_to_holdings remain the same) ...

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
                "value": get_val("value"), 
                "sshPrnamt": get_val("sshPrnamt"),
                "sshPrnamtType": get_val("sshPrnamtType"),
                "investmentDiscretion": get_val("investmentDiscretion")
            })
        return extracted_data
    except Exception as e:
        logger.error(f"❌ Parser failure for {acc_num}: {e}")
        return []

def seed_queue_if_needed(year, qtr):
    """
    Seeds the queue with explicit schema handling to prevent 400 BadRequest errors.
    Returns True if seeding was successful or not needed, False if it failed.
    """
    # 1. Check if we already have data to avoid duplicate seeding
    check_query = f"SELECT count(*) as cnt FROM `{QUEUE_TABLE}` WHERE year={year} AND qtr={qtr}"
    try:
        count = list(client.query(check_query))[0].cnt
        if count > 0:
            logger.info(f"ℹ️ Queue already has {count} items for {year} Q{qtr}. Skipping seed.")
            return True
    except Exception as e:
        logger.error(f"❌ Could not check queue status: {e}")
        return False

    idx_url = f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{qtr}/master.idx"
    logger.info(f"🌐 Fetching SEC index: {idx_url}")
    
    try:
        res = requests.get(idx_url, headers=HEADERS, timeout=30)
        res.raise_for_status()
    except Exception as e:
        logger.error(f"❌ Failed to fetch SEC index: {e}")
        return False

    lines = res.text.split('\n')
    new_rows = []

    for line in lines[10:]:
        if "|13F-HR" in line:
            parts = line.split('|')
            if len(parts) < 5: continue
            
            # Extract and clean identifiers
            raw_cik = str(parts[0]).strip()
            acc_num = parts[4].split('/')[-1].replace('.txt', '')
            
            new_rows.append({
                "cik": raw_cik.zfill(10),  # Keep as string with leading zeros
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
        # ⚠️ CRITICAL: Explicitly define the schema to match your existing table
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
        )
        
        try:
            logger.info(f"📤 Uploading {len(new_rows)} rows to {QUEUE_TABLE}...")
            client.load_table_from_json(new_rows, QUEUE_TABLE, job_config=job_config).result()
            logger.info("✅ Seeding successful.")
            return True
        except Exception as e:
            logger.error(f"❌ BigQuery Load Failed: {e}")
            return False
    return True

def process_batch(year, qtr):
    limit_clause = f"LIMIT {ENV_LIMIT}" if ENV_LIMIT > 0 else ""
    query = f"SELECT * FROM `{QUEUE_TABLE}` WHERE status IN ('pending', 'error_data') AND year={year} AND qtr={qtr} {limit_clause}"
    df = client.query(query).to_dataframe()
    if df.empty: return False

    all_holdings = []
    success_acc_nums = []

    with requests.Session() as session:
        session.headers.update(HEADERS)
        for _, row in df.iterrows():
            acc_num = row['accession_number']
            try:
                time.sleep(0.25) # Slightly slower for Cloud Run stability
                dir_res = session.get(row['dir_url'], timeout=10)
                items = dir_res.json().get('directory', {}).get('item', [])
                xml_files = [i['name'] for i in items if i['name'].lower().endswith('.xml')]
                xml_name = next((n for n in xml_files if 'infotable' in n.lower() or 'informationtable' in n.lower()), None)
                if not xml_name:
                    xml_name = next((n for n in xml_files if 'doc' not in n.lower() and 'primary' not in n.lower()), None)

                if xml_name:
                    xml_url = row['dir_url'].replace('index.json', xml_name)
                    xml_res = session.get(xml_url, timeout=15)
                    holdings = parse_xml_to_holdings(xml_res.text, acc_num, "2020-03-31")
                    for h in holdings:
                        try:
                            # Add CIK and Company Name to each holding so Master isn't NULL
                            h_data = Holding(**h).model_dump()
                            h_data['cik'] = row['cik']
                            h_data['manager_name'] = row['company_name']
                            all_holdings.append(h_data)
                        except Exception: continue
                    success_acc_nums.append(acc_num)
                    logger.info(f"✔️ {row['company_name']}: {len(holdings)} holdings.")
                else:
                    raise Exception("No XML infoTable found.")
            except Exception as e:
                client.query(f"UPDATE `{QUEUE_TABLE}` SET status='error_data' WHERE accession_number='{acc_num}'")

    if all_holdings:
        # 1. Clean start for Staging
        client.delete_table(STAGING_TABLE, not_found_ok=True)
        
        # 2. Strict Schema: No autodetect. This forces filing_date to stay a STRING.
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("accession_number", "STRING"),
                bigquery.SchemaField("filing_date", "STRING"),
                bigquery.SchemaField("nameOfIssuer", "STRING"), # Added this
                bigquery.SchemaField("cusip", "STRING"),
                bigquery.SchemaField("value", "FLOAT"),
                bigquery.SchemaField("sshPrnamt", "FLOAT"),
                bigquery.SchemaField("cik", "STRING"),
                bigquery.SchemaField("manager_name", "STRING"),
                bigquery.SchemaField("sshPrnamtType", "STRING"), # Added for completeness
                bigquery.SchemaField("investmentDiscretion", "STRING"), # Added for completeness
            ],
            write_disposition="WRITE_TRUNCATE",
            autodetect=False  # CRITICAL: Do not let BigQuery guess types
        )
        
        # Load data to Staging
        client.load_table_from_json(all_holdings, STAGING_TABLE, job_config=job_config).result()

        # 3. Atomic Merge with explicit Casting
        merge_sql = f"""
                MERGE `{MASTER_TABLE}` T
                USING `{STAGING_TABLE}` S
                ON T.accession_number = CAST(S.accession_number AS STRING)  -- <--- FIX HERE
                AND T.cusip = CAST(S.cusip AS STRING)                   -- <--- AND HERE
                WHEN NOT MATCHED THEN
                    INSERT (cik, manager_name, issuer_name, cusip, value_usd, shares, filing_date, accession_number)
                    VALUES (
                        CAST(S.cik AS STRING), 
                        S.manager_name, 
                        S.nameOfIssuer, 
                        S.cusip, 
                        CAST(S.value AS INT64), 
                        CAST(S.sshPrnamt AS INT64), 
                        CAST(S.filing_date AS DATETIME), 
                        CAST(S.accession_number AS STRING)
                    )
        """
        client.query(merge_sql).result()
        
        # 4. Mark queue as done
        acc_list = ", ".join([f"'{a}'" for a in success_acc_nums])
        client.query(f"UPDATE `{QUEUE_TABLE}` SET status='done' WHERE accession_number IN ({acc_list})").result()
        logger.info(f"✨ Successfully merged {len(success_acc_nums)} managers.")



    return True

if __name__ == "__main__":
    ENV_LIMIT = int(os.getenv("SCRAPER_LIMIT", "25"))
    seed_queue_if_needed(2020, 2)
    while process_batch(2020, 2):
        logger.info("Batch complete, moving to next...")