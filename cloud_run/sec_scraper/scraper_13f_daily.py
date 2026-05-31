import os
import sys
import time
import logging
import requests
from datetime import datetime, timedelta
from lxml import etree
from google.cloud import bigquery
from google.api_core.exceptions import Forbidden
from pydantic import BaseModel, field_validator, model_validator
from typing import Optional

# --- CONFIGURATION ---
PROJECT_ID = "datascience-projects"
DATASET_ID = "gcp_shareloader"
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
        if v:
            v = str(v).strip()
            # FIX: If the string matches the SEC daily index format 'YYYYMMDD' (length 8, all digits)
            if len(v) == 8 and v.isdigit():
                # Reformat cleanly to 'YYYY-MM-DD 00:00:00'
                return f"{v[0:4]}-{v[4:6]}-{v[6:8]} 00:00:00"
            
            # Fallback for standard inferred dates 'YYYY-MM-DD'
            if "T" not in v and " " not in v:
                return f"{v} 00:00:00"
        return v

    @model_validator(mode='before')
    @classmethod
    def strip_all_strings(cls, data):
        """Fallback safety net: Ensures every incoming string element is stripped of padding."""
        if isinstance(data, dict):
            for k, v in data.items():
                if isinstance(v, str):
                    data[k] = v.strip()
        return data


def parse_xml_to_holdings(xml_content, acc_num, filing_date):
    try:
        tree = etree.fromstring(xml_content.encode('utf-8') if isinstance(xml_content, str) else xml_content)
        nodes = tree.xpath('//*[local-name()="infoTable"]')
        extracted_data = []
        for node in nodes:
            def gv(tag):
                res = node.xpath(f'./*[local-name()="{tag}"]/text()')
                return str(res[0]).strip() if res else None
                
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
            logger.warning(f"🚫 403 Quota hit during {description}. Sleeping {wait}s...")
            time.sleep(wait)
    return False


def run_daily_job(target_date: datetime):
    date_str = target_date.strftime("%Y%m%d")
    year = target_date.year
    qtr = (target_date.month - 1) // 3 + 1
    
    daily_idx_url = f"https://www.sec.gov/Archives/edgar/daily-index/{year}/QTR{qtr}/master.{date_str}.idx"
    logger.info(f"🔍 Fetching Daily SEC Index for Business Day ({target_date.strftime('%Y-%m-%d')}): {daily_idx_url}")
    
    res = requests.get(daily_idx_url, headers=HEADERS)
    if res.status_code == 404:
        logger.warning(f"📭 No daily index found for {date_str}. (The SEC may be closed, it's a holiday, or the index isn't live yet).")
        return
    elif res.status_code != 200:
        logger.error(f"❌ Failed to reach SEC Index. Status code: {res.status_code}")
        return

    lines = res.text.split('\n')
    all_holdings = []
    processed_count = 0

    with requests.Session() as session:
        session.headers.update(HEADERS)
        
        for line in lines:
            if '|13F-HR|' in line:
                parts = line.split('|')
                if len(parts) < 5:
                    continue
                
                cik = parts[0].zfill(10)
                manager_name = parts[1]
                actual_filing_date = parts[3].strip()
                path = parts[4]
                
                acc_num = path.split('/')[-1].split('.')[0]
                dir_path = path.replace('-', '').replace('.txt', '').strip()
                dir_url = f"https://www.sec.gov/Archives/{dir_path}/index.json"
                
                logger.info(f"⏳ Downloading live filing details: {acc_num} ({manager_name})")
                
                try:
                    time.sleep(1.2)
                    dir_res = session.get(dir_url, timeout=30)
                    
                    if dir_res.status_code in [403, 503]:
                        logger.warning(f"🚨 SEC server cooling limit reached ({dir_res.status_code}). Stopping index batch early.")
                        break
                    if dir_res.status_code != 200:
                        continue
                        
                    items = dir_res.json().get('directory', {}).get('item', [])
                    xml_items = [i for i in items if i['name'].lower().endswith('.xml')]
                    xml_name = next((i['name'] for i in xml_items if 'infotable' in i['name'].lower()), None)
                    
                    if not xml_name:
                        candidates = [i for i in xml_items if 'primary' not in i['name'].lower() and 'doc' not in i['name'].lower()]
                        if candidates:
                            xml_name = max(candidates, key=lambda x: int(x['size'] or 0))['name']

                    if xml_name:
                        xml_url = dir_url.replace('index.json', xml_name)
                        xml_res = session.get(xml_url, timeout=15)
                        
                        holdings = parse_xml_to_holdings(xml_res.text, acc_num, actual_filing_date)
                        for h in holdings:
                            h['cik'] = cik
                            h['manager_name'] = manager_name
                            all_holdings.append(Holding(**h).model_dump())
                        
                        processed_count += 1
                        logger.info(f"✔️ {manager_name} parsed successfully with {len(holdings)} lines.")
                except Exception as e:
                    logger.error(f"❌ Failed to parse filing {acc_num}: {e}")

    # --- TRANSMIT DATA INTO BIGQUERY ---
    if all_holdings:
        logger.info(f"📤 Uploading {len(all_holdings)} rows into staging table...")
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

        merge_sql = f"""
            MERGE `{MASTER_TABLE}` T
            USING `{STAGING_TABLE}` S
            ON T.accession_number = S.accession_number AND T.cusip = S.cusip
            WHEN NOT MATCHED THEN
                INSERT (cik, manager_name, issuer_name, cusip, value_usd, shares, put_call, filing_date, accession_number)
                VALUES (S.cik, S.manager_name, S.issuer_name, S.cusip, CAST(S.value_usd AS INT64), CAST(S.shares AS INT64), S.put_call, CAST(S.filing_date AS DATETIME), S.accession_number)
        """
        if execute_dml_safe(merge_sql, "Daily Data Merge"):
            logger.info(f"✨ Daily sync complete. Processed {processed_count} filings.")
    else:
        logger.info("🏁 No Form 13F data discovered in the index file for this date range.")


def get_t_minus_1_business_day(base_date: datetime) -> datetime:
    """Calculates T-1 business day relative to base_date."""
    weekday = base_date.weekday()  # 0=Monday, 5=Saturday, 6=Sunday
    
    if weekday == 0:    # Monday -> Look back to Friday (3 days)
        days_to_subtract = 3
    elif weekday == 6:  # Sunday -> Look back to Friday (2 days)
        days_to_subtract = 2
    else:               # Tuesday through Saturday -> Look back 1 calendar day
        days_to_subtract = 1
        
    return base_date - timedelta(days=days_to_subtract)


if __name__ == "__main__":
    today = datetime.utcnow()
    
    if len(sys.argv) > 1 and sys.argv[1] != "--yesterday":
        try:
            target_run_date = datetime.strptime(sys.argv[1], "%Y%m%d")
            logger.info(f"🎯 Override detected. Target date explicitly set to: {target_run_date.strftime('%Y-%m-%d')}")
        except ValueError:
            logger.error("❌ Invalid date format argument. Please use YYYYMMDD.")
            sys.exit(1)
    else:
        target_run_date = get_t_minus_1_business_day(today)
        
    run_daily_job(target_run_date)