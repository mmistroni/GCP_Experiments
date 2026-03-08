import os
import time
import logging
import requests
from lxml import etree
from google.cloud import bigquery
from google.api_core.exceptions import Forbidden
from pydantic import BaseModel, field_validator
from typing import Optional, List

# --- CONFIGURATION (New Tables for Form 4) ---
PROJECT_ID = "datascience-projects"
DATASET_ID = "gcp_shareloader"
QUEUE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.form4_queue"
MASTER_TABLE = f"{PROJECT_ID}.{DATASET_ID}.form4_master"
STAGING_TABLE = f"{PROJECT_ID}.{DATASET_ID}.temp_form4_staging"

HEADERS = {'User-Agent': 'ResearchAgent/1.0 (contact_mm@yourdomain.com)'}

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)
client = bigquery.Client(project=PROJECT_ID)

# --- MODELS ---
class Form4Transaction(BaseModel):
    accession_number: str
    filing_date: str
    reporting_owner: str
    issuer_ticker: str
    security_title: str
    transaction_date: Optional[str] = None
    transaction_code: Optional[str] = None  # P = Purchase, S = Sale, A = Grant
    shares: float = 0.0
    price_per_share: float = 0.0
    is_derivative: bool = False
    is_director: bool = False
    is_officer: bool = False
    is_ten_percent_owner: bool = False

    @field_validator('filing_date')
    @classmethod
    def format_for_datetime(cls, v):
        if v and " " not in v:
            return f"{v} 00:00:00"
        return v

# --- CORE FUNCTIONS ---

def seed_queue_if_needed(year, qtr):
    """Identifies Form 4 and Form 4/A (Amendments) from SEC Index."""
    logger.info(f"Checking queue for {year} Q{qtr}...")
    check_query = f"SELECT count(*) as cnt FROM `{QUEUE_TABLE}` WHERE year={year} AND qtr={qtr}"
    count = list(client.query(check_query))[0].cnt
    
    if count > 0:
        logger.info(f"Queue already has {count} items. Skipping seed.")
        return

    master_idx_url = f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{qtr}/master.idx"
    idx_res = requests.get(master_idx_url, headers=HEADERS)
    
    queue_data = []
    for line in idx_res.text.split('\n'):
        # Target Form 4 and Amendments
        if '|4|' in line or '|4/A|' in line:
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
                "qtr": qtr
            })

    if queue_data:
        logger.info(f"Seeding {len(queue_data)} Form 4 filings into queue...")
        client.load_table_from_json(queue_data, QUEUE_TABLE).result()

def parse_form4_xml(xml_content, acc_num, filing_date):
    """Parses both Table I (Non-Derivative) and Table II (Derivative) transactions."""
    try:
        tree = etree.fromstring(xml_content.encode('utf-8') if isinstance(xml_content, str) else xml_content)
        
        # Metadata
        ticker = tree.xpath('//issuerTicker/text()')[0] if tree.xpath('//issuerTicker/text()') else "N/A"
        owner = tree.xpath('//reportingOwnerName/text()')[0] if tree.xpath('//reportingOwnerName/text()') else "Unknown"
        
        # Insider Status
        is_director = tree.xpath('//isDirector/text()') == ['1']
        is_officer = tree.xpath('//isOfficer/text()') == ['1']
        is_ten_pct = tree.xpath('//isTenPercentOwner/text()') == ['1']

        transactions = []

        # Table I: Non-Derivative Securities (Direct Stock)
        for node in tree.xpath('//nonDerivativeTransaction'):
            def gv(path):
                res = node.xpath(f'./{path}/value/text()')
                return res[0] if res else None

            transactions.append({
                "accession_number": acc_num,
                "filing_date": filing_date,
                "reporting_owner": owner,
                "issuer_ticker": ticker,
                "security_title": gv("securityTitle") or "Common Stock",
                "transaction_date": gv("transactionDate"),
                "transaction_code": node.xpath('.//transactionCode/text()')[0] if node.xpath('.//transactionCode/text()') else None,
                "shares": float(gv("transactionShares") or 0.0),
                "price_per_share": float(gv("transactionPricePerShare") or 0.0),
                "is_derivative": False,
                "is_director": is_director,
                "is_officer": is_officer,
                "is_ten_percent_owner": is_ten_pct
            })

        # Table II: Derivative Securities (Options, Warrants, RSUs)
        for node in tree.xpath('//derivativeTransaction'):
            def gv(path):
                res = node.xpath(f'./{path}/value/text()')
                return res[0] if res else None

            transactions.append({
                "accession_number": acc_num,
                "filing_date": filing_date,
                "reporting_owner": owner,
                "issuer_ticker": ticker,
                "security_title": gv("securityTitle") or "Derivative",
                "transaction_date": gv("transactionDate"),
                "transaction_code": node.xpath('.//transactionCode/text()')[0] if node.xpath('.//transactionCode/text()') else None,
                "shares": float(gv("transactionShares") or 0.0),
                "price_per_share": float(gv("transactionPricePerShare") or 0.0),
                "is_derivative": True,
                "is_director": is_director,
                "is_officer": is_officer,
                "is_ten_percent_owner": is_ten_pct
            })
            
        return transactions
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
    SCRAPER_LIMIT = 25 
    query = f"SELECT * FROM `{QUEUE_TABLE}` WHERE status IN ('pending', 'error_data') AND year={year} AND qtr={qtr} LIMIT {SCRAPER_LIMIT}"
    
    df = client.query(query).to_dataframe()
    if df.empty: return False

    all_txs = []
    success_acc = []
    failed_acc = []

    with requests.Session() as session:
        session.headers.update(HEADERS)
        for _, row in df.iterrows():
            acc_num = row['accession_number']
            try:
                time.sleep(1.5)
                dir_res = session.get(row['dir_url'], timeout=30)
                
                if dir_res.status_code in [403, 503]:
                    logger.warning(f"🚨 Rate limit or Server Error {dir_res.status_code}. Breaking batch.")
                    break
                
                items = dir_res.json().get('directory', {}).get('item', [])
                # Form 4 XMLs are usually named 'doc4.xml' or 'primary_doc.xml'
                xml_name = next((i['name'] for i in items if i['name'].endswith('.xml')), None)

                if xml_name:
                    xml_url = row['dir_url'].replace('index.json', xml_name)
                    xml_res = session.get(xml_url, timeout=15)
                    
                    # INFER DATE based on Quarter End
                    qtr_map = {1: "03-31", 2: "06-30", 3: "09-30", 4: "12-31"}
                    inferred_date = f"{year}-{qtr_map[qtr]} 00:00:00"

                    txs = parse_form4_xml(xml_res.text, acc_num, inferred_date)
                    for t in txs:
                        all_txs.append(Form4Transaction(**t).model_dump())
                    
                    success_acc.append(acc_num)
                    logger.info(f"✔️ {row['company_name']} ({ticker}) parsed.")
                else:
                    failed_acc.append(acc_num)

            except Exception as e:
                logger.error(f"❌ Failed {acc_num}: {e}")
                failed_acc.append(acc_num)

    # --- DB UPDATES ---
    if failed_acc:
        fail_list = ", ".join([f"'{a}'" for a in failed_acc])
        execute_dml_safe(f"UPDATE `{QUEUE_TABLE}` SET status='error_data' WHERE accession_number IN ({fail_list})", "Fail Update")

    if all_txs:
        # Note: We TRUNCATE staging to keep it fresh
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        client.load_table_from_json(all_txs, STAGING_TABLE, job_config=job_config).result()

        merge_sql = f"""
            MERGE `{MASTER_TABLE}` T
            USING `{STAGING_TABLE}` S
            ON T.accession_number = S.accession_number 
               AND T.reporting_owner = S.reporting_owner 
               AND T.security_title = S.security_title 
               AND T.shares = S.shares
            WHEN NOT MATCHED THEN
                INSERT (accession_number, filing_date, reporting_owner, issuer_ticker, security_title, 
                        transaction_date, transaction_code, shares, price_per_share, is_derivative, 
                        is_director, is_officer, is_ten_percent_owner)
                VALUES (S.accession_number, CAST(S.filing_date AS DATETIME), S.reporting_owner, S.issuer_ticker, 
                        S.security_title, S.transaction_date, S.transaction_code, S.shares, S.price_per_share, 
                        S.is_derivative, S.is_director, S.is_officer, S.is_ten_percent_owner)
        """
        execute_dml_safe(merge_sql, "Merge Form 4")

        if success_acc:
            succ_list = ", ".join([f"'{a}'" for a in success_acc])
            execute_dml_safe(f"UPDATE `{QUEUE_TABLE}` SET status='done' WHERE accession_number IN ({succ_list})", "Success Update")

    return True

if __name__ == "__main__":
    YEAR = int(os.getenv('YEAR', 2020))
    QUARTER = int(os.getenv('QUARTER', 1))
    seed_queue_if_needed(YEAR, QUARTER)
    while process_batch(YEAR, QUARTER):
        time.sleep(10)