import os
import requests
import logging
import time
import argparse
from datetime import datetime, timedelta
from lxml import etree
from google.cloud import bigquery
from tqdm import tqdm
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- CONFIGURATION ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("form4_agent")

# SEC Identity - REQUIRED
HEADERS = {'User-Agent': 'Institutional Research your-email@example.com'}
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT")
DATASET = "gcp_shareloader"
MASTER_TABLE = "form4_master"
STAGING_TABLE = "stg_form4"

# --- STAGE 1: STABLE NETWORK SESSION ---
def get_session():
    """Synchronous session with retries for stability."""
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.headers.update(HEADERS)
    return session

# --- STAGE 2: ROBUST PARSER ---
def parse_form4_xml(xml_content, acc):
    try:
        root = etree.fromstring(xml_content)
        trades = []
        ticker = root.xpath("string(//*[local-name()='issuerTradingSymbol'])")
        owner = root.xpath("string(//*[local-name()='reportingOwnerName'])")
        
        tx_nodes = root.xpath("//*[local-name()='nonDerivativeTransaction']")
        for tx in tx_nodes:
            shares = tx.xpath("string(.//*[local-name()='transactionShares']/*[local-name()='value'])")
            price = tx.xpath("string(.//*[local-name()='transactionPricePerShare']/*[local-name()='value'])")
            code = tx.xpath("string(.//*[local-name()='transactionAcquiredDisposedCode']/*[local-name()='value'])")
            date = tx.xpath("string(.//*[local-name()='transactionDate']/*[local-name()='value'])")
            t_code = tx.xpath("string(.//*[local-name()='transactionCode'])")

            if ticker and shares and float(shares) > 0:
                trades.append({
                    "ticker": ticker,
                    "owner_name": owner,
                    "transaction_code": t_code,
                    "shares": float(shares),
                    "price": float(price) if price else 0.0,
                    "transaction_side": "BUY" if code == 'A' else "SELL",
                    "filing_date": date,
                    "accession_number": acc,
                    "ingested_at": datetime.utcnow().isoformat()
                })
        return trades
    except Exception:
        return []

# --- STAGE 3: BIGQUERY MERGE ---
def load_and_merge(trades_list):
    if not trades_list:
        logger.warning("📭 No trades found to load.")
        return

    client = bigquery.Client()
    stg_ref = f"{PROJECT_ID}.{DATASET}.{STAGING_TABLE}"
    mst_ref = f"{PROJECT_ID}.{DATASET}.{MASTER_TABLE}"

    # Load to Staging
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
        create_disposition="CREATE_IF_NEEDED"
    )
    client.load_table_from_json(trades_list, stg_ref, job_config=job_config).result()

    # Deduplicated Merge
    merge_sql = f"""
    MERGE `{mst_ref}` T USING `{stg_ref}` S
    ON T.accession_number = S.accession_number AND T.shares = S.shares AND T.ticker = S.ticker
    WHEN NOT MATCHED THEN INSERT ROW
    """
    client.query(merge_sql).result()
    logger.info(f"✅ Merged {len(trades_list)} rows into {MASTER_TABLE}")

# --- STAGE 4: FLOW CONTROL ---
def run_form4(mode: str, years: list = None, limit: int = 50):
    session = get_session()
    all_trades = []
    mode = mode.upper()
    filings_to_process = []

    # 1. Collect Filing URLs
    if mode == "DAILY":
        for i in range(5):
            target_date = datetime.now() - timedelta(days=i)
            # Skip weekends to save requests
            if target_date.weekday() >= 5: continue 
            
            date_str = target_date.strftime("%Y%m%d")
            url = f"https://www.sec.gov/Archives/edgar/daily-index/{target_date.year}/QTR{(target_date.month-1)//3+1}/master.{date_str}.idx"
            res = session.get(url, timeout=10)
            if res.status_code == 200:
                for line in res.text.splitlines():
                    if '|4|' in line:
                        p = line.split('|')
                        acc = p[4].split('/')[-1].replace('.txt', '').replace('-', '')
                        filings_to_process.append({"cik": p[0], "acc": acc, "url": f"https://www.sec.gov/Archives/edgar/data/{p[0]}/{acc}/index.json"})

    elif mode == "BACKFILL":
        for year in (years or [2024]):
            for qtr in range(1, 5):
                url = f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{qtr}/master.idx"
                res = session.get(url, timeout=15)
                if res.status_code == 200:
                    for line in res.text.splitlines():
                        if '|4|' in line:
                            p = line.split('|')
                            acc = p[4].split('/')[-1].replace('.txt', '').replace('-', '')
                            filings_to_process.append({"cik": p[0], "acc": acc, "url": f"https://www.sec.gov/Archives/edgar/data/{p[0]}/{acc}/index.json"})

    # 2. Process with Progress Bar
    logger.info(f"🔍 Found {len(filings_to_process)} filings. Processing up to {limit}...")
    for f in tqdm(filings_to_process[:limit], desc=f"Processing {mode}"):
        try:
            dir_res = session.get(f['url'], timeout=5).json()
            xml_name = next(i['name'] for i in dir_res['directory']['item'] if i['name'].endswith('.xml'))
            xml_url = f['url'].replace('index.json', xml_name)
            xml_res = session.get(xml_url, timeout=5)
            all_trades.extend(parse_form4_xml(xml_res.content, f['acc']))
            time.sleep(0.11) # Maintain ~9 requests per second
        except Exception:
            continue

    # 3. Final Load
    load_and_merge(all_trades)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["DAILY", "BACKFILL"], required=True)
    parser.add_argument("--years", nargs="+", type=int)
    parser.add_argument("--limit", type=int, default=50)
    args = parser.parse_args()

    run_form4(mode=args.mode, years=args.years, limit=args.limit)