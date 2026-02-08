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
import sys
import os



# --- CONFIGURATION ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("form4_agent")

# SEC Identity - REQUIRED
HEADERS = {'User-Agent': 'Institutional Research your-email@example.com'}
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", )
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
import json

from google.cloud import bigquery
import os
import json

def load_and_merge(trades_list):
    if not trades_list:
        logger.info("📭 No trades to load.")
        return

    # 1. DATA CLEANING: Ensure filing_date is a string 'YYYY-MM-DD' 
    # and numbers are actual floats/ints before sending to BQ
    for trade in trades_list:
        if 'filing_date' in trade and hasattr(trade['filing_date'], 'strftime'):
            trade['filing_date'] = trade['filing_date'].strftime('%Y-%m-%d')

    # LOCAL DEBUG
    with open("debug_payload.json", "w") as f:
        json.dump(trades_list, f, indent=2)
    logger.info("💾 Saved local 'debug_payload.json' for inspection.")

    client = bigquery.Client(project=os.environ['GOOGLE_CLOUD_PROJECT'])
    master_ref = f"{PROJECT_ID}.{DATASET}.{MASTER_TABLE}"
    
    # 2. EXPLICIT SCHEMA: This prevents the "STRING vs DATE" errors
    # Add/remove fields here based on your actual Form 4 JSON keys
    schema = [
        bigquery.SchemaField("filing_date", "DATE"),
        bigquery.SchemaField("ticker", "STRING"),
        bigquery.SchemaField("issuer", "STRING"), # Note: Your parser doesn't actually create this yet!
        bigquery.SchemaField("owner_name", "STRING"),
        bigquery.SchemaField("transaction_code", "STRING"),
        bigquery.SchemaField("shares", "FLOAT64"),
        bigquery.SchemaField("price", "FLOAT64"),
        bigquery.SchemaField("transaction_side", "STRING"), # <--- ADDED THIS
        bigquery.SchemaField("accession_number", "STRING"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP"),    # <--- ADDED THIS
    ]

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition="WRITE_APPEND",
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )

    try:
        logger.info(f"⏳ Loading {len(trades_list)} rows to {master_ref}...")
        job = client.load_table_from_json(trades_list, master_ref, job_config=job_config)
        job.result() 
        logger.info(f"✅ Successfully appended data to {MASTER_TABLE}.")
    except Exception as e:
        logger.error("❌ BIGQUERY REJECTION REASON:")
        if hasattr(e, 'errors'):
            for error in e.errors:
                logger.error(f"  - {error['message']}")
        else:
            logger.error(f"  - Technical error: {str(e)}")
        raise e

# --- STAGE 4: FLOW CONTROL ---
def run_form4(mode: str, years: list = None, limit: int = 50):
    """
    The central switchboard for SEC Form 4 data ingestion.
    
    Args:
        mode (str): Execution mode. 
            'DAILY' scans the last 5 calendar days of SEC daily indices.
            'BACKFILL' scans the quarterly master indices for the specified years.
        years (list, optional): A list of years (integers) to process if in BACKFILL mode.
            Defaults to [2024] if None.
        limit (int): The maximum number of filings to process per day (DAILY) 
            or per quarter (BACKFILL). Set high (e.g., 200000) for a full scrape.
            
    Notes:
        - Processes data in batches (daily or quarterly) to manage memory usage.
        - Commits to BigQuery at the end of every batch.
        - Respects the SEC's 10 requests-per-second limit via time.sleep(0.11).
    """
    session = get_session()
    mode = mode.upper()

    if mode == "DAILY":
        logger.info("📅 Starting DAILY flow: Processing last 5 days.")
        for i in range(5):
            target_date = datetime.now() - timedelta(days=i)
            # Skip weekends as the SEC does not update daily indices then
            if target_date.weekday() >= 5: 
                continue 
            
            date_str = target_date.strftime("%Y%m%d")
            qtr = (target_date.month - 1) // 3 + 1
            url = f"https://www.sec.gov/Archives/edgar/daily-index/{target_date.year}/QTR{qtr}/master.{date_str}.idx"
            
            daily_trades = []
            logger.info(f"🔍 Checking Daily Index: {date_str}")
            
            # Fetch the day's index and extract filings
            res = session.get(url, timeout=10)
            if res.status_code == 200:
                filings = []
                for line in res.text.splitlines():
                    if '|4|' in line:
                        p = line.split('|')
                        acc = p[4].split('/')[-1].replace('.txt', '').replace('-', '')
                        filings.append({
                            "cik": p[0], 
                            "acc": acc, 
                            "url": f"https://www.sec.gov/Archives/edgar/data/{p[0]}/{acc}/index.json"
                        })
                
                # Process the day's filings
                num_to_scrape = min(len(filings), limit)
                for f in tqdm(filings[:num_to_scrape], desc=f"🚀 Daily {date_str}"):
                    try:
                        # Standard SEC Directory Traversal
                        dir_res = session.get(f['url'], timeout=5).json()
                        xml_name = next(item['name'] for item in dir_res['directory']['item'] if item['name'].endswith('.xml'))
                        xml_url = f['url'].replace('index.json', xml_name)
                        xml_res = session.get(xml_url, timeout=5)
                        
                        trades = parse_form4_xml(xml_res.content, f['acc'])
                        daily_trades.extend(trades)
                        time.sleep(0.11) # Maintain ~9 requests per second
                    except Exception:
                        continue

                # Batch Ingest for the day
                if daily_trades:
                    load_and_merge(daily_trades)
                    daily_trades.clear() # Free up memory
            else:
                logger.warning(f"⚠️ No index found for {date_str}.")

    elif mode == "BACKFILL":
        target_years = years or [2024]
        logger.info(f"💾 Starting BACKFILL flow for years: {target_years}")
        
        for year in target_years:
            for qtr in range(1, 5):
                logger.info(f"⏳ Processing Batch: {year} Q{qtr}")
                qtr_trades = []
                
                # Fetch Quarterly Index
                url = f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{qtr}/master.idx"
                res = session.get(url, timeout=15)
                
                if res.status_code == 200:
                    lines = res.text.splitlines()
                    # Quarterly master.idx contains a longer header; '|4|' identifies Form 4s
                    filings = []
                    for line in lines:
                        if '|4|' in line:
                            p = line.split('|')
                            acc_path = p[4] # Format: edgar/data/CIK/ACC-NUMBER.txt
                            acc = acc_path.split('/')[-1].replace('.txt', '').replace('-', '')
                            filings.append({
                                "cik": p[0], 
                                "acc": acc, 
                                "url": f"https://www.sec.gov/Archives/{acc_path.replace('.txt', '-index.json')}"
                            })

                    # Process the quarter's filings
                    num_to_scrape = min(len(filings), limit)
                    for f in tqdm(filings[:num_to_scrape], desc=f"📦 {year} Q{qtr}"):
                        try:
                            dir_res = session.get(f['url'], timeout=5).json()
                            xml_item = next(i['name'] for i in dir_res['directory']['item'] if i['name'].endswith('.xml'))
                            # Build URL from the directory structure
                            xml_url = f"https://www.sec.gov/Archives/edgar/data/{f['cik']}/{f['acc']}/{xml_item}"
                            xml_res = session.get(xml_url, timeout=5)
                            
                            trades = parse_form4_xml(xml_res.content, f['acc'])
                            qtr_trades.extend(trades)
                            time.sleep(0.11)
                        except Exception:
                            continue

                    # Batch Ingest for the quarter
                    if qtr_trades:
                        load_and_merge(qtr_trades)
                        logger.info(f"✅ Year {year} Q{qtr} complete. Committing records.")
                        qtr_trades.clear() # Essential for memory management
                else:
                    logger.warning(f"⚠️ Could not access index for {year} Q{qtr}.")

    logger.info("🏁 Form 4 Flow Execution Finished.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["DAILY", "BACKFILL"])
    parser.add_argument("--years", nargs="+", type=int)
    parser.add_argument("--limit", type=int) # Remove default here to check fallback
    
    args = parser.parse_args()

    # --- FALLBACK LOGIC ---
    # Check if no arguments were passed (length is 1 because sys.argv[0] is the script name)
    if len(sys.argv) == 1:
        logger.info("No CLI arguments detected. Falling back to Environment Variables.")
        
    # Priority: CLI Arg -> Environment Variable -> Hardcoded Default
    mode = args.mode or os.getenv("AGENT_MODE", "DAILY")
    
    # Handle the years list (env vars are strings, so we must split and convert)
    env_years = os.getenv("AGENT_YEARS")
    years = args.years or ([int(y) for y in env_years.split(",")] if env_years else [2024])
    
    # Handle the limit
    limit = args.limit or int(os.getenv("AGENT_LIMIT", 50))

    # Trigger the agent
    run_form4(mode=mode, years=years, limit=limit)