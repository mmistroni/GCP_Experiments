import os
import requests
import logging
import time
import argparse
import json
from datetime import datetime, timedelta
from lxml import etree
from google.cloud import bigquery
from tqdm import tqdm
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import sys

# --- CONFIGURATION ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("form4_agent")

HEADERS = {'User-Agent': 'Institutional Research your-email@example.com'}
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "datascience-projects")
DATASET = "gcp_shareloader"
MASTER_TABLE = "form4_master"
STAGING_TABLE = "stg_form4"

def get_session():
    session = requests.Session()
    retry_strategy = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retry_strategy))
    session.headers.update(HEADERS)
    return session

def parse_form4_xml(xml_content, acc):
    try:
        root = etree.fromstring(xml_content)
        trades = []
        issuer = root.xpath("string(//*[local-name()='issuerName'])")
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
                    "issuer": issuer,
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

def load_and_merge(trades_list, mode="BACKFILL"):
    """
    Synchronously loads data into a staging table and merges it into the master table.
    Works for both BACKFILL and DAILY modes.
    """
    if not trades_list:
        logger.info(f"📭 [{mode}] No trades to load. Skipping BigQuery operation.")
        return

    # 1. CLEANING: Ensure types match BigQuery schema expectations
    for trade in trades_list:
        if 'filing_date' in trade and hasattr(trade['filing_date'], 'strftime'):
            trade['filing_date'] = trade['filing_date'].strftime('%Y-%m-%d')
        # BigQuery expects FLOAT64 for price/shares; ensure no string leftovers
        trade['shares'] = float(trade.get('shares', 0))
        trade['price'] = float(trade.get('price', 0.0))

    # 2. SETUP: Client and Table References
    client = bigquery.Client(project=os.environ['GOOGLE_CLOUD_PROJECT'])
    master_ref = f"{PROJECT_ID}.{DATASET}.{MASTER_TABLE}"
    
    # Create a unique staging table name using a timestamp
    timestamp_id = int(time.time())
    staging_table_id = f"{PROJECT_ID}.{DATASET}.{STAGING_TABLE}_tmp_{timestamp_id}"
    
    schema = [
        bigquery.SchemaField("filing_date", "DATE"),
        bigquery.SchemaField("ticker", "STRING"),
        bigquery.SchemaField("issuer", "STRING"),
        bigquery.SchemaField("owner_name", "STRING"),
        bigquery.SchemaField("transaction_code", "STRING"),
        bigquery.SchemaField("shares", "FLOAT64"),
        bigquery.SchemaField("price", "FLOAT64"),
        bigquery.SchemaField("transaction_side", "STRING"),
        bigquery.SchemaField("accession_number", "STRING"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP"),
    ]

    try:
        # 3. STAGE: Load JSON data to a temporary table
        job_config = bigquery.LoadJobConfig(
            schema=schema, 
            write_disposition="WRITE_TRUNCATE", 
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        )
        
        logger.info(f"🛰️ [{mode}] Starting BQ Load to staging: {staging_table_id}")
        load_job = client.load_table_from_json(trades_list, staging_table_id, job_config=job_config)
        
        # --- SYNCHRONOUS WAIT ---
        load_job.result() 
        logger.info(f"📥 [{mode}] Data landed in staging. (Job ID: {load_job.job_id})")

        # 4. MERGE: Atomic Upsert to Master Table
        merge_query = f"""
        MERGE `{master_ref}` T
        USING `{staging_table_id}` S
        ON T.accession_number = S.accession_number 
           AND T.ticker = S.ticker 
           AND T.shares = S.shares
        WHEN NOT MATCHED THEN
          INSERT (filing_date, ticker, issuer, owner_name, transaction_code, shares, price, transaction_side, accession_number, ingested_at)
          VALUES (S.filing_date, S.ticker, S.issuer, S.owner_name, S.transaction_code, S.shares, S.price, S.transaction_side, S.accession_number, S.ingested_at)
        """
        
        logger.info(f"🔄 [{mode}] Executing MERGE into {MASTER_TABLE}...")
        merge_job = client.query(merge_query)
        
        # --- SYNCHRONOUS WAIT ---
        merge_job.result() 
        
        logger.info(f"✅ [{mode}] Successfully merged {len(trades_list)} records.")

    except Exception as e:
        logger.error(f"❌ [{mode}] BigQuery Error: {str(e)}")
        # If it's a Backfill, we want to stop entirely if BQ fails
        if mode == "BACKFILL":
            raise e
    finally:
        # 5. CLEANUP: Always remove the staging table
        client.delete_table(staging_table_id, not_found_ok=True)
        logger.info(f"🧹 [{mode}] Staging table {staging_table_id} cleaned up.")


def run_form4(mode: str, years: list = None, quarters: list = None, limit: int = 50):
    session = get_session()
    mode = mode.upper()
    target_quarters = quarters or [1, 2, 3, 4]

    if mode == "DAILY":
        logger.info("📅 Starting DAILY flow.")
        # ... (Your existing daily logic remains the same) ...
        # (Included in full script but truncated here for brevity)

    elif mode == "BACKFILL":
        target_years = years or [2024]
        logger.info(f"💾 BACKFILL: Years {target_years} | Quarters {target_quarters} | Limit {limit}")
        
        for year in target_years:
            for qtr in target_quarters:
                logger.info(f"⏳ Processing: {year} Q{qtr}")
                qtr_trades = []
                url = f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{qtr}/master.idx"
                res = session.get(url, timeout=15)
                
                if res.status_code == 200:
                    filings = [line for line in res.text.splitlines() if '|4|' in line]
                    # Handle Limit 0 as Unlimited
                    num_to_scrape = len(filings) if limit <= 0 else min(len(filings), limit)
                    
                    for line in tqdm(filings[:num_to_scrape], desc=f"📦 Q{qtr}"):
                        try:
                            p = line.split('|')
                            acc = p[4].split('/')[-1].replace('.txt', '').replace('-', '')
                            idx_url = f"https://www.sec.gov/Archives/{p[4].replace('.txt', '-index.json')}"
                            
                            dir_res = session.get(idx_url, timeout=5).json()
                            xml_item = next(i['name'] for i in dir_res['directory']['item'] if i['name'].endswith('.xml'))
                            xml_url = f"https://www.sec.gov/Archives/edgar/data/{p[0]}/{acc}/{xml_item}"
                            
                            xml_res = session.get(xml_url, timeout=5)
                            qtr_trades.extend(parse_form4_xml(xml_res.content, acc))
                            time.sleep(0.11)
                        except: continue

                    if qtr_trades:
                        load_and_merge(qtr_trades)
                else:
                    logger.warning(f"⚠️ Index not found for {year} Q{qtr}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["DAILY", "BACKFILL"])
    parser.add_argument("--years", nargs="+", type=int)
    parser.add_argument("--quarters", nargs="+", type=int)
    parser.add_argument("--limit", type=int)
    args = parser.parse_args()

    # Precedence: CLI -> ENV -> Default
    mode = args.mode or os.getenv("AGENT_MODE", "DAILY")
    
    raw_years = os.getenv("AGENT_YEARS", 2024)
    years = args.years or ([int(y) for y in raw_years.split(",")] if raw_years else [2024])
    
    raw_qtrs = os.getenv("AGENT_QUARTERS", 1)
    quarters = args.quarters or ([int(q) for q in raw_qtrs.split(",")] if raw_qtrs else None)
    
    limit = args.limit if args.limit is not None else int(os.getenv("AGENT_LIMIT", 50))

    run_form4(mode=mode, years=years, quarters=quarters, limit=limit)