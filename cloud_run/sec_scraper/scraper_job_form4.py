import os
import requests
import logging
import time
import argparse
import json
from datetime import datetime, timedelta
import pytz
from lxml import etree
from google.cloud import bigquery
from tqdm import tqdm
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import sys

# --- CONFIGURATION ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("form4_agent")

HEADERS = {'User-Agent': 'Institutional Research mmistroni@gmail.com'}
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "datascience-projects")
DATASET = "gcp_shareloader"
MASTER_TABLE = "form4_master"
STAGING_TABLE = "stg_form4"

# --- STAGE 1: NETWORK & SESSION ---
def get_session():
    session = requests.Session()
    retry_strategy = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retry_strategy))
    session.headers.update(HEADERS)
    return session

def safe_get(session, url, **kwargs):
    """Wrapper to handle SEC 429 Rate Limits gracefully."""
    response = session.get(url, **kwargs)
    if response.status_code == 429:
        retry_after = int(response.headers.get("Retry-After", 60))
        logger.warning(f"🚨 SEC Rate Limit hit! Sleeping for {retry_after}s...")
        time.sleep(retry_after)
        return safe_get(session, url, **kwargs)
    return response

# --- STAGE 2: PARSING ---
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

# --- STAGE 3: DATA LOADING ---
def load_and_merge(trades_list, mode="BACKFILL"):
    if not trades_list:
        logger.info(f"📭 [{mode}] No trades to load.")
        return

    # Keep your specific client instantiation
    client = bigquery.Client(project=os.environ['GOOGLE_CLOUD_PROJECT'])
    master_ref = f"{PROJECT_ID}.{DATASET}.{MASTER_TABLE}"
    staging_table_id = f"{PROJECT_ID}.{DATASET}.{STAGING_TABLE}_tmp_{int(time.time())}"
    
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
        job_config = bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_TRUNCATE", source_format="NEWLINE_DELIMITED_JSON")
        load_job = client.load_table_from_json(trades_list, staging_table_id, job_config=job_config)
        load_job.result() # Wait for load

        merge_query = f"""
        MERGE `{master_ref}` T
        USING `{staging_table_id}` S
        ON T.accession_number = S.accession_number AND T.ticker = S.ticker AND T.shares = S.shares
        WHEN NOT MATCHED THEN
          INSERT (filing_date, ticker, issuer, owner_name, transaction_code, shares, price, transaction_side, accession_number, ingested_at)
          VALUES (S.filing_date, S.ticker, S.issuer, S.owner_name, S.transaction_code, S.shares, S.price, S.transaction_side, S.accession_number, S.ingested_at)
        """
        client.query(merge_query).result() # Wait for merge
        logger.info(f"✅ [{mode}] Successfully merged {len(trades_list)} records.")
    finally:
        client.delete_table(staging_table_id, not_found_ok=True)

# --- STAGE 4: MAIN FLOW ---
def run_form4(mode: str, years: list = None, quarters: list = None, limit: int = 50):
    session = get_session()
    mode = mode.upper()

    if mode == "DAILY":
        # 1. Determine the Target Date
        est = pytz.timezone('US/Eastern')
        
        # Check if user provided a specific date, otherwise default to yesterday
        manual_date = os.getenv("AGENT_DATE")
        if manual_date:
            target_date = datetime.strptime(manual_date, "%Y-%m-%d").replace(tzinfo=est)
            logger.info(f"📅 DAILY: Using manually specified date: {manual_date}")
        else:
            # Default to Yesterday
            target_date = datetime.now(est) - timedelta(days=1)
            logger.info(f"📅 DAILY: Defaulting to 'Yesterday' logic.")

        # 2. Skip weekends (SEC is closed)
        if target_date.weekday() >= 5:
            logger.warning(f"🛑 {target_date.strftime('%Y-%m-%d')} is a weekend. SEC has no daily index.")
            return

        date_str = target_date.strftime("%Y%m%d")
        qtr = (target_date.month - 1) // 3 + 1
        url = f"https://www.sec.gov/Archives/edgar/daily-index/{target_date.year}/QTR{qtr}/master.{date_str}.idx"
        
        logger.info(f"🔍 DAILY: Checking Index for {date_str} at {url}")    
        res = safe_get(session, url)
        
        if res.status_code == 200:
            filings = [line for line in res.text.splitlines() if '|4|' in line]
            num_to_scrape = len(filings) if limit <= 0 else min(len(filings), limit)
            daily_trades = []

            for line in tqdm(filings[:num_to_scrape], desc=f"🚀 Daily {date_str}"):
                try:
                    p = line.split('|')
                    acc = p[4].split('/')[-1].replace('.txt', '').replace('-', '')
                    # Daily path requires different URL construction than Backfill
                    xml_url = f"https://www.sec.gov/Archives/edgar/data/{p[0]}/{acc}/form4.xml"
                    
                    xml_res = safe_get(session, xml_url, timeout=5)
                    if xml_res.status_code != 200: # Fallback for non-standard XML names
                        idx_url = f"https://www.sec.gov/Archives/edgar/data/{p[0]}/{acc}/index.json"
                        dir_res = safe_get(session, idx_url).json()
                        xml_item = next(i['name'] for i in dir_res['directory']['item'] if i['name'].endswith('.xml'))
                        xml_url = f"https://www.sec.gov/Archives/edgar/data/{p[0]}/{acc}/{xml_item}"
                        xml_res = safe_get(session, xml_url)

                    daily_trades.extend(parse_form4_xml(xml_res.content, acc))
                    time.sleep(0.11)
                except: continue
            
            load_and_merge(daily_trades, mode="DAILY")
        else:
            logger.warning(f"⚠️ Index not found for {date_str}")

    elif mode == "BACKFILL":
        target_years = years or [2024]
        target_quarters = quarters or [1, 2, 3, 4]
        logger.info(f"💾 BACKFILL: Years {target_years} | Quarters {target_quarters} | Limit {limit}")
        
        for year in target_years:
            for qtr in target_quarters:
                qtr_trades = []
                url = f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{qtr}/master.idx"
                res = safe_get(session, url)
                
                if res.status_code == 200:
                    filings = [line for line in res.text.splitlines() if '|4|' in line]
                    num_to_scrape = len(filings) if limit <= 0 else min(len(filings), limit)
                    
                    for line in tqdm(filings[:num_to_scrape], desc=f"📦 {year} Q{qtr}"):
                        try:
                            p = line.split('|')
                            acc = p[4].split('/')[-1].replace('.txt', '').replace('-', '')
                            idx_url = f"https://www.sec.gov/Archives/{p[4].replace('.txt', '-index.json')}"
                            
                            dir_res = safe_get(session, idx_url).json()
                            xml_item = next(i['name'] for i in dir_res['directory']['item'] if i['name'].endswith('.xml'))
                            xml_url = f"https://www.sec.gov/Archives/edgar/data/{p[0]}/{acc}/{xml_item}"
                            
                            xml_res = safe_get(session, xml_url)
                            qtr_trades.extend(parse_form4_xml(xml_res.content, acc))
                            time.sleep(0.11)
                        except: continue

                    if qtr_trades:
                        load_and_merge(qtr_trades, mode="BACKFILL")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["DAILY", "BACKFILL"])
    parser.add_argument("--years", nargs="+", type=int)
    parser.add_argument("--quarters", nargs="+", type=int)
    parser.add_argument("--limit", type=int)
    args = parser.parse_args()

    mode = args.mode or os.getenv("AGENT_MODE", "DAILY")
    raw_years = os.getenv("AGENT_YEARS")
    years = args.years or ([int(y) for y in raw_years.split(",")] if raw_years else [2024])
    raw_qtrs = os.getenv("AGENT_QUARTERS")
    quarters = args.quarters or ([int(q) for q in raw_qtrs.split(",")] if raw_qtrs else None)
    limit = args.limit if args.limit is not None else int(os.getenv("AGENT_LIMIT", 50))

    run_form4(mode=mode, years=years, quarters=quarters, limit=limit)