import os
import requests
import logging
import time
import argparse
from datetime import datetime, date
from lxml import etree
from google.cloud import bigquery
from tqdm import tqdm
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- CONFIGURATION ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("form4_unified_agent")

HEADERS = {'User-Agent': 'Institutional Research mmistroni@gmail.com'}
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "datascience-projects")
DATASET = "gcp_shareloader"
MASTER_TABLE = "form4_master"
STAGING_TABLE = "stg_form4"

def get_session():
    session = requests.Session()
    retry_strategy = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
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
            t_date = tx.xpath("string(.//*[local-name()='transactionDate']/*[local-name()='value'])")
            t_code = tx.xpath("string(.//*[local-name()='transactionCode'])")

            if ticker and shares and float(shares) > 0:
                trades.append({
                    "ticker": ticker,
                    "issuer": issuer,
                    "owner_name": owner,
                    "transaction_code": t_code,
                    "shares": float(shares),
                    "price": float(price) if price else 0.0,
                    # Mapping to your BQ 'transaction_side'
                    "transaction_side": "BUY" if code == 'A' else "SELL",
                    "filing_date": t_date,
                    "accession_number": acc,
                    "ingested_at": datetime.utcnow().isoformat()
                })
        return trades
    except Exception as e:
        logger.error(f"Parser error for {acc}: {e}")
        return []

def load_and_merge(trades_list, mode="BACKFILL"):
    if not trades_list:
        logger.info(f"📭 [{mode}] No trades to load.")
        return

    client = bigquery.Client(project=PROJECT_ID)
    master_ref = f"{PROJECT_ID}.{DATASET}.{MASTER_TABLE}"
    staging_table_id = f"{PROJECT_ID}.{DATASET}.{STAGING_TABLE}_tmp_{int(time.time())}"
    
    # Matches your BQ Schema exactly
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
        job_config = bigquery.LoadJobConfig(
            schema=schema, 
            write_disposition="WRITE_TRUNCATE", 
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        )
        
        client.load_table_from_json(trades_list, staging_table_id, job_config=job_config).result()

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
        client.query(merge_query).result()
        logger.info(f"✅ [{mode}] Merged {len(trades_list)} records.")
    finally:
        client.delete_table(staging_table_id, not_found_ok=True)

def process_index_line(session, line):
    """Common logic to go from an index line to parsed trades."""
    try:
        p = line.split('|')
        cik = p[0]
        acc_raw = p[4].split('/')[-1].replace('.txt', '')
        acc = acc_raw.replace('-', '')
        
        # Get the directory listing to find the .xml file
        dir_url = f"https://www.sec.gov/Archives/{p[4].replace('.txt', '-index.json')}"
        dir_res = session.get(dir_url, timeout=5).json()
        xml_name = next(i['name'] for i in dir_res['directory']['item'] if i['name'].endswith('.xml'))
        
        xml_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc}/{xml_name}"
        xml_res = session.get(xml_url, timeout=5)
        
        return parse_form4_xml(xml_res.content, acc_raw)
    except Exception:
        return []

def run_form4(mode: str, years: list, quarters: list, limit: int):
    session = get_session()
    all_trades = []

    if mode.upper() == "DAILY":
        # DAILY MODE: Hits the current SEC Daily Index
        today = date.today()
        qtr = (today.month - 1) // 3 + 1
        date_str = today.strftime("%Y%m%d")
        url = f"https://www.sec.gov/Archives/edgar/daily-index/{today.year}/QTR{qtr}/form.{date_str}.idx"
        
        logger.info(f"📅 DAILY: Fetching {url}")
        res = session.get(url)
        if res.status_code == 200:
            filings = [l for l in res.text.splitlines() if '|4|' in l or '|4/A|' in l]
            for line in tqdm(filings, desc="Processing Daily"):
                all_trades.extend(process_index_line(session, line))
                time.sleep(0.11)
            load_and_merge(all_trades, mode="DAILY")
        else:
            logger.warning(f"No daily index found for {date_str}. (Weekend/Holiday?)")

    else:
        # BACKFILL MODE: Hits the Quarterly Master Indexes
        for year in years:
            for qtr in quarters:
                logger.info(f"💾 BACKFILL: {year} Q{qtr}")
                qtr_trades = []
                url = f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{qtr}/master.idx"
                res = session.get(url)
                if res.status_code == 200:
                    filings = [l for l in res.text.splitlines() if '|4|' in l]
                    num = len(filings) if limit <= 0 else min(len(filings), limit)
                    for line in tqdm(filings[:num], desc=f"📦 {year}Q{qtr}"):
                        qtr_trades.extend(process_index_line(session, line))
                        time.sleep(0.11)
                    load_and_merge(qtr_trades, mode="BACKFILL")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["DAILY", "BACKFILL"])
    args = parser.parse_args()

    mode = args.mode or os.getenv("AGENT_MODE", "DAILY")
    # Parse years/quarters from env or use defaults
    years = [int(y) for y in os.getenv("AGENT_YEARS", "2026").split(",")]
    quarters = [int(q) for q in os.getenv("AGENT_QUARTERS", "1").split(",")]
    limit = int(os.getenv("AGENT_LIMIT", "50"))

    run_form4(mode, years, quarters, limit)