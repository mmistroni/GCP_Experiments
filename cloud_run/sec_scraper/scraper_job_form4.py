import os
import re
import time
import logging
import requests
from lxml import etree
from datetime import datetime
from google.cloud import bigquery

# --- CONFIGURATION ---
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "datascience-projects")
DATASET_ID = "gcp_shareloader"
TABLE_MASTER = f"{PROJECT_ID}.{DATASET_ID}.form4_master"
TABLE_QUEUE = f"{PROJECT_ID}.{DATASET_ID}.form4_queue"
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))

HEADERS = {"User-Agent": "Institutional Research (mmistroni@gmail.com)"}

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("form4_dynamic_worker")
client = bigquery.Client(project=PROJECT_ID)

def clean_numeric(value_str):
    if value_str is None or str(value_str).strip() == "":
        return 0.0
    raw_str = str(value_str).strip()
    # Split at footnotes/brackets to prevent "Footnote Merger" errors
    parts = re.split(r'[\[\(\*]', raw_str)
    prefix = parts[0]
    cleaned = re.sub(r'[^0-9.\-]', '', prefix)
    try:
        return float(cleaned) if cleaned else 0.0
    except (ValueError, TypeError):
        return 0.0

def get_text(node, path):
    res = node.xpath(path)
    if res and hasattr(res[0], 'text') and res[0].text:
        return res[0].text.strip()
    if res and isinstance(res[0], str):
        return res[0].strip()
    return None

def parse_form4_xml(xml_content, accession_number):
    trades = []
    try:
        root = etree.fromstring(xml_content, parser=etree.XMLParser(recover=True))
        ticker = get_text(root, "//*[local-name()='issuerTradingSymbol']")
        issuer = get_text(root, "//*[local-name()='issuerName']")
        owner = get_text(root, "//*[local-name()='reportingOwnerName']")

        paths = ["//*[local-name()='nonDerivativeTransaction']", "//*[local-name()='derivativeTransaction']"]
        
        for path in paths:
            for node in root.xpath(path):
                try:
                    s_val = get_text(node, ".//*[local-name()='transactionShares']/*[local-name()='value']")
                    p_val = get_text(node, ".//*[local-name()='transactionPricePerShare']/*[local-name()='value']")
                    code = get_text(node, ".//*[local-name()='transactionAcquiredDisposedCode']/*[local-name()='value']")
                    date = get_text(node, ".//*[local-name()='transactionDate']/*[local-name()='value']")

                    shares = clean_numeric(s_val)
                    price = clean_numeric(p_val)

                    if shares > 0 or price > 0:
                        trades.append({
                            "accession_number": accession_number,
                            "ticker": ticker if ticker and ticker not in ('NONE', 'UNKNOWN') else "UNKNOWN",
                            "issuer": issuer[:1024] if issuer else "Unknown",
                            "owner_name": owner[:1024] if owner else "Unknown",
                            "shares": shares,
                            "price": price,
                            "total_value": shares * price,
                            "transaction_side": "BUY" if code == 'A' else "SELL",
                            "filing_date": date,
                            "ingested_at": datetime.utcnow().isoformat()
                        })
                except Exception as e:
                    logger.debug(f"Row skip in {accession_number}: {e}")
                    continue
    except Exception as e:
        logger.error(f"Parser failed for {accession_number}: {e}")
    return trades

def bulk_update_queue(accession_list, status):
    """Updates statuses in bulk after a successful Master Load."""
    if not accession_list:
        return
    
    # Using a parameterized query to update multiple rows safely
    placeholders = ", ".join([f"'{acc}'" for acc in accession_list])
    query = f"""
        UPDATE `{TABLE_QUEUE}` 
        SET status = '{status}', updated_at = CURRENT_TIMESTAMP() 
        WHERE accession_number IN ({placeholders})
    """
    client.query(query).result()

def main():
    logger.info(f"🚀 Starting Unified Worker (Atomic Mode) for {DATASET_ID}...")
    
    query = f"SELECT cik, accession_number FROM `{TABLE_QUEUE}` WHERE status = 'pending' LIMIT {BATCH_SIZE}"
    rows = list(client.query(query).result())
    
    if not rows:
        logger.info("🏁 No pending filings found in queue.")
        return

    all_trades = []
    processed_accessions = []
    empty_accessions = []

    with requests.Session() as s:
        s.headers.update(HEADERS)
        for i, row in enumerate(rows, 1):
            cik, acc = row['cik'], row['accession_number']
            clean_acc = acc.replace('-', '')
            
            dir_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{clean_acc}/index.json"
            xml_url = None
            
            try:
                dir_res = s.get(dir_url, timeout=10)
                if dir_res.status_code == 200:
                    items = dir_res.json().get('directory', {}).get('item', [])
                    xml_name = next((it['name'] for it in items if it['name'].endswith('.xml')), None)
                    if xml_name:
                        xml_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{clean_acc}/{xml_name}"
                
                if not xml_url:
                    # Update these immediately as they require no Master upload
                    bulk_update_queue([acc], "no_xml_found")
                    continue

                res = s.get(xml_url, timeout=10)
                if res.status_code == 200:
                    trades = parse_form4_xml(res.content, acc)
                    if trades:
                        all_trades.extend(trades)
                        processed_accessions.append(acc) # Stage for 'done'
                        logger.info(f"[{i}/{len(rows)}] 📥 {acc}: Parsed {len(trades)} trades.")
                    else:
                        empty_accessions.append(acc) # Stage for 'no_trades_found'
                else:
                    logger.error(f"[{i}/{len(rows)}] ❌ {acc}: HTTP {res.status_code}")
            
            except Exception as e:
                logger.error(f"[{i}/{len(rows)}] 🔥 Critical error on {acc}: {e}")
            
            time.sleep(0.12) # SEC Rate Limit compliance

    # --- ATOMIC INGESTION BLOCK ---
    try:
        if all_trades:
            logger.info(f"📤 Uploading {len(all_trades)} trades to BigQuery...")
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
            load_job = client.load_table_from_json(all_trades, TABLE_MASTER, job_config=job_config)
            load_job.result() # Wait for upload to finish successfully
            
            # ONLY update queue to 'done' if the Master Load succeeded
            bulk_update_queue(processed_accessions, "done")
            logger.info(f"✨ Successfully ingested and updated {len(processed_accessions)} filings.")
        
        if empty_accessions:
            bulk_update_queue(empty_accessions, "no_trades_found")
            logger.info(f"📭 Marked {len(empty_accessions)} filings as empty.")

    except Exception as e:
        logger.error(f"🛑 MASTER LOAD FAILED: Data remains in memory. Queue NOT updated. Error: {e}")

if __name__ == "__main__":
    main()