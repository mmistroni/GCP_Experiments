import os
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

def get_text(node, path):
    """Safely extracts text from an XPath path."""
    res = node.xpath(path)
    if res and hasattr(res[0], 'text') and res[0].text:
        return res[0].text.strip()
    if res and isinstance(res[0], str):
        return res[0].strip()
    return None

def parse_form4_xml(xml_content, accession_number):
    """Parses Table I and Table II using local-name to ignore namespaces."""
    trades = []
    try:
        root = etree.fromstring(xml_content, parser=etree.XMLParser(recover=True))
        ticker = get_text(root, "//*[local-name()='issuerTradingSymbol']")
        issuer = get_text(root, "//*[local-name()='issuerName']")
        owner = get_text(root, "//*[local-name()='reportingOwnerName']")

        # Combined logic for Table I and Table II
        paths = ["//*[local-name()='nonDerivativeTransaction']", "//*[local-name()='derivativeTransaction']"]
        for path in paths:
            for node in root.xpath(path):
                try:
                    s_val = get_text(node, ".//*[local-name()='transactionShares']/*[local-name()='value']")
                    p_val = get_text(node, ".//*[local-name()='transactionPricePerShare']/*[local-name()='value']")
                    code = get_text(node, ".//*[local-name()='transactionAcquiredDisposedCode']/*[local-name()='value']")
                    date = get_text(node, ".//*[local-name()='transactionDate']/*[local-name()='value']")

                    trades.append({
                        "accession_number": accession_number,
                        "ticker": ticker,
                        "issuer": issuer[:1024] if issuer else "Unknown",
                        "owner_name": owner[:1024] if owner else "Unknown",
                        "shares": float(s_val) if s_val else 0.0,
                        "price": float(p_val) if p_val else 0.0,
                        "transaction_side": "BUY" if code == 'A' else "SELL",
                        "filing_date": date,
                        "ingested_at": datetime.utcnow().isoformat()
                    })
                except Exception: continue
    except Exception as e:
        logger.error(f"Parser failed for {accession_number}: {e}")
    return trades

def update_queue(acc, status):
    """Updates BQ status to prevent re-processing."""
    query = f"UPDATE `{TABLE_QUEUE}` SET status = @status, updated_at = CURRENT_TIMESTAMP() WHERE accession_number = @acc"
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("status", "STRING", status),
            bigquery.ScalarQueryParameter("acc", "STRING", acc),
        ]
    )
    client.query(query, job_config=job_config).result()

def main():
    logger.info(f"🚀 Starting Dynamic URL Batch for {DATASET_ID}...")
    query = f"SELECT cik, accession_number FROM `{TABLE_QUEUE}` WHERE status = 'pending' LIMIT {BATCH_SIZE}"
    rows = list(client.query(query).result())
    
    if not rows:
        logger.info("🏁 No work found.")
        return

    all_trades = []
    with requests.Session() as s:
        s.headers.update(HEADERS)
        for i, row in enumerate(rows, 1):
            cik, acc = row['cik'], row['accession_number']
            clean_acc = acc.replace('-', '')
            
            # --- THE DYNAMIC URL FIX ---
            # 1. Ask the folder: "What files do you have?"
            dir_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{clean_acc}/index.json"
            xml_url = None
            
            try:
                dir_res = s.get(dir_url, timeout=10)
                if dir_res.status_code == 200:
                    items = dir_res.json().get('directory', {}).get('item', [])
                    # Find the real XML filename
                    xml_name = next((it['name'] for it in items if it['name'].endswith('.xml')), None)
                    if xml_name:
                        xml_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{clean_acc}/{xml_name}"
                
                if not xml_url:
                    logger.warning(f"[{i}/{len(rows)}] ❌ {acc}: No XML found in directory.")
                    update_queue(acc, "no_xml_found")
                    continue

                # 2. Fetch the actual discovered XML
                res = s.get(xml_url, timeout=10)
                if res.status_code == 200:
                    trades = parse_form4_xml(res.content, acc)
                    if trades:
                        all_trades.extend(trades)
                        update_queue(acc, "done")
                        logger.info(f"[{i}/{len(rows)}] ✅ {acc}: Found {len(trades)} trades.")
                    else:
                        update_queue(acc, "no_trades_found")
                        logger.warning(f"[{i}/{len(rows)}] ⚠️ {acc}: Empty XML.")
                else:
                    logger.error(f"[{i}/{len(rows)}] ❌ {acc}: HTTP {res.status_code}")
            
            except Exception as e:
                logger.error(f"[{i}/{len(rows)}] 🔥 {acc}: {e}")
            
            time.sleep(0.12) # SEC Compliance

    # Final Batch Ingest
    if all_trades:
        client.load_table_from_json(all_trades, TABLE_MASTER).result()
        logger.info(f"✨ Ingested {len(all_trades)} trades.")

if __name__ == "__main__":
    main()