import os
import re
import time
import logging
import requests
from lxml import etree
from datetime import datetime
from google.cloud import bigquery

# --- CONFIGURATION ---
PROJECT_ID = "datascience-projects"
DATASET_ID = "gcp_shareloader"
TABLE_MASTER = f"{PROJECT_ID}.{DATASET_ID}.form4_master"

HEADERS = {"User-Agent": "Institutional Research (mmistroni@gmail.com)"}

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("form4_live_worker")
client = bigquery.Client(project=PROJECT_ID)

def clean_numeric(value_str):
    if value_str is None or str(value_str).strip() == "":
        return 0.0
    raw_str = str(value_str).strip()
    # Fix: Split at footnotes/brackets to prevent "1000(4)" becoming 10004
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

        # --- 1. Non-Derivative Transactions (Common Stock) ---
        for node in root.xpath("//*[local-name()='nonDerivativeTransaction']"):
            s_val = get_text(node, ".//*[local-name()='transactionShares']/*[local-name()='value']")
            p_val = get_text(node, ".//*[local-name()='transactionPricePerShare']/*[local-name()='value']")
            code = get_text(node, ".//*[local-name()='transactionAcquiredDisposedCode']/*[local-name()='value']")
            t_date = get_text(node, ".//*[local-name()='transactionDate']/*[local-name()='value']")

            shares, price = clean_numeric(s_val), clean_numeric(p_val)
            if shares > 0:
                trades.append({
                    "accession_number": accession_number,
                    "ticker": ticker[:10] if ticker else "UNKNOWN",
                    "issuer": issuer[:255] if issuer else "Unknown",
                    "owner_name": owner[:255] if owner else "Unknown",
                    "shares": float(shares), 
                    "price": float(price),
                    "total_value": float(shares * price),
                    "transaction_side": "BUY" if code == 'A' else "SELL",
                    "filing_date": t_date,  # BigQuery JSON loader infers YYYY-MM-DD cleanly as DATE
                    "ingested_at": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
                    "is_officer": False, 
                    "is_director": False, 
                    "officer_title": "N/A"
                })

        # --- 2. Derivative Transactions (Options/Warrants) ---
        for node in root.xpath("//*[local-name()='derivativeTransaction']"):
            s_val = get_text(node, ".//*[local-name()='underlyingSecurityShares']/*[local-name()='value']")
            p_val = get_text(node, ".//*[local-name()='transactionPricePerShare']/*[local-name()='value']")
            code = get_text(node, ".//*[local-name()='transactionAcquiredDisposedCode']/*[local-name()='value']")
            t_date = get_text(node, ".//*[local-name()='transactionDate']/*[local-name()='value']")

            shares, price = clean_numeric(s_val), clean_numeric(p_val)
            if shares > 0:
                trades.append({
                    "accession_number": accession_number,
                    "ticker": ticker[:10] if ticker else "UNKNOWN",
                    "issuer": issuer[:255] if issuer else "Unknown",
                    "owner_name": owner[:255] if owner else "Unknown",
                    "shares": float(shares), 
                    "price": float(price),
                    "total_value": float(shares * price),
                    "transaction_side": "BUY" if code == 'A' else "SELL",
                    "filing_date": t_date,
                    "ingested_at": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
                    "is_officer": False, 
                    "is_director": False, 
                    "officer_title": "N/A"
                })
    except Exception as e:
        logger.error(f"Parser failed for {accession_number}: {e}")
    return trades

def get_recently_ingested_keys():
    """
    Looks back over the past 2 days of master table history.
    This creates a highly optimized cache filter for fractions of a penny.
    """
    query = f"""
        SELECT CONCAT(accession_number, '||', owner_name, '||', CAST(shares AS STRING), '||', CAST(filing_date AS STRING)) as record_key
        FROM `{TABLE_MASTER}`
        WHERE ingested_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY)
    """
    try:
        query_job = client.query(query)
        results = query_job.result()
        return {row.record_key for row in results}
    except Exception as e:
        logger.warning(f"⚠️ Could not pull recent keys cache (might be empty or initial run): {e}")
        return set()

def main():
    logger.info("📡 Fetching latest Form 4s from SEC Live RSS feed...")
    RSS_URL = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&count=100&output=atom"
    
    with requests.Session() as s:
        s.headers.update(HEADERS)
        try:
            res = s.get(RSS_URL, timeout=15)
            if res.status_code != 200:
                logger.error(f"SEC RSS Feed Down: {res.status_code}")
                return
            
            root = etree.fromstring(res.content)
            entries = root.xpath("//*[local-name()='entry']")
            logger.info(f"🔎 Found {len(entries)} recent filings in RSS.")
            
            # Pull a fast lookback lookup map to isolate and strip out duplicates locally
            recent_record_keys = get_recently_ingested_keys()
            all_trades = []
            
            for entry in entries:
                link = entry.xpath("*[local-name()='link']/@href")[0]
                parts = link.split('/')
                cik, acc = parts[-3], parts[-2]
                clean_acc = acc.replace('-', '')
                
                dir_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{clean_acc}/index.json"
                dir_res = s.get(dir_url, timeout=10)
                
                if dir_res.status_code == 200:
                    items = dir_res.json().get('directory', {}).get('item', [])
                    xml_name = next((it['name'] for it in items if it['name'].endswith('.xml') and 'target' not in it['name']), None)
                    
                    if xml_name:
                        xml_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{clean_acc}/{xml_name}"
                        xml_res = s.get(xml_url, timeout=10)
                        if xml_res.status_code == 200:
                            trades = parse_form4_xml(xml_res.content, acc)
                            
                            # Filter out duplicate rows locally using our localized search lookup cache keys
                            for t in trades:
                                unique_key = f"{t['accession_number']}||{t['owner_name']}||{str(t['shares'])}||{str(t['filing_date'])}"
                                if unique_key not in recent_record_keys:
                                    all_trades.append(t)
                                    # Add to memory frame to protect against duplicate records within the exact same batch 
                                    recent_record_keys.add(unique_key)
                                    
                            logger.info(f"📥 {acc}: Processed parsed trades.")
                
                time.sleep(0.12) # Respecting the SEC Rate Limit

            # --- TRANSMIT DATA INTO BIGQUERY ---
            if all_trades:
                logger.info(f"📤 Appending {len(all_trades)} unique trades directly into BigQuery...")
                
                job_config = bigquery.LoadJobConfig(
                    schema=[
                        bigquery.SchemaField("ticker", "STRING"),
                        bigquery.SchemaField("issuer", "STRING"),
                        bigquery.SchemaField("owner_name", "STRING"),
                        bigquery.SchemaField("shares", "FLOAT64"),
                        bigquery.SchemaField("price", "FLOAT64"),
                        bigquery.SchemaField("total_value", "FLOAT64"),
                        bigquery.SchemaField("transaction_side", "STRING"),
                        bigquery.SchemaField("filing_date", "DATE"),
                        bigquery.SchemaField("accession_number", "STRING"),
                        bigquery.SchemaField("ingested_at", "TIMESTAMP"),
                        bigquery.SchemaField("is_officer", "BOOL"),
                        bigquery.SchemaField("is_director", "BOOL"),
                        bigquery.SchemaField("officer_title", "STRING"),
                    ],
                    write_disposition="WRITE_APPEND",
                    autodetect=False 
                )
                
                load_job = client.load_table_from_json(all_trades, TABLE_MASTER, job_config=job_config)
                load_job.result()
                logger.info("✨ Successfully appended live data to master history table.")
            else:
                logger.info("🏁 No new unique trades discovered in this window feed.")

        except Exception as e:
            logger.error(f"💥 Live worker failed: {e}")

if __name__ == "__main__":
    main()