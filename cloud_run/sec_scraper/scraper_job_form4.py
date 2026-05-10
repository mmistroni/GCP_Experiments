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
# We bypass TABLE_QUEUE for the daily live run

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
                    "shares": shares, "price": price,
                    "total_value": float(shares * price),
                    "transaction_side": "BUY" if code == 'A' else "SELL",
                    "filing_date": t_date,
                    "ingested_at": datetime.utcnow().isoformat(),
                    "is_officer": False, "is_director": False, "officer_title": "N/A"
                })

        # --- 2. Derivative Transactions (Options/Warrants) ---
        # 🚀 FIX: Now uses 'underlyingSecurityShares' for proper volume
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
                    "shares": shares, "price": price,
                    "total_value": float(shares * price),
                    "transaction_side": "BUY" if code == 'A' else "SELL",
                    "filing_date": t_date,
                    "ingested_at": datetime.utcnow().isoformat(),
                    "is_officer": False, "is_director": False, "officer_title": "N/A"
                })
    except Exception as e:
        logger.error(f"Parser failed for {accession_number}: {e}")
    return trades

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
            
            all_trades = []
            
            for entry in entries:
                link = entry.xpath("*[local-name()='link']/@href")[0]
                # Extract CIK and Accession from link: /data/CIK/ACC/ACC-index.htm
                parts = link.split('/')
                cik, acc = parts[-3], parts[-2]
                clean_acc = acc.replace('-', '')
                
                # Fetch index.json to find the XML file name
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
                            all_trades.extend(trades)
                            logger.info(f"📥 {acc}: Parsed {len(trades)} trades.")
                
                time.sleep(0.12) # SEC Rate Limit

            if all_trades:
                # 🛡️ DEDUPLICATION MERGE: Ensures we don't double-count RSS items
                logger.info(f"📤 Merging {len(all_trades)} trades into BigQuery...")
                
                merge_sql = f"""
                MERGE `{TABLE_MASTER}` T
                USING (
                    SELECT * FROM UNNEST(@trades)
                ) S
                ON T.accession_number = S.accession_number 
                   AND T.owner_name = S.owner_name 
                   AND T.shares = S.shares 
                   AND T.filing_date = S.filing_date
                WHEN NOT MATCHED THEN
                    INSERT (ticker, issuer, owner_name, shares, price, transaction_side, filing_date, accession_number, ingested_at, is_officer, is_director, officer_title)
                    VALUES (ticker, issuer, owner_name, shares, price, transaction_side, filing_date, accession_number, ingested_at, is_officer, is_director, officer_title)
                """
                
                # Convert to BigQuery Structs for the MERGE
                structured_trades = [
                    bigquery.StructQueryParameter("unused", 
                        *[bigquery.ScalarQueryParameter(k, "FLOAT64" if isinstance(v, float) else "STRING" if k != "is_officer" and k != "is_director" else "BOOL", v) 
                          for k, v in t.items()]
                    ) for t in all_trades
                ]
                
                job_config = bigquery.QueryJobConfig(
                    query_parameters=[bigquery.ArrayQueryParameter("trades", "RECORD", structured_trades)]
                )
                client.query(merge_sql, job_config=job_config).result()
                logger.info("✨ Successfully synchronized live data.")
            else:
                logger.info("🏁 No new trades found in this window.")

        except Exception as e:
            logger.error(f"💥 Live worker failed: {e}")

if __name__ == "__main__":
    main()