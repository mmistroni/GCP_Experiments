import time
import logging
import requests
import pandas as pd
from lxml import etree
from google.cloud import bigquery
from datetime import datetime
import traceback
import os

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("13f_scraper")

# SEC-Friendly Headers
HEADERS = {
    'User-Agent': 'Institutional Research (mmapplausetest@gmail.com)', # Use a real-looking email
    'Accept-Encoding': 'gzip, deflate',
    'Host': 'www.sec.gov',
    'Connection': 'keep-alive'
}

def get_with_retry(url, headers, max_retries=3, sleep_time=5):
    """Helper to handle SEC throttling by retrying on empty/short responses."""
    for attempt in range(max_retries):
        try:
            res = requests.get(url, headers=headers, timeout=10)
            # If SEC throttles, they often return a 200 with a tiny HTML body or empty JSON
            if res.status_code == 200 and len(res.content) > 150:
                return res
            
            logger.warning(f"⚠️ Throttled or empty response from {url} (Attempt {attempt+1}/{max_retries})")
            time.sleep(sleep_time * (attempt + 1)) # Exponential-ish backoff
        except Exception as e:
            logger.error(f"📡 Network error on {url}: {e}")
            time.sleep(sleep_time)
    return None

def parse_sec_xml(xml_content, cik, manager_name, filing_date, acc):
    """Parses 13F-HR XML by stripping namespaces."""
    try:
        tree = etree.fromstring(xml_content)
        for elem in tree.getiterator():
            if not (isinstance(elem, etree._Comment) or isinstance(elem, etree._ProcessingInstruction)):
                elem.tag = etree.QName(elem).localname
        etree.cleanup_namespaces(tree)

        nodes = tree.xpath('//infoTable')
        holdings = []
        for node in nodes:
            def find_text(tag):
                res = node.xpath(f'./{tag}')
                return res[0].text if res else None

            val = find_text("value")
            shares = find_text("sshPrnamt")
            if val and shares:
                try:
                    holdings.append({
                        "accession_number": acc,
                        "cik": str(cik),
                        "manager_name": manager_name,
                        "issuer_name": find_text("nameOfIssuer"),
                        "cusip": find_text("cusip"),
                        "value_usd": int(float(val) * 1000),
                        "shares": int(float(shares)),
                        "put_call": find_text("putCall") or "LONG",
                        "filing_date": filing_date
                    })
                except: continue
        return holdings
    except Exception as e:
        logger.error(f"Error parsing XML for {acc}: {e}")
        return []

def run_master_scraper(year: int, qtr: int, limit: int = 10000, debug: bool = False, batch_size: int = 500):
    client = bigquery.Client()
    table_id = f"{client.project}.gcp_shareloader.all_holdings_master"
    partition_date = f"{year}-{min(qtr * 3, 12):02d}-01"

    # 1. Fetch Master Index
    idx_url = f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{qtr}/master.idx"
    r = requests.get(idx_url, headers=HEADERS)
    lines = [l for l in r.text.splitlines() if '13F-HR' in l][:limit]

    batch = []
    processed_count = 0

    for line in lines:
        processed_count += 1
        parts = line.split('|')
        cik, name, path = parts[0], parts[1], parts[4]
        acc = path.split('/')[-1].replace('.txt', '').replace('-', '')

        try:
            # 2. Get Directory JSON with Retry
            dir_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc}/index.json"
            dir_res = get_with_retry(dir_url, HEADERS)
            
            if not dir_res:
                logger.error(f"❌ Skipping {acc}: SEC repeatedly blocked directory access.")
                continue

            items = dir_res.json().get('directory', {}).get('item', [])
            
            # 3. SAFE NEXT: Avoid StopIteration crash
            xml_name = next((i['name'] for i in items if 'infotable' in i['name'].lower() and i['name'].endswith('.xml')), None)

            if not xml_name:
                logger.warning(f"❓ No XML filename found in directory for {acc}. Items found: {len(items)}")
                continue

            # 4. Get XML Content with Retry
            xml_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc}/{xml_name}"
            xml_res = get_with_retry(xml_url, HEADERS)
            
            if not xml_res:
                logger.error(f"❌ Skipping {acc}: Could not fetch XML content.")
                continue
            
            rows = parse_sec_xml(xml_res.content, cik, name, partition_date, acc)

            if rows:
                batch.extend(rows)
                if processed_count % 10 == 0:
                    logger.info(f"✅ Found {len(rows)} holdings for {name} ({acc})")
            
            # 5. Batch Upload to BigQuery
            if len(batch) >= batch_size and not debug:
                logger.info(f"💾 Flushing {len(batch)} rows to BQ...")
                client.load_table_from_dataframe(pd.DataFrame(batch), table_id).result()
                batch = []

            time.sleep(0.3) # Slow down slightly for Cloud Run stability

        except Exception as e:
            logger.error(f"❌ Unexpected Error on {acc}: {str(e)}")
            continue

    # Final Flush
    if batch and not debug:
        client.load_table_from_dataframe(pd.DataFrame(batch), table_id).result()
    
    logger.info(f"✅ JOB COMPLETE. Processed {processed_count} filings.")

if __name__ == "__main__":
    y = int(os.getenv("YEAR", 2025))
    q = int(os.getenv("QTR", 1))
    run_master_scraper(year=y, qtr=q)