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
    'User-Agent': 'Institutional Research mmapplausetest@gmail.com', 
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Connection': 'keep-alive',
    'Host': 'www.sec.gov'
}

def get_with_retry(session, url, max_retries=3, sleep_time=5):
    """Helper to handle SEC throttling using the persistent session."""
    for attempt in range(max_retries):
        try:
            # Explicit timeout to prevent Cloud Run 'hanging'
            res = session.get(url, timeout=20)
            if res.status_code == 200 and len(res.content) > 150:
                return res
            
            logger.warning(f"⚠️ Throttled or empty response from {url} (Attempt {attempt+1}/{max_retries})")
            time.sleep(sleep_time * (attempt + 1)) 
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

    # 1. Fetch Master Index WITH PERSISTENT SESSION & TIMEOUT
    session = requests.Session()
    session.headers.update(HEADERS)
    
    idx_url = f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{qtr}/master.idx"
    logger.info(f"🌐 Attempting to fetch index: {idx_url}")
    
    try:
        # Timeout is critical for Cloud Run to avoid infinite 'Pending' logs
        r = session.get(idx_url, timeout=30)
        r.raise_for_status()
        logger.info(f"📥 Index fetched. Size: {len(r.text)} characters.")
    except Exception as e:
        logger.error(f"💥 Failed to fetch Master Index: {e}")
        return

    lines = [l for l in r.text.splitlines() if '13F-HR' in l][:limit]
    logger.info(f"📊 Filtered {len(lines)} 13F-HR filings to process.")

    batch = []
    processed_count = 0

    for line in lines:
        processed_count += 1
        parts = line.split('|')
        cik, name, path = parts[0], parts[1], parts[4]
        acc = path.split('/')[-1].replace('.txt', '').replace('-', '')

        try:
            # 2. Get Directory JSON
            dir_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc}/index.json"
            dir_res = get_with_retry(session, dir_url)
            
            if not dir_res:
                logger.error(f"❌ Skipping {acc}: Could not reach directory.")
                continue

            items = dir_res.json().get('directory', {}).get('item', [])
            
            # 3. SMART XML SEARCH
            xml_name = next((i['name'] for i in items if 
                            i['name'].lower().endswith('.xml') and 
                            any(p in i['name'].lower() for p in ['infotable', 'informationtable', 'holdings'])), None)

            if not xml_name:
                xml_files = [i for i in items if i['name'].lower().endswith('.xml') and 'primary_doc' not in i['name'].lower()]
                if xml_files:
                    xml_name = max(xml_files, key=lambda x: int(x.get('size', 0)))['name']

            if not xml_name:
                logger.warning(f"❓ No holdings XML found for {acc}. Files present: {[i['name'] for i in items]}")
                continue

            # 4. Get XML Content
            xml_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc}/{xml_name}"
            xml_res = get_with_retry(session, xml_url)
            
            if not xml_res:
                continue
            
            rows = parse_sec_xml(xml_res.content, cik, name, partition_date, acc)

            if rows:
                batch.extend(rows)
                if processed_count % 10 == 0:
                    logger.info(f"✅ [{processed_count}] Processed {name} - Batch: {len(batch)}")
            
            # 5. Batch Upload
            if len(batch) >= batch_size and not debug:
                logger.info(f"💾 Flushing {len(batch)} rows to BQ...")
                client.load_table_from_dataframe(pd.DataFrame(batch), table_id).result()
                batch = []

            # Respect the SEC 10 requests per second rule
            time.sleep(0.15) 

        except Exception as e:
            logger.error(f"❌ Unexpected Error on {acc}: {str(e)}")
            continue

    if batch and not debug:
        logger.info(f"💾 Final flush: {len(batch)} rows.")
        client.load_table_from_dataframe(pd.DataFrame(batch), table_id).result()
    
    logger.info(f"✅ JOB COMPLETE. Total filings processed: {processed_count}")

if __name__ == "__main__":
    y = int(os.getenv("YEAR", 2025))
    q = int(os.getenv("QTR", 1))
    run_master_scraper(year=y, qtr=q)