import time
import logging
import requests
import pandas as pd
from lxml import etree
from google.cloud import bigquery
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("13f_scraper")

HEADERS = {'User-Agent': 'Institutional Research (researcher@data-science.org)'}

def get_existing_accessions(client, table_id, filing_date):
    """Checks BigQuery so we don't re-download what we already have."""
    query = f"SELECT DISTINCT accession_number FROM `{table_id}` WHERE filing_date = '{filing_date}'"
    try:
        query_job = client.query(query)
        results = query_job.result()
        return {row.accession_number for row in results}
    except Exception as e:
        logger.warning(f"Could not fetch existing accessions (might be a new table): {e}")
        return set()

def parse_sec_xml(xml_content, cik, manager_name, filing_date, acc):
    """Parses 13F-HR XML by stripping namespaces to avoid XPath errors."""
    try:
        tree = etree.fromstring(xml_content)
        
        # 1. THE FIX: Strip Namespaces
        for elem in tree.getiterator():
            if not (isinstance(elem, etree._Comment) or isinstance(elem, etree._ProcessingInstruction)):
                elem.tag = etree.QName(elem).localname
        etree.cleanup_namespaces(tree)

        # 2. Simple XPath: No more 'ns:' needed
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
                    val_float = float(val)
                    shares_float = float(shares)
                    holdings.append({
                        "accession_number": acc,
                        "cik": str(cik),
                        "manager_name": manager_name,
                        "issuer_name": find_text("nameOfIssuer"),
                        "cusip": find_text("cusip"),
                        "value_usd": int(val_float * 1000),
                        "shares": int(shares_float),
                        "put_call": find_text("putCall") or "LONG",
                        "filing_date": filing_date
                    })
                except (ValueError, TypeError):
                    continue
        return holdings
    except Exception as e:
        logger.error(f"Error parsing CIK {cik}: {str(e)}")
        return []

# ... (imports and parse_sec_xml remain the same)

import os


# ... (keep your existing imports and parse_sec_xml function)

def run_master_scraper(year: int, qtr: int, limit: int = 10000, debug: bool = False, batch_size: int = 500):
    client = bigquery.Client()
    # Ensure this dataset and table actually exist in your project
    table_id = f"{client.project}.gcp_shareloader.all_holdings_master"

    # FIX: Using a safer date format for BigQuery partitions
    partition_date = f"{year}-{min(qtr * 3, 12):02d}-01"

    if debug:
        logger.info("🛠️ DEBUG MODE: Uploads disabled.")
        existing_accs = set()
    else:
        existing_accs = get_existing_accessions(client, table_id, partition_date)
        logger.info(f"Found {len(existing_accs)} existing filings. Destination: {table_id}")

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

        if acc in existing_accs:
            continue

        try:
            # VOCAL LOGGING: See exactly what we are doing
            dir_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc}/index.json"
            dir_res = requests.get(dir_url, headers=HEADERS)
            dir_res.raise_for_status()  # Crash if SEC blocks us

            items = dir_res.json().get('directory', {}).get('item', [])
            xml_name = next(i['name'] for i in items if 'infotable' in i['name'].lower() and i['name'].endswith('.xml'))

            xml_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc}/{xml_name}"
            xml_content = requests.get(xml_url, headers=HEADERS).content

            rows = parse_sec_xml(xml_content, cik, name, partition_date, acc)

            if rows:
                batch.extend(rows)
                if processed_count % 10 == 0:
                    logger.info(f"✅ Found {len(rows)} holdings for {name} ({acc})")
            else:
                logger.warning(f"⚠️ No holdings found in XML for {name} ({acc})")

            # UPLOAD LOGIC
            logger.info(f'Batcch is {len(batch)}')
            if len(batch) >= batch_size:
                if not debug:
                    logger.info(f"💾 Flushing {len(batch)} rows to BQ...")
                    job = client.load_table_from_dataframe(pd.DataFrame(batch), table_id)
                    job.result()  # Wait for it to finish
                batch = []

            time.sleep(0.12)  # Respect SEC limits

        except Exception as e:
            logger.error(f"❌ Error on {acc}: {str(e)}")
            continue

    # FINAL FLUSH (The part that was missing/skipped)
    if batch:
        if debug:
            logger.info(f"🔍 DEBUG: Final batch would have sent {len(batch)} rows.")
        else:
            logger.info(f"💾 Final flush: Sending last {len(batch)} rows...")
            client.load_table_from_dataframe(pd.DataFrame(batch), table_id).result()

    logger.info(f"✅ JOB COMPLETE. Processed {processed_count} filings.")


if __name__ == "__main__":
    # DYNAMIC CONFIG: Listens to Cloud Run but defaults to safe values
    y = int(os.getenv("YEAR", 2025))
    q = int(os.getenv("QTR", 1))

    # IMPORTANT: Ensure debug is False when running in Cloud Run
    # You can set DEBUG=True in your local terminal to test safely
    is_debug = os.getenv("DEBUG", "false").lower() == "true"

    run_master_scraper(year=y, qtr=q, debug=is_debug)