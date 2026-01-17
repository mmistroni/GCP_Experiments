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

def run_master_scraper(year: int, qtr: int, limit: int = 10000, debug: bool = False, batch_size: int = 15000):
    client = bigquery.Client()
    table_id = f"{client.project}.gcp_shareloader.all_holdings_master"
    partition_date = f"{year}-{(qtr*3):02d}-30"

    # 1. Skip BQ check if debugging
    if debug:
        logger.info("🛠️ DEBUG MODE ACTIVE: BigQuery uploads disabled.")
        existing_accs = set()
    else:
        existing_accs = get_existing_accessions(client, table_id, partition_date)
        logger.info(f"Found {len(existing_accs)} already processed filings for {year} Q{qtr}")

    # 2. Fetch Master Index
    idx_url = f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{qtr}/master.idx"
    logger.info(f"Fetching master index from {idx_url}...")
    r = requests.get(idx_url, headers=HEADERS)
    lines = [l for l in r.text.splitlines() if '13F-HR' in l][:limit]
    total_to_process = len(lines)
    logger.info(f"Filtered {total_to_process} 13F-HR filings to evaluate.")

    batch = []
    new_entries_count = 0
    processed_counter = 0

    for line in lines:
        processed_counter += 1
        if processed_counter % 100 == 0: # More frequent logging for small tests
            logger.info(f"📊 Progress: {processed_counter}/{total_to_process} filings checked.")

        parts = line.split('|')
        cik, name, path = parts[0], parts[1], parts[4]
        acc = path.split('/')[-1].replace('.txt', '').replace('-', '')

        if acc in existing_accs:
            continue
        
        new_entries_count += 1
        try:
            dir_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc}/index.json"
            dir_data = requests.get(dir_url, headers=HEADERS).json()
            items = dir_data.get('directory', {}).get('item', [])
            xml_name = next(i['name'] for i in items if 'infotable' in i['name'].lower() and i['name'].endswith('.xml'))
            
            xml_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc}/{xml_name}"
            xml_content = requests.get(xml_url, headers=HEADERS).content
            rows = parse_sec_xml(xml_content, cik, name, partition_date, acc)
            batch.extend(rows)

            # 4. CONDITIONAL UPLOAD
            if len(batch) >= batch_size:
                if debug:
                    logger.info(f"🔍 DEBUG: Would have uploaded {len(batch)} rows. Sample: {batch[0] if batch else 'None'}")
                else:
                    logger.info(f"💾 Flushing {len(batch)} rows to BigQuery...")
                    client.load_table_from_dataframe(pd.DataFrame(batch), table_id).result()
                batch = []
            
            time.sleep(0.12)
        except Exception as e:
            continue

    if batch:
        if debug:
            logger.info(f"🔍 DEBUG FINAL: Would have uploaded {len(batch)} rows. Total new: {new_entries_count}")
        else:
            logger.info(f"💾 Final flush: Sending last {len(batch)} rows to BigQuery...")
            client.load_table_from_dataframe(pd.DataFrame(batch), table_id).result()
    
    logger.info(f"✅ Scrape Complete. New filings processed: {new_entries_count}")

if __name__ == "__main__":
    # Test your 30 filings WITHOUT touching BigQuery
    run_master_scraper(year=2025, qtr=4, limit=30, debug=True)