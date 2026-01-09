import time
import logging
import requests
import pandas as pd
from lxml import etree
from google.cloud import bigquery
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("13f_scraper")

HEADERS = {'User-Agent': 'Institutional Research (researcher@data-science.org)'}

def get_existing_accessions(client, table_id, filing_date):
    """Checks BigQuery so we don't re-download what we already have."""
    query = f"SELECT DISTINCT accession_number FROM `{table_id}` WHERE filing_date = '{filing_date}'"
    try:
        query_job = client.query(query)
        results = query_job.result()
        return {row.accession_number for row in results}
    except Exception:
        return set()

def parse_sec_xml(xml_content, cik, manager_name, filing_date, acc):
    """Parses 13F-HR XML into list of dicts with Python 3.11 speed."""
    try:
        tree = etree.fromstring(xml_content)
        namespaces = {'ns': tree.nsmap.get(None, '')}
        nodes = tree.xpath('//ns:infoTable', namespaces=namespaces) or tree.xpath('//*[local-name()="infoTable"]')
        
        holdings = []
        for node in nodes:
            def find_text(tag):
                res = node.xpath(f'.//ns:{tag}', namespaces=namespaces) or node.xpath(f'.//*[local-name()="{tag}"]')
                return res[0].text if res else None

            val = find_text("value")
            shares = find_text("sshPrnamt")
            
            if val and shares:
                holdings.append({
                    "accession_number": acc, # Added for deduplication logic
                    "cik": str(cik),
                    "manager_name": manager_name,
                    "issuer_name": find_text("nameOfIssuer"),
                    "cusip": find_text("cusip"),
                    "value_usd": int(float(val) * 1000),
                    "shares": int(float(shares)),
                    "put_call": find_text("putCall"),
                    "filing_date": filing_date
                })
        return holdings
    except Exception as e:
        logger.error(f"Error parsing CIK {cik}: {e}")
        return []

def run_master_scraper(year: int, qtr: int, limit: int = 10000):
    client = bigquery.Client()
    table_id = f"{client.project}.gcp_shareloader.all_holdings_master"
    partition_date = f"{year}-{(qtr*3):02d}-30"

    # 1. Get processed list to avoid redundant work
    existing_accs = get_existing_accessions(client, table_id, partition_date)
    logger.info(f"Found {len(existing_accs)} already processed filings for {year} Q{qtr}")

    # 2. Fetch Master Index
    idx_url = f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{qtr}/master.idx"
    r = requests.get(idx_url, headers=HEADERS)
    lines = [l for l in r.text.splitlines() if '13F-HR' in l][:limit]

    batch = []
    new_entries_count = 0

    for line in lines:
        parts = line.split('|')
        cik, name, path = parts[0], parts[1], parts[4]
        acc = path.split('/')[-1].replace('.txt', '').replace('-', '')

        # 3. INCREMENTAL CHECK
        if acc in existing_accs:
            continue
        
        new_entries_count += 1
        try:
            # Locate XML via directory
            dir_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc}/index.json"
            dir_data = requests.get(dir_url, headers=HEADERS).json()
            items = dir_data.get('directory', {}).get('item', [])
            xml_name = next(i['name'] for i in items if 'infotable' in i['name'].lower() and i['name'].endswith('.xml'))
            
            # Fetch and Parse
            xml_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc}/{xml_name}"
            xml_content = requests.get(xml_url, headers=HEADERS).content
            rows = parse_sec_xml(xml_content, cik, name, partition_date, acc)
            batch.extend(rows)

            # 4. MEMORY-SAFE UPLOAD (Every 15k rows)
            if len(batch) >= 15000:
                client.load_table_from_dataframe(pd.DataFrame(batch), table_id).result()
                batch = []
            
            time.sleep(0.12) # SEC Compliance
        except Exception:
            continue

    if batch:
        client.load_table_from_dataframe(pd.DataFrame(batch), table_id).result()
    
    logger.info(f"Scrape Complete. Processed {new_entries_count} new filings.")