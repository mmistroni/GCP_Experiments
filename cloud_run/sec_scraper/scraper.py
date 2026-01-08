import time
import logging
import requests
import pandas as pd
from lxml import etree
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("13f_scraper")

HEADERS = {'User-Agent': 'Institutional Research (researcher@data-science.org)'}

def parse_sec_xml(xml_content, cik, manager_name, filing_date):
    """Robust parser for Python 3.11 using lxml."""
    try:
        tree = etree.fromstring(xml_content)
        namespaces = {'ns': tree.nsmap.get(None, '')}
        # Dual-path parsing for maximum row recovery
        nodes = tree.xpath('//ns:infoTable', namespaces=namespaces) or tree.xpath('//*[local-name()="infoTable"]')
        
        holdings = []
        for node in nodes:
            # Helper to find tags regardless of namespace shifts
            def find_text(tag):
                res = node.xpath(f'.//ns:{tag}', namespaces=namespaces) or node.xpath(f'.//*[local-name()="{tag}"]')
                return res[0].text if res else None

            val = find_text("value")
            shares = find_text("sshPrnamt")
            
            if val and shares:
                holdings.append({
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
        logger.error(f"Error parsing XML for CIK {cik}: {e}")
        return []

def run_master_scraper(year: int, qtr: int, limit: int = 10000):
    client = bigquery.Client()
    table_id = f"{client.project}.gcp_shareloader.all_holdings_master"
    partition_date = f"{year}-{(qtr*3):02d}-30"

    # 1. Fetch Index
    idx_url = f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{qtr}/master.idx"
    r = requests.get(idx_url, headers=HEADERS)
    lines = [l for l in r.text.splitlines() if '13F-HR' in l][:limit]

    logger.info(f"🚀 Starting scrape for {len(lines)} filings.")

    batch = []
    for line in lines:
        parts = line.split('|')
        cik, name, path = parts[0], parts[1], parts[4]
        acc = path.split('/')[-1].replace('.txt', '').replace('-', '')

        try:
            # Locate XML
            dir_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc}/index.json"
            dir_data = requests.get(dir_url, headers=HEADERS).json()
            items = dir_data.get('directory', {}).get('item', [])
            xml_name = next(i['name'] for i in items if 'infotable' in i['name'].lower() and i['name'].endswith('.xml'))
            
            # Parse and Accumulate
            xml_content = requests.get(f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc}/{xml_name}", headers=HEADERS).content
            batch.extend(parse_sec_xml(xml_content, cik, name, partition_date))

            # Batch Upload every 15,000 rows for memory safety
            if len(batch) >= 15000:
                client.load_table_from_dataframe(pd.DataFrame(batch), table_id).result()
                batch = []
            
            time.sleep(0.12) # Compliance delay
        except Exception:
            continue

    if batch:
        client.load_table_from_dataframe(pd.DataFrame(batch), table_id).result()