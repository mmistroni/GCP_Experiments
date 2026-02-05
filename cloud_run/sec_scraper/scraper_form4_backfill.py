import os
import requests
import logging
import time
from lxml import etree
from google.cloud import bigquery
from scraper_form4 import parse_form4_xml  # Reuse your existing XML parsing logic

# Identity required by SEC
HEADERS = {'User-Agent': 'Institutional Research your-email@example.com'}
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT")
DATASET = "gcp_shareloader"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("form4_backfiller")

def download_quarterly_index(year, qtr):
    """Fetches the master index for a full quarter."""
    url = f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{qtr}/master.idx"
    logger.info(f"📥 Downloading Quarterly Index: {year} Q{qtr}")

    res = requests.get(url, headers=HEADERS)
    if res.status_code != 200:
        logger.error(f"❌ Could not find index for {year} Q{qtr}")
        return []

    # Skip the header lines (usually first 11 lines)
    lines = res.text.splitlines()
    rows = []
    for line in lines:
        if '|4|' in line:  # Only Form 4
            parts = line.split('|')
            # Index format: CIK|Company|Form|Date|Path
            rows.append({
                "cik": parts[0],
                "acc": parts[4].split('/')[-1].replace('.txt', '').replace('-', ''),
                "path": parts[4]
            })
    return rows


def manual_backfill(years=[2024, 2025]):
    client = bigquery.Client()

    for year in years:
        for qtr in range(1, 5):
            filings = download_quarterly_index(year, qtr)
            all_parsed_trades = []

            # SEC Rate Limit: 10 requests per second.
            # We process in small chunks to avoid being blocked.
            for i, f in enumerate(filings[:10]):  # Start with a small limit to test!
                try:
                    # Construct the directory URL to find the XML
                    base_url = f"https://www.sec.gov/Archives/{f['path'].replace('.txt', '-index.json')}"
                    dir_res = requests.get(base_url, headers=HEADERS).json()

                    # Find the specific XML file in the directory
                    xml_name = next(
                        item['name'] for item in dir_res['directory']['item'] if item['name'].endswith('.xml'))
                    xml_url = f"https://www.sec.gov/Archives/edgar/data/{f['cik']}/{f['acc']}/{xml_name}"

                    xml_res = requests.get(xml_url, headers=HEADERS)
                    # Reuse your parse_form4_xml function
                    all_parsed_trades.extend(parse_form4_xml(xml_res.content, f['acc']))

                    if i % 50 == 0:
                        logger.info(f"  Processed {i}/{len(filings)} filings...")
                    time.sleep(0.1)  # Respect the 10 req/sec limit
                except Exception:
                    continue

            # Bulk upload the quarter's data to BigQuery
            if all_parsed_trades:
                stg_table = f"{PROJECT_ID}.{DATASET}.stg_form4"
                client.load_table_from_json(all_parsed_trades, stg_table).result()

                # Merge into master (prevents duplicates if you run twice)
                master_table = f"{PROJECT_ID}.{DATASET}.form4_master"
                merge_sql = f"""
                MERGE `{master_table}` T USING `{stg_table}` S
                ON T.accession_number = S.accession_number AND T.shares = S.shares
                WHEN NOT MATCHED THEN INSERT ROW
                """
                client.query(merge_sql).result()
                logger.info(f"✅ Year {year} Q{qtr} merged into Master.")


if __name__ == "__main__":
    # WARNING: This could take hours for full years. Start with small limits.
    manual_backfill(years=[2024])