import requests
import time
import pandas as pd
import xml.etree.ElementTree as ET
from google.cloud import bigquery
from tqdm import tqdm
from datetime import datetime

# Headers as per your original requirement
HEADERS = {'User-Agent': 'Institutional Research (researcher@data-science.org)'}

def get_bq_config():
    """Infers Project ID from the environment and sets dataset/table."""
    client = bigquery.Client()
    project_id = client.project
    dataset_id = "gcp_shareloader"
    table_id = "all_holdings_master"
    return client, f"{project_id}.{dataset_id}.{table_id}"

def parse_13f_xml(content, cik, manager_name, partition_date):
    """
    Robust parser preserving your {*} wildcard and specific data conversions.
    """
    try:
        root = ET.fromstring(content)
        holdings = []
        # Your specific wildcard logic to handle SEC namespace variations
        for info in root.findall(".//{*}infoTable"):
            holdings.append({
                "cik": str(cik),
                "manager_name": manager_name,
                "issuer_name": info.findtext(".//{*}nameOfIssuer"),
                "cusip": info.findtext(".//{*}cusip"),
                # Preserving your specific math: value * 1000 and int(float()) conversion
                "value_usd": int(float(info.findtext(".//{*}value", 0)) * 1000),
                "shares": int(float(info.findtext(".//{*}sshPrnamt", 0))),
                "put_call": info.findtext(".//{*}putCall"),
                "filing_date": partition_date  
            })
        return holdings
    except Exception:
        return []

def run_master_scraper(year: int, qtr: int, limit: int = 10000):
    """
    Refactored execution engine with idempotency and batching.
    """
    client, table_full_id = get_bq_config()
    
    # Standardizing dates (Your logic)
    quarter_dates = {1: f"{year}-03-31", 2: f"{year}-06-30", 3: f"{year}-09-30", 4: f"{year}-12-31"}
    partition_date = quarter_dates[qtr]

    # --- IDEMPOTENCY: Clean existing data for this quarter before starting ---
    print(f"🧹 Removing existing records for {partition_date}...")
    delete_query = f"DELETE FROM `{table_full_id}` WHERE filing_date = '{partition_date}'"
    client.query(delete_query).result()

    # Step 1: Get SEC Index
    idx_url = f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{qtr}/master.idx"
    r = requests.get(idx_url, headers=HEADERS)
    lines = [l for l in r.text.splitlines() if '13F-HR' in l][:limit]

    print(f"✅ Found {len(lines)} managers. Starting Master Table Import...")

    current_batch = []

    for line in tqdm(lines):
        parts = line.split('|')
        cik, name, path = parts[0], parts[1], parts[4]
        acc = path.split('/')[-1].replace('.txt', '').replace('-', '')

        try:
            # Step A: Locate XML (Your index.json logic)
            dir_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc}/index.json"
            response = requests.get(dir_url, headers=HEADERS).json()
            items = response.get('directory', {}).get('item', [])
            
            # Find the specific infoTable xml
            xml_name = next(i['name'] for i in items if 'infotable' in i['name'].lower() and i['name'].endswith('.xml'))

            # Step B: Parse Data
            xml_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc}/{xml_name}"
            xml_content = requests.get(xml_url, headers=HEADERS).content
            rows = parse_13f_xml(xml_content, cik, name, partition_date)

            current_batch.extend(rows)

            # Step C: Periodic Save (Your 10k threshold)
            if len(current_batch) > 10000:
                _upload_batch(client, table_full_id, current_batch)
                current_batch = [] # Reset memory for Cloud Run efficiency

            # Compliance delay (0.12s from your original script)
            time.sleep(0.12) 

        except Exception as e:
            # Silently continue as per your original robust structure
            continue

    # Final Upload for the remaining items
    if current_batch:
        _upload_batch(client, table_full_id, current_batch)
        print(f"🎉 Successfully updated {table_full_id} for {year} Q{qtr}!")

def _upload_batch(client, table_id, data):
    """Helper to handle BigQuery ingestion."""
    df = pd.DataFrame(data)
    df['filing_date'] = pd.to_datetime(df['filing_date'])
    
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND", # Appending after the initial DELETE ensures idempotency
    )
    
    load_job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    load_job.result() # Wait for completion