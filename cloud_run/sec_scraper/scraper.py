import os
import sys
import time
import logging
import requests
from lxml import etree
from google.cloud import bigquery

# 1. LOGGING & HEADERS
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    stream=sys.stdout 
)
logger = logging.getLogger("13f_feature_agent")

HEADERS = {
    'User-Agent': 'Institutional Research mmapplausetest@gmail.com', 
    'Accept-Encoding': 'gzip, deflate, br',
    'Host': 'www.sec.gov'
}


def ensure_tables_exist(client):
    """Checks for Dataset and Tables; creates them if missing."""
    dataset_id = f"{client.project}.gcp_shareloader"
    
    # 1. Ensure Dataset exists
    client.create_dataset(dataset_id, exists_ok=True)
    
    # 2. Define Queue Table Schema
    queue_table_id = f"{dataset_id}.scraping_queue"
    queue_schema = [
        bigquery.SchemaField("cik", "STRING"),
        bigquery.SchemaField("company_name", "STRING"),
        bigquery.SchemaField("accession_number", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("dir_url", "STRING"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("year", "INTEGER"),
        bigquery.SchemaField("qtr", "INTEGER"),
    ]
    
    # 3. Define Master Holdings Table Schema
    master_table_id = f"{dataset_id}.all_holdings_master"
    master_schema = [
        bigquery.SchemaField("cik", "STRING"),
        bigquery.SchemaField("company_name", "STRING"),
        bigquery.SchemaField("issuer", "STRING"),
        bigquery.SchemaField("cusip", "STRING"),
        bigquery.SchemaField("shares", "FLOAT"),
        bigquery.SchemaField("year", "INTEGER"),
        bigquery.SchemaField("qtr", "INTEGER"),
        bigquery.SchemaField("accession_number", "STRING"),
    ]

    # Create tables if they don't exist
    for table_id, schema in [(queue_table_id, queue_schema), (master_table_id, master_schema)]:
        table = bigquery.Table(table_id, schema=schema)
        # exists_ok=True prevents errors if it's already there
        client.create_table(table, exists_ok=True) 
        logger.info(f"✅ Verified table: {table_id}")

# --- STAGE 1: THE PRODUCER (Build the URL Queue) ---
def build_scraping_queue(client, year, qtr):
    """Fetches master.idx and saves all 13F-HR paths to BigQuery."""
    queue_table = f"{client.project}.gcp_shareloader.scraping_queue"
    
    idx_url = f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{qtr}/master.idx"
    logger.info(f"🔍 Stage 1: Fetching Master Index for {year} Q{qtr}")
    
    res = requests.get(idx_url, headers=HEADERS, timeout=30)
    if res.status_code != 200:
        logger.error(f"❌ Failed to fetch index: {res.status_code}")
        return

    queue_rows = []
    # SEC master.idx is |-separated
    for line in res.text.splitlines():
        if '13F-HR' in line:
            parts = line.split('|')
            # path looks like: edgar/data/123/456-789.txt
            path = parts[4]
            acc = path.split('/')[-1].replace('.txt', '').replace('-', '')
            
            queue_rows.append({
                "cik": parts[0],
                "company_name": parts[1],
                "accession_number": acc,
                "dir_url": f"https://www.sec.gov/Archives/edgar/data/{parts[0]}/{acc}/index.json",
                "status": "pending",
                "year": year,
                "qtr": qtr
            })

    if queue_rows:
        # WRITE_TRUNCATE clears old queue for the new quarter run
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        client.load_table_from_json(queue_rows, queue_table, job_config=job_config).result()
        logger.info(f"✅ Queue built with {len(queue_rows)} filings.")

# --- STAGE 2: THE CONSUMER (Process the URLs) ---
def process_queue_batch(client, limit=500):
    """Queries BigQuery for pending URLs and scrapes them."""
    queue_table = f"{client.project}.gcp_shareloader.scraping_queue"
    master_table = f"{client.project}.gcp_shareloader.all_holdings_master"
    
    # 1. Get batch from Queue
    query = f"SELECT * FROM `{queue_table}` WHERE status = 'pending' LIMIT {limit}"
    batch_df = client.query(query).to_dataframe()
    
    if batch_df.empty:
        logger.info("🏁 No pending items in queue.")
        return

    session = requests.Session()
    session.headers.update(HEADERS)
    
    final_holdings = []
    
    for _, row in batch_df.iterrows():
        try:
            # 2. Get index.json to find the infotable.xml
            time.sleep(0.15) # Safety gap
            dir_res = session.get(row['dir_url'], timeout=10)
            if dir_res.status_code != 200: continue
            
            items = dir_res.json().get('directory', {}).get('item', [])
            xml_name = next((i['name'] for i in items if 'infotable.xml' in i['name'].lower()), None)
            
            if not xml_name:
                # Mark as skipped if no data file found
                client.query(f"UPDATE `{queue_table}` SET status='no_xml' WHERE accession_number='{row['accession_number']}'")
                continue

            # 3. Parse the XML
            xml_url = row['dir_url'].replace('index.json', xml_name)
            xml_res = session.get(xml_url, timeout=15)
            
            root = etree.fromstring(xml_res.content)
            ns = {"ns": root.nsmap.get(None, "")}
            
            for info in root.xpath("//ns:infoTable", namespaces=ns):
                final_holdings.append({
                    "cik": row['cik'],
                    "company_name": row['company_name'],
                    "accession_number": row['accession_number'],
                    "issuer": info.findtext("ns:nameOfIssuer", namespaces=ns),
                    "cusip": info.findtext("ns:cusip", namespaces=ns),
                    "shares": info.findtext("ns:shrsOrPrnAmt/ns:sshPrnAmt", namespaces=ns),
                    "year": row['year'],
                    "qtr": row['qtr']
                })

            # 4. Mark as Done
            client.query(f"UPDATE `{queue_table}` SET status='done' WHERE accession_number='{row['accession_number']}'")
            logger.info(f"✅ Processed {row['company_name']}")

        except Exception as e:
            logger.error(f"💥 Failed {row['accession_number']}: {e}")

    # 5. Bulk Upload Holdings
    if final_holdings:
        client.load_table_from_json(final_holdings, master_table).result()
        logger.info(f"💾 Uploaded {len(final_holdings)} holdings rows.")

# --- ENTRY POINT ---
def run_main_scraper(year, qtr):
    client = bigquery.Client()
    logger.info('Checking if scraping table exist')
    ensure_tables_exist()
    # Check if queue exists/has pending items
    check_query = f"SELECT count(*) as count FROM `{client.project}.gcp_shareloader.scraping_queue` WHERE status='pending'"
    pending_count = list(client.query(check_query))[0].count

    if pending_count == 0:
        # If queue is empty, build it (Stage 1)
        build_scraping_queue(client, year, qtr)
    
    # Always try to process a batch (Stage 2)
    process_queue_batch(client)

if __name__ == "__main__":
    # Get Year/Qtr from ENV for Cloud Run
    y = int(os.getenv("YEAR", 2026))
    q = int(os.getenv("QTR", 1))
    run_main_scraper(y, q)