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
    client.create_dataset(dataset_id, exists_ok=True)
    
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
    
    master_table_id = f"{dataset_id}.all_holdings_master"
    master_schema = [
        bigquery.SchemaField("cik", "STRING"),
        bigquery.SchemaField("manager_name", "STRING"),
        bigquery.SchemaField("issuer_name", "STRING"),
        bigquery.SchemaField("cusip", "STRING"),
        bigquery.SchemaField("value_usd", "INTEGER"),
        bigquery.SchemaField("shares", "INTEGER"),
        bigquery.SchemaField("put_call", "STRING"),
        bigquery.SchemaField("filing_date", "DATETIME"),
        bigquery.SchemaField("accession_number", "STRING"),
    ]

    for table_id, schema in [(queue_table_id, queue_schema), (master_table_id, master_schema)]:
        table = bigquery.Table(table_id, schema=schema)
        client.create_table(table, exists_ok=True) 
        logger.info(f"✅ Verified table: {table_id}")

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
    for line in res.text.splitlines():
        if '13F-HR' in line:
            parts = line.split('|')
            path = parts[4]
            acc = path.split('/')[-1].replace('.txt', '').replace('-', '')
            
            queue_rows.append({
                "cik": str(parts[0]),
                "company_name": parts[1],
                "accession_number": acc,
                "dir_url": f"https://www.sec.gov/Archives/edgar/data/{parts[0]}/{acc}/index.json",
                "status": "pending",
                "year": year,
                "qtr": qtr
            })

    if queue_rows:
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        client.load_table_from_json(queue_rows, queue_table, job_config=job_config).result()
        logger.info(f"✅ Queue built with {len(queue_rows)} filings.")

def process_queue_batch(client, year, qtr, limit=100):
    """Processes a batch and returns True if work was done, False if empty."""
    queue_table = f"{client.project}.gcp_shareloader.scraping_queue"
    master_table = f"{client.project}.gcp_shareloader.all_holdings_master"
    
    quarter_dates = {1: f"{year}-03-31", 2: f"{year}-06-30", 3: f"{year}-09-30", 4: f"{year}-12-31"}
    PARTITION_DATE = quarter_dates[qtr]

    query = f"SELECT * FROM `{queue_table}` WHERE status = 'pending' LIMIT {limit}"
    batch_df = client.query(query).to_dataframe()
    
    if batch_df.empty:
        logger.info("🏁 Queue exhausted.")
        return False

    session = requests.Session()
    session.headers.update(HEADERS)
    final_holdings = []
    processed_accs = []
    
    for _, row in batch_df.iterrows():
        try:
            time.sleep(0.12) # Stay under SEC 10req/sec limit
            dir_res = session.get(row['dir_url'], timeout=10)
            if dir_res.status_code != 200: continue
            
            items = dir_res.json().get('directory', {}).get('item', [])
            xml_name = next((i['name'] for i in items if 'infotable.xml' in i['name'].lower()), None)
            
            if not xml_name:
                client.query(f"UPDATE `{queue_table}` SET status='no_xml' WHERE accession_number='{row['accession_number']}'")
                continue

            xml_url = row['dir_url'].replace('index.json', xml_name)
            xml_res = session.get(xml_url, timeout=15)
            root = etree.fromstring(xml_res.content)
            nodes = root.xpath("//*[local-name()='infoTable']")

            for info in nodes:
                try:
                    issuer = info.xpath("string(*[local-name()='nameOfIssuer'])")
                    cusip = info.xpath("string(*[local-name()='cusip'])")
                    val_str = info.xpath("string(*[local-name()='value'])")
                    shares_xpath = "*[local-name()='shrsOrPrnAmt']/*[translate(local-name(), 'A', 'a')='sshprnamt']"
                    shares_val = info.xpath(f"string({shares_xpath})")
                    pc_str = info.xpath("string(*[local-name()='putCall'])")

                    final_holdings.append({
                        "cik": str(row['cik']),
                        "manager_name": row['company_name'],
                        "issuer_name": issuer,
                        "cusip": cusip,
                        "value_usd": int(float(val_str.replace(',', '') or 0)),
                        "shares": int(float(shares_val.replace(',', '') or 0)),
                        "put_call": pc_str if pc_str else None,
                        "filing_date": PARTITION_DATE,
                        "accession_number": row['accession_number']
                    })
                except Exception: continue

            processed_accs.append(row['accession_number'])
            logger.info(f"✅ Parsed {row['company_name']}")

        except Exception as e:
            logger.error(f"💥 Error processing {row['accession_number']}: {e}")

    # Bulk upload and Batch Status Update
    if final_holdings:
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        try:
            client.load_table_from_json(final_holdings, master_table, job_config=job_config).result()
            
            # AMENDMENT: Batch update status for this batch (outside the for-loop)
            acc_list = ", ".join([f"'{a}'" for a in processed_accs])
            client.query(f"UPDATE `{queue_table}` SET status='done' WHERE accession_number IN ({acc_list})")
            logger.info(f"💾 Saved {len(final_holdings)} rows to master holdings.")
        except Exception as e:
            logger.error(f"❌ Upload Failed: {e}")
    
    return True

def run_master_scraper(year, qtr):
    client = bigquery.Client()
    ensure_tables_exist(client)
    
    # Check if queue has pending items
    check_query = f"SELECT count(*) as count FROM `{client.project}.gcp_shareloader.scraping_queue` WHERE status='pending'"
    pending_count = list(client.query(check_query))[0].count

    if pending_count == 0:
        build_scraping_queue(client, year, qtr)
    
    # AMENDMENT: Loop until all items are processed
    active = True
    while active:
        active = process_queue_batch(client, year, qtr, limit=100)

if __name__ == "__main__":
    y = int(os.getenv("YEAR", 2020))
    q = int(os.getenv("QTR", 1))
    run_master_scraper(y, q)