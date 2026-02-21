import os
import sys
import time
import logging
import requests
from lxml import etree
from google.cloud import bigquery

# 1. ENHANCED LOGGING (Visible in Cloud Run Logs)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    stream=sys.stdout 
)
logger = logging.getLogger("13f_feature_agent")

HEADERS = {
    'User-Agent': 'Institutional Research mmapplausetest@gmail.com',
    'Accept-Encoding': 'gzip, deflate, br',
    'Host': 'www.sec.gov'
}

# 2. SCHEMA DEFINITION
MASTER_SCHEMA = [
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

def ensure_tables_exist(client, dataset_id):
    client.create_dataset(dataset_id, exists_ok=True)
    
    queue_schema = [
        bigquery.SchemaField("cik", "STRING"),
        bigquery.SchemaField("company_name", "STRING"),
        bigquery.SchemaField("accession_number", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("dir_url", "STRING"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("year", "INTEGER"),
        bigquery.SchemaField("qtr", "INTEGER"),
    ]
    
    client.create_table(bigquery.Table(f"{dataset_id}.scraping_queue", schema=queue_schema), exists_ok=True)
    client.create_table(bigquery.Table(f"{dataset_id}.all_holdings_master", schema=MASTER_SCHEMA), exists_ok=True)

def build_scraping_queue(client, year, qtr, queue_table):
    idx_url = f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{qtr}/master.idx"
    logger.info(f"🔍 Fetching SEC Index for {year} Q{qtr}...")
    
    res = requests.get(idx_url, headers=HEADERS, timeout=30)
    if res.status_code != 200:
        logger.error(f"❌ SEC Error {res.status_code}")
        return

    queue_rows = []
    for line in res.text.splitlines():
        if '13F-HR' in line:
            parts = line.split('|')
            acc = parts[4].split('/')[-1].replace('.txt', '').replace('-', '')
            queue_rows.append({
                "cik": str(parts[0]),
                "company_name": parts[1],
                "accession_number": acc,
                "dir_url": f"https://www.sec.gov/Archives/edgar/data/{parts[0]}/{acc}/index.json",
                "status": "pending", "year": year, "qtr": qtr
            })

    if queue_rows:
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        client.load_table_from_json(queue_rows, queue_table, job_config=job_config).result()
        logger.info(f"✅ Queue built with {len(queue_rows)} filings.")

def process_queue_batch(client, year, qtr, queue_table, master_table, limit):
    """
    Processes managers with strict timeouts to prevent 'Zombies'.
    Skips massive filings if they exceed processing time limits.
    """
    query = f"""
        SELECT * FROM `{queue_table}` 
        WHERE status = 'pending' AND year={year} AND qtr={qtr} 
        LIMIT {limit}
    """
    try:
        df = client.query(query).to_dataframe()
    except Exception as e:
        logger.error(f"❌ Failed to fetch queue batch: {e}")
        return 0
        
    if df.empty: return 0

    session = requests.Session()
    session.headers.update(HEADERS)
    job_config = bigquery.LoadJobConfig(schema=MASTER_SCHEMA, write_disposition="WRITE_APPEND", autodetect=False)
    
    q_dates = {1: f"{year}-03-31", 2: f"{year}-06-30", 3: f"{year}-09-30", 4: f"{year}-12-31"}
    p_date = q_dates.get(qtr)

    success_count = 0
    for _, row in df.iterrows():
        holdings = []
        acc_num = row['accession_number']
        manager_name = row['company_name']
        
        try:
            time.sleep(0.15) # SEC Compliance
            # 1. TIMEOUT-PROTECTED SEC REQUEST
            # (5s to connect, 45s to stream the data)
            dir_res = session.get(row['dir_url'], timeout=(5, 45)) 
            if dir_res.status_code != 200: 
                logger.warning(f"⚠️ SEC Index unavailable for {manager_name}")
                continue
            
            items = dir_res.json().get('directory', {}).get('item', [])
            xml_name = next((i['name'] for i in items if 'infotable.xml' in i['name'].lower()), None)
            
            if not xml_name:
                client.query(f"UPDATE `{queue_table}` SET status='no_xml' WHERE accession_number='{acc_num}'")
                continue

            # 2. FETCH AND PARSE XML (Memory Sensitive)
            xml_url = row['dir_url'].replace('index.json', xml_name)
            xml_res = session.get(xml_url, timeout=(10, 60))
            
            # Use lxml to parse (fast, but large files still take CPU)
            root = etree.fromstring(xml_res.content)
            nodes = root.xpath("//*[local-name()='infoTable']")

            for info in nodes:
                try:
                    val_str = info.xpath("string(*[local-name()='value'])")
                    # Handle different XML cases for shares
                    shares_xpath = "*[local-name()='shrsOrPrnAmt']/*[translate(local-name(), 'A', 'a')='sshprnamt']"
                    
                    holdings.append({
                        "cik": str(row['cik']).split('.')[0].strip(),
                        "manager_name": str(manager_name),
                        "issuer_name": str(info.xpath("string(*[local-name()='nameOfIssuer'])")),
                        "cusip": str(info.xpath("string(*[local-name()='cusip'])")),
                        "value_usd": int(float(val_str.replace(',', '') or 0)),
                        "shares": int(float(info.xpath(f"string({shares_xpath})").replace(',', '') or 0)),
                        "put_call": str(info.xpath("string(*[local-name()='putCall'])")) or None,
                        "filing_date": p_date,
                        "accession_number": str(acc_num)
                    })
                except Exception:
                    continue # Skip single bad rows within a filing

            if holdings:
                # 3. VERIFIED BIGQUERY UPLOAD
                # We add a timeout to the job itself to prevent hanging
                job = client.load_table_from_json(holdings, master_table, job_config=job_config)
                job.result(timeout=90) # Wait max 90 seconds for BQ to commit
                
                if job.errors:
                    logger.error(f"❌ BigQuery Schema Error in {manager_name}: {job.errors[0]}")
                    continue

                # 4. FINAL STATUS UPDATE (Atomic)
                update_query = f"UPDATE `{queue_table}` SET status='done' WHERE accession_number='{acc_num}'"
                client.query(update_query).result(timeout=30)
                
                logger.info(f"💾 SAVED: {manager_name} ({len(holdings)} rows)")
                success_count += 1
            else:
                logger.warning(f"Empty filing: {manager_name}")
                client.query(f"UPDATE `{queue_table}` SET status='empty' WHERE accession_number='{acc_num}'")

        except Exception as e:
            # Catch timeouts and unexpected errors so the loop continues to the next manager
            logger.error(f"💥 Failed/Timed-out on {manager_name}: {type(e).__name__} - {e}")
            
    return success_count



def run_master_scraper():
    # SETTINGS
    PROJECT_ID = "datascience-projects"
    YEAR = int(os.getenv("YEAR", 2020))
    QTR = int(os.getenv("QTR", 1))
    
    logger.info(f"🚀 STARTING JOB: {YEAR} Q{QTR} in Project: {PROJECT_ID}")
    
    client = bigquery.Client(project=PROJECT_ID)
    dataset_id = f"{PROJECT_ID}.gcp_shareloader"
    queue_table = f"{dataset_id}.scraping_queue"
    master_table = f"{dataset_id}.all_holdings_master"

    ensure_tables_exist(client, dataset_id)

    # 1. BUILD QUEUE IF NEEDED
    check_q = f"SELECT count(*) as count FROM `{queue_table}` WHERE year={YEAR} AND qtr={QTR}"
    if list(client.query(check_q))[0].count == 0:
        build_scraping_queue(client, YEAR, QTR, queue_table)

    # 2. RUN BATCHES
    total_in_this_run = 0
    # Process up to 10 batches of 20 managers each (Total 200 managers per job execution)
    for _ in range(10):
        count = process_queue_batch(client, YEAR, QTR, queue_table, master_table, limit=20)
        if count == 0: break
        total_in_this_run += count
        logger.info(f"📈 Total Managers saved so far: {total_in_this_run}")

    # 3. CLEANUP CHECK
    pending_check = f"SELECT count(*) as pending FROM `{queue_table}` WHERE status='pending' AND year={YEAR} AND qtr={QTR}"
    rem = list(client.query(pending_check))[0].pending
    
    if rem == 0:
        logger.info(f"🎊 FINISHED {YEAR} Q{QTR}. Clearing queue.")
        client.query(f"DELETE FROM `{queue_table}` WHERE year={YEAR} AND qtr={QTR}")
    else:
        logger.info(f"⏳ {rem} managers still pending for {YEAR} Q{QTR}. Run job again.")

if __name__ == "__main__":
    run_master_scraper()