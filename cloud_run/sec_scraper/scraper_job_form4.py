import os, requests, logging, time, argparse
from datetime import datetime, date
from lxml import etree
from google.cloud import bigquery
from tqdm import tqdm
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- VERBOSE LOGGING CONFIG ---
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("form4_debug")

HEADERS = {
    'User-Agent': 'Research Project (mmistroni@gmail.com)',
    'Accept-Encoding': 'gzip, deflate',
    'Host': 'www.sec.gov'
}

# table names from your env
PROJECT_ID = "datascience-projects"
DATASET = "gcp_shareloader"
QUEUE_TABLE = f"{PROJECT_ID}.{DATASET}.form4_queue"
MASTER_TABLE = f"{PROJECT_ID}.{DATASET}.form4_master"

client = bigquery.Client(project=PROJECT_ID)

def get_session():
    s = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    s.mount('https://', HTTPAdapter(max_retries=retries))
    s.headers.update(HEADERS)
    return s

def debug_seeding(mode, year, qtr, force=False):
    """Seed queue with extra logging."""
    logger.info(f"🚀 Starting Seed - Mode: {mode}, Year: {year}, Qtr: {qtr}")
    
    url = f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{qtr}/master.idx"
    if mode == "DAILY":
        date_str = date.today().strftime("%Y%m%d")
        url = f"https://www.sec.gov/Archives/edgar/daily-index/{year}/QTR{qtr}/form.{date_str}.idx"

    logger.info(f"🔗 Fetching Index: {url}")
    with get_session() as s:
        res = s.get(url)
        logger.info(f"📡 SEC Response: {res.status_code}")
        if res.status_code != 200: return

    lines = [l for l in res.text.splitlines() if '|4|' in l or '|4/A|' in l]
    logger.info(f"📊 Found {len(lines)} Form 4 lines in index file.")

    # Reset logic if --force is used
    if force:
        logger.info("⚠️ FORCE mode: Resetting existing queue status to 'pending'...")
        client.query(f"UPDATE `{QUEUE_TABLE}` SET status='pending' WHERE year={year} AND qtr={qtr}").result()

    # Check existing
    existing_query = f"SELECT accession_number FROM `{QUEUE_TABLE}` WHERE year={year} AND qtr={qtr}"
    existing_accs = {row.accession_number for row in client.query(existing_query)}
    logger.info(f"🔎 Already in BigQuery Queue: {len(existing_accs)} items.")

    new_items = []
    for line in lines:
        p = line.split('|')
        acc = p[4].split('/')[-1].replace('.txt', '')
        if acc not in existing_accs:
            new_items.append({
                "cik": p[0], "accession_number": acc, "index_path": p[4],
                "status": "pending", "year": int(year), "qtr": int(qtr),
                "updated_at": datetime.utcnow().isoformat()
            })

    if new_items:
        logger.info(f"🌱 Inserting {len(new_items)} NEW items into BigQuery.")
        client.load_table_from_json(new_items, QUEUE_TABLE).result()
    else:
        logger.info("✅ No new accession numbers to add.")

def process_batch_verbose(limit):
    """Process with explicit 403 detection."""
    query = f"SELECT * FROM `{QUEUE_TABLE}` WHERE status='pending' LIMIT {limit}"
    df = client.query(query).to_dataframe()
    
    if df.empty:
        # Check if anything is actually 'done' vs 'pending'
        count_query = f"SELECT status, count(*) as cnt FROM `{QUEUE_TABLE}` GROUP BY 1"
        counts = client.query(count_query).to_dataframe()
        logger.info(f"🏜️ No pending items found. Current queue states:\n{counts}")
        return False

    logger.info(f"⚙️ Processing batch of {len(df)} items...")
    
    success_acc = []
    with get_session() as s:
        for _, row in df.iterrows():
            acc = row['accession_number']
            try:
                # 1. Directory lookup
                dir_url = f"https://www.sec.gov/Archives/{row['index_path'].replace('.txt', '-index.json')}"
                res = s.get(dir_url, timeout=10)
                
                if res.status_code == 403:
                    logger.error(f"🚫 403 FORBIDDEN for {acc}. Stopping immediately.")
                    return False

                # 2. Extract XML and fetch (logic omitted for brevity, identical to previous)
                # ... (Standard parse_xml and merge logic here) ...
                
                success_acc.append(acc)
                time.sleep(0.12)
            except Exception as e:
                logger.warning(f"❌ Error on {acc}: {e}")

    if success_acc:
        logger.info(f"✅ Batch complete. Updating {len(success_acc)} records to 'done'.")
        acc_list = ",".join([f"'{a}'" for a in success_acc])
        client.query(f"UPDATE `{QUEUE_TABLE}` SET status='done' WHERE accession_number IN ({acc_list})").result()
    
    return True

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", default="DAILY")
    parser.add_argument("--year", type=int, default=2026)
    parser.add_argument("--qtr", type=int, default=1)
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--force", action="store_true", help="Force reprocessing of existing queue")
    args = parser.parse_args()

    debug_seeding(args.mode, args.year, args.qtr, args.force)
    process_batch_verbose(args.limit)