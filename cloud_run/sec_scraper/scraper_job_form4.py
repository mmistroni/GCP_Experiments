import os, requests, logging, time, argparse
from datetime import datetime, date
from lxml import etree
from google.cloud import bigquery
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- CONFIGURATION ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("form4_resilient_agent")

HEADERS = {
    'User-Agent': 'Institutional Research (mmistroni@gmail.com)',
    'Accept-Encoding': 'gzip, deflate',
    'Host': 'www.sec.gov'
}

PROJECT_ID = "datascience-projects"
DATASET = "gcp_shareloader"
QUEUE_TABLE = f"{PROJECT_ID}.{DATASET}.form4_queue"
MASTER_TABLE = f"{PROJECT_ID}.{DATASET}.form4_master"
STAGING_TABLE = f"{PROJECT_ID}.{DATASET}.stg_form4"

client = bigquery.Client(project=PROJECT_ID)

def get_session():
    s = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    s.mount('https://', HTTPAdapter(max_retries=retries))
    s.headers.update(HEADERS)
    return s

def parse_xml(xml_content, acc):
    """Extracts insider trades from SEC XML."""
    try:
        root = etree.fromstring(xml_content, parser=etree.XMLParser(recover=True))
        trades = []
        ticker = root.xpath("string(//*[local-name()='issuerTradingSymbol'])")
        issuer = root.xpath("string(//*[local-name()='issuerName'])")
        owner = root.xpath("string(//*[local-name()='reportingOwnerName'])")
        
        nodes = root.xpath("//*[local-name()='nonDerivativeTransaction' or local-name()='derivativeTransaction']")
        for tx in nodes:
            shares = tx.xpath("string(.//*[local-name()='transactionShares']/*[local-name()='value'])")
            price = tx.xpath("string(.//*[local-name()='transactionPricePerShare']/*[local-name()='value'])")
            code = tx.xpath("string(.//*[local-name()='transactionAcquiredDisposedCode']/*[local-name()='value'])")
            t_date = tx.xpath("string(.//*[local-name()='transactionDate']/*[local-name()='value'])")
            
            if ticker and shares:
                try:
                    trades.append({
                        "ticker": ticker.strip().upper(),
                        "issuer": issuer.strip() if issuer else "Unknown",
                        "owner_name": owner.strip() if owner else "Unknown",
                        "shares": float(shares),
                        "price": float(price) if (price and price.strip()) else 0.0,
                        "transaction_side": "BUY" if code == 'A' else "SELL",
                        "filing_date": t_date, 
                        "accession_number": acc,
                        "ingested_at": datetime.utcnow().isoformat()
                    })
                except (ValueError, TypeError): continue
        return trades
    except Exception: return []

def seed_queue(mode, year, qtr, force=False):
    logger.info(f"🌱 Seeding check for {year} Q{qtr}")
    url = f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{qtr}/master.idx"
    if mode == "DAILY":
        date_str = date.today().strftime("%Y%m%d")
        url = f"https://www.sec.gov/Archives/edgar/daily-index/{year}/QTR{qtr}/form.{date_str}.idx"

    with get_session() as s:
        res = s.get(url, timeout=30)
        if res.status_code != 200: return

    lines = [l for l in res.text.splitlines() if '|4|' in l or '|4/A|' in l]
    
    if force:
        client.query(f"UPDATE `{QUEUE_TABLE}` SET status='pending' WHERE year={year} AND qtr={qtr}").result()

    existing_query = f"SELECT accession_number FROM `{QUEUE_TABLE}` WHERE year={year} AND qtr={qtr}"
    existing_accs = {row.accession_number for row in client.query(existing_query)}
    
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
        logger.info(f"🌱 Adding {len(new_items)} new filings.")
        for i in range(0, len(new_items), 5000):
            client.load_table_from_json(new_items[i:i+5000], QUEUE_TABLE).result()

def process_batch(batch_limit, year, qtr):
    query = f"SELECT * FROM `{QUEUE_TABLE}` WHERE status='pending' AND year={year} AND qtr={qtr} LIMIT {batch_limit}"
    df = client.query(query).to_dataframe()
    if df.empty: return False, 0

    all_trades, success_acc = [], []
    with get_session() as s:
        for _, row in df.iterrows():
            acc = row['accession_number']
            try:
                # 1. JSON Index Lookup
                dir_url = f"https://www.sec.gov/Archives/{row['index_path'].replace('.txt', '-index.json')}"
                res = s.get(dir_url, timeout=20)
                
                if res.status_code == 403: return False, -1
                if res.status_code != 200:
                    client.query(f"UPDATE `{QUEUE_TABLE}` SET status='failed' WHERE accession_number='{acc}'").result()
                    continue

                # 2. Extract XML Filename
                items = res.json().get('directory', {}).get('item', [])
                xml_name = next((i['name'] for i in items if i['name'].endswith('.xml')), None)
                
                if not xml_name:
                    client.query(f"UPDATE `{QUEUE_TABLE}` SET status='skipped' WHERE accession_number='{acc}'").result()
                    continue
                
                # 3. Fetch XML
                xml_url = f"https://www.sec.gov/Archives/edgar/data/{row['cik']}/{acc.replace('-', '')}/{xml_name}"
                xml_res = s.get(xml_url, timeout=20)
                
                if xml_res.status_code == 200:
                    trades = parse_xml(xml_res.content, acc)
                    if trades: 
                        all_trades.extend(trades)
                        logger.info(f"✅ {acc}: {len(trades)} trades.")
                    else:
                        logger.info(f"ℹ️ {acc}: No trades in XML.")
                    success_acc.append(acc)
                
                time.sleep(0.12)
            except Exception as e:
                logger.error(f"💥 Error {acc}: {e}")
                continue

    # 4. Persistence
    if all_trades:
        tmp_id = f"{STAGING_TABLE}_{int(time.time())}"
        client.load_table_from_json(all_trades, tmp_id).result()
        merge_sql = f"""
            MERGE `{MASTER_TABLE}` T USING `{tmp_id}` S
            ON T.accession_number=S.accession_number AND T.ticker=S.ticker AND T.shares=S.shares AND T.filing_date=S.filing_date
            WHEN NOT MATCHED THEN INSERT (filing_date, ticker, issuer, owner_name, shares, price, transaction_side, accession_number, ingested_at)
            VALUES (S.filing_date, S.ticker, S.issuer, S.owner_name, S.shares, S.price, S.transaction_side, S.accession_number, S.ingested_at)
        """
        client.query(merge_sql).result()
        client.delete_table(tmp_id)

    if success_acc:
        acc_list = ",".join([f"'{a}'" for a in success_acc])
        client.query(f"UPDATE `{QUEUE_TABLE}` SET status='done', updated_at=CURRENT_TIMESTAMP() WHERE accession_number IN ({acc_list})").result()
    
    return True, len(success_acc)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", default=os.getenv('AGENT_MODE', "DAILY"))
    parser.add_argument("--year", type=int, default=int(os.getenv('AGENT_YEAR', date.today().year)))
    parser.add_argument("--qtr", type=int, default=int(os.getenv('AGENT_QTR', (date.today().month - 1) // 3 + 1)))
    parser.add_argument("--limit", type=int, default=int(os.getenv('AGENT_LIMIT', 100)))
    parser.add_argument("--force", action="store_true", default=os.getenv('AGENT_FORCE', 'false').lower() == 'true')
    args = parser.parse_args()

    seed_queue(args.mode, args.year, args.qtr, args.force)
    
    processed_total = 0
    while True:
        batch_size = 250
        if args.limit > 0:
            remaining = args.limit - processed_total
            if remaining <= 0: break
            batch_size = min(batch_size, remaining)

        logger.info(f"🔄 Processing {batch_size} (Total Done: {processed_total})")
        work_done, count = process_batch(batch_size, args.year, args.qtr)
        
        if count == -1: break
        if count == 0:
            if not work_done: break
            # Logic: If query returned rows but 0 were 'success_acc', it means they failed/skipped.
            # We don't increment processed_total, but we also don't break because the next 
            # query will fetch the NEXT 250 records since we updated failed/skipped statuses.
            continue
        
        processed_total += count

    logger.info(f"✨ Session Finished. Ingested: {processed_total}")