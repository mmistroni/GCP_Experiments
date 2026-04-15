import os, requests, logging, time, argparse, re
from datetime import datetime, date
from lxml import etree
from google.cloud import bigquery
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- CONFIGURATION ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("form4_sync_archive_final")

HEADERS = {
    'User-Agent': 'Institutional Research (mmistroni@gmail.com)',
    'Host': 'www.sec.gov',
    'Accept-Encoding': 'gzip, deflate'
}

PROJECT_ID = "datascience-projects"
DATASET = "gcp_shareloader"
QUEUE_TABLE = f"{PROJECT_ID}.{DATASET}.form4_queue"
MASTER_TABLE = f"{PROJECT_ID}.{DATASET}.form4_master"
STAGING_TABLE = f"{PROJECT_ID}.{DATASET}.stg_form4"

client = bigquery.Client(project=PROJECT_ID)

def get_session():
    s = requests.Session()
    retries = Retry(total=10, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    s.mount('https://', HTTPAdapter(max_retries=retries))
    s.headers.update(HEADERS)
    return s

from lxml import etree
from datetime import datetime

def parse_xml(xml_content, acc):
    """
    Robust Form 4 Parser: Captures deep-nested names, roles, and transaction data.
    """
    try:
        # Use recovery=True to handle messy SEC archive XML/HTML wrappers
        parser = etree.XMLParser(recover=True, remove_blank_text=True)
        root = etree.fromstring(xml_content, parser=parser)
        
        trades = []

        # 1. IDENTIFY THE ISSUER (Company Info)
        ticker = root.xpath("string(//*[local-name()='issuerTradingSymbol'])").strip().upper()
        issuer = root.xpath("string(//*[local-name()='issuerName'])").strip()

        # 2. IDENTIFY THE OWNER & ROLE (The 'Who')
        # We look specifically inside the reportingOwner block
        owner_block = root.xpath("//*[local-name()='reportingOwner']")
        if not owner_block:
            return [] # Skip if no owner info exists

        owner = owner_block[0]
        
        # Deep path for name: reportingOwner -> reportingOwnerId -> reportingOwnerName
        owner_name = owner.xpath("string(.//*[local-name()='reportingOwnerName'])").strip()
        
        # Fallback for older formats or different nesting
        if not owner_name:
            owner_name = owner.xpath("string(.//*[contains(local-name(), 'Name')])").strip()

        # Extract Roles (isDirector, isOfficer, isTenPercentOwner)
        is_director = owner.xpath("string(.//*[local-name()='isDirector'])").strip() in ('1', 'true', 'True')
        is_officer = owner.xpath("string(.//*[local-name()='isOfficer'])").strip() in ('1', 'true', 'True')
        officer_title = owner.xpath("string(.//*[local-name()='officerTitle'])").strip() or "N/A"

        # 3. EXTRACT TRANSACTIONS (Table I - Non-Derivative)
        # We focus on Table I as it represents actual stock ownership
        transaction_nodes = root.xpath("//*[local-name()='nonDerivativeTransaction']")

        for node in transaction_nodes:
            # Core Transaction Data
            shares = node.xpath("string(.//*[local-name()='transactionShares']/*[local-name()='value'])")
            price = node.xpath("string(.//*[local-name()='transactionPricePerShare']/*[local-name()='value'])")
            code = node.xpath("string(.//*[local-name()='transactionAcquiredDisposedCode']/*[local-name()='value'])").strip().upper()
            t_date = node.xpath("string(.//*[local-name()='transactionDate']/*[local-name()='value'])")

            # Final validation before appending
            if ticker and owner_name:
                try:
                    trades.append({
                        "ticker": ticker[:10],
                        "issuer": issuer[:255],
                        "owner_name": owner_name[:255],
                        "is_director": is_director,
                        "is_officer": is_officer,
                        "officer_title": officer_title[:255],
                        "shares": float(shares) if shares else 0.0,
                        "price": float(price) if price else 0.0,
                        "transaction_side": "BUY" if code == 'A' else "SELL",
                        "filing_date": t_date, # Handled by your SQL MERGE for casting
                        "accession_number": acc,
                        "ingested_at": datetime.utcnow().isoformat()
                    })
                except ValueError:
                    continue

        return trades

    except Exception as e:
        # Logging the error with the accession number helps find 'broken' XMLs later
        print(f"Error parsing XML for {acc}: {e}")
        return []




def seed_queue(year, qtr):
    """Idempotent seeding of the Quarterly Master Index."""
    url = f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{qtr}/master.idx"
    logger.info(f"📥 Checking Master Index: {url}")
    
    with get_session() as s:
        res = s.get(url, timeout=30)
        if res.status_code != 200:
            logger.error(f"❌ Could not reach SEC Index. Status: {res.status_code}")
            return
        
    lines = [l for l in res.text.splitlines() if '|4|' in l or '|4/A|' in l]
    
    existing_sql = f"SELECT accession_number FROM `{QUEUE_TABLE}` WHERE year={year} AND qtr={qtr}"
    existing = {row.accession_number for row in client.query(existing_sql)}
    
    to_add = []
    for line in lines:
        p = line.split('|')
        acc = p[4].split('/')[-1].replace('.txt', '')
        if acc not in existing:
            to_add.append({
                "cik": p[0], "accession_number": acc, "index_path": p[4],
                "status": "pending", "year": year, "qtr": qtr,
                "updated_at": datetime.utcnow().isoformat()
            })
    
    if to_add:
        logger.info(f"🌱 Seeding {len(to_add)} new entries for {year} Q{qtr}...")
        for i in range(0, len(to_add), 5000):
            client.load_table_from_json(to_add[i:i+5000], QUEUE_TABLE).result()
    else:
        logger.info("✅ Queue already up to date.")

def process_batch_sync(batch_limit, year, qtr):
    """
    Archive Processor: Uses .txt archive to guarantee 100% XML capture.
    """
    query = f"""
        SELECT * FROM `{QUEUE_TABLE}` 
        WHERE status='pending' AND year={year} AND qtr={qtr} 
        ORDER BY accession_number DESC
        LIMIT {batch_limit}
    """
    df = client.query(query).to_dataframe()
    if df.empty: return False, 0

    all_trades, success_accs, failed_accs = [], [], []
    
    with get_session() as s:
        for i, row in enumerate(df.to_dict('records'), 1):
            acc, cik = row['accession_number'], row['cik']
            clean_acc = acc.replace('-', '')
            archive_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{clean_acc}/{acc}.txt"
            
            logger.info(f"➡️ [{i}/{len(df)}] Processing Archive: {acc}")
            
            try:
                res = s.get(archive_url, timeout=12)
                if res.status_code == 200:
                    xml_blocks = re.findall(r'<XML>(.*?)</XML>', res.text, re.DOTALL)
                    found_form4 = False
                    
                    for block in xml_blocks:
                        if '<ownershipDocument>' in block:
                            trades = parse_xml(block.strip().encode('utf-8'), acc)
                            if trades:
                                all_trades.extend(trades)
                                success_accs.append(acc)
                                found_form4 = True
                                break 
                    
                    if not found_form4: failed_accs.append(acc)
                else: failed_accs.append(acc)
            except Exception as e:
                logger.error(f"   💥 Error for {acc}: {e}")
                failed_accs.append(acc)

            time.sleep(0.12) # Rate limit compliance

    if all_trades:
        tmp_id = f"{STAGING_TABLE}_sync_{int(time.time())}"
        client.load_table_from_json(all_trades, tmp_id).result()
        merge_sql = f"""
                MERGE `{MASTER_TABLE}` T
                USING `{tmp_id}` S
                ON T.accession_number = S.accession_number 
                AND T.ticker = S.ticker 
                AND T.shares = S.shares 
                -- Keep T.filing_date as is. Clean ONLY the Staging side.
                AND T.filing_date = CAST(SUBSTR(CAST(S.filing_date AS STRING), 1, 10) AS DATE)
                WHEN NOT MATCHED THEN
                INSERT (filing_date, ticker, issuer, owner_name, shares, price, transaction_side, accession_number, ingested_at)
                VALUES (
                    CAST(SUBSTR(CAST(S.filing_date AS STRING), 1, 10) AS DATE), 
                    S.ticker, 
                    S.issuer, 
                    S.owner_name, 
                    S.shares, 
                    S.price, 
                    S.transaction_side, 
                    S.accession_number, 
                    S.ingested_at
                )
            """
        client.query(merge_sql).result()
        client.delete_table(tmp_id)

    if success_accs:
        acc_str = ",".join([f"'{a}'" for a in success_accs])
        client.query(f"UPDATE `{QUEUE_TABLE}` SET status='done', updated_at=CURRENT_TIMESTAMP() WHERE accession_number IN ({acc_str})").result()
    if failed_accs:
        fail_str = ",".join([f"'{a}'" for a in failed_accs])
        client.query(f"UPDATE `{QUEUE_TABLE}` SET status='not_found' WHERE accession_number IN ({fail_str})").result()

    return True, len(success_accs)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    default_year, default_qtr = date.today().year, (date.today().month - 1) // 3 + 1
    parser.add_argument("--year", type=int, default=int(os.getenv('AGENT_YEAR', default_year)))
    parser.add_argument("--qtr", type=int, default=int(os.getenv('AGENT_QTR', default_qtr)))
    parser.add_argument("--limit", type=int, default=int(os.getenv('AGENT_LIMIT', 500)))
    args = parser.parse_args()

    seed_queue(args.year, args.qtr)
    total_ingested = 0
    while total_ingested < args.limit:
        batch_request = min(100, args.limit - total_ingested)
        has_work, count = process_batch_sync(batch_request, args.year, args.qtr)
        if count == 0: break
        total_ingested += count
        logger.info(f"📊 Progress: {total_ingested}/{args.limit}")