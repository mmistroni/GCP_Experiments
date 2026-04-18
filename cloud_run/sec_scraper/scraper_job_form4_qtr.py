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


import re
from lxml import etree
from datetime import datetime


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

def clean_numeric(value_str):
    """Removes footnote markers [1], commas, and junk from numeric strings."""
    if not value_str: return 0.0
    # Keep only digits, dots, and negative signs
    cleaned = re.sub(r'[^0-9.\-]', '', str(value_str))
    try:
        return float(cleaned) if cleaned else 0.0
    except ValueError:
        return 0.0

def parse_xml(xml_content, acc):
    try:
        parser = etree.XMLParser(recover=True, remove_blank_text=True)
        root = etree.fromstring(xml_content if isinstance(xml_content, bytes) else xml_content.encode('utf-8'), parser=parser)
        
        # Robust Ticker/Issuer extraction using local-name() to bypass namespace issues
        ticker = root.xpath("string(//*[local-name()='issuerTradingSymbol'])").strip().upper() or "UNKNOWN"
        issuer = root.xpath("string(//*[local-name()='issuerName'])").strip() or "N/A"
        owner_name = root.xpath("string(//*[local-name()='reportingOwnerName'])").strip() or "UNKNOWN_OWNER"

        trades = []
        # Find all non-derivative transactions
        transaction_nodes = root.xpath("//*[contains(local-name(), 'nonDerivativeTransaction')]")
        
        # Log findings for visibility
        logger.info(f"🔎 {acc} | Ticker: {ticker} | Found {len(transaction_nodes)} trades")

        for node in transaction_nodes:
            # Extract and clean values
            raw_shares = node.xpath("string(.//*[local-name()='transactionShares']/*[local-name()='value'])")
            raw_price = node.xpath("string(.//*[local-name()='transactionPricePerShare']/*[local-name()='value'])")
            code = node.xpath("string(.//*[local-name()='transactionAcquiredDisposedCode']/*[local-name()='value'])").strip().upper()
            t_date = node.xpath("string(.//*[local-name()='transactionDate']/*[local-name()='value'])")

            shares = clean_numeric(raw_shares)
            price = clean_numeric(raw_price)

            # Data Guard: Block clearly impossible volumes (e.g., BTSG errors)
            if shares * price > 1_000_000_000_000:
                logger.warning(f"🚩 Outlier blocked in {acc}: {ticker} - {shares} shares @ ${price}")
                continue
            
            # Data Guard: Skip if ticker is still missing
            if ticker == "UNKNOWN" or not ticker:
                logger.warning(f"🚩 Missing Ticker in {acc} - skipping record")
                continue

            trades.append({
                "ticker": ticker[:10],
                "issuer": issuer[:255],
                "owner_name": owner_name[:255],
                "shares": shares,
                "price": price,
                "transaction_side": "BUY" if code == 'A' else "SELL",
                "filing_date": t_date,
                "accession_number": acc,
                "ingested_at": datetime.utcnow().isoformat(),
                "is_officer": False, 
                "is_director": False, 
                "officer_title": "N/A"
            })
            
        return trades
    except Exception as e:
        logger.error(f"CRITICAL ERROR parsing {acc}: {e}")
        return []

def process_batch_sync(batch_limit, year, qtr):
    """
    Archive Processor with batch deduplication and robust merging.
    """
    query = f"""
        SELECT * FROM `{QUEUE_TABLE}` 
        WHERE status='pending' AND year={year} AND qtr={qtr} 
        ORDER BY accession_number DESC
        LIMIT {batch_limit * 2}  -- Over-fetch to allow for deduplication
    """
    df = client.query(query).to_dataframe()
    if df.empty: return False, 0

    # DEDUPLICATION: Avoid hitting the same URL twice if the index has duplicates
    df = df.drop_duplicates(subset=['accession_number']).head(batch_limit)
    
    all_trades, success_accs, failed_accs = [], [], []
    records = df.to_dict('records')
    
    with get_session() as s:
        for i, row in enumerate(records, 1):
            acc, cik = row['accession_number'], row['cik']
            clean_acc = acc.replace('-', '')
            archive_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{clean_acc}/{acc}.txt"
            
            logger.info(f"➡️ [{i}/{len(records)}] Processing Archive: {acc}")
            
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
                            # Even if 0 trades found, it's a valid Form 4 we've checked
                            success_accs.append(acc)
                            found_form4 = True
                            break 
                    
                    if not found_form4: failed_accs.append(acc)
                else: failed_accs.append(acc)
            except Exception as e:
                logger.error(f"   💥 Error for {acc}: {e}")
                failed_accs.append(acc)

            time.sleep(0.12) # Respect SEC Rate Limit

    if all_trades:
        merge_sql = f"""
            MERGE `{MASTER_TABLE}` T
            USING (
                SELECT 
                    ticker, issuer, owner_name, is_officer, is_director, officer_title,
                    CAST(shares AS FLOAT64) as shares, 
                    CAST(price AS FLOAT64) as price,
                    transaction_side, 
                    SAFE.PARSE_DATE('%Y-%m-%d', filing_date) as filing_date,
                    accession_number, 
                    TIMESTAMP(ingested_at) as ingested_at
                FROM UNNEST(@trades)
            ) S
            ON T.accession_number = S.accession_number 
            AND T.owner_name = S.owner_name 
            AND T.ticker = S.ticker
            AND T.filing_date = S.filing_date
            AND T.shares = S.shares
            AND T.price = S.price
            AND T.transaction_side = S.transaction_side
            WHEN MATCHED THEN
                UPDATE SET T.ingested_at = S.ingested_at
            WHEN NOT MATCHED THEN
                INSERT (ticker, issuer, owner_name, is_officer, is_director, officer_title, shares, price, transaction_side, filing_date, accession_number, ingested_at)
                VALUES (ticker, issuer, owner_name, is_officer, is_director, officer_title, shares, price, transaction_side, filing_date, accession_number, ingested_at)
        """

        structured_trades = [
            bigquery.StructQueryParameter(
                "unused",
                bigquery.ScalarQueryParameter("ticker", "STRING", t["ticker"]),
                bigquery.ScalarQueryParameter("issuer", "STRING", t["issuer"]),
                bigquery.ScalarQueryParameter("owner_name", "STRING", t["owner_name"]),
                bigquery.ScalarQueryParameter("is_officer", "BOOL", t["is_officer"]),
                bigquery.ScalarQueryParameter("is_director", "BOOL", t["is_director"]),
                bigquery.ScalarQueryParameter("officer_title", "STRING", t["officer_title"]),
                bigquery.ScalarQueryParameter("shares", "FLOAT64", t["shares"]),
                bigquery.ScalarQueryParameter("price", "FLOAT64", t["price"]),
                bigquery.ScalarQueryParameter("transaction_side", "STRING", t["transaction_side"]),
                bigquery.ScalarQueryParameter("filing_date", "STRING", t["filing_date"]),
                bigquery.ScalarQueryParameter("accession_number", "STRING", t["accession_number"]),
                bigquery.ScalarQueryParameter("ingested_at", "TIMESTAMP", t["ingested_at"]),
            ) for t in all_trades
        ]

        try:
            job_config = bigquery.QueryJobConfig(
                query_parameters=[bigquery.ArrayQueryParameter("trades", "RECORD", structured_trades)]
            )
            client.query(merge_sql, job_config=job_config).result()
            logger.info(f"✅ Successfully merged {len(all_trades)} trades.")
        except Exception as e:
            logger.error(f"❌ BigQuery Merge failed: {e}")

    # Update Queue Status
    if success_accs:
        acc_str = ",".join([f"'{a}'" for a in success_accs])
        client.query(f"UPDATE `{QUEUE_TABLE}` SET status='done', updated_at=CURRENT_TIMESTAMP() WHERE accession_number IN ({acc_str})").result()
    if failed_accs:
        fail_str = ",".join([f"'{a}'" for a in failed_accs])
        client.query(f"UPDATE `{QUEUE_TABLE}` SET status='not_found' WHERE accession_number IN ({fail_str})").result()

    actual_count = len(success_accs)
    logger.info(f"💾 Batch finished. Filings: {actual_count}, Trades: {len(all_trades)}")
    return True, actual_count





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