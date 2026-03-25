import os, requests, logging, time, argparse
from datetime import datetime, date, timedelta
from lxml import etree
from google.cloud import bigquery
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- CONFIG ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("form4_daily_agent")

HEADERS = {'User-Agent': 'Institutional Research (mmistroni@gmail.com)', 'Host': 'www.sec.gov'}
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
                        "issuer": issuer.strip()[:1024] if issuer else "Unknown",
                        "owner_name": owner.strip()[:1024] if owner else "Unknown",
                        "shares": float(shares),
                        "price": float(price) if (price and price.strip()) else 0.0,
                        "transaction_side": "BUY" if code == 'A' else "SELL",
                        "filing_date": t_date, 
                        "accession_number": acc,
                        "ingested_at": datetime.utcnow().isoformat()
                    })
                except: continue
        return trades
    except: return []

def seed_daily_queue():
    """Checks last 4 days for new filings."""
    year = date.today().year
    qtr = (date.today().month - 1) // 3 + 1
    new_found = 0

    with get_session() as s:
        for i in range(4):
            check_date = date.today() - timedelta(days=i)
            if check_date.weekday() >= 5: continue # Skip weekends
            
            date_str = check_date.strftime("%Y%m%d")
            # Try standard and crawler indices
            for idx_type in ['form', 'crawler']:
                url = f"https://www.sec.gov/Archives/edgar/daily-index/{year}/QTR{qtr}/{idx_type}.{date_str}.idx"
                logger.info(f"🔍 Checking: {url}")
                res = s.get(url, timeout=15)
                if res.status_code != 200: continue
                
                lines = [l for l in res.text.splitlines() if '|4|' in l or '|4/A|' in l]
                if not lines: continue

                existing_query = f"SELECT accession_number FROM `{QUEUE_TABLE}` WHERE year={year} AND qtr={qtr}"
                existing_accs = {row.accession_number for row in client.query(existing_query)}
                
                to_insert = []
                for line in lines:
                    p = line.split('|')
                    acc = p[4].split('/')[-1].replace('.txt', '')
                    if acc not in existing_accs:
                        to_insert.append({
                            "cik": p[0], "accession_number": acc, "index_path": p[4],
                            "status": "pending", "year": year, "qtr": qtr,
                            "updated_at": datetime.utcnow().isoformat()
                        })
                
                if to_insert:
                    client.load_table_from_json(to_insert, QUEUE_TABLE).result()
                    new_found += len(to_insert)
                    logger.info(f"🌱 Added {len(to_insert)} items from {date_str}")

    return new_found

def process_daily_batch(limit):
    year, qtr = date.today().year, (date.today().month - 1) // 3 + 1
    query = f"SELECT * FROM `{QUEUE_TABLE}` WHERE status='pending' AND year={year} AND qtr={qtr} LIMIT {limit}"
    df = client.query(query).to_dataframe()
    if df.empty: return False, 0

    all_trades, success_accs = [], []
    with get_session() as s:
        for i, row in df.iterrows():
            acc = row['accession_number']
            logger.info(f"➡️ [{i+1}/{len(df)}] Processing {acc}")
            try:
                # 1. Get XML Path
                dir_url = f"https://www.sec.gov/Archives/{row['index_path'].replace('.txt', '-index.json')}"
                res = s.get(dir_url, timeout=10)
                if res.status_code == 403: return False, -1
                if res.status_code != 200:
                    client.query(f"UPDATE `{QUEUE_TABLE}` SET status='failed' WHERE accession_number='{acc}'").result()
                    continue

                items = res.json().get('directory', {}).get('item', [])
                xml_name = next((it['name'] for it in items if it['name'].endswith('.xml')), None)
                if not xml_name:
                    client.query(f"UPDATE `{QUEUE_TABLE}` SET status='skipped' WHERE accession_number='{acc}'").result()
                    continue

                # 2. Extract
                xml_url = f"https://www.sec.gov/Archives/edgar/data/{row['cik']}/{acc.replace('-', '')}/{xml_name}"
                xml_res = s.get(xml_url, timeout=10)
                if xml_res.status_code == 200:
                    trades = parse_xml(xml_res.content, acc)
                    if trades: all_trades.extend(trades)
                    success_accs.append(acc)
                time.sleep(0.12)
            except: continue

    if all_trades:
        tmp = f"{STAGING_TABLE}_daily_{int(time.time())}"
        client.load_table_from_json(all_trades, tmp).result()
        client.query(f"""
            MERGE `{MASTER_TABLE}` T USING `{tmp}` S ON T.accession_number=S.accession_number AND T.ticker=S.ticker AND T.shares=S.shares AND T.filing_date=S.filing_date
            WHEN NOT MATCHED THEN INSERT (filing_date, ticker, issuer, owner_name, shares, price, transaction_side, accession_number, ingested_at)
            VALUES (S.filing_date, S.ticker, S.issuer, S.owner_name, S.shares, S.price, S.transaction_side, S.accession_number, S.ingested_at)
        """).result()
        client.delete_table(tmp)

    if success_accs:
        acc_str = ",".join([f"'{a}'" for a in success_accs])
        client.query(f"UPDATE `{QUEUE_TABLE}` SET status='done', updated_at=CURRENT_TIMESTAMP() WHERE accession_number IN ({acc_str})").result()

    return True, len(success_accs)

if __name__ == "__main__":
    # 1. Force Seeding for the last 4 days
    new_items = seed_daily_queue()
    
    # 2. Run Process
    total_ingested = 0
    while True:
        # We only look for CURRENT quarter/year items to avoid 404s on old indices
        work_done, count = process_daily_batch(100)
        
        if count == -1: break # Rate limited
        if count == 0:
            logger.info("🏁 Daily Queue Clean.")
            break
            
        total_ingested += count
        if total_ingested >= 500: break # Safety cap for daily run

    logger.info(f"✨ Daily Run Complete. Ingested: {total_ingested}")