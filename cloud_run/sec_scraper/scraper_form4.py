import os
import requests
import logging
from datetime import datetime, timedelta
from lxml import etree
from google.cloud import bigquery

# --- CONFIGURATION ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("form4_agent")

HEADERS = {'User-Agent': 'Institutional Research your-email@example.com'}
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT")
DATASET = "gcp_shareloader"


# --- STAGE 1: CRAWL DAILY INDEX ---
def get_daily_index_rows(client, target_date):
    """Fetches Form 4 entries from the SEC Daily Index."""
    date_str = target_date.strftime("%Y%m%d")
    year = target_date.year
    qtr = (target_date.month - 1) // 3 + 1

    url = f"https://www.sec.gov/Archives/edgar/daily-index/{year}/QTR{qtr}/master.{date_str}.idx"
    logger.info(f"🔍 Fetching Index: {url}")

    res = requests.get(url, headers=HEADERS)
    if res.status_code != 200:
        return []

    rows = []
    for line in res.text.splitlines():
        if '|4|' in line:  # Form 4 only
            parts = line.split('|')
            acc = parts[4].split('/')[-1].replace('.txt', '').replace('-', '')
            rows.append({
                "cik": parts[0],
                "company": parts[1],
                "acc": acc,
                "url": f"https://www.sec.gov/Archives/edgar/data/{parts[0]}/{acc}/index.json"
            })
    return rows


# --- STAGE 2: PARSE FORM 4 XML ---
def parse_form4_xml(xml_content, acc):
    root = etree.fromstring(xml_content)
    trades = []

    # 1. EXTRACT IDENTITY (The "Who")
    # This sits in the reportingOwnerRelationship block
    rel_node = root.xpath("//reportingOwnerRelationship")[0]

    # SEC uses '1' or 'true' for boolean flags
    is_dir = rel_node.xpath("string(isDirector)") in ('1', 'true')
    is_off = rel_node.xpath("string(isOfficer)") in ('1', 'true')
    is_10p = rel_node.xpath("string(isTenPercentOwner)") in ('1', 'true')
    title = rel_node.xpath("string(officerTitle)")  # e.g., "CEO"

    ticker = root.xpath("string(//issuerTradingSymbol)")
    owner = root.xpath("string(//reportingOwnerName)")

    # 2. EXTRACT TRANSACTIONS (The "What")
    for tx in root.xpath("//nonDerivativeTransaction"):
        # The 'Code' is vital: 'P' is a market buy (strong), 'A' is a grant (weak)
        trans_code = tx.xpath("string(.//transactionCoding/transactionCode)")

        shares = tx.xpath("string(.//transactionShares/value)")
        price = tx.xpath("string(.//transactionPricePerShare/value)")
        code = tx.xpath("string(.//transactionAcquiredDisposedCode/value)")  # A=Acquired, D=Disposed
        date = tx.xpath("string(.//transactionDate/value)")

        trades.append({
            "ticker": ticker,
            "owner_name": owner,
            "officer_title": title if is_off else None,
            "is_director": is_dir,
            "is_officer": is_off,
            "is_ten_percent": is_10p,
            "transaction_code": trans_code,  # LOOK FOR 'P'
            "shares": float(shares) if shares else 0,
            "price": float(price) if price else 0,
            "side": "BUY" if code == 'A' else "SELL",
            "date": date,
            "accession_number": acc
        })
    return trades

# --- STAGE 3: SAFE MERGE TO BIGQUERY ---
def run_form4_job():
    client = bigquery.Client()
    all_parsed_trades = []

    # Fetch last 3 days to catch late filers
    for i in range(5):
        target_date = datetime.now() - timedelta(days=i)
        filings = get_daily_index_rows(client, target_date)

        for f in filings[:20]:  # Batch limit for testing
            try:
                dir_res = requests.get(f['url'], headers=HEADERS).json()
                xml_name = next(i['name'] for i in dir_res['directory']['item'] if i['name'].endswith('.xml'))
                xml_url = f['url'].replace('index.json', xml_name)
                xml_res = requests.get(xml_url, headers=HEADERS)
                all_parsed_trades.extend(parse_form4_xml(xml_res.content, f['acc']))
            except Exception as e:
                continue

    if all_parsed_trades:
        # Load into STAGING table
        stg_table = f"{PROJECT_ID}.{DATASET}.stg_form4"
        client.load_table_from_json(all_parsed_trades, stg_table,
                                    job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")).result()

        # MERGE into MASTER table
        master_table = f"{PROJECT_ID}.{DATASET}.form4_master"
        merge_sql = f"""
        MERGE `{master_table}` T
        USING `{stg_table}` S
        ON T.accession_number = S.accession_number AND T.shares = S.shares
        WHEN NOT MATCHED THEN INSERT ROW
        """
        client.query(merge_sql).result()
        logger.info(f"✅ Successfully merged {len(all_parsed_trades)} records.")


if __name__ == "__main__":
    run_form4_job()