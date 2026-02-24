import time
import logging
import requests
from lxml import etree
from google.cloud import bigquery
from requests.exceptions import ReadTimeout, ConnectTimeout

# --- CONFIG ---
PROJECT_ID = "datascience-projects"
DATASET_ID = "gcp_shareloader"
QUEUE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.scraping_queue"
MASTER_TABLE = f"{PROJECT_ID}.{DATASET_ID}.all_holdings_master"
HEADERS = {'User-Agent': 'YourCompany/1.0 (contact@domain.com)'}

client = bigquery.Client(project=PROJECT_ID)
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# --- NEW: THE MISSING PARSER ---

def parse_xml_to_holdings(xml_content, acc_num, filing_date):
    """
    Extracts data using namespace-agnostic XPaths.
    This fixes the 'Zero Holdings' bug found in your CSV.
    """
    try:
        tree = etree.fromstring(xml_content.encode('utf-8'))
        # local-name() catches <infoTable>, <ns1:infoTable>, <n1:infoTable>, etc.
        nodes = tree.xpath('//*[local-name()="infoTable"]')
        
        extracted_data = []
        for node in nodes:
            def get_val(tag):
                # Search for the tag inside the current infoTable node
                res = node.xpath(f'./*[local-name()="{tag}"]/text()')
                return res[0] if res else None

            extracted_data.append({
                "accession_number": acc_num,
                "filing_date": filing_date,
                "nameOfIssuer": get_val("nameOfIssuer"),
                "cusip": get_val("cusip"),
                "value": float(get_val("value") or 0),
                "sshPrnamt": float(get_val("sshPrnamt") or 0),
                "sshPrnamtType": get_val("sshPrnamtType"),
                "investmentDiscretion": get_val("investmentDiscretion")
            })
        return extracted_data
    except Exception as e:
        logger.error(f"❌ Parser failure for {acc_num}: {e}")
        return []

# --- THE 3-STAGE PROCESSOR ---

def process_batch(year, qtr, limit=25):
    query = f"""
        SELECT * FROM `{QUEUE_TABLE}`
        WHERE (status = 'pending' OR status = 'error_data')
        AND year = {year} AND qtr = {qtr}
        LIMIT {limit}
    """
    df = client.query(query).to_dataframe()
    if df.empty: return False

    all_holdings = []
    
    with requests.Session() as session:
        session.headers.update(HEADERS)
        for _, row in df.iterrows():
            acc_num = row['accession_number']
            try:
                # 1. Get index.json to find the XML filename
                time.sleep(0.12) # SEC Compliance
                dir_res = session.get(row['dir_url'], timeout=10)
                dir_res.raise_for_status()
                
                # Look for the information table XML
                items = dir_res.json().get('directory', {}).get('item', [])
                xml_name = next((i['name'] for i in items if 'infotable' in i['name'].lower() and i['name'].endswith('.xml')), None)
                
                if not xml_name:
                    # Fallback for older filings or different naming
                    xml_name = next((i['name'] for i in items if i['name'].endswith('.xml')), None)

                if xml_name:
                    # 2. Fetch the actual XML content
                    xml_url = row['dir_url'].replace('index.json', xml_name)
                    xml_res = session.get(xml_url, timeout=15)
                    
                    # 3. PARSE (The missing piece)
                    holdings = parse_xml_to_holdings(xml_res.text, acc_num, "2020-06-30")
                    
                    if holdings:
                        all_holdings.extend(holdings)
                        # Success update
                        client.query(f"UPDATE `{QUEUE_TABLE}` SET status='done', retries=0 WHERE accession_number='{acc_num}'")
                        logger.info(f"✔️ {row['company_name']}: {len(holdings)} rows found.")
                    else:
                        raise Exception("XML found but 0 holdings extracted (Namespace mismatch?)")
                else:
                    raise Exception("No infoTable XML found in directory")

            except Exception as e:
                logger.error(f"⚠️ Failed {row['company_name']}: {e}")
                # Update status with error info (using our parameterized logic from before)
                # ... update_queue_status() ...

    # 4. BULK UPLOAD TO BIGQUERY
    if all_holdings:
        client.load_table_from_json(all_holdings, MASTER_TABLE).result()
    
    return True

if __name__ == "__main__":
    while process_batch(2020, 1):
        logger.info("🚀 Batch complete. Restarting...")