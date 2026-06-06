import json
import re
import os
import logging
from datetime import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from google.cloud import bigquery

# Configure system logging structure for seamless Cloud Logging integration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def parse_date_string(date_str):
    """
    Safely converts strings like 'May 13, 2026' to '2026-05-13'.
    Returns None if empty, missing, or improperly formatted.
    """
    if not date_str or date_str in ["N/A", "-", ""]:
        return None
    try:
        return datetime.strptime(date_str.strip(), "%b %d, %Y").strftime("%Y-%m-%d")
    except Exception as e:
        logger.warning(f"Failed to format string date value '{date_str}': {str(e)}")
        return None

def upsert_to_bigquery(clean_rows):
    """
    Loads parsed data rows into a temporary staging area and performs an
    idempotent, cost-pruned MERGE operation into the production dataset.
    """
    target_table = "datascience-projects.gcp_shareloader.trump_trades"
    staging_table = "datascience-projects.gcp_shareloader.tmp_trump_trades_staging"
    
    # Prune rows missing a valid execution date to protect partitioning boundaries
    rows_to_insert = [r for r in clean_rows if r["Traded"] is not None]
    if not rows_to_insert:
        logger.warning("No rows with valid transaction dates ('Traded') found. Skipping BigQuery ingestion.")
        return

    client = bigquery.Client()
    
    # Dynamically find the oldest execution date in this batch to enforce partition elimination
    min_trade_date = min(r["Traded"] for r in rows_to_insert)
    logger.info(f"Ingestion batch span check: oldest trade detected is {min_trade_date}. Setting partition fence.")

    # 1. Stage the fresh batch into a temporary single-use scratchpad table
    logger.info(f"Streaming {len(rows_to_insert)} items into volatile staging layer: {staging_table}")
    load_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    
    load_job = client.load_table_from_json(rows_to_insert, staging_table, job_config=load_config)
    load_job.result()  # Blocks until execution thread synchronizes

    # 2. Execute target merge using the min_trade_date condition to eliminate unneeded partition scans
    merge_sql = f"""
    MERGE `{target_table}` T
    USING `{staging_table}` S
    ON T.Traded >= '{min_trade_date}'
       AND T.Traded = S.Traded
       AND T.Ticker = S.Ticker
       AND T.Transaction = S.Transaction
    WHEN MATCHED THEN
      UPDATE SET 
        T.Company = S.Company,
        T.Amount_Range = S.Amount_Range,
        T.Filed = S.Filed,
        T.Excess_Return = S.Excess_Return,
        T.Scraped_At = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
      INSERT (Ticker, Company, Transaction, Amount_Range, Filed, Traded, Excess_Return, Scraped_At)
      VALUES (S.Ticker, S.Company, S.Transaction, S.Amount_Range, S.Filed, S.Traded, S.Excess_Return, CURRENT_TIMESTAMP());
    """
    
    try:
        logger.info("Executing cost-optimized partition-pruned MERGE query transaction...")
        query_job = client.query(merge_sql)
        query_job.result()
        logger.info("BigQuery merge completed successfully.")
    finally:
        # Housekeeping: Ensure scratchpad tables are always dropped to keep the dataset clean
        logger.info("Dropping transient staging table references...")
        client.delete_table(staging_table, not_found_ok=True)

def automated_trump_scraper():
    chrome_options = Options()
    
    # Mandatory low-overhead sandbox settings for container execution spaces
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
    
    # Path to the zero-dependency Codespace headless shell engine
    local_shell_path = os.path.abspath("./chrome_binary/chrome-headless-shell-linux64/chrome-headless-shell")

    # --- ENVIRONMENT DETECTION LOGIC ---
    if os.path.exists(local_shell_path):
        logger.info(f"Codespace Active. Bypassing driver managers with shell execution path: {local_shell_path}")
        chrome_options.binary_location = local_shell_path
        chrome_options.add_argument("--headless=old")
        driver = webdriver.Chrome(service=Service(executable_path=local_shell_path), options=chrome_options)
    else:
        logger.info("Fallback to Production System Infrastructure Paths.")
        chrome_options.add_argument("--headless=new")
        
        if os.path.exists("/usr/bin/chromium"):
            chrome_options.binary_location = "/usr/bin/chromium"
        elif os.path.exists("/usr/bin/chromium-browser"):
            chrome_options.binary_location = "/usr/bin/chromium-browser"
            
        driver = webdriver.Chrome(options=chrome_options)
        
    url = "https://www.quiverquant.com/Donald-Trump-Stock-Trades/"
    
    try:
        logger.info(f"Opening connection target: {url}")
        driver.get(url)
        driver.implicitly_wait(10)
        html_content = driver.page_source
    except Exception as e:
        logger.error(f"Selenium execution context failed to extract DOM payload: {str(e)}")
        raise e
    finally:
        driver.quit()

    # --- PARSING ENGINE LAYER (BeautifulSoup processing) ---
    soup = BeautifulSoup(html_content, "html.parser")
    table = soup.find("table", id="tradeTable")
    if not table:
        logger.error("Target data table layout ('#tradeTable') missing from DOM structure. Page structure may have shifted.")
        raise ValueError("Scraper extraction target node missing.")

    headers = []
    thead = table.find("thead")
    if thead:
        for th in thead.find_all("th"):
            header_span = th.find("span", class_="header-name")
            headers.append(header_span.text.strip() if header_span else th.text.strip())

    if not headers:
        headers = ["Stock", "Transaction", "Filed", "Traded", "Excess Return"]

    parsed_rows = []
    tbody = table.find("tbody")
    if tbody:
        for row in tbody.find_all("tr"):
            tds = row.find_all("td")
            if not tds:
                continue
                
            row_data = {
                "Ticker": "N/A", 
                "Company": "N/A", 
                "Transaction": "N/A", 
                "Amount_Range": "N/A", 
                "Filed": "N/A", 
                "Traded": "N/A", 
                "Excess Return": "N/A"
            }
            
            for index, td in enumerate(tds):
                if index < len(headers):
                    header_name = headers[index]
                    text_content = td.text.strip()
                    
                    if header_name == "Stock":
                        ticker_span = td.find("span", class_="ticker-text")
                        if ticker_span:
                            ticker = ticker_span.text.strip()
                            row_data["Ticker"] = ticker
                            row_data["Company"] = text_content.replace(ticker, "", 1).strip()
                        else:
                            match = re.match(r"^([A-Z1-9\-]{1,6})(.*)", text_content)
                            if match:
                                row_data["Ticker"] = match.group(1).strip()
                                row_data["Company"] = match.group(2).strip(" -")
                            else:
                                row_data["Ticker"] = text_content
                                
                    elif header_name == "Transaction":
                        if "Purchase" in text_content:
                            row_data["Transaction"] = "Purchase"
                            row_data["Amount_Range"] = text_content.replace("Purchase", "").strip()
                        elif "Sale" in text_content:
                            row_data["Transaction"] = "Sale"
                            row_data["Amount_Range"] = text_content.replace("Sale", "").strip()
                        else:
                            row_data["Transaction"] = text_content
                            
                    elif header_name == "Filed":
                        row_data["Filed"] = text_content
                    elif header_name == "Traded":
                        row_data["Traded"] = text_content
                    elif header_name == "Excess Return":
                        row_data["Excess Return"] = text_content
            
            # Map into the final structured dictionary formatting string targets into clear BigQuery types
            parsed_rows.append({
                "Ticker": row_data["Ticker"],
                "Company": row_data["Company"],
                "Transaction": row_data["Transaction"],
                "Amount_Range": row_data["Amount_Range"],
                "Filed": parse_date_string(row_data["Filed"]),
                "Traded": parse_date_string(row_data["Traded"]),
                "Excess_Return": row_data["Excess Return"]
            })

    logger.info(f"Successfully compiled {len(parsed_rows)} raw records from HTML source structure.")
    
    # 3. Fire the database layer pipeline
    if parsed_rows:
        upsert_to_bigquery(parsed_rows)
    else:
        logger.warning("Scraper successfully ran but extracted 0 lines of data elements.")

    return json.dumps(parsed_rows, indent=4)

if __name__ == "__main__":
    try:
        automated_trump_scraper()
    except Exception as global_err:
        logger.critical(f"Uncaught pipeline crash error tracking thrown to entry point: {str(global_err)}")
        exit(1)