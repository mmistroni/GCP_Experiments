import json
import re
import os
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

def automated_trump_scraper():
    chrome_options = Options()
    
    # Absolute minimum flags required for secure container spaces
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
    
    # Path to the zero-dependency Codespace headless shell engine
    local_shell_path = os.path.abspath("./chrome_binary/chrome-headless-shell-linux64/chrome-headless-shell")

    # --- ENHANCED ENVIRONMENT DETECTION LOGIC ---
    if os.path.exists(local_shell_path):
        print(f"[INFO] Codespace Headless Shell Active: {local_shell_path}")
        chrome_options.binary_location = local_shell_path
        chrome_options.add_argument("--headless=old") # Required for pure headless shell binaries
        
        # Bypasses ChromeDriver entirely and interacts with the binary directly via CDP
        driver = webdriver.Chrome(options=chrome_options)
        
    else:
        print("[INFO] Fallback to Production System Infrastructure Paths.")
        chrome_options.add_argument("--headless=new") # Modern headless engine for full browsers
        
        if os.path.exists("/usr/bin/chromium"):
            chrome_options.binary_location = "/usr/bin/chromium"
        elif os.path.exists("/usr/bin/chromium-browser"):
            chrome_options.binary_location = "/usr/bin/chromium-browser"
            
        driver = webdriver.Chrome(options=chrome_options)
        
    url = "https://www.quiverquant.com/Donald-Trump-Stock-Trades/"
    
    try:
        driver.get(url)
        driver.implicitly_wait(10)
        html_content = driver.page_source
    except Exception as e:
        return json.dumps({"error": f"Selenium execution failed: {str(e)}"}, indent=4)
    finally:
        driver.quit()

    # --- PARSING LOGIC BLOCK (BeautifulSoup processing) ---
    soup = BeautifulSoup(html_content, "html.parser")
    table = soup.find("table", id="tradeTable")
    if not table:
        return json.dumps({"error": "Target table framework missing"}, indent=4)

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
            
            parsed_rows.append(row_data)

    return json.dumps(parsed_rows, indent=4)

if __name__ == "__main__":
    print(automated_trump_scraper())