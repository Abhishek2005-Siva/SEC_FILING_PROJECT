import requests
import pandas as pd
from tqdm import tqdm
from bs4 import BeautifulSoup
import time
import gspread
from google.oauth2.service_account import Credentials
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ===== CONFIGURATION =====
TEST_MODE = False        # Set to False for full processing
MAX_TEST_ROWS = 5       # Number of rows to process in test mode
SEC_DELAY = 0.2         # Delay between SEC requests (seconds)
INPUT_EXCEL = "filings_limited.xlsx"  # Input Excel file path

# Google Sheets Config
SERVICE_ACCOUNT_JSON = 'filing-project-460314-d0000baa21a5.json'
SPREADSHEET_ID = '1Sd9R2vESUHjhR-CLYQh5J2EdelbIftsQ7F_QRELhdmI'
SHEET_NAME = 'sheet'

# SEC Headers (must contain valid contact info)
SEC_HEADERS = {
    'User-Agent': 'Abhishek Kumar, DataScriptBot/1.0 (contact: abhishek2005.siva@example.com)'
}

# ===== RETRY-ENABLED SESSION =====
def create_retry_session():
    session = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount('https://', adapter)
    session.mount('http://', adapter)
    return session

session = create_retry_session()

# ===== CORE FUNCTIONS =====
def get_content_length(url):
    """Safely retrieve content length for a given URL"""
    try:
        res = session.head(url, headers=SEC_HEADERS, timeout=5, allow_redirects=True)
        if res.status_code == 200:
            return int(res.headers.get('content-length', 0))
    except:
        pass
    return 0

def find_filing_html(filing_url, file_type):
    """Find the filing document link from a SEC filing index page with multiple fallback methods"""
    try:
        res = session.get(filing_url, headers=SEC_HEADERS, timeout=10)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, 'html.parser')

        # Method 1: Standard table lookup (most common)
        table = soup.find('table', class_='tableFile')
        if table:
            rows = table.find_all('tr')[1:]  # Skip header
            for row in rows:
                cols = row.find_all('td')
                if len(cols) >= 4:
                    doc_type = cols[3].text.strip().upper()
                    doc_href = cols[2].find('a')['href'] if cols[2].find('a') else None
                    
                    # Flexible type matching (e.g., "10-K" matches "10-K/A")
                    if doc_href and file_type.upper() in doc_type:
                        full_url = f"https://www.sec.gov{doc_href}" if not doc_href.startswith('http') else doc_href
                        return full_url

        # Method 2: Alternative table structure
        tables = soup.find_all('table')
        for table in tables:
            for row in table.find_all('tr'):
                cols = row.find_all('td')
                if len(cols) >= 2:  # More lenient column check
                    first_col = cols[0].text.strip().upper()
                    if file_type.upper() in first_col:
                        link = cols[0].find('a')
                        if link:
                            href = link['href']
                            full_url = f"https://www.sec.gov{href}" if not href.startswith('http') else href
                            return full_url

        # Method 3: Direct document links in the page
        for link in soup.find_all('a', href=True):
            href = link['href']
            if (file_type.lower() in href.lower() or 
                file_type.upper() in link.text.upper()):
                full_url = f"https://www.sec.gov{href}" if not href.startswith('http') else href
                if full_url.endswith(('.htm', '.html')):  # Prefer HTML files
                    return full_url
                elif not full_url.endswith(('.txt', '.xml')):  # Accept non-text formats
                    return full_url

        print(f"⚠️ Found index page but no {file_type} document at {filing_url}")
        return None

    except Exception as e:
        print(f"⚠️ Error processing {filing_url}: {str(e)}")
        return None

def update_google_sheet(data, file_type):
    """Update Google Sheet with the results"""
    

    try:
        gc = gspread.service_account(SERVICE_ACCOUNT_JSON)
        sh = gc.open_by_key(SPREADSHEET_ID)

        try:
            worksheet = sh.worksheet(SHEET_NAME)
        except gspread.WorksheetNotFound:
            worksheet = sh.add_worksheet(SHEET_NAME, rows=100, cols=3)
        
        # Prepare data with dynamic header based on file_type
        rows = [[f'Company Name', 'Ticker', f'{file_type} HTML Link']]
        rows.extend([[item['name'], item['ticker'], item['html_link']] for item in data])
        
        # Clear & Update
        worksheet.clear()
        worksheet.update('A1', rows)
        
        return True
        
    except Exception as e:
        print(f"❌ Google Sheets error: {str(e)}")
        return False

# ===== MAIN EXECUTION =====
def p_r(file_type):
    print(f"\n=== SEC {file_type} Filing Processor ===")
    print(f"Mode: {'TEST' if TEST_MODE else 'PRODUCTION'}")
    
    try:
        df = pd.read_excel(file_type+"_"+INPUT_EXCEL)
        if TEST_MODE:
            df = df.head(MAX_TEST_ROWS)
            print(f"\n🔹 Processing first {MAX_TEST_ROWS} rows for testing")
        
        # ✅ Convert NaNs to empty strings to prevent JSON serialization issues
        df = df.fillna('')

    except Exception as e:
        print(f"❌ Failed to load input file: {str(e)}")
        return

    results = []
    print(f"\n⏳ Processing {file_type} filings...")
    for _, row in tqdm(df.iterrows(), total=len(df)):
        if html_url := find_filing_html(row['Filing URL'], file_type):
            results.append({
                'name': row['Company Name'],
                'ticker': row['Ticker'],  # Changed from CIK to Ticker to match Excel column
                'html_link': html_url
            })

        time.sleep(SEC_DELAY)
    
    if not results:
        print(f"\n❌ No valid {file_type} filings found")
        return
    
    print(f"\n✅ Found {len(results)} valid {file_type} filings")
    print("\nSample results:")
    for i, item in enumerate(results[:3], 1):
        print(f"{i}. {item['name']} ({item['ticker']}): {item['html_link']}")
    
    if update_google_sheet(results, file_type):
        print(f"\n💾 Successfully updated Google Sheet: {SHEET_NAME}")
    else:
        print("\n❌ Failed to update Google Sheet")
