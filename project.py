import pandas as pd
import requests
from datetime import datetime, timedelta
import re
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import streamlit as st

# ===== STREAMLIT UI CONFIG =====
st.set_page_config(page_title="SEC Filings Scraper", layout="wide")
st.title("📈 SEC Filings Scraper")

# ===== CONFIGURATION =====
SEC_DELAY = 0.5  # SEC recommends >= 0.5s between requests
MAX_RETRIES = 3
VALID_FILING_TYPES = ['10-K', '10-Q', '8-K', 'DEF 14A']

# ===== STREAMLIT UI ELEMENTS =====
with st.sidebar:
    st.header("Search Parameters")
    filing_type = st.selectbox("Filing Type", VALID_FILING_TYPES)
    date_range = st.date_input("Date Range", [])
    
    if len(date_range) == 2:
        from_date, to_date = date_range
    else:
        from_date = to_date = datetime.now().date() - timedelta(days=1)
    
    run_button = st.button("Fetch Filings")

# ===== SEC CLIENT (Same as Original) =====
class SECClient:
    def __init__(self):
        self.session = requests.Session()
        retry = Retry(
            total=MAX_RETRIES,
            backoff_factor=0.5,
            status_forcelist=[500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount('https://', adapter)
        self.headers = {
            "User-Agent": "Your Name your.email@example.com",
            "Accept-Encoding": "gzip, deflate"
        }

    def get_master_index(self, url):
        try:
            response = self.session.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()
            return response.text
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 403:
                st.warning(f"⚠️ SEC denied access: {url.split('/master.')[1]}")
            else:
                st.warning(f"⚠️ HTTP Error: {str(e)[:100]}")
            return None
        except Exception as e:
            st.warning(f"⚠️ Network Error: {str(e)[:100]}")
            return None

# ===== PROCESSING FUNCTIONS (Same as Original) =====
def build_filing_url(cik, filename):
    accession_with_hyphens = filename.split("/")[-1].replace(".txt", "")
    accession_no_hyphens = accession_with_hyphens.replace("-", "")
    return f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_no_hyphens}/{accession_with_hyphens}-index.html"

def get_cik_ticker_map():
    url = "https://www.sec.gov/files/company_tickers.json"
    try:
        response = requests.get(url, headers={"User-Agent": "Your Name your.email@example.com"})
        response.raise_for_status()
        data = response.json()
        return {str(v["cik_str"]).zfill(10): v["ticker"] for v in data.values()}
    except Exception as e:
        st.warning(f"⚠️ Failed to load CIK-to-Ticker mapping: {str(e)[:100]}")
        return {}

# ===== MAIN EXECUTION =====
if run_button and len(date_range) == 2:
    with st.spinner(f"Fetching {filing_type} filings from {from_date} to {to_date}..."):
        client = SECClient()
        all_filings = []
        cik_to_ticker = get_cik_ticker_map()

        delta = to_date - from_date
        date_range = [from_date + timedelta(days=i) for i in range(delta.days + 1)]

        for day in date_range:
            year = day.year
            qtr = (day.month - 1) // 3 + 1
            date_str = day.strftime('%Y%m%d')
            url = f"https://www.sec.gov/Archives/edgar/daily-index/{year}/QTR{qtr}/master.{date_str}.idx"

            content = client.get_master_index(url)
            if not content:
                continue

            lines = content.splitlines()
            data_start_index = 0
            for i, line in enumerate(lines):
                if line.startswith('-------'):
                    data_start_index = i + 1
                    break

            for line in lines[data_start_index:]:
                parts = line.strip().split('|')
                if len(parts) != 5:
                    continue

                try:
                    cik, name, form, date_filed, filename = parts
                    if not form.strip().upper().startswith(filing_type):
                        continue

                    ticker = cik_to_ticker.get(cik.strip().zfill(10), '')
                    all_filings.append({
                        "CIK": cik.strip(),
                        "Company Name": name,
                        "Ticker": ticker,
                        "Form Type": form.strip(),
                        "Date Filed": date_filed.strip(),
                        "Filing URL": build_filing_url(cik.strip(), filename.strip())
                    })
                except Exception as e:
                    st.warning(f"Line processing error: {str(e)[:100]}")

            time.sleep(SEC_DELAY)

        if all_filings:
            df = pd.DataFrame(all_filings)
            df['Date Filed'] = pd.to_datetime(df['Date Filed'])
            
            st.success(f"✅ Found {len(df)} filings")
            st.dataframe(df)
            
            csv = df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="Download as CSV",
                data=csv,
                file_name=f"SEC_{filing_type}_{from_date}_to_{to_date}.csv",
                mime='text/csv'
            )
        else:
            st.warning("⚠️ No filings found matching your criteria")