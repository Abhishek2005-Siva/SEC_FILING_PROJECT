import pandas as pd
import requests
from datetime import datetime, timedelta
import streamlit as st
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

# ===== STREAMLIT UI CONFIG =====
st.set_page_config(page_title="SEC Filings Search", layout="wide")
st.title("SEC Filings Search")

# ===== CONFIGURATION =====
SEC_DELAY = 0.5
MAX_RETRIES = 3

# ===== COMPLETE LIST OF SEC FILING TYPES =====
ALL_FILING_TYPES = [
    '1', '1-A', '1-A POS', '1-A-W', '1-E', '1-E AD', '1-K', '1-SA', '1-U', '1-Z', '1-Z-W',
    '10-12B', '10-12G', '10-C', '10-D', '10-K', '10-K405', '10-KT', '10-M', '10-Q', '10-QT',
    # ... (keep all your existing filing types)
]

# ===== SEARCH FORM =====
with st.form(key='search_form'):
    col1, col2 = st.columns(2)
    
    with col1:
        # Document word or phrase
        st.text_input("Document word or phrase", key="search_phrase")
        
        # Keywords to search for
        st.text_input("Keywords to search for in filing documents", key="keywords")
        
        # Company search
        company_input = st.text_input(
            "Company name, ticker, CIK number or individual's name", 
            key="company"
        )
    
    with col2:
        # Filing types
        filing_types = st.multiselect(
            "Filing types",
            options=ALL_FILING_TYPES,
            default=['10-K', '10-Q', '8-K'],
            key="filing_types"
        )
        
        # Category filter (mocked)
        st.selectbox("Finance", ["All", "Banking", "Insurance"], key="finance_category")
        
        # Date range
        date_col1, date_col2 = st.columns(2)
        with date_col1:
            from_date = st.date_input("Filed from", key="from_date")
        with date_col2:
            to_date = st.date_input("to", key="to_date")
        
        # Location filter (mocked)
        st.selectbox("Principal executive offices in", ["All", "Delaware", "California"], key="location")

    # Form buttons
    submit_col1, submit_col2, _ = st.columns([1,1,4])
    with submit_col1:
        search_button = st.form_submit_button("SEARCH")
    with submit_col2:
        st.form_submit_button("Clear all")

# ===== SEC CLIENT =====
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
            st.warning(f"⚠️ SEC access error: {str(e)[:100]}")
            return None
        except Exception as e:
            st.warning(f"⚠️ Network error: {str(e)[:100]}")
            return None

# ===== HELPER FUNCTIONS =====
def make_clickable(url):
    return f'<a target="_blank" href="{url}">View Filing</a>'

def build_filing_url(cik, filename):
    accession_with_hyphens = filename.split("/")[-1].replace(".txt", "")
    accession_no_hyphens = accession_with_hyphens.replace("-", "")
    return f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_no_hyphens}/{accession_with_hyphens}-index.html"

# ===== MAIN SEARCH LOGIC =====
if search_button:
    with st.spinner("Searching SEC filings..."):
        client = SECClient()
        all_filings = []
        
        # Generate date range
        delta = to_date - from_date
        date_range_days = [from_date + timedelta(days=i) for i in range(delta.days + 1)]
        
        for day in date_range_days:
            year = day.year
            qtr = (day.month - 1) // 3 + 1
            date_str = day.strftime('%Y%m%d')
            url = f"https://www.sec.gov/Archives/edgar/daily-index/{year}/QTR{qtr}/master.{date_str}.idx"
            
            content = client.get_master_index(url)
            if not content:
                continue
                
            # Process filings
            lines = content.splitlines()
            data_start_index = next((i for i, line in enumerate(lines) if line.startswith('-------')), 0) + 1
            
            for line in lines[data_start_index:]:
                parts = line.strip().split('|')
                if len(parts) != 5:
                    continue
                
                try:
                    cik, name, form, date_filed, filename = parts
                    form_upper = form.strip().upper()
                    
                    # Check if form matches selected types
                    if not any(form_upper.startswith(ft) for ft in filing_types):
                        continue
                        
                    filing_url = build_filing_url(cik.strip(), filename.strip())
                    all_filings.append({
                        "Company": name,
                        "Ticker": "",  # Will be filled later
                        "Form": form.strip(),
                        "Filed": date_filed.strip(),
                        "Filing": filing_url
                    })
                except Exception as e:
                    st.warning(f"Error processing filing: {str(e)[:100]}")
            
            time.sleep(SEC_DELAY)

        # Display results
        if all_filings:
            df = pd.DataFrame(all_filings)
            
            # Make URLs clickable
            df['Filing'] = df['Filing'].apply(make_clickable)
            
            # Display as HTML to render links
            st.write(df.to_html(escape=False, index=False), unsafe_allow_html=True)
            
            # Download button
            csv = df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="Download Results",
                data=csv,
                file_name=f"sec_filings_{from_date}_to_{to_date}.csv",
                mime='text/csv'
            )
        else:
            st.warning("No filings found matching your criteria")
