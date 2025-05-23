import pandas as pd
import requests
from datetime import datetime, timedelta
import time
import streamlit as st
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import re

# ===== STATE NAME MAPPING =====
STATE_ABBREV = {
    'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas', 'CA': 'California',
    'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware', 'FL': 'Florida', 'GA': 'Georgia',
    'HI': 'Hawaii', 'ID': 'Idaho', 'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa',
    'KS': 'Kansas', 'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland',
    'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi', 'MO': 'Missouri',
    'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada', 'NH': 'New Hampshire', 'NJ': 'New Jersey',
    'NM': 'New Mexico', 'NY': 'New York', 'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio',
    'OK': 'Oklahoma', 'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina',
    'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah', 'VT': 'Vermont',
    'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia', 'WI': 'Wisconsin', 'WY': 'Wyoming'
}

# ===== STREAMLIT UI CONFIG =====
st.set_page_config(page_title="SEC Filings Scraper", layout="wide")
st.title("📈 SEC Filings Scraper")

# ===== CONFIGURATION =====
SEC_DELAY = 0.5
MAX_RETRIES = 3
SEC_API_URL = "https://efts.sec.gov/LATEST/search-index"

# Common filing types and groups (same as original)
COMMON_FILING_TYPES = ['10-K', '10-Q', '8-K', 'DEF 14A', 'S-1', 'S-3', 'F-1', '20-F']
CUSTOM_FILING_GROUPS = {
    "Non-mgt(custom)": ["DEFC14A", "DEFC14C", "DEFN14A", "DEFR14A", "DFAN14A", 
                       "DFRN14A", "PREC14A", "PREC14C", "PREN14A", "PRER14A", "PRRN14A"],
    "13D(custom)": ["SC 13D", "SCHEDULE 13D"],
    "13G(custom)": ["SC 13G", "SCHEDULE 13G"]
}
ALL_FILING_OPTIONS = list(CUSTOM_FILING_GROUPS.keys()) + COMMON_FILING_TYPES

# ===== STREAMLIT UI ELEMENTS =====
with st.sidebar:
    st.header("Search Parameters")
    
    # Document word/phrase input
    doc_search = st.text_input(
        "Document word or phrase:",
        help="Search for specific text within filings"
    )
    
    # Entity search input
    entity_search = st.text_input(
        "Company name, ticker, CIK number or individual's name:",
        help="Filter by specific company or individual"
    )
    
    # Date Range Selector
    date_range = st.date_input("Select Date Range", [])
    from_date, to_date = (date_range if len(date_range) == 2 else 
                        (datetime.now().date() - timedelta(days=1), 
                         datetime.now().date()))
    
    st.subheader("Filing Type Selection")
    selected_options = st.multiselect(
        "Select filing types or groups:",
        options=ALL_FILING_OPTIONS,
        default=['10-K'],
        help="Select individual filing types or predefined groups"
    )
    
    custom_type = st.text_input(
        "Or enter custom filing type(s):",
        help="Example: '10-K, 10-Q, 8-K'"
    )
    
    show_details = st.checkbox(
        "Show Location and Incorporation Details (Slower)",
        value=True,
        help="Toggle to show/hide location and incorporation columns"
    )
    
    run_button = st.button("Fetch Filings")

# ===== SEC CLIENT CLASS =====
class SECAPIClient:
    def __init__(self, show_details):
        self.session = requests.Session()
        retry = Retry(total=MAX_RETRIES, backoff_factor=0.5, 
                     status_forcelist=[500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount('https://', adapter)
        self.headers = {
            "User-Agent": "Your Name your.email@example.com",
            "Accept-Encoding": "gzip, deflate"
        }
        self.company_cache = {}
        self.show_details = show_details

    def search_filings(self, params):
        try:
            response = self.session.get(
                SEC_API_URL,
                params=params,
                headers=self.headers,
                timeout=10
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            st.warning(f"⚠️ API Error: {str(e)[:100]}")
            return None

    def get_company_details(self, cik):
        if cik in self.company_cache:
            return self.company_cache[cik]
        
        try:
            if not self.show_details:
                return {"entity_name": "", "location": "", "incorporated": ""}
                
            url = f"https://data.sec.gov/submissions/CIK{cik.zfill(10)}.json"
            response = self.session.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            business_address = data.get("addresses", {}).get("business", {})
            location = f"{business_address.get('city', '')}, " \
                      f"{STATE_ABBREV.get(business_address.get('stateOrCountry', ''), '')}"
            
            incorporated = data.get("stateOfIncorporation", 
                                  data.get("incorporationState", ""))
            incorporated = STATE_ABBREV.get(incorporated.upper(), incorporated)
            
            details = {
                "entity_name": data.get("name", ""),
                "location": location,
                "incorporated": incorporated
            }
            
            self.company_cache[cik] = details
            return details
        except Exception as e:
            st.warning(f"⚠️ Failed to fetch details for CIK {cik}: {str(e)[:100]}")
            return {"entity_name": "", "location": "", "incorporated": ""}

# ===== HELPER FUNCTIONS =====
def extract_ticker(entity_name):
    """Extract ticker from entity name (if present in brackets)"""
    match = re.search(r"\(([^)]+)\)", entity_name)
    if match and "CIK" not in match.group(1).split(',')[0].strip():
        return match.group(1).split(',')[0].strip()
    return ""

def expand_filing_types(selected_options):
    filing_types = []
    for option in selected_options:
        if option in CUSTOM_FILING_GROUPS:
            filing_types.extend(CUSTOM_FILING_GROUPS[option])
        else:
            filing_types.append(option)
    return filing_types

def build_filing_url(cik, accession_number):
    """Construct proper SEC filing URL using adsh from API response"""
    try:
        if not accession_number:
            return "URL unavailable"
        
        # Create directory part (accession number without hyphens)
        directory = accession_number.replace("-", "")
        
        # Create filename with original hyphens + index suffix
        filename = f"{accession_number}-index.htm"  # Changed to .htm
        
        return f"https://www.sec.gov/Archives/edgar/data/{cik}/{directory}/{filename}"
    except Exception as e:
        st.warning(f"URL generation error: {str(e)}")
        return "Invalid URL"

def make_clickable(url):
    return f'<a target="_blank" href="{url}" style="text-decoration: none;">🔗 View Filing</a>'

# ===== MAIN PROCESSING =====
if run_button and len(date_range) == 2:
    # Prepare filing types
    filing_types = expand_filing_types(selected_options)
    if custom_type:
        filing_types.extend([x.strip().upper() for x in custom_type.split(',') if x.strip()])
    
    if not filing_types:
        st.error("Please select at least one filing type")
        st.stop()

    with st.spinner("Initializing SEC API Client..."):
        client = SECAPIClient(show_details)
        all_filings = []
        page = 1
        total_filings = 0
        has_more = True

        # Initialize progress
        progress_bar = st.progress(0)
        status_text = st.empty()
        start_time = time.time()

        while has_more:
            params = {
                "dateRange": "custom",
                "startdt": from_date.strftime("%Y-%m-%d"),
                "enddt": to_date.strftime("%Y-%m-%d"),
                "forms": ",".join(filing_types),
                "page": page,
                "from": (page-1)*100
            }
            
            # Add search parameters if provided
            if doc_search:
                params["q"] = doc_search
            if entity_search:
                params["entityName"] = entity_search

            result = client.search_filings(params)
            if not result or "hits" not in result:
                break

            total_filings = result["hits"]["total"]["value"]
            hits = result["hits"]["hits"]
            
            if not hits:
                has_more = False
                break

            # In the main processing loop:
            for hit in hits:
                try:
                    source = hit["_source"]
                    cik = str(source["ciks"][0])
                    
                    # Safely get form with fallback
                    form = source.get("form", "N/A")
                    
                    # Get date with validation
                    date_filed = source.get("file_date", "Unknown date")
                    
                    # Get hit ID with fallback
                    hit_id = hit.get("_id", "")
                    accession_number = source.get("adsh", "")
                    
                    # Build filing URL
                    filing_url = build_filing_url(cik, accession_number) 
                    
                    # Get company details
                    details = client.get_company_details(cik)
                    entity_name =source.get("display_names", ["N/A"])[0]
                    ticker = extract_ticker(entity_name)
                    print(ticker)
                    #print(ticker)
                    clean_entity = re.sub(r"\s*\([A-Z]+\)", "", entity_name)
                    cleaned = re.sub(r"\s*\([^)]*\)", "", source.get("display_names", ["N/A"])[0])
                    
                    all_filings.append({
                        "Form": form,
                        "Date Filed": date_filed,
                        "Ticker": ticker,  # New column
                        "Filing Entity": cleaned,
                        "Filing Person": clean_entity,
                        "Located": details["location"],
                        "Incorporated": details["incorporated"],
                        "Filing": filing_url
                    })
                    
                except Exception as e:
                    st.warning(f"Error processing filing: {str(e)}")
                    continue

            # Update progress
            progress = min(page * 100 / total_filings, 1.0) if total_filings > 0 else 1.0
            progress_bar.progress(progress)
            
            elapsed_time = time.time() - start_time
            status_text.text(
                f"Processed {page} pages ({len(all_filings)} filings)\n"
                f"Elapsed: {timedelta(seconds=int(elapsed_time))}"
            )
            
            page += 1
            time.sleep(SEC_DELAY)

        # Cleanup progress elements
        progress_bar.empty()
        status_text.empty()

        if all_filings:
            df = pd.DataFrame(all_filings)
            df['Date Filed'] = pd.to_datetime(df['Date Filed'])
            df['Filing'] = df['Filing'].apply(make_clickable)
            
            # Final columns setup
            final_columns = ['Form', 'Date Filed', 'Ticker','Filing Entity']
            if show_details:
                final_columns.extend(['Filing Person','Located', 'Incorporated'])
            final_columns.append('Filing')

            st.success(f"✅ Found {len(df)} filings")
            st.write(df[final_columns].to_html(escape=False, index=False), unsafe_allow_html=True)
            
            # CSV download
            csv_df = df[[c for c in final_columns if c != 'Filing']]
            csv = csv_df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="Download as CSV",
                data=csv,
                file_name=f"SEC_filings_{from_date}_to_{to_date}.csv",
                mime='text/csv'
            )
        else:
            st.warning("⚠️ No filings found matching your criteria")
