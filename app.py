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
    'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia', 'WI': 'Wisconsin', 'WY': 'Wyoming',
    'I0': 'Iowa',  # Assuming I0 was meant to be Iowa (IA)
    'E9': 'Delaware',  # Common DE alternative
    'C0': 'Colorado',  # Common CO typo
    'M5': 'Michigan',  # Common MI alternative
    'G4': 'Georgia',  # Common GA alternative
    'O5': 'Ohio',     # Common OH typo
}

# ===== STREAMLIT UI CONFIG =====
st.set_page_config(page_title="SEC Filings Scraper", layout="wide")
st.title("📈 SEC Filings Scraper")

# ===== CONFIGURATION =====
SEC_DELAY = 0.5
MAX_RETRIES = 3

# Common filing types for user selection
COMMON_FILING_TYPES = [
    '10-K', '10-Q', '8-K', 'DEF 14A', 
    'S-1', 'S-3', 'F-1', '20-F', 
    'SCHEDULE 13D', 'SCHEDULE 13G'
]

# Predefined custom filing type groups
CUSTOM_FILING_GROUPS = {
    "non-mgt": ["DEFC14A", "DEFC14C", "DEFN14A", "DEFR14A", "DFAN14A", 
                "DFRN14A", "PREC14A", "PREC14C", "PREN14A", "PRER14A", "PRRN14A"],
    "13d": ["SC 13D", "SCHEDULE 13D"],
    "13g": ["SC 13G", "SCHEDULE 13G"]
}

# Combine all options for the multiselect
ALL_FILING_OPTIONS = COMMON_FILING_TYPES + list(CUSTOM_FILING_GROUPS.keys())

# ===== STREAMLIT UI ELEMENTS =====
with st.sidebar:
    st.header("Search Parameters")

    # Date Range Selector
    date_range = st.date_input("Select Date Range", [])
    from_date, to_date = (date_range if len(date_range) == 2 else 
                         (datetime.now().date() - timedelta(days=1), 
                          datetime.now().date() - timedelta(days=1)))

    # Filing Type Selection
    st.subheader("Filing Type Selection")
    selected_types = st.multiselect(
        "Select from common filing types:",
        options=COMMON_FILING_TYPES,
        default=['10-K']
    )
    
    # Custom filing types input
    custom_type = st.text_input(
        "Enter custom filing type(s) or group names:",
        help="Example: '10-K, 10-Q, 8-K' or 'non-mgt, 13d'"
    )
    
    # Add the checkbox for location/incorporation columns
    show_details = st.checkbox(
        "Show Location and Incorporation Details",
        value=True,
        help="Toggle to show/hide location and incorporation columns"
    )
    
    run_button = st.button("Fetch Filings")

def clean_incorporation_code(code):
    """Clean and normalize incorporation codes"""
    # Remove numbers and special characters
    cleaned = re.sub(r'[^A-Za-z]', '', code).upper()
    
    # Check for common length issues
    if len(cleaned) > 2:
        return cleaned[:2]
    
    # Return cleaned code if not empty
    return cleaned or code

# ===== SEC CLIENT CLASS =====
class SECClient:
    def __init__(self, show_details):
        self.session = requests.Session()
        retry = Retry(total=MAX_RETRIES, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount('https://', adapter)
        self.headers = {
            "User-Agent": "Your Name your.email@example.com",
            "Accept-Encoding": "gzip, deflate"
        }
        self.company_cache = {}
        self.show_details = show_details

    def get_master_index(self, url):
        try:
            response = self.session.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()
            return response.text
        except requests.exceptions.HTTPError as e:
            st.warning(f"⚠️ HTTP Error: {str(e)[:100]}")
            return None
        except Exception as e:
            st.warning(f"⚠️ Network Error: {str(e)[:100]}")
            return None
    
    def get_company_details(self, cik):
        if cik in self.company_cache:
            return self.company_cache[cik]
        
        try:
            if not self.show_details:
                return {
                    "entity_name": "",
                    "location": "",
                    "incorporated": ""
                }
                
            url = f"https://data.sec.gov/submissions/CIK{cik.zfill(10)}.json"
            response = self.session.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            business_address = data.get("addresses", {}).get("business", {})
            location = f"{business_address.get('city', '')}, {STATE_ABBREV.get(business_address.get('stateOrCountry', ''), business_address.get('stateOrCountry', ''))}"
            
            incorporated = data.get("stateOfIncorporation", 
                          data.get("incorporationState", 
                          data.get("incorporated", {}).get("state", "")))
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
            return {
                "entity_name": "",
                "location": "",
                "incorporated": ""
            }

# ===== PROCESSING FUNCTIONS =====
def build_filing_url(cik, filename):
    accession_with_hyphens = filename.split("/")[-1].replace(".txt", "")
    accession_no_hyphens = accession_with_hyphens.replace("-", "")
    return f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_no_hyphens}/{accession_with_hyphens}-index.html"

def get_filing_key(url):
    match = re.search(r'/data/\d+/(.+)$', url)
    return match.group(1) if match else url

def make_clickable(url):
    return f'<a target="_blank" href="{url}" style="display: inline-block; padding: 0.25em 0.5em; background-color: #f0f2f6; border-radius: 3px; text-decoration: none;">View Filing</a>'

def process_custom_input(custom_input):
    """Process custom input to handle both individual types and group names"""
    if not custom_input:
        return []
    
    types = []
    for item in [x.strip().lower() for x in custom_input.split(',') if x.strip()]:
        # Check if it matches any group name
        if item in CUSTOM_FILING_GROUPS:
            types.extend(CUSTOM_FILING_GROUPS[item])
        else:
            # Treat as individual filing type (convert to uppercase)
            types.append(item.upper())
    return types

# ===== MAIN EXECUTION =====
if run_button and len(date_range) == 2:
    # Start with selected types
    filing_types = selected_types.copy()
    
    # Process custom input (can include both individual types and group names)
    custom_types = process_custom_input(custom_type)
    filing_types.extend(custom_types)
    
    if not filing_types:
        st.error("Please select at least one filing type")
        st.stop()

    with st.spinner("Initializing..."):
        client = SECClient(show_details)
        filings_dict = {}
        delta = to_date - from_date
        date_range_days = [from_date + timedelta(days=i) for i in range(delta.days + 1)]
        total_days = len(date_range_days)
        
        # Initialize progress
        progress_bar = st.progress(0)
        status_text = st.empty()
        start_time = time.time()

        for day_idx, day in enumerate(date_range_days):
            # Update progress
            progress = (day_idx + 1) / total_days
            progress_bar.progress(progress)
            
            # Calculate time estimates
            elapsed_time = time.time() - start_time
            avg_time_per_day = elapsed_time / (day_idx + 1) if day_idx > 0 else 0
            remaining_time = avg_time_per_day * (total_days - day_idx - 1)
            
            status_text.text(
                f"Processing day {day.strftime('%Y-%m-%d')} "
                f"({day_idx+1}/{total_days})\n"
                f"Elapsed: {timedelta(seconds=int(elapsed_time))} • "
                f"Remaining: {timedelta(seconds=int(remaining_time)) if day_idx > 0 else 'Calculating...'}"
            )

            # Existing processing code for each day
            year = day.year
            qtr = (day.month - 1) // 3 + 1
            url = f"https://www.sec.gov/Archives/edgar/daily-index/{year}/QTR{qtr}/master.{day.strftime('%Y%m%d')}.idx"

            content = client.get_master_index(url)
            if not content:
                continue

            lines = content.splitlines()
            data_start_index = next((i for i, line in enumerate(lines) if line.startswith('-------')), 0) + 1

            for line in lines[data_start_index:]:
                parts = line.strip().split('|')
                if len(parts) != 5:
                    continue

                try:
                    cik, name, form, date_filed, filename = parts
                    form_upper = form.strip().upper()
                    
                    if not any(form_upper.startswith(ft) for ft in filing_types):
                        continue

                    filing_url = build_filing_url(cik.strip(), filename.strip())
                    filing_key = get_filing_key(filing_url)
                    
                    entity_details = client.get_company_details(cik.strip())
                    clean_name = name.strip()
                    
                    if filing_key in filings_dict:
                        existing = filings_dict[filing_key]
                        
                        # Handle entity names
                        current_entities = [e.strip() for e in existing["Filing Entity"].split(";") if e.strip()]
                        if entity_details["entity_name"] not in current_entities:
                            if len(current_entities) == 0:
                                existing["Filing Entity"] = entity_details["entity_name"]
                            else:
                                existing["Filing Person"] += f"; {entity_details['entity_name']}"
                        
                        # Handle filing person
                        current_persons = [p.strip() for p in existing["Filing Person"].split(";") if p.strip()]
                        if clean_name != entity_details["entity_name"] and clean_name not in current_persons:
                            existing["Filing Person"] += f"; {clean_name}"
                            
                        # Update locations and incorporation only if details are enabled
                        if show_details:
                            existing["Located"] += f"<br> {entity_details['location']}"
                            existing["Incorporated"] += f"<br> {entity_details['incorporated']}"

                    else:
                        filings_dict[filing_key] = {
                            "Form": form.strip(),
                            "Date Filed": date_filed.strip(),
                            "Filing Entity": entity_details["entity_name"],
                            "Filing Person": clean_name if clean_name != entity_details["entity_name"] else "",
                            "Located": entity_details["location"] if show_details else "",
                            "Incorporated": entity_details["incorporated"] if show_details else "",
                            "Filing": filing_url
                        }

                except Exception as e:
                    st.warning(f"Line processing error: {str(e)[:100]}")

            time.sleep(SEC_DELAY)

        # Clean up progress elements
        progress_bar.empty()
        status_text.empty()

        # Modify the final_columns section to:
        if filings_dict:
            df = pd.DataFrame(list(filings_dict.values()))
            df['Date Filed'] = pd.to_datetime(df['Date Filed'])
            df['Filing'] = df['Filing'].apply(make_clickable)

            # Clean up duplicate entries
            for col in ['Filing Entity', 'Filing Person', 'Located', 'Incorporated']:
                df[col] = df[col].apply(lambda x: '; '.join(sorted(set(str(x).split('; ')), key=str.lower)) if x else x)
                df[col] = df[col].str.replace(r'^; |; $', '', regex=True).str.replace('; ;+', '; ', regex=True)

            # Base columns
            final_columns = [
                'Form',
                'Date Filed',
                'Filing Entity',
                'Filing Person',
            ]
            
            # Add detail columns if enabled
            if show_details:
                final_columns.extend(['Located', 'Incorporated'])
            
            # Always add filing link column
            final_columns.append('Filing')

            st.success(f"✅ Found {len(df)} filings")
            st.write(df[final_columns].to_html(escape=False, index=False), unsafe_allow_html=True)
            
            # Prepare CSV without the Filing link column
            csv_columns = [col for col in final_columns if col != 'Filing']
            csv_df = df[csv_columns].fillna('')
            csv = csv_df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="Download as CSV",
                data=csv,
                file_name=f"SEC_filings_{from_date}_to_{to_date}.csv",
                mime='text/csv'
            )
        else:
            st.warning("⚠️ No filings found matching your criteria")
