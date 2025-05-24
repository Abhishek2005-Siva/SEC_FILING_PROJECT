import pandas as pd
import requests
from datetime import datetime, timedelta
import time
import streamlit as st
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import re
from bs4 import BeautifulSoup
import hashlib
import sqlite3
from pathlib import Path

# ===== DATABASE SETUP =====
def init_db():
    conn = sqlite3.connect('users.db')
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS users (
            username TEXT PRIMARY KEY,
            password TEXT,
            email TEXT,
            full_name TEXT
        )
    ''')
    conn.commit()
    conn.close()

# ===== AUTHENTICATION FUNCTIONS =====
def make_hashes(password):
    return hashlib.sha256(str.encode(password)).hexdigest()

def check_hashes(password, hashed_text):
    return make_hashes(password) == hashed_text

def create_user(username, password, email, full_name):
    conn = sqlite3.connect('users.db')
    c = conn.cursor()
    c.execute('INSERT INTO users (username, password, email, full_name) VALUES (?,?,?,?)',
              (username, make_hashes(password), email, full_name))
    conn.commit()
    conn.close()

def login_user(username, password):
    conn = sqlite3.connect('users.db')
    c = conn.cursor()
    c.execute('SELECT * FROM users WHERE username = ?', (username,))
    data = c.fetchone()
    conn.close()
    
    if data and check_hashes(password, data[1]):
        return data
    return None

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
st.set_page_config(
    page_title="SEC Filings Scraper Pro", 
    layout="wide",
    page_icon="📊"
)

# Initialize database
init_db()

# ===== AUTHENTICATION UI =====
def auth_page():
    st.title("🔒 SEC Filings Scraper Pro")
    st.markdown("""
    <style>
        .main {
            background-color: #f8f9fa;
        }
        .stButton>button {
            background-color: #4CAF50;
            color: white;
            border-radius: 5px;
            padding: 0.5rem 1rem;
        }
        .stTextInput>div>div>input {
            border-radius: 5px;
            padding: 0.5rem;
        }
        .tab-container {
            background-color: white;
            border-radius: 10px;
            padding: 2rem;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }
    </style>
    """, unsafe_allow_html=True)
    
    tab1, tab2 = st.tabs(["Login", "Sign Up"])
    
    with tab1:
        with st.container():
            st.subheader("Login to Your Account")
            username = st.text_input("Username", key="login_username")
            password = st.text_input("Password", type="password", key="login_password")
            
            if st.button("Login"):
                user = login_user(username, password)
                if user:
                    st.session_state["authenticated"] = True
                    st.session_state["user"] = {
                        "username": user[0],
                        "email": user[2],
                        "full_name": user[3]
                    }
                    st.rerun()
                else:
                    st.error("Invalid username or password")
    
    with tab2:
        with st.container():
            st.subheader("Create New Account")
            new_username = st.text_input("Choose a Username", key="signup_username")
            new_email = st.text_input("Email Address", key="signup_email")
            new_full_name = st.text_input("Full Name", key="signup_fullname")
            new_password = st.text_input("Create Password", type="password", key="signup_password1")
            confirm_password = st.text_input("Confirm Password", type="password", key="signup_password2")
            
            if st.button("Register"):
                if new_password == confirm_password:
                    try:
                        create_user(new_username, new_password, new_email, new_full_name)
                        st.success("Account created successfully! Please login.")
                    except sqlite3.IntegrityError:
                        st.error("Username already exists")
                else:
                    st.error("Passwords do not match")

# ===== MAIN APPLICATION =====
def main_app():
    # ===== CONFIGURATION =====
    SEC_DELAY = 0.5
    MAX_RETRIES = 3
    SEC_API_URL = "https://efts.sec.gov/LATEST/search-index"

    # Common filing types and groups
    COMMON_FILING_TYPES = ['10-K', '10-Q', '8-K', 'DEF 14A', 'S-1', 'S-3', 'F-1', '20-F']
    CUSTOM_FILING_GROUPS = {
        "Non-mgt (custom)": ["DEFC14A", "DEFC14C", "DEFN14A", "DEFR14A", "DFAN14A", 
                           "DFRN14A", "PREC14A", "PREC14C", "PREN14A", "PRER14A", "PRRN14A"],
        "13D (custom)": ["SC 13D", "SCHEDULE 13D"],
        "13G (custom)": ["SC 13G", "SCHEDULE 13G"]
    }
    ALL_FILING_OPTIONS = list(CUSTOM_FILING_GROUPS.keys()) + COMMON_FILING_TYPES

    # ===== STREAMLIT UI ELEMENTS =====
    st.title(f"📊 SEC Filings Scraper Pro")
    st.markdown(f"Welcome back, **{st.session_state.user['full_name']}**! 👋")
    
    with st.sidebar:
        st.header("🔍 Search Parameters")
        
        # Entity search input
        entity_search = st.text_input(
            "Company name, ticker, or CIK:",
            help="Filter by specific company or individual"
        )
        
        # Date Range Selector
        st.subheader("📅 Date Range")
        date_range = st.date_input("Select Date Range", [], key="date_range")
        from_date, to_date = (date_range if len(date_range) == 2 else 
                            (datetime.now().date() - timedelta(days=1), 
                             datetime.now().date()))
        
        # Filing Type Selector
        st.subheader("📄 Filing Types")
        selected_options = st.multiselect(
            "Select filing types or groups:",
            options=ALL_FILING_OPTIONS,
            default=['10-K'],
            help="Select individual filing types or predefined groups"
        )
        
        # Advanced Options
        with st.expander("⚙️ Advanced Options"):
            doc_search = st.text_input(
                "Document word or phrase:",
                help="Search for specific text within filings"
            )
            
            custom_type = st.text_input(
                "Custom filing type(s):",
                help="Example: '10-K, 10-Q, 8-K'"
            )
            
            show_details = st.checkbox(
                "Show location and incorporation details",
                value=False,
                help="Toggle to show/hide location and incorporation columns"
            )
        
        # Logout button
        if st.button("🚪 Logout"):
            st.session_state["authenticated"] = False
            st.session_state.pop("user", None)
            st.rerun()
        
        run_button = st.button("🔎 Fetch Filings", type="primary")

    # ===== SEC CLIENT CLASS =====
    class SECAPIClient:
        def __init__(self, show_details):
            self.session = requests.Session()
            retry = Retry(total=MAX_RETRIES, backoff_factor=0.5, 
                         status_forcelist=[500, 502, 503, 504])
            adapter = HTTPAdapter(max_retries=retry)
            self.session.mount('https://', adapter)
            self.headers = {
                "User-Agent": f"{st.session_state.user['full_name']} {st.session_state.user['email']}",
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
            filename = f"{accession_number}-index.htm"
            
            return f"https://www.sec.gov/Archives/edgar/data/{cik}/{directory}/{filename}"
        except Exception as e:
            st.warning(f"URL generation error: {str(e)}")
            return "Invalid URL"

    def make_clickable(url, text="View"):
        """Create a properly formatted clickable link only if we have a valid URL"""
        if not url or url == "None":
            return "N/A"
        if url.startswith("http"):
            return f'<a target="_blank" href="{url}" style="text-decoration: none; color: #1a73e8;">🔗 {text}</a>'
        return url 

    def redo_clickable_link(clickable_link):
        if not isinstance(clickable_link, str):
            return clickable_link
        
        pattern = r'<a[^>]*href=["\']([^"\']*)["\'][^>]*>'
        match = re.search(pattern, clickable_link)
        
        if match:
            return match.group(1)
        return clickable_link

    SEC_HEADERS = {
        "User-Agent": f"{st.session_state.user['full_name']} {st.session_state.user['email']}",
        "Accept-Encoding": "gzip, deflate",
        "Accept": "text/html,application/xhtml+xml"
    }

    def get_matching_document(filing_url, form_type):
        """Find the HTML document that matches the form type from the index page"""
        try:
            if not filing_url or "unavailable" in filing_url:
                return None
            
            # Ensure we have a proper SEC URL
            if not filing_url.startswith('http'):
                filing_url = f"https://www.sec.gov{filing_url}"
                
            response = requests.get(filing_url, headers=SEC_HEADERS, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            doc_rows = soup.select('table.tableFile tr')[1:]
            
            best_match_url = ""
            best_match_size = 0
            
            for row in doc_rows:
                cells = row.find_all('td')
                if len(cells) >= 4:
                    current_doc_type = cells[3].get_text().strip()
                    doc_url = cells[2].find('a')['href'] if cells[2].find('a') else ""
                    size_text = cells[4].get_text().strip()
                    
                    if not doc_url.lower().endswith(('.htm', '.html')):
                        continue
                    
                    size = 0
                    if size_text:
                        if 'KB' in size_text:
                            size = float(size_text.replace('KB', '')) * 1024
                        elif 'MB' in size_text:
                            size = float(size_text.replace('MB', '')) * 1024 * 1024
                        else:
                            size = float(size_text) if size_text.replace('.', '').isdigit() else 0
                    
                    if current_doc_type.upper() == form_type.upper():
                        if size > best_match_size:
                            best_match_size = size
                            best_match_url = doc_url
                    
                    elif not best_match_url and form_type.upper() in current_doc_type.upper():
                        if size > best_match_size:
                            best_match_size = size
                            best_match_url = doc_url
            
            if best_match_url:
                # Ensure the URL is absolute
                if not best_match_url.startswith('http'):
                    if best_match_url.startswith('/'):
                        best_match_url = f"https://www.sec.gov{best_match_url}"
                    else:
                        base_url = filing_url[:filing_url.rfind('/')+1]
                        best_match_url = f"{base_url}{best_match_url}"
                return best_match_url
            
            return "None"
        
        except Exception as e:
            st.warning(f"⚠️ Failed to find HTML document: {str(e)[:100]}")
            return "None"

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

                for hit in hits:
                    try:
                        source = hit["_source"]
                        cik = str(source["ciks"][0])
                        form = source.get("form", "N/A")
                        date_filed = source.get("file_date", "Unknown date")
                        hit_id = hit.get("_id", "")
                        accession_number = source.get("adsh", "")
                        filing_url = build_filing_url(cik, accession_number) 
                        
                        details = client.get_company_details(cik)
                        entity_name = source.get("display_names", ["N/A"])[0]
                        ticker = extract_ticker(entity_name)
                        clean_entity = re.sub(r"\s*\([A-Z]+\)", "", entity_name)
                        cleaned = re.sub(r"\s*\([^)]*\)", "", source.get("display_names", ["N/A"])[0])
                        filing_url = build_filing_url(cik, accession_number)
                        main_doc_url = get_matching_document(filing_url, form)
                        

                        all_filings.append({
                            "CIK": cik,
                            "Form": form,
                            "Date Filed": date_filed,
                            "Ticker": ticker,
                            "Filing Entity": cleaned,
                            "Filing Person": clean_entity,
                            "Located": details["location"],
                            "Incorporated": details["incorporated"],
                            "Filing": (filing_url), 
                            "Main Document": (main_doc_url)
                        })
                        
                    except Exception as e:
                        st.warning(f"Error processing filing: {str(e)}")
                        continue

                progress = min(page * 100 / total_filings, 1.0) if total_filings > 0 else 1.0
                progress_bar.progress(progress)
                
                elapsed_time = time.time() - start_time
                status_text.text(
                    f"Processed {page} pages ({len(all_filings)} filings)\n"
                    f"Elapsed: {timedelta(seconds=int(elapsed_time))}"
                )
                
                page += 1
                time.sleep(SEC_DELAY)

            progress_bar.empty()
            status_text.empty()

            if all_filings:
                df = pd.DataFrame(all_filings)
                df['Date Filed'] = pd.to_datetime(df['Date Filed'])

                
                
                
                final_columns = ['Filing', 'Main Document', 'Form', 'Date Filed', 'Ticker', 'Filing Entity']
                if show_details:
                    final_columns.extend(['Filing Person', 'Located', 'Incorporated'])

                st.success(f"✅ Found {len(df)} filings")
                
                # Enhanced results display
                with st.expander("📊 Results Summary", expanded=True):
                    # Display the dataframe with clickable links
                    
                    
                    # Also show a regular dataframe view
                    st.dataframe(
                        df[final_columns],
                        use_container_width=True,
                        hide_index=True
                    )
                
                # CSV download
                csv_df = df[[c for c in final_columns]].copy()
                for column in ['Filing', 'Main Document']:
                    if column in csv_df.columns:
                        csv_df[column] = csv_df[column].apply(redo_clickable_link)
                csv = csv_df.to_csv(index=False).encode('utf-8')
                
                col1, col2 = st.columns(2)
                with col1:
                    st.download_button(
                        label="📥 Download as CSV",
                        data=csv,
                        file_name=f"SEC_filings_{from_date}_to_{to_date}.csv",
                        mime='text/csv'
                    )
                with col2:
                    if st.button("🔄 Run New Search"):
                        st.rerun()
            else:
                st.warning("⚠️ No filings found matching your criteria")

# ===== APP ROUTING =====
if "authenticated" not in st.session_state:
    st.session_state["authenticated"] = False

if st.session_state["authenticated"]:
    main_app()
else:
    auth_page()
