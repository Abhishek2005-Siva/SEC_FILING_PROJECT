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

# ===== COMPLETE LIST OF SEC FILING TYPES =====
ALL_FILING_TYPES = [
    '1', '1-A', '1-A POS', '1-A-W', '1-E', '1-E AD', '1-K', '1-SA', '1-U', '1-Z', '1-Z-W',
    '10-12B', '10-12G', '10-C', '10-D', '10-K', '10-K405', '10-KT', '10-M', '10-Q', '10-QT',
    '10KSB', '10KSB40', '10KSB405', '10KT405', '10QSB', '10SB12B', '10SB12G', '11-K', '11-KT',
    '12G-2', '12G3-2A', '12G3-2B', '12G32BR', '13F-E', '13F-HR', '13F-NT', '13FCONP', '144',
    '15-12B', '15-12G', '15-15D', '15F-12B', '15F-12G', '15F-15D', '18-12B', '18-12G', '18-K',
    '19-B', '19B-4', '19B-4E', '2-A', '2-AF', '2-E', '20-F', '20-FR', '20FR12B', '20FR12G',
    '24F-1', '24F-2EL', '24F-2NT', '24F-2TM', '25', '25-NSE', '253G1', '253G2', '253G3', '253G4',
    '26', '27', '28', '3', '305B2', '34-12H', '34-36CF', '34-36MR', '35-11', '35-2', '35-3',
    '35-7B', '35-APP', '35-CERT', '39-10B2', '39-304C', '39-304D', '39-310B', '4', '40-17F1',
    '40-17F2', '40-17G', '40-17GCS', '40-202A', '40-203A', '40-205A', '40-205E', '40-206A',
    '40-24B2', '40-33', '40-6B', '40-6C', '40-8B25', '40-8F-2', '40-8F-A', '40-8F-B', '40-8F-L',
    '40-8F-M', '40-8FC', '40-APP', '40-F', '40-OIP', '40-RPT', '40FR12B', '40FR12G', '424A',
    '424B1', '424B2', '424B3', '424B4', '424B5', '424B7', '424B8', '424H', '425', '45B-3',
    '485A24E', '485A24F', '485APOS', '485B24E', '485B24F', '485BPOS', '485BXT', '485BXTF',
    '486A24E', '486APOS', '486B24E', '486BPOS', '486BXT', '487', '497', '497AD', '497H2', '497J',
    '497K', '497K1', '497K2', '497K3A', '497K3B', '497VPI', '497VPU', '5', '6-K', '6B NTC',
    '6B ORDR', '7-A', '8-A12B', '8-A12G', '8-B12B', '8-B12G', '8-K', '8-K12B', '8-K12G3',
    '8-K15D5', '8-M', '8A12BEF', '8A12BT', '8F-2 NTC', '8F-2 ORDR', '9-M', 'ABS-15G', 'ABS-EE',
    'ADB', 'ADN-MTL', 'ADV', 'ADV-E', 'ADV-H-C', 'ADV-H-T', 'ADV-NR', 'ADVCO', 'ADVW', 'AFDB',
    'ANNLRPT', 'APP NTC', 'APP ORDR', 'APP WD', 'APP WDG', 'ARS', 'ATS-N', 'ATS-N ORDR INEFF',
    'ATS-N ORDR LTD OPN', 'ATS-N ORDR REVK', 'ATS-N ORDR SUSP', 'ATS-N-C', 'ATS-N-W',
    'ATS-N/A ORDR INEFF', 'ATS-N/CA', 'ATS-N/MA', 'ATS-N/MA CP', 'ATS-N/OFA', 'ATS-N/UA', 'AW',
    'AW WD', 'BDCO', 'BW-2', 'BW-3', 'C', 'C-AR', 'C-AR-W', 'C-AR/A-W', 'C-TR', 'C-TR-W', 'C-U',
    'C-U-W', 'C-W', 'C/A-W', 'CA-1', 'CB', 'CERT', 'CERTAMX', 'CERTARCA', 'CERTBATS', 'CERTBSE',
    'CERTCBO', 'CERTCIN', 'CERTCSE', 'CERTISE', 'CERTNAS', 'CERTNYS', 'CERTPAC', 'CERTPBS',
    'CFPORTAL', 'CFPORTAL-W', 'CORRESP', 'CT ORDER', 'D', 'DEF 14A', 'DEF 14C', 'DEF-OC',
    'DEF13E3', 'DEFA14A', 'DEFA14C', 'DEFC14A', 'DEFC14C', 'DEFM14A', 'DEFM14C', 'DEFN14A',
    'DEFR14A', 'DEFR14C', 'DEFS14A', 'DEFS14C', 'DEL AM', 'DFAN14A', 'DFRN14A', 'DOS', 'DOSLTR',
    'DRS', 'DRSLTR', 'DSTRBRPT', 'EBRD', 'EFFECT', 'F-1', 'F-10', 'F-10EF', 'F-10MEF', 'F-10POS',
    'F-1MEF', 'F-2', 'F-2D', 'F-2DPOS', 'F-2MEF', 'F-3', 'F-3ASR', 'F-3D', 'F-3DPOS', 'F-3MEF',
    'F-4', 'F-4 POS', 'F-4EF', 'F-4MEF', 'F-6', 'F-6 POS', 'F-6EF', 'F-7', 'F-7 POS', 'F-8',
    'F-8 POS', 'F-80', 'F-80POS', 'F-9', 'F-9 POS', 'F-9EF', 'F-9MEF', 'F-N', 'F-X', 'FOCUSN',
    'FWP', 'G-405', 'G-405N', 'G-FIN', 'G-FINW', 'HISTORY', 'IADB', 'IBRD', 'ID-NEWCIK', 'IFC',
    'IRANNOTICE', 'MA', 'MA-A', 'MA-I', 'MA-W', 'MSD', 'MSDCO', 'MSDW', 'N-1', 'N-14', 'N-14 8C',
    'N-14AE', 'N-14MEF', 'N-18F1', 'N-1A', 'N-1A EL', 'N-2', 'N-2 POSASR', 'N-23C-1', 'N-23C-2',
    'N-23C3A', 'N-23C3B', 'N-23C3C', 'N-27D-1', 'N-2ASR', 'N-2MEF', 'N-3', 'N-3 EL', 'N-30B-2',
    'N-30D', 'N-4', 'N-4 EL', 'N-5', 'N-54A', 'N-54C', 'N-6', 'N-6C9', 'N-6F', 'N-8A', 'N-8B-2',
    'N-8B-3', 'N-8B-4', 'N-8F', 'N-8F NTC', 'N-8F ORDR', 'N-CEN', 'N-CR', 'N-CSR', 'N-CSRS',
    'N-MFP', 'N-MFP1', 'N-MFP2', 'N-PX', 'N-Q', 'N-VP', 'N-VPFS', 'N14AE24', 'N14EL24', 'NO ACT',
    'NPORT-EX', 'NPORT-NP', 'NPORT-P', 'NRSRO-CE', 'NRSRO-UPD', 'NSAR-A', 'NSAR-AT', 'NSAR-B',
    'NSAR-BT', 'NSAR-U', 'NT 10-D', 'NT 10-K', 'NT 10-Q', 'NT 11-K', 'NT 15D2', 'NT 20-F',
    'NT N-CEN', 'NT N-MFP', 'NT N-MFP1', 'NT N-MFP2', 'NT NPORT-EX', 'NT NPORT-N', 'NT NPORT-P',
    'NT-NCEN', 'NT-NCSR', 'NT-NSAR', 'NTFNCEN', 'NTFNCSR', 'NTFNSAR', 'NTN 10-D', 'NTN 10D',
    'NTN 10K', 'NTN 10Q', 'NTN 11K', 'NTN 20F', 'NTN15D2', 'OC', 'OIP NTC', 'OIP ORDR', 'POS 8C',
    'POS AM', 'POS AMC', 'POS AMI', 'POS EX', 'POS462B', 'POS462C', 'POSASR', 'PRE 14A', 'PRE 14C',
    'PRE13E3', 'PREA14A', 'PREA14C', 'PREC14A', 'PREC14C', 'PREM14A', 'PREM14C', 'PREN14A',
    'PRER14A', 'PRER14C', 'PRES14A', 'PRES14C', 'PRRN14A', 'PWR-ATT', 'PX14A6G', 'PX14A6N',
    'QRTLYRPT', 'QUALIF', 'REG-NR', 'REGDEX', 'REVOKED', 'RW', 'RW WD', 'S-1', 'S-11', 'S-11MEF',
    'S-1MEF', 'S-2', 'S-20', 'S-2MEF', 'S-3', 'S-3ASR', 'S-3D', 'S-3DPOS', 'S-3MEF', 'S-4',
    'S-4 POS', 'S-4EF', 'S-4MEF', 'S-6', 'S-6EL24', 'S-8', 'S-8 POS', 'S-B', 'S-BMEF', 'SB-1',
    'SB-1MEF', 'SB-2', 'SB-2MEF', 'SBSE', 'SBSE-A', 'SBSE-BD', 'SBSE-C', 'SBSE-W', 'SBSEF',
    'SBSEF-V', 'SBSEF-W', 'SBSEF/A', 'SC 13D', 'SC 13E1', 'SC 13E3', 'SC 13E4', 'SC 13G',
    'SC 14D1', 'SC 14D9', 'SC 14F1', 'SC 14N', 'SC 14N-S', 'SC TO-C', 'SC TO-I', 'SC TO-T',
    'SC13E4F', 'SC14D1F', 'SC14D9', 'SC14D9C', 'SC14D9F', 'SCHEDULE 13D', 'SCHEDULE 13G', 'SD',
    'SDR', 'SDR-A', 'SDR-W', 'SE', 'SEC ACTION', 'SEC STAFF ACTION', 'SEC STAFF LETTER', 'SF-1',
    'SF-3', 'SH-ER', 'SH-NT', 'SL', 'SP 15D2', 'SPDSCL', 'STOP ORDER', 'SUPPL', 'T-3', 'TA-1',
    'TA-2', 'TA-W', 'TACO', 'TH', 'TTW', 'U-1', 'U-12-IA', 'U-12-IB', 'U-13-1', 'U-13-60',
    'U-13E-1', 'U-33-S', 'U-3A-2', 'U-3A3-1', 'U-57', 'U-6B-2', 'U-7D', 'U-9C-3', 'U-R-1',
    'U5A', 'U5B', 'U5S', 'UNDER', 'UPLOAD', 'WDL-REQ', 'X-17A-5'
]

# ===== STREAMLIT UI ELEMENTS =====
with st.sidebar:
    st.header("Search Parameters")
    
    # Date Range Selector
    date_range = st.date_input("Date Range", [])
    if len(date_range) == 2:
        from_date, to_date = date_range
    else:
        from_date = to_date = datetime.now().date() - timedelta(days=1)
    
    # Filing Type Selection - Dual Input System
    st.subheader("Filing Type Selection")
    
    # Option 1: Checkbox Multi-Select
    selected_types = st.multiselect(
        "Select from common filing types:",
        options=['10-K', '10-Q', '8-K', 'DEF 14A', 'S-1', 'S-3', 'F-1', '20-F'],
        default=['10-K']
    )
    
    # Option 2: Free Text Input (with autocomplete)
    custom_type = st.text_input(
        "Or enter custom filing type(s) (comma separated):",
        help="Example: '10-K, 10-Q, 8-K'"
    )
    
    # Combine both inputs
    if custom_type:
        custom_types = [x.strip().upper() for x in custom_type.split(',') if x.strip()]
        filing_types = list(set(selected_types + custom_types))
    else:
        filing_types = selected_types
    
    # Full list expander
    with st.expander("View Complete Filing Type List"):
        st.write(ALL_FILING_TYPES)
    
    run_button = st.button("Fetch Filings")

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
            if e.response.status_code == 403:
                st.warning(f"⚠️ SEC denied access: {url.split('/master.')[1]}")
            else:
                st.warning(f"⚠️ HTTP Error: {str(e)[:100]}")
            return None
        except Exception as e:
            st.warning(f"⚠️ Network Error: {str(e)[:100]}")
            return None

# ===== PROCESSING FUNCTIONS =====
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
if run_button and len(date_range) == 2 and filing_types:
    with st.spinner(f"Fetching {', '.join(filing_types)} filings from {from_date} to {to_date}..."):
        client = SECClient()
        all_filings = []
        cik_to_ticker = get_cik_ticker_map()

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
                    form_upper = form.strip().upper()
                    
                    # Check if form matches any of our selected types
                    if not any(form_upper.startswith(ft) for ft in filing_types):
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
                file_name=f"SEC_filings_{from_date}_to_{to_date}.csv",
                mime='text/csv'
            )
        else:
            st.warning("⚠️ No filings found matching your criteria")
elif run_button and not filing_types:
    st.error("Please select at least one filing type")
