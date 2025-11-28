import os
import pandas as pd
import sys
import re
import time
import threading
import itertools
import math
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta

# --- CONFIGURATION ---
INPUT_FOLDER_NAME = "input_files"
OUTPUT_FOLDER_NAME = "output_files"

# Region 6 Configuration
REQUIRED_PROVINCES = {
    "AKLAN", 
    "ANTIQUE", 
    "CAPIZ", 
    "ILOILO", 
    "GUIMARAS", 
    "NEGROS OCCIDENTAL" 
}

# Mode 3: RSBSA Report Columns
TARGET_COLS_RSBSA = [
    'farmer_address_mun', 
    'farmer_address_bgy', 
    'farmer', 
    'farmworker',
    'fisherfolk', 
    'gender', 
    'agency',
    'birthday',
    'crop_area'
]

# Mode 5: Geotag Cleaning Columns (Strict)
TARGET_COLS_GEOTAG = [
    'GEOREF ID',
    'RSBSA ID',
    'COMMODITY',
    'DECLARED AREA (Ha)',
    'VERIFIED AREA (Ha)',
    'PROVINCE',
    'MUNICIPALITY',
    'BARANGAY',
    'UPLOADER',
    'TRACK DATE'
]

# Output Order for Mode 5
FINAL_COLUMN_ORDER = [
    'GEOREF ID',
    'RSBSA ID',
    'COMMODITY',
    'PROVINCE',
    'MUNICIPALITY',
    'BARANGAY',
    'DECLARED AREA (Ha)',
    'CROP AREA',
    'VERIFIED AREA (Ha)',
    'TRACK DATE',
    'FINDINGS',
    'UPLOADER'
]

# --- UTILS ---
def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')

def print_header():
    print("="*70)
    print("   🌾  RSBSA TOOLBELT (Region 6)")
    print("   Powered by XlsxWriter")
    print("="*70)

class LoadingSpinner:
    """A simple spinner animation running in a separate thread."""
    def __init__(self, message="Processing..."):
        self.message = message
        self.busy = False
        self.delay = 0.1
        self.spinner = itertools.cycle(['|', '/', '-', '\\'])
        self._screen_lock = threading.Lock()

    def spinner_task(self):
        while self.busy:
            with self._screen_lock:
                sys.stdout.write(f'\r{next(self.spinner)} {self.message}')
                sys.stdout.flush()
            time.sleep(self.delay)
            sys.stdout.write('\b' * (len(self.message) + 2))

    def __enter__(self):
        self.busy = True
        self.thread = threading.Thread(target=self.spinner_task)
        self.thread.start()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.busy = False
        self.thread.join()
        sys.stdout.write('\r' + ' ' * (len(self.message) + 2) + '\r')
        sys.stdout.flush()

def ensure_directories():
    if getattr(sys, 'frozen', False):
        application_path = os.path.dirname(sys.executable)
    else:
        application_path = os.path.dirname(os.path.abspath(__file__))
    
    input_path = os.path.join(application_path, INPUT_FOLDER_NAME)
    output_path = os.path.join(application_path, OUTPUT_FOLDER_NAME)
    
    created_new = False
    
    if not os.path.exists(input_path):
        os.makedirs(input_path)
        created_new = True
        
    if not os.path.exists(output_path):
        os.makedirs(output_path)
        created_new = True
        
    return input_path, output_path, created_new

def get_output_filename(default_name):
    print(f"\n(Default: {default_name})")
    name = input("📝 Enter output filename (or Press Enter to use default): ").strip()
    if not name:
        return default_name
    if not name.lower().endswith('.xlsx'):
        name += '.xlsx'
    return name

def clean_sheet_name(name):
    name = re.sub(r'[\[\]:*?/\\]', '', name)
    return name[:31]

def normalize_commodity(val):
    """Normalizes commodity names for matching (Rice/Palay equivalence)"""
    s = str(val).strip().upper()
    if 'RICE' in s or 'PALAY' in s: return 'RICE'
    if 'CORN' in s: return 'CORN'
    if 'SUGAR' in s: return 'SUGAR'
    return 'OTHER'

def select_input_file(input_dir, prompt="Select file number to process", ext_filter=('.xlsx', '.csv')):
    files = [f for f in os.listdir(input_dir) if f.lower().endswith(ext_filter) and not f.startswith('~$')]
    
    if not files:
        print(f"❌ No valid files {ext_filter} found in input folder.")
        return None

    print("\nAvailable Files:")
    for i, f in enumerate(files):
        print(f"   [{i+1}] {f}")
    
    while True:
        choice = input(f"\n{prompt}: ").strip()
        if choice.isdigit() and 1 <= int(choice) <= len(files):
            return os.path.join(input_dir, files[int(choice)-1])
        print("❌ Invalid selection.")

# --- GEOMETRY UTILS (GPX) ---
def haversine_distance(lat1, lon1, lat2, lon2):
    """Calculate distance between two points in meters"""
    R = 6371000  # Earth radius in meters
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))

def calculate_polygon_area(lats, lons):
    """
    Calculate area of polygon in Hectares using Shoelace formula projected to meters.
    """
    if len(lats) < 3: return 0.0
    
    # Convert to approximate meters relative to first point (Local projection)
    R = 6371000
    lat0, lon0 = lats[0], lons[0]
    x = []
    y = []
    
    for r_lat, r_lon in zip(lats, lons):
        # y is lat distance
        y.append(math.radians(r_lat - lat0) * R)
        # x is lon distance adjusted by cos(lat)
        x.append(math.radians(r_lon - lon0) * R * math.cos(math.radians(lat0)))
    
    # Shoelace formula
    area = 0.0
    j = len(x) - 1
    for i in range(len(x)):
        area += (x[j] + x[i]) * (y[j] - y[i])
        j = i
    
    area_sqm = abs(area / 2.0)
    return area_sqm / 10000.0  # Convert m2 to Hectares

# --- MODE 2: MASTERLIST MERGER & ANALYZER ---

def process_masterlist_merger(master_path, parcel_path, output_dir):
    """
    Merges Masterlist with Parcel List.
    1. Checks Anomalies (Identity, ID Duplicates)
    2. Merges but KEEPS all parcel rows (1:Many relationship).
    3. Adds 'HAS_MULTIPLE_LANDHOLDINGS' flag.
    """
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    output_filename = f"Masterlist_Merged_Analysis_{timestamp}.xlsx"
    output_path = os.path.join(output_dir, output_filename)

    print("\n--- Analyzing & Merging Datasets ---")
    
    try:
        # 1. LOAD MASTERLIST
        with LoadingSpinner("Loading Masterlist (Farmer Listing)..."):
            if master_path.lower().endswith('.csv'):
                df_m = pd.read_csv(master_path)
            else:
                df_m = pd.read_excel(master_path)
            
            # Normalize Columns
            df_m.columns = [c.strip().lower() for c in df_m.columns]
            
            # Identify RSBSA Column
            col_rsbsa = next((c for c in df_m.columns if 'rsbsa' in c and 'no' in c), None)
            if not col_rsbsa:
                print("❌ Error: RSBSA No. column not found in Masterlist.")
                return
            
            # Standardize ID for matching
            df_m['KEY_ID'] = df_m[col_rsbsa].astype(str).str.strip().str.upper()

        # 2. LOAD PARCEL LIST
        with LoadingSpinner("Loading Parcel List..."):
            if parcel_path.lower().endswith('.csv'):
                df_p = pd.read_csv(parcel_path)
            else:
                df_p = pd.read_excel(parcel_path)
            
            df_p.columns = [c.strip().lower() for c in df_p.columns]
            
            # Identify Keys
            col_ffrs = next((c for c in df_p.columns if 'ffrs' in c or 'system generated' in c), None)
            # We don't strictly need area/comm/address for the merge key, but we keep them
            
            if not col_ffrs:
                print("❌ Error: FFRS ID column not found in Parcel List.")
                return
            
            df_p['KEY_ID'] = df_p[col_ffrs].astype(str).str.strip().str.upper()

        # 3. ANALYZE ANOMALIES
        with LoadingSpinner("Detecting Anomalies..."):
            
            # A. Identity Conflicts (Same Name+Mid+Last+Bday, Diff ID)
            m_fname = next((c for c in df_m.columns if 'first' in c and 'name' in c), 'first_name')
            m_mname = next((c for c in df_m.columns if 'middle' in c and 'name' in c), 'middle_name')
            m_lname = next((c for c in df_m.columns if 'last' in c and 'name' in c), 'last_name')
            m_bday = next((c for c in df_m.columns if 'birth' in c), 'birthday')

            # Create Signature
            df_m['IDENTITY_SIG'] = (
                df_m[m_fname].fillna('').astype(str).str.strip().str.upper() + 
                df_m[m_mname].fillna('').astype(str).str.strip().str.upper() + 
                df_m[m_lname].fillna('').astype(str).str.strip().str.upper() + 
                df_m[m_bday].astype(str)
            )
            
            dup_sig_mask = df_m.duplicated(subset=['IDENTITY_SIG'], keep=False)
            df_identity_conflicts = df_m[dup_sig_mask].sort_values('IDENTITY_SIG')
            
            # Filter for different IDs
            if not df_identity_conflicts.empty:
                sig_counts = df_identity_conflicts.groupby('IDENTITY_SIG')['KEY_ID'].nunique()
                suspicious_sigs = sig_counts[sig_counts > 1].index
                df_identity_conflicts = df_identity_conflicts[df_identity_conflicts['IDENTITY_SIG'].isin(suspicious_sigs)]

            # B. ID Conflicts
            dup_id_mask = df_m.duplicated(subset=['KEY_ID'], keep=False)
            df_id_duplicates = df_m[dup_id_mask].sort_values('KEY_ID')

            # C. Identify Multiple Landholdings
            # Count parcels per ID in the Parcel List
            parcel_counts = df_p['KEY_ID'].value_counts()
            
            # Map this count back to the Parcel Dataframe
            df_p['PARCEL_COUNT'] = df_p['KEY_ID'].map(parcel_counts)
            df_p['HAS_MULTIPLE_LANDHOLDINGS'] = df_p['PARCEL_COUNT'] > 1

        # 4. MERGE (FULL JOIN STRATEGY)
        with LoadingSpinner("Merging Datasets (Expanding Rows)..."):
            # We perform a RIGHT JOIN (or Outer) to keep all parcel rows.
            # Masterlist data will be duplicated for every parcel row.
            
            # Drop duplicate columns in Parcel List that might exist in Masterlist to avoid collision
            # (Except Key ID and new flags)
            cols_to_use = list(df_p.columns)
            # Optionally remove redundant name columns if they exist in Parcel list to keep output clean?
            # For now, we keep everything from Parcel list as requested to see address/commodity differences.
            
            df_final = pd.merge(df_m, df_p, on='KEY_ID', how='right')
            
            # Fill Multi-Landholding flag for those who matched
            df_final['HAS_MULTIPLE_LANDHOLDINGS'] = df_final['HAS_MULTIPLE_LANDHOLDINGS'].fillna(False)

            # Cleanup
            df_final.drop(columns=['KEY_ID', 'IDENTITY_SIG'], inplace=True)

        # 5. SAVE
        with LoadingSpinner(f"Saving Report to {output_filename}..."):
            with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
                # Sheet 1: Merged Data
                df_final.to_excel(writer, sheet_name='Merged Data', index=False)
                
                # Sheet 2
                if not df_identity_conflicts.empty:
                    cols = [c for c in df_identity_conflicts.columns if c != 'IDENTITY_SIG']
                    df_identity_conflicts[cols].to_excel(writer, sheet_name='Identity Conflicts', index=False)
                
                # Sheet 3
                if not df_id_duplicates.empty:
                    cols = [c for c in df_id_duplicates.columns if c != 'IDENTITY_SIG']
                    df_id_duplicates[cols].to_excel(writer, sheet_name='Duplicate IDs', index=False)

                # Sheet 4: Stats
                unique_farmers_in_merged = df_final[col_rsbsa].nunique()
                stats = pd.DataFrame({
                    'Metric': [
                        'Total Rows in Output',
                        'Unique Farmers (Based on RSBSA ID)',
                        'Rows with Multiple Landholdings',
                        'Potential Identity Theft Cases'
                    ],
                    'Value': [
                        len(df_final), 
                        unique_farmers_in_merged,
                        len(df_final[df_final['HAS_MULTIPLE_LANDHOLDINGS'] == True]),
                        len(df_identity_conflicts)
                    ]
                })
                stats.to_excel(writer, sheet_name='Statistics', index=False)
                
                wb = writer.book
                ws = writer.sheets['Statistics']
                ws.set_column(0, 0, 40)

        print(f"\n🎉 Processing Complete!")
        print(f"   Output: {output_filename}")

    except Exception as e:
        print(f"❌ Critical Error: {e}")

# --- MODE 1: STACK ROWS ---

def run_stack_rows(input_dir, output_dir):
    try:
        all_files = [f for f in os.listdir(input_dir) if f.lower().endswith(('.xlsx', '.csv')) and not f.startswith('~$')]
    except: return
    if not all_files:
        print("❌ No files found.")
        return
    print("\n--- Starting Strict Merge ---")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    output_filename = get_output_filename(f"Stacked_Output_{timestamp}.xlsx")
    merged_data = []
    
    for filename in all_files:
        file_path = os.path.join(input_dir, filename)
        try:
            if filename.lower().endswith('.csv'): df = pd.read_csv(file_path)
            else: df = pd.read_excel(file_path)
            if not df.empty:
                # SORTING LOGIC: Mode 1
                sort_col = next((c for c in df.columns if 'last' in c.lower() and 'name' in c.lower()), None)
                if sort_col:
                    df.sort_values(by=sort_col, inplace=True)
                
                df['Source_File'] = filename
                merged_data.append(df)
                print(f"✅ Buffered: {filename}")
        except: pass
    
    if merged_data:
        final_df = pd.concat(merged_data, ignore_index=True)
        path = os.path.join(output_dir, output_filename)
        with pd.ExcelWriter(path, engine='xlsxwriter') as writer:
            final_df.to_excel(writer, index=False)
        print(f"🎉 Saved: {path}")

# --- MODE 3: COMBINE SHEETS (Renamed from 2) ---

def run_combine_sheets(input_dir, output_dir):
    try:
        all_files = [f for f in os.listdir(input_dir) if f.lower().endswith(('.xlsx', '.csv')) and not f.startswith('~$')]
    except: return
    if not all_files:
        print("❌ No files found.")
        return
    print("\n--- Starting Sheet Combine ---")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    output_filename = get_output_filename(f"Sheets_Output_{timestamp}.xlsx")
    path = os.path.join(output_dir, output_filename)
    
    with pd.ExcelWriter(path, engine='xlsxwriter') as writer:
        for filename in all_files:
            file_path = os.path.join(input_dir, filename)
            base = filename.rsplit('.', 1)[0]
            try:
                if filename.lower().endswith('.csv'):
                    pd.read_csv(file_path).to_excel(writer, sheet_name=clean_sheet_name(base), index=False)
                else:
                    sheets = pd.read_excel(file_path, sheet_name=None)
                    for s, df in sheets.items():
                        writer_name = clean_sheet_name(f"{base}_{s}" if len(sheets)>1 else base)
                        df.to_excel(writer, sheet_name=writer_name[:31], index=False)
                print(f"✅ Added: {filename}")
            except: pass
    print(f"🎉 Saved: {path}")

# --- MODE 4: REGIONAL SUMMARY (Renamed from 3) ---

def process_rsbsa_report(file_path, output_dir):
    # User Inputs
    as_of_input = input("\n📅 Enter 'As Of' Date (e.g., Oct 30, 2024): ").strip()
    if not as_of_input:
        ref_date = datetime.now()
        as_of_str = ref_date.strftime("%B %d, %Y")
    else:
        try:
            ref_date = pd.to_datetime(as_of_input)
            as_of_str = as_of_input
        except:
            print("⚠️  Could not parse date. Using today for age calculations.")
            ref_date = datetime.now()
            as_of_str = as_of_input

    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    default_name = f"RSBSA_Region6_Summary_{timestamp}.xlsx"
    output_filename = get_output_filename(default_name)
    output_path = os.path.join(output_dir, output_filename)

    try:
        def col_filter(col_name):
            return col_name.strip().lower() in TARGET_COLS_RSBSA

        with LoadingSpinner(f"Loading '{os.path.basename(file_path)}'..."):
            xls = pd.read_excel(file_path, sheet_name=None, usecols=col_filter)
        
        sheet_names = set(xls.keys())
        
        # VALIDATE
        missing_provinces = REQUIRED_PROVINCES - sheet_names
        if missing_provinces:
            print("\n🛑 VALIDATION FAILED: Missing Province Sheets")
            print(f"   Missing: {', '.join(missing_provinces)}")
            return
        
        print("✅ Validation Passed.")

        with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
            for province in REQUIRED_PROVINCES:
                with LoadingSpinner(f"Processing {province}..."):
                    df = xls[province]
                    df.columns = [c.strip().lower() for c in df.columns]

                    if df.empty: continue

                    col_mun = 'farmer_address_mun'
                    col_bgy = 'farmer_address_bgy'
                    col_farmer = 'farmer'
                    col_farmworker = 'farmworker'
                    col_fisher = 'fisherfolk'
                    col_gender = 'gender'
                    col_agency = 'agency'
                    col_birthday = 'birthday'
                    col_area = 'crop_area'

                    # Counts
                    df['is_farmer'] = df[col_farmer].astype(str).str.upper().map({'YES': 1}).fillna(0)
                    df['is_farmworker'] = df[col_farmworker].astype(str).str.upper().map({'YES': 1}).fillna(0)
                    df['is_fisher'] = df[col_fisher].astype(str).str.upper().map({'YES': 1}).fillna(0)
                    df['male_count'] = df[col_gender].astype(str).str.upper().map({'MALE': 1}).fillna(0)
                    df['female_count'] = df[col_gender].astype(str).str.upper().map({'FEMALE': 1}).fillna(0)

                    # Age
                    df['bd_dt'] = pd.to_datetime(df[col_birthday], errors='coerce')
                    df['age_years'] = (ref_date - df['bd_dt']).dt.days / 365.25
                    df['age_years'] = df['age_years'].fillna(-1)
                    
                    # Youth 12-30
                    df['is_youth'] = ((df['age_years'] >= 12) & (df['age_years'] <= 30)).astype(int)
                    df['is_working_age'] = ((df['age_years'] > 30) & (df['age_years'] < 60)).astype(int)
                    df['is_senior'] = (df['age_years'] >= 60).astype(int)

                    # Crop Area Analytics
                    if col_area in df.columns:
                        df[col_area] = pd.to_numeric(df[col_area], errors='coerce').fillna(0)
                    else:
                        df[col_area] = 0.0
                    
                    # Rows with area >= 2
                    df['is_large_area'] = (df[col_area] >= 2).astype(int)

                    summary = df.groupby([col_mun, col_bgy]).agg({
                        'is_farmer': 'sum',
                        'is_farmworker': 'sum',
                        'is_fisher': 'sum',
                        col_agency: 'nunique',
                        'male_count': 'sum',
                        'female_count': 'sum',
                        'is_youth': 'sum',
                        'is_working_age': 'sum',
                        'is_senior': 'sum',
                        col_area: 'sum',       # Sum of Declared Area
                        'is_large_area': 'sum' # Count of >= 2
                    }).reset_index()

                    summary.columns = [
                        'Municipality', 'Barangay', 
                        'Farmers', 'Farmworkers', 'Fisherfolk', 
                        'Distinct Agencies', 'Male', 'Female',
                        'Youth (12-30)', 'Working Age (31-59)', 'Senior (60+)',
                        'Total Declared Area (Ha)', 'Farmers with >= 2 Ha'
                    ]

                    summary = summary.sort_values(['Municipality', 'Barangay'])

                    summary.to_excel(writer, sheet_name=province, index=False, startrow=4)
                    
                    # Headers
                    workbook = writer.book
                    worksheet = writer.sheets[province]
                    header_format = workbook.add_format({'bold': True, 'font_size': 14})
                    date_format = workbook.add_format({'italic': True})
                    legend_format = workbook.add_format({'italic': True, 'font_color': 'gray', 'font_size': 10})
                    num_fmt = workbook.add_format({'num_format': '#,##0.00'})
                    
                    worksheet.write('A1', f"RSBSA Summary Report - {province}", header_format)
                    worksheet.write('A2', f"As of: {as_of_str}", date_format)
                    worksheet.write('A3', "Age Legend: Youth (12-30) | Working Age (31-59) | Senior (60+)", legend_format)
                    
                    worksheet.set_column(0, 0, 20)
                    worksheet.set_column(1, 1, 25)
                    worksheet.set_column(2, 10, 15)
                    worksheet.set_column(11, 11, 22, num_fmt) # Format Area Column
                    worksheet.set_column(12, 12, 20)          # Format >=2 Column

        print(f"\n🎉 Report Generated: {output_filename}")
        print(f"   Location: {output_path}")

    except Exception as e:
        print(f"❌ Critical Error: {e}")

# --- MODE 5: GEOTAG PROCESSOR (Renamed from 4) ---

def load_geotag_parcel_reference(parcel_path):
    # Renamed to avoid conflict, same logic
    with LoadingSpinner("Loading & Filtering Parcel List..."):
        if parcel_path.lower().endswith('.csv'): df_head = pd.read_csv(parcel_path, nrows=1)
        else: df_head = pd.read_excel(parcel_path, nrows=1)
        
        cols = [c.strip() for c in df_head.columns]
        col_id = next((c for c in cols if c.upper() == 'FFRS SYSTEM GENERATED NO.'), None)
        col_area = next((c for c in cols if c.upper() == 'CROP AREA'), None)
        col_comm = next((c for c in cols if c.upper() == 'COMMODITY NAME'), None)
        col_prov = next((c for c in cols if c.upper() in ['PROVINCE', 'FARMER ADDRESS 3']), None)
        
        if not all([col_id, col_area, col_comm]): return None, None
        
        usecols = [col_id, col_area, col_comm]
        if col_prov: usecols.append(col_prov)
        
        if parcel_path.lower().endswith('.csv'): df_parcel = pd.read_csv(parcel_path, usecols=usecols)
        else: df_parcel = pd.read_excel(parcel_path, usecols=usecols)
        
        rename_map = {col_id:'KEY_ID', col_area:'CROP AREA', col_comm:'COMMODITY'}
        if col_prov: rename_map[col_prov] = 'PROVINCE'
        df_parcel.rename(columns=rename_map, inplace=True)

        master_province = None
        if 'PROVINCE' in df_parcel.columns: master_province = df_parcel['PROVINCE'].mode()[0].strip().upper()

        mask = df_parcel['COMMODITY'].astype(str).str.contains(r'Rice|Palay|Corn|Sugarcane', flags=re.IGNORECASE, regex=True)
        df_parcel = df_parcel[mask]
        return df_parcel, master_province

def process_single_geotag_logic(geotag_path, df_parcel, master_province, output_dir):
    base_name = os.path.splitext(os.path.basename(geotag_path))[0]
    output_filename = f"{base_name} [clean_enriched].xlsx"
    dupe_filename = f"{base_name} [duplicates].xlsx"
    output_path = os.path.join(output_dir, output_filename)
    dupe_path = os.path.join(output_dir, dupe_filename)

    try:
        if geotag_path.lower().endswith('.csv'): df_geo = pd.read_csv(geotag_path)
        else: df_geo = pd.read_excel(geotag_path)
        
        df_geo.columns = [c.strip() for c in df_geo.columns]
        missing = [c for c in TARGET_COLS_GEOTAG if c not in df_geo.columns]
        if missing: print(f"   ⚠️  Skipping {base_name}: Missing columns {missing}"); return

        if master_province:
            geo_prov = df_geo['PROVINCE'].mode()[0].strip().upper()
            if geo_prov != master_province:
                print(f"   🛑 Skipped {base_name}: Province mismatch ({geo_prov} != {master_province})"); return

        df_geo = df_geo[TARGET_COLS_GEOTAG].copy()
        dupe_mask = df_geo.duplicated(subset=['GEOREF ID'], keep=False)
        df_duplicates = df_geo[dupe_mask].sort_values('GEOREF ID')
        df_clean_geo = df_geo.drop_duplicates(subset=['GEOREF ID'], keep='first')

        if not df_duplicates.empty:
            with pd.ExcelWriter(dupe_path, engine='xlsxwriter') as writer:
                df_duplicates.to_excel(writer, index=False)

        df_merged = pd.merge(df_clean_geo, df_parcel[['KEY_ID', 'CROP AREA', 'COMMODITY']], 
                             left_on='RSBSA ID', right_on='KEY_ID', how='left', suffixes=('', '_parcel'))
        
        def is_match(row):
            if pd.isna(row['COMMODITY_parcel']): return False
            return normalize_commodity(row['COMMODITY']) == normalize_commodity(row['COMMODITY_parcel'])
        
        df_merged['is_match'] = df_merged.apply(is_match, axis=1)
        df_merged.sort_values(by=['GEOREF ID', 'is_match'], ascending=[True, False], inplace=True)
        df_final = df_merged.drop_duplicates(subset=['GEOREF ID'], keep='first').copy()
        
        def finalize_crop_area(row):
            if pd.isna(row['KEY_ID']): return "ID NOT FOUND"
            if not row['is_match']: return "COMMODITY MISMATCH"
            return row['CROP AREA']
        df_final['CROP AREA'] = df_final.apply(finalize_crop_area, axis=1)

        df_final['temp_track_dt'] = pd.to_datetime(df_final['TRACK DATE'], errors='coerce')
        cutoff_date = pd.Timestamp("2024-01-01")
        def calc_findings(row):
            dt = row['temp_track_dt']
            if pd.isna(dt) or dt < cutoff_date: return "INVALID DATE (< 2024)"
            crop_val = row['CROP AREA']; ver_val = row['VERIFIED AREA (Ha)']
            if isinstance(crop_val, str) or pd.isna(crop_val): return "NO CROP AREA"
            try:
                if float(ver_val) > (float(crop_val) + 2): return "ABOVE"
            except: pass
            return "OK"
        df_final['FINDINGS'] = df_final.apply(calc_findings, axis=1)
        df_final.drop(columns=['temp_track_dt'], inplace=True)
        
        missing_final = [c for c in FINAL_COLUMN_ORDER if c not in df_final.columns]
        if not missing_final: 
            df_final = df_final[FINAL_COLUMN_ORDER]
            if 'UPLOADER' in df_final.columns: df_final.sort_values(by=['UPLOADER', 'GEOREF ID'], inplace=True)

        df_final['VERIFIED AREA (Ha)'] = pd.to_numeric(df_final['VERIFIED AREA (Ha)'], errors='coerce').fillna(0)
        df_final['sum_area'] = df_final.apply(lambda x: x['VERIFIED AREA (Ha)'] if x['FINDINGS'] == 'OK' else 0, axis=1)
        
        df_summary = df_final.groupby('UPLOADER')[['sum_area']].sum().reset_index()
        df_summary.rename(columns={'sum_area': 'TOTAL VERIFIED AREA (Ha)'}, inplace=True)
        df_summary.sort_values('TOTAL VERIFIED AREA (Ha)', ascending=False, inplace=True)
        df_final.drop(columns=['sum_area'], inplace=True)

        with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
            df_summary.to_excel(writer, sheet_name='Uploader Summary', index=False)
            wb = writer.book; ws = writer.sheets['Uploader Summary']
            bold = wb.add_format({'bold': True, 'bg_color': '#D9EAD3'})
            num = wb.add_format({'num_format': '#,##0.00'})
            for c, v in enumerate(df_summary.columns): ws.write(0, c, v, bold)
            ws.set_column(0, 0, 35); ws.set_column(1, 1, 25, num)
            df_final.to_excel(writer, sheet_name='Clean Data', index=False)
        print(f"   ✅ Processed: {output_filename}")
    except Exception as e: print(f"   ❌ Error {base_name}: {e}")

def run_mode_5_workflow(input_dir, output_dir):
    print("\n--- Geotag Processor ---")
    parcel_path = select_input_file(input_dir, "Select Parcel List")
    if not parcel_path: return
    df_parcel, master_province = load_geotag_parcel_reference(parcel_path)
    if df_parcel is None: print("❌ Error loading Parcel List."); return
    
    print("\n   Select Target Files:")
    print("   [1] Single File  [2] BATCH PROCESS")
    mode = input("\nSelect: ").strip()
    files = []
    if mode == "1":
        f = select_input_file(input_dir, "Select Geotag File")
        if f: files.append(f)
    elif mode == "2":
        all_f = [os.path.join(input_dir, f) for f in os.listdir(input_dir) if f.lower().endswith(('.xlsx', '.csv'))]
        files = [f for f in all_f if os.path.abspath(f) != os.path.abspath(parcel_path)]
    
    if not files: return
    for f in files: process_single_geotag_logic(f, df_parcel, master_province, output_dir)
    print("\n🎉 Batch Complete!")

# --- MODE 6: CROSS-FILE AUDIT ---
def process_cross_file_audit(input_dir, output_dir):
    files = [f for f in os.listdir(input_dir) if f.lower().endswith('.xlsx') and not f.startswith('~$')]
    if not files: print("❌ No files."); return
    print("\n--- Cross-File Audit ---")
    all_data = []
    for filename in files:
        try:
            with LoadingSpinner(f"Reading {filename}..."):
                df = pd.read_excel(os.path.join(input_dir, filename))
                df.columns = [c.strip().upper() for c in df.columns]
                if 'GEOREF ID' not in df.columns: continue
                if 'FINDINGS' in df.columns: df = df[df['FINDINGS'] == 'OK']
                df['SOURCE_FILE'] = filename
                all_data.append(df)
        except: pass
    
    if not all_data: print("❌ No valid data."); return
    full_df = pd.concat(all_data, ignore_index=True)
    if full_df.empty: return
    
    dupe_mask = full_df.duplicated(subset=['GEOREF ID'], keep=False)
    df_duplicates = full_df[dupe_mask].sort_values(['PROVINCE', 'GEOREF ID'])
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    output_path = os.path.join(output_dir, f"Cross_File_Audit_Report_{timestamp}.xlsx")
    
    with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
        if not df_duplicates.empty:
            prov_counts = df_duplicates.groupby('PROVINCE')['GEOREF ID'].nunique().reset_index()
            uploader_counts = df_duplicates.groupby('UPLOADER')['GEOREF ID'].count().reset_index()
            prov_counts.to_excel(writer, sheet_name='Audit Summary', index=False)
            uploader_counts.to_excel(writer, sheet_name='Audit Summary', startrow=len(prov_counts)+3, index=False)
            for province, group in df_duplicates.groupby('PROVINCE'):
                cols = [c for c in ['GEOREF ID', 'UPLOADER', 'SOURCE_FILE', 'VERIFIED AREA (HA)', 'TRACK DATE'] if c in group.columns]
                group[cols].to_excel(writer, sheet_name=clean_sheet_name(str(province)), index=False)
        else:
            pd.DataFrame({'Status': ['CLEAN']}).to_excel(writer, sheet_name='Audit Summary', index=False)
    print(f"\n🎉 Audit Complete! Saved to {output_path}")

# --- MODE 7: GPX FIXER ---
def process_gpx_fixer(input_dir, output_dir):
    files = [f for f in os.listdir(input_dir) if f.lower().endswith('.gpx')]
    if not files: print("❌ No .gpx files."); return
    print("\n--- GPX Fixer (5s Interval) ---")
    
    ET.register_namespace('', "http://www.topografix.com/GPX/1/1")
    summary_data = []
    fixed_count = 0
    
    for filename in files:
        file_path = os.path.join(input_dir, filename)
        fixed_path = os.path.join(output_dir, f"{os.path.splitext(filename)[0]}[fixed].gpx")
        try:
            with LoadingSpinner(f"Processing {filename}..."):
                events = ('start-ns',); 
                try:
                    for event, (prefix, uri) in ET.iterparse(file_path, events):
                        if not prefix: prefix = ''; ET.register_namespace(prefix, uri)
                except: pass
                
                tree = ET.parse(file_path); root = tree.getroot()
                lats = []; lons = []; points_fixed = 0
                
                current_ele = 0.0; current_time = datetime(2024, 1, 1, 8, 0, 0)
                
                all_trkpts = [node for node in root.iter() if node.tag.endswith('trkpt')]
                if not all_trkpts: 
                    summary_data.append({'Filename': filename, 'Status': 'Error: No Points'}); continue

                for trkpt in all_trkpts:
                    time_tag = None
                    for child in trkpt:
                        if child.tag.endswith('time'): time_tag = child; break
                    if time_tag is not None and time_tag.text:
                        try:
                            current_time = datetime.fromisoformat(time_tag.text.replace('Z', ''))
                            break
                        except: pass

                for i, trkpt in enumerate(all_trkpts):
                    try: lats.append(float(trkpt.attrib['lat'])); lons.append(float(trkpt.attrib['lon']))
                    except: continue
                    
                    ele = None
                    for child in trkpt:
                        if child.tag.endswith('ele'): ele = child; break
                    
                    if ele is not None and ele.text:
                        try: current_ele = float(ele.text)
                        except: pass
                    else:
                        points_fixed += 1; new_ele = ET.Element('ele'); new_ele.text = f"{current_ele:.2f}"
                        trkpt.append(new_ele); ele = new_ele
                    
                    time_tag = None
                    for child in trkpt:
                        if child.tag.endswith('time'): time_tag = child; break
                    
                    new_time_str = (current_time + timedelta(seconds=i * 5)).isoformat() + "Z" # 5 Second Interval
                    if time_tag is None:
                        points_fixed += 1; new_time = ET.Element('time'); new_time.text = new_time_str
                        trkpt.append(new_time); time_tag = new_time
                    else:
                        if time_tag.text != new_time_str: points_fixed += 1
                        time_tag.text = new_time_str
                    
                    if ele is not None and time_tag is not None:
                        try: trkpt.remove(ele); trkpt.remove(time_tag)
                        except: pass
                        trkpt.insert(0, time_tag); trkpt.insert(0, ele)
                
                if points_fixed > 0:
                    tree.write(fixed_path, encoding='UTF-8', xml_declaration=True)
                    status = f"FIXED ({points_fixed})"
                    fixed_count += 1
                else: status = "OK"
                
                area = calculate_polygon_area(lats, lons)
                dist = 0.0
                if len(lats) > 1:
                    for i in range(len(lats)-1): dist += haversine_distance(lats[i], lons[i], lats[i+1], lons[i+1])
                    dist += haversine_distance(lats[-1], lons[-1], lats[0], lons[0])

                summary_data.append({'Filename': filename, 'Points': len(lats), 'Fixes': points_fixed, 'Perimeter': round(dist,2), 'Area': round(area,4), 'Status': status})
        except Exception as e: summary_data.append({'Filename': filename, 'Status': f"Error: {e}"})
    
    if summary_data:
        df = pd.DataFrame(summary_data)
        out = os.path.join(output_dir, f"GPX_Fix_Report_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx")
        with pd.ExcelWriter(out, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False)
            wb = writer.book; ws = writer.sheets['Sheet1']
            fmt = wb.add_format({'bg_color': '#FFF2CC', 'font_color': '#BF9000'})
            ws.conditional_format(1, 5, len(df), 5, {'type': 'text', 'criteria': 'containing', 'value': 'FIXED', 'format': fmt})
            ws.set_column(0, 0, 40)
        print(f"\n🎉 Done. Fixed: {fixed_count}. Report: {out}")

# --- MENU ---
def run_cli_app():
    clear_screen(); print_header()
    input_dir, output_dir, _ = ensure_directories()
    print(f"\n📍 Files: ./{INPUT_FOLDER_NAME}")
    
    while True:
        print("\nSelect Operation:")
        print("   [1] Stack Rows (Merge Files)")
        print("   [2] Masterlist Merger & Analyzer (Merge with Parcels)")
        print("   [3] Combine to Sheets (Tabs)")
        print("   [4] Generate Regional Summary (Analytics)")
        print("   [5] Geotag Processor (Clean & Enrich)")
        print("   [6] Cross-File Audit (Cheating Check)")
        print("   [7] GPX Fixer (5s Interval)")
        print("   [Q] Quit")
        
        choice = input("\nSelect: ").strip().upper()
        if choice == "1": run_stack_rows(input_dir, output_dir)
        elif choice == "2":
            m = select_input_file(input_dir, "Select Masterlist"); 
            if m: 
                p = select_input_file(input_dir, "Select Parcel List")
                if p: process_masterlist_merger(m, p, output_dir)
        elif choice == "3": run_combine_sheets(input_dir, output_dir)
        elif choice == "4":
            t = select_input_file(input_dir); 
            if t: process_rsbsa_report(t, output_dir)
        elif choice == "5": run_mode_5_workflow(input_dir, output_dir) # Renamed logic function
        elif choice == "6": process_cross_file_audit(input_dir, output_dir)
        elif choice == "7": process_gpx_fixer(input_dir, output_dir)
        elif choice == "Q": sys.exit(0)
        else: print("Invalid.")

if __name__ == "__main__":
    try: run_cli_app()
    except KeyboardInterrupt: sys.exit(0)