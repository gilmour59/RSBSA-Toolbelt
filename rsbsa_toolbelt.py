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
    'birthday'
]

# Mode 4: Geotag Cleaning Columns (Strict)
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

# Output Order for Mode 4
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

# --- MODE 3: RSBSA ANALYTICS ---

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

                    summary = df.groupby([col_mun, col_bgy]).agg({
                        'is_farmer': 'sum',
                        'is_farmworker': 'sum',
                        'is_fisher': 'sum',
                        col_agency: 'nunique',
                        'male_count': 'sum',
                        'female_count': 'sum',
                        'is_youth': 'sum',
                        'is_working_age': 'sum',
                        'is_senior': 'sum'
                    }).reset_index()

                    summary.columns = [
                        'Municipality', 'Barangay', 
                        'Farmers', 'Farmworkers', 'Fisherfolk', 
                        'Distinct Agencies', 'Male', 'Female',
                        'Youth (12-30)', 'Working Age (31-59)', 'Senior (60+)'
                    ]

                    summary = summary.sort_values(['Municipality', 'Barangay'])

                    summary.to_excel(writer, sheet_name=province, index=False, startrow=4)
                    
                    # Headers
                    workbook = writer.book
                    worksheet = writer.sheets[province]
                    header_format = workbook.add_format({'bold': True, 'font_size': 14})
                    date_format = workbook.add_format({'italic': True})
                    legend_format = workbook.add_format({'italic': True, 'font_color': 'gray', 'font_size': 10})
                    
                    worksheet.write('A1', f"RSBSA Summary Report - {province}", header_format)
                    worksheet.write('A2', f"As of: {as_of_str}", date_format)
                    worksheet.write('A3', "Age Legend: Youth (12-30) | Working Age (31-59) | Senior (60+)", legend_format)
                    
                    worksheet.set_column(0, 0, 20)
                    worksheet.set_column(1, 1, 25)
                    worksheet.set_column(2, 10, 15)

        print(f"\n🎉 Report Generated: {output_filename}")
        print(f"   Location: {output_path}")

    except Exception as e:
        print(f"❌ Critical Error: {e}")

# --- MODE 4: UNIFIED GEOTAG (BATCH/SINGLE + ENRICH) ---

def load_parcel_reference(parcel_path):
    """Loads and filters the Parcel List DataFrame."""
    with LoadingSpinner("Loading & Filtering Parcel List..."):
        # Read header first
        if parcel_path.lower().endswith('.csv'):
            df_head = pd.read_csv(parcel_path, nrows=1)
        else:
            df_head = pd.read_excel(parcel_path, nrows=1)
        
        cols = [c.strip() for c in df_head.columns]
        
        # Map Columns
        col_id = next((c for c in cols if c.upper() == 'FFRS SYSTEM GENERATED NO.'), None)
        col_area = next((c for c in cols if c.upper() == 'CROP AREA'), None)
        col_comm = next((c for c in cols if c.upper() == 'COMMODITY NAME'), None)
        
        # Determine Province Column in Parcel List
        col_prov = next((c for c in cols if c.upper() in ['PROVINCE', 'FARMER ADDRESS 3']), None)
        
        # Attempt to find Last Name column for sorting
        col_lname = next((c for c in cols if c.upper() == 'LAST NAME'), None)

        if not all([col_id, col_area, col_comm]):
            return None, None
        
        # Load Data
        usecols = [col_id, col_area, col_comm]
        if col_prov: usecols.append(col_prov)
        
        if parcel_path.lower().endswith('.csv'):
            df_parcel = pd.read_csv(parcel_path, usecols=usecols)
        else:
            df_parcel = pd.read_excel(parcel_path, usecols=usecols)
        
        rename_map = {col_id:'KEY_ID', col_area:'CROP AREA', col_comm:'COMMODITY'}
        if col_prov: rename_map[col_prov] = 'PROVINCE'
        
        df_parcel.rename(columns=rename_map, inplace=True)

        # Determine Master Province
        master_province = None
        if 'PROVINCE' in df_parcel.columns:
            master_province = df_parcel['PROVINCE'].mode()[0].strip().upper()

        # Filter Commodity
        mask = df_parcel['COMMODITY'].astype(str).str.contains(r'Rice|Palay|Corn|Sugarcane', flags=re.IGNORECASE, regex=True)
        df_parcel = df_parcel[mask]
        
        return df_parcel, master_province

def process_single_geotag_logic(geotag_path, df_parcel, master_province, output_dir):
    """Core logic for cleaning/enriching a single geotag file against loaded parcel data."""
    base_name = os.path.splitext(os.path.basename(geotag_path))[0]
    output_filename = f"{base_name} [clean_enriched].xlsx"
    dupe_filename = f"{base_name} [duplicates].xlsx"
    output_path = os.path.join(output_dir, output_filename)
    dupe_path = os.path.join(output_dir, dupe_filename)

    try:
        # LOAD GEOTAG
        if geotag_path.lower().endswith('.csv'):
            df_geo = pd.read_csv(geotag_path)
        else:
            df_geo = pd.read_excel(geotag_path)
        
        df_geo.columns = [c.strip() for c in df_geo.columns]
        missing = [c for c in TARGET_COLS_GEOTAG if c not in df_geo.columns]
        if missing:
            print(f"   ⚠️  Skipping {base_name}: Missing columns {missing}")
            return

        # PROVINCE CHECK
        if master_province:
            geo_prov = df_geo['PROVINCE'].mode()[0].strip().upper()
            if geo_prov != master_province:
                print(f"   🛑 Skipped {base_name}: Province mismatch (File: {geo_prov} != Parcel: {master_province})")
                return

        # Filter Columns
        df_geo = df_geo[TARGET_COLS_GEOTAG].copy()

        # Deduplicate GEOREF ID
        dupe_mask = df_geo.duplicated(subset=['GEOREF ID'], keep=False)
        df_duplicates = df_geo[dupe_mask].sort_values('GEOREF ID')
        df_clean_geo = df_geo.drop_duplicates(subset=['GEOREF ID'], keep='first')

        if not df_duplicates.empty:
            with pd.ExcelWriter(dupe_path, engine='xlsxwriter') as writer:
                df_duplicates.to_excel(writer, index=False)

        # MERGE
        # We do NOT need LAST_NAME here anymore as requested
        merge_cols = ['KEY_ID', 'CROP AREA', 'COMMODITY']

        df_merged = pd.merge(
            df_clean_geo,
            df_parcel[merge_cols], 
            left_on='RSBSA ID',
            right_on='KEY_ID',
            how='left',
            suffixes=('', '_parcel')
        )
        
        # MATCH
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

        # FINDINGS
        df_final['temp_track_dt'] = pd.to_datetime(df_final['TRACK DATE'], errors='coerce')
        cutoff_date = pd.Timestamp("2024-01-01")

        def calc_findings(row):
            dt = row['temp_track_dt']
            if pd.isna(dt) or dt < cutoff_date: return "INVALID DATE (< 2024)"
            
            crop_val = row['CROP AREA']
            ver_val = row['VERIFIED AREA (Ha)']
            if isinstance(crop_val, str) or pd.isna(crop_val): return "NO CROP AREA"
            
            try:
                if float(ver_val) > (float(crop_val) + 2): return "ABOVE"
            except: pass
            return "OK"

        df_final['FINDINGS'] = df_final.apply(calc_findings, axis=1)
        df_final.drop(columns=['temp_track_dt'], inplace=True)
        
        # Rearrange
        missing_final = [c for c in FINAL_COLUMN_ORDER if c not in df_final.columns]
        if not missing_final: 
            df_final = df_final[FINAL_COLUMN_ORDER]
            
            # SORTING LOGIC: By Uploader, then GEOREF ID
            if 'UPLOADER' in df_final.columns:
                df_final.sort_values(by=['UPLOADER', 'GEOREF ID'], inplace=True)

        # SUMMARY
        df_final['VERIFIED AREA (Ha)'] = pd.to_numeric(df_final['VERIFIED AREA (Ha)'], errors='coerce').fillna(0)
        df_final['sum_area'] = df_final.apply(lambda x: x['VERIFIED AREA (Ha)'] if x['FINDINGS'] == 'OK' else 0, axis=1)
        
        df_summary = df_final.groupby('UPLOADER')[['sum_area']].sum().reset_index()
        df_summary.rename(columns={'sum_area': 'TOTAL VERIFIED AREA (Ha)'}, inplace=True)
        df_summary.sort_values('TOTAL VERIFIED AREA (Ha)', ascending=False, inplace=True)
        
        df_final.drop(columns=['sum_area'], inplace=True)

        # SAVE
        with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
            df_summary.to_excel(writer, sheet_name='Uploader Summary', index=False)
            
            # Formatting
            wb = writer.book
            ws = writer.sheets['Uploader Summary']
            bold = wb.add_format({'bold': True, 'bg_color': '#D9EAD3'})
            num = wb.add_format({'num_format': '#,##0.00'})
            for c, v in enumerate(df_summary.columns): ws.write(0, c, v, bold)
            ws.set_column(0, 0, 35)
            ws.set_column(1, 1, 25, num)
            
            df_final.to_excel(writer, sheet_name='Clean Data', index=False)

        print(f"   ✅ Processed: {output_filename}")

    except Exception as e:
        print(f"   ❌ Error processing {base_name}: {e}")

def run_mode_4_workflow(input_dir, output_dir):
    print("\n--- Geotag Processor & Enricher ---")
    print("   STEP 1: Select the MASTER PARCEL LIST")
    parcel_path = select_input_file(input_dir, "Select Parcel List")
    if not parcel_path: return

    # Load Parcel Data Once
    df_parcel, master_province = load_parcel_reference(parcel_path)
    if df_parcel is None:
        print("❌ Error loading Parcel List.")
        return
    
    if master_province:
        print(f"   📍 Master Province Detected: {master_province}")
    else:
        print("   ⚠️  No Province column found in Parcel List. Skipping province safety check.")

    print("\n   STEP 2: Select Target Geotag Files")
    print("   [1] Select a single file")
    print("   [2] BATCH PROCESS (All other files in folder)")
    
    mode = input("\nSelect option: ").strip()
    
    files_to_process = []
    
    if mode == "1":
        f = select_input_file(input_dir, "Select Geotag File")
        if f: files_to_process.append(f)
    elif mode == "2":
        all_files = [os.path.join(input_dir, f) for f in os.listdir(input_dir) 
                     if f.lower().endswith(('.xlsx', '.csv')) and not f.startswith('~$')]
        # Exclude the parcel file itself
        files_to_process = [f for f in all_files if os.path.abspath(f) != os.path.abspath(parcel_path)]
        print(f"   Found {len(files_to_process)} files to process.")
    else:
        print("Invalid selection.")
        return

    if not files_to_process:
        print("No files to process.")
        return

    print("\n--- Starting Batch Processing ---")
    for geo_file in files_to_process:
        process_single_geotag_logic(geo_file, df_parcel, master_province, output_dir)
    
    print("\n🎉 Batch Processing Complete!")

# --- MODE 5: CROSS-FILE AUDIT ---

def process_cross_file_audit(input_dir, output_dir):
    """
    Scans all Excel files in input folder (Processed Files).
    Filters rows where FINDINGS == 'OK'.
    Checks for duplicate GEOREF IDs across the entire dataset.
    Splits by Province.
    """
    files = [f for f in os.listdir(input_dir) if f.lower().endswith('.xlsx') and not f.startswith('~$')]
    
    if not files:
        print("❌ No Excel files found to audit.")
        return

    print("\n--- Cross-File Audit (Cheating Detection) ---")
    print(f"   Scanning {len(files)} files for duplicate GEOREF IDs among 'OK' entries...")

    all_data = []
    
    # 1. Load All Data
    for filename in files:
        file_path = os.path.join(input_dir, filename)
        try:
            with LoadingSpinner(f"Reading {filename}..."):
                # We expect columns from Mode 4 output, specifically GEOREF ID, PROVINCE, FINDINGS, UPLOADER
                # Load potentially large files, so read relevant cols if possible, or all if unsure
                df = pd.read_excel(file_path)
                
                # Normalize columns
                df.columns = [c.strip().upper() for c in df.columns]
                
                # Check minimum requirements
                if 'GEOREF ID' not in df.columns or 'PROVINCE' not in df.columns:
                    continue # Skip files that aren't Mode 4 outputs
                
                # Filter for OK Findings (if column exists, otherwise take all)
                if 'FINDINGS' in df.columns:
                    df = df[df['FINDINGS'] == 'OK']
                
                # Tag source
                df['SOURCE_FILE'] = filename
                all_data.append(df)
                
        except Exception as e:
            print(f"   ⚠️ Error reading {filename}: {e}")

    if not all_data:
        print("❌ No valid data found to audit.")
        return

    # 2. Consolidate
    print("   Consolidating data...")
    full_df = pd.concat(all_data, ignore_index=True)
    
    if full_df.empty:
        print("   No 'OK' rows found to audit.")
        return

    # 3. Find Duplicates
    print("   Analyzing duplicates...")
    # Mark all duplicates (keep=False) so we see every instance of the cheating
    dupe_mask = full_df.duplicated(subset=['GEOREF ID'], keep=False)
    df_duplicates = full_df[dupe_mask].sort_values(['PROVINCE', 'GEOREF ID'])
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    output_filename = f"Cross_File_Audit_Report_{timestamp}.xlsx"
    output_path = os.path.join(output_dir, output_filename)

    # 4. Save Report
    try:
        with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
            
            # SHEET 1: SUMMARY
            summary_list = []
            if not df_duplicates.empty:
                # Count duplicates per province
                prov_counts = df_duplicates.groupby('PROVINCE')['GEOREF ID'].nunique().reset_index()
                prov_counts.columns = ['Province', 'Unique Duplicate IDs']
                
                # Count involved uploaders
                uploader_counts = df_duplicates.groupby('UPLOADER')['GEOREF ID'].count().reset_index()
                uploader_counts.columns = ['Uploader', 'Total Duplicate Entries']
                uploader_counts = uploader_counts.sort_values('Total Duplicate Entries', ascending=False)
                
                # Write to sheet
                prov_counts.to_excel(writer, sheet_name='Audit Summary', startrow=0, index=False)
                uploader_counts.to_excel(writer, sheet_name='Audit Summary', startrow=len(prov_counts)+3, index=False)
                
                workbook = writer.book
                ws = writer.sheets['Audit Summary']
                bold = workbook.add_format({'bold': True})
                ws.write(len(prov_counts)+2, 0, "Top Offenders (Uploaders)", bold)
                ws.set_column(0, 1, 25)
            else:
                pd.DataFrame({'Status': ['CLEAN - No Cross-File Duplicates Found']}).to_excel(writer, sheet_name='Audit Summary', index=False)

            # SHEET 2+: PROVINCE DETAILS
            if not df_duplicates.empty:
                # Group by Province for separate sheets
                grouped = df_duplicates.groupby('PROVINCE')
                for province, group in grouped:
                    # Clean sheet name
                    safe_name = clean_sheet_name(str(province))
                    
                    # Select relevant cols
                    audit_cols = ['GEOREF ID', 'UPLOADER', 'SOURCE_FILE', 'VERIFIED AREA (HA)', 'TRACK DATE']
                    # Use intersection to avoid key errors if cols missing
                    cols_to_use = [c for c in audit_cols if c in group.columns]
                    
                    group[cols_to_use].to_excel(writer, sheet_name=safe_name, index=False)
                    
                    # Formatting
                    ws = writer.sheets[safe_name]
                    ws.set_column(0, 0, 25) # Georef
                    ws.set_column(1, 2, 30) # Uploader/File

        print(f"\n🎉 Audit Complete!")
        if df_duplicates.empty:
            print("   ✅ CLEAN: No duplicates found across files.")
        else:
            print(f"   ⚠️  FOUND {len(df_duplicates)} duplicate entries!")
            print(f"   Report saved to: {output_filename}")

    except Exception as e:
        print(f"❌ Save Error: {e}")

# --- MODE 6: GPX FIXER ---

def process_gpx_fixer(input_dir, output_dir):
    """Scans .gpx files, validates, fixes missing tags, and exports Summary."""
    
    files = [f for f in os.listdir(input_dir) if f.lower().endswith('.gpx')]
    
    if not files:
        print("❌ No .gpx files found.")
        return

    print(f"\n--- GPX Fixer & Processor ({len(files)} files) ---")
    print("   Scanning for missing <ele> or <time> tags...")
    
    # --- DYNAMIC NAMESPACE PRESERVATION ---
    ET.register_namespace('', "http://www.topografix.com/GPX/1/1")
    
    summary_data = []
    fixed_count = 0
    
    for filename in files:
        file_path = os.path.join(input_dir, filename)
        fixed_filename = f"{os.path.splitext(filename)[0]}[fixed].gpx"
        fixed_path = os.path.join(output_dir, fixed_filename)
        
        try:
            with LoadingSpinner(f"Processing {filename}..."):
                
                # Scan file to find used namespaces
                events = ('start-ns',)
                try:
                    for event, (prefix, uri) in ET.iterparse(file_path, events):
                        if not prefix: prefix = ''
                        ET.register_namespace(prefix, uri)
                except: pass 
                
                tree = ET.parse(file_path)
                root = tree.getroot()
                
                # Namespace map for searching
                ns = {'gpx': 'http://www.topografix.com/GPX/1/1'}
                
                lats = []
                lons = []
                missing_tags = 0
                points_fixed = 0
                
                current_ele = 0.0 
                
                # --- SEQUENTIAL RETIMING LOGIC START ---
                # Default anchor if none found
                current_time = datetime(2024, 1, 1, 8, 0, 0)
                has_anchor = False
                
                all_trkpts = []
                for node in root.iter():
                    if node.tag.endswith('trkpt'):
                        all_trkpts.append(node)
                        
                if not all_trkpts:
                    summary_data.append({'Filename': filename, 'Status': 'Error: No Points'})
                    continue

                # 1. Find Anchor (T0) from first valid timestamp
                for trkpt in all_trkpts:
                    time_tag = None
                    for child in trkpt:
                        if child.tag.endswith('time'):
                            time_tag = child
                            break
                    
                    if time_tag is not None and time_tag.text:
                        try:
                            t_str = time_tag.text.replace('Z', '')
                            current_time = datetime.fromisoformat(t_str)
                            has_anchor = True
                            break # Found anchor
                        except: pass
                
                # 2. Sequential Rewrite Loop (1Hz)
                for i, trkpt in enumerate(all_trkpts):
                    # Get Geometry
                    try:
                        lat = float(trkpt.attrib['lat'])
                        lon = float(trkpt.attrib['lon'])
                        lats.append(lat)
                        lons.append(lon)
                    except:
                        continue 

                    # HANDLE ELEVATION (Forward Fill)
                    ele = None
                    for child in trkpt:
                        if child.tag.endswith('ele'):
                            ele = child
                            break
                    
                    if ele is not None and ele.text:
                        try: current_ele = float(ele.text)
                        except: pass
                    else:
                        missing_tags += 1
                        points_fixed += 1
                        new_ele = ET.Element('ele')
                        new_ele.text = f"{current_ele:.2f}"
                        trkpt.append(new_ele)

                    # HANDLE TIME (Strict 1Hz Sequential)
                    time_tag = None
                    for child in trkpt:
                        if child.tag.endswith('time'):
                            time_tag = child
                            break
                    
                    # Calculate exactly T0 + i * 5 seconds (Modified to 5s)
                    new_timestamp = current_time + timedelta(seconds=i * 5)
                    new_time_str = new_timestamp.isoformat() + "Z"
                    
                    if time_tag is None:
                        # Create new tag
                        missing_tags += 1
                        points_fixed += 1
                        new_time = ET.Element('time')
                        new_time.text = new_time_str
                        trkpt.append(new_time)
                    else:
                        # Overwrite existing tag
                        if time_tag.text != new_time_str:
                            # Only count as fix if we actually changed it
                            points_fixed += 1
                        time_tag.text = new_time_str

                # Save Fixed File
                if points_fixed > 0:
                    tree.write(fixed_path, encoding='UTF-8', xml_declaration=True)
                    status = f"FIXED ({points_fixed} tags added/updated)"
                    fixed_count += 1
                else:
                    status = "OK"

                area_ha = calculate_polygon_area(lats, lons)
                
                dist_m = 0.0
                if len(lats) > 1:
                    for i in range(len(lats)-1):
                        dist_m += haversine_distance(lats[i], lons[i], lats[i+1], lons[i+1])
                    dist_m += haversine_distance(lats[-1], lons[-1], lats[0], lons[0])

                summary_data.append({
                    'Filename': filename,
                    'Points': len(lats),
                    'Fixes Applied': points_fixed,
                    'Perimeter (m)': round(dist_m, 2),
                    'Area (Ha)': round(area_ha, 4),
                    'Status': status
                })

        except Exception as e:
            summary_data.append({'Filename': filename, 'Status': f"Error: {str(e)}"})

    if summary_data:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        output_filename = f"GPX_Fix_Report_{timestamp}.xlsx"
        output_path = os.path.join(output_dir, output_filename)
        
        df = pd.DataFrame(summary_data)
        
        with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False)
            workbook = writer.book
            worksheet = writer.sheets['Sheet1']
            yellow_fmt = workbook.add_format({'bg_color': '#FFF2CC', 'font_color': '#BF9000'})
            worksheet.conditional_format(1, 5, len(df), 5, {'type': 'text', 'criteria': 'containing', 'value': 'FIXED', 'format': yellow_fmt})
            worksheet.set_column(0, 0, 40)
            
        print(f"\n🎉 Processed {len(files)} files.")
        print(f"   files fixed: {fixed_count}")
        print(f"   Report: {output_filename}")

# --- RE-INCLUDED HELPER FUNCTIONS FOR MODES 1 & 2 ---
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
                # Check for 'last_name' or similar column to sort by
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

# --- MENU LOGIC ---

def run_cli_app():
    clear_screen()
    print_header()

    input_dir, output_dir, just_created = ensure_directories()
    
    print(f"\n📍 Looking for files in: ./{INPUT_FOLDER_NAME}")
    print(f"📍 Saving results to:    ./{OUTPUT_FOLDER_NAME}")

    if just_created:
        print("\n✨ Setup complete.")
        print(f"👉 Please copy your files (.xlsx, .csv, .gpx) into '{INPUT_FOLDER_NAME}'")
        input("   Press Enter when you are ready...")

    while True:
        print("\nSelect Operation:")
        print("   [1] Stack Rows (Strict Mode - Merge files with same columns)")
        print("   [2] Combine to Sheets (Group files into tabs)")
        print("   [3] Generate Regional Summary (Analytics per Barangay)")
        print("   [4] Geotag Processor (Clean & Add Crop Area)")
        print("   [5] Cross-File Audit (Detect Cheating/Duplicates across files)")
        print("   [6] GPX Fixer & Calculator (Auto-add missing ele/time)")
        print("   [Q] Quit")
        
        choice = input("\nSelect option: ").strip().upper()

        if choice == "1":
            run_stack_rows(input_dir, output_dir)
        elif choice == "2":
            run_combine_sheets(input_dir, output_dir)
        elif choice == "3":
            target_file = select_input_file(input_dir)
            if target_file:
                process_rsbsa_report(target_file, output_dir)
        elif choice == "4":
            run_mode_4_workflow(input_dir, output_dir)
        elif choice == "5":
            process_cross_file_audit(input_dir, output_dir)
        elif choice == "6":
            process_gpx_fixer(input_dir, output_dir)
        elif choice == "Q":
            sys.exit(0)
        else:
            print("Invalid selection.")

if __name__ == "__main__":
    try:
        run_cli_app()
    except KeyboardInterrupt:
        sys.exit(0)