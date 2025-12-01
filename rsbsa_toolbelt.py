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
from difflib import SequenceMatcher

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

def similar(a, b):
    """Simple string similarity ratio (0.0 to 1.0)"""
    return SequenceMatcher(None, str(a), str(b)).ratio()

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
    MODE 2: TRIAGE SYSTEM (Granular - One Row Per Parcel)
    
    Logic:
    1. Identify Clean vs Erroneous Farmers in Masterlist.
    2. JOIN Clean Farmers with Parcel List (One-to-Many).
    3. Calculate 'HAS_MULTIPLE_LAND_HOLDINGS'.
    4. Separate into 'With Parcels', 'No Parcels', 'Erroneous'.
    """
    
    # Naming: {MasterlistName}-merged.xlsx
    base_name = os.path.splitext(os.path.basename(master_path))[0]
    output_filename = f"{base_name}-merged.xlsx"
    output_path = os.path.join(output_dir, output_filename)

    print(f"\n--- Starting Triage Analysis for: {base_name} ---")
    
    try:
        # --- 1. LOAD MASTERLIST ---
        with LoadingSpinner("Loading Masterlist..."):
            if master_path.lower().endswith('.csv'): df_m = pd.read_csv(master_path)
            else: df_m = pd.read_excel(master_path)
            
            df_m.columns = [c.strip().lower() for c in df_m.columns]
            
            # Find RSBSA ID
            col_rsbsa = next((c for c in df_m.columns if 'rsbsa' in c and 'no' in c), None)
            if not col_rsbsa:
                print("❌ Error: RSBSA No. column not found.")
                return
            
            df_m['KEY_ID'] = df_m[col_rsbsa].astype(str).str.strip().str.upper()
            df_m['DATA_STATUS'] = 'CLEAN' 
            df_m['ERROR_TAG'] = ''   

        # --- 2. LOAD PARCEL LIST ---
        with LoadingSpinner("Loading Parcel List..."):
            if parcel_path.lower().endswith('.csv'): df_p = pd.read_csv(parcel_path)
            else: df_p = pd.read_excel(parcel_path)
            
            # Preserve original column names for output, but lower them for searching
            original_p_cols = list(df_p.columns)
            df_p.columns = [c.strip().lower() for c in df_p.columns]
            
            # Find Key Columns
            col_ffrs = next((c for c in df_p.columns if 'ffrs' in c or 'system generated' in c), None)
            
            if not col_ffrs:
                print("❌ Error: FFRS ID column not found in Parcel List.")
                return
            
            # Create Key
            df_p['KEY_ID'] = df_p[col_ffrs].astype(str).str.strip().str.upper()
            
            # FLAG MULTIPLE HOLDINGS (New Feature)
            # Count how many times each ID appears in the parcel list
            df_p['parcel_count_temp'] = df_p.groupby('KEY_ID')['KEY_ID'].transform('count')
            df_p['HAS_MULTIPLE_LAND_HOLDINGS'] = df_p['parcel_count_temp'].apply(lambda x: 'YES' if x > 1 else 'NO')
            
            # Restore original column names (mapped) so output looks nice
            # We map the lowercase cols back to original, but keep KEY_ID and HAS_MULTIPLE...
            col_map = {c.strip().lower(): c for c in original_p_cols}
            
            # We need to drop the temporary lowercased columns and keep the calculated ones + Original content
            # Strategy: Rename the current lowercased columns back to Title Case
            df_p = df_p.rename(columns=col_map)

        # --- 3. FLAGGING ERRORS (TRIAGE) ---
        with LoadingSpinner("Triaging: Detecting Duplicates & Conflicts..."):
            
            # A. STRICT DUPLICATE IDs (Masterlist side)
            dup_mask = df_m.duplicated(subset=['KEY_ID'], keep=False)
            df_m.loc[dup_mask, 'DATA_STATUS'] = 'ERROR'
            df_m.loc[dup_mask, 'ERROR_TAG'] += '[Duplicate RSBSA ID] '

            # B. FUZZY IDENTITY CONFLICTS
            m_fname = next((c for c in df_m.columns if 'first' in c and 'name' in c), 'first_name')
            m_lname = next((c for c in df_m.columns if 'last' in c and 'name' in c), 'last_name')
            m_bday = next((c for c in df_m.columns if 'birth' in c), 'birthday')

            df_m['LOOSE_SIG'] = (
                df_m[m_lname].fillna('').astype(str).str.strip().str.upper() + 
                df_m[m_bday].astype(str)
            )
            
            potential_dupes = df_m[df_m.duplicated(subset=['LOOSE_SIG'], keep=False)]
            conflict_ids = set()
            
            if not potential_dupes.empty:
                for sig, group in potential_dupes.groupby('LOOSE_SIG'):
                    if len(group) < 2: continue
                    rows = group.to_dict('records')
                    for i in range(len(rows)):
                        for j in range(i + 1, len(rows)):
                            r1, r2 = rows[i], rows[j]
                            if r1['KEY_ID'] == r2['KEY_ID']: continue 
                            name1 = f"{r1.get(m_fname,'')} {r1.get(m_lname,'')}".strip().upper()
                            name2 = f"{r2.get(m_fname,'')} {r2.get(m_lname,'')}".strip().upper()
                            if similar(name1, name2) > 0.85:
                                conflict_ids.add(r1['KEY_ID'])
                                conflict_ids.add(r2['KEY_ID'])

            if conflict_ids:
                is_conflict = df_m['KEY_ID'].isin(conflict_ids)
                df_m.loc[is_conflict, 'DATA_STATUS'] = 'ERROR'
                df_m.loc[is_conflict, 'ERROR_TAG'] += '[Identity Conflict] '

        # --- 4. MERGING (ONE-TO-MANY) ---
        with LoadingSpinner("Merging Masterlist with Parcels..."):
            
            # Split Masterlist into Clean and Error
            df_m_clean = df_m[df_m['DATA_STATUS'] == 'CLEAN'].copy()
            df_m_error = df_m[df_m['DATA_STATUS'] == 'ERROR'].copy()
            
            # MERGE STEP: Left Join Clean Farmers -> Parcel List
            # This preserves multiple rows for the same farmer
            df_merged = pd.merge(df_m_clean, df_p, on='KEY_ID', how='left')
            
            # Determine status based on merge
            # If a column from df_p is NaN, it means they have no parcel
            # We pick a column we know exists in df_p to check, e.g., 'HAS_MULTIPLE_LAND_HOLDINGS'
            df_merged['HAS_PARCEL'] = df_merged['HAS_MULTIPLE_LAND_HOLDINGS'].notna()
            
            # Fill NaN for farmers with no parcels
            df_merged['HAS_MULTIPLE_LAND_HOLDINGS'] = df_merged['HAS_MULTIPLE_LAND_HOLDINGS'].fillna('NO')

        # --- 5. SPLIT, SORT & SAVE ---
        with LoadingSpinner("Splitting and Sorting..."):
            
            # Split into the 3 buckets
            df_with = df_merged[df_merged['HAS_PARCEL'] == True].copy()
            df_no = df_merged[df_merged['HAS_PARCEL'] == False].copy()
            
            # Clean up columns (Remove helper cols)
            drop_cols = ['LOOSE_SIG', 'DATA_STATUS', 'ERROR_TAG', 'HAS_PARCEL', 'parcel_count_temp']
            final_cols_with = [c for c in df_with.columns if c not in drop_cols]
            final_cols_no = [c for c in df_no.columns if c not in drop_cols and c not in df_p.columns] # Remove parcel cols from 'No Parcel' sheet
            
            # Re-attach the basic Masterlist columns for the "No Parcel" sheet if they got dropped
            # Actually, the merge keeps them. We just want to ensure we don't show empty Parcel columns for "No Parcel" guys.
            # Strategy: Keep only Masterlist columns for df_no
            df_no = df_no[[c for c in df_m.columns if c not in ['LOOSE_SIG', 'DATA_STATUS', 'ERROR_TAG']]]
            
            # --- SORTING LOGIC ---
            # Municipality -> Barangay -> Last Name -> (New) Parcel Address/Commodity
            sort_keys = []
            m_mun = next((c for c in df_with.columns if 'mun' in c.lower() and 'address' in c.lower()), None)
            m_bgy = next((c for c in df_with.columns if 'bgy' in c.lower() and 'address' in c.lower()), None)
            m_last = next((c for c in df_with.columns if 'last' in c.lower() and 'name' in c.lower()), None)
            
            if m_mun: sort_keys.append(m_mun)
            if m_bgy: sort_keys.append(m_bgy)
            if m_last: sort_keys.append(m_last)
            
            if sort_keys:
                print(f"   Sorting by: {' -> '.join(sort_keys)}")
                df_with.sort_values(by=sort_keys, inplace=True)
                df_no.sort_values(by=sort_keys, inplace=True)
                if 'ERROR_TAG' in df_m_error.columns:
                     # For errors, we need to ensure the sort keys exist (they are from masterlist)
                     err_sort = ['ERROR_TAG'] + [k for k in sort_keys if k in df_m_error.columns]
                     df_m_error.sort_values(by=err_sort, inplace=True)

            # SAVE
            with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
                df_with.to_excel(writer, sheet_name='Clean - With Parcels', index=False)
                df_no.to_excel(writer, sheet_name='Clean - No Parcels', index=False)
                
                # For Error Sheet, we only have Masterlist data (no merge done on errors)
                # But we might want to see if those errors *had* parcels. 
                # For now, following strict logic: Errors are quarantined before merging.
                df_m_error.to_excel(writer, sheet_name='Erroneous & Conflicts', index=False)
                
                # Formatting
                wb = writer.book
                ws_err = writer.sheets['Erroneous & Conflicts']
                red_fmt = wb.add_format({'font_color': '#9C0006', 'bg_color': '#FFC7CE'})
                ws_err.conditional_format(1, 0, len(df_m_error), 0, {'type': 'no_blanks', 'format': red_fmt})

        print(f"🎉 Processed: {output_filename}")
        print(f"   (Granular Format: Multiple rows per farmer preserved)")

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        
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
        
        # Sort Final Stacked Data as well if column exists
        sort_col_final = next((c for c in final_df.columns if 'last' in c.lower() and 'name' in c.lower()), None)
        if sort_col_final:
            final_df.sort_values(by=sort_col_final, inplace=True)

        path = os.path.join(output_dir, output_filename)
        with pd.ExcelWriter(path, engine='xlsxwriter') as writer:
            final_df.to_excel(writer, index=False)
        print(f"🎉 Saved: {path}")

# --- MODE 3: COMBINE SHEETS (Renamed from 2) ---

def run_regional_consolidation(input_dir, output_dir):
    """
    MODE 3: REGIONAL CONSOLIDATOR (Flexible)
    1. Scans input folder for Mode 2 outputs.
    2. VALIDATION: 
       - STRICT: No Duplicates (e.g., cannot have 2 Aklan files).
       - RELAXED: Allows incomplete sets (e.g., can run with just 3 provinces).
    3. PROCESSING: Combines available files into 3 Master Files.
    """
    
    print("\n--- Starting Regional Consolidation (Mode 3) ---")
    
    # We use the global REQUIRED_PROVINCES to validate file contents, 
    # but we don't force all of them to be present.
    
    files = [f for f in os.listdir(input_dir) if f.endswith('.xlsx') and not f.startswith('~$')]
    
    if not files:
        print("❌ No Excel files found in input directory.")
        return

    province_map = {} # {'AKLAN': 'filename.xlsx'}
    
    # --- STEP 1: INVENTORY & VALIDATION ---
    print("\n🔍 Scanning files...")
    
    for f in files:
        f_path = os.path.join(input_dir, f)
        try:
            # Peek at 'Clean - With Parcels' to identify province
            # Reading only 50 rows for speed
            df_peek = pd.read_excel(f_path, sheet_name='Clean - With Parcels', nrows=50)
            
            # Find province column
            col_prov = next((c for c in df_peek.columns if 'farmer_address_prv' in c.lower()), None)
            
            if not col_prov:
                print(f"⚠️  Skipping {f}: Column 'farmer_address_prv' not found.")
                continue
                
            prov_list = df_peek[col_prov].dropna().unique()
            if len(prov_list) == 0:
                print(f"⚠️  Skipping {f}: No province data found.")
                continue
            
            detected_prov = str(prov_list[0]).strip().upper()
            
            # Match against valid Region 6 list
            matched_key = None
            for req in REQUIRED_PROVINCES:
                if req in detected_prov:
                    matched_key = req
                    break
            
            if not matched_key:
                print(f"⚠️  Skipping {f}: Province '{detected_prov}' is not in Region 6 list.")
                continue
                
            # STRICT DUPLICATE CHECK
            if matched_key in province_map:
                print(f"❌ CRITICAL ERROR: Duplicate files found for {matched_key}!")
                print(f"   File 1: {province_map[matched_key]}")
                print(f"   File 2: {f}")
                print("   Strict Mode forbids duplicates. Please remove one.")
                return

            province_map[matched_key] = f
            print(f"   ✅ Mapped {matched_key.ljust(18)} -> {f}")

        except ValueError:
            print(f"⚠️  Skipping {f}: Not a valid Mode 2 output (missing sheets).")
        except Exception as e:
            print(f"⚠️  Error reading {f}: {e}")

    # --- STEP 2: CHECKING COVERAGE (Relaxed) ---
    found_provinces = set(province_map.keys())
    missing = REQUIRED_PROVINCES - found_provinces
    
    if not found_provinces:
        print("\n❌ No valid province files found to merge.")
        return

    if missing:
        print(f"\n⚠️  Note: Missing files for: {', '.join(missing)}")
        print("   Proceeding with the available provinces only...")
    else:
        print("\n✅ All 6 Provinces Accounted For.")

    # --- STEP 3: CONSOLIDATION ---
    outputs = {
        'With_Parcels': {'sheet_src': 'Clean - With Parcels', 'filename': 'Regional_With_Parcels.xlsx'},
        'No_Parcels':   {'sheet_src': 'Clean - No Parcels',   'filename': 'Regional_No_Parcels.xlsx'},
        'Erroneous':    {'sheet_src': 'Erroneous & Conflicts','filename': 'Regional_Erroneous.xlsx'}
    }

    try:
        for key, config in outputs.items():
            out_path = os.path.join(output_dir, config['filename'])
            print(f"   Generating {config['filename']}...")
            
            with pd.ExcelWriter(out_path, engine='xlsxwriter') as writer:
                # Iterate through FOUND provinces (sorted alphabetically)
                for prov in sorted(list(found_provinces)):
                    f_name = province_map[prov]
                    f_path = os.path.join(input_dir, f_name)
                    
                    try:
                        df_sheet = pd.read_excel(f_path, sheet_name=config['sheet_src'])
                        df_sheet.to_excel(writer, sheet_name=prov, index=False)
                        
                        if key == 'Erroneous':
                            wb = writer.book
                            ws = writer.sheets[prov]
                            red_fmt = wb.add_format({'font_color': '#9C0006', 'bg_color': '#FFC7CE'})
                            ws.conditional_format(1, 0, len(df_sheet), 0, {'type': 'no_blanks', 'format': red_fmt})
                            
                    except Exception as e:
                        print(f"   ⚠️ Could not read {config['sheet_src']} from {prov}: {e}")
        
        print("\n🎉 Regional Consolidation Complete!")

    except Exception as e:
        print(f"❌ Error during merging: {e}")

# --- MODE 4: REGIONAL SUMMARY (Renamed from 3) ---

def run_regional_analytics_mode4(input_dir, output_dir):
    """
    MODE 4: REGIONAL DASHBOARD GENERATOR
    Analyzes the 3 outputs from Mode 3:
      1. Regional_With_Parcels.xlsx
      2. Regional_No_Parcels.xlsx
      3. Regional_Erroneous.xlsx
    
    Generates: 'Regional_Analytics_Dashboard.xlsx'
    """
    
    print("\n--- Starting Regional Analytics (Mode 4) ---")
    
    # Expected Inputs (Must match Mode 3 outputs)
    files = {
        'PARCEL': 'Regional_With_Parcels.xlsx',
        'NO_PARCEL': 'Regional_No_Parcels.xlsx',
        'ERROR': 'Regional_Erroneous.xlsx'
    }
    
    # Check if files exist in the target directory
    for key, fname in files.items():
        f_path = os.path.join(input_dir, fname)
        if not os.path.exists(f_path):
            print(f"❌ Critical Error: Missing '{fname}'")
            print(f"   Ensure you have run Mode 3 and selected the correct folder (Input/Output).")
            return

    # Initialize Stats Storage
    stats = {p: {
        'Farmers_With_Land': 0, 'Farmers_No_Land': 0, 'Erroneous_Entries': 0,
        'Rice_Area_Ha': 0.0, 'Corn_Area_Ha': 0.0, 'Sugar_Area_Ha': 0.0, 'Total_Land_Ha': 0.0,
        'Male': 0, 'Female': 0, 'Youth_12_30': 0, 'Senior_60_Up': 0
    } for p in REQUIRED_PROVINCES}

    try:
        # --- PHASE 1: ANALYZE FARMERS WITH PARCELS ---
        path_p = os.path.join(input_dir, files['PARCEL'])
        with LoadingSpinner("Analyzing Land Data..."):
            xls = pd.ExcelFile(path_p)
            for prov in REQUIRED_PROVINCES:
                if prov in xls.sheet_names:
                    df = pd.read_excel(xls, sheet_name=prov)
                    
                    stats[prov]['Farmers_With_Land'] = len(df)
                    stats[prov]['Rice_Area_Ha'] = df['AREA_RICE_HA'].sum()
                    stats[prov]['Corn_Area_Ha'] = df['AREA_CORN_HA'].sum()
                    stats[prov]['Sugar_Area_Ha'] = df['AREA_SUGAR_HA'].sum()
                    stats[prov]['Total_Land_Ha'] = df['TOTAL_PARCEL_AREA_HA'].sum()
                    
                    # Demographics (Sex)
                    col_sex = next((c for c in df.columns if 'sex' in c.lower() or 'gender' in c.lower()), None)
                    if col_sex:
                        s_counts = df[col_sex].astype(str).str.upper().value_counts()
                        stats[prov]['Male'] += s_counts.get('MALE', 0) + s_counts.get('M', 0)
                        stats[prov]['Female'] += s_counts.get('FEMALE', 0) + s_counts.get('F', 0)
                    
                    # Demographics (Age)
                    col_bday = next((c for c in df.columns if 'birth' in c.lower()), None)
                    if col_bday:
                        df[col_bday] = pd.to_datetime(df[col_bday], errors='coerce')
                        now = datetime.now()
                        df['AGE'] = (now - df[col_bday]).dt.days // 365
                        stats[prov]['Youth_12_30'] += len(df[(df['AGE'] >= 12) & (df['AGE'] <= 30)])
                        stats[prov]['Senior_60_Up'] += len(df[df['AGE'] >= 60])

        # --- PHASE 2: ANALYZE FARMERS NO PARCELS ---
        path_np = os.path.join(input_dir, files['NO_PARCEL'])
        with LoadingSpinner("Analyzing Farmers w/o Land..."):
            xls = pd.ExcelFile(path_np)
            for prov in REQUIRED_PROVINCES:
                if prov in xls.sheet_names:
                    df = pd.read_excel(xls, sheet_name=prov)
                    stats[prov]['Farmers_No_Land'] = len(df)
                    
                    # Add to demographics
                    col_sex = next((c for c in df.columns if 'sex' in c.lower() or 'gender' in c.lower()), None)
                    if col_sex:
                        s_counts = df[col_sex].astype(str).str.upper().value_counts()
                        stats[prov]['Male'] += s_counts.get('MALE', 0) + s_counts.get('M', 0)
                        stats[prov]['Female'] += s_counts.get('FEMALE', 0) + s_counts.get('F', 0)
                        
                    col_bday = next((c for c in df.columns if 'birth' in c.lower()), None)
                    if col_bday:
                        df[col_bday] = pd.to_datetime(df[col_bday], errors='coerce')
                        now = datetime.now()
                        df['AGE'] = (now - df[col_bday]).dt.days // 365
                        stats[prov]['Youth_12_30'] += len(df[(df['AGE'] >= 12) & (df['AGE'] <= 30)])
                        stats[prov]['Senior_60_Up'] += len(df[df['AGE'] >= 60])

        # --- PHASE 3: ANALYZE ERRORS ---
        path_err = os.path.join(input_dir, files['ERROR'])
        with LoadingSpinner("Analyzing Error Logs..."):
            xls = pd.ExcelFile(path_err)
            for prov in REQUIRED_PROVINCES:
                if prov in xls.sheet_names:
                    df = pd.read_excel(xls, sheet_name=prov)
                    stats[prov]['Erroneous_Entries'] = len(df)

        # --- PHASE 4: COMPILE & SAVE REPORT ---
        output_filename = f"Regional_Analytics_Dashboard_{datetime.now().strftime('%Y%m%d')}.xlsx"
        output_path = os.path.join(output_dir, output_filename)
        
        with LoadingSpinner("Generating Dashboard..."):
            # Convert Dict to DataFrame
            df_stats = pd.DataFrame.from_dict(stats, orient='index')
            df_stats.index.name = 'PROVINCE'
            df_stats.reset_index(inplace=True)
            
            # Add Totals Row
            sum_row = df_stats.sum(numeric_only=True)
            sum_row['PROVINCE'] = 'REGION 6 TOTAL'
            df_stats = pd.concat([df_stats, pd.DataFrame([sum_row])], ignore_index=True)

            with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
                df_stats.to_excel(writer, sheet_name='Executive Summary', index=False)
                
                wb = writer.book
                ws = writer.sheets['Executive Summary']
                
                # Formats
                fmt_header = wb.add_format({'bold': True, 'align': 'center', 'bg_color': '#D9E1F2', 'border': 1})
                fmt_num = wb.add_format({'num_format': '#,##0', 'border': 1})
                fmt_dec = wb.add_format({'num_format': '#,##0.00', 'border': 1})
                fmt_total = wb.add_format({'bold': True, 'bg_color': '#FFFFCC', 'border': 1, 'num_format': '#,##0'})
                
                # Apply Formats
                ws.set_column(0, 0, 20) # Province col
                ws.set_column(1, 3, 15, fmt_num) # Counts
                ws.set_column(4, 7, 18, fmt_dec) # Areas
                ws.set_column(8, 11, 12, fmt_num) # Demographics
                
                # Highlight Total Row
                last_row = len(df_stats)
                ws.set_row(last_row, None, fmt_total)

        print(f"\n📊 Analytics Generated: {output_filename}")

    except Exception as e:
        print(f"❌ Error Generating Analytics: {e}")

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
        elif choice == "3": run_regional_consolidation(input_dir, output_dir)
        elif choice == "4":
            print("\nWhere are the Regional Files located?")
            print(f" [1] Input Folder (./{INPUT_FOLDER_NAME})")
            print(f" [2] Output Folder (./{OUTPUT_FOLDER_NAME})")
            loc = input("Select Source: ").strip()
            
            # Default to output_dir if they choose 2, else input_dir
            target_source_dir = output_dir if loc == '2' else input_dir
            
            run_regional_analytics_mode4(target_source_dir, output_dir)
        elif choice == "5": run_mode_5_workflow(input_dir, output_dir) # Renamed logic function
        elif choice == "6": process_cross_file_audit(input_dir, output_dir)
        elif choice == "7": process_gpx_fixer(input_dir, output_dir)
        elif choice == "Q": sys.exit(0)
        else: print("Invalid.")

if __name__ == "__main__":
    try: run_cli_app()
    except KeyboardInterrupt: sys.exit(0)