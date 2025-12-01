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
    MODE 2: TRIAGE SYSTEM (Strict Mapping & Granular)
    
    Updates:
    - Integrity Check restricted to Birthday, Gender, and Sector (No Names/Addresses).
    - Output preserves Masterlist columns (Left side) as the source of truth.
    - False positives from spelling variations are eliminated.
    """
    
    base_name = os.path.splitext(os.path.basename(master_path))[0]
    output_filename = f"{base_name}-merged.xlsx"
    output_path = os.path.join(output_dir, output_filename)

    print(f"\n--- Starting Triage Analysis for: {base_name} ---")
    
    # --- DEFINING THE STRICT MAPPING ---
    # Key = Masterlist Column (The "Truth")
    # Value = Parcel List Column (The "Comparison Target")
    
    # We COMMENT OUT Names and Addresses to prevent false positives.
    # We only check critical bio-data that must match.
    INTEGRITY_MAP = {
        'rsbsa_no': 'FFRS System Generated No.',
        # 'first_name': 'FIRST NAME',        <-- DISABLED (Too many false positives)
        # 'middle_name': 'MIDDLE NAME',      <-- DISABLED
        # 'last_name': 'LAST NAME',          <-- DISABLED
        # 'ext_name': 'EXT NAME',            <-- DISABLED
        # 'farmer_address_bgy': 'FARMER ADDRESS 1', <-- DISABLED
        # 'farmer_address_mun': 'FARMER ADDRESS 2', <-- DISABLED
        # 'farmer_address_prv': 'FARMER ADDRESS 3', <-- DISABLED
        'birthday': 'BIRTHDATE',
        'gender': 'GENDER',
        'farmer': 'FARMER',
        'farmworker': 'FARMWORKER',
        'fisherfolk': 'FISHERFOLK'
    }

    try:
        # --- 1. LOAD MASTERLIST ---
        with LoadingSpinner("Loading Masterlist..."):
            if master_path.lower().endswith('.csv'): df_m = pd.read_csv(master_path)
            else: df_m = pd.read_excel(master_path)
            
            df_m.columns = [c.strip().lower() for c in df_m.columns]
            
            if 'rsbsa_no' not in df_m.columns:
                print("❌ Error: Column 'rsbsa_no' not found in Masterlist.")
                return
            
            df_m['KEY_ID'] = df_m['rsbsa_no'].astype(str).str.strip().str.upper()
            df_m['DATA_STATUS'] = 'CLEAN' 
            df_m['ERROR_TAG'] = ''   
            df_m['CONFLICT_GROUP'] = ''

        # --- 2. LOAD PARCEL LIST ---
        with LoadingSpinner("Loading Parcel List..."):
            if parcel_path.lower().endswith('.csv'): df_p = pd.read_csv(parcel_path)
            else: df_p = pd.read_excel(parcel_path)
            
            # Map for case-insensitive lookup
            p_cols_map = {c.strip().lower(): c for c in df_p.columns} 
            
            target_key = 'ffrs system generated no.'
            if target_key not in p_cols_map:
                print(f"❌ Error: Parcel Key 'FFRS System Generated No.' not found.")
                return
            
            actual_key_col = p_cols_map[target_key]
            df_p['KEY_ID'] = df_p[actual_key_col].astype(str).str.strip().str.upper()
            
            # Flag Multiple Holdings
            df_p['parcel_count_temp'] = df_p.groupby('KEY_ID')['KEY_ID'].transform('count')
            df_p['HAS_MULTIPLE_LAND_HOLDINGS'] = df_p['parcel_count_temp'].apply(lambda x: 'YES' if x > 1 else 'NO')

        # --- 3. PRE-MERGE TRIAGE (Duplicates & Fuzzy) ---
        with LoadingSpinner("Triaging: Analyzing Masterlist..."):
            # A. STRICT DUPLICATES
            dup_mask = df_m.duplicated(subset=['KEY_ID'], keep=False)
            df_m.loc[dup_mask, 'DATA_STATUS'] = 'ERROR'
            df_m.loc[dup_mask, 'ERROR_TAG'] += '[Duplicate RSBSA ID] '
            df_m.loc[dup_mask, 'CONFLICT_GROUP'] = 'STRICT-' + df_m.loc[dup_mask, 'KEY_ID']

            # B. FUZZY MATCHING
            m_fname = 'first_name' if 'first_name' in df_m.columns else df_m.columns[1]
            m_lname = 'last_name' if 'last_name' in df_m.columns else df_m.columns[3]
            m_bday = 'birthday' if 'birthday' in df_m.columns else 'birthdate'
            
            df_m['LOOSE_SIG'] = (
                df_m[m_lname].fillna('').astype(str).str.strip().str.upper() + 
                df_m[m_bday].astype(str)
            )
            
            candidates = df_m[df_m['DATA_STATUS'] == 'CLEAN'].copy()
            potential_dupes = candidates[candidates.duplicated(subset=['LOOSE_SIG'], keep=False)]
            
            fuzzy_counter = 1
            if not potential_dupes.empty:
                for sig, group in potential_dupes.groupby('LOOSE_SIG'):
                    if len(group) < 2: continue
                    rows = group.to_dict('records')
                    indices = group.index.tolist()
                    
                    for i in range(len(rows)):
                        for j in range(i + 1, len(rows)):
                            r1, r2 = rows[i], rows[j]
                            idx1, idx2 = indices[i], indices[j]
                            
                            name1 = str(r1.get(m_fname,'')).strip().upper()
                            name2 = str(r2.get(m_fname,'')).strip().upper()
                            
                            if similar(name1, name2) > 0.85:
                                if 'gender' in df_m.columns:
                                    s1 = str(r1.get('gender', '')).strip().upper()
                                    s2 = str(r2.get('gender', '')).strip().upper()
                                    if s1 and s2 and s1 != s2: continue 

                                group_id = f"FUZZY-{fuzzy_counter:04d}"
                                df_m.at[idx1, 'DATA_STATUS'] = 'ERROR'
                                df_m.at[idx2, 'DATA_STATUS'] = 'ERROR'
                                df_m.at[idx1, 'ERROR_TAG'] += '[Identity Conflict] '
                                df_m.at[idx2, 'ERROR_TAG'] += '[Identity Conflict] '
                                df_m.at[idx1, 'CONFLICT_GROUP'] = group_id
                                df_m.at[idx2, 'CONFLICT_GROUP'] = group_id
                                fuzzy_counter += 1

        # --- 4. MERGE & INTEGRITY CHECK ---
        with LoadingSpinner("Merging & Validating Integrity..."):
            
            df_m_clean = df_m[df_m['DATA_STATUS'] == 'CLEAN'].copy()
            df_m_error = df_m[df_m['DATA_STATUS'] == 'ERROR'].copy()
            
            # MERGE
            df_merged = pd.merge(df_m_clean, df_p, on='KEY_ID', how='left', suffixes=('', '_PARCEL'))
            df_merged['HAS_PARCEL'] = df_merged['HAS_MULTIPLE_LAND_HOLDINGS'].notna()
            
            integrity_errors = []
            mask_has_parcel = df_merged['HAS_PARCEL'] == True
            
            for idx, row in df_merged[mask_has_parcel].iterrows():
                mismatches = []
                
                # Check ONLY the enabled keys in INTEGRITY_MAP (Birthday, Gender, Sector)
                for m_col, p_col_name in INTEGRITY_MAP.items():
                    if m_col not in df_merged.columns: continue 
                    val_m = str(row[m_col]).strip().upper()
                    
                    actual_p_col = p_cols_map.get(p_col_name.strip().lower())
                    if not actual_p_col: continue 
                    
                    target_p_col = actual_p_col
                    if actual_p_col in df_m.columns: 
                        target_p_col = f"{actual_p_col}_PARCEL"
                    
                    if target_p_col not in df_merged.columns: continue
                    val_p = str(row[target_p_col]).strip().upper()
                    
                    is_empty_m = val_m in ['NAN', 'NONE', '', 'NAT', 'NULL']
                    is_empty_p = val_p in ['NAN', 'NONE', '', 'NAT', 'NULL']
                    
                    if not is_empty_m and not is_empty_p:
                        if val_m != val_p:
                            mismatches.append(f"{m_col.upper()} ({val_m} != {val_p})")
                
                if mismatches:
                    integrity_errors.append({
                        'index': idx,
                        'tag': f"[Data Mismatch] {'; '.join(mismatches)}"
                    })

            if integrity_errors:
                for err in integrity_errors:
                    idx = err['index']
                    tag = err['tag']
                    df_merged.at[idx, 'DATA_STATUS'] = 'ERROR'
                    df_merged.at[idx, 'ERROR_TAG'] = tag
                    df_merged.at[idx, 'CONFLICT_GROUP'] = f"DATA-ERR-{df_merged.at[idx, 'KEY_ID']}"

        # --- 5. CLEANUP & SAVE ---
        with LoadingSpinner("Finalizing Report..."):
            
            df_valid = df_merged[df_merged['DATA_STATUS'] == 'CLEAN'].copy()
            df_conflict_post = df_merged[df_merged['DATA_STATUS'] == 'ERROR'].copy()
            
            df_with = df_valid[df_valid['HAS_PARCEL'] == True].copy()
            df_no = df_valid[df_valid['HAS_PARCEL'] == False].copy()
            
            all_errors = pd.concat([df_m_error, df_conflict_post], ignore_index=True)
            
            # --- COLUMN CLEANUP STRATEGY ---
            # 1. We identify the Parcel Columns that correspond to the "disabled" checks too (Names, Addresses)
            #    because we want to DROP the Parcel version and keep the Masterlist version.
            
            # Full list of columns usually found in Parcel list that duplicate Masterlist info
            cols_to_drop_targets = [
                'FFRS System Generated No.', 'FIRST NAME', 'MIDDLE NAME', 'LAST NAME', 'EXT NAME',
                'FARMER ADDRESS 1', 'FARMER ADDRESS 2', 'FARMER ADDRESS 3',
                'BIRTHDATE', 'GENDER', 'FARMER', 'FARMWORKER', 'FISHERFOLK'
            ]
            
            cols_to_remove = []
            for target in cols_to_drop_targets:
                lower_target = target.strip().lower()
                if lower_target in p_cols_map:
                    real_name = p_cols_map[lower_target]
                    cols_to_remove.append(real_name)           # The original parcel column name
                    cols_to_remove.append(f"{real_name}_PARCEL") # The suffixed version
            
            helpers = ['LOOSE_SIG', 'DATA_STATUS', 'ERROR_TAG', 'HAS_PARCEL', 'parcel_count_temp', 'CONFLICT_GROUP', 'KEY_ID']
            cols_to_remove.extend(helpers)
            
            # Keep clean columns (Masterlist columns + Unique Parcel columns)
            final_cols = [c for c in df_with.columns if c not in cols_to_remove]
            
            # SORTING
            sort_keys = []
            if 'farmer_address_mun' in final_cols: sort_keys.append('farmer_address_mun')
            if 'farmer_address_bgy' in final_cols: sort_keys.append('farmer_address_bgy')
            if 'last_name' in final_cols: sort_keys.append('last_name')
            
            if sort_keys:
                df_with.sort_values(by=sort_keys, inplace=True)
                df_no.sort_values(by=sort_keys, inplace=True)
                
            if not all_errors.empty:
                # Ensure error keys exist
                err_keys = ['CONFLICT_GROUP'] + [k for k in sort_keys if k in all_errors.columns]
                all_errors.sort_values(by=err_keys, inplace=True)

            # SAVE
            with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
                # Sheet 1: With Parcels
                df_with[final_cols].to_excel(writer, sheet_name='Clean - With Parcels', index=False)
                
                # Sheet 2: No Parcels
                cols_no_parcels = [c for c in final_cols if c in df_m.columns]
                df_no[cols_no_parcels].to_excel(writer, sheet_name='Clean - No Parcels', index=False)
                
                # Sheet 3: Errors
                # For errors, we SHOW the removed parcel columns so user can see what happened
                # (e.g., they can visually check if the addresses were actually different)
                err_display_cols = ['CONFLICT_GROUP', 'ERROR_TAG'] + [c for c in all_errors.columns if c not in helpers]
                all_errors[err_display_cols].to_excel(writer, sheet_name='Erroneous & Conflicts', index=False)
                
                wb = writer.book
                ws_err = writer.sheets['Erroneous & Conflicts']
                red_fmt = wb.add_format({'font_color': '#9C0006', 'bg_color': '#FFC7CE'})
                ws_err.conditional_format(1, 0, len(all_errors), 0, {'type': 'no_blanks', 'format': red_fmt})
                ws_err.set_column(0, 1, 30)

        print(f"🎉 Processed: {output_filename}")
        if integrity_errors:
            print(f"   ⚠️ Found {len(integrity_errors)} Data Mismatches (Strict Bio-data).")

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
    MODE 4: FARMERS REGISTRY GENERATOR (FINAL)
    
    Updates:
    - Ownership: Separated into 4 Count Columns (Owner, Tenant, Lessee, Others).
    - Removed: Farm Type.
    - Headers: Strictly at Row 5.
    - Formatting: Full Borders.
    """
    print("\n--- Starting Farmers Registry Generation (Mode 4: Analytics) ---")
    
    # 1. Ask for Date
    as_of_input = input("Enter 'As Of' Date (e.g. Sept 30, 2025): ").strip()
    
    try:
        if not as_of_input:
            ref_date = datetime.now()
            as_of_date = ref_date.strftime("%b %d, %Y")
        else:
            ref_date = pd.to_datetime(as_of_input)
            as_of_date = as_of_input
    except:
        ref_date = datetime.now()
        as_of_date = ref_date.strftime("%b %d, %Y")

    safe_date = "".join([c for c in as_of_date if c.isalnum() or c in (' ', '-', '_')]).strip()
    
    # ==========================================
    # PART A: CLEAN ANALYTICS
    # ==========================================
    print(f"\n🔹 Generating Clean Registry Analytics (As of {as_of_date})...")
    
    clean_files = {
        'clean_with': 'Regional_With_Parcels.xlsx',
        'clean_no':   'Regional_No_Parcels.xlsx'
    }
    
    df_clean = pd.DataFrame()
    with LoadingSpinner("Loading Clean Data..."):
        for f_key, fname in clean_files.items():
            f_path = os.path.join(input_dir, fname)
            if os.path.exists(f_path):
                try:
                    xls = pd.ExcelFile(f_path)
                    for sheet in xls.sheet_names:
                        df_part = pd.read_excel(xls, sheet_name=sheet)
                        df_part['PROVINCE_SHEET'] = sheet
                        df_clean = pd.concat([df_clean, df_part], ignore_index=True)
                except: pass

    if not df_clean.empty:
        with LoadingSpinner("Aggregating Analytics..."):
            df_clean.columns = [c.strip().lower() for c in df_clean.columns]
            df_clean = df_clean.loc[:, ~df_clean.columns.duplicated()]

            # --- COLUMN MAPPING ---
            col_prov = 'province_sheet'
            col_mun = next((c for c in df_clean.columns if 'mun' in c and 'address' in c), 'farmer_address_mun')
            col_bgy = next((c for c in df_clean.columns if 'bgy' in c and 'address' in c), 'farmer_address_bgy')
            col_id  = next((c for c in df_clean.columns if 'rsbsa' in c and 'no' in c), 'rsbsa_no')
            
            # Demographics
            col_sex = next((c for c in df_clean.columns if 'sex' in c or 'gender' in c), None)
            col_bday = next((c for c in df_clean.columns if 'birth' in c), None)
            
            # Farmer Types
            col_frm = next((c for c in df_clean.columns if 'farmer' == c), None)
            col_wrk = next((c for c in df_clean.columns if 'farmworker' in c), None)
            col_fsh = next((c for c in df_clean.columns if 'fisher' in c), None)
            col_agency = next((c for c in df_clean.columns if 'agency' in c), None)

            # Sectoral
            col_youth = next((c for c in df_clean.columns if 'youth' in c), None)
            col_ip    = next((c for c in df_clean.columns if 'ip' in c), None)
            col_tribe = next((c for c in df_clean.columns if 'tribe' in c), None)
            col_arb   = next((c for c in df_clean.columns if 'arb' in c), None)

            # Parcel Details
            col_multi = next((c for c in df_clean.columns if 'multiple' in c), None)
            col_comm  = next((c for c in df_clean.columns if 'commodity' in c or 'commodities' in c), None)
            col_own   = next((c for c in df_clean.columns if 'ownership' in c), None)

            # Areas
            col_area = next((c for c in df_clean.columns if 'crop_area' in c or 'parcel area' in c), None)
            if not col_area: col_area = next((c for c in df_clean.columns if 'total_parcel_area' in c), None)

            # Age Calculation
            if col_bday:
                df_clean[col_bday] = pd.to_datetime(df_clean[col_bday], errors='coerce')
                df_clean['AGE'] = (ref_date - df_clean[col_bday]).dt.days // 365
            else:
                df_clean['AGE'] = 0

            # --- AGGREGATION ---
            clean_outputs = {}

            if col_prov in df_clean.columns:
                for prov in df_clean[col_prov].unique():
                    prov_df = df_clean[df_clean[col_prov] == prov]
                    rows = []
                    
                    for (mun, bgy), group in prov_df.groupby([col_mun, col_bgy]):
                        
                        # Set 1: UNIQUE FARMERS (Head Count)
                        unique_farmers = group.drop_duplicates(subset=[col_id])
                        
                        # Counts
                        n_farmers = unique_farmers[col_frm].astype(str).str.upper().apply(lambda x: 1 if 'YES' in x or 'TRUE' in x else 0).sum() if col_frm else 0
                        n_workers = unique_farmers[col_wrk].astype(str).str.upper().apply(lambda x: 1 if 'YES' in x or 'TRUE' in x else 0).sum() if col_wrk else 0
                        n_fisher  = unique_farmers[col_fsh].astype(str).str.upper().apply(lambda x: 1 if 'YES' in x or 'TRUE' in x else 0).sum() if col_fsh else 0
                        
                        # Demographics
                        n_male = 0; n_female = 0
                        if col_sex:
                            n_male = len(unique_farmers[unique_farmers[col_sex].astype(str).str.upper().isin(['M', 'MALE'])])
                            n_female = len(unique_farmers[unique_farmers[col_sex].astype(str).str.upper().isin(['F', 'FEMALE'])])
                        
                        n_youth_age = len(unique_farmers[unique_farmers['AGE'].between(12, 30)])
                        n_working   = len(unique_farmers[unique_farmers['AGE'].between(31, 59)])
                        n_senior    = len(unique_farmers[unique_farmers['AGE'] >= 60])
                        
                        # Sectoral Counts
                        def count_yes(c): 
                            return unique_farmers[c].astype(str).str.upper().apply(lambda x: 1 if 'YES' in x or 'TRUE' in x else 0).sum() if c else 0
                        
                        cnt_agri_y = count_yes(col_youth)
                        cnt_ip = count_yes(col_ip)
                        cnt_arb = count_yes(col_arb)
                        cnt_multi = count_yes(col_multi)
                        
                        cnt_tribe = 0
                        if col_tribe: 
                            cnt_tribe = unique_farmers[col_tribe].astype(str).str.upper().apply(lambda x: 1 if x not in ['NAN', 'NONE', ''] else 0).sum()

                        # Ownership Counts (Specific Categories)
                        n_owner = 0; n_tenant = 0; n_lessee = 0; n_others = 0
                        if col_own:
                            # We check distinct farmers who have at least one parcel with this status
                            def count_own(keyword):
                                # Filter unique farmers who have 'keyword' in their ownership column
                                # Note: col_own might be comma separated if aggregated, but here we have granular rows from Mode 2? 
                                # Mode 3 merges them. If Mode 2 was granular, Mode 3 output is granular.
                                # So 'group' has multiple rows.
                                # We need to find unique IDs where column contains keyword.
                                subset = group[group[col_own].astype(str).str.upper().str.contains(keyword, na=False)]
                                return subset[col_id].nunique()

                            n_owner = count_own('OWNER')
                            n_tenant = count_own('TENANT')
                            n_lessee = count_own('LESSEE')
                            n_others = count_own('OTHER')

                        # Agencies
                        n_agencies = 0
                        if col_agency:
                            raw = unique_farmers[col_agency].dropna().astype(str).tolist()
                            uset = set()
                            for r in raw:
                                for p in r.split(','):
                                    clean_a = p.strip().upper()
                                    if clean_a and clean_a not in ['NAN', 'NONE', '']: uset.add(clean_a)
                            n_agencies = len(uset)

                        # Set 2: ALL ROWS (Parcel Attributes)
                        # Aggregate Text (Unique Values for Commodities)
                        def get_unique_str(col):
                            if not col: return ""
                            vals = group[col].dropna().astype(str).unique()
                            clean_vals = sorted(list(set([v.strip().title() for v in vals if v.strip().upper() not in ['NAN', 'NONE', '']])))
                            return ", ".join(clean_vals)

                        txt_comm = get_unique_str(col_comm)

                        # Areas
                        tot_area = 0; rice_area = 0; corn_area = 0; sugar_area = 0
                        if col_area:
                            group[col_area] = pd.to_numeric(group[col_area], errors='coerce').fillna(0)
                            tot_area = group[col_area].sum()
                            
                            if col_comm:
                                def get_comm_area(k):
                                    mask = group[col_comm].astype(str).str.upper().str.contains(k, na=False)
                                    return group.loc[mask, col_area].sum()
                                rice_area = get_comm_area('RICE') + get_comm_area('PALAY')
                                corn_area = get_comm_area('CORN') + get_comm_area('MAIS')
                                sugar_area = get_comm_area('SUGAR') + get_comm_area('CANE')

                        # BUILD ROW
                        rows.append({
                            'Municipality': mun, 'Barangay': bgy,
                            'Farmers': n_farmers, 'Farmworkers': n_workers, 'Fisherfolk': n_fisher,
                            'AgriYouth': cnt_agri_y, 'IP': cnt_ip, 'With Tribe': cnt_tribe, 
                            'ARB': cnt_arb, 'Multi-Parcel Holders': cnt_multi,
                            'Distinct Agencies': n_agencies,
                            'Male': n_male, 'Female': n_female,
                            'Youth (12-30)': n_youth_age, 'Working (31-59)': n_working, 'Senior (60+)': n_senior,
                            'Registered Owner': n_owner, 'Tenant': n_tenant, 'Lessee': n_lessee, 'Others': n_others,
                            'Total Area (Ha)': tot_area, 'Rice Area': rice_area, 'Corn Area': corn_area, 'Sugar Area': sugar_area,
                            'Commodities': txt_comm
                        })
                    
                    if rows:
                        df_res = pd.DataFrame(rows).sort_values(by=['Municipality', 'Barangay'])
                        clean_outputs[prov] = df_res

        # SAVE CLEAN
        clean_file = f"Farmers Registry {safe_date}.xlsx"
        try:
            with pd.ExcelWriter(os.path.join(output_dir, clean_file), engine='xlsxwriter') as writer:
                wb = writer.book
                # Formats
                title_fmt = wb.add_format({'bold': True, 'font_size': 14})
                sub_fmt = wb.add_format({'italic': True})
                header_fmt = wb.add_format({'bold': True, 'bg_color': '#D9E1F2', 'border': 1, 'align': 'center', 'valign': 'vcenter', 'text_wrap': True})
                border_fmt = wb.add_format({'border': 1})
                num_fmt = wb.add_format({'num_format': '#,##0', 'border': 1})
                dec_fmt = wb.add_format({'num_format': '#,##0.00', 'border': 1})
                text_border = wb.add_format({'border': 1, 'text_wrap': True})
                
                for prov, df in clean_outputs.items():
                    # Write Data at Row 6 (Index 5)
                    df.to_excel(writer, sheet_name=prov, startrow=5, index=False, header=False)
                    
                    ws = writer.sheets[prov]
                    
                    ws.write(0, 0, f"RSBSA Summary Report - {prov}", title_fmt)
                    ws.write(1, 0, f"\"As of: {as_of_date}\"", sub_fmt)
                    ws.write(2, 0, "Age Legend: Youth (12-30) | Working (31-59) | Senior (60+)", sub_fmt)
                    
                    # Headers at Row 5 (Index 4)
                    for i, col in enumerate(df.columns):
                        ws.write(4, i, col, header_fmt)
                    
                    # Formatting
                    # Mun/Bgy
                    ws.set_column(0, 1, 20, border_fmt)
                    # Counts (C to P approx)
                    ws.set_column(2, 19, 10, num_fmt)
                    # Areas (T to W approx)
                    ws.set_column(20, 23, 12, dec_fmt)
                    # Commodities (X)
                    ws.set_column(24, 24, 30, text_border)
                    
                    # Full Borders
                    end_row = len(df) + 5
                    end_col = len(df.columns) - 1
                    ws.conditional_format(5, 0, end_row, end_col, {'type': 'no_errors', 'format': border_fmt})
            
            print(f"   ✅ Created Clean Analytics: {clean_file}")
            
        except Exception as e:
            print(f"   ❌ Error saving Clean Analytics: {e}")
    else:
        print("   ⚠️ No Clean Data found.")

    # ==========================================
    # PART B: ERRONEOUS ANALYTICS
    # ==========================================
    print("\n🔹 Generating Erroneous Analytics...")
    
    error_file_path = os.path.join(input_dir, 'Regional_Erroneous.xlsx')
    df_err = pd.DataFrame()
    
    with LoadingSpinner("Loading Erroneous Data..."):
        if os.path.exists(error_file_path):
            try:
                xls = pd.ExcelFile(error_file_path)
                for sheet in xls.sheet_names:
                    df_part = pd.read_excel(xls, sheet_name=sheet)
                    df_part['PROVINCE_SHEET'] = sheet
                    df_err = pd.concat([df_err, df_part], ignore_index=True)
            except: pass

    if not df_err.empty:
        df_err.columns = [c.strip().lower() for c in df_err.columns]
        df_err = df_err.loc[:, ~df_err.columns.duplicated()]

        col_prov = 'province_sheet'
        col_mun = next((c for c in df_err.columns if 'mun' in c and 'address' in c), 'farmer_address_mun')
        col_bgy = next((c for c in df_err.columns if 'bgy' in c and 'address' in c), 'farmer_address_bgy')
        col_tag = next((c for c in df_err.columns if 'tag' in c), 'error_tag')
        
        err_outputs = {}
        
        if col_prov in df_err.columns:
            for prov in df_err[col_prov].unique():
                prov_df = df_err[df_err[col_prov] == prov]
                rows = []
                
                if col_mun in prov_df.columns: prov_df[col_mun] = prov_df[col_mun].fillna('UNKNOWN')
                if col_bgy in prov_df.columns: prov_df[col_bgy] = prov_df[col_bgy].fillna('UNKNOWN')
                
                for (mun, bgy), group in prov_df.groupby([col_mun, col_bgy]):
                    total_err = len(group)
                    n_strict = group[col_tag].astype(str).apply(lambda x: 1 if 'Duplicate' in x else 0).sum()
                    n_fuzzy = group[col_tag].astype(str).apply(lambda x: 1 if 'Identity' in x else 0).sum()
                    n_mismatch = group[col_tag].astype(str).apply(lambda x: 1 if 'Mismatch' in x else 0).sum()
                    
                    rows.append({
                        'Municipality': mun, 'Barangay': bgy,
                        'Total Errors': total_err,
                        'Strict Duplicates': n_strict,
                        'Identity Conflicts': n_fuzzy,
                        'Data Mismatches': n_mismatch
                    })
                
                if rows:
                    err_outputs[prov] = pd.DataFrame(rows).sort_values(by=['Municipality', 'Barangay'])

        err_out_file = f"Farmers Registry Erroneous {safe_date}.xlsx"
        try:
            with pd.ExcelWriter(os.path.join(output_dir, err_out_file), engine='xlsxwriter') as writer:
                wb = writer.book
                header_fmt = wb.add_format({'bold': True, 'bg_color': '#FFC7CE', 'font_color': '#9C0006', 'border': 1, 'align': 'center'})
                border_fmt = wb.add_format({'border': 1})
                
                for prov, df in err_outputs.items():
                    df.to_excel(writer, sheet_name=prov, startrow=4, index=False, header=False)
                    ws = writer.sheets[prov]
                    ws.write(0, 0, f"Erroneous Summary - {prov}", wb.add_format({'bold': True, 'font_size': 14, 'font_color': '#9C0006'}))
                    ws.write(1, 0, f"As of: {as_of_date}")
                    
                    for i, col in enumerate(df.columns):
                        ws.write(4, i, col, header_fmt)
                        
                    ws.set_column(0, 1, 25)
                    ws.set_column(2, 5, 18)
                    ws.conditional_format(5, 0, len(df)+4, len(df.columns)-1, 
                                          {'type': 'no_errors', 'format': border_fmt})
            
            print(f"   ✅ Created Erroneous Analytics: {err_out_file}")
            
        except Exception as e:
            print(f"   ❌ Error saving Erroneous Analytics: {e}")
    else:
        print("   ⚠️ No Erroneous Data found.")

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