import os
import pandas as pd
import sys
import re
import time
import threading
import itertools
from datetime import datetime

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
    'UPLOADER'
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

def select_input_file(input_dir, prompt="Select file number to process"):
    files = [f for f in os.listdir(input_dir) if f.lower().endswith(('.xlsx', '.csv')) and not f.startswith('~$')]
    
    if not files:
        print("❌ No valid files (.xlsx/.csv) found in input folder.")
        return None

    print("\nAvailable Files:")
    for i, f in enumerate(files):
        print(f"   [{i+1}] {f}")
    
    while True:
        choice = input(f"\n{prompt}: ").strip()
        if choice.isdigit() and 1 <= int(choice) <= len(files):
            return os.path.join(input_dir, files[int(choice)-1])
        print("❌ Invalid selection.")

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
                    df['is_youth'] = ((df['age_years'] >= 15) & (df['age_years'] <= 30)).astype(int)
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
                        'Youth (15-30)', 'Working Age (31-59)', 'Senior (60+)'
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
                    worksheet.write('A3', "Age Legend: Youth (15-30) | Working Age (31-59) | Senior (60+)", legend_format)
                    
                    worksheet.set_column(0, 0, 20)
                    worksheet.set_column(1, 1, 25)
                    worksheet.set_column(2, 10, 15)

        print(f"\n🎉 Report Generated: {output_filename}")
        print(f"   Location: {output_path}")

    except Exception as e:
        print(f"❌ Critical Error: {e}")

# --- MODE 4: UNIFIED GEOTAG (CLEAN + ENRICH) ---

def process_unified_geotag(geotag_path, parcel_path, output_dir):
    """
    1. Cleans Geotag (Dedupe GEOREF ID, Filter Columns)
    2. Preps Parcel (Filter Commodity, Dedupe ID)
    3. Merges (Adds CROP AREA) with Commodity Check
    4. Calculates FINDINGS
    5. Summarizes VERIFIED AREA per UPLOADER
    """
    base_name = os.path.splitext(os.path.basename(geotag_path))[0]
    output_filename = f"{base_name} [clean_enriched].xlsx"
    dupe_filename = f"{base_name} [duplicates].xlsx"
    output_path = os.path.join(output_dir, output_filename)
    dupe_path = os.path.join(output_dir, dupe_filename)

    print("\n--- Geotag Cleaning & Enrichment ---")
    print(f"   1. Target File: {os.path.basename(geotag_path)}")
    print(f"   2. Source File: {os.path.basename(parcel_path)}")

    try:
        # --- STEP 1: LOAD & CLEAN GEOTAG ---
        with LoadingSpinner("Loading & Cleaning Geotag file..."):
            if geotag_path.lower().endswith('.csv'):
                df_geo = pd.read_csv(geotag_path)
            else:
                df_geo = pd.read_excel(geotag_path)
            
            # Normalize & Check
            df_geo.columns = [c.strip() for c in df_geo.columns]
            missing = [c for c in TARGET_COLS_GEOTAG if c not in df_geo.columns]
            if missing:
                print(f"\n🛑 Error: Geotag file missing columns: {missing}")
                return

            # Filter Columns
            df_geo = df_geo[TARGET_COLS_GEOTAG].copy()

            # Deduplicate GEOREF ID
            dupe_mask = df_geo.duplicated(subset=['GEOREF ID'], keep=False)
            df_duplicates = df_geo[dupe_mask].sort_values('GEOREF ID')
            
            # Keep First unique
            df_clean_geo = df_geo.drop_duplicates(subset=['GEOREF ID'], keep='first')

        print(f"   Geotag Rows: {len(df_geo)} -> {len(df_clean_geo)} (Removed {len(df_geo)-len(df_clean_geo)} duplicates)")

        # Save Duplicates Report if exist
        if not df_duplicates.empty:
            with LoadingSpinner("Saving Duplicates Report..."):
                with pd.ExcelWriter(dupe_path, engine='xlsxwriter') as writer:
                    df_duplicates.to_excel(writer, index=False)
            print(f"   ⚠️  Duplicates report saved: {dupe_filename}")

        # --- STEP 2: PREP PARCEL LIST ---
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
            
            if not all([col_id, col_area, col_comm]):
                print("\n🛑 Error: Parcel file missing required columns.")
                return

            # Load Data
            if parcel_path.lower().endswith('.csv'):
                df_parcel = pd.read_csv(parcel_path, usecols=[col_id, col_area, col_comm])
            else:
                df_parcel = pd.read_excel(parcel_path, usecols=[col_id, col_area, col_comm])
            
            df_parcel.rename(columns={col_id:'KEY_ID', col_area:'CROP AREA', col_comm:'COMMODITY'}, inplace=True)

            # Filter Commodity (Rice, Palay, Corn, Sugarcane)
            mask = df_parcel['COMMODITY'].astype(str).str.contains(r'Rice|Palay|Corn|Sugarcane', flags=re.IGNORECASE, regex=True)
            df_parcel = df_parcel[mask]
            
            # IMPORTANT: We DO NOT dedupe Parcel ID yet. 

        print(f"   Parcel List References: {len(df_parcel)} (Rice/Corn/Sugar)")

        # --- STEP 3: MERGE & COMMODITY MATCH ---
        with LoadingSpinner("Merging & Matching Commodities..."):
            # 1. Merge (This expands rows 1-to-Many)
            df_merged = pd.merge(
                df_clean_geo,
                df_parcel, # Contains KEY_ID, CROP AREA, COMMODITY (y)
                left_on='RSBSA ID',
                right_on='KEY_ID',
                how='left',
                suffixes=('', '_parcel')
            )
            
            # 2. Calculate Match Score
            def is_match(row):
                if pd.isna(row['COMMODITY_parcel']): return False
                return normalize_commodity(row['COMMODITY']) == normalize_commodity(row['COMMODITY_parcel'])
            
            df_merged['is_match'] = df_merged.apply(is_match, axis=1)
            
            # 3. Sort: Prioritize Matches, then whatever else
            df_merged.sort_values(by=['GEOREF ID', 'is_match'], ascending=[True, False], inplace=True)
            
            # 4. Dedupe: Keep best match per GEOREF ID
            df_final = df_merged.drop_duplicates(subset=['GEOREF ID'], keep='first').copy()
            
            # 5. Validate Crop Area based on Match
            def finalize_crop_area(row):
                if pd.isna(row['KEY_ID']): return "ID NOT FOUND"
                if not row['is_match']: return "COMMODITY MISMATCH"
                return row['CROP AREA']
            
            df_final['CROP AREA'] = df_final.apply(finalize_crop_area, axis=1)

        # --- STEP 4: FINDINGS & REARRANGE ---
        with LoadingSpinner("Calculating Findings..."):
            def calc_findings(row):
                crop_val = row['CROP AREA']
                ver_val = row['VERIFIED AREA (Ha)']
                
                if isinstance(crop_val, str):
                    return "NO CROP AREA"
                if pd.isna(crop_val):
                    return "NO CROP AREA"
                
                try:
                    crop_num = float(crop_val)
                    ver_num = float(ver_val)
                    if ver_num > (crop_num + 2):
                        return "ABOVE"
                except:
                    pass
                
                return "OK"

            df_final['FINDINGS'] = df_final.apply(calc_findings, axis=1)
            
            # Rearrange
            missing_final = [c for c in FINAL_COLUMN_ORDER if c not in df_final.columns]
            if not missing_final:
                df_final = df_final[FINAL_COLUMN_ORDER]
            else:
                print(f"   ⚠️ Warning: Could not rearrange columns. Missing: {missing_final}")

        # --- STEP 5: SUMMARIZE BY UPLOADER ---
        with LoadingSpinner("Generating Uploader Summary..."):
            # Ensure VERIFIED AREA is numeric for summing
            df_final['VERIFIED AREA (Ha)'] = pd.to_numeric(df_final['VERIFIED AREA (Ha)'], errors='coerce').fillna(0)
            
            # Group and Sum
            df_summary = df_final.groupby('UPLOADER')[['VERIFIED AREA (Ha)']].sum().reset_index()
            df_summary = df_summary.rename(columns={'VERIFIED AREA (Ha)': 'TOTAL VERIFIED AREA (Ha)'})
            df_summary = df_summary.sort_values('TOTAL VERIFIED AREA (Ha)', ascending=False)

        # --- STEP 6: SAVE ---
        with LoadingSpinner(f"Saving result to {output_filename}..."):
            with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
                # 1. Write Summary Sheet (First)
                df_summary.to_excel(writer, sheet_name='Uploader Summary', index=False)
                
                # Format Summary
                workbook = writer.book
                ws_summ = writer.sheets['Uploader Summary']
                bold_fmt = workbook.add_format({'bold': True, 'bg_color': '#D9EAD3', 'border': 1})
                num_fmt = workbook.add_format({'num_format': '#,##0.00', 'border': 1})
                
                # Header
                for col_num, value in enumerate(df_summary.columns.values):
                    ws_summ.write(0, col_num, value, bold_fmt)
                
                # Columns
                ws_summ.set_column(0, 0, 35) # Uploader Name Width
                ws_summ.set_column(1, 1, 25, num_fmt) # Area Width + Format
                
                # 2. Write Clean Data Sheet (Second)
                df_final.to_excel(writer, sheet_name='Clean Data', index=False)

        print(f"\n🎉 Success! File saved: {output_filename}")

    except Exception as e:
        print(f"❌ Critical Error: {e}")

# --- MENU LOGIC ---

def run_cli_app():
    clear_screen()
    print_header()

    input_dir, output_dir, just_created = ensure_directories()
    
    print(f"\n📍 Looking for files in: ./{INPUT_FOLDER_NAME}")
    print(f"📍 Saving results to:    ./{OUTPUT_FOLDER_NAME}")

    if just_created:
        print("\n✨ Setup complete.")
        print(f"👉 Please copy your .xlsx/.csv files into '{INPUT_FOLDER_NAME}'")
        input("   Press Enter when you are ready...")

    while True:
        print("\nSelect Operation:")
        print("   [1] Stack Rows (Strict Mode - Merge files with same columns)")
        print("   [2] Combine to Sheets (Group files into tabs)")
        print("   [3] Generate Regional Summary (Analytics per Barangay)")
        print("   [4] Geotag Processor (Clean & Add Crop Area)")
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
            print("\n--- Geotag Processor ---")
            print("This will: Clean duplicates -> Filter Columns -> Add CROP AREA from Parcel List")
            
            geo_file = select_input_file(input_dir, "1. Select Raw Geotag File")
            if geo_file:
                parcel_file = select_input_file(input_dir, "2. Select Parcel List File")
                if parcel_file:
                    process_unified_geotag(geo_file, parcel_file, output_dir)
        elif choice == "Q":
            sys.exit(0)
        else:
            print("Invalid selection.")

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

if __name__ == "__main__":
    try:
        run_cli_app()
    except KeyboardInterrupt:
        sys.exit(0)