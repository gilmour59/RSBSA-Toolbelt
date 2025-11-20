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
    'BARANGAY'
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

def select_input_file(input_dir):
    files = [f for f in os.listdir(input_dir) if f.lower().endswith(('.xlsx', '.csv')) and not f.startswith('~$')]
    
    if not files:
        print("❌ No valid files (.xlsx/.csv) found in input folder.")
        return None

    print("\nAvailable Files:")
    for i, f in enumerate(files):
        print(f"   [{i+1}] {f}")
    
    while True:
        choice = input("\nSelect file number to process: ").strip()
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
        # Helper to filter cols
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

# --- MODE 4: GEOTAG CLEANER ---

def process_geotag_cleaning(file_path, output_dir):
    """Deduplicates based on GEOREF ID and Filters Columns"""
    
    # CHANGED: Use original filename + tag instead of timestamp
    base_name = os.path.splitext(os.path.basename(file_path))[0]
    clean_filename = f"{base_name} [clean].xlsx"
    dupe_filename = f"{base_name} [duplicates].xlsx"
    
    clean_path = os.path.join(output_dir, clean_filename)
    dupe_path = os.path.join(output_dir, dupe_filename)

    try:
        # Load Data
        with LoadingSpinner(f"Loading '{os.path.basename(file_path)}'..."):
            if file_path.lower().endswith('.csv'):
                df = pd.read_csv(file_path)
            else:
                df = pd.read_excel(file_path)

        # Normalize Columns: Remove extra spaces and convert to uppercase for matching
        # But we keep original names for output if possible, or map them
        df.columns = [c.strip() for c in df.columns]
        
        # Check for missing target columns
        missing_cols = [c for c in TARGET_COLS_GEOTAG if c not in df.columns]
        if missing_cols:
            print("\n🛑 MISSING COLUMNS!")
            print(f"   The file is missing: {', '.join(missing_cols)}")
            print("   Available columns:", list(df.columns))
            return

        # Filter Columns
        df_filtered = df[TARGET_COLS_GEOTAG].copy()
        
        print(f"\n   Total Rows Loaded: {len(df_filtered)}")

        # Find Duplicates (Duplicate GEOREF ID)
        # keep=False means mark ALL duplicates as True so we can see them in report
        dupe_mask = df_filtered.duplicated(subset=['GEOREF ID'], keep=False)
        df_duplicates = df_filtered[dupe_mask].sort_values('GEOREF ID')
        
        # Create Clean Version (Keep First)
        df_clean = df_filtered.drop_duplicates(subset=['GEOREF ID'], keep='first')

        print(f"   Unique (Clean) Rows: {len(df_clean)}")
        print(f"   Duplicate Rows Found: {len(df_duplicates)}")

        # Save Clean File
        with LoadingSpinner("Saving Cleaned File..."):
            with pd.ExcelWriter(clean_path, engine='xlsxwriter') as writer:
                df_clean.to_excel(writer, index=False, sheet_name='Clean Data')
        
        # Save Duplicates Report (if any)
        if not df_duplicates.empty:
            with LoadingSpinner("Saving Duplicates Report..."):
                with pd.ExcelWriter(dupe_path, engine='xlsxwriter') as writer:
                    df_duplicates.to_excel(writer, index=False, sheet_name='Duplicates')
            print(f"\n⚠️  Duplicates found! Report saved to: {dupe_filename}")
        else:
            print("\n✅ No duplicates found.")

        print(f"🎉 Cleaned file saved to: {clean_filename}")

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
        print("   [4] Geotag Cleaner [Clean] (Deduplicate & Filter Columns)")
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
            print("\n--- Geotagger Accomplishment Cleaner ---")
            print("This will filter columns and remove duplicate GEOREF IDs.")
            target_file = select_input_file(input_dir)
            if target_file:
                process_geotag_cleaning(target_file, output_dir)
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