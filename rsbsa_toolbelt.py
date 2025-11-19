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

# Columns strictly needed for the report (Case Insensitive matching)
TARGET_COLS = [
    'farmer_address_mun', 
    'farmer_address_bgy', 
    'farmer', 
    'farmworker',
    'fisherfolk', 
    'gender', 
    'agency',
    'birthday'    # Added for Age Computation
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
            # Clean the line using backspaces/spaces
            sys.stdout.write('\b' * (len(self.message) + 2))

    def __enter__(self):
        self.busy = True
        self.thread = threading.Thread(target=self.spinner_task)
        self.thread.start()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.busy = False
        self.thread.join()
        sys.stdout.write('\r' + ' ' * (len(self.message) + 2) + '\r') # Erase line
        sys.stdout.flush()

def ensure_directories():
    cwd = os.getcwd()
    input_path = os.path.join(cwd, INPUT_FOLDER_NAME)
    output_path = os.path.join(cwd, OUTPUT_FOLDER_NAME)
    
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
    files = [f for f in os.listdir(input_dir) if f.lower().endswith('.xlsx') and not f.startswith('~$')]
    
    if not files:
        print("❌ No .xlsx files found in input folder.")
        return None

    print("\nAvailable Files:")
    for i, f in enumerate(files):
        print(f"   [{i+1}] {f}")
    
    while True:
        choice = input("\nSelect file number to process: ").strip()
        if choice.isdigit() and 1 <= int(choice) <= len(files):
            return os.path.join(input_dir, files[int(choice)-1])
        print("❌ Invalid selection.")

# --- CORE LOGIC ---

def process_rsbsa_report(file_path, output_dir):
    # 1. User Inputs
    as_of_input = input("\n📅 Enter 'As Of' Date (e.g., Oct 30, 2024): ").strip()
    if not as_of_input:
        ref_date = datetime.now()
        as_of_str = ref_date.strftime("%B %d, %Y")
    else:
        try:
            # Try to parse the user input into a real date object for age calculation
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

    # LOAD
    try:
        def col_filter(col_name):
            return col_name.strip().lower() in TARGET_COLS

        with LoadingSpinner(f"Loading '{os.path.basename(file_path)}' into memory..."):
            # Read all sheets
            xls = pd.read_excel(file_path, sheet_name=None, usecols=col_filter)
        
        sheet_names = set(xls.keys())
        
        # VALIDATE
        missing_provinces = REQUIRED_PROVINCES - sheet_names
        if missing_provinces:
            print("\n🛑 VALIDATION FAILED: Missing Province Sheets")
            print(f"   Missing: {', '.join(missing_provinces)}")
            return
        
        if len(sheet_names) != len(REQUIRED_PROVINCES):
            print("\n🛑 VALIDATION FAILED: Incorrect Sheet Count")
            print(f"   Expected: {len(REQUIRED_PROVINCES)}")
            print(f"   Found:    {len(sheet_names)}")
            return

        print("✅ Validation Passed.")

        # PROCESS
        with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
            
            for province in REQUIRED_PROVINCES:
                with LoadingSpinner(f"Aggregating data for {province}..."):
                    df = xls[province]
                    df.columns = [c.strip().lower() for c in df.columns]

                    if df.empty:
                        print(f"   ⚠️  Skipping {province}: Sheet is empty")
                        continue

                    # Define cols
                    col_mun = 'farmer_address_mun'
                    col_bgy = 'farmer_address_bgy'
                    col_farmer = 'farmer'
                    col_farmworker = 'farmworker'
                    col_fisher = 'fisherfolk'
                    col_gender = 'gender'
                    col_agency = 'agency'
                    col_birthday = 'birthday'

                    # --- BASIC COUNTS ---
                    df['is_farmer'] = df[col_farmer].astype(str).str.upper().map({'YES': 1}).fillna(0)
                    df['is_farmworker'] = df[col_farmworker].astype(str).str.upper().map({'YES': 1}).fillna(0)
                    df['is_fisher'] = df[col_fisher].astype(str).str.upper().map({'YES': 1}).fillna(0)
                    
                    df['male_count'] = df[col_gender].astype(str).str.upper().map({'MALE': 1}).fillna(0)
                    df['female_count'] = df[col_gender].astype(str).str.upper().map({'FEMALE': 1}).fillna(0)

                    # --- AGE PROFILING ---
                    # Convert birthday to datetime, handle errors (bad dates become NaT)
                    df['bd_dt'] = pd.to_datetime(df[col_birthday], errors='coerce')
                    
                    # Calculate Age in Years: (RefDate - BirthDate) / 365.25
                    # fillna(-1) ensures invalid dates don't crash the logic, they just become -1
                    df['age_years'] = (ref_date - df['bd_dt']).dt.days / 365.25
                    df['age_years'] = df['age_years'].fillna(-1)

                    # Classify
                    # Youth: 15 to 30
                    df['is_youth'] = ((df['age_years'] >= 15) & (df['age_years'] <= 30)).astype(int)
                    # Working Age: 31 to 59
                    df['is_working_age'] = ((df['age_years'] > 30) & (df['age_years'] < 60)).astype(int)
                    # Senior: 60+
                    df['is_senior'] = (df['age_years'] >= 60).astype(int)

                    # --- AGGREGATION ---
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

                    # Write
                    # Changed startrow from 2 to 4 to make room for the legend
                    summary.to_excel(writer, sheet_name=province, index=False, startrow=4)
                    
                    # Header
                    workbook = writer.book
                    worksheet = writer.sheets[province]
                    header_format = workbook.add_format({'bold': True, 'font_size': 14})
                    date_format = workbook.add_format({'italic': True})
                    legend_format = workbook.add_format({'italic': True, 'font_color': 'gray', 'font_size': 10})
                    
                    worksheet.write('A1', f"RSBSA Summary Report - {province}", header_format)
                    worksheet.write('A2', f"As of: {as_of_str}", date_format)
                    worksheet.write('A3', "Age Legend: Youth (15-30) | Working Age (31-59) | Senior (60+)", legend_format)
                    
                    # Formatting columns
                    worksheet.set_column(0, 0, 20) # Mun
                    worksheet.set_column(1, 1, 25) # Bgy
                    worksheet.set_column(2, 10, 15) # Stats
                
                print(f"   ✅ Processed {province}")

        print(f"\n🎉 Report Generated: {output_filename}")
        print(f"   Location: {output_path}")

    except Exception as e:
        print(f"❌ Critical Error: {e}")

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
    
    validation_errors = []
    expected_columns = None
    master_file = None
    merged_data = []

    print("\n") # Spacer for spinner
    for filename in all_files:
        file_path = os.path.join(input_dir, filename)
        try:
            with LoadingSpinner(f"Reading {filename}..."):
                if filename.lower().endswith('.csv'):
                    df = pd.read_csv(file_path)
                else:
                    df = pd.read_excel(file_path)
            
            if df.empty: continue

            current_columns = list(df.columns)

            if expected_columns is None:
                expected_columns = current_columns
                master_file = filename
            elif current_columns != expected_columns:
                validation_errors.append(f"{filename}: Mismatch vs {master_file}")
                continue
            
            df['Source_File'] = filename
            merged_data.append(df)
            print(f"✅ Buffered: {filename}")

        except Exception as e:
            validation_errors.append(f"{filename}: Error {e}")

    if validation_errors:
        print("\n🛑 VALIDATION FAILED")
        for err in validation_errors:
            print(f"   ❌ {err}")
        return

    if merged_data:
        print("\n")
        final_df = pd.concat(merged_data, ignore_index=True)
        output_path = os.path.join(output_dir, output_filename)
        
        try:
            with LoadingSpinner(f"Saving {len(final_df)} rows to Excel..."):
                with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
                    final_df.to_excel(writer, index=False)
            
            print(f"🎉 Saved: {output_path}")
            
            # Cleanup
            for filename in all_files:
                try: os.remove(os.path.join(input_dir, filename))
                except: pass
        except Exception as e:
            print(f"❌ Save Error: {e}")

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
    output_path = os.path.join(output_dir, output_filename)
    files_to_delete = []

    print("\n")
    try:
        with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
            for filename in all_files:
                file_path = os.path.join(input_dir, filename)
                base_filename = filename.rsplit('.', 1)[0]
                try:
                    frames = {}
                    
                    with LoadingSpinner(f"Reading {filename}..."):
                        if filename.lower().endswith('.csv'):
                            frames[base_filename] = pd.read_csv(file_path)
                        else:
                            sheets = pd.read_excel(file_path, sheet_name=None)
                            for s_name, df in sheets.items():
                                t_name = base_filename if len(sheets)==1 else f"{base_filename}_{s_name}"
                                frames[t_name] = df
                    
                    with LoadingSpinner(f"Writing sheets for {filename}..."):
                        for raw_name, df in frames.items():
                            final_name = clean_sheet_name(raw_name)
                            ctr = 1
                            orig = final_name
                            while final_name in writer.book.sheetnames:
                                final_name = f"{orig[:28]}_{ctr}"
                                ctr += 1
                            df.to_excel(writer, sheet_name=final_name, index=False)
                    
                    print(f"✅ Added: {filename}")
                    files_to_delete.append(filename)
                except Exception as e:
                    print(f"❌ Error {filename}: {e}")
        
        print(f"🎉 Saved: {output_path}")
        for f in files_to_delete:
            try: os.remove(os.path.join(input_dir, f))
            except: pass
            
    except Exception as e:
        print(f"❌ Critical Error: {e}")

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
        print("   [3] Generate Regional Summary (Sanitize & Count per Barangay)")
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
        elif choice == "Q":
            sys.exit(0)
        else:
            print("Invalid selection.")

if __name__ == "__main__":
    try:
        run_cli_app()
    except KeyboardInterrupt:
        sys.exit(0)