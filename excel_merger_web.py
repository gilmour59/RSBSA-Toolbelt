import os
import pandas as pd
import sys
import re
from datetime import datetime

# --- CONFIGURATION ---
INPUT_FOLDER_NAME = "input_files"
OUTPUT_FOLDER_NAME = "output_files"

def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')

def print_header():
    print("="*70)
    print("   🚀  HIGH-SPEED EXCEL TOOLBELT (CLI)")
    print("   Powered by XlsxWriter")
    print("="*70)

def ensure_directories():
    """Checks if input/output folders exist, creates them if not."""
    cwd = os.getcwd()
    input_path = os.path.join(cwd, INPUT_FOLDER_NAME)
    output_path = os.path.join(cwd, OUTPUT_FOLDER_NAME)
    
    created_new = False
    
    if not os.path.exists(input_path):
        os.makedirs(input_path)
        print(f"📁 Created input folder:  {INPUT_FOLDER_NAME}/")
        created_new = True
        
    if not os.path.exists(output_path):
        os.makedirs(output_path)
        print(f"📁 Created output folder: {OUTPUT_FOLDER_NAME}/")
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

def run_cli_app():
    clear_screen()
    print_header()

    # 1. Setup Folders
    input_dir, output_dir, just_created = ensure_directories()
    
    print(f"\n📍 Looking for files in: ./{INPUT_FOLDER_NAME}")
    print(f"📍 Saving results to:    ./{OUTPUT_FOLDER_NAME}")

    if just_created:
        print("\n✨ I noticed this is your first run (or folders were missing).")
        print(f"👉 Please copy your .xlsx/.csv files into the '{INPUT_FOLDER_NAME}' folder now.")
        input("   Press Enter when you are ready...")

    # 2. Scan Files (Loop until files are found)
    while True:
        print(f"\n🔍 Scanning '{INPUT_FOLDER_NAME}'...")
        try:
            all_files = [f for f in os.listdir(input_dir) if f.lower().endswith(('.xlsx', '.csv')) and not f.startswith('~$')]
        except Exception as e:
            print(f"❌ Error accessing directory: {e}")
            return

        if not all_files:
            print(f"⚠️  No .xlsx or .csv files found in '{INPUT_FOLDER_NAME}'.")
            retry = input("   👉 Add files and press Enter to retry (or type 'q' to quit): ")
            if retry.lower() == 'q':
                return
        else:
            break

    print(f"   ✅ Found {len(all_files)} files.")

    # 3. Get Output Config
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    default_filename = f"Merged_Output_{timestamp}.xlsx"
    
    output_filename = get_output_filename(default_filename)
    output_path = os.path.join(output_dir, output_filename)

    # 4. Select Mode
    print("\nSelect Operation:")
    print("   [1] Stack Rows (Strict Mode - Requires same columns)")
    print("   [2] Combine to Sheets (One file, separate tabs)")
    
    choice = input("\nEnter 1 or 2: ").strip()

    # --- MODE 1: STACK ROWS ---
    if choice == "1":
        print("\n--- Starting Strict Merge ---")
        validation_errors = []
        expected_columns = None
        master_file = None
        merged_data = []

        for filename in all_files:
            file_path = os.path.join(input_dir, filename)
            try:
                # Pass 1: Read & Validate
                if filename.lower().endswith('.csv'):
                    df = pd.read_csv(file_path)
                else:
                    df = pd.read_excel(file_path)
                
                if df.empty:
                    validation_errors.append(f"{filename}: Empty file")
                    continue

                current_columns = list(df.columns)

                if expected_columns is None:
                    expected_columns = current_columns
                    master_file = filename
                    print(f"   ℹ️  Master Schema: {filename}")
                elif current_columns != expected_columns:
                    validation_errors.append(f"{filename}: Mismatch vs {master_file}")
                    continue
                
                df['Source_File'] = filename
                merged_data.append(df)
                print(f"   ✅ Buffered: {filename}")

            except Exception as e:
                validation_errors.append(f"{filename}: Error {e}")

        if validation_errors:
            print("\n🛑 VALIDATION FAILED - NO FILES DELETED")
            for err in validation_errors:
                print(f"   ❌ {err}")
            print("\nPlease fix these files and try again.")
            input("\nPress Enter to exit...")
            return

        if merged_data:
            print("\n⏳ Concatenating & Saving...")
            final_df = pd.concat(merged_data, ignore_index=True)
            
            try:
                with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
                    final_df.to_excel(writer, index=False)
                print(f"🎉 Done! Saved to: {output_path}")
                
                # --- CLEANUP LOGIC ---
                print("\n🗑️  Cleanup: Removing processed input files...")
                for filename in all_files:
                    try:
                        os.remove(os.path.join(input_dir, filename))
                        print(f"   Deleted: {filename}")
                    except Exception as e:
                        print(f"   ⚠️ Failed to delete {filename}: {e}")

            except Exception as e:
                print(f"❌ Error saving file: {e}")
                print("   ⚠️ Input files were NOT deleted due to save error.")

    # --- MODE 2: SEPARATE SHEETS ---
    elif choice == "2":
        print("\n--- Starting Sheet Combine ---")
        print("⏳ Processing files... (This is fast!)")
        
        files_to_delete = []

        try:
            with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
                for filename in all_files:
                    file_path = os.path.join(input_dir, filename)
                    base_filename = filename.rsplit('.', 1)[0]
                    
                    try:
                        frames_to_write = {}

                        if filename.lower().endswith('.csv'):
                            df = pd.read_csv(file_path)
                            frames_to_write[base_filename] = df
                        else:
                            excel_sheets = pd.read_excel(file_path, sheet_name=None)
                            for sheet_name, df in excel_sheets.items():
                                if len(excel_sheets) == 1:
                                    target_name = base_filename
                                else:
                                    target_name = f"{base_filename}_{sheet_name}"
                                frames_to_write[target_name] = df

                        for raw_name, df in frames_to_write.items():
                            final_sheet_name = clean_sheet_name(raw_name)
                            
                            counter = 1
                            original_name = final_sheet_name
                            while final_sheet_name in writer.book.sheetnames:
                                final_sheet_name = f"{original_name[:28]}_{counter}"
                                counter += 1

                            df.to_excel(writer, sheet_name=final_sheet_name, index=False)
                        
                        print(f"   ✅ Added: {filename}")
                        files_to_delete.append(filename) # Mark for deletion

                    except Exception as e:
                        print(f"   ❌ Error on {filename}: {e}")
                        # Note: We do NOT add this file to 'files_to_delete'

            print(f"\n🎉 Done! Saved to: {output_path}")
            
            # --- CLEANUP LOGIC ---
            if files_to_delete:
                print("\n🗑️  Cleanup: Removing successfully processed files...")
                for filename in files_to_delete:
                    try:
                        os.remove(os.path.join(input_dir, filename))
                        print(f"   Deleted: {filename}")
                    except Exception as e:
                        print(f"   ⚠️ Failed to delete {filename}: {e}")
            
            if len(files_to_delete) < len(all_files):
                print(f"\n⚠️  {len(all_files) - len(files_to_delete)} files were skipped/failed and remain in the folder.")

        except Exception as e:
            print(f"❌ Critical Save Error: {e}")
            print("   ⚠️ Input files were NOT deleted due to save error.")

    else:
        print("Invalid selection.")

    input("\nPress Enter to exit...")

if __name__ == "__main__":
    try:
        run_cli_app()
    except KeyboardInterrupt:
        sys.exit(0)