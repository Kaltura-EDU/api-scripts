# Process: Kaltura Update - media entry Extra Flavor recreate Request, KU-efr.py
# Summary: Connects to the Kaltura API using administrative credentials to verify and trigger the recreation of flavor ID 487041 "Basic/Small - WEB/MBL (H264/400)" for targeted media entries. Supports single manual entries or batch CSV processing.
# Last Updated: 2026-06-23 14:58:42

import os
import sys
import csv
import time
from datetime import datetime
from dotenv import load_dotenv

# Ensure Kaltura SDK components are accessible
try:
    from KalturaClient import KalturaClient, KalturaConfiguration
    from KalturaClient.exceptions import KalturaException
    from KalturaClient.Plugins.Core import KalturaSessionType, KalturaFlavorAssetFilter
except ImportError as e:
    print(f"Import Error Details: {e}")
    sys.exit(1)

# Load environment configurations
load_dotenv()

PARTNER_ID = os.getenv("PARTNER_ID")
ADMIN_SECRET = os.getenv("ADMIN_SECRET")
ADMIN_ID = os.getenv("ADMIN_ID")
SESSION_EXPIRY = 43200  # 12 hours in seconds

# Terminal Color Codes
COLOR_INFO = '\033[94m'   # Blue
COLOR_ERROR = '\033[91m'  # Red
COLOR_RESET = '\033[0m'   # Reset

if not all([PARTNER_ID, ADMIN_SECRET, ADMIN_ID]):
    print(f"{COLOR_ERROR}Error: Missing credentials in .env file. Please check PARTNER_ID, ADMIN_SECRET, and ADMIN_ID.{COLOR_RESET}")
    sys.exit(1)

def get_kaltura_client():
    """Initializes and authenticates an admin session with Kaltura."""
    try:
        config = KalturaConfiguration()
        config.serviceUrl = "https://www.kaltura.com"
        client = KalturaClient(config)
        
        ks = client.session.start(
            ADMIN_SECRET,
            ADMIN_ID,
            KalturaSessionType.ADMIN,
            int(PARTNER_ID),
            SESSION_EXPIRY,
            ""
        )
        client.setKs(ks)
        return client
    except Exception as e:
        print(f"{COLOR_ERROR}Failed to authenticate with Kaltura: {str(e)}{COLOR_RESET}")
        sys.exit(1)

def process_entry(client, entry_id):
    """
    Checks if flavor 487041 exists for the entry. 
    If missing, requests flavor creation via flavorAsset.convert.
    Returns: (Flavor requested Y/N, Status String, Is Error Boolean)
    """
    try:
        asset_filter = KalturaFlavorAssetFilter()
        asset_filter.entryIdEqual = entry_id
        
        flavor_assets = client.flavorAsset.list(asset_filter)
        
        flavor_exists = False
        for asset in flavor_assets.objects:
            if getattr(asset, 'flavorParamsId', None) == 487041:
                flavor_exists = True
                break
                
        if flavor_exists:
            return "N", "Skipped - Flavor Param already exists", False
        else:
            # Trigger creation of flavor asset 487041
            client.flavorAsset.convert(entry_id, 487041)
            return "Y", "200 - Success: Conversion request received", False
            
    except KalturaException as e:
        return "N", f"Error - Code: {e.code}, Message: {e.message}", True
    except Exception as e:
        return "N", f"Error - Unexpected Exception: {str(e)}", True

def format_elapsed_time(seconds):
    hours, rem = divmod(seconds, 3600)
    minutes, secs = divmod(rem, 60)
    return f"{int(hours):02d}:{int(minutes):02d}:{int(secs):02d}"

def write_report(summary_data, data_rows, report_filename):
    """Creates the export folder and structures the final report CSV file."""
    export_folder = "exports"
    if not os.path.exists(export_folder):
        os.makedirs(export_folder)
        
    report_path = os.path.join(export_folder, report_filename)
    
    with open(report_path, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        # Summary block at the top
        writer.writerow(["SUMMARY"])
        for key, val in summary_data.items():
            writer.writerow([key, val])
        writer.writerow([]) # Spacer
        
        # Tabular data headers
        writer.writerow(["Entry ID", "Flavor creation requested? (Y/N)", "Kaltura Response"])
        writer.writerows(data_rows)
        
    print(f"\nReport successfully saved to: {report_path}")

def main():
    print("====================================================")
    print("KU-efr: Kaltura Update - media entry Extra Flavor recreate Request")
    print("====================================================\n")
    
    print("Select Mode:")
    print("1) Single Media Entry ID")
    print("2) Batch Input via CSV")
    mode = input("Enter choice (1 or 2): ").strip()
    
    if mode not in ['1', '2']:
        print(f"{COLOR_ERROR}Invalid choice. Exiting.{COLOR_RESET}")
        sys.exit(1)
        
    print("\nConnecting to Kaltura Client...")
    client = get_kaltura_client()
    print(f"{COLOR_INFO}Authenticated successfully.{COLOR_RESET}\n")
    
    start_time = time.time()
    run_date_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    safe_time_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    processed_rows = []
    total_entries = 0
    error_count = 0
    report_filename = ""
    
    if mode == '1':
        entry_id = input("Enter Single Media Entry ID: ").strip()
        if not entry_id:
            print(f"{COLOR_ERROR}Entry ID cannot be blank.{COLOR_RESET}")
            sys.exit(1)
            
        # Verify that the media entry actually exists
        try:
            client.media.get(entry_id)
        except KalturaException as e:
            print(f"{COLOR_ERROR}Error: Media entry ID '{entry_id}' does not exist or is inaccessible ({e.message}).{COLOR_RESET}")
            sys.exit(1)
            
        total_entries = 1
        print(f"Processing entry {entry_id}...")
        requested, response_str, is_err = process_entry(client, entry_id)
        if is_err:
            error_count += 1
            
        processed_rows.append([entry_id, requested, response_str])
        report_filename = f"{safe_time_str}_FlavRegen_{entry_id}.csv"
        
    elif mode == '2':
        csv_input = input("Enter the input CSV filename (extension .csv optional): ").strip()
        if not csv_input.lower().endswith('.csv'):
            csv_input += '.csv'
            
        if not os.path.exists(csv_input):
            print(f"{COLOR_ERROR}Error: The file '{csv_input}' could not be found.{COLOR_RESET}")
            sys.exit(1)
            
        # Count total entries (excluding header)
        with open(csv_input, mode='r', encoding='utf-8') as f:
            reader = csv.reader(f)
            try:
                next(reader) # Skip header
                entry_ids = [row[0].strip() for row in reader if row and row[0].strip()]
                total_entries = len(entry_ids)
            except StopIteration:
                print(f"{COLOR_ERROR}Error: The CSV file is empty or missing a header row.{COLOR_RESET}")
                sys.exit(1)
                
        if total_entries == 0:
            print(f"{COLOR_ERROR}No valid entry IDs found in the file.{COLOR_RESET}")
            sys.exit(1)
            
        # Establish processing file path
        clean_input_name = os.path.basename(csv_input).replace('.csv', '')
        proc_filename = f"{safe_time_str}_{clean_input_name}_processing.csv"
        
        print(f"Starting batch process. Real-time updates saved to: {proc_filename}\n")
        
        with open(proc_filename, mode='w', newline='', encoding='utf-8') as pf:
            p_writer = csv.writer(pf)
            p_writer.writerow(["Entry ID", "Flavor creation requested? (Y/N)", "Kaltura Response"])
            
            for idx, entry_id in enumerate(entry_ids, start=1):
                requested, response_str, is_err = process_entry(client, entry_id)
                if is_err:
                    error_count += 1
                    
                row_data = [entry_id, requested, response_str]
                processed_rows.append(row_data)
                p_writer.writerow(row_data)
                pf.flush() # Forces writing immediately to disk
                
                # Update status inline
                elapsed = time.time() - start_time
                elapsed_str = format_elapsed_time(elapsed)
                sys.stdout.write(
                    f"\rElapsed Time: {elapsed_str}, Total Entries: {idx}/{total_entries}, Entries with error responses: {error_count}"
                )
                sys.stdout.flush()
        print() # Linebreak after loop completion
        report_filename = f"report_{safe_time_str}.csv"
        
    # Compile Summary Data Block
    total_elapsed_time = format_elapsed_time(time.time() - start_time)
    summary_data = {
        "Date Time": run_date_time,
        "Total Elapsed Time": total_elapsed_time,
        "Total Entries processed": total_entries,
        "Total Entries with errors": error_count
    }
    
    # Generate final export spreadsheet
    write_report(summary_data, processed_rows, report_filename)
    print("\nProcess Complete.")
    
    # --- END KALTURA SESSION ---
    try:
        client.session.end()
        print(f"{COLOR_INFO}Kaltura session successfully terminated.{COLOR_RESET}")
    except Exception as e:
        print(f"{COLOR_ERROR}Warning: Unable to terminate Kaltura session explicitly. Error: {e}{COLOR_RESET}")

if __name__ == "__main__":
    main()