"""
Process: Kaltura Report - Live Stream Entry Detail, KR-lsed.py
Last Updated: 2026-08-06 15:41:30
Summary: This script generates an admin session via the Kaltura API to retrieve and report details 
(userId, name, createdAt, lastBroadcast) for live stream entries. It supports both single entry 
verification and multithreaded batch processing from a CSV file. It also retrieves recordStatus 
and conditionally fetches recording quantities and IDs using baseEntry.list.

"""

import os
import sys
import csv
import time
from datetime import datetime
import threading
import concurrent.futures
from dotenv import load_dotenv

# Ensure the Kaltura API client is installed: pip install KalturaApiClient python-dotenv
try:
    from KalturaClient import KalturaClient
    from KalturaClient.Base import KalturaConfiguration
    from KalturaClient.Plugins.Core import KalturaSessionType, KalturaBaseEntryFilter
except ImportError:
    print("Error: KalturaApiClient is not installed. Please run: pip install KalturaApiClient")
    sys.exit(1)

# Load environment variables
load_dotenv()
PARTNER_ID = os.getenv('PARTNER_ID')
ADMIN_SECRET = os.getenv('ADMIN_SECRET')
ADMIN_ID = os.getenv('ADMIN_ID')

if not all([PARTNER_ID, ADMIN_SECRET, ADMIN_ID]):
    print("Error: Missing credentials in .env file.")
    print("Please ensure PARTNER_ID, ADMIN_SECRET, and ADMIN_ID are set.")
    sys.exit(1)

# Initialize global threading lock for console and file writing
write_lock = threading.Lock()
processed_count = 0

def get_kaltura_client(ks=None):
    """Initializes and returns a Kaltura Client. Generates a new session if ks is not provided."""
    config = KalturaConfiguration()
    client = KalturaClient(config)
    
    if ks:
        client.setKs(ks)
    else:
        try:
            ks = client.session.start(
                ADMIN_SECRET, 
                ADMIN_ID, 
                KalturaSessionType.ADMIN, 
                PARTNER_ID, 
                86400, 
                ""
            )
            client.setKs(ks)
        except Exception as e:
            print(f"\nFailed to generate Kaltura Admin Session: {e}")
            sys.exit(1)
    return client, client.getKs()

def format_timestamp(ts):
    """Converts a Unix timestamp to ISO-9075 format (YYYY-MM-DD HH:MM:SS)"""
    if not ts or ts == 0 or str(ts).lower() == "none":
        return "N/A"
    try:
        return datetime.fromtimestamp(int(ts)).strftime('%Y-%m-%d %H:%M:%S')
    except Exception:
        return "Invalid Timestamp"

def format_elapsed_time(start_time):
    """Returns elapsed time in hh:mm:ss format"""
    elapsed = int(time.time() - start_time)
    hours, rem = divmod(elapsed, 3600)
    minutes, seconds = divmod(rem, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

def print_status(start_time, processed, total):
    """Updates the status line in the console"""
    elapsed_str = format_elapsed_time(start_time)
    sys.stdout.write(f"\rElapsed Time: {elapsed_str}, Total Entries: {processed}/{total}")
    sys.stdout.flush()

def fetch_entry_data(client, entry_id):
    """Fetches live stream and recording data for a given entry ID."""
    try:
        entry = client.liveStream.get(entry_id)
        
        # Determine Record Status
        status_val = getattr(entry, 'recordStatus', None)
        
        # Handle enum value if returned as object by SDK
        if hasattr(status_val, 'value'):
            status_val = status_val.value
            
        status_map = {0: "DISABLED", 1: "APPENDED", 2: "PER_SESSION", "0": "DISABLED", "1": "APPENDED", "2": "PER_SESSION"}
        record_status = status_map.get(status_val, str(status_val))
        
        record_qty = ""
        record_ids = ""
        
        # If recording is enabled, perform secondary lookup
        if record_status in ["APPENDED", "PER_SESSION"]:
            entry_filter = KalturaBaseEntryFilter()
            entry_filter.rootEntryIdEqual = entry_id
            
            try:
                record_results = client.baseEntry.list(entry_filter)
                record_qty = str(record_results.totalCount)
                record_ids = ";".join([rec.id for rec in record_results.objects])
            except Exception as e:
                record_qty = "Error"
                record_ids = f"Failed to fetch records: {e}"

        return [
            entry_id,
            entry.userId,
            entry.name,
            format_timestamp(entry.createdAt),
            format_timestamp(entry.lastBroadcast),
            record_status,
            record_qty,
            record_ids
        ]
        
    except Exception as e:
        # If API throws an error (e.g., Entry not found or not a Live Stream)
        return [entry_id, "Error", str(e), "N/A", "N/A", "N/A", "", ""]

def worker_process_entry(entry_id, ks, temp_csv_path, start_time, total_entries):
    """Worker thread function to process an entry and append to temp file."""
    global processed_count
    
    # Initialize a new client instance for this thread using the active KS
    client, _ = get_kaltura_client(ks)
    data_row = fetch_entry_data(client, entry_id)
    
    with write_lock:
        with open(temp_csv_path, mode='a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(data_row)
            
        processed_count += 1
        print_status(start_time, processed_count, total_entries)

def create_final_report(start_time, total_processed, output_filename, data_rows=None, temp_csv_path=None):
    """Generates the final report in the exports folder."""
    export_dir = "exports"
    if not os.path.exists(export_dir):
        os.makedirs(export_dir)
        
    export_path = os.path.join(export_dir, output_filename)
    elapsed_str = format_elapsed_time(start_time)
    
    with open(export_path, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        # Write horizontal summary
        writer.writerow(["Total Elapsed Time", "Total Entries processed"])
        writer.writerow([elapsed_str, total_processed])
        writer.writerow([]) # Blank line
        
        # Write data header
        writer.writerow(["Entry ID", "User Id", "Name", "Created At", "Last Broadcast", "Record Status", "Record Qty", "Record Ids"])
        
        # Write data
        if temp_csv_path:
            # Batch mode: read from temp file and copy over
            with open(temp_csv_path, mode='r', newline='', encoding='utf-8') as temp_file:
                temp_reader = csv.reader(temp_file)
                for row in temp_reader:
                    writer.writerow(row)
        elif data_rows:
            # Single mode: write passed data
            writer.writerows(data_rows)

    print(f"\n\nReport generated successfully at: {export_path}")

def process_single_entry():
    """Handles the single entry processing mode."""
    entry_id = input("\nEnter the single Live Stream Entry ID: ").strip()
    if not entry_id:
        print("Entry ID cannot be empty.")
        return

    print("\nGenerating Kaltura Session...")
    client, ks = get_kaltura_client()
    
    print(f"Verifying entry: {entry_id}...")
    start_time = time.time()
    print_status(start_time, 0, 1)
    
    data_row = fetch_entry_data(client, entry_id)
    
    # If the second element is "Error", it failed verification
    if data_row[1] == "Error":
        print(f"\nFailed to process Entry ID {entry_id}. Reason: {data_row[2]}")
    else:
        print_status(start_time, 1, 1)
        timestamp = datetime.now().strftime('%Y%m%d-%H%M')
        filename = f"{timestamp}_SingleLiveEntrySummary_{entry_id}.csv"
        create_final_report(start_time, 1, filename, data_rows=[data_row])

def process_batch_entries():
    """Handles the batch CSV processing mode."""
    global processed_count
    
    filename_input = input("\nEnter the input CSV filename (extension .csv optional): ").strip()
    if not filename_input:
        print("Filename cannot be empty.")
        return
        
    if not filename_input.lower().endswith('.csv'):
        filename_input += '.csv'
        
    if not os.path.exists(filename_input):
        print(f"\nError: File '{filename_input}' does not exist.")
        sys.exit(1)

    # Read and count entries
    entry_ids = []
    with open(filename_input, mode='r', newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        try:
            next(reader) # Skip header
        except StopIteration:
            print("\nError: CSV file is empty.")
            sys.exit(1)
            
        for row in reader:
            if row and row[0].strip():
                entry_ids.append(row[0].strip())

    total_entries = len(entry_ids)
    if total_entries == 0:
        print("\nNo entry IDs found in the file (ensure they are in the first column).")
        sys.exit(1)

    print("\nGenerating Kaltura Session...")
    _, ks = get_kaltura_client()

    # Generate processing file name
    timestamp_full = datetime.now().strftime('%Y%m%d_%H%M%S')
    base_name = os.path.splitext(os.path.basename(filename_input))[0]
    temp_filename = f"{timestamp_full}_{base_name}_lsesProcessing.csv"
    
    print(f"Starting batch processing of {total_entries} entries...")
    start_time = time.time()
    processed_count = 0
    print_status(start_time, 0, total_entries)

    # Process using ThreadPoolExecutor for 10 concurrent API requests
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [
            executor.submit(worker_process_entry, eid, ks, temp_filename, start_time, total_entries)
            for eid in entry_ids
        ]
        concurrent.futures.wait(futures)

    # Generate the final report
    timestamp_short = datetime.now().strftime('%Y%m%d-%H%M')
    final_output_filename = f"{timestamp_short}_BatchLiveEntrySummary_{base_name}.csv"
    create_final_report(start_time, total_entries, final_output_filename, temp_csv_path=temp_filename)
    
    # Delete temporary processing file upon successful completion
    if os.path.exists(temp_filename):
        try:
            os.remove(temp_filename)
        except OSError as e:
            print(f"\nWarning: Failed to delete temporary processing file '{temp_filename}'. Reason: {e}")

def main():
    print("=== Kaltura Report - Live Stream Entry Detail ===")
    print("1. Process Single Live Entry ID")
    print("2. Batch Process from CSV File")
    
    choice = input("\nSelect processing mode (1 or 2): ").strip()
    
    if choice == '1':
        process_single_entry()
    elif choice == '2':
        process_batch_entries()
    else:
        print("Invalid choice. Exiting script.")
        sys.exit(1)

if __name__ == "__main__":
    main()