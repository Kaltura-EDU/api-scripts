"""
PROCESS: Kaltura Report - User Status Summary, KR-uss.py
LAST UPDATED: 2026-08-11 08:35:15
SUMMARY:
This script retrieves user status, media quantity, and timestamps via single ID lookup 
or batch CSV input using Batched API Polling. Includes a timed prompt to toggle the 
ignore list, programmatic ID filtering (>30 chars), local temporary file processing, 
and generates a final execution summary report alongside the standard data export.
"""

import csv
import os
import sys
import time
import glob
import shutil
import socket
import re
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from dotenv import load_dotenv
from KalturaClient import *
from KalturaClient.Plugins.Core import *

# --- Global Safeguard: Prevent indefinite network hangs ---
socket.setdefaulttimeout(60)

# --- Static Configuration ---
SERVICE_URL = "https://www.kaltura.com"
EXPORT_SUBFOLDER = "exports"
SESSION_EXPIRY = 43200

# ANSI Color Codes
CYAN = "\033[36m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
RED = "\033[31m"
RESET = "\033[0m"

# --- Environment Setup ---
if not os.path.exists('.env'):
    env_files = glob.glob('*.env')
    if env_files:
        source_env = env_files[0]
        print(f"{CYAN}.env not found. Auto-copying '{source_env}' to '.env'...{RESET}")
        shutil.copy(source_env, '.env')

load_dotenv()

# --- Credentials ---
PARTNER_ID_ENV = os.getenv('KALTURA_PARTNER_ID') or os.getenv('PARTNER_ID')
ADMIN_SECRET = os.getenv('KALTURA_ADMIN_SECRET') or os.getenv('ADMIN_SECRET')
ADMIN_ID = os.getenv('KALTURA_USER_ID') or os.getenv('ADMIN_ID')

if not all([PARTNER_ID_ENV, ADMIN_SECRET, ADMIN_ID]):
    print(f"{RED}Error: Missing credentials in .env!{RESET}")
    exit(1)

PARTNER_ID = int(PARTNER_ID_ENV)

# --- Threading & Batch Settings ---
POOL_SIZE = 10    
BATCH_SIZE = 50   
MAX_RETRIES = 5   

# Kaltura Maps & Headers
STATUS_MAP = {0: "BLOCKED", 1: "ACTIVE", 2: "DELETED"}
HEADERS = ['UserId', 'status', 'media_qty', 'created', 'updated']
PROCESSING_HEADERS = ['UserId', 'status', 'media_qty', 'created', 'updated', 'Notes']

def timed_input(prompt, timeout=10, default="Y"):
    """Cross-platform timed input with default fallback."""
    print(prompt, end='', flush=True)
    if sys.platform == 'win32':
        import msvcrt
        start_time = time.time()
        input_str = ''
        while time.time() - start_time < timeout:
            if msvcrt.kbhit():
                char = msvcrt.getwche()
                if char in ('\r', '\n'):
                    print()
                    return input_str.strip() if input_str.strip() else default
                elif char == '\x08': # Backspace
                    if input_str:
                        input_str = input_str[:-1]
                        sys.stdout.write(' \b')
                        sys.stdout.flush()
                else:
                    input_str += char
            time.sleep(0.05)
        print(f"\n{YELLOW}Timer expired. Defaulting to '{default}'.{RESET}")
        return default
    else:
        import select
        i, o, e = select.select([sys.stdin], [], [], timeout)
        if i:
            ans = sys.stdin.readline().strip()
            return ans if ans else default
        else:
            print(f"\n{YELLOW}Timer expired. Defaulting to '{default}'.{RESET}")
            return default

def format_date(ts):
    if not ts or str(ts) in ["0", "-1", "None"]:
        return "N/A"
    try:
        return datetime.fromtimestamp(int(ts)).strftime('%Y-%m-%d %H:%M:%S')
    except:
        return "N/A"

def load_ignore_list(use_ignore=True):
    ignore_set = set()
    if not use_ignore:
        print(f"{YELLOW}Bypassing 'kaltura-ignore.csv' override for evaluation.{RESET}")
        return ignore_set

    if os.path.exists("kaltura-ignore.csv"):
        print(f"{CYAN}Found 'kaltura-ignore.csv'. Loading ignore list...{RESET}")
        with open("kaltura-ignore.csv", mode='r', encoding='utf-8-sig', errors='replace') as f:
            reader = csv.reader(f)
            for row in reader:
                if row and row[0]:
                    ignore_set.add(row[0].strip().lower())
        print(f"{CYAN}Loaded {len(ignore_set)} users to ignore.{RESET}")
    return ignore_set

def is_programmatic_id(user_id):
    """
    Identifies programmatic Canvas/Blackboard/Kaltura hashes and UUIDs.
    Extracts base ID by stripping roles, then checks if length > 30 and alphanumeric.
    """
    base_id = str(user_id).strip()
    
    # 1. Strip known named roles to isolate the base user ID
    match = re.search(r'[-_]([a-zA-Z]+)$', base_id)
    if match:
        role_str = match.group(1).lower()
        known_roles = {
            'admin', 'instructor', 'learner', 'student', 'teacher', 'ta', 
            'observer', 'designer', 'guest', 'user', 'member'
        }
        if role_str in known_roles:
            base_id = base_id[:match.start()]
            
    # 2. Must be strictly longer than 30 characters
    if len(base_id) > 30:
        # 3. Only alphanumeric + special programmatic characters (No '@' or '.' for emails)
        if re.match(r'^[a-zA-Z0-9_+/\-]+$', base_id):
            return True
            
    return False

def format_elapsed_time(seconds):
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    return f"{int(h):02d}:{int(m):02d}:{int(s):02d}"

def get_service(client, service_name):
    if hasattr(client, service_name):
        return getattr(client, service_name)
        
    lc_service = service_name[0].lower() + service_name[1:] if service_name else service_name
    if hasattr(client, lc_service):
        return getattr(client, lc_service)

    plugin_hints = []
    if "Asset" in service_name:
        plugin_hints.append(service_name.split("Asset")[0])
    if "Params" in service_name:
        plugin_hints.append(service_name.split("Params")[0])

    for hint in plugin_hints:
        for p_name in [hint, hint.lower()]:
            if hasattr(client, p_name):
                plugin_ns = getattr(client, p_name)
                if hasattr(plugin_ns, service_name):
                    return getattr(plugin_ns, service_name)
                if hasattr(plugin_ns, lc_service):
                    return getattr(plugin_ns, lc_service)

    if hasattr(client, 'plugins'):
        p_obj = getattr(client, 'plugins')
        if hasattr(p_obj, service_name):
            return getattr(p_obj, service_name)
        if hasattr(p_obj, lc_service):
            return getattr(p_obj, lc_service)

    raise AttributeError(f"Kaltura Client environment error: Service '{service_name}' could not be resolved.")

def get_kaltura_client():
    config = KalturaConfiguration(PARTNER_ID)
    config.serviceUrl = SERVICE_URL
    client = KalturaClient(config)
    ks = client.session.start(ADMIN_SECRET, ADMIN_ID, KalturaSessionType.ADMIN, PARTNER_ID, SESSION_EXPIRY)
    client.setKs(ks)
    return client

def process_user_batch(ks, batch_ids):
    client = get_kaltura_client()
    client.setKs(ks)
    
    # 1. FETCH USERS IN BULK
    user_map = {}
    u_filter = KalturaUserFilter()
    u_filter.idIn = ",".join(batch_ids)
    user_service = get_service(client, 'user')
    
    for attempt in range(MAX_RETRIES):
        try:
            user_results = user_service.list(u_filter, KalturaFilterPager(pageSize=500))
            user_map = {str(u.id).strip().lower(): u for u in getattr(user_results, 'objects', []) if hasattr(u, 'id')}
            break
        except Exception:
            if attempt == MAX_RETRIES - 1:
                pass 
            time.sleep(2 ** attempt)

    # 2. FETCH MEDIA IN BULK 
    media_counts = {str(uid).strip().lower(): 0 for uid in batch_ids}
    seen_media = set()
    
    m_filter = KalturaMediaEntryFilter()
    m_filter.userIdIn = ",".join(batch_ids)
    m_filter.orderBy = KalturaMediaEntryOrderBy.CREATED_AT_ASC
    media_service = get_service(client, 'media')
    
    has_more = True
    while has_more:
        pager = KalturaFilterPager(pageSize=500, pageIndex=1)
        while True:
            for attempt in range(MAX_RETRIES):
                try:
                    result = media_service.list(m_filter, pager)
                    break
                except Exception:
                    if attempt == MAX_RETRIES - 1:
                        has_more = False 
                        break
                    time.sleep(2 ** attempt) 
            
            if not has_more:
                break
                
            objects = getattr(result, 'objects', []) or []
            
            for obj in objects:
                uid_from_media = getattr(obj, 'userId', None)
                if uid_from_media:
                    safe_media_uid = str(uid_from_media).strip().lower()
                    if safe_media_uid in media_counts and obj.id not in seen_media:
                        seen_media.add(obj.id)
                        media_counts[safe_media_uid] += 1
                    
            if len(objects) < 500:
                has_more = False
                break
                
            pager.pageIndex += 1
            if pager.pageIndex > 15: 
                if objects:
                    new_ts = objects[-1].createdAt
                    if getattr(m_filter, 'createdAtGreaterThanOrEqual', None) == new_ts:
                        m_filter.createdAtGreaterThanOrEqual = new_ts + 1
                    else:
                        m_filter.createdAtGreaterThanOrEqual = new_ts
                break

    # 3. COMPILE ROWS WITH FALLBACK
    batch_rows = []
    for uid in batch_ids:
        safe_uid = str(uid).strip().lower()
        user = user_map.get(safe_uid)
        
        if not user:
            try:
                user = user_service.get(uid)
            except Exception:
                pass
                
        if user:
            updated_ts = getattr(user, 'statusUpdatedAt', None)
            if not updated_ts or str(updated_ts) in ["0", "-1", "None"]:
                updated_ts = getattr(user, 'updatedAt', None)

            status_val = STATUS_MAP.get(user.status.value if hasattr(user.status, 'value') else getattr(user, 'status', None), "Unknown")
            
            batch_rows.append([
                user.id, 
                status_val,
                media_counts.get(safe_uid, 0),
                format_date(getattr(user, 'createdAt', None)),
                format_date(updated_ts)
            ])
        else:
            batch_rows.append([uid, "NOT_FOUND", 0, "N/A", "N/A"])
            
    return batch_rows

def process_single_user(use_ignore_list):
    user_id = input(f"\n{GREEN}Enter the User ID: {RESET}").strip()
    if not user_id:
        print(f"{RED}Error: User ID cannot be empty.{RESET}")
        return

    ignore_set = load_ignore_list(use_ignore_list)
    
    if not os.path.exists(EXPORT_SUBFOLDER):
        print(f"{CYAN}Creating subfolder: {EXPORT_SUBFOLDER}{RESET}")
        os.makedirs(EXPORT_SUBFOLDER)

    now = datetime.now().strftime('%Y%m%d-%H%M')
    temp_csv = f"{now}_ussProcessing.csv"
    summary_csv = os.path.join(EXPORT_SUBFOLDER, f"{now}_{user_id}_ussProcessed.csv")
    final_csv = os.path.join(EXPORT_SUBFOLDER, f"{now}_UserStatusSummary_{user_id}.csv")

    start_time = time.time()
    success = False

    try:
        with open(temp_csv, mode='w', newline='', encoding='utf-8') as f_out:
            writer = csv.writer(f_out)
            writer.writerow(PROCESSING_HEADERS)
            
        users_processed_count = 0
        users_ignored_count = 0

        # Programmatic IDs are checked regardless of the ignore list toggle
        if user_id.lower() in ignore_set or is_programmatic_id(user_id):
            print(f"{YELLOW}User ID marked for Ignore. Skipping.{RESET}")
            users_ignored_count = 1
            row = [user_id, "N/A", 0, "N/A", "N/A", "Ignore"]
            with open(temp_csv, mode='a', newline='', encoding='utf-8') as f_out:
                csv.writer(f_out).writerow(row)
        else:
            print(f"{CYAN}Fetching data for User ID: {user_id}...{RESET}")
            main_client = get_kaltura_client()
            ks = main_client.getKs()
            
            batch_rows = process_user_batch(ks, [user_id])
            for row in batch_rows:
                row.append("") 
                with open(temp_csv, mode='a', newline='', encoding='utf-8') as f_out:
                    csv.writer(f_out).writerow(row)
                    
            users_processed_count = 1
            try:
                main_client.session.end()
            except:
                pass

        elapsed_str = format_elapsed_time(time.time() - start_time)

        with open(summary_csv, mode='w', newline='', encoding='utf-8') as f_out:
            writer = csv.writer(f_out)
            writer.writerow(["Elapsed Time", "Users Processed", "Users Ignored"])
            writer.writerow([elapsed_str, users_processed_count, users_ignored_count])
            writer.writerow([])
            
            with open(temp_csv, mode='r', encoding='utf-8') as f_in:
                reader = csv.reader(f_in)
                for r in reader:
                    writer.writerow(r)

        with open(final_csv, mode='w', newline='', encoding='utf-8') as f_out:
            writer = csv.writer(f_out)
            writer.writerow(HEADERS)
            
            with open(temp_csv, mode='r', encoding='utf-8') as f_in:
                reader = csv.reader(f_in)
                next(reader, None) 
                for r in reader:
                    if len(r) > 5 and r[5] == "Ignore":
                        continue
                    writer.writerow(r[:5])

        print(f"{CYAN}Summary Report saved to: {summary_csv}{RESET}")
        print(f"{CYAN}Final Report saved to: {final_csv}{RESET}")
        success = True

    finally:
        if success and os.path.exists(temp_csv):
            os.remove(temp_csv)

def process_batch_file(use_ignore_list):
    user_input = input(f"\n{GREEN}Enter the input CSV filename (extension .csv optional): {RESET}").strip()
    if not user_input:
        print(f"{RED}Error: Filename cannot be empty.{RESET}")
        return
        
    if user_input.lower().endswith('.csv'):
        user_input = user_input[:-4]
        
    input_csv = f"{user_input}.csv"
    ignore_set = load_ignore_list(use_ignore_list)

    if not os.path.exists(EXPORT_SUBFOLDER):
        print(f"{CYAN}Creating subfolder: {EXPORT_SUBFOLDER}{RESET}")
        os.makedirs(EXPORT_SUBFOLDER)

    now = datetime.now().strftime('%Y%m%d-%H%M')
    temp_csv = f"{now}_ussProcessing.csv"
    summary_csv = os.path.join(EXPORT_SUBFOLDER, f"{now}_{user_input}_ussProcessed.csv")
    final_csv = os.path.join(EXPORT_SUBFOLDER, f"{now}_BatchStatusSummary_{user_input}.csv")

    ids = []
    
    print(f"{CYAN}Reading {input_csv}...{RESET}")
    with open(input_csv, mode='r', encoding='utf-8-sig', errors='replace') as f:
        reader = csv.reader(f)
        next(reader, None) 
        for row in reader:
            if row and row[0]:
                ids.append(row[0].strip().strip('"').strip("'"))
    
    total_ids = len(ids)
    if total_ids == 0: 
        print(f"{RED}Error: No IDs found in {input_csv}.{RESET}")
        return

    print(f"{CYAN}Loaded {total_ids} IDs. Filtering and processing...{RESET}")
    
    with open(temp_csv, mode='w', newline='', encoding='utf-8') as f_out:
        writer = csv.writer(f_out)
        writer.writerow(PROCESSING_HEADERS)

    chunks = [ids[i:i + BATCH_SIZE] for i in range(0, total_ids, BATCH_SIZE)]
    custom_format = CYAN + "{desc} Processed: {n_fmt}/{total_fmt} | {percentage:3.0f}% | Elapsed Time: {elapsed}" + RESET
    
    start_time = time.time()
    users_processed_count = 0
    users_ignored_count = 0
    success = False

    try:
        with tqdm(total=total_ids, desc="Export Progress:", bar_format=custom_format) as pbar:
            with ThreadPoolExecutor(max_workers=POOL_SIZE) as executor:
                
                dispatch_client = get_kaltura_client()
                dispatch_ks = dispatch_client.getKs()
                
                future_to_chunk = {}
                
                for chunk in chunks:
                    active_chunk = []
                    ignored_chunk = []
                    
                    for uid in chunk:
                        # Programmatic IDs are always filtered out, but the file list is toggled
                        if uid.lower() in ignore_set or is_programmatic_id(uid):
                            ignored_chunk.append(uid)
                        else:
                            active_chunk.append(uid)
                    
                    users_ignored_count += len(ignored_chunk)
                    users_processed_count += len(active_chunk)
                    
                    ignored_rows = [[uid, "N/A", 0, "N/A", "N/A", "Ignore"] for uid in ignored_chunk]
                    
                    if ignored_rows:
                        with open(temp_csv, mode='a', newline='', encoding='utf-8') as f_out:
                            writer = csv.writer(f_out)
                            writer.writerows(ignored_rows)
                        pbar.update(len(ignored_rows))
                    
                    if active_chunk:
                        f = executor.submit(process_user_batch, dispatch_ks, active_chunk)
                        future_to_chunk[f] = active_chunk
                
                for future in as_completed(future_to_chunk):
                    active_chunk = future_to_chunk[future]
                    try:
                        batch_rows = future.result()
                        for r in batch_rows:
                            r.append("") # Append empty Note
                    except Exception:
                        batch_rows = [[uid, "ERROR", -1, "N/A", "N/A", "Error"] for uid in active_chunk]
                    
                    with open(temp_csv, mode='a', newline='', encoding='utf-8') as f_out:
                        writer = csv.writer(f_out)
                        writer.writerows(batch_rows)
                    
                    pbar.update(len(active_chunk))
                    time.sleep(0.01)

        try:
            dispatch_client.session.end()
        except:
            pass
            
        elapsed_str = format_elapsed_time(time.time() - start_time)

        print(f"\n{CYAN}Generating final reports...{RESET}")

        with open(summary_csv, mode='w', newline='', encoding='utf-8') as f_out:
            writer = csv.writer(f_out)
            writer.writerow(["Elapsed Time", "Users Processed", "Users Ignored"])
            writer.writerow([elapsed_str, users_processed_count, users_ignored_count])
            writer.writerow([])
            
            with open(temp_csv, mode='r', encoding='utf-8') as f_in:
                reader = csv.reader(f_in)
                for r in reader:
                    writer.writerow(r)
                    
        with open(final_csv, mode='w', newline='', encoding='utf-8') as f_out:
            writer = csv.writer(f_out)
            writer.writerow(HEADERS)
            
            with open(temp_csv, mode='r', encoding='utf-8') as f_in:
                reader = csv.reader(f_in)
                next(reader, None) 
                for r in reader:
                    if len(r) > 5 and r[5] == "Ignore":
                        continue
                    writer.writerow(r[:5]) 

        print(f"{CYAN}Summary Report saved to: {summary_csv}{RESET}")
        print(f"{CYAN}Final Report saved to: {final_csv}{RESET}")
        
        success = True

    finally:
        if success and os.path.exists(temp_csv):
            os.remove(temp_csv)
            print(f"{CYAN}Cleaned up temporary processing file.{RESET}")

def main():
    print(f"{CYAN}--- Kaltura Report - User Status Summary (KR-uss) ---{RESET}")
    
    # Check if they want to load the ignore list
    ignore_response = timed_input(f"{GREEN}Include ignore list? (Y/N) [10s timeout]: {RESET}", timeout=10, default="Y")
    use_ignore_list = ignore_response.strip().upper() == "Y"
    
    print(f"\n{GREEN}1. Process a Single User{RESET}")
    print(f"{GREEN}2. Process an Input File (Batch){RESET}")
    
    choice = input(f"{GREEN}Select an option (1 or 2): {RESET}").strip()
    
    if choice == '1':
        process_single_user(use_ignore_list)
    elif choice == '2':
        process_batch_file(use_ignore_list)
    else:
        print(f"{RED}Invalid selection. Exiting.{RESET}")

if __name__ == "__main__":
    main()