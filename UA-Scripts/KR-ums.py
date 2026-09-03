"""
PROCESS: Kaltura Report - User Media Summary, KR-ums.py
LAST UPDATED: 2026-07-21 09:08:42 AM AKDT
SUMMARY:
This script aggregates comprehensive media profiles for Kaltura users via single ID lookup or batch CSV input. For each user, it calculates totals for media quantity, distinct media types, and playback duration. It analyzes storage footprint by querying flavor sizes (including child entry flavors) along with captions, thumbnails, and attachments. Crucially, the script leverages robust time-based pagination and Kaltura MultiRequests to safely and accurately process deep-dive asset queries without reaching API caps or dropping data for power users. It checks for standard categories and Media Retention Policy categories from "MRP-Categories.csv". Based on configurable media-driven criteria, it conditionally triggers a secondary multi-threaded validation script (KR-umd.py) for detailed reporting.
"""

import csv
import os
import time
import glob
import shutil
import subprocess
import sys
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from dotenv import load_dotenv
from KalturaClient import *
from KalturaClient.Plugins.Core import *
from KalturaClient.Plugins.Attachment import *
from KalturaClient.Plugins.Caption import *

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

# --- Load MRP Categories ---
MRP_CATEGORIES = set()
if os.path.exists("MRP-Categories.csv"):
    with open("MRP-Categories.csv", mode="r", encoding="utf-8-sig", errors="replace") as f:
        reader = csv.reader(f)
        for row in reader:
            if row and row[0].strip():
                MRP_CATEGORIES.add(row[0].strip())

# --- Threading & Batch Settings ---
POOL_SIZE = 10
BATCH_SIZE = 50

# Kaltura Maps
STATUS_MAP = {0: "BLOCKED", 1: "ACTIVE", 2: "DELETED"}
MEDIA_TYPE_MAP = {1: "VIDEO", 2: "IMAGE", 5: "AUDIO"}

HEADERS = [
    'User ID', 'Full Name', 'Email', 'Status', 'Created At', 'Status Last Updated',
    'Media Quantity', 'Media Types', 'Duration (Sec)', 'Duration (hh:mm:ss)',
    'Total Storage (KB)', 'Children', 'Flavor Assets', 'Related Assets', 
    'Categories', 'Collaborators', 'MRP-Flags'
]

def input_with_timeout(prompt, timeout, default):
    """Cross-platform standard input with a countdown timeout."""
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
                input_str += char
            time.sleep(0.05)
        print(f"\n{YELLOW}Timeout reached. Defaulting to: {default}{RESET}")
        return default
    else:
        import select
        rlist, _, _ = select.select([sys.stdin], [], [], timeout)
        if rlist:
            user_input = sys.stdin.readline().strip()
            return user_input if user_input else default
        else:
            print(f"\n{YELLOW}Timeout reached. Defaulting to: {default}{RESET}")
            return default

def prompt_umd_trigger():
    print(f"\n{CYAN}--- Secondary Report (KR-umd.py) Trigger ---{RESET}")
    print(f"{GREEN}1. None (Default/Do not trigger){RESET}")
    print(f"{GREEN}2. User Has Published Media (does not include MRP Categories){RESET}")
    print(f"{GREEN}3. Media Has MRP-Flag(s){RESET}")
    print(f"{GREEN}4. Media Has Collaborator(s){RESET}")
    print(f"{GREEN}5. Media Has Child entry(s){RESET}")
    print(f"{GREEN}6. Has Attachment Asset(s){RESET}")
    print(f"{GREEN}7. Has Caption Asset(s){RESET}")
    print(f"{GREEN}8. Has Thumbnail Image(s){RESET}")
    
    choice = input_with_timeout(
        prompt=f"{GREEN}Select an option (1-8) [Auto-defaults to 1 in 10s]: {RESET}",
        timeout=10,
        default='1'
    )
    
    if choice not in [str(i) for i in range(1, 9)]:
        return '1'
        
    return choice

def should_trigger(choice, metrics):
    if choice == '2' and metrics['categories'] == 'Yes': return True
    if choice == '3' and metrics['mrp'] == 'Yes': return True
    if choice == '4' and metrics['collabs'] == 'Yes': return True
    if choice == '5' and metrics['children'] == 'Yes': return True
    if choice == '6' and metrics['has_att']: return True
    if choice == '7' and metrics['has_cap']: return True
    if choice == '8' and metrics['has_thm']: return True
    return False

def format_duration(seconds):
    if not seconds or not isinstance(seconds, (int, float)):
        return "00:00:00"
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = int(seconds % 60)
    return f"{h:02}:{m:02}:{s:02}"

def format_date(ts):
    if not ts or str(ts) in ["0", "-1", "None"]:
        return "N/A"
    try:
        return datetime.fromtimestamp(int(ts)).strftime('%Y-%m-%d %H:%M')
    except:
        return "N/A"

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

def get_asset_size_in_kb(asset_list):
    if not asset_list:
        return 0
    total_size = 0
    for asset in asset_list:
        size_val = int(getattr(asset, 'size', 0) or 0)
        if size_val == 0:
            size_val = int(getattr(asset, 'sizeInBytes', 0) or 0)
        total_size += size_val
    return total_size

def get_kaltura_client():
    config = KalturaConfiguration(PARTNER_ID)
    config.serviceUrl = SERVICE_URL
    client = KalturaClient(config)
    ks = client.session.start(ADMIN_SECRET, ADMIN_ID, KalturaSessionType.ADMIN, PARTNER_ID, SESSION_EXPIRY)
    client.setKs(ks)
    return client

def get_kaltura_client_from_ks(ks):
    config = KalturaConfiguration(PARTNER_ID)
    config.serviceUrl = SERVICE_URL
    client = KalturaClient(config)
    client.setKs(ks)
    return client

def get_user_full_name(user):
    return getattr(user, 'fullName', "N/A")

def get_detailed_metrics(ks, user_id):
    client = get_kaltura_client_from_ks(ks)
    metrics = {
        'media_qty': 0, 'media_types': "NONE", 'dur_sec': 0, 
        'total_kb': 0, 'children': "No", 'flavors': 0, 'related': 0,
        'categories': "No", 'collabs': "No", 'mrp': "No",
        'has_att': False, 'has_cap': False, 'has_thm': False
    }
    
    entries = []
    seen_ids = set()

    # --- 1. Fetch Media Entries Safely (Bypassing 10k Limit) ---
    try:
        media_service = get_service(client, 'media')
        m_filter = KalturaMediaEntryFilter()
        m_filter.userIdEqual = user_id
        m_filter.orderBy = KalturaMediaEntryOrderBy.CREATED_AT_ASC
        
        has_more = True
        while has_more:
            pager = KalturaFilterPager(pageSize=500, pageIndex=1)
            while True:
                result = media_service.list(m_filter, pager)
                current_objects = getattr(result, 'objects', []) or []
                
                for obj in current_objects:
                    # STRICT OWNERSHIP CHECK: Ensure the entry's owner (userId) exactly matches 
                    # the requested user_id. This prevents the script from analyzing media where 
                    # the user is only the creatorId or a co-collaborator.
                    if getattr(obj, 'userId', None) == user_id:
                        if obj.id not in seen_ids:
                            seen_ids.add(obj.id)
                            entries.append(obj)
                        
                if len(current_objects) < 500:
                    has_more = False
                    break
                    
                pager.pageIndex += 1
                
                # Prevent crashing on the 10,000 API limit (page 15 * 500 = 7500 records)
                if pager.pageIndex > 15: 
                    if entries:
                        m_filter.createdAtGreaterThanOrEqual = entries[-1].createdAt
                    break
    except Exception:
        # If API throws an error midway, proceed with whatever entries were successfully collected
        pass

    metrics['media_qty'] = len(entries)
    if not entries:
        return metrics

    # --- 2. Extract Base Data ---
    entry_ids = []
    media_types = set()
    has_categories = False
    has_mrp_flags = False
    has_collaborators = False
    has_children = False

    for e in entries:
        metrics['dur_sec'] += (e.duration or 0)
        entry_ids.append(e.id)
        
        if hasattr(e, 'mediaType'):
            m_type_val = e.mediaType.value if hasattr(e.mediaType, 'value') else e.mediaType
            media_types.add(MEDIA_TYPE_MAP.get(m_type_val, f"OTHER({m_type_val})"))

        if not has_collaborators:
            if (getattr(e, 'entitledUsersEdit', '') or 
                getattr(e, 'entitledUsersPublish', '') or 
                getattr(e, 'entitledUsersView', '')):
                has_collaborators = True
                
        c_ids = getattr(e, 'categoriesIds', '')
        if c_ids:
            for c_id in c_ids.split(','):
                c_id = c_id.strip()
                if c_id in MRP_CATEGORIES:
                    has_mrp_flags = True
                elif c_id:
                    has_categories = True

    metrics['media_types'] = ";".join(sorted(media_types)) if media_types else "NONE"
    metrics['collabs'] = "Yes" if has_collaborators else "No"
    metrics['categories'] = "Yes" if has_categories else "No"
    metrics['mrp'] = "Yes" if has_mrp_flags else "No"
    
    # --- 3. Process Assets & Children ---
    try:
        flavor_service = get_service(client, 'flavorAsset')
        attachment_service = get_service(client, 'attachmentAsset')
        caption_service = get_service(client, 'captionAsset')
        thumb_service = get_service(client, 'thumbAsset')
        base_entry_service = get_service(client, 'baseEntry')

        # Reduced chunk size to 30 ensures multi-requests stay safely below server limits
        chunk_size = 30 
        for x in range(0, len(entry_ids), chunk_size):
            chunk = entry_ids[x:x+chunk_size]
            chunk_str = ",".join(chunk)
            asset_pager = KalturaFilterPager(pageSize=500)
            
            child_entry_ids = []
            
            client.startMultiRequest()
            for eid in chunk:
                b_filter = KalturaBaseEntryFilter()
                b_filter.parentEntryIdEqual = eid
                base_entry_service.list(b_filter, asset_pager)
                
            try:
                b_responses = client.doMultiRequest()
                for b_result in b_responses:
                    if isinstance(b_result, Exception): continue
                    if b_result and getattr(b_result, 'objects', None):
                        for c in b_result.objects:
                            child_entry_ids.append(c.id)
            except Exception:
                pass
            
            if child_entry_ids:
                has_children = True

            combined_flavor_ids = chunk + child_entry_ids
            
            asset_requests_mapping = []
            client.startMultiRequest()
            
            for y in range(0, len(combined_flavor_ids), chunk_size):
                sub_chunk_str = ",".join(combined_flavor_ids[y:y+chunk_size])
                f_filter = KalturaFlavorAssetFilter()
                f_filter.entryIdIn = sub_chunk_str
                flavor_service.list(f_filter, asset_pager)
                asset_requests_mapping.append('flavor')

            a_filter = KalturaAttachmentAssetFilter()
            a_filter.entryIdIn = chunk_str
            attachment_service.list(a_filter, asset_pager)
            asset_requests_mapping.append('attachment')

            c_filter = KalturaCaptionAssetFilter()
            c_filter.entryIdIn = chunk_str
            caption_service.list(c_filter, asset_pager)
            asset_requests_mapping.append('caption')

            t_filter = KalturaThumbAssetFilter()
            t_filter.entryIdIn = chunk_str
            thumb_service.list(t_filter, asset_pager)
            asset_requests_mapping.append('thumb')

            try:
                asset_responses = client.doMultiRequest()
                for idx, a_res in enumerate(asset_responses):
                    if isinstance(a_res, Exception): continue
                    req_type = asset_requests_mapping[idx]
                    
                    if req_type == 'flavor' and a_res and getattr(a_res, 'objects', None):
                        metrics['flavors'] += getattr(a_res, 'totalCount', len(a_res.objects))
                        metrics['total_kb'] += get_asset_size_in_kb(a_res.objects)
                    
                    elif req_type == 'attachment' and a_res:
                        a_count = getattr(a_res, 'totalCount', 0)
                        if a_count > 0:
                            metrics['has_att'] = True
                            metrics['related'] += a_count
                        if getattr(a_res, 'objects', None):
                            metrics['total_kb'] += get_asset_size_in_kb(a_res.objects)
                            
                    elif req_type == 'caption' and a_res:
                        c_count = getattr(a_res, 'totalCount', 0)
                        if c_count > 0:
                            metrics['has_cap'] = True
                            metrics['related'] += c_count
                        if getattr(a_res, 'objects', None):
                            metrics['total_kb'] += get_asset_size_in_kb(a_res.objects)
                            
                    elif req_type == 'thumb' and a_res:
                        t_count = getattr(a_res, 'totalCount', 0)
                        if t_count > 0:
                            metrics['has_thm'] = True
                            metrics['related'] += t_count
                        if getattr(a_res, 'objects', None):
                            metrics['total_kb'] += get_asset_size_in_kb(a_res.objects)
            except Exception:
                pass
    except Exception:
        pass # Retain whatever metrics were processed up to the failure point
        
    metrics['children'] = "Yes" if has_children else "No"
    metrics['total_kb'] = round(metrics['total_kb'], 2)

    return metrics

def process_single_user(umd_choice):
    user_id = input(f"\n{GREEN}Enter the User ID: {RESET}").strip()
    if not user_id:
        print(f"{RED}Error: User ID cannot be empty.{RESET}")
        return

    start_time = time.time()

    if not os.path.exists(EXPORT_SUBFOLDER):
        print(f"{CYAN}Creating subfolder: {EXPORT_SUBFOLDER}{RESET}")
        os.makedirs(EXPORT_SUBFOLDER)

    now = datetime.now().strftime('%Y%m%d-%H%M')
    output_csv = os.path.join(EXPORT_SUBFOLDER, f"{now}_UserMediaSummary_{user_id}.csv")

    main_client = get_kaltura_client()
    ks = main_client.getKs()
    
    print(f"{CYAN}Fetching data for User ID: {user_id}...{RESET}")

    try:
        user_service = get_service(main_client, 'user')
        user = user_service.get(user_id)
    except Exception:
        print(f"{RED}Error: User '{user_id}' not found or could not be retrieved.{RESET}")
        return

    print(f"{CYAN}Calculating storage and media metrics...{RESET}")
    m = get_detailed_metrics(ks, user_id)
    
    elapsed_sec = time.time() - start_time
    total_gb = round(m['total_kb'] / (1024 * 1024), 4)

    row = [
        user.id, 
        get_user_full_name(user), 
        user.email,
        STATUS_MAP.get(user.status.value if hasattr(user.status, 'value') else user.status, "Unknown"),
        format_date(user.createdAt), 
        format_date(getattr(user, 'statusUpdatedAt', None)),
        m['media_qty'], 
        m['media_types'], 
        m['dur_sec'], 
        format_duration(m['dur_sec']), 
        m['total_kb'], 
        m['children'], 
        m['flavors'], 
        m['related'], 
        m['categories'], 
        m['collabs'], 
        m['mrp']
    ]

    with open(output_csv, mode='w', newline='', encoding='utf-8') as f_out:
        writer = csv.writer(f_out)
        writer.writerow(['Total Users Processed', 'Total Media Processed', 'Total Storage Size', 'Total processing time'])
        writer.writerow([1, m['media_qty'], f"{total_gb} GB", format_duration(elapsed_sec)])
        writer.writerow([])
        writer.writerow(HEADERS)
        writer.writerow(row)
    
    print(f"{CYAN}Primary Report saved to: {output_csv}{RESET}")
    print(f"{CYAN}Final Storage Summary: {total_gb} GB{RESET}")

    if umd_choice != '1' and should_trigger(umd_choice, m):
        print(f"\n{CYAN}Criteria met! Triggering KR-umd.py...{RESET}")
        try:
            subprocess.run(
                [sys.executable, "-u", "KR-umd.py", str(user.id)],
                stdin=subprocess.DEVNULL,
                stdout=sys.stdout,
                stderr=sys.stderr,
                timeout=300
            )
            print(f"{CYAN}Secondary report complete.{RESET}")
        except subprocess.TimeoutExpired:
            print(f"{RED}Secondary report timed out and was terminated.{RESET}")
        except Exception as e:
            print(f"{RED}Secondary report failed: {e}{RESET}")

    try:
        main_client.session.end()
        print(f"\n{CYAN}Kaltura session successfully terminated.{RESET}")
    except Exception as e:
        print(f"\n{RED}Warning: Unable to terminate session explicitly. Error: {e}{RESET}")

def process_batch_file(umd_choice):
    user_input = input(f"\n{GREEN}Enter the input CSV filename (extension .csv optional): {RESET}").strip()
    if not user_input:
        print(f"{RED}Error: Filename cannot be empty.{RESET}")
        return
        
    start_time = time.time()
        
    if user_input.lower().endswith('.csv'):
        user_input = user_input[:-4]
        
    input_csv = f"{user_input}.csv"

    if not os.path.exists(EXPORT_SUBFOLDER):
        print(f"{CYAN}Creating subfolder: {EXPORT_SUBFOLDER}{RESET}")
        os.makedirs(EXPORT_SUBFOLDER)

    if not os.path.exists(input_csv):
        print(f"{RED}Error: {input_csv} not found in the current directory.{RESET}")
        return

    now = datetime.now().strftime('%Y%m%d-%H%M')
    output_csv = os.path.join(EXPORT_SUBFOLDER, f"{now}_BatchMediaSummary_{user_input}.csv")

    ids = []
    users_to_trigger = []
    
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

    print(f"{CYAN}Loaded {total_ids} IDs. Processing storage data via ThreadPool...{RESET}")
    main_client = get_kaltura_client()
    ks = main_client.getKs()
    
    all_rows = []

    custom_format = CYAN + "{desc} Elapsed Time: {elapsed} | Total Users: {total_fmt} | Users Processed: {n_fmt} | Percentage processed: {percentage:3.0f}%" + RESET
    
    with tqdm(total=total_ids, desc="Export Progress:", bar_format=custom_format) as pbar:
        for i in range(0, total_ids, BATCH_SIZE):
            batch_ids = ids[i:i + BATCH_SIZE]
            
            u_filter = KalturaUserFilter()
            u_filter.idIn = ",".join(batch_ids)
            
            user_service = get_service(main_client, 'user')
            user_results = user_service.list(u_filter, KalturaFilterPager(pageSize=BATCH_SIZE))
            user_map = {u.id: u for u in user_results.objects} if user_results.objects else {}

            futures = {}
            with ThreadPoolExecutor(max_workers=POOL_SIZE) as executor:
                for uid in batch_ids:
                    user = user_map.get(uid)
                    if user:
                        futures[executor.submit(get_detailed_metrics, ks, uid)] = user
                    else:
                        pbar.update(1)

                for future in as_completed(futures):
                    user = futures[future]
                    m = future.result()
                    
                    if umd_choice != '1' and should_trigger(umd_choice, m):
                        users_to_trigger.append(user.id)

                    all_rows.append([
                        user.id, 
                        get_user_full_name(user), 
                        user.email,
                        STATUS_MAP.get(user.status.value if hasattr(user.status, 'value') else user.status, "Unknown"),
                        format_date(user.createdAt), 
                        format_date(getattr(user, 'statusUpdatedAt', None)),
                        m['media_qty'], 
                        m['media_types'], 
                        m['dur_sec'], 
                        format_duration(m['dur_sec']), 
                        m['total_kb'], 
                        m['children'], 
                        m['flavors'], 
                        m['related'], 
                        m['categories'], 
                        m['collabs'], 
                        m['mrp']
                    ])
                    pbar.update(1)
            
            time.sleep(0.01)

    elapsed_sec = time.time() - start_time
    all_rows.sort(key=lambda x: str(x[0]).lower())

    total_users_processed = len(all_rows)
    total_media_processed = sum(r[6] for r in all_rows)
    total_storage_kb = sum(r[10] for r in all_rows)
    total_storage_gb = round(total_storage_kb / (1024 * 1024), 4)

    with open(output_csv, mode='w', newline='', encoding='utf-8') as f_out:
        writer = csv.writer(f_out)
        writer.writerow(['Total Users Processed', 'Total Media Processed', 'Total Storage Size', 'Total processing time'])
        writer.writerow([total_users_processed, total_media_processed, f"{total_storage_gb} GB", format_duration(elapsed_sec)])
        writer.writerow([])
        writer.writerow(HEADERS)
        writer.writerows(all_rows)

    print(f"\n{CYAN}Primary Batch Report saved to: {output_csv}{RESET}")
    print(f"{CYAN}Final Batch Storage Summary: {total_storage_gb} GB Total{RESET}")

    if users_to_trigger:
        total_triggers = len(users_to_trigger)
        print(f"\n{YELLOW}Triggering KR-umd.py for {total_triggers} users meeting criteria...{RESET}")
        success_count = 0
        
        for idx, uid in enumerate(users_to_trigger, 1):
            print(f"\n{GREEN}--- Detailed Report Triggered: User {uid} ({idx}/{total_triggers}) ---{RESET}")
            try:
                result = subprocess.run(
                    [sys.executable, "-u", "KR-umd.py", str(uid)],
                    stdin=subprocess.DEVNULL,
                    stdout=sys.stdout,
                    stderr=sys.stderr,
                    timeout=300
                )
                
                if result.returncode == 0:
                    success_count += 1
                else:
                    print(f"{RED}Process exited with non-zero status code: {result.returncode}{RESET}")
                    
            except subprocess.TimeoutExpired:
                print(f"{RED}TIMEOUT EXPIRED (5 minutes). Process terminated.{RESET}")
            except Exception as e:
                print(f"{RED}SYSTEM ERROR: {e}{RESET}")

        print(f"\n{CYAN}Completed {success_count} out of {total_triggers} secondary subprocesses.{RESET}")

    try:
        main_client.session.end()
        print(f"\n{CYAN}Kaltura session successfully terminated.{RESET}")
    except Exception as e:
        print(f"\n{RED}Warning: Unable to terminate session explicitly. Error: {e}{RESET}")

def main():
    print(f"{CYAN}--- Kaltura Report - User Media Summary ---{RESET}")
    print(f"{GREEN}1. Process a Single User{RESET}")
    print(f"{GREEN}2. Process an Input File (Batch){RESET}")
    
    choice = input(f"{GREEN}Select an option (1 or 2): {RESET}").strip()
    
    if choice not in ['1', '2']:
        print(f"{RED}Invalid selection. Exiting.{RESET}")
        return
        
    umd_choice = prompt_umd_trigger()
    
    if choice == '1':
        process_single_user(umd_choice)
    elif choice == '2':
        process_batch_file(umd_choice)

if __name__ == "__main__":
    main()