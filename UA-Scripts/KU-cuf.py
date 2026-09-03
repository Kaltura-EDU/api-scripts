"""
PROCESS: Kaltura Update - Cleanup User Extra Flavors, KU-cuf.py
LAST UPDATED: 2026-08-11 12:01:34
SUMMARY:
This script cleans up extra transcoded media flavors to reclaim storage space.
*OPTIMIZATIONS: Explicit counter clamping on completion to guarantee 100% terminal updates for both Search and Evaluation phases.
"""

import os
import csv
import time
import sys
import glob
import shutil
import socket
import random
import threading
import concurrent.futures
from datetime import datetime
from dotenv import load_dotenv
from KalturaClient import *
from KalturaClient.Plugins.Core import *

# --- TERMINAL COLOR CODES ---
COLOR_RESET = "\033[0m"
COLOR_INPUT = "\033[92m"   
COLOR_INFO = "\033[96m"    
COLOR_ERROR = "\033[91m"   
COLOR_WARN = "\033[93m"    

socket.setdefaulttimeout(120)
ui_lock = threading.Lock()

# --- GLOBAL UI STATE ---
ui_state = {
    'phase': 0, 'done': False, 'start_time': time.time(),
    'processed_users': 0, 'total_users': 0, 'processed_entries': 0,
    'total_entries': 0, 'retries': 0, 'last_error': 'None'
}

if not os.path.exists('.env'):
    fallback_files = [f for f in (glob.glob('*.env') + glob.glob('.env.*')) if os.path.isfile(f)]
    if fallback_files: shutil.copy(fallback_files[0], '.env')

load_dotenv()
PARTNER_ID = os.getenv('PARTNER_ID')
ADMIN_SECRET = os.getenv('ADMIN_SECRET')
ADMIN_ID = os.getenv('ADMIN_ID')
SERVICE_URL = "https://www.kaltura.com"

# --- TUNING PARAMETERS ---
MAX_WORKERS_PHASE1 = 15     
MAX_WORKERS_PHASE2 = 10     
USER_CHUNK_SIZE = 30        
BATCH_SIZE = 50             
CLEANUP_CATEGORY_ID = "409913462" 
SESSION_EXPIRY = 43200  

# --- MRP THRESHOLDS (Days) ---
MRP_MIN_CREATED_DAYS = 1103   # Media must be AT LEAST this old to process
MRP_MAX_VIEWED_DAYS = 1095    # Skip processing if viewed within this many days

MEDIA_TYPE_MAP = {
    1: "Video", 2: "Image", 5: "Audio", 6: "Live Stream Video", 
    7: "Live Stream Audio", 201: "Live Stream Video", 202: "Live Stream Audio", -1: "Data"
}

PROCESSING_HEADER = [
    "User ID", "Media Entry ID", "Media Type", "Media Name", 
    "Created Date", "Last Viewed", "Flavors Deleted", "Child Flavs Deleted", 
    "Total Size Before (KB)", "Total Size Deleted (KB)", 
    "Collaborators", "Categories", "Notes", "Operation", "Error"
]

def get_session():
    config = KalturaConfiguration(PARTNER_ID)
    config.serviceUrl = SERVICE_URL
    client = KalturaClient(config)
    ks = client.session.start(ADMIN_SECRET, ADMIN_ID, KalturaSessionType.ADMIN, PARTNER_ID, SESSION_EXPIRY)
    return ks

def get_thread_client(ks):
    config = KalturaConfiguration(PARTNER_ID)
    config.serviceUrl = SERVICE_URL
    client = KalturaClient(config)
    client.setKs(ks)
    return client

def api_retry(func, *args, retries=3, base_delay=1, **kwargs):
    last_exception = None
    for attempt in range(retries):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            last_exception = e
            error_str = str(e).lower()
            
            with ui_lock:
                ui_state['last_error'] = str(e).replace('\n', ' ')[:40]
            
            if "invalid" in error_str and "session" in error_str: raise e 
            if "property" in error_str or "does not exist" in error_str: raise e
            
            if attempt > 0: 
                with ui_lock: ui_state['retries'] += 1
            
            time.sleep(base_delay * (2 ** attempt) + random.uniform(0.1, 0.5))
            
    raise last_exception

def draw_ui_line(title):
    elapsed = format_time_hhmmss(time.time() - ui_state['start_time'])
    err_disp = ui_state.get('last_error', 'None')
    err_str = f" | Err: {err_disp}" if err_disp != 'None' else ""
    
    # Clamp counters to totals on completion frame
    p_users = ui_state['total_users'] if ui_state['done'] else ui_state['processed_users']
    p_entries = ui_state['total_entries'] if ui_state['done'] else ui_state['processed_entries']

    if ui_state['phase'] == 1:
        sys.stdout.write(f"\r{COLOR_INFO}{title}: {elapsed} | Users: {p_users}/{ui_state['total_users']} | Media: {ui_state['total_entries']} | Retries: {ui_state['retries']}{err_str}   \033[K{COLOR_RESET}")
    elif ui_state['phase'] == 2:
        sys.stdout.write(f"\r{COLOR_INFO}{title}: {elapsed} | Done: {p_entries}/{ui_state['total_entries']} | Retries: {ui_state['retries']}{err_str}   \033[K{COLOR_RESET}")
    sys.stdout.flush()

def ui_monitor_thread(title):
    while not ui_state['done']:
        with ui_lock:
            draw_ui_line(title)
        time.sleep(0.5)
        
    # Render final pass to guarantee 100% completion numbers are drawn on exit
    with ui_lock:
        draw_ui_line(title)
        sys.stdout.write("\n")
        sys.stdout.flush()

def validate_user(client, user_id):
    try:
        api_retry(client.user.get, user_id)
        return True
    except:
        return False

def kb_to_gb(kb_val):
    return round(float(kb_val) / (1024 * 1024), 4) if kb_val else 0.0000

def format_time_hhmmss(seconds):
    hh, rem = divmod(seconds, 3600)
    mm, ss = divmod(rem, 60)
    return f"{int(hh):02d}:{int(mm):02d}:{int(ss):02d}"

def get_flavor_size(flavor):
    val_legacy = float(getattr(flavor, 'size', 0) or 0.0)
    val_bytes = float(getattr(flavor, 'sizeInBytes', 0) or 0.0)
    return val_legacy if val_legacy > 0 else val_bytes

def get_timed_input(prompt, timeout=10, default="Y"):
    start_time = time.time()
    if sys.platform == "win32":
        import msvcrt
        result = ""
        while True:
            remaining = int(timeout - (time.time() - start_time))
            if remaining < 0: return default
            sys.stdout.write(f"\r\033[K{prompt} [Auto-defaults to '{default}' in {remaining}s]: {result}")
            sys.stdout.flush()
            if msvcrt.kbhit():
                char = msvcrt.getwche()
                if char in ('\r', '\n'): return result if result else default
                elif char == '\x08': 
                    result = result[:-1]
                    sys.stdout.write(" \b")
                else: result += char
            time.sleep(0.1)
    else:
        import select
        while True:
            remaining = int(timeout - (time.time() - start_time))
            if remaining < 0: return default
            sys.stdout.write(f"\r\033[K{prompt} [Auto-defaults to '{default}' in {remaining}s]: ")
            sys.stdout.flush()
            ready, _, _ = select.select([sys.stdin], [], [], 0.5)
            if ready:
                result = sys.stdin.readline().strip()
                return result if result else default

def fetch_flavors_multirequest(client, entry_ids_list):
    """Bundles flavor lookups into a single MultiRequest call."""
    if not entry_ids_list: return []
    all_flavors = []
    
    for i in range(0, len(entry_ids_list), 50):
        chunk = entry_ids_list[i:i+50]
        try:
            client.startMultiRequest()
            for e_id in chunk:
                filter = KalturaFlavorAssetFilter()
                filter.entryIdEqual = e_id
                pager = KalturaFilterPager()
                pager.pageSize = 100
                client.flavorAsset.list(filter, pager)
            
            results = api_retry(client.doMultiRequest, retries=2)
            for res in results:
                if hasattr(res, 'objects'):
                    all_flavors.extend(res.objects)
        except Exception:
            for e_id in chunk:
                try:
                    filter = KalturaFlavorAssetFilter()
                    filter.entryIdEqual = e_id
                    pager = KalturaFilterPager()
                    pager.pageSize = 100
                    res = api_retry(client.flavorAsset.list, filter, pager)
                    if hasattr(res, 'objects'): all_flavors.extend(res.objects)
                except Exception: pass
    return all_flavors

def fetch_children_multirequest(client, entry_ids_list):
    """Bundles child entry lookups into a single MultiRequest call."""
    if not entry_ids_list: return []
    all_children = []
    
    for i in range(0, len(entry_ids_list), 50):
        chunk = entry_ids_list[i:i+50]
        try:
            client.startMultiRequest()
            for e_id in chunk:
                filter = KalturaMediaEntryFilter()
                filter.parentEntryIdEqual = e_id
                pager = KalturaFilterPager()
                pager.pageSize = 100
                client.baseEntry.list(filter, pager)
            
            results = api_retry(client.doMultiRequest, retries=2)
            for res in results:
                if hasattr(res, 'objects'):
                    all_children.extend(res.objects)
        except Exception:
            pass
    return all_children

def evaluate_flavors_for_cleanup(flavors):
    flavors_to_delete, deleted_fids, deleted_kb = [], [], 0
    
    source_flavor = next((f for f in flavors if getattr(f, 'isOriginal', False)), None)
    if not source_flavor:
        source_flavor = next((f for f in flavors if getattr(f, 'flavorParamsId', None) in [0, "0"]), None)
    if not source_flavor:
        source_flavor = max(flavors, key=get_flavor_size) if flavors else None
        
    if source_flavor:
        for f in flavors:
            if f.id == source_flavor.id: continue
            fid = getattr(f, 'flavorParamsId', "Unknown")
            f_kb = get_flavor_size(f)
            flavors_to_delete.append(f)
            deleted_fids.append(fid)
            deleted_kb += f_kb
                
    return flavors_to_delete, deleted_fids, deleted_kb

def fetch_media_for_user_chunk(u_id_chunk, global_ks):
    client = get_thread_client(global_ks)
    user_entries = []
    filter = KalturaMediaEntryFilter()
    filter.userIdIn = ",".join(u_id_chunk)
    pager = KalturaFilterPager()
    pager.pageIndex = 1
    pager.pageSize = 500
    
    while True:
        try:
            results = api_retry(client.media.list, filter, pager)
            if hasattr(results, 'objects'):
                for obj in results.objects:
                    found_u_id = obj.userId if obj.userId else "Unknown_User"
                    user_entries.append((found_u_id, obj))
                    
            if len(user_entries) >= getattr(results, 'totalCount', 0) or len(getattr(results, 'objects', [])) < 500: 
                break
            pager.pageIndex += 1
        except Exception as e:
            if "session" in str(e).lower() or "invalid" in str(e).lower():
                client = get_thread_client(get_session()) 
                continue
            break

    with ui_lock:
        ui_state['processed_users'] += len(u_id_chunk)
        ui_state['total_entries'] += len(user_entries)

    return user_entries

def process_batch(batch, global_ks, today_dt, mrp_on, modetype):
    batch_report = []
    batch_unique_fids = set()
    batch_size_before, batch_size_freed = 0, 0
    batch_flag1_count, batch_flag2_count = 0, 0
    batch_success_count, batch_fail_count = 0, 0
    
    try:
        client = get_thread_client(global_ks)
        staged_deletes, staged_updates = [], []
        entry_results = {}
        entry_ids = [entry.id for u_id, entry in batch]
        
        try:
            parent_flavor_results = fetch_flavors_multirequest(client, entry_ids)
        except Exception as e:
            if "session" in str(e).lower() or "invalid" in str(e).lower():
                client = get_thread_client(get_session())
                parent_flavor_results = fetch_flavors_multirequest(client, entry_ids)
            else:
                parent_flavor_results = []

        flavors_by_entry = {}
        for f in parent_flavor_results:
            flavors_by_entry.setdefault(getattr(f, 'entryId', None), []).append(f)

        all_child_entries = fetch_children_multirequest(client, entry_ids)
        child_entries_by_parent = {}
        for c in all_child_entries:
            child_entries_by_parent.setdefault(getattr(c, 'parentEntryId', None), []).append(c)

        child_flavors_by_entry = {}
        if all_child_entries:
            all_child_ids = [c.id for c in all_child_entries]
            child_flavor_results = fetch_flavors_multirequest(client, all_child_ids)
            for f in child_flavor_results:
                child_flavors_by_entry.setdefault(getattr(f, 'entryId', None), []).append(f)

        for u_id, entry in batch:
            try:
                parent_flavors = flavors_by_entry.get(entry.id, [])
                child_entries = child_entries_by_parent.get(entry.id, [])
                
                parent_size = sum(get_flavor_size(f) for f in parent_flavors)
                child_size = sum(get_flavor_size(f) for c in child_entries for f in child_flavors_by_entry.get(c.id, []))
                    
                entry_size_before = parent_size + child_size
                batch_size_before += entry_size_before
                
                cats = entry.categories if entry.categories else ""
                collabs = []
                if getattr(entry, 'entitledUsersEdit', None): collabs.extend([u.strip() for u in entry.entitledUsersEdit.split(',')])
                if getattr(entry, 'entitledUsersPublish', None): collabs.extend([u.strip() for u in entry.entitledUsersPublish.split(',')])
                collabs_str = ";".join(set(collabs)) if collabs else "none"

                created_epoch = getattr(entry, 'createdAt', None)
                played_epoch = getattr(entry, 'lastPlayedAt', None)
                
                days_created = (today_dt - datetime.fromtimestamp(created_epoch)).days if created_epoch else 0
                created_str = datetime.fromtimestamp(created_epoch).strftime("%Y-%m-%d") if created_epoch else "Unknown"
                
                days_viewed = (today_dt - datetime.fromtimestamp(played_epoch)).days if played_epoch else None
                played_str = datetime.fromtimestamp(played_epoch).strftime("%Y-%m-%d") if played_epoch else "None"

                notes, flag_type = "Processed Successfully", 0
                if mrp_on:
                    if days_created >= MRP_MIN_CREATED_DAYS and days_viewed is not None and days_viewed < MRP_MAX_VIEWED_DAYS:
                        notes, flag_type = f"Not Processed, View < {MRP_MAX_VIEWED_DAYS} days.", 1
                        batch_flag1_count += 1
                    elif days_created < MRP_MIN_CREATED_DAYS:
                        notes, flag_type = f"Not Processed, Created < {MRP_MIN_CREATED_DAYS} days.", 2
                        batch_flag2_count += 1
                else: notes = "Processed Successfully (MRP Off)"

                parent_deleted_fids, child_deleted_fids, entry_deleted_kb = [], [], 0

                if flag_type == 0:
                    if parent_flavors:
                        f_objs, p_fids, p_kb = evaluate_flavors_for_cleanup(parent_flavors)
                        for f in f_objs: staged_deletes.append((entry.id, f))
                        parent_deleted_fids.extend(p_fids)
                        entry_deleted_kb += p_kb
                        
                    for c in child_entries:
                        if c_flavs := child_flavors_by_entry.get(c.id, []):
                            f_objs, c_fids, c_kb = evaluate_flavors_for_cleanup(c_flavs)
                            for f in f_objs: staged_deletes.append((entry.id, f))
                            child_deleted_fids.extend(c_fids)
                            entry_deleted_kb += c_kb
                            
                    batch_unique_fids.update(parent_deleted_fids)
                    batch_unique_fids.update(child_deleted_fids)
                    
                    if parent_deleted_fids or child_deleted_fids:
                        current_cat_ids = entry.categoriesIds if entry.categoriesIds else ""
                        if CLEANUP_CATEGORY_ID not in [x.strip() for x in current_cat_ids.split(',')]:
                            new_cat_ids = f"{current_cat_ids},{CLEANUP_CATEGORY_ID}" if current_cat_ids else CLEANUP_CATEGORY_ID
                            update_entry = KalturaMediaEntry()
                            update_entry.categoriesIds = new_cat_ids
                            staged_updates.append((entry.id, update_entry))
                                
                batch_size_freed += entry_deleted_kb
                media_type_name = MEDIA_TYPE_MAP.get(entry.mediaType.value, f"Unknown ({entry.mediaType.value})") if getattr(entry, 'mediaType', None) else "N/A"
                
                parent_flavs_str = ", ".join(map(str, parent_deleted_fids)) if parent_deleted_fids else "none"
                child_flavs_str = ", ".join(map(str, child_deleted_fids)) if child_deleted_fids else ("None" if not child_entries else "none")

                entry_results[entry.id] = {
                    'row_base': [
                        u_id, entry.id, media_type_name, entry.name, created_str, played_str,
                        parent_flavs_str, child_flavs_str, entry_size_before,
                        entry_deleted_kb if flag_type == 0 else 0, collabs_str,
                        cats if cats else "none", notes
                    ],
                    'status': 'success',
                    'error': ''
                }
            except Exception as e:
                entry_results[entry.id] = {
                    'row_base': [u_id, entry.id, "Error", "Error", "Error", "Error", "none", "none", 0, 0, "none", "none", "Staging Failed"],
                    'status': 'fail', 'error': f"Evaluation Error: {str(e)}"
                }

        if modetype == "LIVE" and (staged_deletes or staged_updates):
            try:
                client.startMultiRequest()
                request_mappings = [] 
                
                for e_id, f in staged_deletes:
                    client.flavorAsset.delete(f.id)
                    request_mappings.append(e_id)
                    
                for e_id, update_entry in staged_updates:
                    client.media.update(e_id, update_entry)
                    request_mappings.append(e_id)
                    
                mr_results = api_retry(client.doMultiRequest)
                for i, res in enumerate(mr_results):
                    if isinstance(res, Exception) or (hasattr(res, 'objectType') and 'Exception' in res.objectType):
                        mapped_e_id = request_mappings[i]
                        entry_results[mapped_e_id]['status'] = 'fail'
                        err_msg = getattr(res, 'message', str(res))
                        entry_results[mapped_e_id]['error'] += f"[API Error: {err_msg}] "
            except Exception:
                for e_id, f in staged_deletes:
                    try: api_retry(client.flavorAsset.delete, f.id)
                    except Exception as ex: 
                        entry_results[e_id]['status'] = 'fail'
                        entry_results[e_id]['error'] += f"[Delete Error: {str(ex)}] "
                for e_id, update_entry in staged_updates:
                    try: api_retry(client.media.update, e_id, update_entry)
                    except Exception as ex: 
                        entry_results[e_id]['status'] = 'fail'
                        entry_results[e_id]['error'] += f"[Update Error: {str(ex)}] "
                        
        for u_id, entry in batch:
            data = entry_results.get(entry.id)
            if data:
                row = data['row_base'] + [data['status'], data['error'].strip()]
                batch_report.append(row)
                if data['status'] == 'success': batch_success_count += 1
                else: batch_fail_count += 1
                
    except Exception as e:
        for u_id, entry in batch:
            batch_report.append([u_id, entry.id, "Error", "Error", "Error", "Error", "none", "none", 0, 0, "none", "none", "Thread Crash", "fail", str(e)])
            batch_fail_count += 1

    finally:
        with ui_lock: ui_state['processed_entries'] += len(batch)
            
    return batch_report, batch_size_before, batch_size_freed, batch_unique_fids, batch_flag1_count, batch_flag2_count, batch_success_count, batch_fail_count

def main():
    global ui_state
    
    print(f"{COLOR_INFO}--- Operation Mode Selection ---{COLOR_RESET}")
    print(f"{COLOR_INPUT}Select Mode:\n1. DryRun (Report only, NO deletions)\n2. LIVE (Perform deletions){COLOR_RESET}")
    
    mode_input = get_timed_input(f"{COLOR_INPUT}[Selection]{COLOR_RESET}", timeout=10, default="1").strip()
    print() 
    
    if mode_input == '2':
        modetype, modetype_name = "LIVE", "LIVE"
        print(f"{COLOR_ERROR}WARNING: LIVE mode selected. Deletions WILL be executed.{COLOR_RESET}\n")
    else:
        modetype, modetype_name = "DryRun", "DryRun"
        print(f"{COLOR_INFO}Notice: DryRun mode selected. NO deletions will occur.{COLOR_RESET}\n")

    if not PARTNER_ID or not ADMIN_SECRET or not ADMIN_ID:
        print(f"{COLOR_ERROR}Error: Missing credentials. Please check your .env file setup.{COLOR_RESET}")
        return

    print(f"{COLOR_INFO}Kaltura Session Key Authentication...{COLOR_RESET}")
    ks = get_session()
    client = get_thread_client(ks)
    
    print(f"{COLOR_INFO}--- Kaltura Media Cleanup Processing ---{COLOR_RESET}")
    print(f"{COLOR_INFO}1. Single Media Entry\n2. Single User\n3. Bulk Media Entry (via CSV)\n4. Bulk User (via CSV){COLOR_RESET}")
    choice = input(f"{COLOR_INPUT}Select an option: {COLOR_RESET}").strip()

    mrp_input = get_timed_input(f"{COLOR_INPUT}Continue with MRP enabled (Y/n)?{COLOR_RESET}", timeout=10, default="Y").strip().lower()
    print() 
    
    mrp_on = False if mrp_input == 'n' else True

    user_ids, all_discovered_entries, file_label, input_filename = [], [], "", ""

    if choice == '1':
        e_id = input(f"{COLOR_INPUT}Enter Media Entry ID: {COLOR_RESET}").strip()
        try:
            entry = client.media.get(e_id)
            u_id = entry.userId if entry.userId else "Unknown_User"
            user_ids.append(u_id); all_discovered_entries.append((u_id, entry))
            file_label, input_filename = "entry", e_id
            print(f"{COLOR_INFO}Entry validated.{COLOR_RESET}")
        except:
            print(f"{COLOR_ERROR}Error: Entry not found.{COLOR_RESET}")
            return

    elif choice == '2':
        u_id = input(f"{COLOR_INPUT}Enter User ID: {COLOR_RESET}").strip()
        if validate_user(client, u_id):
            user_ids.append(u_id)
            file_label, input_filename = "user", u_id
        else:
            print(f"{COLOR_ERROR}Error: User not found.{COLOR_RESET}")
            return

    elif choice == '3':
        csv_input = input(f"{COLOR_INPUT}Enter CSV filename: {COLOR_RESET}").strip()
        csv_path = csv_input if csv_input.lower().endswith('.csv') else f"{csv_input}.csv"
        input_filename = csv_input[:-4] if csv_input.lower().endswith('.csv') else csv_input
        
        if not os.path.exists(csv_path): return print(f"{COLOR_ERROR}File not found.{COLOR_RESET}")
            
        file_label = "bulkentries"
        entry_ids_list = []
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader, None)  
            for row in reader:
                if row and row[0].strip(): entry_ids_list.append(row[0].strip())
                    
        if not entry_ids_list: return print(f"{COLOR_ERROR}CSV empty.{COLOR_RESET}")
            
        print(f"{COLOR_INFO}Fetching metadata for {len(entry_ids_list)} entries...{COLOR_RESET}")
        for i in range(0, len(entry_ids_list), 100):
            chunk = entry_ids_list[i:i+100]
            filter = KalturaMediaEntryFilter()
            filter.idIn = ",".join(chunk)
            pager = KalturaFilterPager(); pager.pageSize = 500
            
            try:
                results = api_retry(client.media.list, filter, pager)
                for obj in results.objects:
                    u_id = obj.userId if obj.userId else "Unknown_User"
                    if u_id not in user_ids: user_ids.append(u_id)
                    all_discovered_entries.append((u_id, obj))
            except Exception as e:
                print(f"{COLOR_WARN}Warning: Fetch failed: {e}{COLOR_RESET}")

    elif choice == '4':
        csv_input = input(f"{COLOR_INPUT}Enter CSV filename: {COLOR_RESET}").strip()
        csv_path = csv_input if csv_input.lower().endswith('.csv') else f"{csv_input}.csv"
        input_filename = csv_input[:-4] if csv_input.lower().endswith('.csv') else csv_input
        
        if not os.path.exists(csv_path): return print(f"{COLOR_ERROR}File not found.{COLOR_RESET}")
            
        file_label = "bulkusers"
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader, None)  
            for row in reader:
                if row: user_ids.append(row[0].strip())
    else:
        return print(f"{COLOR_ERROR}Invalid selection.{COLOR_RESET}")

    if not user_ids and len(all_discovered_entries) == 0:
        return print(f"{COLOR_INFO}No valid inputs.{COLOR_RESET}")

    script_start_time = time.time()
    ui_state['start_time'] = script_start_time

    if not os.path.exists("Exports"): os.makedirs("Exports")
    timestamp = datetime.now().strftime("%Y%m%d-%H%M")
    
    processing_filepath = os.path.join("Exports", f"{timestamp}_{input_filename}_{modetype_name}_cufProcessing.csv")
    final_filepath = os.path.join("Exports", f"{timestamp}_{file_label}cleanup_{modetype_name}_{input_filename}.csv")

    with open(processing_filepath, 'w', newline='', encoding='utf-8') as pf:
        csv.writer(pf).writerow(PROCESSING_HEADER)

    if choice in ['2', '4']:
        print(f"{COLOR_INFO}Fetching media entries concurrently...{COLOR_RESET}")
        ui_state.update({'phase': 1, 'total_users': len(user_ids), 'done': False})
        ui_thread = threading.Thread(target=ui_monitor_thread, args=("Search Time",), daemon=True)
        ui_thread.start()

        user_chunks = [user_ids[i:i + USER_CHUNK_SIZE] for i in range(0, len(user_ids), USER_CHUNK_SIZE)]
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS_PHASE1) as executor:
            for future in concurrent.futures.as_completed({executor.submit(fetch_media_for_user_chunk, chunk, ks): chunk for chunk in user_chunks}):
                try:
                    if entries := future.result(): all_discovered_entries.extend(entries)
                except Exception: pass
                    
        ui_state['done'] = True; ui_thread.join()

    total_count = len(all_discovered_entries)
    if total_count == 0:
        if os.path.exists(processing_filepath): os.remove(processing_filepath)
        return print(f"{COLOR_INFO}No media entries found.{COLOR_RESET}")

    unique_flavor_ids, global_size_before, global_size_freed = set(), 0, 0
    total_flag1, total_flag2, total_success, total_fail = 0, 0, 0, 0

    print(f"{COLOR_INFO}Starting evaluation of {total_count} entries...{COLOR_RESET}", flush=True)
    ui_state.update({'phase': 2, 'total_entries': total_count, 'processed_entries': 0, 'retries': 0, 'last_error': 'None', 'done': False})
    
    ui_thread = threading.Thread(target=ui_monitor_thread, args=("Evaluation Time",), daemon=True)
    ui_thread.start()

    flat_batches = [all_discovered_entries[i:i + BATCH_SIZE] for i in range(0, total_count, BATCH_SIZE)]
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS_PHASE2) as executor:
        for future in concurrent.futures.as_completed({executor.submit(process_batch, batch, ks, datetime.now(), mrp_on, modetype): batch for batch in flat_batches}):
            try:
                if result := future.result():
                    c_report, c_before, c_freed, c_fids, c_flag1, c_flag2, c_succ, c_fail = result
                    if c_report:
                        with open(processing_filepath, 'a', newline='', encoding='utf-8') as pf:
                            writer = csv.writer(pf)
                            for row in c_report: writer.writerow(row)

                    global_size_before += c_before; global_size_freed += c_freed
                    unique_flavor_ids.update(c_fids)
                    total_flag1 += c_flag1; total_flag2 += c_flag2
                    total_success += c_succ; total_fail += c_fail
            except Exception: pass 
                
    ui_state['done'] = True; ui_thread.join()

    print(f"{COLOR_INFO}Finalizing report...{COLOR_RESET}", flush=True)
    
    flavor_legend = {}
    for fid in unique_flavor_ids:
        if fid in ["Unknown", 0, "0"]: flavor_legend[fid] = "Original Source File"
        else:
            try: flavor_legend[fid] = getattr(api_retry(client.flavorParams.get, fid, retries=2), 'description', getattr(api_retry(client.flavorParams.get, fid, retries=2), 'name', str(fid)))
            except: flavor_legend[fid] = "N/A (Definition not found)"

    final_data_rows = []
    with open(processing_filepath, 'r', encoding='utf-8') as pf:
        reader = csv.reader(pf); header_row = next(reader, None)
        for row in reader:
            if row: final_data_rows.append(row)

    final_data_rows.sort(key=lambda x: (x[0], x[1]))

    with open(final_filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["Total Processing Time", "ModeType", "Total Users", "Total Media Entries", "Total Space Before (GB)", "Total Space Freed (GB)", f"Flag 1 (View < {MRP_MAX_VIEWED_DAYS}d) Count", f"Flag 2 (Created < {MRP_MIN_CREATED_DAYS}d) Count", "Total Successes", "Total Failures"])
        writer.writerow([format_time_hhmmss(time.time() - script_start_time), modetype_name, len(user_ids), total_count, kb_to_gb(global_size_before), kb_to_gb(global_size_freed), total_flag1, total_flag2, total_success, total_fail])
        writer.writerow([]) 
        if header_row: writer.writerow(header_row)
        for row in final_data_rows: writer.writerow(row)
        writer.writerow([])
        writer.writerow(["FLAVOR ID LEGEND"])
        writer.writerow(["Flavor ID", "Description"])
        for fid, desc in flavor_legend.items(): writer.writerow([fid, desc])

    print(f"{COLOR_INFO}Cleanup complete. Final report generated: {final_filepath}{COLOR_RESET}")

    if os.path.exists(processing_filepath):
        try: os.remove(processing_filepath)
        except Exception: pass

    try: client.session.end()
    except Exception: pass

if __name__ == "__main__":
    main()