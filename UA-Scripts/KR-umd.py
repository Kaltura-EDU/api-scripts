"""
PROCESS: Kaltura Report User Media Detail, KR-umd.py
LAST UPDATED: 2026-07-13 08:35:42 AKDT
SUMMARY:
This script accepts a User ID via command-line arguments or interactive input to generate an itemized CSV report of all owned Kaltura media entries. It uses highly optimized bulk-fetching (entryIdIn) to retrieve related assets, and explicitly fetches categoryEntry data to map private/entitled categories hidden by default. To bypass Kaltura's 10,000 API pagination limit, it utilizes a time-windowed infinite scrolling mechanism. Data is streamed directly to the CSV per-page to maintain a near-zero memory footprint and ensure data recovery in the event of an interruption.
"""

import os
import csv
import time
import sys
import glob
import shutil
from collections import defaultdict
from datetime import datetime
from dotenv import load_dotenv
from KalturaClient import *
from KalturaClient.Plugins.Core import *
from KalturaClient.Plugins.Attachment import *
from KalturaClient.Plugins.Caption import *

# --- ENV FILE HANDLING ---
if not os.path.exists(".env"):
    env_files = [f for f in glob.glob("*.env") if f != ".env"]
    if env_files:
        source_env = env_files[0]
        print(f"No '.env' file found. Copying '{source_env}' to '.env'...")
        shutil.copy(source_env, ".env")

# Load environment variables
load_dotenv()

# --- CONFIGURATION ---
PARTNER_ID_STR = os.getenv("PARTNER_ID")
PARTNER_ID = int(PARTNER_ID_STR) if PARTNER_ID_STR else None
ADMIN_SECRET = os.getenv("ADMIN_SECRET")
ADMIN_ID = os.getenv("ADMIN_ID")  
SERVICE_URL = "https://www.kaltura.com"
EXPORT_DIR = "exports"

MEDIA_TYPE_MAP = {
    1: "Video",
    2: "Image",
    5: "Audio",
    6: "LiveStream (Video)",
    201: "Presentation",
    100: "Document"
}

def get_kaltura_client(provided_ks=None):
    if not all([PARTNER_ID, ADMIN_SECRET, ADMIN_ID]):
        raise ValueError("Missing required environment variables (PARTNER_ID, ADMIN_SECRET, or ADMIN_ID)")
        
    config = KalturaConfiguration(PARTNER_ID)
    config.serviceUrl = SERVICE_URL
    client = KalturaClient(config)
    
    if provided_ks:
        client.setKs(provided_ks)
    else:
        expiry = 43200 
        ks = client.session.start(ADMIN_SECRET, ADMIN_ID, KalturaSessionType.ADMIN, PARTNER_ID, expiry)
        client.setKs(ks)
        
    return client

def get_service(client, service_name):
    """Safely resolves a Kaltura service attribute across SDK architectures."""
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

def validate_user(client, target_user_id):
    try:
        get_service(client, 'user').get(target_user_id)
        return True
    except:
        return False

def format_date(timestamp):
    if not timestamp or timestamp <= 0:
        return "never"
    return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M')

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

def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def main():
    target_user_id = None
    provided_ks = None
    is_standalone = True

    if len(sys.argv) > 1:
        target_user_id = sys.argv[1]
        print(f"Received User ID from batch process: {target_user_id}")
        if len(sys.argv) > 2:
            provided_ks = sys.argv[2]
            is_standalone = False
            print("Inherited Kaltura Session from parent script.")
    else:
        target_user_id = input("Enter the User ID to export media data for:  ").strip()

    if not target_user_id:
        print("Error: User ID cannot be empty.")
        return
    
    try:
        client = get_kaltura_client(provided_ks)
    except Exception as e:
        print(f"Connection Error: {e}")
        return

    print(f"Validating User ID: {target_user_id}...")
    if not validate_user(client, target_user_id):
        print("Error: User ID not found or invalid.")
        return

    if not os.path.exists(EXPORT_DIR):
        os.makedirs(EXPORT_DIR)

    # Prepare initial filter and pager
    filter = KalturaMediaEntryFilter()
    filter.userIdEqual = target_user_id
    filter.orderBy = KalturaMediaEntryOrderBy.CREATED_AT_ASC  # Crucial for 10k bypass
    
    pager = KalturaFilterPager()
    pager.pageSize = 20
    
    batch_pager = KalturaFilterPager()
    batch_pager.pageSize = 500 
    
    media_service = get_service(client, 'media')
    
    # Get total count for display purposes
    initial_result = media_service.list(filter, pager)
    total_entries = initial_result.totalCount
    
    if total_entries == 0:
        print(f"No media entries found for user {target_user_id}.")
        return

    print(f"Found {total_entries} entries. Processing stream to disk...")

    flavor_service = get_service(client, 'flavorAsset')
    attachment_service = get_service(client, 'attachmentAsset')
    caption_service = get_service(client, 'captionAsset')
    thumb_service = get_service(client, 'thumbAsset')
    base_entry_service = get_service(client, 'baseEntry')
    category_service = get_service(client, 'category')
    category_entry_service = get_service(client, 'categoryEntry')

    # Tracking variables
    grand_total_size_kb = 0
    processed_count = 0
    start_time = time.time()
    found_flavor_ids = set()
    found_category_ids = set()
    total_children_found = 0
    total_related_files_found = 0
    
    # Pagination & Deduplication tracking
    page_index = 1
    last_created_at = 0
    seen_entry_ids = set()

    now_str = datetime.now().strftime("%Y%m%d-%H%M")
    file_path = os.path.join(EXPORT_DIR, f"{now_str}_mediaDetail_{target_user_id}.csv")

    headers = [
        "entry id", "media type", "media name", "created", "edited", "viewed", 
        "flavors", "size_kb", "Children", "children_flavors", "children_kb", 
        "attachments", "attachments_kb", "captions", "captions_kb", "thumbs", 
        "thumbs_kb", "Category Ids", "co-editors", "co-publishers", "AdminTags", "Tags"
    ]

    # --- OPEN CSV AND STREAM DATA ---
    with open(file_path, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(headers)

        while True:
            pager.pageIndex = page_index
            entries_result = media_service.list(filter, pager)
            entries = entries_result.objects if entries_result else []
            
            if not entries:
                break # We've reached the absolute end of the user's library

            # --- BULK FETCHING FOR CURRENT PAGE ---
            valid_entries = []
            for entry in entries:
                if entry.id not in seen_entry_ids:
                    valid_entries.append(entry)
                    seen_entry_ids.add(entry.id)
                    last_created_at = entry.createdAt
            
            if not valid_entries:
                # If everything on this page was a duplicate, move to next page
                page_index += 1
                continue

            entry_ids = [entry.id for entry in valid_entries]
            entry_ids_str = ",".join(entry_ids)
            
            # 1. Bulk Flavors
            f_filter = KalturaFlavorAssetFilter(entryIdIn=entry_ids_str)
            f_res = flavor_service.list(f_filter, batch_pager)
            page_flavors = f_res.objects if f_res else []
            
            # 2. Bulk Attachments
            a_filter = KalturaAttachmentAssetFilter(entryIdIn=entry_ids_str)
            a_res = attachment_service.list(a_filter, batch_pager)
            page_attachments = a_res.objects if a_res else []
            
            # 3. Bulk Captions
            c_filter = KalturaCaptionAssetFilter(entryIdIn=entry_ids_str)
            c_res = caption_service.list(c_filter, batch_pager)
            page_captions = c_res.objects if c_res else []
            
            # 4. Bulk Thumbs
            t_filter = KalturaThumbAssetFilter(entryIdIn=entry_ids_str)
            t_res = thumb_service.list(t_filter, batch_pager)
            page_thumbs = t_res.objects if t_res else []

            # 5. Bulk Category Entries (Captures private/entitled categories)
            ce_filter = KalturaCategoryEntryFilter()
            ce_filter.entryIdIn = entry_ids_str
            ce_res = category_entry_service.list(ce_filter, batch_pager)
            page_category_entries = ce_res.objects if ce_res else []

            # --- MAP BULK DATA TO DICTIONARIES ---
            flavors_map = defaultdict(list)
            for f in page_flavors: flavors_map[f.entryId].append(f)
                
            attachments_map = defaultdict(list)
            for a in page_attachments: attachments_map[a.entryId].append(a)
                
            captions_map = defaultdict(list)
            for c in page_captions: captions_map[c.entryId].append(c)
                
            thumbs_map = defaultdict(list)
            for t in page_thumbs: thumbs_map[t.entryId].append(t)

            category_entries_map = defaultdict(list)
            for ce in page_category_entries:
                category_entries_map[ce.entryId].append(str(ce.categoryId))

            # --- PROCESS ENTRIES AND STREAM TO CSV ---
            for entry in valid_entries:
                
                flavors = flavors_map.get(entry.id, [])
                flavor_ids = [f.flavorParamsId for f in flavors if f.flavorParamsId is not None]
                found_flavor_ids.update(flavor_ids)
                flavor_names_str = ";".join(map(str, flavor_ids)) if flavor_ids else "None"
                entry_size_kb = get_asset_size_in_kb(flavors)
                
                attachments = attachments_map.get(entry.id, [])
                a_count = len(attachments)
                a_kb = get_asset_size_in_kb(attachments)
                
                captions = captions_map.get(entry.id, [])
                c_count = len(captions)
                c_kb = get_asset_size_in_kb(captions)
                
                thumbs = thumbs_map.get(entry.id, [])
                t_count = len(thumbs)
                t_kb = get_asset_size_in_kb(thumbs)

                total_related_files_found += (a_count + c_count + t_count)

                # Children & Child Flavors (Targeted API fetch)
                child_filter = KalturaBaseEntryFilter()
                child_filter.parentEntryIdEqual = entry.id
                child_res = base_entry_service.list(child_filter, batch_pager)
                children = child_res.objects if child_res else []

                child_ids_str = "None"
                child_kb = 0
                child_flavors_str = "None"

                if children:
                    child_ids_str = ";".join([c.id for c in children])
                    total_children_found += len(children)

                    cf_filter = KalturaFlavorAssetFilter(entryIdIn=",".join([c.id for c in children]))
                    cf_res = flavor_service.list(cf_filter, batch_pager)
                    c_flavors = cf_res.objects if cf_res else []

                    if c_flavors:
                        child_kb = get_asset_size_in_kb(c_flavors)
                        c_flavor_ids = [f.flavorParamsId for f in c_flavors if f.flavorParamsId is not None]
                        found_flavor_ids.update(c_flavor_ids)
                        child_flavors_str = ";".join([str(fid) for fid in c_flavor_ids]) if c_flavor_ids else "None"

                # Categories (Merging base property with junction table mapping)
                cat_ids_str = getattr(entry, 'categoriesIds', "")
                extracted_cats = set()
                if cat_ids_str:
                    for c in cat_ids_str.split(','):
                        c_clean = c.strip()
                        if c_clean:
                            extracted_cats.add(c_clean)
                
                if entry.id in category_entries_map:
                    for c_id in category_entries_map[entry.id]:
                        extracted_cats.add(c_id)

                cat_ids_out = "None"
                if extracted_cats:
                    found_category_ids.update(extracted_cats)
                    cat_ids_out = ";".join(sorted(list(extracted_cats)))

                admin_tags_str = getattr(entry, 'adminTags', "")
                tags_str = getattr(entry, 'tags', "")

                grand_total_size_kb += (entry_size_kb + a_kb + c_kb + t_kb + child_kb)
                
                raw_type = getattr(entry, 'mediaType', 0)
                type_val = raw_type.value if hasattr(raw_type, 'value') else raw_type
                
                attach_count_out = a_count if a_count > 0 else "None"
                thumb_count_out = t_count if t_count > 0 else "None"

                row_dict = {
                    "entry id": entry.id,
                    "media type": MEDIA_TYPE_MAP.get(type_val, "Unknown"),
                    "media name": entry.name,
                    "created": format_date(entry.createdAt),
                    "edited": format_date(entry.updatedAt),
                    "viewed": format_date(getattr(entry, 'lastPlayedAt', 0)),
                    "flavors": flavor_names_str,
                    "size_kb": entry_size_kb,
                    "Children": child_ids_str,
                    "children_flavors": child_flavors_str,
                    "children_kb": child_kb,
                    "attachments": attach_count_out,
                    "attachments_kb": a_kb,
                    "captions": c_count,
                    "captions_kb": c_kb,
                    "thumbs": thumb_count_out,
                    "thumbs_kb": t_kb,
                    "Category Ids": cat_ids_out,
                    "co-editors": getattr(entry, 'entitledUsersEdit', "None") or "None",
                    "co-publishers": getattr(entry, 'entitledUsersPublish', "None") or "None",
                    "AdminTags": admin_tags_str if admin_tags_str else "None",
                    "Tags": tags_str if tags_str else "None"
                }

                # Write the row immediately to disk
                writer.writerow([row_dict[col] for col in headers])
                processed_count += 1
                
                elapsed_sec = int(time.time() - start_time)
                h = elapsed_sec // 3600
                m = (elapsed_sec % 3600) // 60
                s = elapsed_sec % 60
                formatted_elapsed = f"{h:02d}:{m:02d}:{s:02d}"
                
                status_msg = f"Elapsed Time: {formatted_elapsed} | Entries processed: {processed_count}/{total_entries} | Children: {total_children_found} | Related: {total_related_files_found}"
                print(f"\r{status_msg}\033[K", end="", flush=True)
            
            # Force write to disk per page so data is safe if script dies
            file.flush()

            # --- KALTURA 10K CAP BYPASS ---
            # Once we approach page 450 (9,000 entries), we slide our timeline forward 
            # and reset the page back to 1 to avoid the 10,000 API cap crash.
            if page_index >= 450:
                filter.createdAtGreaterThanOrEqual = last_created_at
                page_index = 1
            else:
                page_index += 1

        print() 

        # Final Total Time calculation
        total_time_seconds = int(time.time() - start_time)
        final_h = total_time_seconds // 3600
        final_m = (total_time_seconds % 3600) // 60
        final_s = total_time_seconds % 60
        formatted_processing_time = f"{final_h:02d}:{final_m:02d}:{final_s:02d}"

        # --- Flavor Legend Logic ---
        print("Fetching flavor definitions...")
        flavor_legend = []
        
        if 0 in found_flavor_ids:
            flavor_legend.append([0, "Source", "Original file uploaded to Kaltura"])
            found_flavor_ids.remove(0)

        if found_flavor_ids:
            flavor_id_list = list(found_flavor_ids)
            for id_chunk in chunks(flavor_id_list, 50):
                params_filter = KalturaFlavorParamsFilter(idIn=",".join(map(str, id_chunk)))
                try:
                    flavor_params_service = get_service(client, 'flavorParams')
                    fp_res = flavor_params_service.list(params_filter, batch_pager)
                    if fp_res and fp_res.objects:
                        for p in fp_res.objects:
                            flavor_legend.append([p.id, p.name, p.description if p.description else "No description available"])
                except Exception:
                    pass
                
        flavor_legend.sort(key=lambda x: int(x[0]) if str(x[0]).isdigit() else x[0])

        # --- Category Legend Logic ---
        print("Fetching category definitions...")
        parsed_categories = {}
        
        if found_category_ids:
            cat_id_list = list(found_category_ids)
            for id_chunk in chunks(cat_id_list, 50):
                try:
                    cat_filter = KalturaCategoryFilter(idIn=",".join(id_chunk))
                    cat_result = category_service.list(cat_filter, batch_pager)
                    
                    if cat_result and cat_result.objects:
                        for cat_obj in cat_result.objects:
                            f_ids = (cat_obj.fullIds or "").split('>')
                            f_names = (cat_obj.fullName or "").split('>')
                            
                            for i in range(len(f_ids)):
                                c_id = f_ids[i].strip()
                                c_name = f_names[i].strip() if i < len(f_names) else ""
                                if not c_id: 
                                    continue
                                
                                c_depth = i
                                c_parent = f_ids[i-1].strip() if i > 0 else 0
                                if c_depth == 0 and (c_parent == 0 or c_parent == "0"):
                                    c_parent = "Parent"
                                    
                                parsed_categories[c_id] = [c_id, c_name, c_depth, c_parent]
                except Exception:
                    pass

        category_legend = sorted(
            parsed_categories.values(), 
            key=lambda x: (int(x[2]), int(x[0]) if str(x[0]).isdigit() else x[0])
        )

        # --- Write Final Summaries and Legends to EOF ---
        grand_total_gb = round(grand_total_size_kb / (1024 ** 2), 4)

        writer.writerow([])
        writer.writerow(["--- USER SUMMARY ---", "", "", ""])
        writer.writerow(["User Id", "Total Media Entries", "Total Size (Gb)", "Total Processing Time"])
        writer.writerow([target_user_id, processed_count, f"{grand_total_gb:.4f}", formatted_processing_time])
        writer.writerow([])
        
        writer.writerow(["--- FLAVOR LEGEND ---", "", "", "", "--- CATEGORY LEGEND ---", "", "", ""])
        writer.writerow(["Flavor ID", "Flavor Name", "Description", "", "Category ID", "Category Name", "depth", "Parent ID"])
        
        max_legend_rows = max(len(flavor_legend), len(category_legend))
        for i in range(max_legend_rows):
            f_row = flavor_legend[i] if i < len(flavor_legend) else ["", "", ""]
            c_row = category_legend[i] if i < len(category_legend) else ["", "", "", ""]
            
            csv_row = [f_row[0], f_row[1], f_row[2], "", c_row[0], c_row[1], c_row[2], c_row[3]]
            writer.writerow(csv_row)

    print(f"\nExport complete! File saved in: {file_path}")

    if is_standalone:
        try:
            client.session.end()
            print("Kaltura session successfully terminated.")
        except Exception as cleanup_error:
            print(f"Warning: Failed to end Kaltura session: {cleanup_error}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\nAn error occurred: {e}")