'''
Downloads source files from Kaltura media entries into a subfolder named
"kaltura_downloads", based on one of four search criteria: a tag, category ID,
comma-delimited list of entry IDs, or owner's user ID.

Entries that are not valid downloadable media (e.g., playlists)
are automatically skipped. Filenames are optionally cleaned to remove
"(Source)" and trailing underscores or dashes. If multiple entries share the
same filename, the entry ID is appended to keep filenames unique.

After each run, a timestamped CSV report is saved to the download folder
listing every entry processed, with metadata fields including entry ID, name,
owner, creation date, duration, tags, categories, download status, and the
actual filename written to disk.

You can change the name of the download folder and filename cleaning behavior
using the global variables defined at the top of the script.

Be sure to provide your partner ID and admin secret in the global variables
before running the script.
'''

import csv
import datetime
import getpass
import os
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from urllib.parse import urlparse
from KalturaClient import KalturaClient, KalturaConfiguration
from KalturaClient.Plugins.Core import (
    KalturaBaseEntryFilter, KalturaCategoryFilter, KalturaFilterPager,
    KalturaSessionType, KalturaFlavorAssetFilter
)
from KalturaClient.exceptions import KalturaException, KalturaClientException
import re

# ---- CONFIGURABLE VARIABLES ----
# PARTNER_ID = "" DO NOT USE--script will request input
# ADMIN_SECRET = "" DO NOT USE--script will request input
DOWNLOAD_FOLDER = "kaltura_downloads"
RETRY_ATTEMPTS = 3
REMOVE_SUFFIX = True
MAX_WORKERS = 5
# -- END CONFIGURABLE VARIABLES --

CSV_HEADERS = [
    "Entry ID", "Name", "Description", "Owner", "Creator ID",
    "Created At", "Updated At", "Duration", "Media Type",
    "Tags", "Categories", "Download Status", "Downloaded Filename",
]

MEDIA_TYPE_MAP = {1: "Video", 2: "Image", 5: "Audio"}


class ThreadSafeCSVWriter:
    """Wraps csv.writer so concurrent threads can call writerow without
    corrupting the file or interleaving rows."""
    def __init__(self, writer, file):
        self._writer = writer
        self._file = file
        self._lock = threading.Lock()

    def writerow(self, row):
        with self._lock:
            self._writer.writerow(row)
            self._file.flush()


def _safe_folder_name(name):
    """Strip characters that are invalid in folder names across platforms."""
    return re.sub(r'[\\/:*?"<>|\s]+', '_', name).strip('_')


def _fmt_ts(ts):
    if not ts:
        return ""
    return datetime.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


def _fmt_duration(seconds):
    if seconds is None:
        return ""
    h, rem = divmod(int(seconds), 3600)
    m, s = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def _media_type_label(entry):
    val = getattr(entry.mediaType, "value", getattr(entry, "mediaType", None))
    return MEDIA_TYPE_MAP.get(val, str(val) if val is not None else "")


def write_csv_row(writer, entry, status, filename=""):
    writer.writerow([
        entry.id,
        getattr(entry, "name", "") or "",
        getattr(entry, "description", "") or "",
        getattr(entry, "userId", "") or "",
        getattr(entry, "creatorId", "") or "",
        _fmt_ts(getattr(entry, "createdAt", None)),
        _fmt_ts(getattr(entry, "updatedAt", None)),
        _fmt_duration(getattr(entry, "duration", None)),
        _media_type_label(entry),
        getattr(entry, "tags", "") or "",
        getattr(entry, "categories", "") or "",
        status,
        filename or "",
    ])


def get_kaltura_client(partner_id, admin_secret):
    config = KalturaConfiguration(partner_id)
    config.serviceUrl = "https://www.kaltura.com/"
    client = KalturaClient(config)
    ks = client.session.start(
        admin_secret, "admin", KalturaSessionType.ADMIN, partner_id,
        privileges="all:*,disableentitlement"
    )
    client.setKs(ks)
    return client


def get_entry_details(client, entry_id):
    """Retrieve entry details with retry logic in case of API failures."""
    for attempt in range(RETRY_ATTEMPTS):
        try:
            return client.baseEntry.get(entry_id)
        except KalturaException as e:
            print(
                f"⚠️ Attempt {attempt+1}: Failed to retrieve entry "
                f"{entry_id}. Error: {e}"
                )
            time.sleep(2 ** attempt)  # Exponential backoff
    print(f"❌ Giving up on entry {entry_id} after {RETRY_ATTEMPTS} attempts.")
    return None


def resolve_category_names(client, names_str):
    """Resolve one or more comma-delimited category names to a list of
    (name, id_str) tuples. Uses freeText search (same as KMC search) to find
    candidates, then filters client-side for exact name match. For names with
    multiple matches, prompts the user to choose."""
    names = [n.strip() for n in names_str.split(",") if n.strip()]
    resolved = []

    for name in names:
        cat_filter = KalturaCategoryFilter()
        cat_filter.freeText = name
        pager = KalturaFilterPager()
        pager.pageSize = 500
        pager.pageIndex = 1

        candidates = []
        while True:
            result = client.category.list(cat_filter, pager)
            if not result.objects:
                break
            candidates.extend(result.objects)
            if len(result.objects) < pager.pageSize:
                break
            pager.pageIndex += 1

        matches = [c for c in candidates if c.name == name]

        if not matches:
            print(f"Error: No category found with name '{name}'.")
            sys.exit(1)

        if len(matches) == 1:
            cat = matches[0]
            print(f"Resolved '{name}' → {cat.fullName} (ID: {cat.id})")
            resolved.append((name, str(cat.id)))
        else:
            print(f"\nMultiple categories found with name '{name}':")
            for i, cat in enumerate(matches, start=1):
                print(f"  [{i}] ID: {cat.id}  —  Full path: {cat.fullName}")
            while True:
                choice = input(
                    f"Enter the number of the category to use for '{name}': "
                ).strip()
                if choice.isdigit() and 1 <= int(choice) <= len(matches):
                    cat = matches[int(choice) - 1]
                    print(f"Using: {cat.fullName} (ID: {cat.id})")
                    resolved.append((name, str(cat.id)))
                    break
                print(f"Please enter a number between 1 and {len(matches)}.")

    return resolved


def get_entries(client, method, identifier):
    entries = []
    entry_filter = KalturaBaseEntryFilter()
    pager = KalturaFilterPager()
    pager.pageSize = 100
    pager.pageIndex = 1

    if method == "tag":
        entry_filter.tagsMultiLikeOr = identifier
    elif method == "category":
        entry_filter.categoryAncestorIdIn = identifier
    elif method == "entry_ids":
        entry_filter.idIn = identifier
    elif method == "owner_id":
        entry_filter.userIdIn = identifier
    else:
        print("Invalid method selection.")
        return []

    while True:
        result = None
        for attempt in range(RETRY_ATTEMPTS):
            try:
                result = client.baseEntry.list(entry_filter, pager)
                break
            except (KalturaException, KalturaClientException) as e:
                if attempt < RETRY_ATTEMPTS - 1:
                    print(
                        f"⚠️ Attempt {attempt + 1}: Error fetching entries "
                        f"(page {pager.pageIndex}): {e}. Retrying..."
                    )
                    time.sleep(2 ** attempt)
                else:
                    print(
                        f"❌ Failed to fetch entries (page {pager.pageIndex}) "
                        f"after {RETRY_ATTEMPTS} attempts: {e}"
                    )
        if result is None or not result.objects:
            break
        entries.extend(result.objects)
        pager.pageIndex += 1

    return entries


def get_child_entries(client, parent_entry_id):
    child_filter = KalturaBaseEntryFilter()
    child_filter.parentEntryIdEqual = parent_entry_id
    pager = KalturaFilterPager()
    for attempt in range(RETRY_ATTEMPTS):
        try:
            children = client.baseEntry.list(child_filter, pager).objects
            return children if children else []
        except (KalturaException, KalturaClientException) as e:
            print(
                f"⚠️ Attempt {attempt+1}: Error retrieving child entries for "
                f"{parent_entry_id}: {e}"
            )
            if attempt < RETRY_ATTEMPTS - 1:
                time.sleep(2 ** attempt)
    print(
        f"❌ Giving up on child entries for {parent_entry_id} after "
        f"{RETRY_ATTEMPTS} attempts."
    )
    return []


def get_flavor_download_url(client, entry):
    # Retrieve the original flavor asset download URL for a given entry.
    flavor_filter = KalturaFlavorAssetFilter()
    flavor_filter.entryIdEqual = entry.id
    pager = KalturaFilterPager()
    try:
        flavors = client.flavorAsset.list(flavor_filter, pager).objects
        original_flavor = next(
            (f for f in flavors if getattr(f, 'isOriginal', False)), None
            )
        if original_flavor:
            return client.flavorAsset.getUrl(original_flavor.id)
    except KalturaException as e:
        print(
            f"⚠️ Warning: Could not retrieve flavor asset for entry "
            f"{entry.id}. Error: {e}"
              )
    return None


def get_download_url(client, entry):
    # Skip entries that are not media entries (e.g., playlists, documents)
    if not hasattr(entry, "mediaType"):
        return None

    entry_details = get_entry_details(client, entry.id)
    if not entry_details or not hasattr(entry_details, "mediaType"):
        return None

    media_type = getattr(
        entry_details.mediaType, 'value', entry_details.mediaType
        )
    if media_type == 2:  # Image entries
        return entry_details.downloadUrl

    return get_flavor_download_url(client, entry)


def get_file_name(url, entry_id, download_folder):
    """Extract the filename from the URL or HTTP response headers.
    Returns None if the file already exists in the download folder.
    Uses entry_id to disambiguate entries that share the same name."""
    filename = None

    try:
        response = requests.head(url, allow_redirects=True)
        if "Content-Disposition" in response.headers:
            content_disp = response.headers["Content-Disposition"]
            if "filename=" in content_disp:
                filename = content_disp.split("filename=")[1].strip('"')
    except requests.RequestException as e:
        print(f"⚠️ Warning: Could not determine filename from headers: {e}")

    if not filename:
        filename = os.path.basename(urlparse(url).path)

    if REMOVE_SUFFIX:
        base, ext = os.path.splitext(filename)
        base = re.sub(r"[\s_]*\(Source\)[\s_]*", "", base)
        base = re.sub(r"[_\-\s]+$", "", base)
        filename = f"{base}{ext}"

    if os.path.exists(os.path.join(download_folder, filename)):
        # Collision: try an entry-ID-qualified name to distinguish same-titled entries
        base, ext = os.path.splitext(filename)
        filename_with_id = f"{base}_{entry_id}{ext}"
        if os.path.exists(os.path.join(download_folder, filename_with_id)):
            return None  # Already downloaded (entry-ID version exists)
        return filename_with_id

    return filename


def download_file(url, filename, download_folder):
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()

        os.makedirs(download_folder, exist_ok=True)
        file_path = os.path.join(download_folder, filename)

        with open(file_path, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
    except requests.RequestException as e:
        print(f"❌ Failed to download {filename}: {e}")


def worker(queue, client):
    while queue:
        entry = queue.pop(0)
        url = get_download_url(client, entry)
        if url:
            filename = get_file_name(url, entry.id)
            download_file(url, filename)
        else:
            print(
                f"⚠️ Skipping {entry.id} ({entry.name}): No valid download "
                f"URL found."
                )

        children = get_child_entries(client, entry.id)
        for child in children:
            child_url = get_download_url(client, child)
            if child_url:
                child_filename = get_file_name(child_url, child.id)
                download_file(child_url, child_filename)
            else:
                print(
                    f"⚠️ Skipping child entry {child.id} ({child.name}): No "
                    f"valid download URL found."
                    )

        time.sleep(1)  # Prevent overwhelming the server


def process_entry(client, entry, index, csv_writer, download_folder):
    url = get_download_url(client, entry)
    if url:
        filename = get_file_name(url, entry.id, download_folder)
        if filename is None:
            print(
                f"{index}. ⏭️ Skipping {entry.id} ({entry.name}): "
                f"already downloaded."
                )
            write_csv_row(csv_writer, entry, "Already Downloaded")
        else:
            download_file(url, filename, download_folder)
            print(f"{index}. ✅ Downloaded: {filename}")
            write_csv_row(csv_writer, entry, "Downloaded", filename)
    else:
        print(
            f"{index}. ⚠️ Skipping {entry.id} ({entry.name}): No valid "
            f"download URL found."
            )
        write_csv_row(csv_writer, entry, "Skipped (no URL)")

    children = get_child_entries(client, entry.id)
    for child in children:
        child_url = get_download_url(client, child)
        if child_url:
            child_filename = get_file_name(child_url, child.id, download_folder)
            if child_filename is None:
                print(f"  ↳ ⏭️ Skipping child {child.id} ({child.name}): already downloaded.")
                write_csv_row(csv_writer, child, "Already Downloaded")
            else:
                download_file(child_url, child_filename, download_folder)
                print(f"  ↳ ✅ Downloaded child: {child_filename}")
                write_csv_row(csv_writer, child, "Downloaded", child_filename)
        else:
            print(f"  ↳ ⚠️ Skipping child {child.id} ({child.name}): No valid download URL found.")
            write_csv_row(csv_writer, child, "Skipped (no URL)")


def _process_entry_with_retry(client, entry, idx, csv_writer, download_folder):
    """Returns True on success, False if all retry attempts are exhausted."""
    for attempt in range(RETRY_ATTEMPTS):
        try:
            process_entry(client, entry, idx, csv_writer, download_folder)
            return True
        except Exception as e:
            if attempt < RETRY_ATTEMPTS - 1:
                print(
                    f"⚠️ Attempt {attempt + 1}/{RETRY_ATTEMPTS}: Error on "
                    f"{entry.id}: {e}. Retrying..."
                )
                time.sleep(2 ** attempt)
            else:
                print(
                    f"❌ Giving up on {entry.id} after {RETRY_ATTEMPTS} "
                    f"attempts: {e}"
                )
    return False


def main():
    partner_id = input("Enter your Partner ID: ").strip()
    admin_secret = getpass.getpass("Enter your Admin Secret: ").strip()

    client = get_kaltura_client(partner_id, admin_secret)

    method_mapping = {
        "1": "tag", "2": "category_id", "3": "category_name",
        "4": "entry_ids", "5": "owner_id",
    }
    prompts = {
        "tag": "Enter tag(s): ",
        "category_id": "Enter category ID(s): ",
        "category_name": "Enter category name(s): ",
        "entry_ids": "Enter entry ID(s): ",
        "owner_id": "Enter owner user ID(s): ",
    }

    while True:
        print("\nSelect download method:")
        print("  (Each field accepts comma-delimited values; multiple values are treated as OR.)")
        print("[1] Tag(s)")
        print("[2] Category ID(s)")
        print("[3] Category name(s)")
        print("[4] Entry ID(s)")
        print("[5] Owner user ID(s)")

        method_choice = input("Enter the number corresponding to your choice: ").strip()
        if method_choice not in method_mapping:
            print("Error: Invalid choice. Please enter 1, 2, 3, 4, or 5.")
            continue

        method = method_mapping[method_choice]
        identifier = input(prompts[method]).strip()
        if not identifier:
            print("Error: You must provide a valid identifier.")
            continue

        # Build a list of (label, internal_method, term_identifier) per search term
        if method == "category_name":
            name_id_pairs = resolve_category_names(client, identifier)
            terms = [(name, "category", cat_id) for name, cat_id in name_id_pairs]
        elif method == "category_id":
            ids = [t.strip() for t in identifier.split(",") if t.strip()]
            terms = [(cat_id, "category", cat_id) for cat_id in ids]
        else:
            vals = [t.strip() for t in identifier.split(",") if t.strip()]
            terms = [(val, method, val) for val in vals]

        # Offer subdirectory option for applicable methods with multiple terms
        use_subdirs = False
        if method in ("tag", "category_id", "category_name", "owner_id") and len(terms) > 1:
            resp = input(
                "Download into subdirectories named after each search term? [y/N] "
            ).strip().lower()
            use_subdirs = resp in ("y", "yes")

        # Build download batches: (label, folder, internal_method, identifier)
        if use_subdirs:
            batches = [
                (label, os.path.join(DOWNLOAD_FOLDER, _safe_folder_name(label)), m, ident)
                for label, m, ident in terms
            ]
        else:
            internal_method = terms[0][1]
            all_idents = ",".join(ident for _, _, ident in terms)
            batches = [(None, DOWNLOAD_FOLDER, internal_method, all_idents)]

        # Fetch entries for all batches upfront
        batch_results = []  # [(label, folder, entries)]
        for label, folder, batch_method, batch_ident in batches:
            if label:
                print(f"Fetching entries for '{label}'...")
            entries = get_entries(client, batch_method, batch_ident)
            entries = [e for e in entries if hasattr(e, "mediaType")]
            batch_results.append((label, folder, entries))

        total_entries = sum(len(e) for _, _, e in batch_results)
        if total_entries == 0:
            print("No entries found.")
        else:
            if not use_subdirs:
                print(f"Found {total_entries} entries. Starting downloads...")

            caffeinate = None
            if sys.platform == "darwin":
                try:
                    caffeinate = subprocess.Popen(["caffeinate", "-i"])
                    print("☕ Keeping your Mac awake for the duration of the download.")
                except FileNotFoundError:
                    pass

            total_failures = 0
            try:
                for label, folder, entries in batch_results:
                    if not entries:
                        print(f"No entries found for '{label}'. Skipping.")
                        continue

                    if use_subdirs:
                        print(f"\nFound {len(entries)} entries for '{label}'. Downloading to {folder}/...")

                    os.makedirs(folder, exist_ok=True)
                    timestamp = datetime.datetime.now().strftime("%Y-%m-%d-%H%M")
                    csv_path = os.path.join(folder, f"{timestamp}_download_report.csv")

                    with open(csv_path, "w", newline="", encoding="utf-8") as csv_file:
                        writer = ThreadSafeCSVWriter(csv.writer(csv_file), csv_file)
                        writer.writerow(CSV_HEADERS)
                        failures = 0
                        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                            futures = {
                                executor.submit(
                                    _process_entry_with_retry,
                                    client, entry, idx, writer, folder
                                ): entry
                                for idx, entry in enumerate(entries, start=1)
                            }
                            for future in as_completed(futures):
                                try:
                                    if future.result() is False:
                                        failures += 1
                                except Exception as e:
                                    entry = futures[future]
                                    print(f"❌ Unexpected error on {entry.id}: {e}")
                                    failures += 1

                    total_failures += failures
                    if failures:
                        print(f"⚠️ Report saved to: {csv_path} ({failures} failure(s))")
                    else:
                        print(f"✅ Report saved to: {csv_path}")
            finally:
                if caffeinate:
                    caffeinate.terminate()

            if total_failures:
                print(f"⚠️ Downloads complete with {total_failures} failure(s). See above for details.")
            else:
                print("✅ All downloads complete!")

        again = input("\nWould you like to download more entries? [y/N] ").strip().lower()
        if again not in ("y", "yes"):
            break


if __name__ == "__main__":
    main()
