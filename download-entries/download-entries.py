'''
Downloads source files from entries into a subfolder "kaltura_downloads" based
on a tag, category ID, or comma-delimited list of entry IDs. You can change
the name of the folder if desired with one of the global variables.

Be sure to provide your partner ID and admin secret below before attempting to
run the script.
'''

import os
import time
import requests
import threading
from urllib.parse import urlparse
from KalturaClient import KalturaClient, KalturaConfiguration
from KalturaClient.Plugins.Core import (
    KalturaBaseEntryFilter, KalturaFilterPager, KalturaSessionType,
    KalturaFlavorAssetFilter
)
from KalturaClient.exceptions import KalturaException

# ---- CONFIGURABLE VARIABLES ----
PARTNER_ID = ""
ADMIN_SECRET = ""
DOWNLOAD_FOLDER = "kaltura_downloads"
MAX_SIMULTANEOUS_DOWNLOADS = 6  # Adjust based on observed Kaltura limits
RETRY_ATTEMPTS = 3
# -- END CONFIGURABLE VARIABLES --


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


def get_entries(client, method, identifier):
    entries = []
    entry_filter = KalturaBaseEntryFilter()
    if method == "tag":
        entry_filter.tagsLike = identifier
    elif method == "category":
        entry_filter.categoriesIdsMatchOr = identifier
    elif method == "entry_ids":
        entry_filter.idIn = identifier
    else:
        print("Invalid method selection.")
        return []

    pager = KalturaFilterPager()
    try:
        result = client.baseEntry.list(entry_filter, pager)
        if result.objects:
            entries.extend(result.objects)
    except KalturaException as e:
        print(f"Error retrieving entries: {e}")
    return entries


def get_child_entries(client, parent_entry_id):
    child_filter = KalturaBaseEntryFilter()
    child_filter.parentEntryIdEqual = parent_entry_id
    pager = KalturaFilterPager()
    try:
        children = client.baseEntry.list(child_filter, pager).objects
        return children if children else []
    except KalturaException as e:
        print(f"Error retrieving child entries for {parent_entry_id}: {e}")
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
    entry_details = get_entry_details(client, entry.id)
    if not entry_details:
        return None

    media_type = getattr(
        entry_details.mediaType, 'value', entry_details.mediaType
        )
    if media_type == 2:  # Image entries
        return entry_details.downloadUrl

    return get_flavor_download_url(client, entry)


def get_file_name(url, counter=0):
    """Extract the filename directly from the URL or HTTP response headers,
    avoiding overwrites."""
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

    file_path = os.path.join(DOWNLOAD_FOLDER, filename)
    while os.path.exists(file_path):
        counter += 1
        base, ext = os.path.splitext(filename)
        filename = f"{base}_{counter}{ext}"
        file_path = os.path.join(DOWNLOAD_FOLDER, filename)

    return filename


def download_file(url, filename):
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()

        os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)
        file_path = os.path.join(DOWNLOAD_FOLDER, filename)

        with open(file_path, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        print(f"✅ Downloaded: {filename}")
    except requests.RequestException as e:
        print(f"❌ Failed to download {filename}: {e}")


def worker(queue, client):
    while queue:
        entry = queue.pop(0)
        url = get_download_url(client, entry)
        if url:
            filename = get_file_name(url)
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
                child_filename = get_file_name(child_url)
                download_file(child_url, child_filename)
            else:
                print(
                    f"⚠️ Skipping child entry {child.id} ({child.name}): No "
                    f"valid download URL found."
                    )

        time.sleep(1)  # Prevent overwhelming the server


def main():
    client = get_kaltura_client(PARTNER_ID, ADMIN_SECRET)

    print("\nSelect download method:")
    print("[1] A tag")
    print("[2] A category ID")
    print("[3] A comma-delimited list of entry IDs")

    method_choice = input(
        "Enter the number corresponding to your choice: "
        ).strip()
    method_mapping = {"1": "tag", "2": "category", "3": "entry_ids"}

    if method_choice not in method_mapping:
        print("Error: Invalid choice. Please enter 1, 2, or 3.")
        return

    method = method_mapping[method_choice]
    identifier = input("Enter the identifier: ").strip()
    if not identifier:
        print("Error: You must provide a valid identifier.")
        return

    entries = get_entries(client, method, identifier)
    if not entries:
        print("No entries found. Exiting.")
        return

    print(f"Found {len(entries)} entries. Starting downloads...")

    queue = entries[:]
    threads = []

    for _ in range(min(MAX_SIMULTANEOUS_DOWNLOADS, len(entries))):
        thread = threading.Thread(target=worker, args=(queue, client))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

    print("✅ All downloads complete!")


if __name__ == "__main__":
    main()
