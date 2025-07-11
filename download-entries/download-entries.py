'''
Downloads source files from Kaltura media entries into a subfolder named
"kaltura_downloads", based on one of four search criteria: a tag, category ID,
comma-delimited list of entry IDs, or owner's user ID.

Entries that are not valid downloadable media (e.g., playlists)
are automatically skipped. Filenames are optionally cleaned to remove
"(Source)" and trailing underscores or dashes.

You can change the name of the download folder and filename cleaning behavior
using the global variables defined at the top of the script.

Be sure to provide your partner ID and admin secret in the global variables
before running the script.
'''

import os
import time
import requests
from urllib.parse import urlparse
from KalturaClient import KalturaClient, KalturaConfiguration
from KalturaClient.Plugins.Core import (
    KalturaBaseEntryFilter, KalturaFilterPager, KalturaSessionType,
    KalturaFlavorAssetFilter
)
from KalturaClient.exceptions import KalturaException
import re

# ---- CONFIGURABLE VARIABLES ----
# PARTNER_ID = "" DO NOT USE--script will request input
# ADMIN_SECRET = "" DO NOT USE--script will request input
DOWNLOAD_FOLDER = "kaltura_downloads"
RETRY_ATTEMPTS = 3
REMOVE_SUFFIX = True
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
    pager = KalturaFilterPager()
    pager.pageSize = 100
    pager.pageIndex = 1

    if method == "tag":
        entry_filter.tagsLike = identifier
    elif method == "category":
        entry_filter.categoryAncestorIdIn = identifier
    elif method == "entry_ids":
        entry_filter.idIn = identifier
    elif method == "owner_id":
        entry_filter.userIdEqual = identifier
    else:
        print("Invalid method selection.")
        return []

    try:
        while True:
            result = client.baseEntry.list(entry_filter, pager)
            if not result.objects:
                break
            entries.extend(result.objects)
            pager.pageIndex += 1
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

    # 🔍 Clean up filename if REMOVE_SUFFIX is enabled
    if REMOVE_SUFFIX:
        base, ext = os.path.splitext(filename)

        # Remove (Source), with or without surrounding spaces/underscores
        base = re.sub(r"[\s_]*\(Source\)[\s_]*", "", base)

        # Clean up trailing underscore, space, or dash
        base = re.sub(r"[_\-\s]+$", "", base)

        filename = f"{base}{ext}"

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


def process_entry(client, entry, index):
    url = get_download_url(client, entry)
    if url:
        filename = get_file_name(url)
        print(f"{index}. ✅ Downloaded: {filename}")
        download_file(url, filename)
    else:
        print(
            f"{index}. ⚠️ Skipping {entry.id} ({entry.name}): No valid "
            f"download URL found."
            )

    children = get_child_entries(client, entry.id)
    for child in children:
        child_url = get_download_url(client, child)
        if child_url:
            child_filename = get_file_name(child_url)
            print(f"{index}. ✅ Downloaded child: {child_filename}")
            download_file(child_url, child_filename)
        else:
            print(
                f"{index}. ⚠️ Skipping child entry {child.id} ({child.name}): "
                f"No valid download URL found."
                )


def main():
    partner_id = input("Enter your Partner ID: ").strip()
    admin_secret = input("Enter your Admin Secret: ").strip()

    client = get_kaltura_client(partner_id, admin_secret)

    print("\nSelect download method:")
    print("[1] A tag")
    print("[2] A category ID")
    print("[3] A comma-delimited list of entry IDs")
    print("[4] An owner's user ID")

    method_choice = input(
        "Enter the number corresponding to your choice: "
        ).strip()
    method_mapping = {
        "1": "tag", "2": "category", "3": "entry_ids", "4": "owner_id"
        }

    if method_choice not in method_mapping:
        print("Error: Invalid choice. Please enter 1, 2, 3, or 4.")
        return

    method = method_mapping[method_choice]
    identifier = input("Enter the identifier: ").strip()
    if not identifier:
        print("Error: You must provide a valid identifier.")
        return

    entries = get_entries(client, method, identifier)
    # Filter out non-media entries early (e.g., playlists)
    entries = [e for e in entries if hasattr(e, "mediaType")]

    if not entries:
        print("No entries found. Exiting.")
        return

    print(f"Found {len(entries)} entries. Starting downloads...")

    # Process one entry at a time (no threading)
    for idx, entry in enumerate(entries, start=1):
        process_entry(client, entry, idx)

    print("✅ All downloads complete!")


if __name__ == "__main__":
    main()
