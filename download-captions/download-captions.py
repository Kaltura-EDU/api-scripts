'''
This script downloads all caption files for entries based on user input of
either a tag, a category ID, or a comma-delimited list of entry IDs.

Be sure to insert your partner ID and admin secret to the variable values
below.
'''

import os
import urllib.request
from KalturaClient import KalturaClient, KalturaConfiguration
from KalturaClient.Plugins.Core import (
    KalturaBaseEntryFilter, KalturaFilterPager, KalturaSessionType
)
from KalturaClient.Plugins.Caption import KalturaCaptionAssetFilter
import re
from datetime import datetime, timezone
import ssl

# Configuration Variables
# PARTNER_ID = "" DO NOT USE--script will request input
# ADMIN_SECRET = "" DO NOT USE--script will request input
DOWNLOAD_FOLDER = "captions_download"


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


def sanitize_filename(name, max_length=100):
    name = re.sub(r'[^a-zA-Z0-9_-]', '_', name)  # Replace invalid characters
    return name[:max_length]  # Truncate if necessary


def get_entries(client, method, identifier):
    entries = []
    filter = KalturaBaseEntryFilter()
    if method == "tag":
        filter.tagsLike = identifier
    elif method == "category":
        filter.categoriesIdsMatchOr = identifier
    elif method == "entry_ids":
        filter.idIn = identifier
    else:
        print("Invalid method selection.")
        return []

    pager = KalturaFilterPager()
    pager.pageSize = 500  # Max allowed
    pager.pageIndex = 1

    try:
        while True:
            result = client.baseEntry.list(filter, pager)
            if not result.objects:
                break
            entries.extend(result.objects)
            if len(entries) >= result.totalCount:
                break
            pager.pageIndex += 1
    except Exception as e:
        print(f"Error retrieving entries: {e}")

    return entries


def get_captions(client, entry_id):
    caption_filter = KalturaCaptionAssetFilter()
    caption_filter.entryIdEqual = entry_id
    pager = KalturaFilterPager()

    try:
        result = client.caption.captionAsset.list(caption_filter, pager)
        return result.objects if result.objects else []
    except Exception as e:
        print(f"Error retrieving captions for entry {entry_id}: {e}")
        return []


def download_captions(client, captions, entry, counter):
    if not os.path.exists(DOWNLOAD_FOLDER):
        os.makedirs(DOWNLOAD_FOLDER)

    entry_date = datetime.fromtimestamp(entry.createdAt, tz=timezone.utc).strftime('%Y-%m-%d')
    entry_id = entry.id
    entry_title = sanitize_filename(entry.name)

    for caption in captions:
        try:
            caption_label = sanitize_filename(caption.label)
            caption_url = client.caption.captionAsset.getUrl(caption.id, 0)
            file_path = os.path.join(DOWNLOAD_FOLDER, f"{entry_date}_{entry_id}_{entry_title}_{caption_label}.srt")

            try:
                with urllib.request.urlopen(caption_url) as response:
                    with open(file_path, "wb") as file:
                        file.write(response.read())
                    print(f"{counter[0]}. Downloaded: {file_path}")
                    counter[0] += 1
            except ssl.SSLError as ssl_err:
                print(f"⚠️ SSL error downloading {caption.label} for entry {entry.id}: {ssl_err}")
                print("If you're on macOS, try running Install Certificates.command from your Python folder.")

        except Exception as e:
            print(f"Error downloading caption {caption.id}: {e}")


def main():
    partner_id = input("Enter your Partner ID: ").strip()
    admin_secret = input("Enter your Admin Secret: ").strip()

    client = get_kaltura_client(partner_id, admin_secret)

    print("\nHow do you want to find entries?")
    print("[1] A tag")
    print("[2] A category ID")
    print("[3] A comma-delimited list of entry IDs")

    method_mapping = {
        "1": ("tag", "Enter the tag name: "),
        "2": ("category", "Enter the category ID: "),
        "3": ("entry_ids", "Enter the entry IDs (comma-separated): ")
    }

    method_choice = input(
        "Enter the number corresponding to your choice: "
        ).strip()
    if method_choice not in method_mapping:
        print("Error: Invalid choice.")
        return

    method, prompt_text = method_mapping[method_choice]
    identifier = input(prompt_text).strip()
    if not identifier:
        print("Error: You must provide a valid identifier.")
        return

    entries = get_entries(client, method, identifier)
    print(f"{len(entries)} entries found.")
    counter = [1]  # Mutable counter to track downloaded captions
    if not entries:
        print("No entries found. Exiting.")
        return

    for entry in entries:
        captions = get_captions(client, entry.id)
        if captions:
            download_captions(client, captions, entry, counter)
        else:
            print(f"No captions found for entry {entry.id}")

    print("Caption download complete.")


if __name__ == "__main__":
    main()
