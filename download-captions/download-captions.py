'''
This script downloads all caption files for entries based on user input of
either a tag, a category ID, or a comma-delimited list of entry IDs.
'''

import os
import urllib.request
from KalturaClient import KalturaClient, KalturaConfiguration
from KalturaClient.Plugins.Core import (
    KalturaBaseEntryFilter, KalturaFilterPager, KalturaSessionType
)
from KalturaClient.Plugins.Caption import KalturaCaptionAssetFilter
import re
from datetime import datetime

# Configuration Variables
PARTNER_ID = ""
ADMIN_SECRET = ""
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
    try:
        result = client.baseEntry.list(filter, pager)
        if result.objects:
            entries.extend(result.objects)
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


def download_captions(client, captions, entry):
    if not os.path.exists(DOWNLOAD_FOLDER):
        os.makedirs(DOWNLOAD_FOLDER)

    entry_date = datetime.utcfromtimestamp(entry.createdAt).strftime('%Y-%m-%d')
    entry_id = entry.id
    entry_title = sanitize_filename(entry.name)

    for caption in captions:
        try:
            caption_label = sanitize_filename(caption.label)
            caption_url = client.caption.captionAsset.getUrl(caption.id, 0)
            file_path = os.path.join(DOWNLOAD_FOLDER, f"{entry_date}_{entry_id}_{entry_title}_{caption_label}.srt")

            with urllib.request.urlopen(caption_url) as response:
                with open(file_path, "wb") as file:
                    file.write(response.read())
                    print(f"Downloaded: {file_path}")
        except Exception as e:
            print(f"Error downloading caption {caption.id}: {e}")


def main():
    client = get_kaltura_client(PARTNER_ID, ADMIN_SECRET)

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
    if not entries:
        print("No entries found. Exiting.")
        return

    for entry in entries:
        captions = get_captions(client, entry.id)
        if captions:
            download_captions(client, captions, entry)
        else:
            print(f"No captions found for entry {entry.id}")

    print("Caption download complete.")


if __name__ == "__main__":
    main()
