"""
This script hides captions with a specific label (e.g., "English
(auto-generated)") from the Kaltura player. It allows admins to search for
entries using a tag, category ID, or a list of entry IDs.

The script performs the following steps:
1. Prompts the user to select a search method:
   - Tag
   - Category ID
   - Comma-delimited list of entry IDs
2. Retrieves matching media entries.
3. For each entry, fetches associated caption assets.
4. If a caption label matches CAPTION_LABEL, sets its displayOnPlayer field to
   False.
5. Prompts the user to confirm changes before applying them.
6. Logs the process and outputs results to a timestamped CSV file.
"""

import csv
import sys
from datetime import datetime
import pytz
from KalturaClient import KalturaClient
from KalturaClient.Base import KalturaConfiguration
from KalturaClient.Plugins.Core import (
    KalturaSessionType, KalturaFilterPager, KalturaMediaEntryFilter,
    KalturaAssetFilter
)
from KalturaClient.Plugins.Caption import KalturaCaptionAsset

# === GLOBAL CONFIGURATION ===
PARTNER_ID = ""
ADMIN_SECRET = ""
USER_ID = ""
PRIVILEGES = "all:*,disableentitlement"
EXPIRY = 86400  # Session expiration in seconds
CAPTION_LABEL = "English (auto-generated)"  # Customize for your environment

# === Kaltura client session ===
config = KalturaConfiguration(PARTNER_ID)
config.serviceUrl = "https://www.kaltura.com/"
client = KalturaClient(config)
client.setKs(client.session.start(
    ADMIN_SECRET, USER_ID, KalturaSessionType.ADMIN, PARTNER_ID, EXPIRY,
    PRIVILEGES
))


def get_all_caption_assets(entry_id):
    """
    Retrieves all caption assets for a specified media entry.
    Arguments:
        entry_id (str): The ID of the media entry for which to retrieve
        caption assets.
    Returns:
        list: A list of KalturaCaptionAsset objects associated with the entry.
    """
    try:
        caption_filter = KalturaAssetFilter()
        caption_filter.entryIdEqual = entry_id
        caption_result = client.caption.captionAsset.list(caption_filter)
        return caption_result.objects
    except Exception as e:
        print(f"Error retrieving captions for entry {entry_id}: {str(e)}")
        return []


def update_caption_visibility(caption_asset_id, display_on_player):
    """
    Updates the visibility of a caption asset on the player.
    Arguments:
        caption_asset_id (str): The ID of the caption asset to update.
        display_on_player (bool): A boolean indicating whether the caption
        should be visible on the player.
    Returns:
        KalturaCaptionAsset: The updated caption asset object.
    """
    try:
        caption_asset = KalturaCaptionAsset()
        caption_asset.displayOnPlayer = display_on_player
        updated_caption = (
            client.caption.captionAsset.update(caption_asset_id, caption_asset)
        )
        print(
            f"Updated caption asset {caption_asset_id} to displayOnPlayer = "
            f"{display_on_player}"
            )
        return updated_caption
    except Exception as e:
        print(f"Error updating caption asset {caption_asset_id}: {str(e)}")
        return None


def get_entries(method, identifier):
    """
    Retrieves media entries based on the selected method.

    Args:
        method (str): One of "tag", "category", or "entry_ids".
        identifier (str): Tag name, category ID, or comma-separated entry IDs.

    Returns:
        list: A list of KalturaMediaEntry objects matching the filter.
    """
    entry_filter = KalturaMediaEntryFilter()

    if method == "tag":
        entry_filter.tagsMultiLikeAnd = identifier
    elif method == "category":
        entry_filter.categoriesIdsMatchOr = identifier
    elif method == "entry_ids":
        entry_filter.idIn = identifier
    else:
        print("Invalid method.")
        return []

    pager = KalturaFilterPager()
    pager.pageSize = 50
    pager.pageIndex = 1

    entries = []
    while True:
        result = client.media.list(entry_filter, pager)
        if not result.objects:
            break
        entries.extend(result.objects)
        pager.pageIndex += 1

    print(f"Found {len(entries)} entries using method '{method}'")
    return entries


if __name__ == "__main__":
    print("\nHow would you like to identify the entries to modify?")
    print("[1] A tag")
    print("[2] A category ID")
    print("[3] A comma-delimited list of entry IDs")
    print("[X] Exit")

    method_map = {
        "1": ("tag", "Enter the tag name: "),
        "2": ("category", "Enter the category ID: "),
        "3": ("entry_ids", "Enter the entry IDs (comma-separated): ")
    }

    choice = input("Enter the number corresponding to your choice: ").strip()

    if choice.upper() == "X":
        print("Exiting script.")
        client.session.end()
        sys.exit(0)

    if choice not in method_map:
        print("Invalid choice.")
        client.session.end()
        sys.exit(1)

    method, prompt = method_map[choice]
    identifier = input(prompt).strip()

    if not identifier:
        print("You must provide a valid value.")
        client.session.end()
        sys.exit(1)

    entries = get_entries(method, identifier)

    affected_entries_count = 0

    # Determine the number of entries that would be affected
    for entry in entries:
        entry_id = entry.id
        caption_assets = get_all_caption_assets(entry_id)

        # Check if any caption has the label "English (auto-generated)"
        for caption in caption_assets:
            if caption.label == CAPTION_LABEL:
                affected_entries_count += 1
                # Count each entry only once if it has at least one matching
                # caption
                break

    print(f"Total entries that would be affected: {affected_entries_count}")

    # Prompt the user for confirmation
    if affected_entries_count > 0:
        proceed = input(
            f"Do you want to proceed with updating these "
            f"{affected_entries_count} entries? (yes/no): "
            ).strip().lower()
        if proceed != 'yes':
            print("Operation cancelled by the user.")
            client.session.end()
            sys.exit(0)

    # Get the current date and time in Pacific Time
    pacific = pytz.timezone('America/Los_Angeles')
    current_time = datetime.now(pacific)
    formatted_time = current_time.strftime('%Y-%m-%d-%H%M')

    # Create the CSV filename
    csv_filename = f"{formatted_time}_captionUpdates.csv"

    # Open CSV file to write the output
    with open(csv_filename, mode='w', newline='') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow([
            "Entry ID", "Entry Name", "Caption ID", "Caption Label",
            "Change Summary", "Timestamp"
            ])

        # Process each entry
        for entry in entries:
            entry_id = entry.id
            entry_name = entry.name
            print(f"Processing entry: {entry_id} - {entry_name}")

            # Get all caption assets for the current entry
            caption_assets = get_all_caption_assets(entry_id)
            entry_updated = False

            # Check all captions
            for caption in caption_assets:
                if caption.label == CAPTION_LABEL:
                    # Set displayOnPlayer to False
                    update_caption_visibility(caption.id, False)
                    timestamp = current_time.strftime('%Y-%m-%d %H:%M:%S')
                    writer.writerow([
                        entry_id, entry_name, caption.id, caption.label,
                        "displayOnPlayer set to False", timestamp
                        ])
                    entry_updated = True
                else:
                    # Write the caption info without updating
                    writer.writerow([
                        entry_id, entry_name, caption.id, caption.label,
                        "No Change", ""
                        ])

    print(
        f"Script completed. Changes have been applied and logged to "
        f"'{csv_filename}'."
        )

    # End the session
    client.session.end()
