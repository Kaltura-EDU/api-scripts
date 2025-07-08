"""
This script batch-renames Kaltura media entries based on entry IDs, tags, or
category memberships.

Usage:
- At a command prompt, type "python3 rename-entries.py". 

Required modules: KalturaApiClient, lxml
"""

import sys
import csv
from datetime import datetime
from KalturaClient import KalturaClient
from KalturaClient.Base import KalturaConfiguration
from KalturaClient.Plugins.Core import (
    KalturaSessionType, KalturaBaseEntryFilter, KalturaFilterPager,
    KalturaBaseEntry
)
from KalturaClient.exceptions import KalturaException


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


def get_entries_by_ids(client, entry_ids):
    entries = []
    for eid in entry_ids:
        try:
            e = client.baseEntry.get(eid)
            entries.append(e)
        except KalturaException:
            print(f"Warning: Entry {eid} not found or not accessible.")
    return entries


def get_entries_by_tag(client, tag):
    entry_filter = KalturaBaseEntryFilter()
    # This will find entries whose tags contain the given tag string
    entry_filter.tagsLike = tag
    pager = KalturaFilterPager()

    # Get all entries matching the tag
    entries = []
    page_index = 1
    pager.pageSize = 100

    while True:
        pager.pageIndex = page_index
        response = client.baseEntry.list(entry_filter, pager)
        if not response.objects:
            break
        entries.extend(response.objects)
        if len(response.objects) < pager.pageSize:
            break
        page_index += 1

    return entries


def get_entries_by_category(client, category_id):
    entry_filter = KalturaBaseEntryFilter()
    entry_filter.categoriesIdsMatchOr = str(category_id)
    pager = KalturaFilterPager()

    entries = []
    page_index = 1
    pager.pageSize = 100

    while True:
        pager.pageIndex = page_index
        response = client.baseEntry.list(entry_filter, pager)
        if not response.objects:
            break
        entries.extend(response.objects)
        if len(response.objects) < pager.pageSize:
            break
        page_index += 1

    return entries


def main():
    # Prompt for PID and Admin Secret
    partner_id = input("Enter your Partner ID: ").strip()
    admin_secret = input("Enter your Admin Secret: ").strip()
    client = get_kaltura_client(partner_id, admin_secret)

    # Prompt the user for how they want to select entries
    print("How do you want to select entries?")
    print("1: Comma-delimited list of entry IDs")
    print("2: By tag")
    print("3: By category ID")
    selection = input("Enter 1, 2, or 3: ").strip()

    entries = []
    selection_mode = None
    selection_value = None

    if selection == "1":
        entry_ids_input = input("Enter comma-delimited list of entry IDs: ")
        entry_ids = [
            e.strip() for e in entry_ids_input.split(",") if e.strip()
            ]
        entries = get_entries_by_ids(client, entry_ids)
        selection_mode = "ids"
        selection_value = len(entries)  # Just store number for summary
    elif selection == "2":
        tag = input("Enter the tag: ").strip()
        entries = get_entries_by_tag(client, tag)
        selection_mode = "tag"
        selection_value = tag
    elif selection == "3":
        category_id = input("Enter the category ID: ").strip()
        entries = get_entries_by_category(client, category_id)
        selection_mode = "category"
        selection_value = category_id
    else:
        print("Invalid selection. Exiting.")
        sys.exit(1)

    if not entries:
        print("No entries found. Exiting.")
        sys.exit(0)

    # Prompt for prefix or suffix
    print("Do you want to add text before or after the existing title?")
    print("P: Prefix (before)")
    print("S: Suffix (after)")
    prefix_or_suffix = input("Enter P or S: ").strip().upper()
    if prefix_or_suffix not in ["P", "S"]:
        print("Invalid choice. Exiting.")
        sys.exit(1)

    # Prompt for the text to append
    text_to_add = input("Enter the text you want to add: ").strip()

    # Show confirmation message
    num_entries = len(entries)
    if selection_mode == "ids":
        confirmation_msg = (
            f"Add [{text_to_add}] to {num_entries} entries' titles?"
        )
    elif selection_mode == "tag":
        confirmation_msg = (
            f"Add [{text_to_add}] to {num_entries} entries' titles with tag "
            f"'{selection_value}'?"
        )
    else:  # category
        confirmation_msg = (
            f"Add [{text_to_add}] to {num_entries} entries' titles in "
            f"category {selection_value}?"
        )

    print(confirmation_msg)
    confirm = input("Proceed? (Y/N): ").strip().upper()
    if confirm != "Y":
        print("Operation cancelled.")
        sys.exit(0)

    # Prepare CSV output
    now = datetime.now()
    timestamp_str = now.strftime("%Y-%m-%d-%H%M")
    csv_filename = f"EntriesRenamed_{timestamp_str}.csv"

    with open(csv_filename, mode='w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Entry ID", "Original Title", "New Title"])

        # Update entries
        for e in entries:
            original_title = e.name
            if prefix_or_suffix == "P":
                new_title = f"{text_to_add}{original_title}"
            else:
                new_title = f"{original_title}{text_to_add}"

            # Update entry title
            entry_update = KalturaBaseEntry()
            entry_update.name = new_title
            updated_entry = client.baseEntry.update(e.id, entry_update)

            # Onscreen feedback
            print(f"Updated entry {e.id}: '{original_title}' -> '{new_title}'")

            # Write to CSV
            writer.writerow([e.id, original_title, new_title])

    print(f"Renaming complete. Results saved to {csv_filename}.")

if __name__ == "__main__":
    main()