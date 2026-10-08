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

from dotenv import load_dotenv

# Optional .env: only the reliability settings below are read from it.
load_dotenv()

# ── Network retry ──────────────────────────────────────────────────────
# KalturaClientException (timeouts, resets) is NOT a KalturaException, so
# plain `except KalturaException` misses it. Knobs come from .env:
# REQUEST_TIMEOUT, MAX_NETWORK_RETRIES, NETWORK_RETRY_DELAY.
import os as _os
import time as _time

import requests as _requests
from KalturaClient.exceptions import (
    KalturaClientException as _KalturaClientException,
)


def _retry_env_int(name, default):
    try:
        return int((_os.getenv(name) or "").strip() or default)
    except ValueError:
        return default


def call_with_retry(fn, *args, **kwargs):
    """Call fn(*args, **kwargs), retrying with linear backoff on transient
    network failures. Real API errors (KalturaException) are re-raised
    untouched so callers can handle them."""
    retries = _retry_env_int("MAX_NETWORK_RETRIES", 5)
    delay = _retry_env_int("NETWORK_RETRY_DELAY", 5)
    for attempt in range(1, retries + 1):
        try:
            return fn(*args, **kwargs)
        except (
            _KalturaClientException,
            _requests.exceptions.RequestException,
        ) as exc:
            if attempt == retries:
                raise
            wait = delay * attempt
            print(
                f"    [network error: {exc}; retry "
                f"{attempt}/{retries} in {wait}s]"
            )
            _time.sleep(wait)



def get_kaltura_client(partner_id, admin_secret):
    config = KalturaConfiguration(partner_id)
    config.requestTimeout = _retry_env_int("REQUEST_TIMEOUT", 120)
    config.serviceUrl = "https://www.kaltura.com/"
    client = KalturaClient(config)
    try:
        ks = call_with_retry(client.session.start,
            admin_secret, "admin", KalturaSessionType.ADMIN, partner_id,
            privileges="all:*,disableentitlement"
        )
    except Exception as e:
        if getattr(e, "code", "") == "START_SESSION_ERROR":
            print(
                "\n❌ Could not log in to Kaltura. Partner ID "
                f"[{partner_id}] and the Admin Secret were not accepted.\n"
                "   Double-check both values — the secret must be the "
                "Administrator secret (not the User secret),\n"
                "   copied exactly from KMC → Settings → Integration Settings.\n"
            )
        elif type(e).__name__ == "KalturaClientException":
            print(
                "\n❌ Could not reach Kaltura to start a session.\n"
                f"   {e}\n   Check your internet connection and try again.\n"
            )
        else:
            print(f"\n❌ Could not start Kaltura session: {e}\n")
        raise SystemExit(1)
    client.setKs(ks)
    return client


def get_entries_by_ids(client, entry_ids):
    entries = []
    for eid in entry_ids:
        try:
            e = call_with_retry(client.baseEntry.get, eid)
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
        response = call_with_retry(client.baseEntry.list, entry_filter, pager)
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
        response = call_with_retry(client.baseEntry.list, entry_filter, pager)
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
    csv_filename = f"{timestamp_str}_EntriesRenamed.csv"

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
            updated_entry = call_with_retry(client.baseEntry.update, e.id, entry_update)

            # Onscreen feedback
            print(f"Updated entry {e.id}: '{original_title}' -> '{new_title}'")

            # Write to CSV
            writer.writerow([e.id, original_title, new_title])

    print(f"Renaming complete. Results saved to {csv_filename}.")

if __name__ == "__main__":
    main()