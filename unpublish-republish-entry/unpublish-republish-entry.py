"""
unpublish-republish-entry.py

Automates the unpublish → republish workflow for Kaltura entries assigned to a single
category (Media Gallery) so support can confirm fixes immediately via the API.

Configuration is via a .env file or environment variables. See .env.example.
"""

from __future__ import annotations
import os
import sys
from typing import List

from KalturaClient import KalturaClient, KalturaConfiguration
from KalturaClient.Plugins.Core import (
    KalturaSessionType,
    KalturaCategoryFilter,
    KalturaCategoryEntry,
    KalturaCategoryEntryFilter,
)

# load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    # dotenv optional: user can still set env vars another way
    pass

# CONFIG from environment (or .env)
PARTNER_ID = os.getenv("PARTNER_ID", "")
ADMIN_SECRET = os.getenv("ADMIN_SECRET", "")
USER_ID = os.getenv("USER_ID", "api-gbdavis")
PRIVILEGES = os.getenv("PRIVILEGES", "all:*,disableentitlement")
USE_CATEGORY_NAME = os.getenv("USE_CATEGORY_NAME", "False").lower() in ("1", "true", "yes")
CATEGORY_PATH_PREFIX = os.getenv("CATEGORY_PATH_PREFIX", "")
CHANNEL_NAME_ENV = os.getenv("CHANNEL_NAME", "").strip()

# Allow comma-separated entry list via env: ENTRY_IDS="1_foo,1_bar"
ENV_ENTRY_IDS = os.getenv("ENTRY_IDS", "").strip()

# === START KALTURA SESSION ===
if not PARTNER_ID or not ADMIN_SECRET:
    print("❌ PARTNER_ID and ADMIN_SECRET must be set in environment or .env. Exiting.")
    sys.exit(1)

config = KalturaConfiguration()
config.serviceUrl = os.getenv("KALTURA_SERVICE_URL", "https://www.kaltura.com")
config.partnerId = int(PARTNER_ID)
client = KalturaClient(config)

ks = client.session.start(
    ADMIN_SECRET,
    USER_ID,
    KalturaSessionType.ADMIN,
    int(PARTNER_ID),
    privileges=PRIVILEGES,
)
client.setKs(ks)

# === INPUTS ===
if ENV_ENTRY_IDS:
    entry_ids = [e.strip() for e in ENV_ENTRY_IDS.split(",") if e.strip()]
else:
    raw = input("Entry ID(s) (comma-separated if multiple): ").strip()
    entry_ids = [e.strip() for e in raw.split(",") if e.strip()]

if not entry_ids:
    print("❌ No entry IDs provided. Exiting.")
    sys.exit(1)

if USE_CATEGORY_NAME:
    if not CATEGORY_PATH_PREFIX:
        print("❌ CATEGORY_PATH_PREFIX must be set when USE_CATEGORY_NAME is True. Exiting.")
        sys.exit(1)
    # allow CHANNEL_NAME to come from env or prompt
    if CHANNEL_NAME_ENV:
        channel_name = CHANNEL_NAME_ENV
    else:
        channel_name = input("Channel name (e.g., Canvas course ID): ").strip()

    cat_filter = KalturaCategoryFilter()
    cat_filter.fullNameEqual = CATEGORY_PATH_PREFIX + channel_name
    cat_result = client.category.list(cat_filter)
    cat_objs = getattr(cat_result, "objects", []) or []
    if not cat_objs:
        print(f"❌ No category found with full name '{CATEGORY_PATH_PREFIX + channel_name}'. Exiting.")
        sys.exit(1)
    category_id = str(cat_objs[0].id)
    print(f"✅ Found category ID: {category_id} for full name '{CATEGORY_PATH_PREFIX + channel_name}'")
else:
    category_id = input("Category ID: ").strip()
    if not category_id:
        print("❌ No category ID provided. Exiting.")
        sys.exit(1)
    print(f"✅ Using category ID: {category_id}")

# single category for all entry IDs

def entry_in_category(entry_id: str, category_id: str) -> bool:
    f = KalturaCategoryEntryFilter()
    f.categoryIdEqual = category_id
    f.entryIdEqual = entry_id
    resp = client.categoryEntry.list(f)
    return getattr(resp, "totalCount", 0) > 0


def remove_from_category(entry_id: str, category_id: str) -> bool:
    f = KalturaCategoryEntryFilter()
    f.categoryIdEqual = category_id
    f.entryIdEqual = entry_id
    status_resp = client.categoryEntry.list(f)
    is_active = getattr(status_resp, "totalCount", 0) > 0 and getattr(status_resp.objects[0].status, "value", None) == 2
    if not is_active:
        print(f"⚠️ Entry {entry_id} is not in an active state for category {category_id}. Skipping removal.")
        return True  # treat as success so we can attempt re-add below
    try:
        # note: SDK sometimes has argument order quirks; using names for clarity
        client.categoryEntry.delete(entryId=entry_id, categoryId=category_id)
        return True
    except Exception as exc:
        # normalize error text
        msg = str(exc).replace("Entry doesn't assigned", "Entry isn't assigned")
        print(f"⚠️ Could not remove entry: {msg}")
        return False


def add_to_category(entry_id: str, category_id: str) -> bool:
    assoc = KalturaCategoryEntry()
    assoc.categoryId = category_id
    assoc.entryId = entry_id
    try:
        client.categoryEntry.add(assoc)
        return True
    except Exception as exc:
        print(f"⚠️ Could not re-add entry: {exc}")
        return False


# iterate entries
for entry_id in entry_ids:
    print("\n---")
    print(f"Processing entry: {entry_id} against category {category_id}")

    # REMOVE
    print("🔄 Removing entry from category...")
    ok = remove_from_category(entry_id, category_id)
    if not ok:
        print("❌ Removal failed. Skipping this entry.")
        continue

    # VERIFY REMOVAL
    removed_check = not entry_in_category(entry_id, category_id)
    if removed_check:
        print(f"✅ Confirmed that entry ID {entry_id} is no longer in category {category_id}")
    else:
        print("❌ Failed to confirm removal. Entry still appears in category. Skipping re-add.")
        continue

    # ADD
    print("🔄 Adding entry to category...")
    ok = add_to_category(entry_id, category_id)
    if not ok:
        print("❌ Add failed. Skipping.")
        continue

    # VERIFY ADD
    added_check = entry_in_category(entry_id, category_id)
    if added_check:
        print(f"✅ Confirmed that entry ID {entry_id} is now in category {category_id}")
    else:
        print("❌ Failed to confirm addition. Entry still not appearing in category.")
