"""
unpublish-republish-entry.py

Automates the unpublish → republish workflow for Kaltura entries assigned to a single
category (Media Gallery) so support can confirm fixes immediately via the API.

Configuration is via a .env file or environment variables. See .env.example.
"""

from __future__ import annotations
import os
import getpass
import sys
from typing import List

from KalturaClient import KalturaClient, KalturaConfiguration
from KalturaClient.Plugins.Core import (
    KalturaSessionType,
    KalturaCategoryFilter,
    KalturaCategoryEntry,
    KalturaCategoryEntryFilter,
)

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


# load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    # dotenv optional: user can still set env vars another way
    pass

# CONFIG from environment (or .env)
PARTNER_ID = os.getenv("PARTNER_ID", "")
# The admin secret is never read from .env -- it is prompted at runtime.
ADMIN_SECRET = ""
USER_ID = os.getenv("USER_ID", "api-user")
PRIVILEGES = os.getenv("PRIVILEGES", "all:*,disableentitlement")
USE_CATEGORY_NAME = os.getenv("USE_CATEGORY_NAME", "False").lower() in ("1", "true", "yes")
CATEGORY_PATH_PREFIX = os.getenv("CATEGORY_PATH_PREFIX", "")
CHANNEL_NAME_ENV = os.getenv("CHANNEL_NAME", "").strip()

# Allow comma-separated entry list via env: ENTRY_IDS="1_foo,1_bar"
ENV_ENTRY_IDS = os.getenv("ENTRY_IDS", "").strip()

# === START KALTURA SESSION ===
if not PARTNER_ID:
    print("❌ PARTNER_ID must be set in environment or .env. Exiting.")
    sys.exit(1)

def _prompt_admin_secret():
    """Ask for the admin secret. It is never read from .env."""
    if os.getenv("ADMIN_SECRET", "").strip():
        print(
            "\n⚠️  ADMIN_SECRET is set in your .env. This script does not "
            "read it -- you will be asked for the secret instead.\n"
            "   Please delete that line from .env so the secret is not "
            "stored on disk.\n"
        )
    secret = getpass.getpass(
        "Enter your Kaltura admin secret (input hidden): "
    ).strip()
    if not secret:
        print("❌ No admin secret entered. Exiting.")
        raise SystemExit(1)
    return secret


ADMIN_SECRET = _prompt_admin_secret()

config = KalturaConfiguration()
config.requestTimeout = _retry_env_int("REQUEST_TIMEOUT", 120)
config.serviceUrl = os.getenv("KALTURA_SERVICE_URL", "https://www.kaltura.com")
config.partnerId = int(PARTNER_ID)
client = KalturaClient(config)

try:
    ks = call_with_retry(client.session.start,
        ADMIN_SECRET,
        USER_ID,
        KalturaSessionType.ADMIN,
        int(PARTNER_ID),
        privileges=PRIVILEGES,
    )
except Exception as e:
    if getattr(e, "code", "") == "START_SESSION_ERROR":
        print(
            "\n❌ Could not log in to Kaltura. Partner ID "
            f"[{PARTNER_ID}] and the Admin Secret were not accepted.\n"
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
    cat_result = call_with_retry(client.category.list, cat_filter)
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
    resp = call_with_retry(client.categoryEntry.list, f)
    return getattr(resp, "totalCount", 0) > 0


def remove_from_category(entry_id: str, category_id: str) -> bool:
    f = KalturaCategoryEntryFilter()
    f.categoryIdEqual = category_id
    f.entryIdEqual = entry_id
    status_resp = call_with_retry(client.categoryEntry.list, f)
    is_active = getattr(status_resp, "totalCount", 0) > 0 and getattr(status_resp.objects[0].status, "value", None) == 2
    if not is_active:
        print(f"⚠️ Entry {entry_id} is not in an active state for category {category_id}. Skipping removal.")
        return True  # treat as success so we can attempt re-add below
    try:
        # note: SDK sometimes has argument order quirks; using names for clarity
        call_with_retry(client.categoryEntry.delete, entryId=entry_id, categoryId=category_id)
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
        call_with_retry(client.categoryEntry.add, assoc)
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
