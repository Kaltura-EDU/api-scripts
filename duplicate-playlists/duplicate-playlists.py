import os
import csv
import getpass
import xml.etree.ElementTree as ET
from urllib.parse import unquote
from datetime import datetime
from dotenv import load_dotenv
from KalturaClient import KalturaClient, KalturaConfiguration
from KalturaClient.Plugins.Core import (
    KalturaSessionType,
    KalturaCategoryFilter,
    KalturaFilterPager,
)
from KalturaClient.Plugins.Metadata import (
    KalturaMetadataObjectType, KalturaMetadataFilter
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


# Load environment variables
load_dotenv()

PARTNER_ID = os.getenv("PARTNER_ID")
USER_ID = os.getenv("USER_ID")
PRIVILEGES = os.getenv("PRIVILEGES", "all:*,disableentitlement")
METADATA_PROFILE_ID = os.getenv("METADATA_PROFILE_ID")

SOURCE_CATEGORY_ID = os.getenv("SOURCE_CATEGORY_ID", "").strip()
SOURCE_CATEGORY_NAME = os.getenv("SOURCE_CATEGORY_NAME", "").strip()
DESTINATION_CATEGORY_ID = os.getenv("DESTINATION_CATEGORY_ID", "").strip()
DESTINATION_CATEGORY_NAME = os.getenv(
    "DESTINATION_CATEGORY_NAME", ""
).strip()

# Validate source/destination inputs before starting the session
if SOURCE_CATEGORY_ID and SOURCE_CATEGORY_NAME:
    print(
        "Error: set SOURCE_CATEGORY_ID or SOURCE_CATEGORY_NAME in .env,"
        " not both."
    )
    exit()
if not SOURCE_CATEGORY_ID and not SOURCE_CATEGORY_NAME:
    print(
        "Error: set either SOURCE_CATEGORY_ID or SOURCE_CATEGORY_NAME"
        " in .env."
    )
    exit()
if DESTINATION_CATEGORY_ID and DESTINATION_CATEGORY_NAME:
    print(
        "Error: set DESTINATION_CATEGORY_ID or DESTINATION_CATEGORY_NAME"
        " in .env, not both."
    )
    exit()
if not DESTINATION_CATEGORY_ID and not DESTINATION_CATEGORY_NAME:
    print(
        "Error: set either DESTINATION_CATEGORY_ID or"
        " DESTINATION_CATEGORY_NAME in .env."
    )
    exit()

ADMIN_SECRET = getpass.getpass("Enter your Kaltura admin secret: ")

# Set up Kaltura session
config = KalturaConfiguration()
config.requestTimeout = _retry_env_int("REQUEST_TIMEOUT", 120)
config.serviceUrl = "https://www.kaltura.com"
config.partnerId = int(PARTNER_ID)
client = KalturaClient(config)

try:
    ks = call_with_retry(client.session.start,
        ADMIN_SECRET,
        USER_ID,
        KalturaSessionType.ADMIN,
        int(PARTNER_ID),
        privileges=PRIVILEGES
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


def resolve_category(cat_id, cat_name, id_var_name):
    """Return (id_str, name_str) resolved from either a category ID or name."""
    if cat_id:
        cat = call_with_retry(client.category.get, int(cat_id))
        return str(cat.id), cat.name
    cat_filter = KalturaCategoryFilter()
    cat_filter.freeText = cat_name
    pager = KalturaFilterPager()
    pager.pageSize = 500
    pager.pageIndex = 1
    candidates = []
    while True:
        result = call_with_retry(client.category.list, cat_filter, pager)
        if not result.objects:
            break
        candidates.extend(result.objects)
        if len(result.objects) < pager.pageSize:
            break
        pager.pageIndex += 1
    matches = [c for c in candidates if c.name == cat_name]
    if not matches:
        print(f"Error: no category found with name '{cat_name}'.")
        exit()
    if len(matches) > 1:
        ids = ", ".join(str(c.id) for c in matches)
        print(
            f"Error: multiple categories match name '{cat_name}'"
            f" (IDs: {ids}). Use {id_var_name} instead."
        )
        exit()
    cat = matches[0]
    return str(cat.id), cat.name


# Helper to get custom metadata XML for a category
def get_category_metadata_xml(category_id):
    filter = KalturaMetadataFilter()
    filter.metadataProfileIdEqual = int(METADATA_PROFILE_ID)
    filter.objectIdEqual = str(category_id)
    filter.metadataObjectTypeEqual = KalturaMetadataObjectType.CATEGORY
    result = call_with_retry(client.metadata.metadata.list, filter)
    return result.objects[0] if result.objects else None


# Helper to extract value for channelPlaylistsIds from XML
def extract_playlist_ids(xml_string):
    root = ET.fromstring(unquote(xml_string))
    for detail in root.findall("Detail"):
        if detail.find("Key").text == "channelPlaylistsIds":
            value = detail.find("Value").text or ""
            return value.split(",") if value else []
    return []


# Helper to update metadata XML with new playlist IDs
def update_metadata_xml(xml_string, new_ids):
    root = ET.fromstring(unquote(xml_string))
    found = False
    for detail in root.findall("Detail"):
        if detail.find("Key").text == "channelPlaylistsIds":
            current = detail.find("Value").text or ""
            all_ids = set(current.split(",") if current else [])
            all_ids.update(new_ids)
            detail.find("Value").text = ",".join(all_ids)
            found = True
            break
    if not found:
        new_detail = ET.SubElement(root, "Detail")
        ET.SubElement(new_detail, "Key").text = "channelPlaylistsIds"
        ET.SubElement(new_detail, "Value").text = ",".join(new_ids)
    return ET.tostring(root, encoding="unicode")


# Resolve source and destination categories
source_cat_id, source_category_name = resolve_category(
    SOURCE_CATEGORY_ID, SOURCE_CATEGORY_NAME, "SOURCE_CATEGORY_ID"
)
destination_cat_id, destination_category_name = resolve_category(
    DESTINATION_CATEGORY_ID, DESTINATION_CATEGORY_NAME,
    "DESTINATION_CATEGORY_ID"
)

# Retrieve and extract playlist IDs
source_metadata = get_category_metadata_xml(source_cat_id)
if not source_metadata:
    print("No metadata found for source category.")
    exit()

playlist_ids = extract_playlist_ids(source_metadata.xml)

# Confirm before proceeding
confirmation = input(
    f"{len(playlist_ids)} playlists found in {source_category_name} "
    f"({source_cat_id}). Duplicate them and assign to "
    f"{destination_category_name} ({destination_cat_id})? [y/N] "
).strip().lower()
if confirmation != "y":
    print("Aborted.")
    exit()

# Clone playlists and get names
cloned_pairs = []
for pid in playlist_ids:
    print(f"Duplicating {pid}...")
    new_playlist = call_with_retry(client.playlist.clone, pid)
    original = call_with_retry(client.playlist.get, pid)
    cloned_pairs.append((original.name, pid, new_playlist.id))

# Retrieve destination metadata object
destination_metadata = get_category_metadata_xml(destination_cat_id)
if not destination_metadata:
    print("No metadata found for destination category.")
    exit()

# Update XML and push to Kaltura
updated_xml = (
    update_metadata_xml(
        destination_metadata.xml,
        [new for _, _, new in cloned_pairs]
    )
)
call_with_retry(client.metadata.metadata.update, destination_metadata.id, updated_xml)

# Output CSV
timestamp = datetime.now().strftime("%Y-%m-%d-%H%M")
csv_filename = f"{timestamp}_duplicate-playlists.csv"
with open(csv_filename, "w", newline="") as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow([
        "playlist_name", "source_category_id", "source_category_name",
        "source_playlist_id", "destination_category_id",
        "destination_category_name", "destination_playlist_id"
    ])
    for name, old_id, new_id in cloned_pairs:
        writer.writerow([
            name, source_cat_id, source_category_name,
            old_id, destination_cat_id, destination_category_name,
            new_id
        ])

print(
    f"{len(cloned_pairs)} playlists added to category "
    f"{destination_category_name} ({destination_cat_id})"
)
print(f"Results saved to {csv_filename}.")
