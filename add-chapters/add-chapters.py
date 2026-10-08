import csv
import re
import sys
import os
from getpass import getpass
from dotenv import load_dotenv
from KalturaClient import KalturaClient, KalturaConfiguration
from KalturaClient.Plugins.Core import KalturaSessionType
from KalturaClient.Plugins.ThumbCuePoint import KalturaThumbCuePoint

# ── Input files ────────────────────────────────────────────────────────
# Input files live in the input/ folder next to this script. A bare
# filename, a leading "input/", or an absolute path all work.
def resolve_input_path(name):
    name = os.path.expanduser(str(name).strip())
    if os.path.isabs(name):
        return name
    base = os.path.dirname(os.path.abspath(__file__))
    norm = name.replace("\\", "/")
    candidate = os.path.join(
        base, name if norm.startswith("input/") else os.path.join("input", name)
    )
    if not os.path.exists(candidate) and os.path.exists(name):
        return os.path.abspath(name)  # relative to where you launched it
    return candidate


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


# LOAD CREDENTIALS FROM .env FILE =============================================
load_dotenv()

PARTNER_ID = os.getenv("PARTNER_ID")
# The admin secret is never read from .env -- it is prompted below.
ADMIN_SECRET = ""
USER_ID = os.getenv("USER_ID")
PRIVILEGES = "all:*,disableentitlement"
CSV_FILENAME = os.getenv("CSV_FILENAME")

# START SESSION ===============================================================
if os.getenv("ADMIN_SECRET", "").strip():
    print(
        "\n⚠️  ADMIN_SECRET is set in your .env. This script does not read "
        "it -- you will be asked for the secret instead.\n"
        "   Please delete that line from .env so the secret is not stored "
        "on disk.\n"
    )
ADMIN_SECRET = getpass("Enter your Kaltura admin secret (input hidden): ").strip()
if not ADMIN_SECRET:
    print("❌ No admin secret entered. Exiting.")
    raise SystemExit(1)

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


# VALIDATE TIMECODE FORMAT ====================================================
def validate_timecode_format(timecode):
    return re.match(r"^\d{2}:\d{2}:\d{2}$", timecode)


# CONVERT TIMECODE TO MILLISECONDS ============================================
def timecode_to_milliseconds(timecode):
    hh, mm, ss = map(int, timecode.split(":"))
    return (hh * 3600000) + (mm * 60000) + (ss * 1000)


# READ CSV AND PROCESS CHAPTERS ===============================================
try:
    with open(resolve_input_path(CSV_FILENAME), mode='r', encoding='utf-8-sig') as file:
        reader = csv.DictReader(file)
        expected_headers = ["entry_id", "timecode", "chapter_title", "chapter_description", "search_tags"]

        # Strip trailing empty headers before comparison
        actual_headers = [h for h in reader.fieldnames if h and h.strip() != ""]

        if actual_headers != expected_headers:
            print(f"ERROR: CSV headers must be exactly: {', '.join(expected_headers)}")
            sys.exit(1)

        for row in reader:
            entry_id = row["entry_id"].strip()
            timecode = row["timecode"].strip()
            chapter_title = row["chapter_title"].strip()
            chapter_description = row["chapter_description"].strip()
            search_tags = row["search_tags"].strip()

            if not validate_timecode_format(timecode):
                print(f"ERROR: Invalid timecode format in row: {row}")
                continue

            start_time_ms = timecode_to_milliseconds(timecode)

            cue_point = KalturaThumbCuePoint()
            cue_point.cuePointType = "thumbCuePoint.Thumb"
            cue_point.entryId = entry_id
            cue_point.tags = search_tags
            cue_point.startTime = start_time_ms
            cue_point.userId = USER_ID if USER_ID else None
            cue_point.description = chapter_description
            cue_point.title = chapter_title
            cue_point.subType = 2  # 2 = CHAPTER
            cue_point.objectType = "KalturaThumbCuePoint"

            try:
                call_with_retry(client.cuePoint.cuePoint.add, cue_point)
                print(f"Added chapter: {entry_id} | {timecode} | {chapter_title}")
            except Exception as e:
                print(f"ERROR adding chapter for entry {entry_id}: {e}")

except FileNotFoundError:
    print(
        f"ERROR: File not found: {resolve_input_path(CSV_FILENAME)}\n"
        "Put your CSV in the input/ folder next to the script."
    )
    sys.exit(1)
