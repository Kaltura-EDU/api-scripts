import csv
import re
import sys
import os
from getpass import getpass
from dotenv import load_dotenv
from KalturaClient import KalturaClient, KalturaConfiguration
from KalturaClient.Plugins.Core import KalturaSessionType
from KalturaClient.Plugins.ThumbCuePoint import KalturaThumbCuePoint

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
config.serviceUrl = "https://www.kaltura.com"
config.partnerId = int(PARTNER_ID)
client = KalturaClient(config)

try:
    ks = client.session.start(
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
    with open(CSV_FILENAME, mode='r', encoding='utf-8-sig') as file:
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
                client.cuePoint.cuePoint.add(cue_point)
                print(f"Added chapter: {entry_id} | {timecode} | {chapter_title}")
            except Exception as e:
                print(f"ERROR adding chapter for entry {entry_id}: {e}")

except FileNotFoundError:
    print(f"ERROR: File '{CSV_FILENAME}' not found.")
    sys.exit(1)
