import csv
import re
import sys
import os
from dotenv import load_dotenv
from KalturaClient import KalturaClient, KalturaConfiguration
from KalturaClient.Plugins.Core import KalturaSessionType
from KalturaClient.Plugins.ThumbCuePoint import KalturaThumbCuePoint

# LOAD CREDENTIALS FROM .env FILE =============================================
load_dotenv()

PARTNER_ID = os.getenv("PARTNER_ID")
ADMIN_SECRET = os.getenv("ADMIN_SECRET")
USER_ID = os.getenv("USER_ID")
PRIVILEGES = "all:*,disableentitlement"
CSV_FILENAME = os.getenv("CSV_FILENAME")

# START SESSION ===============================================================
config = KalturaConfiguration()
config.serviceUrl = "https://www.kaltura.com"
config.partnerId = int(PARTNER_ID)
client = KalturaClient(config)

ks = client.session.start(
    ADMIN_SECRET,
    USER_ID,
    KalturaSessionType.ADMIN,
    int(PARTNER_ID),
    privileges=PRIVILEGES
)
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
