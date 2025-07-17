import datetime
import pytz
import pandas as pd
import os
from dotenv import load_dotenv
from KalturaClient import KalturaClient, KalturaConfiguration
from KalturaClient.Plugins.Core import (
    KalturaMediaEntryFilter, KalturaFilterPager, KalturaSessionType,
)
from KalturaClient.Plugins.Audit import KalturaAuditTrailFilter

# === LOAD ENVIRONMENT VARIABLES ==============================================
load_dotenv()

PARTNER_ID = os.getenv("PARTNER_ID")
ADMIN_SECRET = os.getenv("ADMIN_SECRET")
USER_ID = os.getenv("USER_ID")
SERVICE_URL = os.getenv("SERVICE_URL", "https://www.kaltura.com/")
PRIVILEGES = os.getenv("PRIVILEGES", "all:*,disableentitlement")

OWNER_ID = os.getenv("OWNER_ID")
TAGS = os.getenv("TAGS")
CATEGORY_IDS = os.getenv("CATEGORY_IDS")
CREATOR_ID = os.getenv("CREATOR_ID")
DATE_START = os.getenv("DATE_START")
DATE_END = os.getenv("DATE_END")
TIMEZONE = os.getenv("TIMEZONE", "America/Los_Angeles")
MIN_DELAY_SECONDS = int(os.getenv("MIN_REPLACEMENT_DELAY_MINUTES", 30)) * 60
MIN_DELAY_MINUTES = int(os.getenv("MIN_DELAY_MINUTES", 30))
MAX_REPLACEMENTS = int(os.getenv("MAX_REPLACEMENTS", 3))

# === CONFIGURE TIMEZONE ======================================================
USER_TZ = pytz.timezone(TIMEZONE)


def to_user_tz_string(timestamp):
    dt = datetime.datetime.fromtimestamp(
        timestamp, tz=datetime.timezone.utc
        ).astimezone(USER_TZ)
    return dt.strftime("%Y-%m-%d %H:%M:%S %Z")


def date_to_unix(date_str):
    dt = datetime.datetime.strptime(date_str, "%Y-%m-%d")
    return int(USER_TZ.localize(dt).timestamp())


# === CREATE KALTURA SESSION ==================================================
config = KalturaConfiguration()
config.serviceUrl = SERVICE_URL
client = KalturaClient(config)
ks = client.session.start(
    ADMIN_SECRET, USER_ID, KalturaSessionType.ADMIN, PARTNER_ID,
    privileges=PRIVILEGES
)
client.setKs(ks)

# === FILTER ENTRIES ==========================================================
entry_filter = KalturaMediaEntryFilter()

if OWNER_ID:
    entry_filter.userIdEqual = OWNER_ID
if TAGS:
    entry_filter.tagsMultiLikeOr = TAGS
if CATEGORY_IDS:
    entry_filter.categoryAncestorIdIn = CATEGORY_IDS
if DATE_START:
    entry_filter.createdAtGreaterThanOrEqual = date_to_unix(DATE_START)
if DATE_END:
    entry_filter.createdAtLessThanOrEqual = date_to_unix(DATE_END)
if CREATOR_ID:
    entry_filter.creatorIdEqual = CREATOR_ID

pager = KalturaFilterPager()
pager.pageSize = 100

entries = []
pager = KalturaFilterPager()
pager.pageSize = 100
page_index = 1

while True:
    pager.pageIndex = page_index
    result = client.media.list(entry_filter, pager)
    if not result.objects:
        break
    entries.extend(result.objects)
    if len(result.objects) < pager.pageSize:
        break
    page_index += 1

rows = []

print(f"\nRetrieved {len(entries)} entries matching filter criteria.\n")

for entry in entries:
    print(f"Processing: {entry.id} ({entry.name})")

    creator = getattr(entry, "creatorId", "")
    owner = getattr(entry, "userId", "")
    entry_id = entry.id
    title = entry.name
    created_at = entry.createdAt

    audit_filter = KalturaAuditTrailFilter()
    audit_filter.entryIdEqual = entry_id
    audit_logs = client.audit.auditTrail.list(audit_filter).objects

    # Filter for valid replacements after the minimum delay
    MIN_DELAY_SECONDS = MIN_DELAY_MINUTES * 60
    replacement_logs = [
        log for log in audit_logs
        if log.entryPoint == "media::updatecontent"
        and log.createdAt - created_at >= MIN_DELAY_SECONDS
    ]

    if replacement_logs:
        row = {
            "entry_id": entry.id,
            "title": entry.name,
            "creator_id": creator,
            "owner_id": owner,
            "created_at": to_user_tz_string(created_at)
        }

        # Sort replacement logs by time
        replacement_logs.sort(key=lambda x: x.createdAt)

        for i in range(1, MAX_REPLACEMENTS + 1):
            if i <= len(replacement_logs):
                log = replacement_logs[i - 1]
                row[f"replacement{str(i).zfill(2)}"] = (
                    to_user_tz_string(log.createdAt)
                )
                row[f"replacement{str(i).zfill(2)}_user"] = log.userId
            else:
                row[f"replacement{str(i).zfill(2)}"] = ""
                row[f"replacement{str(i).zfill(2)}_user"] = ""

        rows.append(row)


# === EXPORT TO EXCEL WITH MULTIPLE SHEETS ====================================

timestamp = datetime.datetime.now().strftime("%Y-%m-%d-%H%M")
filename = f"{timestamp}_ReplacementsAudit.xlsx"

# Prepare the main results DataFrame
columns = ["entry_id", "title", "creator_id", "owner_id", "created_at"]
for i in range(1, MAX_REPLACEMENTS + 1):
    columns.append(f"replacement{str(i).zfill(2)}")
    columns.append(f"replacement{str(i).zfill(2)}_user")

df_results = pd.DataFrame(rows, columns=columns)

# Prepare a dictionary of search parameters (excluding session credentials)
search_terms = {
    "OWNER_ID": OWNER_ID,
    "TAGS": TAGS,
    "CATEGORY_IDS": CATEGORY_IDS,
    "CREATOR_ID": CREATOR_ID,
    "DATE_START": DATE_START,
    "DATE_END": DATE_END,
    "TIMEZONE": TIMEZONE,
    "MIN_DELAY_MINUTES": MIN_DELAY_MINUTES,
    "MAX_REPLACEMENTS": MAX_REPLACEMENTS
}

df_terms = pd.DataFrame([search_terms])

# Write both sheets to the same Excel file
with pd.ExcelWriter(filename, engine="openpyxl") as writer:
    df_results.to_excel(writer, sheet_name="Results", index=False)
    df_terms.to_excel(writer, sheet_name="Search_Terms", index=False)

print(f"\n✅ Exported results and search terms to: {filename}")
