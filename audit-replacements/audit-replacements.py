import datetime
import pytz
import pandas as pd
from KalturaClient import KalturaClient, KalturaConfiguration
from KalturaClient.Plugins.Core import (
    KalturaMediaEntryFilter,
    KalturaFilterPager,
    KalturaSessionType,
)
from KalturaClient.Plugins.Audit import KalturaAuditTrailFilter

# === Global Config ===
PARTNER_ID = ""
ADMIN_SECRET = ""
USER_ID = "api-gbdavis"
SERVICE_URL = "https://www.kaltura.com/"

# === Kaltura Session ===
config = KalturaConfiguration()
config.serviceUrl = SERVICE_URL
client = KalturaClient(config)
ks = client.session.start(
    ADMIN_SECRET, USER_ID, KalturaSessionType.ADMIN, PARTNER_ID,
    privileges="all:*,disableentitlement"
)
client.setKs(ks)

# === Timezone Config ===
PACIFIC = pytz.timezone("America/Los_Angeles")


def to_pt_string(timestamp):
    dt = datetime.datetime.fromtimestamp(
        timestamp, tz=datetime.timezone.utc
        ).astimezone(PACIFIC)
    return dt.strftime("%Y-%m-%d %H:%M:%S PT")


# === Select Entry Source ===
print("Select entry source:")
print("[1] Tag")
print("[2] Category ID")
print("[3] Comma-delimited list of entry IDs")

choice = input("Enter your choice (1–3): ").strip()
if choice not in {"1", "2", "3"}:
    print("Invalid choice. Exiting.")
    exit()

identifier = input("Enter the tag, category ID, or entry IDs: ").strip()

entry_filter = KalturaMediaEntryFilter()
pager = KalturaFilterPager()
pager.pageSize = 100

entries = []

if choice == "1":
    entry_filter.tagsMultiLikeOr = identifier
elif choice == "2":
    entry_filter.categoryAncestorIdIn = identifier
elif choice == "3":
    entry_filter.idIn = identifier
else:
    print("Unrecognized input. Exiting.")
    exit()

entries = client.media.list(entry_filter, pager).objects
if not entries:
    print("No entries found matching the criteria.")
    exit()

rows = []

for entry in entries:
    print(f"Checking: {entry.id} ({entry.name})")
    creator = getattr(entry, 'creatorId', '')
    entry_id = entry.id
    title = entry.name
    created_at = entry.createdAt

    audit_filter = KalturaAuditTrailFilter()
    audit_filter.entryIdEqual = entry_id
    audit_logs = client.audit.auditTrail.list(audit_filter).objects

    replacement_logs = [
        log for log in audit_logs
        if (
            log.entryPoint == "media::updatecontent"
            and log.createdAt > created_at
        )
    ]

    if replacement_logs:
        rows.append({
            "entry_id": entry.id,
            "title": entry.name,
            "action": "creation",
            "user_id": creator,
            "timestamp": to_pt_string(created_at)
        })
        for log in replacement_logs:
            rows.append({
                "entry_id": entry.id,
                "title": entry.name,
                "action": "replacement",
                "user_id": log.userId,
                "timestamp": to_pt_string(log.createdAt)
            })

# === Export to Excel ===
timestamp = datetime.datetime.now().strftime("%Y-%m-%d-%H%M")
filename = f"{timestamp}_ReplacementsAudit.xlsx"
df = pd.DataFrame(rows, columns=[
    "entry_id", "title", "action", "user_id", "timestamp"
])
df.to_excel(filename, index=False)
print(f"\n✅ Exported replacements report to: {filename}")
