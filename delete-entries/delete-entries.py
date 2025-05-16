'''
This script permanently deletes one or more Kaltura media entries based on
entry IDs provided by the user. It authenticates using an admin session,
retrieves entry metadata for confirmation, and writes a report to a timestamped
CSV file before deletion.

Key features:
- Prompts for comma-separated entry IDs to delete.
- Retrieves and displays entry metadata (name, owner, duration).
- Exports a report CSV listing all entries and deletion status.
- Skips any entries that cannot be retrieved.
- Requires user confirmation before performing deletions.

Usage:
    1. Enter your parnter ID and your Kaltura instance's admin secret below.
    2. Run the script and enter entry IDs when prompted.
    3. To proceed with deletion, type "DELETE" when prompted for confirmation.
'''

import csv
from KalturaClient import KalturaClient, KalturaConfiguration
from KalturaClient.Plugins.Core import KalturaSessionType
from datetime import datetime
from KalturaClient.exceptions import KalturaException


# Configuration ---------------------------------------------------------------
PARTNER_ID = ""
ADMIN_SECRET = ""
USER_ID = ""
SERVICE_URL = "https://www.kaltura.com/"
PRIVILEGES = "all:*,disableentitlement"
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
OUTPUT_CSV = f"deleted_entries_{timestamp}.csv"

# Start session ---------------------------------------------------------------
config = KalturaConfiguration(PARTNER_ID)
config.serviceUrl = SERVICE_URL
client = KalturaClient(config)

ks = client.session.start(
    ADMIN_SECRET,
    USER_ID,
    KalturaSessionType.ADMIN,
    PARTNER_ID,
    privileges=PRIVILEGES
)
client.setKs(ks)

# Get entry IDs from user -----------------------------------------------------
entry_ids_input = input("Enter comma-separated entry IDs to delete: ").strip()
entry_ids = [eid.strip() for eid in entry_ids_input.split(',') if eid.strip()]

# Collect entry info ----------------------------------------------------------
report = []
for eid in entry_ids:
    try:
        entry = client.baseEntry.get(eid)
        report.append({
            "entry_id": eid,
            "entry_name": entry.name,
            "owner_user_id": entry.userId,
            "duration_seconds": entry.duration,
            "status": "OK"
        })
    except KalturaException as e:
        print(f"[SKIPPED] Could not retrieve info for entry ID {eid}: {e}")
        report.append({
            "entry_id": eid,
            "entry_name": "",
            "owner_user_id": "",
            "duration_seconds": "",
            "status": "NOT FOUND"
        })

if all(r["status"] != "OK" for r in report):
    print("\n[INFO] No valid entries to delete. Exiting.")
    with open(OUTPUT_CSV, mode="w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(
            csvfile, fieldnames=[
                "entry_id", "entry_name", "owner_user_id",
                "duration_seconds", "status"
                ]
            )
        writer.writeheader()
        writer.writerows(report)
    exit()

#  Write to CSV ---------------------------------------------------------------
if report:
    with open(OUTPUT_CSV, mode="w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(
            csvfile, fieldnames=[
                "entry_id", "entry_name", "owner_user_id",
                "duration_seconds", "status"
                ]
            )
        writer.writeheader()
        writer.writerows(report)
    print(f"\n[INFO] Wrote report to {OUTPUT_CSV}")
else:
    print("\n[ERROR] No valid entries to report. Exiting.")
    exit()

# Confirm and delete ----------------------------------------------------------
confirm = input("\nType 'DELETE' to permanently delete these entries: ")
if confirm.strip().upper() == "DELETE":
    for row in report:
        eid = row["entry_id"]
        if row.get("status") != "OK":
            continue  # Skip entries already marked as not found

        try:
            client.baseEntry.delete(eid)
            print(f"[DELETED] Entry {eid}")
            row["status"] = "DELETED"
        except KalturaException as e:
            print(
                f"[SKIPPED] Entry {eid} could not be deleted "
                f"(probably already gone): {e}"
                )
            row["status"] = "ALREADY DELETED"

    deleted_count = sum(1 for row in report if row.get("status") == "DELETED")
    print(f"\n[INFO] {deleted_count} entries successfully deleted.")
else:
    print("[ABORTED] No entries deleted.")
