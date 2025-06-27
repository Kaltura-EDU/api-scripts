"""
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
"""

import csv
from os import getenv

from KalturaClient import KalturaClient, KalturaConfiguration
from KalturaClient.Plugins.Core import (
    KalturaSessionType,
    KalturaAssetFilter,
    KalturaFilterPager,
    KalturaFlavorAssetFilter,
)
from datetime import datetime
from KalturaClient.exceptions import KalturaException
from dotenv import load_dotenv, find_dotenv


# ==== Global Variables ====
# find the .env file and load it
load_dotenv(find_dotenv())

# Configuration ---------------------------------------------------------------
PARTNER_ID = int(getenv("PARTNER_ID"))
ADMIN_SECRET = getenv("ADMIN_SECRET")
USER_ID = getenv("USER_ID")
FLAVOR_SOURCE = getenv("FLAVOR_SOURCE")
SERVICE_URL = "https://www.kaltura.com/"
PRIVILEGES = "all:*,disableentitlement"
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
OUTPUT_CSV = f"deleted_flavors_{timestamp}.csv"

# Start session ---------------------------------------------------------------
config = KalturaConfiguration(PARTNER_ID)
config.serviceUrl = SERVICE_URL
client = KalturaClient(config)

ks = client.session.start(
    ADMIN_SECRET, USER_ID, KalturaSessionType.ADMIN, PARTNER_ID, privileges=PRIVILEGES
)
client.setKs(ks)

# Get entry IDs from user -----------------------------------------------------
entry_ids_input = input(
    f"Enter comma-separated entry IDs to keep only the source flavor {FLAVOR_SOURCE}: "
).strip()
entry_ids = [eid.strip() for eid in entry_ids_input.split(",") if eid.strip()]

# Collect entry info ----------------------------------------------------------
report = []
for eid in entry_ids:

    try:
        entry = client.media.get(eid)

        if FLAVOR_SOURCE in entry.flavorParamsIds.split(","):
            flavor_filter = KalturaFlavorAssetFilter()
            flavor_filter.entryIdEqual = eid
            flavors = client.flavorAsset.list(flavor_filter)

            flavors_to_delete = [
                flavor.id
                for flavor in flavors.objects
                if flavor.flavorParamsId != int(FLAVOR_SOURCE)
            ]
            flavors_to_delete_size = round(
                sum(
                    [
                        flavor.size
                        for flavor in flavors.objects
                        if flavor.flavorParamsId != int(FLAVOR_SOURCE)
                    ]
                )
                / 1024,
                2,
            )  # MegaBytes

            report.append(
                {
                    "entry_id": eid,
                    "entry_name": entry.name,
                    "owner_user_id": entry.userId,
                    "entry_flavor_ids": entry.flavorParamsIds,
                    "entry_flavors_to_delete": ",".join(flavors_to_delete),
                    "entry_flavors_to_delete_size": flavors_to_delete_size,
                    "status": "OK",
                }
            )
        else:
            raise Exception(f"No flavor {FLAVOR_SOURCE}")

    except Exception as e:
        print(f"[SKIPPED] Could not retrieve info for entry ID {eid}: {e}")
        report.append(
            {
                "entry_id": eid,
                "entry_name": "",
                "owner_user_id": "",
                "entry_flavor_ids": "",
                "entry_flavors_to_delete": "",
                "entry_flavors_to_delete_size": "",
                "status": "NOT FOUND",
            }
        )

if all(r["status"] != "OK" for r in report):
    print("\n[INFO] No valid entries to delete. Exiting.")
    with open(OUTPUT_CSV, mode="w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(
            csvfile,
            fieldnames=[
                "entry_id",
                "entry_name",
                "owner_user_id",
                "entry_flavor_ids",
                "entry_flavors_to_delete",
                "entry_flavors_to_delete_size",
                "status",
            ],
        )
        writer.writeheader()
        writer.writerows(report)
    exit()

#  Write to CSV ---------------------------------------------------------------
if report:
    with open(OUTPUT_CSV, mode="w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(
            csvfile,
            fieldnames=[
                "entry_id",
                "entry_name",
                "owner_user_id",
                "entry_flavor_ids",
                "entry_flavors_to_delete",
                "entry_flavors_to_delete_size",
                "status",
            ],
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
        flavors_to_delete = row["entry_flavors_to_delete"]
        if row.get("status") != "OK" or flavors_to_delete == "":
            continue  # Skip entries already marked as not found

        flavor_deleted_count = 0
        for flavor_id in flavors_to_delete.split(","):
            # double check to be sure
            if flavor_id != FLAVOR_SOURCE:
                try:
                    client.flavorAsset.delete(flavor_id)
                    print(f"[DELETED] Flavor {flavor_id} for Entry {eid}")
                    flavor_deleted_count = flavor_deleted_count + 1
                except KalturaException as e:
                    print(
                        f"[SKIPPED] Flavor for Entry {eid} could not be deleted "
                        f"(probably already gone): {e}"
                    )
        print(
            f"\n[INFO] {flavor_deleted_count} flavor successfully deleted for Entry {eid}."
        )
else:
    print("[ABORTED] No entries deleted.")
