"""
This script permanently deletes or recycles one or more Kaltura media entries based on
entry IDs provided by the user. It authenticates using an admin session,
retrieves entry metadata for confirmation, and writes a report to a timestamped
CSV file before deletion or recycling.

Key features:
- Prompts for comma-separated entry IDs to delete.
- Retrieves and displays entry metadata (name, owner, duration).
- Exports a report CSV listing all entries and deletion/recycling status.
- Skips any entries that cannot be retrieved.
- Requires user confirmation before performing deletions/recycling.

Usage:
    1. Enter your partner ID and your Kaltura instance's admin secret in the .env file.
    2. Enter the entry IDs in the .env file or in a dedicated CSV file.
    2. Run the script.
    3. To proceed with deletion, type "DELETE" when prompted for confirmation. To proceed with recycling, type "RECYCLE" when prompted.
"""

import csv
import os
import sys
from typing import List
from urllib.parse import ResultBase

from KalturaClient import KalturaClient, KalturaConfiguration
from KalturaClient.Plugins.Core import KalturaSessionType
from datetime import datetime
from KalturaClient.exceptions import KalturaException

from dotenv import load_dotenv, find_dotenv

# =============================================================================
# Env / config ----------------------------------------------------------------
# =============================================================================
load_dotenv(find_dotenv())

def require_env_int(name: str) -> int:
    raw = os.getenv(name, "").strip()
    if not raw.isdigit():
        print(f"[ERROR] Missing or invalid {name} in .env", file=sys.stderr)
        sys.exit(2)
    return int(raw)

def get_env_csv(name: str) -> List[str]:
    raw = os.getenv(name, "") or ""
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    return parts

def now_stamp() -> str:
    # e.g., 2025-08-28-1412 (YYYY-MM-DD-HHMM, 24-hour clock)
    return datetime.now().strftime("%Y-%m-%d-%H%M")

PARTNER_ID = require_env_int("PARTNER_ID")
ADMIN_SECRET = os.getenv("ADMIN_SECRET", "").strip()
if not ADMIN_SECRET:
    print("[ERROR] Missing ADMIN_SECRET in .env", file=sys.stderr)
    sys.exit(2)

USER_ID = os.getenv("USER_ID", "").strip()  # optional
SERVICE_URL = os.getenv("SERVICE_URL", "https://www.kaltura.com").rstrip("/")
PRIVILEGES = os.getenv("PRIVILEGES", "all:*,disableentitlement")

ENTRY_IDS = get_env_csv("ENTRY_IDS")

# Support for CSV-based entry ID selection
CSV_FILENAME = os.getenv("CSV_FILENAME", "").strip()
ENTRY_ID_COLUMN_HEADER = os.getenv("ENTRY_ID_COLUMN_HEADER", "").strip()
# =============================================================================
# Helper for loading entry IDs from CSV ---------------------------------------
# =============================================================================


def load_entry_ids_from_csv() -> List[str]:
    """
    Loads entry IDs from the specified CSV file and column.
    Returns a list of non-empty entry IDs (as strings).
    """
    if not CSV_FILENAME or not ENTRY_ID_COLUMN_HEADER:
        return []
    # Path relative to script directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(script_dir, CSV_FILENAME)
    entry_ids = []
    try:
        with open(csv_path, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            # Normalize headers: strip surrounding quotes and whitespace
            reader.fieldnames = [h.strip().strip('"') for h in reader.fieldnames]
            for row in reader:
                eid = (row.get(ENTRY_ID_COLUMN_HEADER, "") or "").strip()
                if eid:
                    entry_ids.append(eid)
    except Exception as ex:
        print(f"[ERROR] Failed to load entry IDs from CSV: {csv_path}: {ex}", file=sys.stderr)
        sys.exit(2)
    return entry_ids

TS = now_stamp()

# Ensure outputs go into a "reports" subfolder alongside this script
REPORTS_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "reports"
)
os.makedirs(REPORTS_DIR, exist_ok=True)

# Filenames start with the timestamp,
# e.g., 2025-08-28-1412_deleted_entries.csv
PREVIEW_CSV = os.path.join(REPORTS_DIR, f"{TS}_deleted_entries_PREVIEW.csv")
RESULT_CSV = os.path.join(REPORTS_DIR, f"{TS}_deleted_entries_RESULT.csv")

# ==== Kaltura client bootstrap ===============================================
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

# Get entry IDs from .csv or .env file -----------------------------------------------------
if CSV_FILENAME:
    entry_ids = load_entry_ids_from_csv()
elif ENTRY_IDS:
    entry_ids = ENTRY_IDS
else:
    print("\n[ERROR] No valid ENTRY_IDS or CSV_FILENAME or ENTRY_ID_COLUMN_HEADER env variables. Exiting.")
    exit()

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
            "plays": entry.plays,
            "status": "FOUND"
        })
    except KalturaException as e:
        print(f"[SKIPPED] Could not retrieve info for entry ID {eid}: {e}")
        report.append({
            "entry_id": eid,
            "entry_name": "",
            "owner_user_id": "",
            "duration_seconds": "",
            "plays": "",
            "status": "NOT FOUND"
        })

if all(r["status"] != "FOUND" for r in report):
    print("\n[INFO] No valid entries to delete. Exiting.")
    with open(PREVIEW_CSV, mode="w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(
            csvfile, fieldnames=[
                "entry_id", "entry_name", "owner_user_id",
                "duration_seconds", "plays", "status"
                ]
            )
        writer.writeheader()
        writer.writerows(report)
    exit()

#  Write to CSV ---------------------------------------------------------------
if report:
    with open(PREVIEW_CSV, mode="w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(
            csvfile, fieldnames=[
                "entry_id", "entry_name", "owner_user_id",
                "duration_seconds", "plays", "status"
                ]
            )
        writer.writeheader()
        writer.writerows(report)
    print(f"\n[INFO] Wrote report to {PREVIEW_CSV}")
else:
    print("\n[ERROR] No valid entries to report. Exiting.")
    exit()

# Confirm and delete ----------------------------------------------------------
confirm = input("\nType 'DELETE' to permanently delete these entries or RECYCLE to put them in the owner's recycle bin: ")
match confirm.strip().upper():
    case "DELETE":
        action_log = "DELETED"
        action_call = client.baseEntry.delete
    case "RECYCLE":
        action_log = "RECYCLED"
        action_call = client.baseEntry.recycle
    case _:
        print(f"[ABORTED] No entries deleted or recycled. Unknown action: {confirm.strip().upper()}")
        exit()


for row in report:
    eid = row["entry_id"]
    if row.get("status") != "FOUND":
        continue  # Skip entries already marked as not found

    try:
        res = action_call(eid)
        display = res.displayInSearch.getValue()
        status = res.status.getValue()
        print(f"[{action_log}] Entry {eid} - DisplayInSeach {display} - Status {status}")
        row["status"] = f"{action_log} - DisplayInSeach {display} - Status {status}"
    except KalturaException as e:
        print(
            f"[SKIPPED] Entry {eid} could not be {action_log.lower()}."
            f"(probably already gone): {e}"
            )
        row["status"] = f"ALREADY {action_log}"

processed_count = sum(1 for row in report if row.get("status") == action_log)
print(f"\n[INFO] {processed_count} entries successfully {action_log.lower()}.")

#  Write results to CSV ---------------------------------------------------------------
if report:
    with open(RESULT_CSV, mode="w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(
            csvfile, fieldnames=[
                "entry_id", "entry_name", "owner_user_id",
                "duration_seconds", "plays", "status"
            ]
        )
        writer.writeheader()
        writer.writerows(report)
    print(f"\n[INFO] Wrote report to {RESULT_CSV}")
else:
    print("\n[ERROR] No valid entries to report. Exiting.")
    exit()