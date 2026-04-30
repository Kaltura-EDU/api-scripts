"""
This script permanently deletes or recycles one or more Kaltura media entries
based on entry IDs provided by the user. It authenticates using an admin
session, retrieves entry metadata for confirmation, and writes a report to a
timestamped CSV file before deletion or recycling.

Key features:
- Prompts for comma-separated entry IDs to delete.
- Retrieves and displays entry metadata (name, owner, duration).
- Exports a report CSV listing all entries and deletion/recycling status.
- Skips any entries that cannot be retrieved.
- Requires user confirmation before performing deletions/recycling.
- DRY_RUN=true skips confirmation and API calls; writes a result CSV with
  status "DRY RUN" so you can verify the entry list before committing.
- MAX_WORKERS controls concurrent API calls (default 1).
- LOOKUP_BEFORE_ACTION=true fetches entry metadata before deleting, giving
  richer output columns (name, owner, duration, plays). Set to false to skip
  the lookup phase and go straight to deletion — faster, but those columns
  will be blank in the result CSV.

Usage:
    1. Enter your partner ID and your Kaltura instance's admin secret in the
       .env file.
    2. Enter the entry IDs in the .env file or in a dedicated CSV file.
    3. Run the script.
    4. To proceed with deletion, type "DELETE" when prompted for confirmation.
       To proceed with recycling, type "RECYCLE" when prompted.
"""

import csv
import os
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv, find_dotenv
from KalturaClient import KalturaClient, KalturaConfiguration
from KalturaClient.Plugins.Core import KalturaSessionType


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

DRY_RUN = (
    os.getenv("DRY_RUN", "").strip().lower() in {"1", "true", "yes", "y", "on"}
)
MAX_WORKERS = max(1, int(os.getenv("MAX_WORKERS", "1").strip() or "1"))
REQUEST_TIMEOUT_SEC = max(
    5, int(os.getenv("REQUEST_TIMEOUT_SEC", "30").strip() or "30")
)
REQUEST_CONNECT_TIMEOUT_SEC = max(
    3, int(os.getenv("REQUEST_CONNECT_TIMEOUT_SEC", "10").strip() or "10")
)
LOOKUP_BEFORE_ACTION = (
    os.getenv("LOOKUP_BEFORE_ACTION", "true").strip().lower()
    not in {"0", "false", "no", "n", "off"}
)
FORCE_DELETE = (
    os.getenv("FORCE_DELETE", "").strip().lower()
    in {"1", "true", "yes", "y", "on"}
)

ENTRY_IDS = get_env_csv("ENTRY_IDS")

# Support for CSV-based entry ID selection
CSV_FILENAME = os.getenv("CSV_FILENAME", "").strip()
ENTRY_ID_COLUMN_HEADER = os.getenv("ENTRY_ID_COLUMN_HEADER", "").strip()


# =============================================================================
# Kaltura session helpers -----------------------------------------------------
# =============================================================================

def build_client() -> KalturaClient:
    config = KalturaConfiguration(PARTNER_ID)
    config.serviceUrl = SERVICE_URL
    client = KalturaClient(config)
    ks = client.session.start(
        ADMIN_SECRET,
        USER_ID,
        KalturaSessionType.ADMIN,
        PARTNER_ID,
        privileges=PRIVILEGES,
    )
    client.setKs(ks)
    return client


_thread_local = threading.local()


def get_thread_ks() -> str:
    if not hasattr(_thread_local, "client"):
        _thread_local.client = build_client()
    return _thread_local.client.getKs()


# =============================================================================
# Raw API helpers -------------------------------------------------------------
# =============================================================================

def _api_url(action: str) -> str:
    return f"{SERVICE_URL}/api_v3/service/baseentry/action/{action}"


def _raw_call(
    action: str,
    entry_id: str,
    ks: str,
    force: bool = False,
) -> requests.Response:
    data: Dict = {"entryId": entry_id, "ks": ks}
    if force:
        data["force"] = "1"
    return requests.post(
        _api_url(action),
        data=data,
        timeout=(REQUEST_CONNECT_TIMEOUT_SEC, REQUEST_TIMEOUT_SEC),
    )


def _xml_field(text: str, tag: str) -> str:
    start = text.find(f"<{tag}>")
    end = text.find(f"</{tag}>")
    if start != -1 and end != -1 and end > start:
        return text[start + len(tag) + 2:end].strip()
    return ""


def _response_error(
    response: requests.Response,
) -> Tuple[Optional[str], str]:
    text = response.text or ""
    code = _xml_field(text, "code") or None
    message = _xml_field(text, "message") or text.strip()
    return code, message


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
            reader.fieldnames = [
                h.strip().strip('"') for h in reader.fieldnames
            ]
            for row in reader:
                eid = (row.get(ENTRY_ID_COLUMN_HEADER, "") or "").strip()
                if eid:
                    entry_ids.append(eid)
    except Exception as ex:
        print(
            f"[ERROR] Failed to load entry IDs from CSV: {csv_path}: {ex}",
            file=sys.stderr,
        )
        sys.exit(2)
    return entry_ids


# =============================================================================
# Worker functions ------------------------------------------------------------
# =============================================================================

def lookup_one(eid: str) -> Dict:
    not_found = {
        "entry_id": eid,
        "entry_name": "",
        "owner_user_id": "",
        "duration_seconds": "",
        "plays": "",
        "status": "NOT FOUND",
    }
    try:
        response = _raw_call("get", eid, get_thread_ks())
        code, message = _response_error(response)
        if code:
            print(
                f"[SKIPPED] Could not retrieve info for entry ID {eid}: "
                f"{message}"
            )
            return not_found
        text = response.text or ""
        return {
            "entry_id": eid,
            "entry_name": _xml_field(text, "name"),
            "owner_user_id": _xml_field(text, "userId"),
            "duration_seconds": _xml_field(text, "duration"),
            "plays": _xml_field(text, "plays"),
            "status": "FOUND",
        }
    except requests.RequestException as e:
        print(
            f"[SKIPPED] Could not retrieve info for entry ID {eid}: {e}"
        )
        return not_found


def action_one(
    row: Dict,
    action: str,
    action_log: str,
) -> Dict:
    eid = row["entry_id"]
    if row.get("status") != "FOUND":
        return row

    out = dict(row)
    try:
        response = _raw_call(action, eid, get_thread_ks(), force=FORCE_DELETE)
        code, message = _response_error(response)
        if code:
            print(
                f"[SKIPPED] Entry {eid} could not be {action_log.lower()} "
                f"({code}): {message}"
            )
            out["status"] = f"FAILED: {code}"
        else:
            print(f"[{action_log}] Entry {eid}")
            out["status"] = action_log
    except requests.RequestException as e:
        print(
            f"[SKIPPED] Entry {eid} could not be {action_log.lower()}: {e}"
        )
        out["status"] = "FAILED: connection error"
    return out


# =============================================================================
# Main ------------------------------------------------------------------------
# =============================================================================

TS = now_stamp()

# Ensure outputs go into an "output" subfolder alongside this script
OUTPUT_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "output"
)
os.makedirs(OUTPUT_DIR, exist_ok=True)

PREVIEW_CSV = os.path.join(OUTPUT_DIR, f"{TS}_deleted_entries_PREVIEW.csv")
RESULT_CSV = os.path.join(OUTPUT_DIR, f"{TS}_deleted_entries_RESULT.csv")

FIELDNAMES = [
    "entry_id", "entry_name", "owner_user_id",
    "duration_seconds", "plays", "status",
]

# Get entry IDs from .csv or .env file ----------------------------------------
if CSV_FILENAME:
    entry_ids = load_entry_ids_from_csv()
elif ENTRY_IDS:
    entry_ids = ENTRY_IDS
else:
    print(
        "\n[ERROR] No valid ENTRY_IDS or CSV_FILENAME or "
        "ENTRY_ID_COLUMN_HEADER env variables. Exiting."
    )
    exit()

# Lookup phase ----------------------------------------------------------------
total = len(entry_ids)

if LOOKUP_BEFORE_ACTION:
    print(
        f"\n[INFO] Looking up {total} entries "
        f"(MAX_WORKERS={MAX_WORKERS})..."
    )

    report_by_id: Dict[str, Dict] = {}
    completed = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(lookup_one, eid): eid for eid in entry_ids}
        for fut in as_completed(futures):
            result = fut.result()
            report_by_id[result["entry_id"]] = result
            completed += 1
            if completed % 100 == 0 or completed == total:
                print(f"  {completed}/{total} looked up...")

    # Preserve original CSV order
    report = [report_by_id[eid] for eid in entry_ids if eid in report_by_id]

    if all(r["status"] != "FOUND" for r in report):
        print("\n[INFO] No valid entries to delete. Exiting.")
        with open(PREVIEW_CSV, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
            writer.writeheader()
            writer.writerows(report)
        exit()

    # Write preview CSV
    with open(PREVIEW_CSV, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(report)
    print(f"\n[INFO] Wrote preview to {PREVIEW_CSV}")
else:
    print("\n[INFO] Skipping lookup phase (LOOKUP_BEFORE_ACTION=false).")
    report = [
        {
            "entry_id": eid,
            "entry_name": "",
            "owner_user_id": "",
            "duration_seconds": "",
            "plays": "",
            "status": "FOUND",
        }
        for eid in entry_ids
    ]

# Dry run: write result CSV and exit without touching anything ----------------
if DRY_RUN:
    print("\n[DRY RUN] No entries will be deleted or recycled.")
    for row in report:
        if row.get("status") == "FOUND":
            row["status"] = "DRY RUN"
    with open(RESULT_CSV, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(report)
    print(f"[DRY RUN] Wrote result to {RESULT_CSV}")
    exit()

# Confirm and delete ----------------------------------------------------------
confirm = input(
    "\nType 'DELETE' to permanently delete these entries "
    "or 'RECYCLE' to put them in the owner's recycle bin: "
)
match confirm.strip().upper():
    case "DELETE":
        action_log = "DELETED"
        action = "delete"
    case "RECYCLE":
        action_log = "RECYCLED"
        action = "recycle"
    case _:
        print(
            f"[ABORTED] No entries deleted or recycled. "
            f"Unknown action: {confirm.strip().upper()}"
        )
        exit()

# Action phase ----------------------------------------------------------------
found_rows = [r for r in report if r.get("status") == "FOUND"]
skipped_rows = [r for r in report if r.get("status") != "FOUND"]
total_found = len(found_rows)
print(f"\n[INFO] Running {action_log} on {total_found} entries...")
print(f"[INFO] Writing results incrementally to {RESULT_CSV}")

# Write header and any skipped (NOT FOUND) rows upfront
with open(RESULT_CSV, mode="w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
    writer.writeheader()
    writer.writerows(skipped_rows)

write_lock = threading.Lock()
completed = 0
processed_count = 0

with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
    futures = {
        pool.submit(action_one, row, action, action_log): row["entry_id"]
        for row in found_rows
    }
    for fut in as_completed(futures):
        result = fut.result()
        completed += 1
        if result.get("status") == action_log:
            processed_count += 1
        with write_lock:
            with open(
                RESULT_CSV, mode="a", newline="", encoding="utf-8"
            ) as f:
                writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
                writer.writerow(result)
        if completed % 100 == 0 or completed == total_found:
            print(f"  {completed}/{total_found} processed...")

print(f"\n[INFO] {processed_count} entries successfully {action_log.lower()}.")
print(f"[INFO] Wrote report to {RESULT_CSV}")
