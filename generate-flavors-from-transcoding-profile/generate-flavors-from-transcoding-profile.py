#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Generate Flavors from Transcoding Profile

For a set of entries, generates any flavor assets defined in a given
transcoding profile that are missing or in an error state.

Flavor params already in an active state (READY, QUEUED, CONVERTING,
WAIT_FOR_CONVERT, IMPORTING, VALIDATING, EXPORTING) are skipped.
Flavor params in ERROR or NOT_APPLICABLE, and those with no existing asset,
are queued for conversion.

Writes a preview CSV before converting, then a results CSV after.
"""

import csv
import os
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from dotenv import load_dotenv, find_dotenv

from KalturaClient import KalturaClient, KalturaConfiguration
from KalturaClient.Plugins.Core import (
    KalturaSessionType,
    KalturaFilterPager,
    KalturaMediaEntryFilter,
    KalturaFlavorAssetFilter,
    KalturaConversionProfileAssetParamsFilter,
)
from KalturaClient.exceptions import KalturaException

# =============================================================================
# Env / config ----------------------------------------------------------------
# =============================================================================
load_dotenv(find_dotenv())


def require_env(name: str) -> str:
    val = os.getenv(name, "").strip()
    if not val:
        print(f"[ERROR] Missing {name} in .env", file=sys.stderr)
        sys.exit(2)
    return val


def require_env_int(name: str) -> int:
    raw = os.getenv(name, "").strip()
    if not raw.isdigit():
        print(f"[ERROR] Missing or invalid {name} in .env", file=sys.stderr)
        sys.exit(2)
    return int(raw)


def get_env_csv(name: str) -> List[str]:
    raw = os.getenv(name, "") or ""
    return [p.strip() for p in raw.split(",") if p.strip()]


def now_stamp() -> str:
    return datetime.now().strftime("%Y-%m-%d-%H%M")


PARTNER_ID = require_env_int("PARTNER_ID")
ADMIN_SECRET = require_env("ADMIN_SECRET")
USER_ID = os.getenv("USER_ID", "").strip()
SERVICE_URL = os.getenv("SERVICE_URL", "https://www.kaltura.com").rstrip("/")
PRIVILEGES = os.getenv("PRIVILEGES", "all:*,disableentitlement")
TRANSCODING_PROFILE_ID = require_env_int("TRANSCODING_PROFILE_ID")
MAX_WORKERS = max(1, int(os.getenv("MAX_WORKERS", "5")))

ENTRY_IDS = get_env_csv("ENTRY_IDS")
TAGS = get_env_csv("TAGS")
CATEGORY_IDS = get_env_csv("CATEGORY_IDS")

CSV_FILENAME = os.getenv("CSV_FILENAME", "").strip()
ENTRY_ID_COLUMN_HEADER = os.getenv("ENTRY_ID_COLUMN_HEADER", "").strip()

# Populated by load_entry_ids_from_csv()
CSV_ORIGINAL_ROWS: Dict[str, Dict[str, str]] = {}
CSV_ORIGINAL_FIELDNAMES: List[str] = []

# Flavor asset statuses that mean "already handled — skip"
# (READY=2, QUEUED=0, CONVERTING=1, WAIT_FOR_CONVERT=6, IMPORTING=7,
#  VALIDATING=8, EXPORTING=9)
_SKIP_STATUSES = {0, 1, 2, 6, 7, 8, 9}

# =============================================================================
# Output paths ----------------------------------------------------------------
# =============================================================================
TS = now_stamp()
REPORTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output")
os.makedirs(REPORTS_DIR, exist_ok=True)

PREVIEW_CSV = os.path.join(REPORTS_DIR, f"{TS}_generate_flavors_PREVIEW.csv")
RESULT_CSV = os.path.join(REPORTS_DIR, f"{TS}_generate_flavors_RESULT.csv")

# =============================================================================
# Kaltura client --------------------------------------------------------------
# =============================================================================
# NOTE: The Kaltura client is not thread-safe. All API calls must be made
# while holding _CLIENT_LOCK to prevent concurrent access.
_CLIENT_LOCK = threading.Lock()

cfg = KalturaConfiguration(PARTNER_ID)
cfg.serviceUrl = SERVICE_URL
client = KalturaClient(cfg)
ks = client.session.start(
    ADMIN_SECRET, USER_ID, KalturaSessionType.ADMIN, PARTNER_ID,
    privileges=PRIVILEGES,
)
client.setKs(ks)

# =============================================================================
# CSV helpers -----------------------------------------------------------------
# =============================================================================

SCRIPT_FIELDNAMES = [
    "entry_id", "entry_name", "owner_user_id", "existing_conversion_profile_id",
    "transcoding_profile_id", "transcoding_profile_name",
    "profile_flavor_params_ids", "flavors_to_generate", "flavors_to_generate_count",
    "flavors_skipped", "flavors_skipped_count",
    "flavors_generated_count", "status", "error",
]


def write_csv(path: str, rows: List[Dict], extra_fieldnames: Optional[List[str]] = None):
    fieldnames: List[str] = list(extra_fieldnames or [])
    for fn in SCRIPT_FIELDNAMES:
        if fn not in fieldnames:
            fieldnames.append(fn)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore", restval="")
        w.writeheader()
        w.writerows(rows)


# =============================================================================
# CSV entry ID loading --------------------------------------------------------
# =============================================================================

def load_entry_ids_from_csv() -> List[str]:
    global CSV_ORIGINAL_ROWS, CSV_ORIGINAL_FIELDNAMES
    if not CSV_FILENAME or not ENTRY_ID_COLUMN_HEADER:
        return []
    script_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(script_dir, CSV_FILENAME)
    entry_ids = []
    try:
        with open(csv_path, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            reader.fieldnames = [h.strip().strip('"') for h in reader.fieldnames]
            CSV_ORIGINAL_FIELDNAMES = list(reader.fieldnames)
            for row in reader:
                eid = (row.get(ENTRY_ID_COLUMN_HEADER, "") or "").strip()
                if eid:
                    entry_ids.append(eid)
                    CSV_ORIGINAL_ROWS[eid] = dict(row)
    except Exception as ex:
        print(f"[ERROR] Failed to load entry IDs from CSV: {csv_path}: {ex}", file=sys.stderr)
        sys.exit(2)
    return entry_ids


# =============================================================================
# Kaltura API helpers ---------------------------------------------------------
# =============================================================================

def get_profile_flavor_params_ids(profile_id: int) -> List[int]:
    """Return all assetParamsIds defined in the given conversion profile."""
    f = KalturaConversionProfileAssetParamsFilter()
    f.conversionProfileIdEqual = profile_id
    pager = KalturaFilterPager(pageSize=500, pageIndex=1)
    params_ids = []
    while True:
        with _CLIENT_LOCK:
            resp = client.conversionProfileAssetParams.list(f, pager)
        if not resp or not getattr(resp, "objects", None):
            break
        for obj in resp.objects:
            aid = getattr(obj, "assetParamsId", None)
            if aid is not None:
                params_ids.append(int(aid))
        if len(resp.objects) < pager.pageSize:
            break
        pager.pageIndex += 1
    return params_ids


def list_flavors(entry_id: str) -> list:
    ff = KalturaFlavorAssetFilter()
    ff.entryIdEqual = entry_id
    pager = KalturaFilterPager(pageSize=500, pageIndex=1)
    flavors = []
    while True:
        with _CLIENT_LOCK:
            resp = client.flavorAsset.list(ff, pager)
        if not resp or not getattr(resp, "objects", None):
            break
        flavors.extend(resp.objects)
        if len(resp.objects) < pager.pageSize:
            break
        pager.pageIndex += 1
    return flavors


def iter_selected_entries() -> List:
    selected = []

    if CSV_FILENAME and ENTRY_ID_COLUMN_HEADER:
        ids = load_entry_ids_from_csv()
        print(f"[INFO] CSV mode: {len(ids)} entry IDs from '{CSV_FILENAME}' "
              f"(column '{ENTRY_ID_COLUMN_HEADER}').")
        ignored = [name for name, val in [
            ("ENTRY_IDS", ENTRY_IDS),
            ("TAGS", TAGS),
            ("CATEGORY_IDS", CATEGORY_IDS),
        ] if val]
        if ignored:
            print(f"[WARN] CSV mode is active. The following .env settings are set "
                  f"but will be ignored: {', '.join(ignored)}")
        for eid in ids:
            try:
                with _CLIENT_LOCK:
                    selected.append(client.media.get(eid))
            except Exception as ex:
                print(f"[WARN] media.get failed for {eid}: {ex}")
        return selected

    if ENTRY_IDS:
        for eid in ENTRY_IDS:
            try:
                with _CLIENT_LOCK:
                    selected.append(client.media.get(eid))
            except Exception as ex:
                print(f"[WARN] media.get failed for {eid}: {ex}")
        return selected

    f = KalturaMediaEntryFilter()
    if TAGS:
        f.tagsMultiLikeOr = ",".join(TAGS)
    if CATEGORY_IDS:
        f.categoriesIdsMatchOr = ",".join(CATEGORY_IDS)

    pager = KalturaFilterPager(pageSize=500, pageIndex=1)
    page = 0
    while True:
        page += 1
        try:
            with _CLIENT_LOCK:
                resp = client.media.list(f, pager)
        except KalturaException as ex:
            print(f"[ERROR] media.list failed on page {page}: {ex}")
            break
        if not resp or not resp.objects:
            break
        selected.extend(resp.objects)
        if len(resp.objects) < pager.pageSize:
            break
        pager.pageIndex += 1

    return selected


# =============================================================================
# Per-entry preview logic -----------------------------------------------------
# =============================================================================

def _status_label(status_int: int) -> str:
    labels = {
        -1: "ERROR", 0: "QUEUED", 1: "CONVERTING", 2: "READY",
        3: "DELETED", 4: "NOT_APPLICABLE", 5: "TEMP",
        6: "WAIT_FOR_CONVERT", 7: "IMPORTING", 8: "VALIDATING", 9: "EXPORTING",
    }
    return labels.get(status_int, str(status_int))


def build_preview_row(
    entry,
    profile_params_ids: List[int],
    profile_name: str,
) -> Dict:
    entry_id = getattr(entry, "id", "")
    name = getattr(entry, "name", "")
    owner = getattr(entry, "userId", "")
    existing_conv = getattr(entry, "conversionProfileId", "")
    original = CSV_ORIGINAL_ROWS.get(entry_id, {})

    base = {
        **original,
        "entry_id": entry_id,
        "entry_name": name,
        "owner_user_id": owner,
        "existing_conversion_profile_id": str(existing_conv),
        "transcoding_profile_id": str(TRANSCODING_PROFILE_ID),
        "transcoding_profile_name": profile_name,
        "profile_flavor_params_ids": ",".join(str(x) for x in profile_params_ids),
        "flavors_to_generate": "",
        "flavors_to_generate_count": "0",
        "flavors_skipped": "",
        "flavors_skipped_count": "0",
        "flavors_generated_count": "0",
        "status": "",
        "error": "",
    }

    try:
        existing_flavors = list_flavors(entry_id)
    except Exception as ex:
        base["status"] = "ERROR"
        base["error"] = f"flavorAsset.list failed: {ex}"
        return base

    # Build map: flavorParamsId -> status int for existing assets
    existing_map: Dict[int, int] = {}
    for fa in existing_flavors:
        fp_id = getattr(fa, "flavorParamsId", None)
        status = getattr(fa, "status", None)
        if fp_id is not None:
            status_val = int(status.value) if hasattr(status, "value") else int(status or -99)
            existing_map[int(fp_id)] = status_val

    to_generate: List[int] = []
    skipped: List[Tuple[int, str]] = []

    for fp_id in profile_params_ids:
        if fp_id not in existing_map:
            to_generate.append(fp_id)
        elif existing_map[fp_id] in _SKIP_STATUSES:
            skipped.append((fp_id, _status_label(existing_map[fp_id])))
        else:
            # ERROR, NOT_APPLICABLE, DELETED, or unknown — attempt to (re)generate
            to_generate.append(fp_id)

    base["flavors_to_generate"] = ",".join(str(x) for x in to_generate)
    base["flavors_to_generate_count"] = str(len(to_generate))
    base["flavors_skipped"] = ",".join(f"{fid}({lbl})" for fid, lbl in skipped)
    base["flavors_skipped_count"] = str(len(skipped))
    base["status"] = "READY" if to_generate else "SKIPPED_ALL_PRESENT"

    return base


# =============================================================================
# Per-entry conversion worker -------------------------------------------------
# =============================================================================

def convert_entry_flavors(row: Dict) -> Dict:
    """Convert all flagged flavor params for a single entry. Returns updated row."""
    entry_id = row["entry_id"]
    fp_ids = [x.strip() for x in row["flavors_to_generate"].split(",") if x.strip()]
    generated = 0
    errors = []

    for fp_id_str in fp_ids:
        try:
            with _CLIENT_LOCK:
                client.flavorAsset.convert(entry_id, int(fp_id_str))
            generated += 1
            print(f"[QUEUED] entry={entry_id} flavorParamsId={fp_id_str}", flush=True)
        except KalturaException as ex:
            errors.append(f"flavorParamsId={fp_id_str}: {ex}")

    rr = dict(row)
    rr["flavors_generated_count"] = str(generated)
    if errors and generated == 0:
        rr["status"] = "FAILED"
    elif errors:
        rr["status"] = "PARTIAL"
    else:
        rr["status"] = "CONVERTED"
    rr["error"] = "; ".join(errors)
    return rr


# =============================================================================
# Main ------------------------------------------------------------------------
# =============================================================================

def main():
    # Validate transcoding profile
    print(f"[INFO] Fetching transcoding profile {TRANSCODING_PROFILE_ID} …")
    try:
        profile = client.conversionProfile.get(TRANSCODING_PROFILE_ID)
    except KalturaException as ex:
        print(f"[ERROR] conversionProfile.get failed: {ex}", file=sys.stderr)
        sys.exit(1)
    profile_name = getattr(profile, "name", "") or ""
    print(f"[INFO] Profile: '{profile_name}' (ID {TRANSCODING_PROFILE_ID})")

    # Get flavor params IDs for this profile
    print("[INFO] Fetching flavor params for profile …")
    profile_params_ids = get_profile_flavor_params_ids(TRANSCODING_PROFILE_ID)
    if not profile_params_ids:
        print("[ERROR] No flavor params found for this transcoding profile. Exiting.")
        sys.exit(1)
    print(f"[INFO] Profile defines {len(profile_params_ids)} flavor param(s): "
          f"{profile_params_ids}")

    # Select entries
    print("[INFO] Selecting entries …")
    entries = iter_selected_entries()
    print(f"[INFO] Found {len(entries)} candidate entries")

    if not entries:
        print("[INFO] No entries to process. Exiting.")
        return

    # Build preview (parallelized)
    print(f"[INFO] Building preview with {MAX_WORKERS} worker(s) …")
    preview_rows: List[Dict] = [None] * len(entries)  # type: ignore[list-item]

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_index = {
            executor.submit(build_preview_row, e, profile_params_ids, profile_name): i
            for i, e in enumerate(entries)
        }
        completed = 0
        total = len(future_to_index)
        for fut in as_completed(future_to_index):
            idx = future_to_index[fut]
            preview_rows[idx] = fut.result()
            completed += 1
            if total >= 25 and (completed % 25 == 0 or completed == total):
                print(f"[INFO] Preview progress: {completed}/{total}", flush=True)

    write_csv(PREVIEW_CSV, preview_rows, CSV_ORIGINAL_FIELDNAMES)
    print(f"[INFO] Wrote preview → {PREVIEW_CSV}")

    ready = [r for r in preview_rows if r["status"] == "READY"]
    if not ready:
        print("[INFO] All flavors already present/in-progress for all entries. Exiting.")
        return

    total_to_generate = sum(int(r["flavors_to_generate_count"]) for r in ready)
    print(f"\n[PLAN] Entries requiring conversion: {len(ready)} "
          f"| Flavor assets to generate: {total_to_generate} "
          f"| Workers: {MAX_WORKERS}")

    confirm = input(
        "\nType 'CONVERT' to queue flavor generation for the listed entries: "
    ).strip().upper()
    if confirm != "CONVERT":
        print("[ABORTED] No conversions performed.")
        return

    # Execute conversions (parallelized)
    # Build a map so results can be merged back into preview_rows order
    result_by_entry: Dict[str, Dict] = {}

    ready_rows = [r for r in preview_rows if r["status"] == "READY" and r["flavors_to_generate"]]
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_entry = {
            executor.submit(convert_entry_flavors, r): r["entry_id"]
            for r in ready_rows
        }
        completed = 0
        total = len(future_to_entry)
        for fut in as_completed(future_to_entry):
            entry_id = future_to_entry[fut]
            result_by_entry[entry_id] = fut.result()
            completed += 1
            if total >= 25 and (completed % 25 == 0 or completed == total):
                print(f"[INFO] Conversion progress: {completed}/{total}", flush=True)

    # Reconstruct result rows in original order
    result_rows: List[Dict] = []
    for r in preview_rows:
        if r["entry_id"] in result_by_entry:
            result_rows.append(result_by_entry[r["entry_id"]])
        else:
            result_rows.append(r)

    write_csv(RESULT_CSV, result_rows, CSV_ORIGINAL_FIELDNAMES)
    print(f"\n[INFO] Wrote results → {RESULT_CSV}")
    print("[DONE]")


if __name__ == "__main__":
    main()
