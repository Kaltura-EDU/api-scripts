#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Delete Non-Source Flavors - safer, flexible bulk tool.

Deletes all non-source flavors for selected entries, preserving the source
flavor (using isOriginal, tags, or largest file fallback) and any flavor
params listed in ADDITIONAL_FLAVORS_TO_KEEP. Skips single-flavor entries,
handles multi-stream children, and writes preview/results CSV reports to the
output/ folder.

Configuration is managed through a .env file next to this script (see
README and .env.example). For security, the admin secret is NOT read from
.env; the script prompts for it at runtime so it is never stored on disk.

- MAX_WORKERS controls concurrency for both the lookup and deletion phases.
- PROGRESS_EVERY controls how often progress is printed (default every 100).
- REQUEST_TIMEOUT, MAX_NETWORK_RETRIES and NETWORK_RETRY_DELAY control how
  network timeouts and dropped connections are retried.
"""

import csv
import getpass
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv

from KalturaClient import KalturaClient, KalturaConfiguration
from KalturaClient.Plugins.Core import (
    KalturaSessionType, KalturaFilterPager, KalturaMediaEntryFilter,
    KalturaFlavorAssetFilter
)

from KalturaClient.exceptions import KalturaClientException, KalturaException

# =============================================================================
# Env / config ----------------------------------------------------------------
# =============================================================================
# Load .env alongside this script, not relying on current working directory
load_dotenv(dotenv_path=Path(__file__).with_name(".env"), override=False)


def env_value(*names: str) -> str:
    """First non-empty value among names. Later names are pre-2.0 spellings
    of the same setting, still honored so older .env files keep working."""
    for name in names:
        value = os.getenv(name, "").strip()
        if value:
            return value
    return ""


def get_env_csv(*names: str) -> List[str]:
    return [p.strip() for p in env_value(*names).split(",") if p.strip()]


def env_int(default: int, *names: str) -> int:
    raw = env_value(*names)
    try:
        return int(raw) if raw else default
    except ValueError:
        print(f"[ERROR] {names[0]} in .env must be a whole number.",
              file=sys.stderr)
        sys.exit(2)


_FALSY = {"0", "false", "no", "off", ""}


def env_bool(name: str, default: str) -> bool:
    return os.getenv(name, default).strip().lower() not in _FALSY


# NOTE: the admin secret is intentionally NOT read from .env. It is prompted
# for interactively at runtime (see main) so it is never stored on disk.
PARTNER_ID = env_value("KALTURA_PARTNER_ID", "PARTNER_ID")
KALTURA_USER = env_value("KALTURA_USER", "USER_ID") or "admin"
SERVICE_URL = (
    env_value("KALTURA_SERVICE_URL", "SERVICE_URL")
    or "https://www.kaltura.com"
).rstrip("/")
PRIVILEGES = "all:*,disableentitlement"

ADDITIONAL_FLAVORS_TO_KEEP = get_env_csv("ADDITIONAL_FLAVORS_TO_KEEP")

# Which entries to process. INPUT_FILENAME (a CSV in input/) wins, then
# ENTRY_IDS; TAGS and CATEGORY_IDS are combined (an entry must match both
# when both are set). Leave all blank to be asked.
INPUT_FILENAME = env_value("INPUT_FILENAME", "CSV_FILENAME", "CSV_FILE")
COLUMN_HEADER_ENTRY_ID = (
    env_value("COLUMN_HEADER_ENTRY_ID", "ENTRY_ID_COLUMN_HEADER")
    or "Entry ID"
)
ENTRY_IDS = get_env_csv("ENTRY_IDS")
TAGS = get_env_csv("TAGS")
CATEGORY_IDS = get_env_csv("CATEGORY_IDS")

# ---------- Reliability knobs (from .env) ----------
# Seconds before an individual API request times out.
REQUEST_TIMEOUT = env_int(120, "REQUEST_TIMEOUT", "REQUEST_TIMEOUT_SEC")
# Retries for transient network errors before giving up.
MAX_NETWORK_RETRIES = max(1, env_int(5, "MAX_NETWORK_RETRIES"))
# Base seconds between retries (grows linearly: delay × attempt).
NETWORK_RETRY_DELAY = env_int(5, "NETWORK_RETRY_DELAY")

MAX_WORKERS = max(1, env_int(10, "MAX_WORKERS"))
PROGRESS_EVERY = max(1, env_int(100, "PROGRESS_EVERY"))

# When true (default), skip media.get calls and use a stub with just the entry
# ID. Faster for large runs where you already know the entries are valid.
SKIP_ENTRY_METADATA = env_bool("SKIP_ENTRY_METADATA", "true")
# When true, skip the interactive "Type DELETE" confirmation prompt.
# Safe for unattended runs once you trust the preview output.
SKIP_CONFIRMATION = env_bool("SKIP_CONFIRMATION", "false")

# Populated by load_entry_ids_from_csv() when CSV mode is active.
# Maps entry_id -> original row dict so output CSVs can preserve input columns.
CSV_ORIGINAL_ROWS: Dict[str, Dict[str, str]] = {}
CSV_ORIGINAL_FIELDNAMES: List[str] = []

SCRIPT_DIR = Path(__file__).resolve().parent
INPUT_DIR = SCRIPT_DIR / "input"
OUTPUT_DIR = SCRIPT_DIR / "output"

TS = datetime.now().strftime("%Y-%m-%d-%H%M")
PREVIEW_CSV = OUTPUT_DIR / f"{TS}_deleted_flavors_PREVIEW.csv"
RESULT_CSV = OUTPUT_DIR / f"{TS}_deleted_flavors_RESULT.csv"


# =============================================================================
# Kaltura client helpers ------------------------------------------------------
# =============================================================================

def call_with_retry(fn, *args, **kwargs):
    """Call fn(*args, **kwargs), retrying with linear backoff on transient
    network failures (timeouts, connection resets). Kaltura raises those as
    KalturaClientException, which is NOT a subclass of KalturaException, so
    it is caught here separately; requests' own errors are covered too. A
    real, well-formed API error (KalturaException) is never retried here —
    callers handle those."""
    for attempt in range(1, MAX_NETWORK_RETRIES + 1):
        try:
            return fn(*args, **kwargs)
        except (
            KalturaClientException,
            requests.exceptions.RequestException,
        ) as exc:
            if attempt == MAX_NETWORK_RETRIES:
                raise
            delay = NETWORK_RETRY_DELAY * attempt
            print(
                f"    [network error: {exc}; retry "
                f"{attempt}/{MAX_NETWORK_RETRIES} in {delay}s]"
            )
            time.sleep(delay)


def make_client() -> KalturaClient:
    cfg = KalturaConfiguration(int(PARTNER_ID))
    cfg.serviceUrl = SERVICE_URL
    cfg.requestTimeout = REQUEST_TIMEOUT
    return KalturaClient(cfg)


def start_session(admin_secret: str) -> str:
    """Start one admin session and return its KS. Worker threads reuse it,
    so the admin secret never leaves main()."""
    client = make_client()
    try:
        return call_with_retry(
            client.session.start,
            admin_secret, KALTURA_USER, KalturaSessionType.ADMIN,
            int(PARTNER_ID), privileges=PRIVILEGES,
        )
    except Exception as e:
        if getattr(e, "code", "") == "START_SESSION_ERROR":
            print(
                "\n❌ Could not log in to Kaltura. Partner ID "
                f"[{PARTNER_ID}] and the Admin Secret were not accepted.\n"
                "   Double-check both values — the secret must be the "
                "Administrator secret (not the User secret),\n"
                "   copied exactly from KMC → Settings → "
                "Integration Settings.\n"
            )
        elif type(e).__name__ == "KalturaClientException":
            print(
                "\n❌ Could not reach Kaltura to start a session.\n"
                f"   {e}\n   Check your internet connection and try again.\n"
            )
        else:
            print(f"\n❌ Could not start Kaltura session: {e}\n")
        raise SystemExit(1)


KS = ""
_thread_local = threading.local()


def get_thread_client() -> KalturaClient:
    # One client per thread, all sharing the session started in main().
    if not hasattr(_thread_local, "client"):
        client = make_client()
        client.setKs(KS)
        _thread_local.client = client
    return _thread_local.client


# =============================================================================
# Helper for loading entry IDs from CSV ---------------------------------------
# =============================================================================

def resolve_input_path(filename: str) -> Path:
    # INPUT_FILENAME is a file inside input/. A leading "input/" is accepted
    # too, since older .env files used that form.
    path = Path(filename)
    if path.parts and path.parts[0] == "input":
        path = Path(*path.parts[1:])
    return INPUT_DIR / path


def load_entry_ids_from_csv(filename: str) -> List[str]:
    """
    Loads entry IDs from the COLUMN_HEADER_ENTRY_ID column of a CSV in the
    input folder (matched case-insensitively; a one-column CSV works whatever
    its header). Returns the non-empty, de-duplicated entry IDs in file order.
    Populates CSV_ORIGINAL_ROWS and CSV_ORIGINAL_FIELDNAMES as a side effect.
    """
    global CSV_ORIGINAL_FIELDNAMES
    csv_path = resolve_input_path(filename)
    if not csv_path.is_file():
        print(
            f"[ERROR] Input file not found: {csv_path}\n"
            "        Put your CSV in the input folder next to this script "
            "and check INPUT_FILENAME.",
            file=sys.stderr,
        )
        sys.exit(2)

    entry_ids = []
    try:
        with open(csv_path, "r", newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            # Normalize headers: strip surrounding quotes and whitespace
            reader.fieldnames = [
                h.strip().strip('"') for h in (reader.fieldnames or [])
            ]
            CSV_ORIGINAL_FIELDNAMES = list(reader.fieldnames)
            column = next(
                (h for h in reader.fieldnames
                 if h.lower() == COLUMN_HEADER_ENTRY_ID.lower()),
                reader.fieldnames[0] if len(reader.fieldnames) == 1 else None
            )
            if column is None:
                print(
                    f"[ERROR] No \"{COLUMN_HEADER_ENTRY_ID}\" column in "
                    f"{csv_path.name}. Columns found: "
                    f"{', '.join(reader.fieldnames) or '(none)'}\n"
                    "        Set COLUMN_HEADER_ENTRY_ID in .env to the right "
                    "column.",
                    file=sys.stderr,
                )
                sys.exit(2)
            for row in reader:
                eid = (row.get(column, "") or "").strip()
                if eid and eid not in CSV_ORIGINAL_ROWS:
                    entry_ids.append(eid)
                    CSV_ORIGINAL_ROWS[eid] = dict(row)
    except (OSError, csv.Error, UnicodeDecodeError) as ex:
        print(
            f"[ERROR] Failed to load entry IDs from CSV: {csv_path}: {ex}",
            file=sys.stderr,
        )
        sys.exit(2)
    return entry_ids


# =============================================================================
# Utilities -------------------------------------------------------------------
# =============================================================================

def _as_int(x) -> int:
    try:
        return int(x)
    except Exception:
        try:
            # size sometimes returned as string
            return int(str(x).strip())
        except Exception:
            return 0


def pick_source_flavor(flavor_objects) -> Tuple[Optional[str], Optional[str]]:
    """
    Return (source_flavor_id, reason) where reason in
    {'isOriginal','tags:source','largest'}.
    If none can be determined, returns (None, None).
    """
    if not flavor_objects:
        return None, None

    # 1) isOriginal == True
    for fa in flavor_objects:
        if getattr(fa, "isOriginal", False) is True:
            return getattr(fa, "id", None), "isOriginal"

    # 2) tags contains 'source'
    for fa in flavor_objects:
        tags = getattr(fa, "tags", "")
        if isinstance(tags, str) and "source" in tags.lower():
            return getattr(fa, "id", None), "tags:source"

    # 3) largest by size (fallback — review preview CSV before confirming)
    max_fa = None
    max_size = -1
    for fa in flavor_objects:
        size_b = _as_int(getattr(fa, "size", 0))
        if size_b > max_size:
            max_size = size_b
            max_fa = fa
    if max_fa is not None:
        print(
            f"[WARN] No isOriginal or 'source' tag found; "
            f"assuming largest flavor "
            f"({getattr(max_fa, 'id', '?')}) is source. "
            f"Verify in preview CSV before confirming."
        )
        return getattr(max_fa, "id", None), "largest"

    return None, None


def _list_all(list_fn, list_filter) -> List:
    """Page through a list call. Errors propagate: a partial flavor list
    must never be mistaken for the whole list."""
    pager = KalturaFilterPager(pageSize=500, pageIndex=1)
    objects = []
    while True:
        resp = call_with_retry(list_fn, list_filter, pager)
        if not resp or not getattr(resp, "objects", None):
            break
        objects.extend(resp.objects)
        if len(resp.objects) < pager.pageSize:
            break
        pager.pageIndex += 1
    return objects


def list_flavors(entry_id: str):
    ff = KalturaFlavorAssetFilter()
    ff.entryIdEqual = entry_id
    return _list_all(get_thread_client().flavorAsset.list, ff)


def list_children(entry_id: str):
    """
    Return a list of child entries (multi-stream components) for a given
    parent entry. Uses media.list with parentEntryIdEqual.
    """
    mf = KalturaMediaEntryFilter()
    mf.parentEntryIdEqual = entry_id
    return _list_all(get_thread_client().media.list, mf)


def search_entries() -> List:
    """Return entry objects matching TAGS and/or CATEGORY_IDS."""
    f = KalturaMediaEntryFilter()
    if TAGS:
        f.tagsMultiLikeOr = ",".join(TAGS)
    if CATEGORY_IDS:
        f.categoriesIdsMatchOr = ",".join(CATEGORY_IDS)
    try:
        return _list_all(get_thread_client().media.list, f)
    except (KalturaException, KalturaClientException) as ex:
        print(f"[ERROR] media.list failed: {ex}", file=sys.stderr)
        sys.exit(1)


SCRIPT_FIELDNAMES = [
    "role", "entry_id", "parent_entry_id", "entry_name",
    "owner_user_id", "conversion_profile_id", "total_flavors",
    "source_flavor_id", "source_reason", "flavors_to_delete",
    "flavors_deleted_count", "kilobytes_saved", "is_multistream",
    "child_count", "status", "error",
]


def _all_fieldnames(extra: Optional[List[str]] = None) -> List[str]:
    fieldnames: List[str] = list(extra or [])
    for fn in SCRIPT_FIELDNAMES:
        if fn not in fieldnames:
            fieldnames.append(fn)
    return fieldnames


def _write_result_header(
    path: Path,
    extra_fieldnames: Optional[List[str]] = None,
) -> List[str]:
    fieldnames = _all_fieldnames(extra_fieldnames)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames,
                           extrasaction="ignore", restval="")
        w.writeheader()
    return fieldnames


def _append_result_row(
    path: Path,
    fieldnames: List[str],
    row: Dict[str, str],
    lock: threading.Lock,
) -> None:
    with lock:
        with open(path, "a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames,
                               extrasaction="ignore", restval="")
            w.writerow(row)


def make_row(role: str, e, parent_id: str = "", **fields) -> Dict[str, str]:
    """A report row with defaults filled in; fields override them."""
    entry_id = getattr(e, "id", "")
    row = {
        "role": role,
        "entry_id": entry_id,
        "parent_entry_id": parent_id,
        "entry_name": getattr(e, "name", "") or "",
        "owner_user_id": getattr(e, "userId", "") or "",
        "conversion_profile_id": getattr(e, "conversionProfileId", "") or "",
        "total_flavors": "0",
        "source_flavor_id": "",
        "source_reason": "",
        "flavors_to_delete": "",
        "flavors_deleted_count": "0",
        "kilobytes_saved": "0",
        "is_multistream": "",
        "child_count": "",
        "status": "",
        "error": "",
    }
    row.update(fields)
    # Seed parent rows with original CSV row data (if CSV mode).
    if role == "PARENT":
        return {**CSV_ORIGINAL_ROWS.get(entry_id, {}), **row}
    return row


def plan_entry(role: str, e, parent_id: str = "",
               **extra) -> Dict[str, str]:
    """List one entry's flavors and decide which to delete."""
    try:
        flavors = list_flavors(getattr(e, "id", ""))
    except (KalturaException, KalturaClientException,
            requests.exceptions.RequestException) as ex:
        return make_row(role, e, parent_id, status="ERROR",
                        error=f"flavorAsset.list failed: {ex}", **extra)

    total = len(flavors)
    if total <= 1:
        return make_row(role, e, parent_id, total_flavors=str(total),
                        status="SKIPPED_SINGLE_FLAVOR", **extra)

    src_id, src_reason = pick_source_flavor(flavors)
    if not src_id:
        return make_row(role, e, parent_id, total_flavors=str(total),
                        status="SKIPPED_NO_SOURCE_DETECTED", **extra)

    to_delete_ids: List[str] = []
    kb_saved = 0
    for fa in flavors:
        fa_id = getattr(fa, "id", "")
        fa_flavor_id = str(getattr(fa, "flavorParamsId", ""))
        if fa_id == src_id:
            continue
        if fa_flavor_id in ADDITIONAL_FLAVORS_TO_KEEP:
            continue
        to_delete_ids.append(fa_id)
        kb_saved += _as_int(getattr(fa, "size", 0))

    return make_row(
        role, e, parent_id,
        total_flavors=str(total),
        source_flavor_id=src_id,
        source_reason=src_reason or "",
        flavors_to_delete=",".join(to_delete_ids),
        flavors_deleted_count=str(len(to_delete_ids)),
        kilobytes_saved=str(kb_saved),
        status="READY",
        **extra,
    )


def build_preview_rows_for_entry(e) -> List[Dict[str, str]]:
    """
    Builds preview rows for a single parent entry, including any children
    (multi-stream). Returns a list of row dicts (parent first, then children).
    """
    parent_id = getattr(e, "id", "")
    child_error = ""
    try:
        children = list_children(parent_id)
    except (KalturaException, KalturaClientException,
            requests.exceptions.RequestException) as ex:
        print(f"[WARN] child lookup failed for {parent_id}: {ex}")
        children = []
        child_error = f"child lookup failed: {ex}"

    parent_row = plan_entry(
        "PARENT", e,
        is_multistream="UNKNOWN" if child_error else (
            "YES" if children else "NO"),
        child_count=str(len(children)),
    )
    if child_error:
        parent_row["error"] = "; ".join(
            x for x in (parent_row["error"], child_error) if x
        )

    rows = [parent_row]
    for c in children:
        rows.append(plan_entry("CHILD", c, parent_id))
    return rows


class _EntryStub:
    """Minimal stand-in for a media entry when SKIP_ENTRY_METADATA=true."""
    def __init__(self, entry_id: str) -> None:
        self.id = entry_id
        self.name = ""
        self.userId = ""
        self.conversionProfileId = ""


def process_entry_id(eid: str) -> List[Dict[str, str]]:
    """Worker: fetch entry metadata then build preview rows."""
    if SKIP_ENTRY_METADATA:
        return build_preview_rows_for_entry(_EntryStub(eid))
    try:
        e = call_with_retry(get_thread_client().media.get, eid)
    except Exception as ex:
        print(f"[WARN] media.get failed for {eid}: {ex}")
        return [make_row("PARENT", _EntryStub(eid), status="ERROR",
                         error=f"media.get failed: {ex}")]
    return build_preview_rows_for_entry(e)


def delete_row_flavors(
    row: Dict[str, str],
    result_csv: Path,
    fieldnames: List[str],
    write_lock: threading.Lock,
) -> Dict[str, str]:
    """Worker: delete all flagged flavors for one preview row."""
    if row["status"] != "READY" or not row["flavors_to_delete"]:
        _append_result_row(result_csv, fieldnames, row, write_lock)
        return row

    eid = row["entry_id"]
    client = get_thread_client()
    deleted_count = 0
    error = ""

    for flv in row["flavors_to_delete"].split(","):
        flv = flv.strip()
        if not flv:
            continue
        try:
            call_with_retry(client.flavorAsset.delete, flv)
            deleted_count += 1
            print(
                f"[DELETED] {flv} for entry {eid} "
                f"(role={row.get('role', '')}, "
                f"parent={row.get('parent_entry_id', '')})"
            )
        except (KalturaException, KalturaClientException,
                requests.exceptions.RequestException) as ex:
            print(f"[WARN] delete {flv} failed for entry {eid}: {ex}")
            error = f"{error}; delete {flv} failed: {ex}"

    if deleted_count == int(row["flavors_deleted_count"]) and not error:
        new_status = "DELETED"
    elif deleted_count > 0:
        new_status = "PARTIAL"
    else:
        new_status = "FAILED"

    rr = dict(row)
    rr["flavors_deleted_count"] = str(deleted_count)
    rr["status"] = new_status
    rr["error"] = error.strip("; ")

    _append_result_row(result_csv, fieldnames, rr, write_lock)
    return rr


# =============================================================================
# Entry selection -------------------------------------------------------------
# =============================================================================

def choose_selection() -> Tuple[str, List[str]]:
    """Return (mode, values) from .env, or ask if nothing is set. mode is
    'csv', 'ids', or 'search'. An empty search would match EVERY entry in
    the partner, so the script never runs without a selection."""
    global TAGS, CATEGORY_IDS
    if INPUT_FILENAME:
        ignored = [n for n, v in (("ENTRY_IDS", ENTRY_IDS), ("TAGS", TAGS),
                                  ("CATEGORY_IDS", CATEGORY_IDS)) if v]
        if ignored:
            print(f"[WARN] INPUT_FILENAME is set, so {', '.join(ignored)} "
                  "in .env is ignored.")
        return "csv", [INPUT_FILENAME]
    if ENTRY_IDS:
        if TAGS or CATEGORY_IDS:
            print("[WARN] ENTRY_IDS is set, so TAGS/CATEGORY_IDS in .env "
                  "are ignored.")
        return "ids", ENTRY_IDS
    if TAGS or CATEGORY_IDS:
        return "search", []

    print("\nWhich entries should have their non-source flavors deleted?")
    print("[1] Entries with a tag (or comma-separated tags)")
    print("[2] Entries in a category ID (or comma-separated IDs)")
    print("[3] A comma-separated list of entry IDs")
    print("[4] A CSV file of entry IDs in the input folder")
    choice = input("Enter the number corresponding to your choice: ").strip()
    prompts = {
        "1": "Enter the tag(s): ",
        "2": "Enter the category ID(s): ",
        "3": "Enter the entry IDs: ",
        "4": "Enter the file name in the input folder: ",
    }
    if choice not in prompts:
        print("Error: Invalid choice. Please enter 1, 2, 3, or 4.")
        sys.exit(2)
    answer = input(prompts[choice]).strip()
    values = [v.strip() for v in answer.split(",") if v.strip()]
    if not values:
        print("Error: You must provide a value.")
        sys.exit(2)

    if choice == "1":
        TAGS = values
        return "search", []
    if choice == "2":
        CATEGORY_IDS = values
        return "search", []
    if choice == "3":
        return "ids", values
    return "csv", [answer]


# =============================================================================
# Main workflow ---------------------------------------------------------------
# =============================================================================

def main():
    global PARTNER_ID, KS
    _start = time.monotonic()

    def _elapsed() -> str:
        secs = int(time.monotonic() - _start)
        h, rem = divmod(secs, 3600)
        m, s = divmod(rem, 60)
        if h:
            return f"{h}h{m:02d}m{s:02d}s"
        if m:
            return f"{m}m{s:02d}s"
        return f"{s}s"

    if os.getenv("ADMIN_SECRET"):
        print(
            "⚠️ ADMIN_SECRET is set in your .env or environment and is being "
            "ignored. The admin secret is always typed in at runtime; please "
            "delete it from .env."
        )

    if PARTNER_ID:
        print(f"Partner ID (from .env): {PARTNER_ID}")
    else:
        PARTNER_ID = input("Enter your Kaltura Partner ID: ").strip()
    if not PARTNER_ID.isdigit():
        print("Error: The Partner ID must be a number.")
        sys.exit(2)
    admin_secret = getpass.getpass(
        "Enter your Kaltura Admin Secret (input is hidden): "
    ).strip()
    if not admin_secret:
        print("Error: Admin secret cannot be empty.")
        sys.exit(2)
    KS = start_session(admin_secret)

    mode, values = choose_selection()

    print(
        f"[INFO] MAX_WORKERS={MAX_WORKERS} | "
        f"PROGRESS_EVERY={PROGRESS_EVERY} | "
        f"REQUEST_TIMEOUT={REQUEST_TIMEOUT} | "
        f"SKIP_ENTRY_METADATA={SKIP_ENTRY_METADATA} | "
        f"SKIP_CONFIRMATION={SKIP_CONFIRMATION}"
    )

    OUTPUT_DIR.mkdir(exist_ok=True)

    # -------------------------------------------------------------------------
    # Phase 1: Build preview rows
    # -------------------------------------------------------------------------
    if mode == "csv":
        targets = load_entry_ids_from_csv(values[0])
        worker = process_entry_id
        print(
            f"[INFO] CSV mode: {len(targets)} entry IDs from '{values[0]}' "
            f"(column '{COLUMN_HEADER_ENTRY_ID}')."
        )
    elif mode == "ids":
        targets = list(dict.fromkeys(values))
        worker = process_entry_id
        print(f"[INFO] {len(targets)} entry IDs selected.")
    else:
        print(
            f"[INFO] Searching entries (tags: {', '.join(TAGS) or '-'}; "
            f"categories: {', '.join(CATEGORY_IDS) or '-'})..."
        )
        targets = search_entries()
        worker = build_preview_rows_for_entry
        print(f"[INFO] Found {len(targets)} candidate entries.")

    total = len(targets)
    if not total:
        print("[INFO] No entries to process. Exiting.")
        return
    print(
        f"[INFO] Listing flavors and building the deletion plan "
        f"(MAX_WORKERS={MAX_WORKERS})..."
    )

    rows_by_index: Dict[int, List[Dict]] = {}
    completed = 0
    lock = threading.Lock()

    preview_fieldnames = _write_result_header(
        PREVIEW_CSV, CSV_ORIGINAL_FIELDNAMES
    )
    print(f"[INFO] Writing preview incrementally to {PREVIEW_CSV}")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {
            pool.submit(worker, target): i
            for i, target in enumerate(targets)
        }
        for fut in as_completed(futures):
            rows = fut.result()
            idx = futures[fut]
            for r in rows:
                _append_result_row(PREVIEW_CSV, preview_fieldnames, r, lock)
            with lock:
                rows_by_index[idx] = rows
                completed += 1
                child_count = int(rows[0].get("child_count") or 0)
                if child_count:
                    print(
                        f"  {completed}/{total} [{_elapsed()}] — entry "
                        f"{rows[0]['entry_id']} has {child_count} "
                        f"child(ren); plan applied to each."
                    )
                elif completed % PROGRESS_EVERY == 0 or completed == total:
                    print(
                        f"  {completed}/{total} [{_elapsed()}] "
                        f"entries processed..."
                    )

    # Preserve selection order. An entry can appear twice (listed itself and
    # found as a child of a listed parent); plan it only once.
    preview_rows: List[Dict] = []
    seen = set()
    for i in range(total):
        for r in rows_by_index.get(i, []):
            if r["entry_id"] not in seen:
                seen.add(r["entry_id"])
                preview_rows.append(r)

    print(f"[INFO] Wrote pre-deletion plan → {PREVIEW_CSV}")

    # -------------------------------------------------------------------------
    # Summary before confirmation
    # -------------------------------------------------------------------------
    ready = [
        r for r in preview_rows
        if r["status"] == "READY" and r["flavors_to_delete"]
    ]
    if not ready:
        print("[INFO] No entries require deletion. Exiting.")
        return

    parents_ready = sum(1 for r in ready if r.get("role") == "PARENT")
    children_ready = sum(1 for r in ready if r.get("role") == "CHILD")
    total_flavors_to_delete = sum(
        int(r.get("flavors_deleted_count", "0") or 0) for r in ready
    )
    total_kb_to_save = sum(
        _as_int(r.get("kilobytes_saved", "0")) for r in ready
    )
    print(
        f"[PLAN] Parents ready: {parents_ready} | "
        f"Children ready: {children_ready} | "
        f"Flavors to delete: {total_flavors_to_delete} | "
        f"KB potentially saved: {total_kb_to_save}"
    )

    if not SKIP_CONFIRMATION:
        confirm = input(
            "\nReview the preview CSV, then type 'DELETE' to permanently "
            "delete the listed flavors: "
        ).strip().upper()
        if confirm != "DELETE":
            print("[ABORTED] No deletions performed.")
            return
    else:
        print("[INFO] SKIP_CONFIRMATION=true — proceeding without prompt.")

    # -------------------------------------------------------------------------
    # Phase 2: Delete flavors (incremental result CSV)
    # -------------------------------------------------------------------------
    fieldnames = _write_result_header(RESULT_CSV, CSV_ORIGINAL_FIELDNAMES)
    write_lock = threading.Lock()
    total_rows = len(preview_rows)
    completed = 0
    deleted_count = 0
    partial_count = 0
    failed_count = 0

    print(
        f"\n[INFO] Deleting flavors across {total_rows} rows "
        f"(MAX_WORKERS={MAX_WORKERS})..."
    )
    print(f"[INFO] Writing results incrementally to {RESULT_CSV}")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {
            pool.submit(
                delete_row_flavors, row, RESULT_CSV, fieldnames, write_lock
            ): i
            for i, row in enumerate(preview_rows)
        }
        for fut in as_completed(futures):
            rr = fut.result()
            completed += 1
            status = rr.get("status", "")
            if status == "DELETED":
                deleted_count += 1
            elif status == "PARTIAL":
                partial_count += 1
            elif status == "FAILED":
                failed_count += 1
            if completed % PROGRESS_EVERY == 0 or completed == total_rows:
                print(
                    f"  {completed}/{total_rows} [{_elapsed()}] rows "
                    f"processed (deleted={deleted_count}, "
                    f"partial={partial_count}, "
                    f"failed={failed_count})..."
                )

    print(f"\n[INFO] Wrote results → {RESULT_CSV}")
    print(
        f"[DONE] Elapsed={_elapsed()} | "
        f"Deleted={deleted_count} | "
        f"Partial={partial_count} | "
        f"Failed={failed_count}"
    )


if __name__ == "__main__":
    main()
