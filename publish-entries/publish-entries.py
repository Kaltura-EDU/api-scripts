"""
Bulk Publish Entries to Kaltura Categories

Reads a CSV of entry IDs and category IDs and publishes each entry to its
target category via categoryEntry.add. Supports optional row filtering
(e.g., retry only rows where publish_status == error), configurable
concurrency, and automatic retry with exponential backoff.

Before calling categoryEntry.add, the script pre-checks which pairs are
already published via categoryEntry.list, avoiding the slow SDK timeout
that occurs when add is called on an already-existing pair.

Usage:
    python3 publish-entries.py <input_csv>
    python3 publish-entries.py            # uses INPUT_CSV_FILENAME from .env

Input CSV (minimum):
    entry_id     - Kaltura entry ID
    category_id  - Kaltura category ID to publish the entry into

Column names and an optional filter column are configurable via .env.

Output (in ./output/):
    <timestamp>_publish-entries-report.csv

Author: Galen Davis
"""

import csv
import getpass
import os
import random
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from dotenv import load_dotenv
from KalturaClient import KalturaClient, KalturaConfiguration
from KalturaClient.Plugins.Core import (
    KalturaCategoryEntry,
    KalturaCategoryEntryFilter,
    KalturaFilterPager,
    KalturaSessionType,
)

# ── Input files ────────────────────────────────────────────────────────
# Input files live in the input/ folder next to this script. A bare
# filename, a leading "input/", or an absolute path all work.
def resolve_input_path(name):
    name = os.path.expanduser(str(name).strip())
    if os.path.isabs(name):
        return name
    base = os.path.dirname(os.path.abspath(__file__))
    norm = name.replace("\\", "/")
    candidate = os.path.join(
        base, name if norm.startswith("input/") else os.path.join("input", name)
    )
    if not os.path.exists(candidate) and os.path.exists(name):
        return os.path.abspath(name)  # relative to where you launched it
    return candidate


# ── Network retry ──────────────────────────────────────────────────────
# KalturaClientException (timeouts, resets) is NOT a KalturaException, so
# plain `except KalturaException` misses it. Knobs come from .env:
# REQUEST_TIMEOUT, MAX_NETWORK_RETRIES, NETWORK_RETRY_DELAY.
import os as _os
import time as _time

import requests as _requests
from KalturaClient.exceptions import (
    KalturaClientException as _KalturaClientException,
)


def _retry_env_int(name, default):
    try:
        return int((_os.getenv(name) or "").strip() or default)
    except ValueError:
        return default


def call_with_retry(fn, *args, **kwargs):
    """Call fn(*args, **kwargs), retrying with linear backoff on transient
    network failures. Real API errors (KalturaException) are re-raised
    untouched so callers can handle them."""
    retries = _retry_env_int("MAX_NETWORK_RETRIES", 5)
    delay = _retry_env_int("NETWORK_RETRY_DELAY", 5)
    for attempt in range(1, retries + 1):
        try:
            return fn(*args, **kwargs)
        except (
            _KalturaClientException,
            _requests.exceptions.RequestException,
        ) as exc:
            if attempt == retries:
                raise
            wait = delay * attempt
            print(
                f"    [network error: {exc}; retry "
                f"{attempt}/{retries} in {wait}s]"
            )
            _time.sleep(wait)


load_dotenv()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PARTNER_ID = int(os.getenv("PARTNER_ID", "0"))
# The admin secret is never read from .env -- it is prompted at runtime.
ADMIN_SECRET = ""
USER_ID = os.getenv("USER_ID", "")
SERVICE_URL = os.getenv("SERVICE_URL", "https://www.kaltura.com")
SESSION_EXPIRY = int(os.getenv("SESSION_EXPIRY", "86400"))

# CSV column names
ENTRY_ID_COLUMN = os.getenv("ENTRY_ID_COLUMN", "entry_id")
CATEGORY_ID_COLUMN = os.getenv("CATEGORY_ID_COLUMN", "category_id")

# Optional: only process rows where STATUS_COLUMN == STATUS_FILTER.
# Leave STATUS_COLUMN empty to process all rows.
# Example: STATUS_COLUMN=publish_status, STATUS_FILTER=error
STATUS_COLUMN = os.getenv("STATUS_COLUMN", "")
STATUS_FILTER = os.getenv("STATUS_FILTER", "error")

# Concurrency and retry
THREAD_COUNT = int(os.getenv("THREAD_COUNT", "10"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "4"))

REPORTS_DIR = "output"
os.makedirs(REPORTS_DIR, exist_ok=True)
RUN_TIMESTAMP = datetime.now().strftime("%Y-%m-%d-%H%M")
OUTPUT_CSV = os.path.join(
    REPORTS_DIR, f"{RUN_TIMESTAMP}_publish-entries-report.csv"
)

OUTPUT_FIELDS = ["entry_id", "category_id", "status", "error"]

if not PARTNER_ID:
    print("Error: PARTNER_ID must be set in your .env file.")
    sys.exit(1)

def _prompt_admin_secret():
    """Ask for the admin secret. It is never read from .env."""
    if os.getenv("ADMIN_SECRET", "").strip():
        print(
            "\n⚠️  ADMIN_SECRET is set in your .env. This script does not "
            "read it -- you will be asked for the secret instead.\n"
            "   Please delete that line from .env so the secret is not "
            "stored on disk.\n"
        )
    secret = getpass.getpass(
        "Enter your Kaltura admin secret (input hidden): "
    ).strip()
    if not secret:
        print("❌ No admin secret entered. Exiting.")
        raise SystemExit(1)
    return secret


ADMIN_SECRET = _prompt_admin_secret()

# ---------------------------------------------------------------------------
# Thread safety
# ---------------------------------------------------------------------------
_thread_local = threading.local()
_print_lock = threading.Lock()


def log(msg: str):
    with _print_lock:
        print(msg)


# ---------------------------------------------------------------------------
# Kaltura client
# ---------------------------------------------------------------------------

def create_client() -> KalturaClient:
    config = KalturaConfiguration()
    config.requestTimeout = _retry_env_int("REQUEST_TIMEOUT", 120)
    config.serviceUrl = SERVICE_URL
    config.partnerId = PARTNER_ID
    client = KalturaClient(config)
    try:
        ks = call_with_retry(client.session.start,
            ADMIN_SECRET,
            USER_ID,
            KalturaSessionType.ADMIN,
            PARTNER_ID,
            expiry=SESSION_EXPIRY,
            privileges="all:*,disableentitlement",
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
    return client


def get_client() -> KalturaClient:
    if not hasattr(_thread_local, "client"):
        _thread_local.client = create_client()
    return _thread_local.client


# ---------------------------------------------------------------------------
# Retry
# ---------------------------------------------------------------------------

def _is_retryable(exc: Exception) -> bool:
    msg = str(exc).lower()
    no_retry = (
        "duplicate", "already exist", "already assigned",
        "invalid ks", "invalid session",
    )
    return not any(p in msg for p in no_retry)


def with_retry(fn, label: str = ""):
    last_exc = None
    for attempt in range(MAX_RETRIES):
        try:
            return fn()
        except Exception as exc:
            last_exc = exc
            if attempt < MAX_RETRIES - 1 and _is_retryable(exc):
                delay = (2 ** attempt) + random.uniform(0, 1)
                log(
                    f"  Retry {attempt + 1}/{MAX_RETRIES - 1} "
                    f"for {label} in {delay:.1f}s: {exc}"
                )
                time.sleep(delay)
            else:
                raise
    raise last_exc


# ---------------------------------------------------------------------------
# Pre-check: which (entry_id, category_id) pairs already exist?
# ---------------------------------------------------------------------------

def get_already_published(
    client: KalturaClient, rows: list
) -> set:
    """
    Return the set of (entry_id, category_id_str) pairs that are already
    published. Uses categoryEntry.list with batched entryIdIn filters to
    avoid the slow SDK timeout that occurs when add is called on a duplicate.
    """
    if not rows:
        return set()

    existing = set()
    entry_ids = list({eid for eid, _ in rows})
    cat_ids = list({cid for _, cid in rows})
    cat_id_filter = ",".join(cat_ids)

    # Batch by entry IDs to keep filter strings reasonable
    batch_size = 200
    for i in range(0, len(entry_ids), batch_size):
        batch = entry_ids[i:i + batch_size]
        filt = KalturaCategoryEntryFilter()
        filt.entryIdIn = ",".join(batch)
        filt.categoryIdIn = cat_id_filter
        pager = KalturaFilterPager()
        pager.pageSize = 500
        pager.pageIndex = 1
        while True:
            resp = call_with_retry(client.categoryEntry.list, filt, pager)
            for ce in resp.objects:
                existing.add((ce.entryId, str(ce.categoryId)))
            if len(resp.objects) < pager.pageSize:
                break
            pager.pageIndex += 1

    return existing


# ---------------------------------------------------------------------------
# Worker
# ---------------------------------------------------------------------------

def publish_one(entry_id: str, category_id: str) -> dict:
    log(f"  Publishing {entry_id} -> category {category_id}...")
    ce = KalturaCategoryEntry()
    ce.entryId = entry_id
    ce.categoryId = int(category_id)
    error_msg = ""
    try:
        with_retry(
            lambda: call_with_retry(get_client().categoryEntry.add, ce),
            label=f"{entry_id} -> {category_id}",
        )
        status = "ok"
        log(f"  OK: {entry_id}")
    except Exception as exc:
        status = "error"
        error_msg = str(exc)
        log(f"  ERROR: {entry_id}: {exc}")
    return {
        "entry_id": entry_id,
        "category_id": category_id,
        "status": status,
        "error": error_msg,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    if len(sys.argv) >= 2:
        input_csv = sys.argv[1]
    else:
        input_csv = os.getenv("INPUT_CSV_FILENAME", "").strip()
        if not input_csv:
            print(
                "Error: no input file specified.\n"
                "Pass it as an argument or set INPUT_CSV_FILENAME in .env."
            )
            sys.exit(1)

    input_csv = resolve_input_path(input_csv)
    if not os.path.exists(input_csv):
        print(
            f"File not found: {input_csv}\n"
            "Put your CSV in the input/ folder next to the script."
        )
        sys.exit(1)

    print(f"Reading {input_csv}...")
    rows = []
    with open(input_csv, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames or []
        missing = [
            col for col in (ENTRY_ID_COLUMN, CATEGORY_ID_COLUMN)
            if col not in headers
        ]
        if missing:
            print(
                f"Error: column(s) not found in CSV: "
                f"{', '.join(missing)}\n"
                f"Set ENTRY_ID_COLUMN / CATEGORY_ID_COLUMN in .env "
                f"if your CSV uses different names."
            )
            sys.exit(1)
        for row in reader:
            if STATUS_COLUMN:
                if row.get(STATUS_COLUMN, "").strip() != STATUS_FILTER:
                    continue
            entry_id = row[ENTRY_ID_COLUMN].strip()
            category_id = row[CATEGORY_ID_COLUMN].strip()
            if entry_id and category_id:
                rows.append((entry_id, category_id))

    if not rows:
        filter_note = (
            f" matching {STATUS_COLUMN}={STATUS_FILTER}"
            if STATUS_COLUMN else ""
        )
        print(f"No rows found{filter_note}. Nothing to do.")
        sys.exit(0)

    filter_note = (
        f" (filtered to {STATUS_COLUMN}={STATUS_FILTER})"
        if STATUS_COLUMN else ""
    )
    print(
        f"  {len(rows)} entry/category pair(s) to process{filter_note}."
    )

    # Pre-check: find which pairs are already published so we don't call
    # categoryEntry.add on them (add on an existing pair causes a ~90s
    # SDK timeout before the duplicate error is returned).
    print("Connecting to Kaltura...")
    main_client = create_client()
    print("Checking which entries are already published...")
    already_published = get_already_published(main_client, rows)

    to_publish = [
        (eid, cid) for eid, cid in rows
        if (eid, cid) not in already_published
    ]
    n_already = len(rows) - len(to_publish)
    if n_already:
        print(f"  {n_already} already published — skipping.")
    print(f"  {len(to_publish)} to publish.")

    results = []
    n_ok = n_err = 0

    # Record already-published pairs without making any API calls
    for eid, cid in rows:
        if (eid, cid) in already_published:
            results.append({
                "entry_id": eid,
                "category_id": cid,
                "status": "already_published",
                "error": "",
            })

    if to_publish:
        print(f"Publishing with {THREAD_COUNT} thread(s)...")
        with ThreadPoolExecutor(max_workers=THREAD_COUNT) as pool:
            futures = {
                pool.submit(publish_one, eid, cid): (eid, cid)
                for eid, cid in to_publish
            }
            for future in as_completed(futures):
                result = future.result()
                results.append(result)
                if result["status"] == "ok":
                    n_ok += 1
                else:
                    n_err += 1

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        writer.writerows(results)

    already_note = f", {n_already} already published" if n_already else ""
    print(f"\nDone. {n_ok} published{already_note}, {n_err} error(s).")
    print(f"Report: {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
