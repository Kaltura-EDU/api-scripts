"""audit-captions.py

Audits Kaltura media entries to determine whether each has a ready (usable)
caption asset, and optionally whether it has a ready Extended Audio
Description (EAD) asset.

Input: choose one of three ways to select entries via an interactive
runtime prompt (not a CLI flag):
    1. A CSV of entry IDs (INPUT_FILENAME / COLUMN_HEADER_ENTRY_ID in .env)
    2. Tag(s) (comma-delimited, OR logic)
    3. Entry ID(s), typed directly (comma-delimited)

Output: a timestamped CSV in output/ with columns:
    entryId, title, userId, captions, EAD
`userId` is the entry owner's Kaltura user ID. `captions` and `EAD` are "Y"
or "N". If CHECK_EAD is off, the EAD column is left blank rather than "N",
since it was never actually checked.

"Ready" means Kaltura has fully processed the caption/EAD file and it is
usable (KalturaCaptionAssetStatus.READY) — not just uploaded or queued.

Notes:
- PARTNER_ID is a plain (non-secret) value read from .env or prompted if
  blank; the Admin Secret is always prompted via getpass and never read
  from .env.
- Extended Audio Descriptions are distinguished from regular captions by the
  caption asset's `usage` field (KalturaCaptionAssetUsage), not by name.
  requirements.txt pins KalturaApiClient>=21.20.0, the oldest version
  confirmed to have this field.
- Entries are audited concurrently, one Kaltura session per worker thread
  (MAX_WORKERS in .env, default 3). The run keeps the computer awake via
  wakepy for its duration (on_fail="warn": if sleep prevention can't be
  activated on this system, warn and continue rather than stop the run).
- API calls are paced across all worker threads (AUDIT_RATE_PER_SEC in
  .env, default 5/sec) so a large batch doesn't fire requests faster than
  Kaltura allows. That default is a reasonable starting point, not a number
  measured against a real account's throttle limit — adjust it if you see
  errors that look like rate limiting.
"""

from __future__ import annotations

import csv
import getpass
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from datetime import datetime

from dotenv import load_dotenv
from wakepy import keep

# Load .env alongside this script, not relying on current working directory.
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

from KalturaClient import KalturaClient, KalturaConfiguration
from KalturaClient.Plugins.Core import (
    KalturaBaseEntryFilter, KalturaFilterPager, KalturaSessionType,
)
from KalturaClient.exceptions import KalturaException, KalturaClientException
from KalturaClient.Plugins.Caption import (
    KalturaCaptionAssetFilter, KalturaCaptionAssetStatus,
)

# Audio descriptions are distinguished from captions by the caption asset's
# `usage` field (KalturaCaptionAssetUsage). requirements.txt pins
# KalturaApiClient>=21.20.0, the oldest version confirmed to have this field,
# so this import should always succeed — the except below is a safety net,
# not an expected path.
try:
    from KalturaClient.Plugins.Caption import KalturaCaptionAssetUsage
    AUDIO_DESCRIPTION_USAGE = getattr(
        KalturaCaptionAssetUsage, "EXTENDED_AUDIO_DESCRIPTION", "1"
    )
except ImportError:
    AUDIO_DESCRIPTION_USAGE = "1"


def _env_bool(name: str, default: bool = False) -> bool:
    val = os.getenv(name, "").strip().lower()
    if not val:
        return default
    return val in ("1", "true", "yes", "y", "on")


def _env_int(name: str, default: int) -> int:
    val = os.getenv(name, "").strip()
    return int(val) if val else default


def _env_float(name: str, default: float) -> float:
    val = os.getenv(name, "").strip()
    return float(val) if val else default


PARTNER_ID = os.getenv("PARTNER_ID", "").strip()
CHECK_EAD = _env_bool("CHECK_EAD", default=False)
RETRY_ATTEMPTS = _env_int("RETRY_ATTEMPTS", 3)
MAX_WORKERS = max(1, _env_int("MAX_WORKERS", 3))
AUDIT_RATE_PER_SEC = max(0.0, _env_float("AUDIT_RATE_PER_SEC", 5.0))
INPUT_FILENAME = os.getenv("INPUT_FILENAME", "").strip()
COLUMN_HEADER_ENTRY_ID = os.getenv("COLUMN_HEADER_ENTRY_ID", "").strip()
OUTPUT_FOLDER = "output"


class RateLimiter:
    """Spaces API calls evenly across all worker threads, at a target rate
    of calls per second. Threads reserve the next available time slot under
    a lock, then sleep outside it, so one waiting thread never blocks
    another thread's reservation. A rate of 0 disables pacing entirely."""

    def __init__(self, per_sec: float):
        self.interval = 1.0 / per_sec if per_sec > 0 else 0.0
        self._lock = threading.Lock()
        self._next = 0.0

    def wait(self) -> None:
        if not self.interval:
            return
        with self._lock:
            slot = max(time.monotonic(), self._next)
            self._next = slot + self.interval
        remaining = slot - time.monotonic()
        if remaining > 0:
            time.sleep(remaining)


def _usage_value(cap) -> str:
    """Return a caption asset's `usage` as a plain string ("0"/"1"/…), or ""
    if the SDK doesn't expose it. `usage` is a KalturaCaptionAssetUsage enum
    object whose value is read via .getValue(); older SDKs (< 22.0.0) omit
    the field entirely, so getattr returns None."""
    raw = getattr(cap, "usage", None)
    if raw is None:
        return ""
    if hasattr(raw, "getValue"):
        raw = raw.getValue()
    return str(raw).strip() if raw is not None else ""


def get_kaltura_client(partner_id, admin_secret):
    """Starts an admin Kaltura session for the given partner ID."""
    config = KalturaConfiguration(partner_id)
    config.serviceUrl = "https://www.kaltura.com/"
    client = KalturaClient(config)
    try:
        ks = client.session.start(
            admin_secret, "admin", KalturaSessionType.ADMIN, partner_id,
            privileges="all:*,disableentitlement"
        )
    except KalturaException as e:
        if getattr(e, "code", "") == "START_SESSION_ERROR":
            print(
                "\n❌ Could not log in to Kaltura.\n"
                f"   Partner ID [{partner_id}] and the Admin Secret you "
                "entered were not accepted.\n\n"
                "   Please double-check both values and try again:\n"
                "     • The Partner ID is the numeric account ID.\n"
                "     • The Admin Secret must be the *Administrator* secret "
                "(not the User secret),\n"
                "       copied exactly from KMC → Settings → Integration "
                "Settings.\n"
            )
        else:
            print(f"\n❌ Could not start Kaltura session: {e}\n")
        sys.exit(1)
    except KalturaClientException as e:
        print(
            "\n❌ Could not reach Kaltura to start a session.\n"
            f"   {e}\n"
            "   Check your internet connection and try again.\n"
        )
        sys.exit(1)
    client.setKs(ks)
    return client


def get_thread_client(partner_id, admin_secret, thread_local):
    """One Kaltura session per worker thread, built lazily and cached on
    first use — a KalturaClient's session state isn't safe to share across
    threads, so each worker thread needs its own."""
    client = getattr(thread_local, "client", None)
    if client is None:
        client = get_kaltura_client(partner_id, admin_secret)
        thread_local.client = client
    return client


def get_entry_details(client, entry_id, rate_limiter):
    """Fetch an entry's title and owner user ID, with retry. Returns
    (None, None) if the entry could not be retrieved (e.g.
    ENTRY_ID_NOT_FOUND)."""
    for attempt in range(RETRY_ATTEMPTS):
        try:
            rate_limiter.wait()
            entry = client.baseEntry.get(entry_id)
            return entry.name, (entry.userId or "")
        except KalturaException as e:
            if getattr(e, "code", "") == "ENTRY_ID_NOT_FOUND":
                return None, None
            print(f"⚠️ Attempt {attempt + 1}: Failed to retrieve entry {entry_id}. Error: {e}")
            time.sleep(2 ** attempt)
    print(f"❌ Giving up on entry {entry_id} after {RETRY_ATTEMPTS} attempts.")
    return None, None


def get_entries_by_tag(client, tags, rate_limiter):
    """Returns a list of (entry_id, title, user_id) for entries matching the
    given comma-delimited tags (OR logic). Title and owner come from the
    search result directly — no extra per-entry lookup needed."""
    entry_filter = KalturaBaseEntryFilter()
    entry_filter.tagsMultiLikeOr = tags
    pager = KalturaFilterPager()
    pager.pageSize = 100
    pager.pageIndex = 1

    results = []
    while True:
        rate_limiter.wait()
        page = client.baseEntry.list(entry_filter, pager).objects
        results.extend((e.id, e.name, e.userId or "") for e in page)
        if len(page) < pager.pageSize:
            break
        pager.pageIndex += 1
    return results


def get_entry_ids_from_csv(input_filename, column_header):
    """Returns a list of entry ID strings read from the configured CSV."""
    input_path = Path(input_filename)
    if not input_path.exists():
        raise RuntimeError(f"Input CSV not found: {input_filename}")

    with open(input_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            raise RuntimeError("Input CSV has no headers")
        norm_header = column_header.strip().lower()
        fieldname_map = {str(h).strip().lower(): h for h in reader.fieldnames}
        if norm_header not in fieldname_map:
            raise RuntimeError(
                f"Column '{column_header}' not found in {input_filename}. "
                f"Available columns: {', '.join(reader.fieldnames)}"
            )
        actual_header = fieldname_map[norm_header]
        ids = [
            str(row[actual_header]).strip()
            for row in reader
            if str(row.get(actual_header, "")).strip()
        ]
    return ids


def check_entry_captions(client, entry_id, check_ead, rate_limiter):
    """Returns (has_caption, has_ead) where has_ead is None if check_ead is
    False (not checked, not "no"). Only READY assets count."""
    cap_filter = KalturaCaptionAssetFilter()
    cap_filter.entryIdEqual = entry_id
    cap_filter.statusEqual = KalturaCaptionAssetStatus.READY
    pager = KalturaFilterPager()
    pager.pageSize = 100
    pager.pageIndex = 1

    has_caption = False
    has_ead = False

    while True:
        rate_limiter.wait()
        page = client.caption.captionAsset.list(cap_filter, pager).objects
        for cap in page:
            if _usage_value(cap) == str(AUDIO_DESCRIPTION_USAGE):
                has_ead = True
            else:
                has_caption = True
        if len(page) < pager.pageSize:
            break
        pager.pageIndex += 1

    return has_caption, (has_ead if check_ead else None)


def process_row(partner_id, admin_secret, thread_local, entry_id, known_title, known_user_id, check_ead, rate_limiter):
    """Runs in a worker thread: resolve title/owner (if not already known)
    and check caption/EAD readiness for one entry. Returns an output row
    dict. Any unhandled error is caught here rather than left to propagate,
    so one bad entry doesn't abort the whole batch."""
    try:
        client = get_thread_client(partner_id, admin_secret, thread_local)

        title, user_id = known_title, known_user_id
        if title is None:
            title, user_id = get_entry_details(client, entry_id, rate_limiter)

        if title is None:
            return {
                "entryId": entry_id, "title": "[entry not found]", "userId": "",
                "captions": "", "EAD": "",
            }

        has_caption, has_ead = check_entry_captions(client, entry_id, check_ead, rate_limiter)
        return {
            "entryId": entry_id,
            "title": title,
            "userId": user_id,
            "captions": "Y" if has_caption else "N",
            "EAD": "" if has_ead is None else ("Y" if has_ead else "N"),
        }
    except Exception as exc:
        print(f"❌ Unhandled error auditing {entry_id}: {exc}")
        return {
            "entryId": entry_id, "title": "[error during audit]", "userId": "",
            "captions": "", "EAD": "",
        }


def main():
    partner_id = PARTNER_ID or input("Enter your Partner ID: ").strip()
    admin_secret = getpass.getpass("Enter your Admin Secret: ").strip()
    client = get_kaltura_client(partner_id, admin_secret)
    rate_limiter = RateLimiter(AUDIT_RATE_PER_SEC)

    print("\nSelect input method:")
    print("  (Tag(s) and Entry ID(s) accept comma-delimited values; multiple values are treated as OR.)")
    print("[1] CSV of entry IDs")
    print("[2] Tag(s)")
    print("[3] Entry ID(s), comma-delimited")

    method_mapping = {"1": "csv", "2": "tag", "3": "entry_ids"}
    while True:
        choice = input("Enter the number corresponding to your choice: ").strip()
        if choice in method_mapping:
            method = method_mapping[choice]
            break
        print("Error: Invalid choice. Please enter 1, 2, or 3.")

    # Resolve to a list of (entry_id, title_or_None, user_id_or_None) —
    # title/user_id are filled in later for methods where they aren't
    # already known.
    rows = []  # [(entry_id, title_or_None, user_id_or_None)]

    if method == "csv":
        input_filename = INPUT_FILENAME or input("Enter path to input CSV: ").strip()
        column_header = COLUMN_HEADER_ENTRY_ID or input("Enter the entry ID column header: ").strip()
        entry_ids = get_entry_ids_from_csv(input_filename, column_header)
        rows = [(eid, None, None) for eid in entry_ids]

    elif method == "tag":
        tags = input("Enter tag(s): ").strip()
        if not tags:
            print("Error: You must provide at least one tag.")
            sys.exit(1)
        rows = get_entries_by_tag(client, tags, rate_limiter)

    elif method == "entry_ids":
        identifier = input("Enter entry ID(s): ").strip()
        entry_ids = [t.strip() for t in identifier.split(",") if t.strip()]
        if not entry_ids:
            print("Error: You must provide at least one entry ID.")
            sys.exit(1)
        rows = [(eid, None, None) for eid in entry_ids]

    if not rows:
        print("No entries found.")
        return

    print(f"\nAuditing {len(rows)} entrie(s)...")
    print(f"CHECK_EAD: {CHECK_EAD} | MAX_WORKERS: {MAX_WORKERS} | AUDIT_RATE_PER_SEC: {AUDIT_RATE_PER_SEC:g}")

    run_ts = datetime.now().strftime("%Y-%m-%d-%H%M")
    output_dir = Path(__file__).resolve().parent / OUTPUT_FOLDER
    output_dir.mkdir(parents=True, exist_ok=True)
    out_csv = output_dir / f"{run_ts}_caption_audit.csv"

    fieldnames = ["entryId", "title", "userId", "captions", "EAD"]
    thread_local = threading.local()
    total = len(rows)

    with keep.running(on_fail="warn") as wakepy_mode:
        if wakepy_mode.active:
            print(
                "System sleep prevented for the duration of this run "
                f"(wakepy, method: {wakepy_mode.active_method})."
            )

        with open(out_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
                futures = {
                    pool.submit(
                        process_row, partner_id, admin_secret, thread_local,
                        entry_id, known_title, known_user_id, CHECK_EAD, rate_limiter,
                    ): entry_id
                    for entry_id, known_title, known_user_id in rows
                }

                completed = 0
                for fut in as_completed(futures):
                    entry_id = futures[fut]
                    completed += 1
                    result = fut.result()
                    writer.writerow(result)
                    f.flush()
                    print(f"[{completed}/{total}] Checked {entry_id}")

    print(f"\n✅ Done. Output CSV: {out_csv}")


if __name__ == "__main__":
    main()
