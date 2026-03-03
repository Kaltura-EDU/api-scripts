"""Reassign Kaltura entry ownership using a CSV mapping of old_user -> new_user.

Uses baseEntry.list and baseEntry.update.
Supports pagination, DRY_RUN, retries/backoff, and concurrency.
Outputs timestamped CSV, summary, and error logs.
Configured via .env.
"""

from __future__ import annotations

import csv
import os
import random
import sys
import time
import threading
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, as_completed, wait
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

try:
    # Preferred: pip install python-dotenv
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    load_dotenv = None

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    # Python 3.9+ should have zoneinfo. If not, fall back to UTC.
    ZoneInfo = None

from KalturaClient import KalturaClient, KalturaConfiguration
from KalturaClient.Plugins.Core import (
    KalturaBaseEntry,
    KalturaBaseEntryFilter,
    KalturaFilterPager,
    KalturaSessionType,
)


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def _env_bool(name: str, default: bool = False) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return val.strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_int(name: str, default: int) -> int:
    val = os.getenv(name)
    if val is None or val.strip() == "":
        return default
    try:
        return int(val)
    except ValueError:
        raise ValueError(f"Env var {name} must be an integer (got {val!r}).")


def _env_float(name: str, default: float) -> float:
    val = os.getenv(name)
    if val is None or val.strip() == "":
        return default
    try:
        return float(val)
    except ValueError:
        raise ValueError(f"Env var {name} must be a number (got {val!r}).")


def _timestamp_tttt(timezone_name: str) -> str:
    tz = None
    if ZoneInfo is not None:
        try:
            tz = ZoneInfo(timezone_name)
        except Exception:
            tz = None

    now = datetime.now(tz=tz)
    # TTTT = HHMM (24-hour time)
    return now.strftime("%Y-%m-%d-%H%M")


def _load_env() -> None:
    if load_dotenv is not None:
        load_dotenv()

# NOTE: The Kaltura client object is not guaranteed to be thread-safe.
# We use a lock to avoid concurrent access that can cause intermittent failures.
_CLIENT_LOCK = threading.Lock()



def _print_progress(message: str) -> None:
    # Always flush so feedback appears immediately in long runs.
    print(message, flush=True)


# -----------------------------------------------------------------------------
# Friendly exception printer
# -----------------------------------------------------------------------------

def _print_friendly_exception(exc: Exception, input_filename: str) -> None:
    """Print a concise, user-friendly error message.

    We intentionally avoid a full traceback for expected input/config errors.
    Set SHOW_TRACEBACK=1 in .env to see the full stack trace.
    """

    print("\nERROR:", flush=True)

    # Friendly handling for the most common CSV header mistake.
    msg = str(exc)
    if isinstance(exc, ValueError) and "missing expected headers" in msg.lower():
        print(msg, flush=True)
        print("\nWhat this usually means:", flush=True)
        print(
            "- Your input CSV does not have the required header row.\n"
            "- The first row appears to be data, not headers.",
            flush=True,
        )
        print("\nExpected first row (example):", flush=True)
        print("entry_id,owner_new", flush=True)

        # Try to show the first line of the file to make the issue obvious.
        try:
            with open(input_filename, "r", encoding="utf-8-sig") as f:
                first_line = f.readline().strip("\n")
            if first_line:
                print("\nYour file's first row was:", flush=True)
                print(first_line, flush=True)
        except Exception:  # noqa: BLE001
            pass

        print(
            "\nFix options:\n"
            "- Add/restore the header row, OR\n"
            "- Set COLUMN_HEADER_ENTRY_ID and COLUMN_HEADER_OWNER in .env to match your headers.",
            flush=True,
        )
        return

    # Generic path for other expected exceptions.
    print(msg, flush=True)
    if isinstance(exc, FileNotFoundError):
        print(
            "\nTip: Confirm INPUT_FILENAME points to an existing CSV file.",
            flush=True,
        )
    elif isinstance(exc, RuntimeError) and "Missing required env vars" in msg:
        print(
            "\nTip: Check your .env has the required Kaltura connection settings.",
            flush=True,
        )


# -----------------------------------------------------------------------------
# Data structures
# -----------------------------------------------------------------------------


@dataclass(frozen=True)
class MappingRow:
    old_user: str
    new_user: str


@dataclass(frozen=True)
class EntryMappingRow:
    entry_id: str
    new_user: str


@dataclass
class UpdateResult:
    entry_id: str
    entry_name: str
    owner_old: str
    owner_new: str
    success: bool
    error: Optional[str] = None


# -----------------------------------------------------------------------------
# Kaltura client/session
# -----------------------------------------------------------------------------


def build_client() -> KalturaClient:
    partner_id = os.getenv("PARTNER_ID")
    admin_secret = os.getenv("ADMIN_SECRET")
    user_id = os.getenv("USER_ID")
    service_url = os.getenv("SERVICE_URL")
    privileges = os.getenv("PRIVILEGES")

    missing = [
        name
        for name, val in [
            ("PARTNER_ID", partner_id),
            ("ADMIN_SECRET", admin_secret),
            ("USER_ID", user_id),
            ("SERVICE_URL", service_url),
            ("PRIVILEGES", privileges),
        ]
        if not val
    ]
    if missing:
        raise RuntimeError(
            "Missing required env vars: " + ", ".join(missing)
        )

    config = KalturaConfiguration()
    config.serviceUrl = service_url
    config.partnerId = int(partner_id)
    client = KalturaClient(config)

    ks = client.session.start(
        admin_secret,
        user_id,
        KalturaSessionType.ADMIN,
        int(partner_id),
        privileges=privileges,
    )
    client.setKs(ks)
    return client


# -----------------------------------------------------------------------------
# CSV mapping load + validation
# -----------------------------------------------------------------------------


def read_mapping_csv(
    input_filename: str,
    header_old: str,
    header_new: str,
) -> List[MappingRow]:
    if not os.path.exists(input_filename):
        raise FileNotFoundError(f"Input CSV not found: {input_filename}")

    rows: List[MappingRow] = []
    with open(input_filename, "r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            raise ValueError("Input CSV appears to have no header row.")

        fieldnames = {h.strip(): h for h in reader.fieldnames}
        if header_old not in fieldnames or header_new not in fieldnames:
            raise ValueError(
                "Input CSV missing expected headers. "
                f"Expected: {header_old!r}, {header_new!r}. "
                f"Found: {reader.fieldnames!r}"
            )

        for i, row in enumerate(reader, start=2):
            old_user = (row.get(fieldnames[header_old]) or "").strip()
            new_user = (row.get(fieldnames[header_new]) or "").strip()

            if not old_user or not new_user:
                raise ValueError(
                    "Blank usernames are not allowed. "
                    f"Row {i} has old={old_user!r}, new={new_user!r}."
                )

            rows.append(MappingRow(old_user=old_user, new_user=new_user))

    if not rows:
        raise ValueError("Input CSV has no mapping rows.")

    # Detect conflicts/duplicates
    mapping: Dict[str, str] = {}
    for mr in rows:
        if mr.old_user in mapping and mapping[mr.old_user] != mr.new_user:
            raise ValueError(
                "Conflicting mappings for the same old user. "
                f"User {mr.old_user!r} maps to both {mapping[mr.old_user]!r} "
                f"and {mr.new_user!r}."
            )
        mapping[mr.old_user] = mr.new_user

    # Collapse duplicates (same old->same new) while preserving input order
    seen: Set[Tuple[str, str]] = set()
    collapsed: List[MappingRow] = []
    for mr in rows:
        key = (mr.old_user, mr.new_user)
        if key in seen:
            continue
        seen.add(key)
        collapsed.append(mr)

    return collapsed


def read_entry_mapping_csv(
    input_filename: str,
    header_entry_id: str,
    header_owner: str,
) -> List[EntryMappingRow]:
    if not os.path.exists(input_filename):
        raise FileNotFoundError(f"Input CSV not found: {input_filename}")

    rows: List[EntryMappingRow] = []
    with open(input_filename, "r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            raise ValueError("Input CSV appears to have no header row.")

        fieldnames = {h.strip(): h for h in reader.fieldnames}
        if header_entry_id not in fieldnames or header_owner not in fieldnames:
            raise ValueError(
                "Input CSV missing expected headers. "
                f"Expected: {header_entry_id!r}, {header_owner!r}. "
                f"Found: {reader.fieldnames!r}"
            )

        for i, row in enumerate(reader, start=2):
            entry_id = (row.get(fieldnames[header_entry_id]) or "").strip()
            new_user = (row.get(fieldnames[header_owner]) or "").strip()

            if not entry_id or not new_user:
                raise ValueError(
                    "Blank values are not allowed. "
                    f"Row {i} has entry_id={entry_id!r}, owner_new={new_user!r}."
                )

            rows.append(EntryMappingRow(entry_id=entry_id, new_user=new_user))

    if not rows:
        raise ValueError("Input CSV has no mapping rows.")

    # Detect conflicts for the same entry_id
    mapping: Dict[str, str] = {}
    for mr in rows:
        if mr.entry_id in mapping and mapping[mr.entry_id] != mr.new_user:
            raise ValueError(
                "Conflicting mappings for the same entry_id. "
                f"Entry {mr.entry_id!r} maps to both {mapping[mr.entry_id]!r} "
                f"and {mr.new_user!r}."
            )
        mapping[mr.entry_id] = mr.new_user

    # Collapse duplicates (same entry_id->same owner) while preserving input order
    seen: Set[Tuple[str, str]] = set()
    collapsed: List[EntryMappingRow] = []
    for mr in rows:
        key = (mr.entry_id, mr.new_user)
        if key in seen:
            continue
        seen.add(key)
        collapsed.append(mr)

    return collapsed




def validate_user_ids(
    client: KalturaClient,
    user_ids: Sequence[str],
    progress_every: int,
    label: str,
) -> Tuple[set[str], List[str]]:
    """Validate that user IDs exist.

    This is best-effort validation. It never raises; it returns invalid IDs and
    human-readable messages that can be written to the error log.

    Kaltura commonly returns KalturaAPIException code INVALID_USER_ID.
    """

    # Preserve deterministic order while de-duplicating.
    seen: set[str] = set()
    unique_users: List[str] = []
    for uid in user_ids:
        uid = (uid or "").strip()
        if not uid or uid in seen:
            continue
        seen.add(uid)
        unique_users.append(uid)

    total = len(unique_users)
    if total == 0:
        return set(), []

    invalid: set[str] = set()
    messages: List[str] = []
    start = time.time()

    for idx, uid in enumerate(unique_users, start=1):
        if idx == 1 or idx % max(progress_every, 1) == 0 or idx == total:
            elapsed = int(time.time() - start)
            _print_progress(
                f"  Validating {label} user {idx}/{total}: {uid} (elapsed {elapsed}s)"
            )

        try:
            client.user.get(uid)
        except Exception as exc:  # noqa: BLE001
            exc_str = str(exc)
            # Treat INVALID_USER_ID as a normal "doesn't exist" case.
            if "INVALID_USER_ID" in exc_str:
                invalid.add(uid)
                messages.append(
                    f"USER VALIDATION ({label}): {uid} INVALID_USER_ID"
                )
            else:
                invalid.add(uid)
                messages.append(
                    f"USER VALIDATION ({label}): {uid} ERROR: {exc_str}"
                )

    return invalid, messages


# -----------------------------------------------------------------------------
# BaseEntry list + update
# -----------------------------------------------------------------------------


def iter_entries_by_owner(
    client: KalturaClient,
    owner_user_id: str,
    page_size: int,
) -> Iterable[KalturaBaseEntry]:
    entry_filter = KalturaBaseEntryFilter()
    entry_filter.userIdEqual = owner_user_id

    pager = KalturaFilterPager()
    pager.pageSize = page_size

    page_index = 1
    total_count: Optional[int] = None

    while True:
        pager.pageIndex = page_index
        if page_index == 1:
            print(
                f"Listing entries for owner {owner_user_id!r} (pageSize={page_size})...",
                flush=True,
            )
        else:
            print(
                f"  Fetching page {page_index} for owner {owner_user_id!r}...",
                flush=True,
            )
        result = client.baseEntry.list(entry_filter, pager)

        if total_count is None:
            total_count = int(getattr(result, "totalCount", 0) or 0)
            # Kaltura list actions commonly cap at 10k results.
            if total_count >= 10000:
                # We do not abort automatically, but we will warn via stderr.
                print(
                    "WARNING: totalCount >= 10,000 for owner "
                    f"{owner_user_id!r}. Results may be capped by the API.",
                    file=sys.stderr,
                )

        objects = getattr(result, "objects", None) or []
        if not objects:
            break

        for obj in objects:
            yield obj

        if len(objects) < page_size:
            break

        page_index += 1


def _sleep_request_delay(delay_sec: float) -> None:
    if delay_sec > 0:
        time.sleep(delay_sec)


def update_owner_with_retry(
    client: KalturaClient,
    entry_id: str,
    entry_name: str,
    owner_old: str,
    owner_new: str,
    dry_run: bool,
    max_retries: int,
    backoff_base_sec: float,
    request_delay_sec: float,
) -> UpdateResult:
    if dry_run:
        return UpdateResult(
            entry_id=entry_id,
            entry_name=entry_name,
            owner_old=owner_old,
            owner_new=owner_new,
            success=True,
        )

    attempt = 0
    while True:
        try:
            _sleep_request_delay(request_delay_sec)
            entry_update = KalturaBaseEntry()
            entry_update.userId = owner_new
            with _CLIENT_LOCK:
                client.baseEntry.update(entry_id, entry_update)
            return UpdateResult(
                entry_id=entry_id,
                entry_name=entry_name,
                owner_old=owner_old,
                owner_new=owner_new,
                success=True,
            )
        except Exception as exc:  # noqa: BLE001
            attempt += 1
            if attempt > max_retries:
                return UpdateResult(
                    entry_id=entry_id,
                    entry_name=entry_name,
                    owner_old=owner_old,
                    owner_new=owner_new,
                    success=False,
                    error=str(exc),
                )

            # Exponential backoff + jitter
            sleep_for = backoff_base_sec * (2 ** (attempt - 1))
            sleep_for *= random.uniform(0.8, 1.2)
            time.sleep(sleep_for)


def process_entry_mapping_row(
    client: KalturaClient,
    entry_id: str,
    owner_new: str,
    dry_run: bool,
    max_retries: int,
    backoff_base_sec: float,
    request_delay_sec: float,
) -> UpdateResult:
    """Fetch entry to get name/old owner, then update owner."""
    try:
        with _CLIENT_LOCK:
            entry = client.baseEntry.get(entry_id)
        entry_name = getattr(entry, "name", "")
        owner_old = getattr(entry, "userId", "")

        # No-op (already the requested owner)
        if owner_old == owner_new:
            return UpdateResult(
                entry_id=entry_id,
                entry_name=entry_name,
                owner_old=owner_old,
                owner_new=owner_new,
                success=True,
            )

        return update_owner_with_retry(
            client,
            entry_id,
            entry_name,
            owner_old,
            owner_new,
            dry_run,
            max_retries,
            backoff_base_sec,
            request_delay_sec,
        )
    except Exception as exc:  # noqa: BLE001
        return UpdateResult(
            entry_id=entry_id,
            entry_name="",
            owner_old="",
            owner_new=owner_new,
            success=False,
            error=str(exc),
        )


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------


def main() -> int:
    _load_env()

    input_filename = os.getenv("INPUT_FILENAME", "input.csv")
    show_traceback = _env_bool("SHOW_TRACEBACK", default=False)

    # MODE determines how the input CSV is interpreted:
    # - owner_map: old_user -> new_user (reassign all entries owned by each old user)
    # - entry_map: entry_id -> owner_new (reassign only the listed entry IDs)
    mode = os.getenv("MODE", "owner_map").strip().lower()

    header_old = os.getenv("COLUMN_HEADER_OLD", "old_username")
    header_new = os.getenv("COLUMN_HEADER_NEW", "new_username")

    header_entry_id = os.getenv("COLUMN_HEADER_ENTRY_ID", "entry_id")
    header_owner = os.getenv("COLUMN_HEADER_OWNER", "owner_new")
    timezone_name = os.getenv("TIMEZONE", "UTC")

    dry_run = _env_bool("DRY_RUN", default=True)
    max_workers = _env_int("MAX_WORKERS", default=10)
    page_size = _env_int("PAGE_SIZE", default=100)
    max_retries = _env_int("MAX_RETRIES", default=3)
    backoff_base_sec = _env_float("BACKOFF_BASE_SEC", default=0.5)
    request_delay_sec = _env_float("REQUEST_DELAY_SEC", default=0.0)

    validate_old_users = _env_bool("VALIDATE_OLD_USERS", default=True)
    validate_progress_every = _env_int("VALIDATE_PROGRESS_EVERY", default=10)
    validate_new_users = _env_bool("VALIDATE_NEW_USERS", default=False)

    ts = _timestamp_tttt(timezone_name)

    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)

    run_tag = "dryRun" if dry_run else "live"

    out_csv = os.path.join(output_dir, f"reassignOwners_{run_tag}_{ts}.csv")
    out_summary = os.path.join(
        output_dir,
        f"reassignOwners_{run_tag}_{ts}_summary.txt",
    )
    out_errors = os.path.join(
        output_dir,
        f"reassignOwners_{run_tag}_{ts}_errors.txt",
    )

    _print_progress("\n=== Reassign Owners (baseEntry) ===")
    _print_progress(f"Timestamp: {ts} ({timezone_name})")
    _print_progress(f"Input CSV: {input_filename}")
    _print_progress(f"MODE: {mode}")
    _print_progress(f"DRY_RUN: {dry_run}")
    _print_progress(f"MAX_WORKERS: {max_workers} | PAGE_SIZE: {page_size}")
    _print_progress(
        f"Retries: {max_retries} | Backoff base: {backoff_base_sec}s | "
        f"Request delay: {request_delay_sec}s"
    )
    _print_progress(
        f"Validate old users: {validate_old_users} | Validate new users: {validate_new_users} | "
        f"Validate progress every: {validate_progress_every}"
    )
    _print_progress("-----------------------------------\n")

    # Basic sanity
    if max_workers < 1:
        raise ValueError("MAX_WORKERS must be >= 1")
    if page_size < 1 or page_size > 500:
        # Kaltura often allows up to 500; keep it reasonable.
        raise ValueError("PAGE_SIZE must be between 1 and 500")

    if mode not in {"owner_map", "entry_map"}:
        raise ValueError("MODE must be 'owner_map' or 'entry_map'.")

    client = build_client()

    _print_progress("Reading mapping CSV...")

    validation_errors: List[str] = []

    # Collect entries per owner and schedule updates
    results: List[UpdateResult] = []
    errors: List[str] = []

    if mode == "owner_map":
        mapping = read_mapping_csv(input_filename, header_old, header_new)
        _print_progress(f"Loaded {len(mapping)} mapping row(s) from CSV.")

        # Reject no-op rows (old==new) but don't treat as fatal
        effective_mapping = [m for m in mapping if m.old_user != m.new_user]

        if len(effective_mapping) != len(mapping):
            _print_progress(
                f"Skipped {len(mapping) - len(effective_mapping)} no-op row(s) where old==new."
            )

        if validate_old_users:
            _print_progress("Validating OLD user IDs via user.get...")
            old_ids = [m.old_user for m in effective_mapping]
            invalid_old, old_msgs = validate_user_ids(
                client,
                old_ids,
                progress_every=validate_progress_every,
                label="OLD",
            )
            validation_errors.extend(old_msgs)

            if invalid_old:
                before = len(effective_mapping)
                effective_mapping = [
                    m for m in effective_mapping if m.old_user not in invalid_old
                ]
                skipped = before - len(effective_mapping)
                _print_progress(
                    f"Skipping {skipped} mapping row(s) because OLD userId was invalid."
                )

        if validate_new_users:
            _print_progress("Validating NEW user IDs via user.get... (non-blocking)")
            new_ids = [m.new_user for m in effective_mapping]
            _, new_msgs = validate_user_ids(
                client,
                new_ids,
                progress_every=validate_progress_every,
                label="NEW",
            )
            validation_errors.extend(new_msgs)

        _print_progress("User validation phase complete.\n")

        errors.extend(validation_errors)

    else:
        entry_mapping = read_entry_mapping_csv(
            input_filename,
            header_entry_id=header_entry_id,
            header_owner=header_owner,
        )
        _print_progress(f"Loaded {len(entry_mapping)} entry mapping row(s) from CSV.")

        # Optionally validate new owners (recommended for live runs)
        if validate_new_users:
            _print_progress("Validating NEW owner user IDs via user.get... (non-blocking)")
            new_ids = [m.new_user for m in entry_mapping]
            _, new_msgs = validate_user_ids(
                client,
                new_ids,
                progress_every=validate_progress_every,
                label="NEW",
            )
            validation_errors.extend(new_msgs)

        _print_progress("User validation phase complete.\n")

        errors.extend(validation_errors)

    summary_lines: List[str] = []
    summary_lines.append(f"Timestamp: {ts} ({timezone_name})")
    summary_lines.append(f"Input CSV: {input_filename}")
    summary_lines.append(f"MODE: {mode}")
    summary_lines.append(f"DRY_RUN: {dry_run}")
    summary_lines.append(f"MAX_WORKERS: {max_workers}")
    summary_lines.append("")

    with open(out_csv, "w", newline="", encoding="utf-8") as f_out:
        writer = csv.writer(f_out)
        writer.writerow(
            ["entry_id", "entry_name", "owner_old", "owner_new", "success", "error"]
        )

        if mode == "owner_map":
            for m in effective_mapping:
                old_user = m.old_user
                new_user = m.new_user

                _print_progress(f"Processing mapping: {old_user} -> {new_user}")

                entries = list(iter_entries_by_owner(client, old_user, page_size))
                summary_lines.append(
                    f"{old_user} -> {new_user}: {len(entries)} entr(y/ies)"
                )

                _print_progress(
                    f"Found {len(entries)} entr(y/ies) owned by {old_user}."
                )

                if not entries:
                    _print_progress(f"Done with mapping: {old_user} -> {new_user}\n")
                    continue

                if dry_run:
                    _print_progress(
                        f"DRY_RUN: would update {len(entries)} entr(y/ies) from {old_user} to {new_user}."
                    )
                else:
                    _print_progress(
                        f"Updating {len(entries)} entr(y/ies) from {old_user} to {new_user}..."
                    )

                futures = []
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    for e in entries:
                        entry_id = getattr(e, "id", "")
                        entry_name = getattr(e, "name", "")
                        if not entry_id:
                            continue

                        futures.append(
                            executor.submit(
                                update_owner_with_retry,
                                client,
                                entry_id,
                                entry_name,
                                old_user,
                                new_user,
                                dry_run,
                                max_retries,
                                backoff_base_sec,
                                request_delay_sec,
                            )
                        )

                    completed = 0
                    total = len(futures)
                    for fut in as_completed(futures):
                        res = fut.result()
                        completed += 1
                        if total >= 25 and (completed % 25 == 0 or completed == total):
                            _print_progress(
                                f"  Progress for {old_user}: {completed}/{total} processed"
                            )
                        results.append(res)

                        # Always write a row so the output includes every attempted entry.
                        writer.writerow(
                            [
                                res.entry_id,
                                res.entry_name,
                                res.owner_old,
                                res.owner_new,
                                "success" if res.success else "fail",
                                "" if res.success else (res.error or ""),
                            ]
                        )

                        if not res.success:
                            msg = (
                                f"ENTRY {res.entry_id} ({res.entry_name!r}): "
                                f"{res.owner_old} -> {res.owner_new} FAILED: {res.error}"
                            )
                            errors.append(msg)

                _print_progress(f"Done with mapping: {old_user} -> {new_user}\n")

        else:
            summary_lines.append(f"Entry mapping rows: {len(entry_mapping)}")

            if dry_run:
                _print_progress(
                    f"DRY_RUN: would process {len(entry_mapping)} entryId->owner row(s)."
                )
            else:
                _print_progress(
                    f"Updating ownership for {len(entry_mapping)} entryId->owner row(s)..."
                )

            _print_progress(
                "Fetching entry details (baseEntry.get) and computing changes..."
            )

            future_to_entry_id: Dict[object, str] = {}
            futures: List[object] = []
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                for m in entry_mapping:
                    fut = executor.submit(
                        process_entry_mapping_row,
                        client,
                        m.entry_id,
                        m.new_user,
                        dry_run,
                        max_retries,
                        backoff_base_sec,
                        request_delay_sec,
                    )
                    future_to_entry_id[fut] = m.entry_id
                    futures.append(fut)

                completed = 0
                total = len(futures)

                # More frequent updates for small runs so it doesn't look hung.
                progress_every = 5 if total < 100 else 25

                start_ts = time.time()
                pending = set(futures)
                last_progress_time = time.time()

                while pending:
                    done, pending = wait(
                        pending,
                        timeout=10,
                        return_when=FIRST_COMPLETED,
                    )

                    if not done:
                        # No completions in the last 10 seconds: emit a stall hint.
                        elapsed = int(time.time() - start_ts)
                        sample_ids = [future_to_entry_id[f] for f in list(pending)[:5]]
                        _print_progress(
                            "  Still working... "
                            f"{total - len(pending)}/{total} complete (elapsed {elapsed}s). "
                            f"Pending sample: {sample_ids}"
                        )
                        continue

                    for fut in done:
                        try:
                            res = fut.result()
                        except Exception as exc:  # noqa: BLE001
                            entry_id = future_to_entry_id.get(fut, "")
                            res = UpdateResult(
                                entry_id=entry_id,
                                entry_name="",
                                owner_old="",
                                owner_new="",
                                success=False,
                                error=str(exc),
                            )

                        completed += 1
                        last_progress_time = time.time()

                        if completed % progress_every == 0 or completed == total:
                            elapsed = int(time.time() - start_ts)
                            _print_progress(
                                f"  Progress: {completed}/{total} processed (elapsed {elapsed}s)"
                            )

                        results.append(res)

                        # Always write a row so the output includes every attempted entry.
                        writer.writerow(
                            [
                                res.entry_id,
                                res.entry_name,
                                res.owner_old,
                                res.owner_new,
                                "success" if res.success else "fail",
                                "" if res.success else (res.error or ""),
                            ]
                        )

                        if not res.success:
                            msg = (
                                f"ENTRY {res.entry_id} ({res.entry_name!r}): "
                                f"{res.owner_old} -> {res.owner_new} FAILED: {res.error}"
                            )
                            errors.append(msg)

    # Write error log
    with open(out_errors, "w", encoding="utf-8") as f_err:
        if errors:
            f_err.write("\n".join(errors) + "\n")
        else:
            f_err.write("No errors.\n")

    # Write summary
    success_count = sum(1 for r in results if r.success)
    fail_count = sum(1 for r in results if not r.success)

    summary_lines.append("")
    summary_lines.append("Totals")
    summary_lines.append(f"Successful updates: {success_count}")
    summary_lines.append(f"Failed updates: {fail_count}")

    with open(out_summary, "w", encoding="utf-8") as f_sum:
        f_sum.write("\n".join(summary_lines) + "\n")

    _print_progress("\nRun complete.")
    print(f"Created: {out_csv}")
    print(f"Created: {out_summary}")
    print(f"Created: {out_errors}")

    if dry_run:
        print("DRY_RUN is enabled: no ownership changes were made.")

    # Exit non-zero if failures occurred
    return 1 if fail_count else 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(130)
    except (ValueError, FileNotFoundError, RuntimeError) as exc:
        # Expected/"user input" errors: print a friendly message.
        # Try to read INPUT_FILENAME for context; fall back to a generic name.
        input_filename = os.getenv("INPUT_FILENAME", "input.csv")
        show_traceback = _env_bool("SHOW_TRACEBACK", default=False)

        if show_traceback:
            raise

        _print_friendly_exception(exc, input_filename)
        raise SystemExit(2)
