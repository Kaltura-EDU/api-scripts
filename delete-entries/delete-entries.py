"""
This script permanently deletes or recycles one or more Kaltura media entries
based on entry IDs provided by the user. It authenticates using an admin
session, retrieves entry metadata for confirmation, and writes a report to a
timestamped CSV file before deletion or recycling.

Key features:
- Accepts entry IDs via .env or a CSV file.
- Retrieves and displays entry metadata (name, owner, duration).
- Exports a report CSV listing all entries and deletion/recycling status.
- Skips any entries that cannot be retrieved.
- Requires user confirmation before performing deletions/recycling.
- DRY_RUN=true skips confirmation and API calls; writes a result CSV with
  status "DRY RUN" so you can verify the entry list before committing.
- MAX_WORKERS controls concurrent API calls (default 1).
- DELETE_RATE_PER_SEC paces calls to stay under Kaltura's delete throttle,
  which rejects the excess with ACTION_BLOCKED rather than queueing it.
  ACTION_BLOCKED responses are retried with backoff.
- Transient network failures (timeouts, dropped connections) are retried
  automatically; see MAX_NETWORK_RETRIES / NETWORK_RETRY_DELAY.
- LOOKUP_BEFORE_ACTION=true fetches entry metadata before deleting, giving
  richer output columns (name, owner, duration, plays). Set to false to skip
  the lookup phase and go straight to deletion — faster, but those columns
  will be blank in the result CSV.

Usage:
    1. Set your partner ID and other configuration in the .env file.
    2. Enter the entry IDs in the .env file or in a dedicated CSV file.
    3. Run the script and enter your admin secret when prompted. The
       secret is never read from or stored in .env.
    4. To proceed with deletion, type "DELETE" when prompted for confirmation.
       To proceed with recycling, type "RECYCLE" when prompted.
"""

import csv
import getpass
import html
import os
import sys
import textwrap
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv
from KalturaClient import KalturaClient, KalturaConfiguration
from KalturaClient.Plugins.Core import KalturaSessionType
from KalturaClient.exceptions import KalturaClientException


# =============================================================================
# Env / config ----------------------------------------------------------------
# =============================================================================
load_dotenv(dotenv_path=Path(__file__).with_name(".env"))


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
    return datetime.now().strftime("%Y-%m-%d-%H%M")


def _wrap(text: str, indent: str, first: Optional[str] = None) -> str:
    """Wrap summary text to 78 columns so long messages stay readable."""
    return textwrap.fill(
        text,
        width=78,
        initial_indent=first if first is not None else indent,
        subsequent_indent=indent,
    )


PARTNER_ID = require_env_int("PARTNER_ID")
ADMIN_SECRET = ""  # set in main() via getpass

USER_ID = os.getenv("USER_ID", "").strip()
SERVICE_URL = os.getenv("SERVICE_URL", "https://www.kaltura.com").rstrip("/")
PRIVILEGES = os.getenv("PRIVILEGES", "all:*,disableentitlement")

DRY_RUN = (
    os.getenv("DRY_RUN", "").strip().lower()
    in {"1", "true", "yes", "y", "on"}
)
MAX_WORKERS = max(1, int(os.getenv("MAX_WORKERS", "1").strip() or "1"))
REQUEST_TIMEOUT_SEC = max(
    5, int(os.getenv("REQUEST_TIMEOUT_SEC", "30").strip() or "30")
)
REQUEST_CONNECT_TIMEOUT_SEC = max(
    3, int(os.getenv("REQUEST_CONNECT_TIMEOUT_SEC", "10").strip() or "10")
)
# Attempts (including the first) before a transient network error gives up.
MAX_NETWORK_RETRIES = max(
    1, int(os.getenv("MAX_NETWORK_RETRIES", "5").strip() or "5")
)
# Base seconds between retries; grows linearly (delay x attempt).
NETWORK_RETRY_DELAY = max(
    1, int(os.getenv("NETWORK_RETRY_DELAY", "5").strip() or "5")
)
# Kaltura throttles deletes. Measured on a production account: while the
# script attempted ~21.6 calls/sec, successful deletes held at ~3.1-3.6/sec
# and the excess came back as ACTION_BLOCKED. Pacing the calls means nearly
# all of them land instead of ~15%. Set to 0 to disable pacing.
DELETE_RATE_PER_SEC = float(
    os.getenv("DELETE_RATE_PER_SEC", "2.5").strip() or "2.5"
)
# ACTION_BLOCKED is a throttle response, not a permanent state, so it is
# worth retrying after a pause.
BLOCKED_RETRIES = max(
    0, int(os.getenv("BLOCKED_RETRIES", "4").strip() or "4")
)
BLOCKED_RETRY_DELAY = max(
    1, int(os.getenv("BLOCKED_RETRY_DELAY", "10").strip() or "10")
)

LOOKUP_BEFORE_ACTION = (
    os.getenv("LOOKUP_BEFORE_ACTION", "true").strip().lower()
    not in {"0", "false", "no", "n", "off"}
)
ENTRY_IDS = get_env_csv("ENTRY_IDS")

CSV_FILENAME = os.getenv("CSV_FILENAME", "").strip()
ENTRY_ID_COLUMN_HEADER = os.getenv("ENTRY_ID_COLUMN_HEADER", "").strip()


# =============================================================================
# Kaltura session helpers -----------------------------------------------------
# =============================================================================

def call_with_retry(fn, *args, **kwargs):
    """Call fn(*args, **kwargs), retrying with linear backoff on transient
    network failures (timeouts, connection resets). Kaltura raises those as
    KalturaClientException, which is NOT a subclass of KalturaException, so
    it is caught here separately; requests' own errors are covered too. A
    well-formed API error is never retried here — callers handle those."""
    for attempt in range(1, MAX_NETWORK_RETRIES + 1):
        try:
            return fn(*args, **kwargs)
        except (
            KalturaClientException,
            requests.exceptions.RequestException,
        ) as exc:
            if attempt == MAX_NETWORK_RETRIES:
                raise
            # Record the retry: for a non-idempotent call like delete, the
            # lost request may already have done the work server-side.
            _thread_local.retried = True
            delay = NETWORK_RETRY_DELAY * attempt
            print(
                f"    [network error: {exc}; retry "
                f"{attempt}/{MAX_NETWORK_RETRIES} in {delay}s]"
            )
            time.sleep(delay)


def build_client() -> KalturaClient:
    config = KalturaConfiguration(PARTNER_ID)
    config.serviceUrl = SERVICE_URL
    config.requestTimeout = REQUEST_TIMEOUT_SEC
    client = KalturaClient(config)
    try:
        ks = call_with_retry(
            client.session.start,
            ADMIN_SECRET,
            USER_ID,
            KalturaSessionType.ADMIN,
            PARTNER_ID,
            privileges=PRIVILEGES,
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
        elif isinstance(e, KalturaClientException):
            print(
                "\n❌ Could not reach Kaltura to start a session.\n"
                f"   {e}\n   Check your internet connection and try again.\n"
            )
        else:
            print(f"\n❌ Could not start Kaltura session: {e}\n")
        raise SystemExit(1)
    client.setKs(ks)
    return client


class RateLimiter:
    """Spaces calls evenly across all worker threads.

    Threads reserve the next slot under a lock, then sleep outside it, so a
    waiting thread never holds up the others' reservations.
    """

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
        delay = slot - time.monotonic()
        if delay > 0:
            time.sleep(delay)


_rate_limiter = RateLimiter(DELETE_RATE_PER_SEC)


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
) -> requests.Response:
    data: Dict = {"entryId": entry_id, "ks": ks}
    return call_with_retry(
        requests.post,
        _api_url(action),
        data=data,
        timeout=(REQUEST_CONNECT_TIMEOUT_SEC, REQUEST_TIMEOUT_SEC),
    )


def _xml_field(text: str, tag: str) -> str:
    """Pull one tag's text out of an API response.

    The value is XML-escaped on the wire, so an API message reads
    `Action &quot;delete&quot; ... is blocked` and an entry named
    `Ben & Jerry's` arrives as `Ben &amp; Jerry&#39;s`. Unescape it here so
    both the console and the CSV show what a person actually wrote.
    """
    start = text.find(f"<{tag}>")
    end = text.find(f"</{tag}>")
    if start != -1 and end != -1 and end > start:
        return html.unescape(text[start + len(tag) + 2:end].strip())
    return ""


def _response_error(
    response: requests.Response,
) -> Tuple[Optional[str], str]:
    """Return an (error code, message) pair; the code is None on success.

    A non-2xx response (gateway error, HTML error page) is a failure even
    though it carries no Kaltura error block — without this check such a
    response would read as an empty-but-valid entry.
    """
    text = response.text or ""
    if not response.ok:
        detail = text.strip()[:200] or response.reason or ""
        return f"HTTP {response.status_code}", detail

    # Kaltura errors arrive as <error><code>..</code><message>..</message>.
    # Scope the search to that block so a field of the entry itself can
    # never be mistaken for an error code.
    start = text.find("<error>")
    if start != -1:
        end = text.find("</error>", start)
        block = text[start:end] if end != -1 else text[start:]
        return (
            _xml_field(block, "code") or "API_ERROR",
            _xml_field(block, "message") or block.strip(),
        )

    code = _xml_field(text, "code") or None
    message = _xml_field(text, "message") or text.strip()
    return code, message


# Extra guidance for error codes that have a known next step, shown once in
# the end-of-run summary rather than on all 162 lines of an identical failure.
FAILURE_HINTS = {
    "ACTION_BLOCKED": (
        "This is Kaltura's throttle response, not a property of these"
        " entries — the same entry typically deletes fine on a later"
        " attempt. It means calls were still arriving faster than the"
        " account allows. Lower DELETE_RATE_PER_SEC, or raise"
        " BLOCKED_RETRIES / BLOCKED_RETRY_DELAY, then re-run with just"
        " the failed IDs."
    ),
    "ENTRY_ID_NOT_FOUND": (
        "These IDs do not exist in this partner account — check for typos,"
        " already-deleted entries, or IDs from a different account."
    ),
}


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
    script_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(script_dir, CSV_FILENAME)
    entry_ids = []
    try:
        with open(csv_path, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            reader.fieldnames = [
                h.strip().strip('"') for h in reader.fieldnames
            ]
            for row in reader:
                eid = (row.get(ENTRY_ID_COLUMN_HEADER, "") or "").strip()
                if eid:
                    entry_ids.append(eid)
    except Exception as ex:
        print(
            f"[ERROR] Failed to load entry IDs from CSV:"
            f" {csv_path}: {ex}",
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
        for attempt in range(BLOCKED_RETRIES + 1):
            _rate_limiter.wait()
            _thread_local.retried = False
            response = _raw_call(action, eid, get_thread_ks())
            code, message = _response_error(response)
            # ACTION_BLOCKED means we outran Kaltura's throttle, not that
            # this entry is undeletable. Back off and try it again.
            if code == "ACTION_BLOCKED" and attempt < BLOCKED_RETRIES:
                delay = BLOCKED_RETRY_DELAY * (attempt + 1)
                print(
                    f"[THROTTLED] Entry {eid} blocked; retry"
                    f" {attempt + 1}/{BLOCKED_RETRIES} in {delay}s"
                )
                time.sleep(delay)
                continue
            break
        if code == "ENTRY_ID_NOT_FOUND" and getattr(
            _thread_local, "retried", False
        ):
            # The entry was there at lookup and is gone now, and our own
            # call to it timed out and was re-sent. A delete is not
            # idempotent: the lost request reached Kaltura and did the
            # work, so the retry found nothing left to delete. Reporting
            # this as a failure would send the operator chasing an entry
            # that is already gone.
            print(
                f"[{action_log}] Entry {eid}"
                " (first attempt timed out; retry confirmed it gone)"
            )
            out["status"] = f"{action_log} (first attempt timed out)"
        elif code:
            print(
                f"[SKIPPED] Entry {eid} could not be"
                f" {action_log.lower()} ({code}): {message}"
            )
            out["status"] = f"FAILED: {code}"
            out["_message"] = message
        else:
            print(f"[{action_log}] Entry {eid}")
            out["status"] = action_log
    except requests.RequestException as e:
        print(
            f"[SKIPPED] Entry {eid} could not be {action_log.lower()}: {e}"
        )
        out["status"] = "FAILED: connection error"
        out["_message"] = str(e)
    return out


# =============================================================================
# Main ------------------------------------------------------------------------
# =============================================================================

def main():
    global ADMIN_SECRET

    # Resolve the entry IDs before prompting, so a configuration mistake
    # surfaces before the user types a secret.
    if CSV_FILENAME:
        entry_ids = load_entry_ids_from_csv()
        empty_hint = (
            f"\n[ERROR] No entry IDs found in {CSV_FILENAME} under the"
            f" column '{ENTRY_ID_COLUMN_HEADER}'. Check CSV_FILENAME and"
            " ENTRY_ID_COLUMN_HEADER in .env. Exiting."
        )
    elif ENTRY_IDS:
        entry_ids = ENTRY_IDS
        empty_hint = "\n[ERROR] ENTRY_IDS is empty. Exiting."
    else:
        print(
            "\n[ERROR] No valid ENTRY_IDS or CSV_FILENAME /"
            " ENTRY_ID_COLUMN_HEADER env variables. Exiting."
        )
        sys.exit(1)

    if not entry_ids:
        print(empty_hint)
        sys.exit(1)

    # Deleting the same entry twice reports a spurious failure on the
    # second pass, so collapse duplicates while preserving input order.
    deduped = list(dict.fromkeys(entry_ids))
    if len(deduped) != len(entry_ids):
        print(
            f"[INFO] Ignored {len(entry_ids) - len(deduped)} duplicate"
            " entry ID(s) in the input."
        )
    entry_ids = deduped

    ADMIN_SECRET = getpass.getpass("Enter your Kaltura admin secret: ")
    if not ADMIN_SECRET:
        print("[ERROR] Admin secret cannot be empty.", file=sys.stderr)
        sys.exit(1)

    # Start a session now so bad credentials fail here with a readable
    # message, rather than inside every worker thread mid-run.
    _thread_local.client = build_client()

    ts = now_stamp()
    output_dir = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "output"
    )
    os.makedirs(output_dir, exist_ok=True)
    preview_csv = os.path.join(
        output_dir, f"{ts}_deleted_entries_PREVIEW.csv"
    )
    result_csv = os.path.join(
        output_dir, f"{ts}_deleted_entries_RESULT.csv"
    )

    fieldnames = [
        "entry_id", "entry_name", "owner_user_id",
        "duration_seconds", "plays", "status",
    ]

    total = len(entry_ids)

    # Lookup phase ------------------------------------------------------------
    if LOOKUP_BEFORE_ACTION:
        print(
            f"\n[INFO] Looking up {total} entries"
            f" (MAX_WORKERS={MAX_WORKERS})..."
        )

        report_by_id: Dict[str, Dict] = {}
        looked_up = 0

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = {
                pool.submit(lookup_one, eid): eid for eid in entry_ids
            }
            for fut in as_completed(futures):
                result = fut.result()
                report_by_id[result["entry_id"]] = result
                looked_up += 1
                if looked_up % 100 == 0 or looked_up == total:
                    print(f"  {looked_up}/{total} looked up...")

        report = [
            report_by_id[eid]
            for eid in entry_ids
            if eid in report_by_id
        ]

        if all(r["status"] != "FOUND" for r in report):
            print("\n[INFO] No valid entries to delete. Exiting.")
            with open(
                preview_csv, mode="w", newline="", encoding="utf-8"
            ) as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(report)
            return

        with open(preview_csv, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(report)
        print(f"\n[INFO] Wrote preview to {preview_csv}")
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

    # Dry run -----------------------------------------------------------------
    if DRY_RUN:
        print("\n[DRY RUN] No entries will be deleted or recycled.")
        for row in report:
            if row.get("status") == "FOUND":
                row["status"] = "DRY RUN"
        with open(result_csv, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(report)
        print(f"[DRY RUN] Wrote result to {result_csv}")
        return

    # Confirm -----------------------------------------------------------------
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
            return

    # Action phase ------------------------------------------------------------
    found_rows = [r for r in report if r.get("status") == "FOUND"]
    skipped_rows = [r for r in report if r.get("status") != "FOUND"]
    total_found = len(found_rows)
    print(f"\n[INFO] Running {action_log} on {total_found} entries...")
    if DELETE_RATE_PER_SEC > 0:
        eta = total_found / DELETE_RATE_PER_SEC
        print(
            f"[INFO] Pacing at {DELETE_RATE_PER_SEC:g} calls/sec to stay"
            f" under Kaltura's throttle — roughly {eta / 60:.1f} min"
            " at best, longer if any need retrying."
        )
    print(f"[INFO] Writing results incrementally to {result_csv}")

    with open(result_csv, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(skipped_rows)

    write_lock = threading.Lock()
    completed = 0
    processed_count = 0
    failure_counts: Dict[str, int] = {}
    failure_messages: Dict[str, str] = {}

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {
            pool.submit(
                action_one, row, action, action_log
            ): row["entry_id"]
            for row in found_rows
        }
        for fut in as_completed(futures):
            result = fut.result()
            completed += 1
            status = result.get("status", "")
            if status.startswith(action_log):
                processed_count += 1
            elif status.startswith("FAILED: "):
                code = status[len("FAILED: "):]
                failure_counts[code] = failure_counts.get(code, 0) + 1
                failure_messages.setdefault(code, result.get("_message", ""))
            # _message is for the summary only; keep it out of the CSV.
            row_out = {k: result.get(k, "") for k in fieldnames}
            with write_lock:
                with open(
                    result_csv, mode="a", newline="", encoding="utf-8"
                ) as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writerow(row_out)
            if completed % 100 == 0 or completed == total_found:
                print(f"  {completed}/{total_found} processed...")

    print(
        f"\n[INFO] {processed_count} entries successfully"
        f" {action_log.lower()}."
    )

    if failure_counts:
        failed_total = sum(failure_counts.values())
        print(
            f"[INFO] {failed_total} entries could not be"
            f" {action_log.lower()}:"
        )
        for code, count in sorted(
            failure_counts.items(), key=lambda kv: (-kv[1], kv[0])
        ):
            print(f"\n       {count:>6} × {code}")
            message = failure_messages.get(code, "")
            if message:
                print(_wrap(message, "              "))
            hint = FAILURE_HINTS.get(code)
            if hint:
                print(
                    _wrap(hint, " " * 16, first=" " * 14 + "→ ")
                )

    if skipped_rows:
        print(
            f"[INFO] {len(skipped_rows)} entries were skipped before the"
            f" {action_log.lower()} step (see the status column)."
        )

    print(f"[INFO] Wrote report to {result_csv}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[ABORTED] Interrupted by user.")
        sys.exit(130)
    except Exception as e:
        print(f"[ERROR] Unhandled error: {e}", file=sys.stderr)
        traceback.print_exc()
        sys.exit(1)
