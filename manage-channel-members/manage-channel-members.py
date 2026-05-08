"""
Manage Kaltura Channel Members

Add, remove, verify, or change the role of users in Kaltura categories
(MediaSpace channels) in bulk from a CSV.

Usage:
    python3 manage-channel-members.py <input_csv>
    python3 manage-channel-members.py   # uses INPUT_CSV_FILENAME from .env

Input CSV columns:
    username    - Kaltura/MediaSpace username
    category_id - Kaltura category ID
    action      - add | remove | verify | change_role
    role        - member | manager | contributor | moderator | owner
                  Required for: add, change_role
                  Optional for: verify (verifies role if provided)
                  Ignored for:  remove

Output (in ./output/):
    <timestamp>_manage-members-report.csv

Notes on ownership:
    Setting a user as owner (action=add or change_role with role=owner)
    updates category.owner via category.update(). Unlike the MediaSpace
    UI, the Kaltura API does NOT automatically demote the previous owner
    — their categoryUser entry is unchanged. To demote the old owner,
    add a separate change_role row for them after the ownership transfer.

    Changing the role of the current channel owner requires transferring
    ownership first (add a row with action=add, role=owner for the new
    owner), then add a second row to change the old owner's role.

Author: Galen Davis
"""

import csv
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
    KalturaCategory,
    KalturaCategoryUser,
    KalturaCategoryUserPermissionLevel,
    KalturaSessionType,
)

load_dotenv()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PARTNER_ID = int(os.getenv("PARTNER_ID", "0"))
ADMIN_SECRET = os.getenv("ADMIN_SECRET", "")
USER_ID = os.getenv("USER_ID", "")
SERVICE_URL = os.getenv("SERVICE_URL", "https://www.kaltura.com")
SESSION_EXPIRY = int(os.getenv("SESSION_EXPIRY", "86400"))
THREAD_COUNT = int(os.getenv("THREAD_COUNT", "10"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "4"))

REPORTS_DIR = "output"
os.makedirs(REPORTS_DIR, exist_ok=True)
RUN_TIMESTAMP = datetime.now().strftime("%Y-%m-%d-%H%M")
OUTPUT_CSV = os.path.join(
    REPORTS_DIR, f"{RUN_TIMESTAMP}_manage-members-report.csv"
)

OUTPUT_FIELDS = ["username", "category_id", "action", "role", "result"]

VALID_ACTIONS = {"add", "remove", "verify", "change_role"}
VALID_ROLES = {"member", "manager", "contributor", "moderator", "owner"}
ROLES_REQUIRING_ROLE_FIELD = {"add", "change_role"}

ROLE_TO_PERM = {
    "manager": KalturaCategoryUserPermissionLevel.MANAGER,
    "moderator": KalturaCategoryUserPermissionLevel.MODERATOR,
    "contributor": KalturaCategoryUserPermissionLevel.CONTRIBUTOR,
    "member": KalturaCategoryUserPermissionLevel.MEMBER,
}

# Permission level integers returned by the API -> role name
PERM_TO_ROLE = {0: "manager", 1: "moderator", 2: "contributor", 3: "member"}

if not PARTNER_ID or not ADMIN_SECRET:
    print(
        "Error: PARTNER_ID and ADMIN_SECRET must be set in your .env file."
    )
    sys.exit(1)

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
    config.serviceUrl = SERVICE_URL
    config.partnerId = PARTNER_ID
    client = KalturaClient(config)
    ks = client.session.start(
        ADMIN_SECRET,
        USER_ID,
        KalturaSessionType.ADMIN,
        PARTNER_ID,
        expiry=SESSION_EXPIRY,
        privileges="all:*,disableentitlement",
    )
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
        "not_found", "invalid_object",
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
# Member lookup
# ---------------------------------------------------------------------------

def _is_not_found(exc: Exception) -> bool:
    msg = str(exc).lower()
    return any(p in msg for p in (
        "not_found", "invalid_object_id", "category_user_not_found",
    ))


def get_member_role(client, category_id: str, username: str):
    """
    Return the user's current role string in the category
    ('owner', 'manager', 'moderator', 'contributor', 'member'),
    or None if the user is not in the category at all.
    Raises on unexpected API errors.
    """
    cat = client.category.get(int(category_id))
    if cat.owner == username:
        return "owner"
    try:
        cu = client.categoryUser.get(int(category_id), username)
        return PERM_TO_ROLE.get(int(cu.permissionLevel), "unknown")
    except Exception as exc:
        if _is_not_found(exc):
            return None
        raise


# ---------------------------------------------------------------------------
# Row helpers
# ---------------------------------------------------------------------------

def make_result(row: dict, result: str) -> dict:
    return {
        "username": row["username"].strip(),
        "category_id": row["category_id"].strip(),
        "action": row["action"].strip(),
        "role": row.get("role", "").strip(),
        "result": result,
    }


# ---------------------------------------------------------------------------
# Per-row processor
# ---------------------------------------------------------------------------

def process_row(row: dict) -> dict:
    username = row["username"].strip()
    category_id = row["category_id"].strip()
    action = row["action"].strip().lower()
    role = row.get("role", "").strip().lower()
    client = get_client()

    log(f"  [{action}] {username} / category {category_id}")

    try:
        # ----------------------------------------------------------------
        if action == "add":
            if role == "owner":
                cat = KalturaCategory()
                cat.owner = username
                cid = int(category_id)
                with_retry(
                    lambda: client.category.update(cid, cat),
                    label=f"set owner {username}",
                )
                result = (
                    "set as owner — previous owner's role was not "
                    "automatically changed"
                )
            else:
                cu = KalturaCategoryUser()
                cu.categoryId = int(category_id)
                cu.userId = username
                cu.permissionLevel = ROLE_TO_PERM[role]
                with_retry(
                    lambda: client.categoryUser.add(cu),
                    label=f"add {username}",
                )
                result = f"added as {role}"

        # ----------------------------------------------------------------
        elif action == "remove":
            cat = client.category.get(int(category_id))
            if cat.owner == username:
                result = (
                    "error: user is the channel owner and cannot be "
                    "removed without transferring ownership first"
                )
            else:
                try:
                    cid = int(category_id)
                    uname = username
                    with_retry(
                        lambda: client.categoryUser.delete(cid, uname),
                        label=f"remove {username}",
                    )
                    result = "removed"
                except Exception as exc:
                    result = (
                        "not in channel"
                        if _is_not_found(exc)
                        else f"error: {exc}"
                    )

        # ----------------------------------------------------------------
        elif action == "verify":
            current_role = get_member_role(client, category_id, username)
            if current_role is None:
                result = (
                    f"not in channel (expected {role})"
                    if role else "not in channel"
                )
            elif not role:
                result = f"in channel as {current_role}"
            elif current_role == role:
                result = (
                    f"in channel as {current_role} (matches expected)"
                )
            else:
                result = (
                    f"in channel as {current_role} (expected {role})"
                )

        # ----------------------------------------------------------------
        elif action == "change_role":
            current_role = get_member_role(client, category_id, username)
            if current_role is None:
                result = "not in channel — no change made"
            elif current_role == role:
                result = f"already {role} — no change made"
            elif current_role == "owner":
                result = (
                    "error: cannot change the channel owner's role "
                    "without first transferring ownership — add a row "
                    "with action=add and role=owner for the new owner, "
                    "then change this user's role"
                )
            elif role == "owner":
                cid = int(category_id)
                cat = KalturaCategory()
                cat.owner = username
                with_retry(
                    lambda: client.category.update(cid, cat),
                    label=f"set owner {username}",
                )
                result = (
                    f"role changed from {current_role} to owner — "
                    "previous owner's role was not automatically changed"
                )
            else:
                cu = KalturaCategoryUser()
                cu.categoryId = int(category_id)
                cu.userId = username
                cu.permissionLevel = ROLE_TO_PERM[role]
                cid = int(category_id)
                uname = username
                with_retry(
                    lambda: client.categoryUser.update(cid, uname, cu),
                    label=f"change role {username}",
                )
                result = f"role changed from {current_role} to {role}"

        # ----------------------------------------------------------------
        else:
            result = f"error: unknown action '{action}'"

    except Exception as exc:
        result = f"error: {exc}"

    log(f"  [{action}] {username}: {result}")
    return make_result(row, result)


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
                "Pass it as an argument or set "
                "INPUT_CSV_FILENAME in .env."
            )
            sys.exit(1)

    if not os.path.exists(input_csv):
        print(f"File not found: {input_csv}")
        sys.exit(1)

    print(f"Reading {input_csv}...")
    rows = []
    with open(input_csv, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        headers = set(reader.fieldnames or [])
        required = {"username", "category_id", "action"}
        missing = required - headers
        if missing:
            print(
                f"Error: missing columns: {', '.join(sorted(missing))}"
            )
            sys.exit(1)

        for i, row in enumerate(reader, start=2):
            action = row.get("action", "").strip().lower()
            role = row.get("role", "").strip().lower()
            errs = []
            if action not in VALID_ACTIONS:
                errs.append(
                    f"invalid action '{action}' — "
                    f"must be one of: {', '.join(sorted(VALID_ACTIONS))}"
                )
            if action in ROLES_REQUIRING_ROLE_FIELD and not role:
                errs.append(
                    f"role is required for action '{action}'"
                )
            if role and role not in VALID_ROLES:
                errs.append(
                    f"invalid role '{role}' — "
                    f"must be one of: {', '.join(sorted(VALID_ROLES))}"
                )
            if errs:
                for e in errs:
                    print(f"Row {i}: {e}")
                sys.exit(1)
            rows.append(row)

    print(f"  {len(rows)} row(s) to process.")
    print(f"Processing with {THREAD_COUNT} thread(s)...")

    results = []
    with ThreadPoolExecutor(max_workers=THREAD_COUNT) as pool:
        futures = {
            pool.submit(process_row, row): row for row in rows
        }
        for future in as_completed(futures):
            results.append(future.result())

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        writer.writerows(results)

    print(f"\nDone. Report: {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
