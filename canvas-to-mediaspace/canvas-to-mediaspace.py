"""
Canvas to MediaSpace Emergency Migration
========================================
Creates MediaSpace channels from Canvas courses and publishes all Canvas
media gallery entries (including embedded/InContext entries) to the new
channels.

Usage:
    python3 canvas-to-mediaspace.py <courses_csv> <users_csv>

    Input files can also be set in .env (CLI args override .env values).

Courses CSV columns (required):
    course_id                   - SIS course ID; becomes the MediaSpace
                                  channel name
    canvas_course_id            - 5-digit Canvas course ID; used to find
                                  the Kaltura category
    courseDisplayName           - Human-readable course name (display only)
    primary_instructor_username - Set as channel owner

Users CSV columns (required):
    username      - Kaltura/MediaSpace username
    sis_course_id - Matches course_id in the courses CSV
    role          - Canvas role (Student, Teacher, TA, etc.)

Role -> channel membership mapping:
    primary_instructor_username (from courses CSV) -> owner
    Teacher role (not already the owner)           -> manager
    All other roles                                -> member

Resume behavior:
    If a previous run was interrupted, re-running with the same input
    files automatically picks up from where it left off. To force a
    fresh run, delete output/.run_state.json first.

Outputs (in ./output/, written incrementally as courses complete):
    <timestamp>_channel_mapping.csv
    <timestamp>_channel_members.csv
    <timestamp>_published_entries.csv

Author: Galen Davis
"""

import csv
import json
import os
import random
import subprocess
import sys
import threading
import time
from collections import OrderedDict, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from urllib.parse import quote_plus

import pytz
from dotenv import load_dotenv
from KalturaClient import KalturaClient, KalturaConfiguration
from KalturaClient.Plugins.Core import (
    KalturaBaseEntryFilter,
    KalturaCategory,
    KalturaCategoryEntry,
    KalturaCategoryEntryFilter,
    KalturaCategoryFilter,
    KalturaCategoryUser,
    KalturaCategoryUserPermissionLevel,
    KalturaFilterPager,
    KalturaSessionType,
)

load_dotenv()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PARTNER_ID = int(os.getenv("PARTNER_ID", "2323111"))
ADMIN_SECRET = os.getenv("ADMIN_SECRET", "REDACTED")
USER_ID = os.getenv("USER_ID", "api-gbdavis")
SERVICE_URL = os.getenv("SERVICE_URL", "https://www.kaltura.com")

PARENT_ID = int(os.getenv("PARENT_ID", "78494121"))
PRIVACY_CONTEXT = os.getenv("PRIVACY_CONTEXT", "MediaSpace")
FULL_NAME_PREFIX = os.getenv(
    "FULL_NAME_PREFIX", "MediaSpace>site>channels>"
)
MEDIA_SPACE_BASE_URL = os.getenv(
    "MEDIA_SPACE_BASE_URL", "https://mediaspace.ucsd.edu/channel/"
)

# Channel creation settings (matching existing channel defaults)
# privacy/list=3 (members only), join=3 (invite only), inheritance=2
CHANNEL_PRIVACY = int(os.getenv("CHANNEL_PRIVACY", "3"))
USER_JOIN_POLICY = int(os.getenv("USER_JOIN_POLICY", "3"))
APPEAR_IN_LIST = int(os.getenv("APPEAR_IN_LIST", "3"))
INHERITANCE_TYPE = int(os.getenv("INHERITANCE_TYPE", "2"))
DEFAULT_PERMISSION_LEVEL = int(os.getenv("DEFAULT_PERMISSION_LEVEL", "3"))
CONTRIBUTION_POLICY = int(os.getenv("CONTRIBUTION_POLICY", "2"))
MODERATION = int(os.getenv("MODERATION", "0"))

CANVAS_CAT_PREFIX = os.getenv(
    "CANVAS_CAT_PREFIX", "Canvas_Prod>site>channels>"
)

# Session TTL in seconds. Default 86400 = 24 hours.
SESSION_EXPIRY = int(os.getenv("SESSION_EXPIRY", "86400"))

# Outer thread pool: courses processed concurrently.
# Recommended range: 3–10.
THREAD_COUNT = int(os.getenv("THREAD_COUNT", "5"))

# Inner thread pool: member additions and entry publications per course.
# THREAD_COUNT * MEMBER_THREADS = max concurrent Kaltura requests.
# Recommended range: 5–15.
MEMBER_THREADS = int(os.getenv("MEMBER_THREADS", "10"))

# Total retry attempts per API call (1 = no retry, 4 = 3 retries).
# Retries use exponential backoff. Set to 1 to disable.
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "4"))

PACIFIC = pytz.timezone("America/Los_Angeles")
REPORTS_DIR = "output"
os.makedirs(REPORTS_DIR, exist_ok=True)

# Set in main(); may be overridden on resume to match the prior run
RUN_TIMESTAMP = datetime.now().strftime("%Y-%m-%d-%H%M")

STATE_FILE = os.path.join(REPORTS_DIR, ".run_state.json")

# ---------------------------------------------------------------------------
# Output CSV field definitions (module-level so open/resume logic can share)
# ---------------------------------------------------------------------------
MAPPING_FIELDS = [
    "course_id", "canvas_course_id", "canvas_display_name",
    "primary_instructor", "canvas_kaltura_category_id",
    "canvas_kaltura_category_fullname", "ms_channel_name",
    "ms_channel_id", "ms_channel_url",
]
MEMBERS_FIELDS = [
    "course_id", "canvas_course_id", "ms_channel_name",
    "ms_channel_id", "username", "canvas_role", "ms_role", "add_status",
]
ENTRIES_FIELDS = [
    "course_id", "canvas_course_id", "ms_channel_name",
    "ms_channel_id", "entry_id", "entry_name", "creator_user_id",
    "created_at_pacific", "publish_status",
]

# ---------------------------------------------------------------------------
# Thread safety
# ---------------------------------------------------------------------------
_thread_local = threading.local()
_print_lock = threading.Lock()


def log(course_id: str, msg: str):
    """Thread-safe print prefixed with [course_id]."""
    with _print_lock:
        print(f"[{course_id}] {msg}")


# ---------------------------------------------------------------------------
# Caffeinate
# ---------------------------------------------------------------------------

def start_caffeinate():
    """Prevent macOS from idle-sleeping; no-op on other platforms."""
    if sys.platform != "darwin":
        return
    try:
        subprocess.Popen(
            ["caffeinate", "-i", "-w", str(os.getpid())],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        print("caffeinate active — system sleep prevented")
    except FileNotFoundError:
        print(
            "WARNING: caffeinate not found; "
            "system may sleep during long runs"
        )


# ---------------------------------------------------------------------------
# Retry helper
# ---------------------------------------------------------------------------

def _is_retryable(exc: Exception) -> bool:
    """Return False for errors that won't benefit from a retry."""
    msg = str(exc).lower()
    no_retry_phrases = (
        "duplicate", "already exist", "already assigned",
        "invalid ks", "invalid session",
    )
    return not any(p in msg for p in no_retry_phrases)


def with_retry(fn, course_id: str = "", label: str = ""):
    """
    Call fn() up to MAX_RETRIES times with exponential backoff.
    Raises the final exception if all attempts fail.
    Skips retry for errors that are clearly permanent (duplicates, etc.).
    """
    last_exc = None
    for attempt in range(MAX_RETRIES):
        try:
            return fn()
        except Exception as exc:
            last_exc = exc
            if attempt < MAX_RETRIES - 1 and _is_retryable(exc):
                delay = (2 ** attempt) + random.uniform(0, 1)
                log(
                    course_id,
                    f"  Retry {attempt + 1}/{MAX_RETRIES - 1} for "
                    f"{label} in {delay:.1f}s: {exc}"
                )
                time.sleep(delay)
            else:
                raise
    raise last_exc  # unreachable, but satisfies type checkers


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
    """Return this thread's Kaltura client, creating it on first use."""
    if not hasattr(_thread_local, "client"):
        _thread_local.client = create_client()
    return _thread_local.client


# ---------------------------------------------------------------------------
# Input parsing
# ---------------------------------------------------------------------------

def load_courses(csv_path: str) -> OrderedDict:
    courses: OrderedDict = OrderedDict()
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            cid = row["course_id"].strip()
            courses[cid] = {
                "canvas_course_id": row["canvas_course_id"].strip(),
                "display_name": row["courseDisplayName"].strip(),
                "owner": row["primary_instructor_username"].strip(),
            }
    return courses


def load_users(csv_path: str) -> dict:
    users: dict = defaultdict(list)
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            users[row["sis_course_id"].strip()].append(
                {
                    "username": row["username"].strip(),
                    "role": row["role"].strip(),
                }
            )
    return users


# ---------------------------------------------------------------------------
# Resume state (stores only completed course IDs — rows go straight to CSV)
# ---------------------------------------------------------------------------

def save_state(
    completed_ids: set,
    courses_csv_abs: str,
    users_csv_abs: str,
):
    """Write a small state file containing only completed course IDs."""
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(
            {
                "timestamp": RUN_TIMESTAMP,
                "courses_csv": courses_csv_abs,
                "users_csv": users_csv_abs,
                "completed_course_ids": list(completed_ids),
            },
            f,
        )


def load_state(courses_csv_abs: str, users_csv_abs: str):
    """
    Return (completed_ids, timestamp) if a matching state file exists,
    else None.
    """
    if not os.path.exists(STATE_FILE):
        return None
    try:
        with open(STATE_FILE, encoding="utf-8") as f:
            state = json.load(f)
    except Exception as exc:
        print(f"WARNING: could not read state file: {exc}")
        return None
    if (
        state.get("courses_csv") != courses_csv_abs
        or state.get("users_csv") != users_csv_abs
    ):
        print(
            "Found a state file for different input files; "
            "ignoring it and starting fresh."
        )
        return None
    return set(state["completed_course_ids"]), state["timestamp"]


# ---------------------------------------------------------------------------
# Duplicate-channel guard
# ---------------------------------------------------------------------------

def get_existing_ms_channel_names(client: KalturaClient) -> set:
    """Return channel name segments already under FULL_NAME_PREFIX."""
    filt = KalturaCategoryFilter()
    filt.fullNameStartsWith = FULL_NAME_PREFIX
    pager = KalturaFilterPager()
    pager.pageSize = 500
    pager.pageIndex = 1
    names: set = set()
    while True:
        resp = client.category.list(filt, pager)
        for cat in resp.objects:
            if ">" in cat.fullName:
                names.add(cat.fullName.split(">")[-1].strip())
        if len(resp.objects) < pager.pageSize:
            break
        pager.pageIndex += 1
    return names


# ---------------------------------------------------------------------------
# Canvas category lookup
# ---------------------------------------------------------------------------

def find_canvas_root_category(
    client: KalturaClient, canvas_course_id: str
):
    """
    Return the KalturaCategory for Canvas_Prod>site>channels><id>, or None.
    """
    target = f"{CANVAS_CAT_PREFIX}{canvas_course_id}"
    filt = KalturaCategoryFilter()
    filt.nameOrReferenceIdStartsWith = canvas_course_id
    pager = KalturaFilterPager()
    pager.pageSize = 100
    pager.pageIndex = 1
    resp = client.category.list(filt, pager)
    for cat in resp.objects:
        if cat.fullName.strip() == target:
            return cat
    return None


def get_all_category_ids_in_subtree(
    client: KalturaClient, root_id: int
) -> list:
    """Return [root_id] + IDs of all descendant categories."""
    ids = [root_id]
    filt = KalturaCategoryFilter()
    filt.ancestorIdIn = str(root_id)
    pager = KalturaFilterPager()
    pager.pageSize = 500
    pager.pageIndex = 1
    while True:
        resp = client.category.list(filt, pager)
        ids.extend(cat.id for cat in resp.objects)
        if len(resp.objects) < pager.pageSize:
            break
        pager.pageIndex += 1
    return ids


# ---------------------------------------------------------------------------
# Entry collection
# ---------------------------------------------------------------------------

def get_entry_ids_across_categories(
    client: KalturaClient, category_ids: list
) -> set:
    """
    Return the deduplicated set of entry IDs across all given category IDs
    (covers both the main gallery and InContext subcategories).
    """
    if not category_ids:
        return set()
    entry_ids: set = set()
    filt = KalturaCategoryEntryFilter()
    filt.categoryIdIn = ",".join(str(i) for i in category_ids)
    pager = KalturaFilterPager()
    pager.pageSize = 500
    pager.pageIndex = 1
    while True:
        resp = client.categoryEntry.list(filt, pager)
        if not resp.objects:
            break
        for ce in resp.objects:
            entry_ids.add(ce.entryId)
        if len(resp.objects) < pager.pageSize:
            break
        pager.pageIndex += 1
    return entry_ids


def get_entry_details(client: KalturaClient, entry_ids: set) -> dict:
    """Return {entry_id: KalturaBaseEntry} for a set of IDs (batch 100)."""
    details = {}
    ids = list(entry_ids)
    batch_size = 100
    for i in range(0, len(ids), batch_size):
        batch = ids[i:i + batch_size]
        filt = KalturaBaseEntryFilter()
        filt.idIn = ",".join(batch)
        pager = KalturaFilterPager()
        pager.pageSize = batch_size
        resp = client.baseEntry.list(filt, pager)
        for entry in resp.objects:
            details[entry.id] = entry
    return details


# ---------------------------------------------------------------------------
# Channel creation and population
# ---------------------------------------------------------------------------

def create_channel(
    client: KalturaClient, channel_name: str, owner_username: str
):
    """Create a private MediaSpace channel; return the new KalturaCategory."""
    cat = KalturaCategory()
    cat.name = channel_name
    cat.owner = owner_username
    cat.privacy = CHANNEL_PRIVACY
    cat.userJoinPolicy = USER_JOIN_POLICY
    cat.appearInList = APPEAR_IN_LIST
    cat.inheritanceType = INHERITANCE_TYPE
    cat.defaultPermissionLevel = DEFAULT_PERMISSION_LEVEL
    cat.contributionPolicy = CONTRIBUTION_POLICY
    cat.moderation = MODERATION
    cat.parentId = PARENT_ID
    cat.privacyContext = PRIVACY_CONTEXT
    return client.category.add(cat)


def add_channel_member(
    client: KalturaClient,
    category_id: int,
    username: str,
    perm_level,
    course_id: str = "",
) -> bool:
    """Add a user to a channel with retry. Returns True on success."""
    cu = KalturaCategoryUser()
    cu.categoryId = category_id
    cu.userId = username
    cu.permissionLevel = perm_level
    try:
        with_retry(
            lambda: client.categoryUser.add(cu),
            course_id=course_id,
            label=f"add {username}",
        )
        return True
    except Exception as exc:
        log(course_id, f"WARNING: could not add {username}: {exc}")
        return False


def publish_entry(
    client: KalturaClient,
    entry_id: str,
    category_id: int,
    course_id: str = "",
) -> str:
    """
    Publish an entry to a channel category with retry.
    Returns 'ok', 'already_published', or 'error'.
    """
    ce = KalturaCategoryEntry()
    ce.categoryId = category_id
    ce.entryId = entry_id
    try:
        with_retry(
            lambda: client.categoryEntry.add(ce),
            course_id=course_id,
            label=f"publish {entry_id}",
        )
        return "ok"
    except Exception as exc:
        msg = str(exc).lower()
        if "already assigned" in msg or "already_exists" in msg:
            return "already_published"
        log(course_id, f"WARNING: could not publish {entry_id}: {exc}")
        return "error"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def ms_channel_url(channel_name: str, category_id: int) -> str:
    encoded = quote_plus(quote_plus(channel_name))
    return (
        f"{MEDIA_SPACE_BASE_URL.rstrip('/')}/{encoded}/{category_id}"
    )


def format_ts(unix_ts) -> str:
    if not unix_ts:
        return ""
    dt = datetime.fromtimestamp(unix_ts, tz=pytz.utc).astimezone(PACIFIC)
    return dt.strftime("%Y-%m-%d %H:%M:%S %Z")


def out_path(filename: str) -> str:
    """Return the full path for an output file using RUN_TIMESTAMP."""
    return os.path.join(REPORTS_DIR, f"{RUN_TIMESTAMP}_{filename}")


# ---------------------------------------------------------------------------
# Per-course processor (runs inside worker threads)
# ---------------------------------------------------------------------------

def process_course(
    course_id: str, course: dict, course_users: list
) -> dict:
    """
    Create a MediaSpace channel, add members, and publish Canvas entries
    for a single course. Returns {mapping, members, entries}.
    Raises on channel creation failure (marks course as failed in main).
    """
    client = get_client()
    canvas_course_id = course["canvas_course_id"]
    display_name = course["display_name"]
    owner_username = course["owner"]
    members_rows = []
    entries_rows = []

    log(course_id, f"Starting (Canvas ID: {canvas_course_id})")

    # Canvas category lookup
    canvas_cat = find_canvas_root_category(client, canvas_course_id)
    if canvas_cat:
        subtree_ids = get_all_category_ids_in_subtree(
            client, canvas_cat.id
        )
        log(
            course_id,
            f"Canvas category {canvas_cat.id} found; "
            f"{len(subtree_ids)} subtree ID(s)"
        )
    else:
        log(
            course_id,
            "WARNING: No Canvas category found; "
            "entry migration skipped"
        )
        subtree_ids = []

    # Create MediaSpace channel
    try:
        new_channel = create_channel(client, course_id, owner_username)
        log(course_id, f"Channel created (MS ID: {new_channel.id})")
    except Exception as exc:
        log(course_id, f"ERROR creating channel: {exc}")
        raise

    mapping_row = {
        "course_id": course_id,
        "canvas_course_id": canvas_course_id,
        "canvas_display_name": display_name,
        "primary_instructor": owner_username,
        "canvas_kaltura_category_id": (
            canvas_cat.id if canvas_cat else ""
        ),
        "canvas_kaltura_category_fullname": (
            canvas_cat.fullName if canvas_cat else ""
        ),
        "ms_channel_name": course_id,
        "ms_channel_id": new_channel.id,
        "ms_channel_url": ms_channel_url(course_id, new_channel.id),
    }

    # Owner — set via category.owner at creation; record in CSV only
    members_rows.append(
        {
            "course_id": course_id,
            "canvas_course_id": canvas_course_id,
            "ms_channel_name": course_id,
            "ms_channel_id": new_channel.id,
            "username": owner_username,
            "canvas_role": "Teacher",
            "ms_role": "owner",
            "add_status": "ok",
        }
    )

    # Add members in parallel (MEMBER_THREADS at a time)
    # Teacher (non-owner) -> manager; everyone else -> member
    to_add = [
        u for u in course_users if u["username"] != owner_username
    ]
    if to_add:
        log(
            course_id,
            f"Adding {len(to_add):,} user(s) "
            f"({MEMBER_THREADS} at a time)..."
        )

    def _add_member(user):
        canvas_role = user["role"]
        if canvas_role.lower() == "teacher":
            perm = KalturaCategoryUserPermissionLevel.MANAGER
            ms_role = "manager"
        else:
            perm = KalturaCategoryUserPermissionLevel.MEMBER
            ms_role = "member"
        ok = add_channel_member(
            get_client(), new_channel.id,
            user["username"], perm, course_id
        )
        return user, ms_role, ok

    n_managers = n_members = n_errors = done = 0
    with ThreadPoolExecutor(max_workers=MEMBER_THREADS) as mem_pool:
        mem_futures = {
            mem_pool.submit(_add_member, u): u for u in to_add
        }
        for future in as_completed(mem_futures):
            user, ms_role, ok = future.result()
            done += 1
            if ok:
                if ms_role == "manager":
                    n_managers += 1
                else:
                    n_members += 1
            else:
                n_errors += 1
            if done % 50 == 0:
                log(
                    course_id,
                    f"  ... {done:,}/{len(to_add):,} users added"
                )
            members_rows.append(
                {
                    "course_id": course_id,
                    "canvas_course_id": canvas_course_id,
                    "ms_channel_name": course_id,
                    "ms_channel_id": new_channel.id,
                    "username": user["username"],
                    "canvas_role": user["role"],
                    "ms_role": ms_role,
                    "add_status": "ok" if ok else "error",
                }
            )

    err_note = f", {n_errors} error(s)" if n_errors else ""
    log(
        course_id,
        f"Members: {n_managers} manager(s), "
        f"{n_members} member(s){err_note}"
    )

    # Collect and publish entries in parallel
    if subtree_ids:
        entry_ids = get_entry_ids_across_categories(client, subtree_ids)
        if entry_ids:
            log(
                course_id,
                f"{len(entry_ids)} entries; fetching metadata..."
            )
            entry_details = get_entry_details(client, entry_ids)

            def _publish_one(entry_id):
                status = publish_entry(
                    get_client(), entry_id,
                    new_channel.id, course_id
                )
                return entry_id, status, entry_details.get(entry_id)

            n_published = n_already = n_pub_err = done_pub = 0
            n_entries = len(entry_ids)
            with ThreadPoolExecutor(
                max_workers=MEMBER_THREADS
            ) as pub_pool:
                pub_futures = {
                    pub_pool.submit(_publish_one, eid): eid
                    for eid in entry_ids
                }
                for future in as_completed(pub_futures):
                    entry_id, status, entry = future.result()
                    done_pub += 1
                    if status == "ok":
                        n_published += 1
                    elif status == "already_published":
                        n_already += 1
                    else:
                        n_pub_err += 1
                    if done_pub % 10 == 0:
                        log(
                            course_id,
                            f"  ... {done_pub}/{n_entries} "
                            "entries published"
                        )
                    entries_rows.append(
                        {
                            "course_id": course_id,
                            "canvas_course_id": canvas_course_id,
                            "ms_channel_name": course_id,
                            "ms_channel_id": new_channel.id,
                            "entry_id": entry_id,
                            "entry_name": (
                                entry.name if entry else ""
                            ),
                            "creator_user_id": (
                                getattr(entry, "creatorId", None)
                                or getattr(entry, "userId", "")
                                if entry else ""
                            ),
                            "created_at_pacific": (
                                format_ts(entry.createdAt)
                                if entry else ""
                            ),
                            "publish_status": status,
                        }
                    )

            already_note = (
                f", {n_already} already published" if n_already else ""
            )
            pub_err_note = (
                f", {n_pub_err} error(s)" if n_pub_err else ""
            )
            log(
                course_id,
                f"Published {n_published} entry/ies"
                f"{already_note}{pub_err_note}"
            )
        else:
            log(course_id, "No entries found in Canvas category")

    return {
        "mapping": mapping_row,
        "members": members_rows,
        "entries": entries_rows,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    global RUN_TIMESTAMP

    # Resolve input files (CLI args override .env)
    if len(sys.argv) >= 3:
        courses_csv, users_csv = sys.argv[1], sys.argv[2]
    else:
        courses_csv = os.getenv("COURSES_CSV_FILENAME", "").strip()
        users_csv = os.getenv("USERS_CSV_FILENAME", "").strip()
        if not courses_csv or not users_csv:
            print(
                "Error: input files not specified.\n"
                "Either pass them as arguments:\n"
                "  python3 canvas-to-mediaspace.py "
                "<courses_csv> <users_csv>\n"
                "Or set COURSES_CSV_FILENAME and USERS_CSV_FILENAME "
                "in your .env file."
            )
            sys.exit(1)

    for path in (courses_csv, users_csv):
        if not os.path.exists(path):
            print(f"File not found: {path}")
            sys.exit(1)

    start_caffeinate()

    print("Loading course and user data...")
    courses = load_courses(courses_csv)
    users = load_users(users_csv)
    total_users = sum(len(v) for v in users.values())
    print(
        f"  {len(courses)} course(s), "
        f"{total_users:,} user enrollment(s)."
    )

    # Resume detection
    courses_csv_abs = os.path.abspath(courses_csv)
    users_csv_abs = os.path.abspath(users_csv)
    prior = load_state(courses_csv_abs, users_csv_abs)

    if prior:
        completed_ids, prev_ts = prior
        RUN_TIMESTAMP = prev_ts
        print(
            f"Resuming run {prev_ts}: "
            f"{len(completed_ids)} of {len(courses)} course(s) "
            "already completed."
        )
        resuming = True
    else:
        completed_ids = set()
        resuming = False
        print(f"Starting new run: {RUN_TIMESTAMP}")

    courses_to_process = OrderedDict(
        (cid, c) for cid, c in courses.items()
        if cid not in completed_ids
    )
    skipped = len(courses) - len(courses_to_process)
    if skipped:
        print(f"  Skipping {skipped} already-completed course(s).")

    # Open output CSVs (append if resuming, write if fresh)
    mapping_f = members_f = entries_f = None

    n_mapping = n_members = n_entries_written = 0
    failed_courses: set = set()

    try:
        mode = "a" if resuming else "w"
        mapping_f = open(
            out_path("channel_mapping.csv"), mode,
            newline="", encoding="utf-8"
        )
        members_f = open(
            out_path("channel_members.csv"), mode,
            newline="", encoding="utf-8"
        )
        entries_f = open(
            out_path("published_entries.csv"), mode,
            newline="", encoding="utf-8"
        )
        mapping_w = csv.DictWriter(mapping_f, fieldnames=MAPPING_FIELDS)
        members_w = csv.DictWriter(members_f, fieldnames=MEMBERS_FIELDS)
        entries_w = csv.DictWriter(entries_f, fieldnames=ENTRIES_FIELDS)

        if not resuming:
            mapping_w.writeheader()
            members_w.writeheader()
            entries_w.writeheader()

        if not courses_to_process:
            print("All courses already completed.")
        else:
            # Pre-flight duplicate check
            print("Connecting to Kaltura (main thread)...")
            main_client = create_client()
            print("Checking for duplicate MediaSpace channel names...")
            existing_names = get_existing_ms_channel_names(main_client)

            if resuming:
                # Channels that exist but weren't recorded are assumed done
                ghosts = [
                    cid for cid in courses_to_process
                    if cid in existing_names
                ]
                if ghosts:
                    print(
                        f"WARNING: {len(ghosts)} course(s) have channels "
                        "in MediaSpace not recorded in the state file. "
                        "Treating as completed and skipping:"
                    )
                    for g in ghosts:
                        print(f"  - {g}")
                        completed_ids.add(g)
                    courses_to_process = OrderedDict(
                        (cid, c) for cid, c in courses.items()
                        if cid not in completed_ids
                    )
            else:
                conflicts = [
                    cid for cid in courses_to_process
                    if cid in existing_names
                ]
                if conflicts:
                    print(
                        "\nERROR: The following channel names already "
                        "exist in MediaSpace:"
                    )
                    for c in conflicts:
                        print(f"  - {c}")
                    print(
                        "\nNo changes were made. "
                        "Resolve conflicts and re-run."
                    )
                    sys.exit(1)

            print(
                f"\nStarting migration with {THREAD_COUNT} course "
                f"thread(s), {MEMBER_THREADS} member thread(s) each..."
            )

            done_count = len(completed_ids)
            total_count = len(courses)

            with ThreadPoolExecutor(
                max_workers=THREAD_COUNT
            ) as executor:
                futures = {
                    executor.submit(
                        process_course, cid, course,
                        users.get(cid, []),
                    ): cid
                    for cid, course in courses_to_process.items()
                }
                for future in as_completed(futures):
                    cid = futures[future]
                    try:
                        result = future.result()

                        # Write rows immediately — no accumulation
                        mapping_w.writerow(result["mapping"])
                        members_w.writerows(result["members"])
                        entries_w.writerows(result["entries"])
                        mapping_f.flush()
                        members_f.flush()
                        entries_f.flush()

                        n_mapping += 1
                        n_members += len(result["members"])
                        n_entries_written += len(result["entries"])

                        completed_ids.add(cid)
                        # State file now tiny: just completed IDs
                        save_state(
                            completed_ids,
                            courses_csv_abs,
                            users_csv_abs,
                        )
                        done_count += 1
                        with _print_lock:
                            print(
                                f"[{cid}] Done "
                                f"({done_count}/{total_count})"
                            )
                    except Exception as exc:
                        failed_courses.add(cid)
                        with _print_lock:
                            print(f"[{cid}] FAILED: {exc}")

    finally:
        for fh in (mapping_f, members_f, entries_f):
            if fh is not None:
                try:
                    fh.close()
                except Exception:
                    pass

        print(f"\n{'=' * 60}")
        print("Output files:")
        print(f"  {out_path('channel_mapping.csv')}")
        print(f"  {out_path('channel_members.csv')}")
        print(f"  {out_path('published_entries.csv')}")

        if failed_courses:
            print(
                f"\n  {len(failed_courses)} course(s) failed:"
            )
            for cid in sorted(failed_courses):
                print(f"    - {cid}")
            print(
                "  State file preserved. Re-run with the same input "
                "files to retry failed courses."
            )
        else:
            if os.path.exists(STATE_FILE):
                os.remove(STATE_FILE)
            print("\n  State file removed (run complete).")

        print(
            f"\nSummary: {n_mapping} channel(s), "
            f"{n_members:,} member record(s), "
            f"{n_entries_written:,} entry publication(s)."
        )


if __name__ == "__main__":
    main()
