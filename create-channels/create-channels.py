"""
Bulk Kaltura Channel Creation

Create Kaltura MediaSpace channels in bulk from a CSV in input/. Settings
come from a .env file next to the script; column header names are
configurable there too. A record of the run is written to output/.

The admin secret is always prompted at runtime and never read from or
written to disk.

Author: Galen Davis
"""

import csv
import getpass
import os
import sys
import time
import traceback
import unicodedata
from datetime import datetime
from pathlib import Path
from urllib.parse import quote_plus

import requests
from dotenv import dotenv_values, load_dotenv
from KalturaClient import KalturaClient, KalturaConfiguration
from KalturaClient.Plugins.Core import (
    KalturaCategory,
    KalturaCategoryFilter,
    KalturaCategoryUser,
    KalturaCategoryUserPermissionLevel,
    KalturaFilterPager,
    KalturaSessionType,
)
from KalturaClient.exceptions import KalturaClientException

SCRIPT_DIR = Path(__file__).resolve().parent
ENV_PATH = SCRIPT_DIR / ".env"
INPUT_DIR = SCRIPT_DIR / "input"
OUTPUT_DIR = SCRIPT_DIR / "output"

load_dotenv(dotenv_path=ENV_PATH)

# ADMIN_SECRET is prompted at runtime in main() — never read it from .env.
ADMIN_SECRET = ""


# ---------- Env helpers ----------
CLEANED_VARS = []


def env_str(key, default=""):
    """Read an env var, dropping invisible format characters such as
    zero-width spaces (U+200B). They sneak in when IDs are copy-pasted from
    web pages or chat, and Python's strip() keeps them."""
    raw = os.getenv(key, default)
    val = "".join(c for c in raw if unicodedata.category(c) != "Cf")
    if val != raw:
        CLEANED_VARS.append(key)
    return val.strip()


def env_int(key, default):
    raw = env_str(key)
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        print(f"Error: {key} in your .env file must be a number, got {raw!r}.")
        sys.exit(2)


# ---------- Configuration from .env ----------
PARTNER_ID = env_str("KALTURA_PARTNER_ID")
KALTURA_USER = env_str("KALTURA_USER")
SERVICE_URL = env_str("KALTURA_SERVICE_URL", "https://www.kaltura.com")

REQUEST_TIMEOUT = env_int("REQUEST_TIMEOUT", 120)
MAX_NETWORK_RETRIES = env_int("MAX_NETWORK_RETRIES", 5)
NETWORK_RETRY_DELAY = env_int("NETWORK_RETRY_DELAY", 5)

MEDIASPACE_URL = env_str("KALTURA_MEDIASPACE_URL")
PARENT_ID = env_str("KALTURA_PARENT_ID")
FULL_NAME_PREFIX = env_str(
    "KALTURA_FULL_NAME_PREFIX", "MediaSpace>site>channels>"
)
PRIVACY_CONTEXT = env_str("KALTURA_PRIVACY_CONTEXT", "MediaSpace")
USER_JOIN_POLICY = env_int("KALTURA_USER_JOIN_POLICY", 3)
APPEAR_IN_LIST = env_int("KALTURA_APPEAR_IN_LIST", 3)
INHERITANCE_TYPE = env_int("KALTURA_INHERITANCE_TYPE", 2)
DEFAULT_PERMISSION_LEVEL = env_int("KALTURA_DEFAULT_PERMISSION_LEVEL", 3)
CONTRIBUTION_POLICY = env_int("KALTURA_CONTRIBUTION_POLICY", 2)
MODERATION = env_int("KALTURA_MODERATION", 0)

INPUT_FILENAME = env_str("INPUT_FILENAME")

# CSV header names (customize if your CSV uses different headers)
CHANNEL_NAME_HEADER = env_str("KALTURA_CHANNEL_NAME_HEADER", "channelName")
OWNER_ID_HEADER = env_str("KALTURA_OWNER_ID_HEADER", "owner")
CHANNEL_MANAGERS_HEADER = env_str(
    "KALTURA_CHANNEL_MANAGERS_HEADER", "managers"
)
CHANNEL_MODERATORS_HEADER = env_str(
    "KALTURA_CHANNEL_MODERATORS_HEADER", "moderators"
)
CHANNEL_CONTRIBUTORS_HEADER = env_str(
    "KALTURA_CHANNEL_CONTRIBUTORS_HEADER", "contributors"
)
CHANNEL_MEMBERS_HEADER = env_str("KALTURA_CHANNEL_MEMBERS_HEADER", "members")

# Optional user columns, from most to least access. A user listed under more
# than one role gets the highest one.
ROLE_COLUMNS = [
    ("Manager", CHANNEL_MANAGERS_HEADER,
     KalturaCategoryUserPermissionLevel.MANAGER),
    ("Moderator", CHANNEL_MODERATORS_HEADER,
     KalturaCategoryUserPermissionLevel.MODERATOR),
    ("Contributor", CHANNEL_CONTRIBUTORS_HEADER,
     KalturaCategoryUserPermissionLevel.CONTRIBUTOR),
    ("Member", CHANNEL_MEMBERS_HEADER,
     KalturaCategoryUserPermissionLevel.MEMBER),
]
PRIVACY_SETTING_HEADER = env_str("KALTURA_PRIVACY_SETTING_HEADER", "privacy")

# Privacy for every channel. A privacy column in the CSV (named by
# KALTURA_PRIVACY_SETTING_HEADER) overrides it row by row.
CHANNEL_PRIVACY = env_str("KALTURA_CHANNEL_PRIVACY")

# Names used before v2.0.0. If any are still in .env, the script stops and
# says what to rename, rather than silently ignoring them.
LEGACY_NAMES = {
    "PARTNER_ID": "KALTURA_PARTNER_ID",
    "ADMIN_SECRET": None,
    "USER_ID": "KALTURA_USER",
    "SERVICE_URL": "KALTURA_SERVICE_URL",
    "MEDIA_SPACE_BASE_URL": "KALTURA_MEDIASPACE_URL",
    "PARENT_ID": "KALTURA_PARENT_ID",
    "FULL_NAME_PREFIX": "KALTURA_FULL_NAME_PREFIX",
    "PRIVACY_CONTEXT": "KALTURA_PRIVACY_CONTEXT",
    "USER_JOIN_POLICY": "KALTURA_USER_JOIN_POLICY",
    "APPEAR_IN_LIST": "KALTURA_APPEAR_IN_LIST",
    "INHERITANCE_TYPE": "KALTURA_INHERITANCE_TYPE",
    "DEFAULT_PERMISSION_LEVEL": "KALTURA_DEFAULT_PERMISSION_LEVEL",
    "CONTRIBUTION_POLICY": "KALTURA_CONTRIBUTION_POLICY",
    "MODERATION": "KALTURA_MODERATION",
    "INPUT_CSV_FILENAME": "INPUT_FILENAME",
    "CHANNEL_NAME_HEADER": "KALTURA_CHANNEL_NAME_HEADER",
    "OWNER_ID_HEADER": "KALTURA_OWNER_ID_HEADER",
    "CHANNEL_MEMBERS_HEADER": "KALTURA_CHANNEL_MEMBERS_HEADER",
    "PRIVACY_SETTING_HEADER": "KALTURA_PRIVACY_SETTING_HEADER",
}


def check_legacy_names():
    """Stop if .env still uses pre-v2.0.0 variable names. Reads the file
    itself, not the shell environment, so shell variables can't trigger
    it."""
    if not ENV_PATH.exists():
        return
    found = [k for k in dotenv_values(ENV_PATH) if k in LEGACY_NAMES]
    if not found:
        return
    print(
        "\n❌ Your .env file uses variable names from an older version of "
        "this script.\n   As of v2.0.0 they start with KALTURA_. Please "
        "update .env:\n"
    )
    for old in found:
        new = LEGACY_NAMES[old]
        if new:
            print(f"   {old:26} → rename to {new}")
        else:
            print(
                f"   {old:26} → delete this line (the secret is typed in "
                "when the script runs)"
            )
    print("\n   See .env.example for the full list.\n")
    sys.exit(2)


# ---------- Network retry ----------
def call_with_retry(fn, *args, **kwargs):
    """Call fn(*args, **kwargs), retrying with linear backoff on transient
    network failures (timeouts, connection resets). Kaltura raises those as
    KalturaClientException, which is NOT a subclass of KalturaException, so
    it is caught here separately; requests' own errors are covered too. A
    real, well-formed API error (KalturaException) is never retried."""
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


class _SdkRetryLogger:
    """The SDK silently retries failed HTTP requests (5 tries over about 75
    seconds) and only reports them to a logger. Print those retries so a
    slow or failing connection doesn't look like a hang; drop all other
    SDK debug output."""

    def log(self, msg):
        if "retrying" in msg:
            summary = msg.split(" Context:")[0]
            print(f"    [Kaltura SDK: {summary}]", flush=True)


# ---------- Kaltura session ----------
def start_client(admin_secret):
    config = KalturaConfiguration()
    config.serviceUrl = SERVICE_URL
    config.requestTimeout = REQUEST_TIMEOUT
    # Must be set before KalturaClient(config): the client decides whether
    # to log in its constructor.
    config.setLogger(_SdkRetryLogger())
    client = KalturaClient(config)
    try:
        ks = call_with_retry(
            client.session.start,
            admin_secret,
            KALTURA_USER or None,
            KalturaSessionType.ADMIN,
            int(PARTNER_ID),
            privileges="all:*,disableentitlement",
        )
    except Exception as e:
        if getattr(e, "code", "") == "START_SESSION_ERROR":
            print(
                "\n❌ Could not log in to Kaltura. Partner ID "
                f"[{PARTNER_ID}] and the Admin Secret were not accepted.\n"
                "   Double-check both values — the secret must be the "
                "Administrator secret (not the User secret),\n"
                "   copied exactly from KMC → Settings → Integration "
                "Settings.\n"
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


# ---------- Input ----------
def resolve_input_path():
    """Resolve INPUT_FILENAME inside input/. A leading "input/" (as older
    .env files had) is accepted too."""
    name = INPUT_FILENAME.replace("\\", "/")
    if name.startswith("input/"):
        name = name[len("input/"):]
    return INPUT_DIR / name


def read_rows(input_path):
    with open(input_path, newline="", encoding="utf-8-sig") as csvfile:
        return list(csv.DictReader(csvfile))


def privacy_column(rows):
    """Return the CSV column holding per-channel privacy values, or "" if
    every channel uses KALTURA_CHANNEL_PRIVACY instead. The column is used
    only when KALTURA_PRIVACY_SETTING_HEADER names one the CSV has."""
    if PRIVACY_SETTING_HEADER and PRIVACY_SETTING_HEADER in rows[0]:
        return PRIVACY_SETTING_HEADER
    return ""


def row_privacy(row, column):
    """A row's own privacy value wins; a blank cell (or no column) falls
    back to KALTURA_CHANNEL_PRIVACY."""
    if column:
        value = (row.get(column) or "").strip()
        if value:
            return value
    return CHANNEL_PRIVACY


def describe_privacy_source(column):
    if column and CHANNEL_PRIVACY:
        return (
            f"'{column}' column; blank cells use {CHANNEL_PRIVACY} "
            "(KALTURA_CHANNEL_PRIVACY)"
        )
    if column:
        return f"'{column}' column"
    if not CHANNEL_PRIVACY:
        return "not set (see the error below)"
    note = ""
    if PRIVACY_SETTING_HEADER:
        note = f"; the CSV has no '{PRIVACY_SETTING_HEADER}' column"
    return (
        f"{CHANNEL_PRIVACY} ({PRIVACY_LABELS.get(CHANNEL_PRIVACY, '?')}) "
        f"for every channel, from KALTURA_CHANNEL_PRIVACY{note}"
    )


def print_column_map(rows, column):
    """Show where each setting comes from before anything else happens, so
    a mistyped header in .env is easy to spot."""
    csv_headers = list(rows[0].keys())

    def source(header, required):
        if not header:
            return "(not used)"
        if header in csv_headers:
            return f"'{header}' column"
        if required:
            return f"MISSING: the CSV has no '{header}' column"
        return f"(not used; the CSV has no '{header}' column)"

    lines = [
        ("Channel name", source(CHANNEL_NAME_HEADER, True)),
        ("Owner", source(OWNER_ID_HEADER, True)),
        ("Privacy", describe_privacy_source(column)),
    ]
    lines += [
        (f"{label}s", source(header, False))
        for label, header, _level in ROLE_COLUMNS
    ]
    used = {CHANNEL_NAME_HEADER, OWNER_ID_HEADER, column}
    used |= {header for _label, header, _level in ROLE_COLUMNS}
    ignored = [h for h in csv_headers if h not in used]

    print("\nWhere each setting comes from:")
    for label, text in lines:
        print(f"  {label + ':':14} {text}")
    if ignored:
        print(f"  {'Ignored:':14} {', '.join(ignored)}")
    print()


def validate_rows(rows, column):
    """Check headers and every row before anything is created. Returns
    True if the CSV is usable."""
    if CHANNEL_PRIVACY and CHANNEL_PRIVACY not in ("1", "2", "3"):
        print(
            "Error: KALTURA_CHANNEL_PRIVACY in your .env file must be 1, 2, "
            f"or 3, got {CHANNEL_PRIVACY!r}."
        )
        return False
    if not column and not CHANNEL_PRIVACY:
        print(
            "Error: no privacy setting. Either set KALTURA_CHANNEL_PRIVACY "
            "in .env to use one\nvalue for every channel, or add a privacy "
            "column to your CSV and put its\nheader in "
            "KALTURA_PRIVACY_SETTING_HEADER."
        )
        if PRIVACY_SETTING_HEADER:
            print(
                f"(KALTURA_PRIVACY_SETTING_HEADER is "
                f"'{PRIVACY_SETTING_HEADER}', but the CSV has no column "
                "by that name.)"
            )
        return False

    required_headers = {CHANNEL_NAME_HEADER, OWNER_ID_HEADER}
    csv_headers = set(rows[0].keys()) if rows else set()
    missing_headers = required_headers - csv_headers
    if missing_headers:
        print(
            "Error: missing column headers in input CSV:"
            f" {', '.join(sorted(missing_headers))}"
        )
        return False

    for i, row in enumerate(rows, start=2):
        missing_fields = [
            field_name for field_name, header_key in [
                ("channelName", CHANNEL_NAME_HEADER),
                ("owner", OWNER_ID_HEADER),
            ]
            if not (row.get(header_key) or "").strip()
        ]
        if not row_privacy(row, column):
            missing_fields.append(
                "privacy (or set KALTURA_CHANNEL_PRIVACY for blank cells)"
            )
        if missing_fields:
            channel_preview = (
                (row.get(CHANNEL_NAME_HEADER) or "").strip() or "<unnamed>"
            )
            print(
                f"Error: row {i}: missing field(s):"
                f" {', '.join(missing_fields)}"
                f" (channelName: '{channel_preview}')"
            )
            return False

        privacy = row_privacy(row, column)
        if privacy not in ("1", "2", "3"):
            print(
                f"Error: row {i}: invalid privacy value"
                f" '{privacy}'. Must be 1, 2, or 3."
            )
            return False

        assignments, skipped_owner = role_assignments(row)
        channel_name = row[CHANNEL_NAME_HEADER].strip()
        if not assignments:
            print(
                f"⚠️  Row {i}: no users besides the owner for channel"
                f" '{channel_name}'."
            )
        if skipped_owner:
            print(
                f"ℹ️  Row {i}: the owner of '{channel_name}' is also listed "
                "under a role; they're left out\n   of the role lists, since "
                "the owner already has full control."
            )
    return True


# ---------- Channels ----------
def get_existing_channel_names(client):
    cat_filter = KalturaCategoryFilter()
    cat_filter.fullNameStartsWith = FULL_NAME_PREFIX
    pager = KalturaFilterPager()
    pager.pageSize = 500
    pager.pageIndex = 1

    existing_names = set()

    while True:
        response = call_with_retry(client.category.list, cat_filter, pager)
        for category in response.objects:
            full_path = category.fullName.strip()
            if full_path.startswith(FULL_NAME_PREFIX):
                last_segment = full_path.split(">")[-1].strip()
                existing_names.add(last_segment)
        if len(response.objects) < pager.pageSize:
            break
        pager.pageIndex += 1

    return existing_names


def channel_link_base():
    """KALTURA_MEDIASPACE_URL may be the site URL or already end in
    /channel/; accept either."""
    if "/channel/" in MEDIASPACE_URL:
        return MEDIASPACE_URL
    return MEDIASPACE_URL.rstrip("/") + "/channel/"


def split_ids(cell):
    """Parse a comma-separated cell into stripped, non-empty user IDs."""
    return [u.strip() for u in (cell or "").split(",") if u.strip()]


def role_assignments(row):
    """Return ([(userId, role_label, permission_level)], skipped_owner),
    one entry per user, at the highest role they're listed under. The
    channel owner is left out: Kaltura already gives the owner full
    control."""
    owner = (row.get(OWNER_ID_HEADER) or "").strip().lower()
    assignments = []
    seen = set()
    skipped_owner = False
    for label, header, level in ROLE_COLUMNS:
        if not header:
            continue
        for user_id in split_ids(row.get(header)):
            if user_id.lower() == owner:
                skipped_owner = True
                continue
            if user_id.lower() in seen:
                continue
            seen.add(user_id.lower())
            assignments.append((user_id, label, level))
    return assignments, skipped_owner


def describe_roles(assignments):
    counts = [
        f"{label.lower()}s: {n}"
        for label, _header, _level in ROLE_COLUMNS
        if (n := sum(1 for _u, lbl, _l in assignments if lbl == label))
    ]
    return ", ".join(counts) if counts else "no other users"


PRIVACY_LABELS = {
    "1": "anyone",
    "2": "logged-in users",
    "3": "members only",
}


def confirm(rows, column):
    print(
        f"\nAbout to create {len(rows)} channel(s) under parent category "
        f"{PARENT_ID}:\n"
    )
    for row in rows:
        assignments, _skipped = role_assignments(row)
        privacy = row_privacy(row, column)
        print(
            f"  - {row[CHANNEL_NAME_HEADER].strip()}"
            f" [owner: {row[OWNER_ID_HEADER].strip()},"
            f" privacy: {privacy} ({PRIVACY_LABELS[privacy]}),"
            f" {describe_roles(assignments)}]"
        )
    answer = input("\nCreate these channels? [y/N]: ").strip().lower()
    return answer in ("y", "yes")


def create_channel(client, row, link_base, column):
    """Create one channel and add its users by role. A user who can't be
    added is recorded and skipped, so one bad user ID doesn't stop the
    run."""
    channel_name = row[CHANNEL_NAME_HEADER].strip()
    owner = row[OWNER_ID_HEADER].strip()
    assignments, _skipped = role_assignments(row)
    privacy = row_privacy(row, column)

    category = KalturaCategory()
    category.name = channel_name
    category.owner = owner
    category.privacy = int(privacy)
    category.userJoinPolicy = USER_JOIN_POLICY
    category.appearInList = APPEAR_IN_LIST
    category.inheritanceType = INHERITANCE_TYPE
    category.defaultPermissionLevel = DEFAULT_PERMISSION_LEVEL
    category.contributionPolicy = CONTRIBUTION_POLICY
    category.moderation = MODERATION
    category.parentId = int(PARENT_ID)
    category.privacyContext = PRIVACY_CONTEXT

    created = call_with_retry(client.category.add, category)
    print(f"Created channel: {created.id} ({channel_name}) [Owner: {owner}]")

    added = {label: [] for label, _header, _level in ROLE_COLUMNS}
    failed = []
    for user_id, label, level in assignments:
        category_user = KalturaCategoryUser()
        category_user.categoryId = created.id
        category_user.userId = user_id
        category_user.permissionLevel = level
        try:
            call_with_retry(client.categoryUser.add, category_user)
            added[label].append(user_id)
            print(f"  Added {label.lower()}: {user_id}")
        except Exception as e:
            failed.append(f"{user_id} as {label.lower()} ({e})")
            print(f"  ✖ Could not add {label.lower()} {user_id}: {e}")

    result = {
        "channelName": channel_name,
        "categoryId": created.id,
        "channelLink": (
            f"{link_base}{quote_plus(quote_plus(channel_name))}/{created.id}"
        ),
        "owner": owner,
        "privacy": privacy,
    }
    for label, users in added.items():
        result[f"{label.lower()}sAdded"] = ", ".join(users)
    result["usersFailed"] = "; ".join(failed)
    return result


def write_results(results):
    OUTPUT_DIR.mkdir(exist_ok=True)
    stamp = datetime.now().strftime("%Y-%m-%d-%H%M")
    path = OUTPUT_DIR / f"{stamp}_create-channels.csv"
    fieldnames = [
        "channelName", "categoryId", "channelLink", "owner", "privacy",
        "managersAdded", "moderatorsAdded", "contributorsAdded",
        "membersAdded", "usersFailed",
    ]
    with open(path, mode="w", newline="", encoding="utf-8") as out:
        writer = csv.DictWriter(out, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)
    return path


def main():
    check_legacy_names()

    if os.getenv("ADMIN_SECRET"):
        print(
            "⚠️  ADMIN_SECRET is set in your environment and is being "
            "ignored. Delete it;\n   the secret is typed in when the script "
            "runs.\n"
        )
    if CLEANED_VARS:
        print(
            "⚠️  Removed invisible characters (e.g. zero-width spaces) "
            f"from: {', '.join(CLEANED_VARS)}.\n   They usually come from "
            "copy-pasting; consider retyping those lines in .env.\n"
        )

    missing = [
        key for key, val in (
            ("KALTURA_PARTNER_ID", PARTNER_ID),
            ("KALTURA_PARENT_ID", PARENT_ID),
            ("KALTURA_MEDIASPACE_URL", MEDIASPACE_URL),
            ("INPUT_FILENAME", INPUT_FILENAME),
        ) if not val
    ]
    if missing:
        for key in missing:
            print(f"Error: {key} is not set in your .env file.")
        return
    if not PARTNER_ID.isdigit():
        print("Error: KALTURA_PARTNER_ID must be a number.")
        return
    if not PARENT_ID.isdigit():
        print("Error: KALTURA_PARENT_ID must be a number (a category ID).")
        return

    input_path = resolve_input_path()
    if not input_path.is_file():
        print(
            f"Error: input file not found: {input_path}\n"
            "Put your CSV in the input/ folder next to the script and set "
            "INPUT_FILENAME\nin .env to its filename."
        )
        return
    rows = read_rows(input_path)
    if not rows:
        print(f"Error: {input_path.name} has no data rows.")
        return
    print(f"📄 Using input file: input/{input_path.name}")

    # Validate every row before logging in or making any changes.
    column = privacy_column(rows)
    print_column_map(rows, column)
    if not validate_rows(rows, column):
        return

    global ADMIN_SECRET
    ADMIN_SECRET = getpass.getpass("Enter your Kaltura admin secret: ")
    if not ADMIN_SECRET:
        print("Error: Admin secret cannot be empty.")
        return

    client = start_client(ADMIN_SECRET)

    existing_channel_names = get_existing_channel_names(client)
    duplicate_names = [
        row[CHANNEL_NAME_HEADER].strip()
        for row in rows
        if row[CHANNEL_NAME_HEADER].strip() in existing_channel_names
    ]
    if duplicate_names:
        print(
            "🚫 The following channel names already exist and cannot"
            " be reused:"
        )
        for name in duplicate_names:
            print(f"  - {name}")
        print(
            "\nNo channels were created. Please update your CSV file"
            " to remove or rename the duplicates and try again."
        )
        return

    if not confirm(rows, column):
        print("Cancelled. No channels were created.")
        return

    link_base = channel_link_base()
    results = []
    try:
        for row in rows:
            results.append(create_channel(client, row, link_base, column))
    finally:
        # Always write whatever was completed, even if an error
        # interrupted the run.
        if results:
            path = write_results(results)
            print(f"\nResults saved to {path.relative_to(SCRIPT_DIR)}")

    failed = [r for r in results if r["usersFailed"]]
    if failed:
        print(
            f"\n⚠️  {len(failed)} channel(s) had users who could not be "
            "added. Check each user ID,\n   then add them in MediaSpace or "
            "KMC. The results CSV lists them under usersFailed."
        )
    print(f"\nDone: {len(results)} channel(s) created.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("✖ Unhandled error:", e)
        traceback.print_exc()
