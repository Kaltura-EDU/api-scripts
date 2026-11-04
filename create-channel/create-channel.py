"""
Creates a new MediaSpace channel (Kaltura category) with specified
properties. Optionally adds managers, moderators, contributors, and members
at the time of creation, and writes a record of the run to output/.

The admin secret is always prompted at runtime and never read from or
written to disk.
"""

import csv
import getpass
import os
import sys
import time
import traceback
import urllib.parse
from datetime import datetime
from pathlib import Path

import requests
from dotenv import dotenv_values, load_dotenv
from KalturaClient import KalturaClient, KalturaConfiguration
from KalturaClient.Plugins.Core import (
    KalturaCategory,
    KalturaCategoryUser,
    KalturaCategoryUserPermissionLevel,
    KalturaSessionType,
)
from KalturaClient.exceptions import KalturaClientException

SCRIPT_DIR = Path(__file__).resolve().parent
ENV_PATH = SCRIPT_DIR / ".env"
OUTPUT_DIR = SCRIPT_DIR / "output"

load_dotenv(dotenv_path=ENV_PATH)


# ---------- Env helpers ----------
def env_str(key, default=""):
    return os.getenv(key, default).strip()


def env_int(key, default):
    raw = env_str(key)
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        print(f"Error: {key} in your .env file must be a number, got {raw!r}.")
        sys.exit(2)


def env_list(key):
    """Parse a comma-separated env var into stripped, non-empty strings."""
    return [v.strip() for v in env_str(key).split(",") if v.strip()]


# ---------- Configuration from .env ----------
PARTNER_ID = env_str("KALTURA_PARTNER_ID")
KALTURA_USER = env_str("KALTURA_USER")
SERVICE_URL = env_str("KALTURA_SERVICE_URL", "https://www.kaltura.com")

REQUEST_TIMEOUT = env_int("REQUEST_TIMEOUT", 120)
MAX_NETWORK_RETRIES = env_int("MAX_NETWORK_RETRIES", 5)
NETWORK_RETRY_DELAY = env_int("NETWORK_RETRY_DELAY", 5)

MEDIASPACE_URL = env_str("KALTURA_MEDIASPACE_URL").rstrip("/")
PARENT_ID_RAW = env_str("KALTURA_PARENT_ID")
PRIVACY_CONTEXT = env_str("KALTURA_PRIVACY_CONTEXT", "MediaSpace")

CHANNEL_NAME = env_str("KALTURA_CHANNEL_NAME")
CHANNEL_DESCRIPTION = env_str("KALTURA_CHANNEL_DESCRIPTION")
CHANNEL_OWNER = env_str("KALTURA_CHANNEL_OWNER")

# Role lists, from most to least access. A user listed under more than one
# role gets the highest one.
ROLE_LISTS = [
    ("Manager", "KALTURA_MANAGERS",
     KalturaCategoryUserPermissionLevel.MANAGER),
    ("Moderator", "KALTURA_MODERATORS",
     KalturaCategoryUserPermissionLevel.MODERATOR),
    ("Contributor", "KALTURA_CONTRIBUTORS",
     KalturaCategoryUserPermissionLevel.CONTRIBUTOR),
    ("Member", "KALTURA_MEMBERS",
     KalturaCategoryUserPermissionLevel.MEMBER),
]

CHANNEL_PRIVACY = env_int("KALTURA_CHANNEL_PRIVACY", 3)
USER_JOIN_POLICY = env_int("KALTURA_USER_JOIN_POLICY", 3)
APPEAR_IN_LIST = env_int("KALTURA_APPEAR_IN_LIST", 3)
INHERITANCE_TYPE = env_int("KALTURA_INHERITANCE_TYPE", 2)
DEFAULT_PERMISSION_LEVEL = env_int("KALTURA_DEFAULT_PERMISSION_LEVEL", 3)
CONTRIBUTION_POLICY = env_int("KALTURA_CONTRIBUTION_POLICY", 2)
MODERATION = env_int("KALTURA_MODERATION", 0)

# Names used before v1.4.0. If any are still in .env, the script stops and
# says what to rename, rather than silently ignoring them.
LEGACY_NAMES = {
    "PARTNER_ID": "KALTURA_PARTNER_ID",
    "ADMIN_SECRET": None,
    "USER_ID": "KALTURA_USER",
    "MEDIA_SPACE_URL": "KALTURA_MEDIASPACE_URL",
    "PARENT_ID": "KALTURA_PARENT_ID",
    "PRIVACY_CONTEXT": "KALTURA_PRIVACY_CONTEXT",
    "CHANNEL_DESCRIPTION": "KALTURA_CHANNEL_DESCRIPTION",
    "CHANNEL_NAME": "KALTURA_CHANNEL_NAME",
    "OWNER": "KALTURA_CHANNEL_OWNER",
    "MEMBERS": "KALTURA_MEMBERS",
    "CHANNEL_PRIVACY": "KALTURA_CHANNEL_PRIVACY",
    "USER_JOIN_POLICY": "KALTURA_USER_JOIN_POLICY",
    "APPEAR_IN_LIST": "KALTURA_APPEAR_IN_LIST",
    "INHERITANCE_TYPE": "KALTURA_INHERITANCE_TYPE",
    "DEFAULT_PERMISSION_LEVEL": "KALTURA_DEFAULT_PERMISSION_LEVEL",
    "CONTRIBUTION_POLICY": "KALTURA_CONTRIBUTION_POLICY",
    "MODERATION": "KALTURA_MODERATION",
}


def check_legacy_names():
    """Stop if .env still uses pre-v1.4.0 variable names. Reads the file
    itself, not the shell environment, so shell variables can't trigger
    it."""
    if not ENV_PATH.exists():
        return
    found = [k for k in dotenv_values(ENV_PATH) if k in LEGACY_NAMES]
    if not found:
        return
    print(
        "\n❌ Your .env file uses variable names from an older version of "
        "this script.\n   As of v1.4.0 they start with KALTURA_. Please "
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


# ---------- Kaltura session ----------
def start_client(admin_secret):
    config = KalturaConfiguration()
    config.serviceUrl = SERVICE_URL
    config.requestTimeout = REQUEST_TIMEOUT
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


# ---------- Channel setup ----------
def build_role_assignments():
    """Return [(userId, role_label, permission_level)], one per user, with
    each user at the highest role they're listed under. The channel owner
    is left out: Kaltura already gives the owner full control."""
    assignments = []
    seen = set()
    skipped_owner = False
    for label, key, level in ROLE_LISTS:
        for user_id in env_list(key):
            if user_id.lower() == CHANNEL_OWNER.lower():
                skipped_owner = True
                continue
            if user_id.lower() in seen:
                continue
            seen.add(user_id.lower())
            assignments.append((user_id, label, level))
    return assignments, skipped_owner


def channel_url(category):
    encoded_name = urllib.parse.quote(category.name).replace(" ", "+")
    double_encoded_name = urllib.parse.quote(encoded_name)
    return f"{MEDIASPACE_URL}/channel/{double_encoded_name}/{category.id}"


def confirm(assignments, skipped_owner):
    print("\nAbout to create this channel:\n-----------------------------")
    print(f"{'Channel Name:':22} {CHANNEL_NAME}")
    print(f"{'Description:':22} {CHANNEL_DESCRIPTION or '(none)'}")
    print(f"{'Owner:':22} {CHANNEL_OWNER or '(none set)'}")
    print(f"{'Parent Category ID:':22} {PARENT_ID_RAW or '(none: top level)'}")
    for label, _key, _level in ROLE_LISTS:
        users = [u for u, lbl, _ in assignments if lbl == label]
        print(f"{label + 's:':22} {', '.join(users) if users else '(none)'}")
    if skipped_owner:
        print(
            "\nNote: the owner was also listed under a role. They were left "
            "out of the role lists,\nsince the owner already has full "
            "control of the channel."
        )
    if not CHANNEL_OWNER:
        print(
            "\nWarning: KALTURA_CHANNEL_OWNER is blank, so the channel will "
            "have no owner."
        )
    print(
        "\nThis settings list comes from your .env file. Check it for "
        "values left over from an earlier run."
    )
    answer = input("\nCreate this channel? [y/N]: ").strip().lower()
    return answer in ("y", "yes")


def write_record(created, url, results):
    OUTPUT_DIR.mkdir(exist_ok=True)
    stamp = datetime.now().strftime("%Y-%m-%d-%H%M")
    path = OUTPUT_DIR / f"{stamp}_create-channel.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            ["category_id", "channel_name", "channel_url", "user_id",
             "role", "status", "error"]
        )
        writer.writerow(
            [created.id, created.name, url, created.owner or "",
             "Owner", "ok", ""]
        )
        for user_id, label, status, error in results:
            writer.writerow(
                [created.id, created.name, url, user_id, label, status,
                 error]
            )
    return path


def main():
    check_legacy_names()

    missing = [
        key for key, val in (
            ("KALTURA_PARTNER_ID", PARTNER_ID),
            ("KALTURA_CHANNEL_NAME", CHANNEL_NAME),
            ("KALTURA_MEDIASPACE_URL", MEDIASPACE_URL),
        ) if not val
    ]
    if missing:
        for key in missing:
            print(f"Error: {key} is not set in your .env file.")
        return
    if not PARTNER_ID.isdigit():
        print("Error: KALTURA_PARTNER_ID must be a number.")
        return
    if PARENT_ID_RAW and not PARENT_ID_RAW.isdigit():
        print("Error: KALTURA_PARENT_ID must be a number (a category ID).")
        return

    admin_secret = getpass.getpass("Enter your Kaltura admin secret: ")
    if not admin_secret:
        print("Error: Admin secret cannot be empty.")
        return

    # Log in before confirming, so a bad secret fails fast.
    client = start_client(admin_secret)

    assignments, skipped_owner = build_role_assignments()
    if not confirm(assignments, skipped_owner):
        print("Cancelled. No channel was created.")
        return

    # Step 1: Create the channel. Optional text fields are only set when
    # filled in: the SDK sends "" as a real value, not as "leave unset".
    category = KalturaCategory()
    category.name = CHANNEL_NAME
    if CHANNEL_DESCRIPTION:
        category.description = CHANNEL_DESCRIPTION
    if CHANNEL_OWNER:
        category.owner = CHANNEL_OWNER
    if PARENT_ID_RAW:
        category.parentId = int(PARENT_ID_RAW)
    category.privacy = CHANNEL_PRIVACY
    category.userJoinPolicy = USER_JOIN_POLICY
    category.appearInList = APPEAR_IN_LIST
    category.inheritanceType = INHERITANCE_TYPE
    category.defaultPermissionLevel = DEFAULT_PERMISSION_LEVEL
    category.contributionPolicy = CONTRIBUTION_POLICY
    category.moderation = MODERATION
    category.privacyContext = PRIVACY_CONTEXT  # Ensure appearInList works

    created = call_with_retry(client.category.add, category)
    url = channel_url(created)
    print(f"\nChannel created (category ID {created.id}).")

    # Step 2: Add users by role. One failure doesn't stop the rest.
    results = []
    for user_id, label, level in assignments:
        category_user = KalturaCategoryUser()
        category_user.categoryId = created.id
        category_user.userId = user_id
        category_user.permissionLevel = level
        try:
            call_with_retry(client.categoryUser.add, category_user)
            results.append((user_id, label, "ok", ""))
            print(f"  Added {label.lower()}: {user_id}")
        except Exception as e:
            results.append((user_id, label, "failed", str(e)))
            print(f"  ✖ Could not add {label.lower()} {user_id}: {e}")

    record_path = write_record(created, url, results)

    # Summary
    print("\nChannel created.\n-----------------")
    print(f"{'Channel Name:':20} {created.name}")
    print(f"{'Category ID:':20} {created.id}")
    print(f"{'Channel Owner:':20} {created.owner or '(none)'}")
    for label, _key, _level in ROLE_LISTS:
        added = [u for u, lbl, st, _ in results if lbl == label and st == "ok"]
        print(f"{label + 's:':20} {', '.join(added) if added else 'None'}")
    print(f"{'Channel URL:':20} {url}")
    failed = [r for r in results if r[2] == "failed"]
    if failed:
        print(
            f"\n⚠ {len(failed)} user(s) could not be added. Check each "
            "user ID, then add them in MediaSpace\n  or KMC. Details:"
        )
        for user_id, label, _st, error in failed:
            print(f"  - {user_id} ({label}): {error}")
    print(f"\nRecord saved to {record_path.relative_to(SCRIPT_DIR)}")
    print("Script execution complete.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("✖ Unhandled error:", e)
        traceback.print_exc()
